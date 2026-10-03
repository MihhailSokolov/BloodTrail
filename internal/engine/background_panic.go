// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// The engine's background work -- the snapshot rebuild that boot load and
// fallback recovery retry (rebuildOnce, engine.go), boot load's one attempt
// to start from the snapshot file (bootFromSnapshotFile, below), and the
// compaction goroutine's fold and adoption (foldAndAdoptCompaction,
// compact.go) -- runs on goroutines no request path can recover for, so a
// panic in it used to end the process. When the panic depended on the data,
// as kind id 32767 once made every snapshot Build panic, the API crashed on
// every restart: the rebuild that panicked was the boot load itself, and a
// file whose reading or replay panicked was read again by every boot until
// someone deleted it.
//
// All three now recover it into the engine's one always-safe state, fallback:
// every query goes to PostgreSQL, exactly what stock BloodHound does, and the
// recovery rebuild keeps retrying on its usual backoff. A panic that depends
// on the data then costs a slower API instead of an outage, and one that does
// not is gone on the next attempt. Serving is never left on: the work that
// panicked is the work that builds or maintains the replica, and a replica
// whose construction panicked cannot be vouched for.

// backgroundPanicked handles one recovered background panic: it logs it at
// ERROR with its stack, enters fallback for it, and returns it as an error.
// what names the work that panicked ("snapshot rebuild", "compaction") and
// prefixes the log message, the fallback reason and the error; attrs are
// added to the log line.
//
// It takes applyMu, which enterFallback's callers hold, so it must not be
// called with applyMu held. Every caller recovers in a frame above every
// applyMu critical section it reaches (bootFromSnapshotFile above
// adoptSnapshotFileAttempt's, for one), and each of those unlocks in a
// defer, so the lock is already released by the time a panic arrives here.
func (e *Engine) backgroundPanicked(ctx context.Context, what string, r any, attrs ...any) error {
	args := append([]any{slog.Any("panic", r), slog.String("stack", string(debug.Stack()))}, attrs...)
	e.cfg.Log.ErrorContext(ctx, "bloodtrail: "+what+" panicked", args...)

	e.applyMu.Lock()
	e.enterFallback(ctx, fmt.Sprintf("%s panicked: %v", what, r))
	e.applyMu.Unlock()

	return fmt.Errorf("engine: %s panicked: %v", what, r)
}

// recoverRebuildPanic is rebuildOnce's deferred recover, and must be deferred
// by it directly (recover only stops a panic when called by the deferred
// function itself). A recovered panic becomes rebuildOnce's result: not
// adopted, with the panic as the error -- which boot load and fallback
// recovery already log and retry on their backoff.
func (e *Engine) recoverRebuildPanic(ctx context.Context, trigger string, adopted *bool, err *error) {
	if r := recover(); r != nil {
		*adopted = false
		*err = e.backgroundPanicked(ctx, "snapshot rebuild", r, slog.String("trigger", trigger))
	}
}

// bootFromSnapshotFile is runBootLoad's one attempt to start from the
// snapshot file: tryLoadSnapshotFile, with a panic anywhere in it -- the
// read, the replay of the boot gap buffer, the warm-up, the publish --
// recovered by recoverSnapshotFileBootPanic instead of ending the process.
func (e *Engine) bootFromSnapshotFile(ctx context.Context) (adopted bool) {
	defer e.recoverSnapshotFileBootPanic(ctx, &adopted)
	return e.tryLoadSnapshotFile(ctx)
}

// recoverSnapshotFileBootPanic is bootFromSnapshotFile's deferred recover,
// and must be deferred by it directly. A recovered panic is logged and
// enters fallback (backgroundPanicked) -- a view the attempt may already
// have published is not vouched for, and the boot loop's rebuild, which
// runs next, replaces it -- and the attempt reports nothing adopted. The
// file is then deleted (durably, as removeSnapshotFile does): a panic that
// depends on the file would otherwise recur at every boot, and deleting a
// snapshot file never costs more than one rebuild. The delete comes after
// the fallback, which keeps any save from starting until that rebuild has
// replaced the view.
func (e *Engine) recoverSnapshotFileBootPanic(ctx context.Context, adopted *bool) {
	r := recover()
	if r == nil {
		return
	}
	*adopted = false
	path, ok := e.snapshotFilePath()
	_ = e.backgroundPanicked(ctx, "snapshot file boot", r, slog.String("path", path))
	if !ok {
		return
	}
	removed, err := snapshot.RemoveSnapshotFile(path)
	switch {
	case err != nil:
		e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file invalidation failed",
			slog.String("path", path), slog.Bool("removed", removed), slog.Any("error", err))
	case removed:
		e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file invalidated",
			slog.String("path", path),
			slog.String("reason", "booting from it panicked"))
	}
}
