// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
)

// The engine's background work -- the snapshot rebuild that boot load and
// fallback recovery retry (rebuildOnce, engine.go), and the compaction
// goroutine's fold and adoption (foldAndAdoptCompaction, compact.go) -- runs
// on goroutines no request path can recover for, so a panic in it used to end
// the process. When the panic depended on the data, as kind id 32767 once
// made every snapshot Build panic, the API crashed on every restart: the
// rebuild that panicked was the boot load itself.
//
// Both now recover it into the engine's one always-safe state, fallback:
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
// called with applyMu held. Both callers recover in a frame above every
// applyMu critical section they reach, and each of those unlocks in a defer,
// so the lock is already released by the time a panic arrives here.
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
