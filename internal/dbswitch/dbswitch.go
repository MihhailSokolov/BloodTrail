// SPDX-License-Identifier: Apache-2.0

// Package dbswitch reads and writes BloodHound's database_switch table, which
// names the active graph driver and overrides the bhe_graph_driver setting.
package dbswitch

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

const (
	readSQL  = "select driver from database_switch limit 1"
	countSQL = "select (select count(*) from node) || '|' || (select count(*) from edge)"
	// clearSQL ends the watermark lineage in the same statement string, and
	// so the same transaction (psql -c runs the whole string as one), as the
	// truncate: a graph emptied behind BloodTrail's back must never leave a
	// lineage behind that a saved snapshot file still matches.
	clearSQL = "truncate table edge, node; " + endLineageSQL
)

// endLineageSQL ends BloodTrail's watermark lineage, if BloodTrail ever ran
// against this database (internal/engine's watermark.go, at
// watermarkLineageDDL, has the protocol): a fresh random lineage replaces the
// current one, so no snapshot file saved before this statement names the
// lineage PostgreSQL is in afterwards, and the next BloodTrail boot refuses
// every such file and rebuilds from PostgreSQL instead.
//
// The counter is bumped too, for an engine that predates lineages: a table
// without the lineage column was last run by one, and to it an advance
// nothing will ever account for is what refuses the file. A newer engine
// refuses on the lineage alone and ignores the bump.
//
// A database without the table is left untouched: BloodTrail never ran
// there, and the engine gives the table it creates a lineage of its own.
//
// The table and its columns are the engine's (watermarkDDL and
// watermarkLineageDDL), named again here because this package cannot import
// the engine without pulling it into the command-line tool. What keeps the
// two spellings together is the engine's lineage_integration_test.go, which
// runs this very statement, through Store, against a table the engine
// created, and requires the next boot to refuse the file it ends; this
// package's own integration test covers the table older engines left.
//
// One line, like every statement here, so an error echoing the command
// stays readable.
const endLineageSQL = "do $$ begin " +
	"if to_regclass('bloodtrail_watermark') is null then return; end if; " +
	"update bloodtrail_watermark set counter = counter + 1, updated_at = now() where id = 1; " +
	"if exists (select 1 from pg_attribute where attrelid = to_regclass('bloodtrail_watermark') and attname = 'lineage' and not attisdropped) then " +
	"update bloodtrail_watermark set lineage = gen_random_uuid() where id = 1; " +
	"end if; end $$"

var driverNamePattern = regexp.MustCompile(`^[a-z0-9+_-]{1,32}$`)

func setSQL(driver string) string {
	return "create table if not exists database_switch (driver text not null, primary key(driver)); " +
		"delete from database_switch; insert into database_switch (driver) values ('" + driver + "')"
}

// isMissingRelation checks if an error is specifically about a missing relation (table).
// It returns true only for errors matching `relation "<name>" does not exist` where
// name is one of the provided relation names (case-sensitive).
func isMissingRelation(err error, relations ...string) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	for _, rel := range relations {
		if strings.Contains(errStr, fmt.Sprintf(`relation "%s" does not exist`, rel)) {
			return true
		}
	}
	return false
}

// Store runs psql inside the application database container.
type Store struct {
	Compose  dockerx.Compose
	Service  string
	User     string
	Database string
}

func (s Store) psql(ctx context.Context, sql string) ([]byte, error) {
	return s.Compose.Exec(ctx, s.Service, nil, "psql", "-v", "ON_ERROR_STOP=1", "-U", s.User, "-d", s.Database, "-tAc", sql)
}

// Read returns the active driver row. present is false when the row or the
// table does not exist, which BloodHound treats as "use the configured driver".
func (s Store) Read(ctx context.Context) (string, bool, error) {
	out, err := s.psql(ctx, readSQL)
	if err != nil {
		if isMissingRelation(err, "database_switch") {
			return "", false, nil
		}
		return "", false, err
	}
	driver := strings.TrimSpace(string(out))
	return driver, driver != "", nil
}

// Set replaces the single row with the given driver name.
func (s Store) Set(ctx context.Context, driver string) error {
	if !driverNamePattern.MatchString(driver) {
		return fmt.Errorf("refusing to write driver name %q", driver)
	}
	_, err := s.psql(ctx, setSQL(driver))
	return err
}

// Delete removes the row so BloodHound falls back to bhe_graph_driver.
func (s Store) Delete(ctx context.Context) error {
	_, err := s.psql(ctx, "delete from database_switch")
	if isMissingRelation(err, "database_switch") {
		return nil
	}
	return err
}

// CountGraph returns node and edge counts from the PostgreSQL graph tables,
// zero if the tables are absent (graph still on Neo4j).
func (s Store) CountGraph(ctx context.Context) (int64, int64, error) {
	out, err := s.psql(ctx, countSQL)
	if err != nil {
		if isMissingRelation(err, "node", "edge") {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	parts := strings.Split(strings.TrimSpace(string(out)), "|")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("unexpected count output %q", out)
	}
	nodes, err1 := strconv.ParseInt(parts[0], 10, 64)
	edges, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, fmt.Errorf("unexpected count output %q", out)
	}
	return nodes, edges, nil
}

// ClearGraph empties the PostgreSQL graph tables. BloodHound's migrator only
// inserts, so a second migration into a graph an earlier one already filled
// collides with the unique object id index; clearing first is the only way to
// replace the graph rather than layer on top of it. Absent tables mean there
// is no PostgreSQL graph yet, which is already the wanted state.
//
// The truncate ends BloodTrail's watermark lineage as it commits (clearSQL):
// neither it nor the migration that refills the tables bumps the watermark
// counter, so a snapshot file saved from the graph being replaced would
// otherwise still read as current to the next BloodTrail boot.
func (s Store) ClearGraph(ctx context.Context) error {
	_, err := s.psql(ctx, clearSQL)
	if isMissingRelation(err, "node", "edge") {
		return nil
	}
	return err
}

// EndWatermarkLineage ends BloodTrail's watermark lineage (endLineageSQL), so
// that no snapshot file saved so far can be adopted by a later BloodTrail
// boot. The installer calls it wherever the graph changes hands with a
// writer that does not bump the watermark counter -- the stock BloodHound
// image, BloodHound's migrator: once a rollback has the stock image running,
// and again right before an install starts BloodTrail.
func (s Store) EndWatermarkLineage(ctx context.Context) error {
	_, err := s.psql(ctx, endLineageSQL)
	return err
}
