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
)

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
