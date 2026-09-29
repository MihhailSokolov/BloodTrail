// SPDX-License-Identifier: Apache-2.0

//go:build integration

// TestOpenGraphParity replays, through the real driver, the graph calls
// BloodHound makes for OpenGraph data, and checks every step against the
// plain pg driver reading the same database. The call shapes are copied from
// BloodHound v9.6.0 (unchanged through v9.7.1):
//
//   - an upload (services/graphify: ingest.go, ingestnodes.go,
//     ingestrelationships.go) runs inside one BatchOperation. It registers
//     its metadata.source_kind and each node kind in the `kind` table and
//     calls RefreshKinds, then upserts nodes with UpdateNodeBy (objectid
//     identity, the source kind as identity kind and first kind) and edges
//     with UpdateRelationshipBy, creating any endpoint that does not exist
//     yet as a stub;
//   - an edge endpoint given by name or property is first resolved to an
//     objectid by a read query in its own transaction (graphify/endpoint/
//     fetch.go), which cannot see the upload's own unflushed nodes;
//   - the pathfinding endpoint looks both nodes up by objectid and asks
//     FetchAllShortestPaths for the pair (queries/graph.go, analysis.go);
//   - "clear database" deletes by source kind, sourceless data, or edge
//     kind (daemons/datapipe/delete.go).
//
// Each step must leave the replica serving reads from memory with results
// equal to PostgreSQL's, with no fallback and no rebuild.
package integration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

var (
	// The upload's metadata.source_kind. (The failed upload's source kind is
	// named per run: see TestOpenGraphParity.)
	ogtSourceKind = graph.StringKind("OGTBase")

	ogtUser  = graph.StringKind("OGTUser")
	ogtTeam  = graph.StringKind("OGTTeam")
	ogtRepo  = graph.StringKind("OGTRepository")
	ogtRole  = graph.StringKind("OGTRepoRole")
	ogtOwner = graph.StringKind("OGTOrgOwner")
	ogtThing = graph.StringKind("OGTThing") // uploaded without a source kind

	ogtMemberOf = graph.StringKind("OGTMemberOf")
	ogtHasRole  = graph.StringKind("OGTHasRole")
	ogtCanWrite = graph.StringKind("OGTCanWrite")
	ogtCanAdmin = graph.StringKind("OGTCanAdmin")
	ogtCanRead  = graph.StringKind("OGTCanRead")
	ogtSyncedTo = graph.StringKind("OGTSyncedTo")
	ogtLinks    = graph.StringKind("OGTLinks")

	// BloodHound's own kinds: ad.Entity, azure.Entity, common.MigrationData.
	adEntity          = graph.StringKind("Base")
	azEntity          = graph.StringKind("AZBase")
	adUser            = graph.StringKind("User")
	adGroup           = graph.StringKind("Group")
	adMemberOf        = graph.StringKind("MemberOf")
	migrationDataKind = graph.StringKind("MigrationData")

	// ogtTraversable is the edge-kind filter the pathfinding endpoint builds
	// for only_traversable=true once an extension marks these kinds
	// traversable (OGTCanRead is not).
	ogtTraversable = graph.Kinds{adMemberOf, ogtMemberOf, ogtHasRole, ogtCanWrite, ogtCanAdmin, ogtSyncedTo}

	ogtIngestTime = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
)

const ogtADUser = "S-1-5-21-1000000000-2000000000-3000000000-1105"

// mergeNodeKinds is graphify.MergeNodeKinds: the source kind first, then
// the declared kinds, without empty kinds or duplicates.
func mergeNodeKinds(source graph.Kind, kinds ...graph.Kind) graph.Kinds {
	var (
		merged graph.Kinds
		seen   = map[string]bool{}
	)
	for _, kind := range append(graph.Kinds{source}, kinds...) {
		if kind == nil || kind == graph.EmptyKind || seen[kind.String()] {
			continue
		}
		seen[kind.String()] = true
		merged = append(merged, kind)
	}
	return merged
}

// ogtNode is graphify.IngestNode's update: objectid and name upper-cased,
// lastseen set to the ingest time.
func ogtNode(source graph.Kind, objectID string, props map[string]any, labels ...graph.Kind) graph.NodeUpdate {
	normalized := map[string]any{}
	for key, value := range props {
		normalized[key] = value
	}
	normalized["lastseen"] = ogtIngestTime
	normalized["objectid"] = strings.ToUpper(objectID)
	if name, ok := normalized["name"].(string); ok {
		normalized["name"] = strings.ToUpper(name)
	}
	return graph.NodeUpdate{
		Node:               graph.PrepareNode(graph.AsProperties(normalized), mergeNodeKinds(source, labels...)...),
		IdentityKind:       source,
		IdentityProperties: []string{"objectid"},
	}
}

// ogtEndpoint is one resolved edge endpoint: an objectid and its declared
// kind (nil when the upload does not name one).
type ogtEndpoint struct {
	objectID string
	kind     graph.Kind
}

// ogtEdge is one update from graphify.ingestibleRelationshipsToUpdates.
func ogtEdge(source graph.Kind, start ogtEndpoint, kind graph.Kind, end ogtEndpoint, props map[string]any) graph.RelationshipUpdate {
	relProps := map[string]any{"lastseen": ogtIngestTime}
	for key, value := range props {
		relProps[key] = value
	}
	endpoint := func(e ogtEndpoint) *graph.Node {
		return graph.PrepareNode(graph.AsProperties(map[string]any{
			"objectid": strings.ToUpper(e.objectID),
			"lastseen": ogtIngestTime,
		}), mergeNodeKinds(source, e.kind)...)
	}
	return graph.RelationshipUpdate{
		Start:                   endpoint(start),
		StartIdentityKind:       source,
		StartIdentityProperties: []string{"objectid"},
		End:                     endpoint(end),
		EndIdentityKind:         source,
		EndIdentityProperties:   []string{"objectid"},
		Relationship:            graph.PrepareRelationship(graph.AsProperties(relProps), kind),
	}
}

// ogtHarness carries what every step of the replay needs.
type ogtHarness struct {
	ctx    context.Context
	bt     graph.Database
	d      *bloodtrail.Driver
	oracle graph.Database
	pool   *pgxpool.Pool
	buf    *lockedBuffer
}

// registerKinds is RegisterSourceKind / RegisterNodeKind: each kind goes
// into the shared `kind` table, then the driver refreshes its kind map.
func (h ogtHarness) registerKinds(t *testing.T, kinds ...graph.Kind) {
	t.Helper()
	for _, kind := range kinds {
		if kind == nil || kind == graph.EmptyKind {
			continue
		}
		if _, err := h.pool.Exec(h.ctx, "insert into kind (name) values ($1) on conflict do nothing", kind.String()); err != nil {
			t.Fatalf("register kind %s: %v", kind, err)
		}
		if err := h.bt.RefreshKinds(h.ctx); err != nil {
			t.Fatalf("RefreshKinds: %v", err)
		}
	}
}

// resolveObjectID is graphify/endpoint's getNodeObjectID for a match_by
// name (case-insensitive) or property (exact) endpoint.
func resolveObjectID(ctx context.Context, db graph.Database, kind graph.Kind, key string, value string, ignoreCase bool) (string, error) {
	match := query.Equals(query.NodeProperty(key), query.Parameter(value))
	if ignoreCase {
		match = cypher.NewComparison(
			cypher.NewSimpleFunctionInvocation(cypher.ToLowerFunction, query.NodeProperty(key)),
			cypher.OperatorEquals,
			query.Parameter(strings.ToLower(value)),
		)
	}

	var objectID string
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Filter(query.And(query.Kind(query.Node(), kind), match)).Query(func(results graph.Result) error {
			defer results.Close()
			if !results.Next() {
				if err := results.Error(); err != nil {
					return err
				}
				return graph.ErrNoResultsFound
			}
			if err := results.Scan(&objectID); err != nil {
				return err
			}
			if results.Next() {
				return errors.New("ambiguous matcher with more than one node matched")
			}
			return results.Error()
		}, query.Returning(query.NodeProperty("objectid")))
	})
	return objectID, err
}

// resolveBoth resolves an endpoint through both drivers and requires the
// same answer from each.
func (h ogtHarness) resolveBoth(t *testing.T, kind graph.Kind, key, value string, ignoreCase bool) (string, error) {
	t.Helper()
	got, gotErr := resolveObjectID(h.ctx, h.bt, kind, key, value, ignoreCase)
	want, wantErr := resolveObjectID(h.ctx, h.oracle, kind, key, value, ignoreCase)
	if got != want || errors.Is(gotErr, graph.ErrNoResultsFound) != errors.Is(wantErr, graph.ErrNoResultsFound) || (gotErr == nil) != (wantErr == nil) {
		t.Fatalf("resolve %s.%s = %q: bloodtrail (%q, %v), postgresql (%q, %v)", kind, key, value, got, gotErr, want, wantErr)
	}
	return got, gotErr
}

// requireRead runs text through both drivers, requires equal results, and
// requires the replica to have served it from memory.
func (h ogtHarness) requireRead(t *testing.T, text string) {
	t.Helper()
	want, wantErr := runCorpusQuery(t, h.ctx, h.oracle, text)
	before := markerCount(h.buf, cypherServedMarker)
	got, gotErr := runCorpusQuery(t, h.ctx, h.bt, text)
	served := markerCount(h.buf, cypherServedMarker) - before

	if gotErr != nil || wantErr != nil {
		t.Fatalf("%s: bloodtrail error %v, postgresql error %v", text, gotErr, wantErr)
	}
	assertCorpusResultsMatch(t, hasOrderBy(text), !projectsPathVariable(text), got, want)
	if served != 1 {
		t.Fatalf("%s: %q log count changed by %d, want 1", text, cypherServedMarker, served)
	}
}

// requireDelegatedRead runs text through both drivers, requires equal
// results, and requires the replica to have declined it for reason.
func (h ogtHarness) requireDelegatedRead(t *testing.T, text, reason string) {
	t.Helper()
	want, wantErr := runCorpusQuery(t, h.ctx, h.oracle, text)
	logBefore := len(h.buf.String())
	got, gotErr := runCorpusQuery(t, h.ctx, h.bt, text)
	tail := h.buf.String()[logBefore:]

	if gotErr != nil || wantErr != nil {
		t.Fatalf("%s: bloodtrail error %v, postgresql error %v", text, gotErr, wantErr)
	}
	assertCorpusResultsMatch(t, hasOrderBy(text), !projectsPathVariable(text), got, want)
	if strings.Contains(tail, cypherServedMarker) || !strings.Contains(tail, "reason="+reason) {
		t.Fatalf("%s: want a %q decline and no serve, log:\n%s", text, reason, tail)
	}
}

// fetchNodeByObjectIDIncludeOpenGraph is analysis.
// FetchNodeByObjectIDIncludeOpenGraph: an AD node, then an Azure node, then
// any node that is neither.
func fetchNodeByObjectIDIncludeOpenGraph(tx graph.Transaction, objectID string) (*graph.Node, error) {
	for _, kind := range []graph.Kind{adEntity, azEntity} {
		node, err := tx.Nodes().Filter(query.And(
			query.Equals(query.NodeProperty("objectid"), objectID),
			query.Kind(query.Node(), kind),
		)).First()
		if err == nil {
			return node, nil
		}
		if !graph.IsErrNotFound(err) {
			return nil, err
		}
	}
	return tx.Nodes().Filter(query.And(
		query.Equals(query.NodeProperty("objectid"), objectID),
		query.Not(query.Kind(query.Node(), adEntity, azEntity)),
	)).First()
}

// pathfind is the pathfinding endpoint's getAllShortestPathsInternal for a
// start/end objectid pair restricted to edgeKinds, returning its paths'
// signatures, sorted.
func pathfind(t *testing.T, ctx context.Context, db graph.Database, startID, endID string, edgeKinds graph.Kinds) []string {
	t.Helper()
	var paths graph.PathSet
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		start, err := fetchNodeByObjectIDIncludeOpenGraph(tx, startID)
		if err != nil {
			return err
		}
		end, err := fetchNodeByObjectIDIncludeOpenGraph(tx, endID)
		if err != nil {
			return err
		}
		return tx.Relationships().Filter(query.And(
			query.Equals(query.StartID(), start.ID),
			query.Equals(query.EndID(), end.ID),
			query.KindIn(query.Relationship(), edgeKinds...),
		)).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				if len(path.Edges) > 0 {
					paths.AddPath(path)
				}
			}
			return cursor.Error()
		})
	}); err != nil {
		t.Fatalf("pathfind %s -> %s: %v", startID, endID, err)
	}
	signatures := renderPathSignatures(paths)
	sort.Strings(signatures)
	return signatures
}

// requirePathfind runs pathfind through both drivers, requires equal
// results, and requires the replica's path engine to have served it.
func (h ogtHarness) requirePathfind(t *testing.T, startID, endID string, wantPaths int) {
	t.Helper()
	want := pathfind(t, h.ctx, h.oracle, startID, endID, ogtTraversable)
	if len(want) != wantPaths {
		t.Fatalf("pathfind %s -> %s: postgresql returned %d paths, want %d: the fixture no longer shows this case", startID, endID, len(want), wantPaths)
	}
	before := markerCount(h.buf, servedMarker)
	got := pathfind(t, h.ctx, h.bt, startID, endID, ogtTraversable)
	if served := markerCount(h.buf, servedMarker) - before; served != 1 {
		t.Fatalf("pathfind %s -> %s: %q log count changed by %d, want 1", startID, endID, servedMarker, served)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("pathfind %s -> %s differs:\n  bloodtrail: %v\n  postgresql: %v", startID, endID, got, want)
	}
}

// requireCounts checks node and edge counts by kind through the builder
// API, served from memory and equal to PostgreSQL's.
func (h ogtHarness) requireCounts(t *testing.T, nodeKinds, edgeKinds graph.Kinds) {
	t.Helper()
	for _, kind := range nodeKinds {
		want := nodeCountByKind(t, h.ctx, h.oracle, kind)
		requireMarkerDelta(t, h.buf, builderServedMarker, 1, "node count of "+kind.String(),
			func() int64 { return nodeCountByKind(t, h.ctx, h.bt, kind) }, want)
	}
	for _, kind := range edgeKinds {
		want := relCountByKind(t, h.ctx, h.oracle, kind)
		requireMarkerDelta(t, h.buf, builderServedMarker, 1, "edge count of "+kind.String(),
			func() int64 { return relCountByKind(t, h.ctx, h.bt, kind) }, want)
	}
}

// requireWholeGraph requires every node and every edge to read the same
// through both drivers.
func (h ogtHarness) requireWholeGraph(t *testing.T) {
	t.Helper()
	requireServedNodeSignaturesEqual(t, h.ctx, h.buf, h.bt, h.oracle, `MATCH (n) RETURN n`, "every node")
	h.requireRead(t, `MATCH p = ()-[]->() RETURN p`)
}

// ogtReads is what a user of the uploaded data runs from the Cypher box
// or the UI: label and property scans over every OpenGraph value type,
// objectid lookups, multi-kind and stub nodes, variable-length and
// shortest paths over custom edge kinds (both allShortestPaths answers),
// hybrid AD-to-OpenGraph paths, and aggregates.
var ogtReads = []string{
	`MATCH (n:OGTUser) RETURN n`,
	`MATCH (n:OGTRepository) WHERE n.visibility = 'private' RETURN n`,
	`MATCH (n:OGTRepository) WHERE n.stars > 10 AND n.archived = false RETURN n`,
	`MATCH (n:OGTRepository) WHERE 'infra' IN n.topics RETURN n`,
	`MATCH (n:OGTRepository) WHERE n.score >= 2.5 RETURN n.name, n.score`,
	`MATCH (n) WHERE n.objectid = 'OGT_USER_4' RETURN n LIMIT 1`,
	`MATCH (n:OGTOrgOwner) RETURN n`,
	`MATCH (n) WHERE n.objectid = 'OGT_USER_9' RETURN n`,
	`MATCH (n) WHERE n.objectid = '` + ogtADUser + `' RETURN n`,
	`MATCH p = (u:OGTUser)-[:OGTMemberOf*1..]->(t:OGTTeam) WHERE u.objectid = 'OGT_USER_2' RETURN p`,
	`MATCH p = (:OGTRepoRole)-[r:OGTCanWrite]->(:OGTRepository) RETURN p`,
	`MATCH p = shortestPath((u:OGTUser)-[:OGTMemberOf|OGTHasRole|OGTCanWrite|OGTCanAdmin*1..]->(r:OGTRepository)) WHERE u.objectid = 'OGT_USER_2' AND r.objectid = 'OGT_REPO_1' RETURN p`,
	`MATCH p = allShortestPaths((u:OGTUser)-[:OGTMemberOf|OGTHasRole|OGTCanWrite|OGTCanAdmin*1..]->(r:OGTRepository)) WHERE r.objectid = 'OGT_REPO_1' RETURN p`,
	`MATCH p = allShortestPaths((u:OGTUser)-[:OGTMemberOf|OGTHasRole|OGTCanWrite|OGTCanAdmin*1..]->(r:OGTRepository)) WHERE u.objectid IN ['OGT_USER_1', 'OGT_USER_2', 'OGT_USER_3'] AND r.objectid = 'OGT_REPO_1' RETURN p`,
	`MATCH p = shortestPath((u:User)-[:MemberOf|OGTSyncedTo|OGTMemberOf|OGTHasRole|OGTCanWrite|OGTCanAdmin*1..]->(r:OGTRepository)) WHERE u.objectid = '` + ogtADUser + `' AND r.objectid = 'OGT_REPO_1' RETURN p`,
	`MATCH (n:OGTBase) RETURN count(n) AS nodes`,
	`MATCH ()-[r:OGTMemberOf]->() RETURN count(r) AS edges`,
	`MATCH (u:OGTUser)-[:OGTMemberOf]->(t:OGTTeam) RETURN t.name AS team, count(u) AS members`,
	`MATCH (n:OGTThing) RETURN n`,
}

// TestOpenGraphParity is this file's replay; see the package doc above.
func TestOpenGraphParity(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	buf := installLogCapture(t)
	ctx := context.Background()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("captured log:\n%s", buf.String())
		}
	})

	// Wiped through the raw driver before the replica exists, as
	// TestWriteThroughDifferential does.
	oracle, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, oracle)

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	t.Cleanup(func() { _ = bt.Close(ctx) })
	d, ok := bt.(*bloodtrail.Driver)
	if !ok {
		t.Fatalf("expected *bloodtrail.Driver, got %T", bt)
	}

	// BloodHound's graph schema indexes objectid, which UpdateNodeBy's
	// upsert needs.
	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{
		Name:            graphtest.GraphName,
		NodeConstraints: []graph.Constraint{{Field: "objectid", Type: graph.BTreeIndex}},
	}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	waitForBootLoad(t, d)

	rebuilds := bloodtrail.TestingEngine(d).RebuildCount()
	fallbacks := markerCount(buf, fallbackEnteredMarker)
	h := ogtHarness{ctx: ctx, bt: bt, d: d, oracle: oracle, pool: pool, buf: buf}

	// Named per run: the `kind` table is never truncated, and a name an
	// earlier run registered would be known to the boot load already.
	failedSourceKind := graph.StringKind(fmt.Sprintf("OGTFailedBase%d", time.Now().UnixNano()))

	// What a BloodHound database holds before any OpenGraph upload: its
	// migration-data node, and an AD user in a group from a collector
	// upload (graphify.IngestNodes/IngestRelationships with ad.Entity).
	t.Run("ADBaseline", func(t *testing.T) {
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			_, err := tx.CreateNode(graph.AsProperties(map[string]any{"Major": 9, "Minor": 6, "Patch": 0}), migrationDataKind)
			return err
		}); err != nil {
			t.Fatalf("create migration data: %v", err)
		}
		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			if err := batch.UpdateNodeBy(ogtNode(adEntity, ogtADUser, map[string]any{"name": "esid@testlab.local", "enabled": true}, adUser)); err != nil {
				return err
			}
			if err := batch.UpdateNodeBy(ogtNode(adEntity, "S-1-5-21-1000000000-2000000000-3000000000-512", map[string]any{"name": "domain admins@testlab.local"}, adGroup)); err != nil {
				return err
			}
			return batch.UpdateRelationshipBy(ogtEdge(adEntity,
				ogtEndpoint{ogtADUser, adUser}, adMemberOf, ogtEndpoint{"S-1-5-21-1000000000-2000000000-3000000000-512", adGroup}, nil))
		}); err != nil {
			t.Fatalf("AD baseline batch: %v", err)
		}
		h.requireWholeGraph(t)
	})

	// An upload with metadata.source_kind: nodes of every OpenGraph value
	// type (string, integer, float, boolean, arrays), a node with two kinds
	// besides its source kind, id-matched edges with properties, an edge
	// whose endpoint does not exist yet (stub), and an edge from the AD
	// user, which gains the source kind.
	t.Run("UploadWithSourceKind", func(t *testing.T) {
		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			h.registerKinds(t, ogtSourceKind)
			h.registerKinds(t, ogtUser, ogtTeam, ogtRepo, ogtRole, ogtOwner)

			for _, node := range []graph.NodeUpdate{
				ogtNode(ogtSourceKind, "OGT_ORG_1", map[string]any{"name": "acme"}, graph.StringKind("OGTOrganization")),
				ogtNode(ogtSourceKind, "OGT_USER_1", map[string]any{"name": "alice", "admin": false, "logins": 12}, ogtUser),
				ogtNode(ogtSourceKind, "OGT_USER_2", map[string]any{"name": "bob", "admin": false, "logins": 3}, ogtUser),
				ogtNode(ogtSourceKind, "OGT_USER_3", map[string]any{"name": "carol", "admin": true, "logins": 40}, ogtUser),
				ogtNode(ogtSourceKind, "OGT_USER_4", map[string]any{"name": "dave", "admin": true, "emails": []string{"dave@acme.test", "d@acme.test"}}, ogtUser, ogtOwner),
				ogtNode(ogtSourceKind, "OGT_TEAM_1", map[string]any{"name": "platform"}, ogtTeam),
				ogtNode(ogtSourceKind, "OGT_TEAM_2", map[string]any{"name": "security"}, ogtTeam),
				ogtNode(ogtSourceKind, "OGT_REPO_1", map[string]any{"name": "infra", "full_name": "acme/infra", "visibility": "private", "stars": 42, "archived": false, "score": 3.5, "topics": []string{"infra", "terraform"}}, ogtRepo),
				ogtNode(ogtSourceKind, "OGT_REPO_2", map[string]any{"name": "website", "full_name": "acme/website", "visibility": "public", "stars": 7, "archived": true, "score": 1.25, "topics": []string{"web"}}, ogtRepo),
				ogtNode(ogtSourceKind, "OGT_ROLE_1", map[string]any{"name": "infra-admin"}, ogtRole),
				ogtNode(ogtSourceKind, "OGT_ROLE_2", map[string]any{"name": "infra-write"}, ogtRole),
				ogtNode(ogtSourceKind, "OGT_ROLE_3", map[string]any{"name": "website-read"}, ogtRole),
			} {
				if err := batch.UpdateNodeBy(node); err != nil {
					return err
				}
			}

			id := func(objectID string, kind graph.Kind) ogtEndpoint { return ogtEndpoint{objectID, kind} }
			for _, edge := range []graph.RelationshipUpdate{
				ogtEdge(ogtSourceKind, id("OGT_USER_1", ogtUser), ogtMemberOf, id("OGT_TEAM_1", ogtTeam), map[string]any{"role": "member"}),
				ogtEdge(ogtSourceKind, id("OGT_USER_2", ogtUser), ogtMemberOf, id("OGT_TEAM_2", ogtTeam), map[string]any{"role": "maintainer"}),
				ogtEdge(ogtSourceKind, id("OGT_TEAM_2", ogtTeam), ogtMemberOf, id("OGT_TEAM_1", ogtTeam), nil),
				ogtEdge(ogtSourceKind, id("OGT_TEAM_1", ogtTeam), ogtHasRole, id("OGT_ROLE_2", ogtRole), map[string]any{"assigned_by": "dave"}),
				ogtEdge(ogtSourceKind, id("OGT_ROLE_1", ogtRole), ogtCanAdmin, id("OGT_REPO_1", ogtRepo), map[string]any{"permission": "admin"}),
				ogtEdge(ogtSourceKind, id("OGT_ROLE_3", ogtRole), ogtCanRead, id("OGT_REPO_2", ogtRepo), map[string]any{"permission": "read"}),
				ogtEdge(ogtSourceKind, id("OGT_USER_4", ogtUser), ogtLinks, id("OGT_REPO_2", nil), nil),
				ogtEdge(ogtSourceKind, id("OGT_USER_9", ogtUser), ogtMemberOf, id("OGT_TEAM_2", ogtTeam), map[string]any{"role": "member"}),
				ogtEdge(ogtSourceKind, id(ogtADUser, adUser), ogtSyncedTo, id("OGT_USER_2", ogtUser), map[string]any{"via": "saml"}),
			} {
				if err := batch.UpdateRelationshipBy(edge); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("upload batch: %v", err)
		}

		h.requireWholeGraph(t)
		h.requireCounts(t, graph.Kinds{ogtSourceKind, ogtUser, ogtRepo, adUser}, graph.Kinds{ogtMemberOf, ogtSyncedTo})
	})

	// A second upload whose edges name their endpoints by name or by
	// property, resolved to objectids first; one resolves to nothing and is
	// dropped with a warning, as BloodHound does.
	t.Run("UploadResolvingEndpoints", func(t *testing.T) {
		carol, err := h.resolveBoth(t, ogtUser, "name", "carol", true)
		if err != nil {
			t.Fatalf("resolve carol: %v", err)
		}
		infraAdmin, err := h.resolveBoth(t, ogtRole, "name", "Infra-Admin", true)
		if err != nil {
			t.Fatalf("resolve infra-admin: %v", err)
		}
		infra, err := h.resolveBoth(t, ogtRepo, "full_name", "acme/infra", false)
		if err != nil {
			t.Fatalf("resolve acme/infra: %v", err)
		}
		if _, err := h.resolveBoth(t, ogtUser, "name", "nobody", true); !errors.Is(err, graph.ErrNoResultsFound) {
			t.Fatalf("resolve nobody: err = %v, want graph.ErrNoResultsFound", err)
		}

		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			h.registerKinds(t, ogtSourceKind)
			if err := batch.UpdateRelationshipBy(ogtEdge(ogtSourceKind,
				ogtEndpoint{carol, ogtUser}, ogtHasRole, ogtEndpoint{infraAdmin, ogtRole}, nil)); err != nil {
				return err
			}
			return batch.UpdateRelationshipBy(ogtEdge(ogtSourceKind,
				ogtEndpoint{"OGT_ROLE_2", nil}, ogtCanWrite, ogtEndpoint{infra, ogtRepo}, map[string]any{"permission": "write"}))
		}); err != nil {
			t.Fatalf("second upload batch: %v", err)
		}
		h.requireWholeGraph(t)
	})

	// An upload without metadata: no source kind, so its nodes are
	// "sourceless" data and carry only their declared kind.
	t.Run("UploadWithoutSourceKind", func(t *testing.T) {
		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			h.registerKinds(t, ogtThing)
			for _, objectID := range []string{"OGT_THING_1", "OGT_THING_2"} {
				if err := batch.UpdateNodeBy(ogtNode(graph.EmptyKind, objectID, map[string]any{"name": strings.ToLower(objectID)}, ogtThing)); err != nil {
					return err
				}
			}
			return batch.UpdateRelationshipBy(ogtEdge(graph.EmptyKind,
				ogtEndpoint{"OGT_THING_1", ogtThing}, ogtLinks, ogtEndpoint{"OGT_THING_2", ogtThing}, nil))
		}); err != nil {
			t.Fatalf("sourceless upload batch: %v", err)
		}
		h.requireWholeGraph(t)
	})

	// An upload that fails validation (a node with four kinds) after
	// registering its source kind: the kind exists, no row carries it. The
	// replica learns kinds from rows, so a query naming it goes to
	// PostgreSQL until something teaches the replica the kind (the
	// sourceless delete below does).
	t.Run("FailedUploadRegistersItsSourceKind", func(t *testing.T) {
		if err := bt.BatchOperation(ctx, func(graph.Batch) error {
			h.registerKinds(t, failedSourceKind)
			return nil
		}); err != nil {
			t.Fatalf("failed upload batch: %v", err)
		}
		h.requireDelegatedRead(t, fmt.Sprintf(`MATCH (n:%s) RETURN n`, failedSourceKind), "unsupported")
	})

	t.Run("Reads", func(t *testing.T) {
		for _, text := range ogtReads {
			h.requireRead(t, text)
		}
		// bob reaches infra through two teams (4 hops), the AD user through
		// bob (5), and OGTCanRead is not traversable.
		h.requirePathfind(t, "OGT_USER_2", "OGT_REPO_1", 1)
		h.requirePathfind(t, ogtADUser, "OGT_REPO_1", 1)
		h.requirePathfind(t, "OGT_ROLE_3", "OGT_REPO_2", 0)
		h.requireCounts(t, graph.Kinds{ogtSourceKind, ogtUser, ogtTeam, ogtRepo, ogtRole, ogtOwner, ogtThing},
			graph.Kinds{ogtMemberOf, ogtHasRole, ogtCanWrite, ogtCanAdmin, ogtCanRead, ogtSyncedTo, ogtLinks})
	})

	// "Clear database" for one edge kind (deleteRelationships).
	t.Run("DeleteEdgeKind", func(t *testing.T) {
		if err := d.DeleteRelationshipsByKinds(ctx, graph.Kinds{ogtCanRead}); err != nil {
			t.Fatalf("DeleteRelationshipsByKinds: %v", err)
		}
		h.requireWholeGraph(t)
		h.requireCounts(t, nil, graph.Kinds{ogtCanRead, ogtCanWrite})
	})

	// "Clear database" for sourceless data: every node carrying none of the
	// registered source kinds goes, the failed upload's row-less one
	// included, and migration data stays.
	t.Run("DeleteSourcelessData", func(t *testing.T) {
		if err := d.DeleteNodesByKinds(ctx, nil, graph.Kinds{migrationDataKind, adEntity, azEntity, ogtSourceKind, failedSourceKind}); err != nil {
			t.Fatalf("DeleteNodesByKinds: %v", err)
		}
		if n := nodeCountByKind(t, ctx, oracle, ogtThing); n != 0 {
			t.Fatalf("test fixture assumption violated: postgresql still holds %d OGTThing nodes", n)
		}
		h.requireWholeGraph(t)
		h.requireCounts(t, graph.Kinds{ogtThing, ogtUser, migrationDataKind}, graph.Kinds{ogtLinks})
		h.requireRead(t, fmt.Sprintf(`MATCH (n:%s) RETURN n`, failedSourceKind))
	})

	// "Clear database" for the upload's source kind: its nodes go, the AD
	// user that gained the source kind with them, and their edges.
	t.Run("DeleteSourceKind", func(t *testing.T) {
		if err := d.DeleteNodesByKinds(ctx, graph.Kinds{ogtSourceKind}, graph.Kinds{migrationDataKind}); err != nil {
			t.Fatalf("DeleteNodesByKinds: %v", err)
		}
		if n := nodeCountByKind(t, ctx, oracle, ogtSourceKind); n != 0 {
			t.Fatalf("test fixture assumption violated: postgresql still holds %d OGTBase nodes", n)
		}
		h.requireWholeGraph(t)
		for _, text := range ogtReads {
			h.requireRead(t, text)
		}
		h.requireCounts(t, graph.Kinds{ogtSourceKind, adUser, adGroup, migrationDataKind}, graph.Kinds{adMemberOf, ogtMemberOf})
	})

	assertRebuildCountUnchanged(t, d, rebuilds, t.Name())
	assertNoNewFallback(t, buf, fallbacks, t.Name())
}
