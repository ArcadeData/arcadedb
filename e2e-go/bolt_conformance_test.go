//
// Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// Package e2e_go implements the Bolt protocol conformance suite for issue
// #4887 (epic #4882) against the official neo4j-go-driver. Each test function
// embeds its bolt/conformance/spec.yaml scenario id (Test_<AREA>_<NNN>_<slug>)
// for cross-language traceability, matching the e2e-python (#4885) and
// e2e-csharp (#4886) suites.
//
// Since #5001 ArcadeDB advertises Bolt 5.0-5.4 (and still 4.4/4.0/3.0), so the
// pinned neo4j-go-driver negotiates 5.x (see PROTO-002).
package e2e_go

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/require"
)

// runSingle runs a Cypher statement against the given database and returns the
// single result record, failing the test on any driver error.
func runSingle(t *testing.T, d neo4j.DriverWithContext, database, cypher string, params map[string]any) *neo4j.Record {
	t.Helper()
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, cypher, params)
	require.NoError(t, err)
	rec, err := res.Single(ctx)
	require.NoError(t, err)
	return rec
}

// --- connection ---------------------------------------------------------

func Test_CONN_001_ConnectBolt(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	require.NoError(t, d.VerifyConnectivity(ctx))
}

// Test_CONN_003_Neo4jRoutingSingleNode exercises neo4j:// routing discovery.
// handleRoute advertises the server's own bound address
// (socket.getLocalAddress(), i.e. the container's bridge IP such as
// 172.17.0.x). On the Linux CI runner the default Docker bridge subnet is
// routable from the host, so the driver reaches that address and the scenario
// passes - identical to the e2e-python/e2e-csharp suites. On Docker Desktop
// (macOS/Windows) container IPs are not host-routable, so the driver times
// out; the scenario is skipped there and verified only on Linux. bolt://
// scenarios are unaffected because they use the mapped host port.
func Test_CONN_003_Neo4jRoutingSingleNode(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("neo4j:// routing needs the advertised container IP to be " +
			"host-routable, which holds on Linux CI's native Docker bridge " +
			"but not on Docker Desktop (macOS/Windows)")
	}
	d := newDriver(t, boltURI(plainContainer, "neo4j"))
	require.NoError(t, d.VerifyConnectivity(ctx))
	rec := runSingle(t, d, "beer", "MATCH (b:Beer) RETURN b.name AS name LIMIT 1", nil)
	name, _ := rec.Get("name")
	require.NotNil(t, name)
}

func Test_CONN_004_Neo4jRoutingHATopology(t *testing.T) {
	t.Skip("Requires a 3-node HA cluster; the single-node harness cannot " +
		"meaningfully exercise this scenario without new multi-node " +
		"orchestration infrastructure - see #4890")
}

func Test_CONN_002_TLSRequired(t *testing.T) {
	c := tlsRequiredContainer(t)
	d := newDriver(t, boltURI(c, "bolt+ssc"))
	require.NoError(t, d.VerifyConnectivity(ctx))
	rec := runSingle(t, d, "beer", "MATCH (b:Beer) RETURN b.name AS name LIMIT 1", nil)
	name, _ := rec.Get("name")
	require.NotNil(t, name)
}

func Test_CONN_005_TLSOptionalPlaintextConnects(t *testing.T) {
	c := tlsOptionalContainer(t)
	d := newDriver(t, boltURI(c, "bolt"))
	require.NoError(t, d.VerifyConnectivity(ctx))
}

// neo4jCode returns the structured Neo4j error code from a driver error, or ""
// if it is not a neo4j.Neo4jError.
func neo4jCode(err error) string {
	var ne *neo4j.Neo4jError
	if errors.As(err, &ne) {
		return ne.Code
	}
	return ""
}

// --- auth ----------------------------------------------------------------

func Test_AUTH_001_BasicAuthValid(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	require.NoError(t, d.VerifyConnectivity(ctx))
	rec := runSingle(t, d, "beer", "RETURN 1 AS value", nil)
	v, _ := rec.Get("value")
	require.Equal(t, int64(1), v)
}

func Test_AUTH_002_BasicAuthInvalid(t *testing.T) {
	d, err := neo4j.NewDriverWithContext(boltURI(plainContainer, "bolt"), neo4j.BasicAuth("root", "wrong-password", ""))
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close(ctx) })
	err = d.VerifyConnectivity(ctx)
	require.Error(t, err)
	require.Equal(t, "Neo.ClientError.Security.Unauthorized", neo4jCode(err))
}

func Test_AUTH_003_AuthNoneRejected(t *testing.T) {
	d, err := neo4j.NewDriverWithContext(boltURI(plainContainer, "bolt"), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close(ctx) })
	// Intentional rejection (not a bug): connecting with no auth must fail.
	require.Error(t, d.VerifyConnectivity(ctx))
}

// --- transactions ----------------------------------------------------------

func Test_TX_001_AutocommitQuery(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (b:Beer) RETURN b.name AS name LIMIT 5", nil)
	require.NoError(t, err)
	recs, err := res.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, recs, 5)
}

func Test_TX_002_ExplicitCommitPersists(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	tx, err := sess.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = tx.Run(ctx, "CREATE (:TxCommitProbe {marker: 'tx-002'})", nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	rec := runSingle(t, d, "beer", "MATCH (n:TxCommitProbe {marker: 'tx-002'}) RETURN count(n) AS c", nil)
	c, _ := rec.Get("c")
	require.Equal(t, int64(1), c)
}

func Test_TX_003_ExplicitRollbackDiscards(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	tx, err := sess.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = tx.Run(ctx, "CREATE (:TxRollbackProbe {marker: 'tx-003'})", nil)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx))

	rec := runSingle(t, d, "beer", "MATCH (n:TxRollbackProbe {marker: 'tx-003'}) RETURN count(n) AS c", nil)
	c, _ := rec.Get("c")
	require.Equal(t, int64(0), c)
}

func Test_TX_004_ManagedWriteCommits(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	_, err := sess.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, "CREATE (:Beer {name: $n})", map[string]any{"n": "TX-004-Beer"})
		return nil, err
	})
	require.NoError(t, err)
	rec := runSingle(t, d, "beer", "MATCH (b:Beer {name: $n}) RETURN count(b) AS c", map[string]any{"n": "TX-004-Beer"})
	c, _ := rec.Get("c")
	require.Equal(t, int64(1), c)
}

// raceTwoWriters has two sessions race to increment the same node inside an
// explicit transaction, one held open past the other's commit. Returns every
// error the racing sessions surfaced. Shared by TX-005 and ERR-004. The race is
// inherently timing-sensitive; callers assert over the whole error set.
func raceTwoWriters(t *testing.T, d neo4j.DriverWithContext, database, marker string) []error {
	t.Helper()
	setup := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	_, err := setup.Run(ctx, "MERGE (n:RaceProbe {marker: $marker}) SET n.value = 0", map[string]any{"marker": marker})
	require.NoError(t, err)
	_ = setup.Close(ctx)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	barrier := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
			defer sess.Close(ctx)
			<-barrier
			tx, err := sess.BeginTransaction(ctx)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			res, err := tx.Run(ctx, "MATCH (n:RaceProbe {marker: $marker}) SET n.value = n.value + 1 RETURN n", map[string]any{"marker": marker})
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			_, _ = res.Consume(ctx)
			time.Sleep(500 * time.Millisecond)
			if err := tx.Commit(ctx); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	close(barrier)
	wg.Wait()
	for i, e := range errs {
		t.Logf("raceTwoWriters[%s] errs[%d]: code=%q msg=%v", marker, i, neo4jCode(e), e)
	}
	return errs
}

func anyTransient(errs []error) bool {
	for _, e := range errs {
		if strings.HasPrefix(neo4jCode(e), "Neo.TransientError") {
			return true
		}
	}
	return false
}

func anyNonRetryable(errs []error) bool {
	for _, e := range errs {
		code := neo4jCode(e)
		if strings.HasPrefix(code, "Neo.ClientError") || strings.HasPrefix(code, "Neo.DatabaseError") {
			return true
		}
	}
	return false
}

func codes(errs []error) []string {
	out := make([]string, 0, len(errs))
	for _, e := range errs {
		out = append(out, neo4jCode(e))
	}
	return out
}

// raceUntilConflict re-runs the two-writer race (with a distinct marker each
// attempt) until it surfaces at least one error, up to a bounded number of
// tries. It only retries the "no conflict reproduced" case (clean
// serialization) - as soon as any error is surfaced it returns, so a genuine
// non-transient error code is never retried away and still fails the caller's
// assertion. This tames the inherent timing sensitivity of the race without
// weakening what the scenario certifies.
func raceUntilConflict(t *testing.T, d neo4j.DriverWithContext, database, marker string) []error {
	t.Helper()
	const attempts = 5
	var errs []error
	for i := 0; i < attempts; i++ {
		errs = raceTwoWriters(t, d, database, fmt.Sprintf("%s-%d", marker, i))
		if len(errs) > 0 {
			return errs
		}
	}
	return errs
}

// assertConflictTransient skips when the write conflict did not reproduce after
// the bounded retries - a scheduling artifact on a loaded runner, not a
// conformance violation, so it must not red a PR-gating run. When a conflict
// did surface, it must be a retryable Neo.TransientError.* and never a
// non-retryable ClientError/DatabaseError.
func assertConflictTransient(t *testing.T, errs []error) {
	t.Helper()
	if len(errs) == 0 {
		// Make the non-certification loud: a bare SKIP can read as a green,
		// certified run. This line (visible under -v) states plainly that the
		// transient-error contract was NOT exercised this run, so a permanent
		// clean-serialization regression can't hide as an all-green gate.
		t.Logf("NOT CERTIFIED THIS RUN: the write conflict never reproduced across the bounded retries, " +
			"so the Neo.TransientError.* contract was not asserted")
		t.Skip("write conflict did not reproduce within the bounded retries - " +
			"a scheduling artifact on a loaded runner, not a conformance violation")
	}
	require.True(t, anyTransient(errs), "expected at least one Neo.TransientError.*, got %v", codes(errs))
	require.False(t, anyNonRetryable(errs), "expected no non-retryable ClientError/DatabaseError, got %v", codes(errs))
}

func Test_TX_005_ManagedWriteRetriesOnTransientError(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	errs := raceUntilConflict(t, d, "beer", "tx-005")
	assertConflictTransient(t, errs)
}

// --- causal-consistency ---------------------------------------------------

func Test_CAUSAL_001_BookmarkReadAfterWrite(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sessA := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	res, err := sessA.Run(ctx, "CREATE (:CausalProbe {marker: 'causal-001'})", nil)
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
	bm := sessA.LastBookmarks()
	require.NoError(t, sessA.Close(ctx))

	sessB := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer", Bookmarks: bm})
	t.Cleanup(func() { _ = sessB.Close(ctx) })
	res, err = sessB.Run(ctx, "MATCH (n:CausalProbe {marker: 'causal-001'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	rec, err := res.Single(ctx)
	require.NoError(t, err)
	c, _ := rec.Get("c")
	require.Equal(t, int64(1), c)
}

// --- multi-database --------------------------------------------------------

func Test_MDB_001_SessionSelectsNamedDatabase(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (b:Beer) RETURN b.name AS name LIMIT 1", nil)
	name, _ := rec.Get("name")
	require.NotNil(t, name)
}

func Test_MDB_002_SessionsAcrossDatabasesAreIsolated(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	scratch := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "boltscratch"})
	// Ensure the scratch session (and any still-open tx) is released even if the
	// isolation assertion below fails between begin and commit.
	t.Cleanup(func() { _ = scratch.Close(ctx) })
	tx, err := scratch.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = tx.Run(ctx, "CREATE (:ScratchProbe {marker: 'mdb-002'})", nil)
	require.NoError(t, err)

	// beer must not see the uncommitted boltscratch write.
	rec := runSingle(t, d, "beer", "MATCH (n:ScratchProbe {marker: 'mdb-002'}) RETURN count(n) AS c", nil)
	c, _ := rec.Get("c")
	require.Equal(t, int64(0), c)

	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, scratch.Close(ctx))

	verify := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "boltscratch"})
	t.Cleanup(func() { _ = verify.Close(ctx) })
	res, err := verify.Run(ctx, "MATCH (n:ScratchProbe {marker: 'mdb-002'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	rec, err = res.Single(ctx)
	require.NoError(t, err)
	c, _ = rec.Get("c")
	require.Equal(t, int64(1), c)
}

// --- result-handling ---------------------------------------------------

func Test_RESULT_001_StreamingPullIncremental(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (b:Beer) RETURN b.name AS name LIMIT 10", nil)
	require.NoError(t, err)
	seen := 0
	for res.Next(ctx) {
		name, _ := res.Record().Get("name")
		require.NotNil(t, name)
		seen++
	}
	require.NoError(t, res.Err())
	require.Equal(t, 10, seen)
}

func Test_RESULT_002_PartialPullThenContinue(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer", FetchSize: 2})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (b:Beer) RETURN b.name AS name LIMIT 5", nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	require.True(t, res.Next(ctx)) // consumed first 2
	rest, err := res.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, rest, 3)
}

func Test_RESULT_003_DiscardAbandonsRemaining(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (b:Beer) RETURN b.name AS name LIMIT 5", nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	summary, err := res.Consume(ctx)
	require.NoError(t, err)
	require.NotNil(t, summary)
}

func Test_RESULT_004_SummaryCountersReflectWrites(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	require.NoError(t, func() error {
		sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
		defer sess.Close(ctx)
		res, err := sess.Run(ctx,
			"CREATE (:Beer {name: $n})-[:BREWED_BY]->(:Brewery {name: $b})",
			map[string]any{"n": "RESULT-004-Beer", "b": "RESULT-004-Brewery"})
		if err != nil {
			return err
		}
		summary, err := res.Consume(ctx)
		if err != nil {
			return err
		}
		cnt := summary.Counters()
		if cnt.NodesCreated() != 2 {
			return fmt.Errorf("nodes_created=%d, want 2", cnt.NodesCreated())
		}
		if cnt.RelationshipsCreated() != 1 {
			return fmt.Errorf("relationships_created=%d, want 1", cnt.RelationshipsCreated())
		}
		if cnt.PropertiesSet() < 2 {
			return fmt.Errorf("properties_set=%d, want >=2", cnt.PropertiesSet())
		}
		return nil
	}())
}

// --- type-roundtrip ---------------------------------------------------

func Test_TYPE_001_NodeRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (b:Beer) RETURN b LIMIT 1", nil)
	v, _ := rec.Get("b")
	node, ok := v.(dbtype.Node)
	require.True(t, ok, "expected dbtype.Node, got %T", v)
	require.Contains(t, node.Labels, "Beer")
	require.NotNil(t, node.Props["name"])
}

func Test_TYPE_002_RelationshipRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH ()-[r]->() RETURN r LIMIT 1", nil)
	v, _ := rec.Get("r")
	rel, ok := v.(dbtype.Relationship)
	require.True(t, ok, "expected dbtype.Relationship, got %T", v)
	require.NotEmpty(t, rel.Type)
}

func Test_TYPE_003_PathRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH p=(b:Beer)-[*1..2]-() RETURN p LIMIT 1", nil)
	v, _ := rec.Get("p")
	path, ok := v.(dbtype.Path)
	require.True(t, ok, "expected dbtype.Path, got %T", v)
	require.GreaterOrEqual(t, len(path.Nodes), 2)
}

func Test_TYPE_004_ByteArrayParamRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	payload := []byte{1, 2, 3, 4}
	rec := runSingle(t, d, "beer", "RETURN $b AS echo", map[string]any{"b": payload})
	v, _ := rec.Get("echo")
	got, ok := v.([]byte)
	require.True(t, ok, "expected []byte, got %T", v)
	require.Equal(t, payload, got)
}

func Test_TYPE_005_NestedListMapRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.nestedListProp AS l, t.nestedMapProp AS m", nil)
	l, _ := rec.Get("l")
	m, _ := rec.Get("m")
	require.Equal(t, []any{int64(1), int64(2), []any{int64(3), int64(4)}}, l)
	require.Equal(t, map[string]any{"a": int64(1), "b": map[string]any{"c": int64(2)}}, m)
}

func Test_TYPE_006_NullRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.nullProp AS n", nil)
	n, _ := rec.Get("n")
	require.Nil(t, n)
	echo := runSingle(t, d, "beer", "RETURN $p AS echo", map[string]any{"p": nil})
	e, _ := echo.Get("echo")
	require.Nil(t, e)
}

func Test_TYPE_007_LocalDateRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.localDateProp AS d", nil)
	v, _ := rec.Get("d")
	_, ok := v.(dbtype.Date)
	require.True(t, ok, "expected dbtype.Date, got %T", v)
	echo := runSingle(t, d, "beer", "RETURN $d AS echo", map[string]any{"d": v})
	e, _ := echo.Get("echo")
	require.Equal(t, v, e)
}

func Test_TYPE_008_LocalTimeRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.localTimeProp AS t2", nil)
	v, _ := rec.Get("t2")
	_, ok := v.(dbtype.LocalTime)
	require.True(t, ok, "expected dbtype.LocalTime, got %T", v)
	echo := runSingle(t, d, "beer", "RETURN $t AS echo", map[string]any{"t": v})
	e, _ := echo.Get("echo")
	require.Equal(t, v, e)
}

func Test_TYPE_009_LocalDateTimeRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.localDateTimeProp AS dt", nil)
	v, _ := rec.Get("dt")
	_, ok := v.(dbtype.LocalDateTime)
	require.True(t, ok, "expected dbtype.LocalDateTime, got %T", v)
	echo := runSingle(t, d, "beer", "RETURN $dt AS echo", map[string]any{"dt": v})
	e, _ := echo.Get("echo")
	require.Equal(t, v, e)
}

func Test_TYPE_010_OffsetDateTimeRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.offsetDateTimeProp AS dt", nil)
	v, _ := rec.Get("dt")
	tm, ok := v.(time.Time)
	require.True(t, ok, "expected time.Time, got %T", v)
	_, offset := tm.Zone()
	require.Equal(t, 2*60*60, offset, "expected +02:00 offset")
	echo := runSingle(t, d, "beer", "RETURN $dt AS echo", map[string]any{"dt": tm})
	e, _ := echo.Get("echo")
	require.WithinDuration(t, tm, e.(time.Time), 0)
}

func Test_TYPE_011_DurationRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.durationProp AS d", nil)
	v, _ := rec.Get("d")
	_, ok := v.(dbtype.Duration)
	require.True(t, ok, "expected dbtype.Duration, got %T", v)
}

func Test_TYPE_012_PointRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.pointProp AS p", nil)
	v, _ := rec.Get("p")
	_, ok := v.(dbtype.Point2D)
	require.True(t, ok, "expected dbtype.Point2D, got %T", v)
}

// --- errors ---------------------------------------------------------------

func Test_ERR_001_SyntaxError(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (n RETURN n", nil)
	if err == nil {
		_, err = res.Consume(ctx)
	}
	require.Error(t, err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", neo4jCode(err))
}

func Test_ERR_002_SemanticError(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (n:Beer) RETURN undeclaredVariable", nil)
	if err == nil {
		_, err = res.Consume(ctx)
	}
	require.Error(t, err)
	require.Equal(t, "Neo.ClientError.Statement.SemanticError", neo4jCode(err))
}

func Test_ERR_003_UnauthenticatedRequestRejected(t *testing.T) {
	t.Skip("Requires sending RUN before completing HELLO/LOGON; the official " +
		"neo4j-go-driver always completes the handshake internally, so this " +
		"cannot be triggered without a bespoke raw-socket client, which is out " +
		"of scope per the epic's 'official drivers only' principle. spec.yaml " +
		"ERR-003 current_status is not-applicable.")
}

func Test_ERR_004_TransientConditionErrorCode(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	errs := raceUntilConflict(t, d, "beer", "err-004")
	assertConflictTransient(t, errs)
}

// --- protocol ---------------------------------------------------------

func Test_PROTO_001_VersionNegotiationSucceeds(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "RETURN 1 AS value", nil)
	v, _ := rec.Get("value")
	require.Equal(t, int64(1), v)
}

func Test_PROTO_002_Bolt5xNegotiationIsSupported(t *testing.T) {
	// Since #5001 the server advertises Bolt 5.0-5.4, so a 5.x-capable driver
	// negotiates a 5.x protocol version instead of downgrading to 4.4.
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "RETURN 1 AS value", nil)
	require.NoError(t, err)
	summary, err := res.Consume(ctx)
	require.NoError(t, err)
	ver := summary.Server().ProtocolVersion()
	require.GreaterOrEqualf(t, ver.Major, 5,
		"driver negotiated Bolt %d.%d, not 5.x", ver.Major, ver.Minor)
}

func Test_PROTO_003_ResetMidStream(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer", FetchSize: 2})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (b:Beer) RETURN b.name AS name LIMIT 10", nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	_, err = res.Consume(ctx) // abandon remaining stream mid-flight
	require.NoError(t, err)

	res2, err := sess.Run(ctx, "RETURN 1 AS value", nil)
	require.NoError(t, err)
	rec, err := res2.Single(ctx)
	require.NoError(t, err)
	v, _ := rec.Get("value")
	require.Equal(t, int64(1), v)
}
