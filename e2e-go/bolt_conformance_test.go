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
// ArcadeDB negotiates Bolt 4.4/4.0/3.0 at most (see PROTO-002 - it never
// advertises 5.x). This suite depends on the pinned neo4j-go-driver continuing
// to silently downgrade to 4.4; a future driver major that drops legacy
// negotiation would break the suite, not just PROTO-002.
package e2e_go

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
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
// (macOS/Windows) container IPs are not host-routable, so this scenario can
// only be fully verified in CI; bolt:// scenarios are unaffected because they
// use the mapped host port.
func Test_CONN_003_Neo4jRoutingSingleNode(t *testing.T) {
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
