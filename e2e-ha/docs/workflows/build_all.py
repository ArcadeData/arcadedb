#!/usr/bin/env python3
"""Builds one animated SVG per e2e-ha integration test class.

Run:  python3 build_all.py          (rewrites every *.svg in this folder, then index.html)

Each entry below mirrors one test class in
e2e-ha/src/test/java/com/arcadedb/containers/ha/. A class with several @Test methods is drawn from
its primary method, with the siblings listed on a "variants" card so nothing is silently dropped.
"""

import json
import os

from workflow_svg import (AMBER, BLUE, GREEN, GREY, PURPLE, RED, Scene, build, chip, client_to,
                          code_card, cross_stamp, cut, isolated, layout, node_off, node_progress,
                          node_state, raft_link, stamp, toxic, variants_card)

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = "../../src/test/java/com/arcadedb/containers/ha"

DIAGRAMS = []            # (slug, title, subtitle, java_file, methods, blurb, loop_seconds)


# ---------------------------------------------------------------------------- shared scenes
def boot(n, persistent=False, quorum="majority", seed="", leader=0):
    """The four phases every HA scenario starts with: containers, start, election, database + schema."""
    others = [i for i in range(n) if i != leader]
    ctor = "createPersistentArcadeContainer()" if persistent else "createArcadeContainer()"
    return [
        Scene(
            step=f"{ctor.replace('()', '')} x{n}",
            caption=(f"{n} ArcadeDB containers on one Docker network, bind-mounted so data survives a restart."
                     if persistent else
                     f"{n} ArcadeDB containers on one Docker network, all given the same Raft server list."),
            detail=f"SERVER_LIST = arcadedb-0:2434:2480,...   quorum={quorum}"
                   + ("   (persistent: databases + replication bind-mounted)" if persistent else ""),
            dur=3.4,
            body=[code_card(92, [
                "-Darcadedb.ha.enabled=true",
                f"-Darcadedb.ha.quorum={quorum}",
                "-Darcadedb.ha.raft.port=2434",
                "-Darcadedb.ha.serverList=...",
                "-Darcadedb.server.readinessRequiresHA",
            ], title="JAVA_OPTS (every node)", color=AMBER)]
            + [node_state(i, "STARTING", AMBER, "ha.enabled=true") for i in range(n)],
        ),
        Scene(
            step="startCluster()",
            caption="All containers start in parallel; Testcontainers waits for each node's HTTP health probe.",
            detail='waitingFor(Wait.forHttp("/api/v1/health").forPort(2480).forStatusCode(204))',
            dur=3.2,
            body=[node_state(i, "UP", BLUE, "health 204") for i in range(n)]
                 + [client_to(i, "GET /api/v1/health" if i == 0 else "", BLUE, packet=True) for i in range(n)],
        ),
        Scene(
            step="waitForRaftLeader(60s)",
            caption="Every node's cluster endpoint is polled once a second until one of them claims leadership.",
            detail='GET /api/v1/cluster  ->  body contains "isLeader":true   (the run fails after 60s)',
            dur=3.4,
            body=[node_state(leader, "LEADER", GREEN, "isLeader:true")]
                 + [node_state(i, "FOLLOWER", BLUE, "isLeader:false") for i in others]
                 + [client_to(i, "GET /api/v1/cluster" if i == leader else "", BLUE, packet=True) for i in range(n)]
                 + [raft_link(leader, others[0], "election", AMBER)],
        ),
        Scene(
            step="createDatabase() + createSchema()",
            caption=("Database and schema are written through the leader, which Raft carries to every follower."
                     if not seed else seed),
            detail="CREATE VERTEX TYPE User, Photo - EDGE TYPE HasUploaded, FriendOf, Likes - UNIQUE_HASH / FULL_TEXT / GEOSPATIAL",
            dur=3.6,
            body=[code_card(92, [
                "CREATE VERTEX TYPE User / Photo",
                "CREATE EDGE TYPE HasUploaded",
                "CREATE EDGE TYPE FriendOf / Likes",
                "CREATE INDEX ... UNIQUE_HASH",
                "CREATE INDEX ... FULL_TEXT / GEOSPATIAL",
            ], title="playwithpictures", color=PURPLE),
                node_state(leader, "LEADER", GREEN, "schema committed")]
            + [node_state(i, "FOLLOWER", PURPLE, "reopening db") for i in others]
            + [client_to(leader, 'command("sqlscript")', GREEN, packet=True)]
            + [raft_link(leader, i, "schema entry" if i == others[0] else "") for i in others],
        ),
    ]


def emit(slug, title, subtitle, scenes, java, methods, blurb, n=3, proxy=False):
    layout(n, proxy)
    total = build(os.path.join(HERE, slug + ".svg"), title, subtitle, scenes,
                  footer="loop: %.0fs" % sum(s.dur for s in scenes))
    DIAGRAMS.append({"slug": slug, "title": title, "java": java, "methods": methods,
                     "blurb": blurb, "scenes": len(scenes), "loop": round(total)})
    print(f"  {slug}.svg  ({total:.0f}s, {len(scenes)} scenes)")


# ============================================================================ SimpleHaScenarioIT
def simple_ha():
    layout(2)
    s = boot(2)
    s += [
        Scene(
            step="db1/db2.checkSchema()",
            caption="Both nodes are asked for the schema: the follower must report the five types, not just the leader.",
            detail='assertThat(schema.existsType("User" | "Photo" | "HasUploaded" | "FriendOf" | "Likes")).isTrue()',
            dur=3.2,
            body=[node_state(0, "LEADER", GREEN, "5 types"), node_state(1, "FOLLOWER", GREEN, "5 types"),
                  stamp(0, "schema ok"), stamp(1, "schema ok"),
                  client_to(0, "getSchema()", GREEN), client_to(1, "", GREEN)],
        ),
        Scene(
            step="db1.addUserAndPhotos(10, 10)",
            caption="Ten users, each with ten photos, are written to the leader - one locked transaction per vertex.",
            detail="BEGIN; LOCK TYPE User, Photo, HasUploaded; CREATE VERTEX ...; CREATE EDGE HasUploaded; COMMIT RETRY 30;",
            dur=3.8,
            body=[node_state(0, "LEADER", GREEN, "writing"), node_state(1, "FOLLOWER", BLUE, "applying log"),
                  node_progress(0, 1.0, GREEN, "10 users - 100 photos"), node_progress(1, 0.45, BLUE, "catching up"),
                  client_to(0, "110 transactions", GREEN, packet=True), raft_link(0, 1, "replicated writes")],
        ),
        Scene(
            step="Awaitility.await() <= 30s",
            caption="Both nodes are polled once a second until the follower's counts equal the leader's.",
            detail="until(() -> users2 == users1 && photos2 == photos1)   atMost 30s, pollInterval 1s",
            dur=3.4,
            body=[node_state(0, "LEADER", GREEN, "users 10 - photos 100"),
                  node_state(1, "FOLLOWER", GREEN, "users 10 - photos 100"),
                  node_progress(0, 1.0), node_progress(1, 1.0),
                  client_to(0, "SELECT count(*)", BLUE, packet=True), client_to(1, "", BLUE, packet=True),
                  raft_link(0, 1, "converged", GREEN)],
        ),
        Scene(
            step="assert both nodes, then close",
            caption="Converged counts are not enough: both nodes are asserted against the absolute expected totals.",
            detail="db1/db2.assertThatUserCountIs(10) and assertThatPhotoCountIs(100), then close()",
            dur=3.6,
            body=[code_card(92, ["db1.assertThatUserCountIs(10)", "db2.assertThatUserCountIs(10)",
                                 "db1.assertThatPhotoCountIs(100)", "db2.assertThatPhotoCountIs(100)"],
                            title="final assertions", color=GREEN),
                  node_state(0, "PASS", GREEN, "10 users - 100 photos"),
                  node_state(1, "PASS", GREEN, "10 users - 100 photos"),
                  stamp(0, "asserted"), stamp(1, "asserted")],
        ),
    ]
    emit("simple-ha-scenario", "SimpleHaScenarioIT - twoNodeRaftReplication()",
         "Two-node Raft HA: schema and data replication", s,
         "SimpleHaScenarioIT.java", ["twoNodeRaftReplication"],
         "The baseline: two nodes, one leader, schema and data must reach the follower.", n=2)


# ============================================================================ ThreeInstancesScenarioIT
def three_instances():
    layout(3)
    s = boot(3, seed="Database and schema go to the node that holds leadership - node 0 is not necessarily it.")
    s += [
        Scene(
            step="awaitSchema(60s) on all three",
            caption="Each node is polled until it answers the schema query, and the wait is measured rather than slept through.",
            detail="A follower applying a schema entry reopens the database and answers \"Database ... is not available\" meanwhile",
            dur=3.4,
            body=[node_state(i, "READY", GREEN, "schema readable") for i in range(3)]
                 + [stamp(i, "schema ok") for i in range(3)]
                 + [client_to(i, "awaitSchema()" if i == 0 else "", GREEN) for i in range(3)],
        ),
        Scene(
            step="addUserAndPhotos(10,10) from EACH",
            caption="All three nodes write concurrently: two of them are followers, so their commands forward to the leader.",
            detail="db1.addUserAndPhotos(10,10); db2.addUserAndPhotos(10,10); db3.addUserAndPhotos(10,10)",
            dur=3.8,
            body=[node_state(0, "LEADER", GREEN, "writing"),
                  node_state(1, "FOLLOWER", BLUE, "forwarding"), node_state(2, "FOLLOWER", BLUE, "forwarding"),
                  client_to(0, "110 tx", GREEN, packet=True), client_to(1, "110 tx", BLUE, packet=True),
                  client_to(2, "110 tx", BLUE, packet=True),
                  raft_link(1, 0, "forward to leader", AMBER), raft_link(0, 2, "replicate")],
        ),
        Scene(
            step="await 30 users / 300 photos",
            caption="Convergence here is an absolute target, not just equality: every node must reach exactly 30 and 300.",
            detail="until(users1 == 30 && users2 == 30 && users3 == 30 && photos1 == 300 && ...)   atMost 60s",
            dur=3.4,
            body=[node_state(i, "CONVERGED", GREEN, "30 users - 300 photos") for i in range(3)]
                 + [node_progress(i, 1.0) for i in range(3)]
                 + [client_to(i, "SELECT count(*)" if i == 0 else "", BLUE, packet=True) for i in range(3)],
        ),
        Scene(
            step="db2.addUserAndPhotos(100, 10)",
            caption="A second, larger burst from node 1 alone, this time checked only for equality across the three nodes.",
            detail="until(users1 == users2 && users2 == users3 && photos1 == photos2 && photos2 == photos3)   atMost 60s",
            dur=3.8,
            body=[node_state(1, "FOLLOWER", GREEN, "1100 tx"), node_state(0, "LEADER", GREEN, "applying"),
                  node_state(2, "FOLLOWER", BLUE, "applying"),
                  node_progress(0, 1.0), node_progress(1, 1.0), node_progress(2, 0.7, BLUE),
                  client_to(1, "100 users x 10 photos", GREEN, packet=True),
                  raft_link(1, 0, "forward", AMBER), raft_link(0, 2, "replicate")],
        ),
        Scene(
            step="full convergence, then close",
            caption="All three nodes must agree on both counts before the wrappers are closed.",
            detail="db1.close(); db2.close(); db3.close()   -   tearDown skips compareAllDatabases (non-persistent containers)",
            dur=3.4,
            body=[node_state(i, "PASS", GREEN, "130 users - 1300 photos") for i in range(3)]
                 + [stamp(i, "equal") for i in range(3)],
        ),
    ]
    emit("three-instances-scenario", "ThreeInstancesScenarioIT - threeNodeReplication()",
         "Three-node Raft HA: replication across all nodes with consistency check", s,
         "ThreeInstancesScenarioIT.java", ["threeNodeReplication"],
         "Three nodes, writes issued from all of them, absolute counts then equality.")


# ============================================================================ LoadThreeInstancesScenarioIT
def load_three_instances():
    layout(3)
    s = boot(3)
    s += [
        Scene(
            step="awaitSchema(60s) on all three",
            caption="The schema wait is measured on each node before any load starts.",
            detail="db1.awaitSchema(60); db2.awaitSchema(60); db3.awaitSchema(60)   -   logs the elapsed milliseconds",
            dur=3.0,
            body=[node_state(i, "READY", GREEN, "schema readable") for i in range(3)] + [stamp(i, "ok") for i in range(3)],
        ),
        Scene(
            step="500 users x 10 photos, per node",
            caption="Each node takes a 500-user batch: 1500 users and 15000 photos cross the Raft log.",
            detail="db1.addUserAndPhotos(500,10); db2.addUserAndPhotos(500,10); db3.addUserAndPhotos(500,10)",
            dur=3.8,
            body=[node_state(0, "LEADER", GREEN, "5500 tx"), node_state(1, "FOLLOWER", BLUE, "5500 tx"),
                  node_state(2, "FOLLOWER", BLUE, "5500 tx"),
                  client_to(0, "load", GREEN, packet=True), client_to(1, "load", BLUE, packet=True),
                  client_to(2, "load", BLUE, packet=True), raft_link(0, 2, "replicate")],
        ),
        Scene(
            step="await equality across nodes",
            caption="Under this volume the assertion is equality, not a fixed number: the point is that no node lags behind.",
            detail="until(users1 == users2 && users2 == users3 && photos1 == photos2 && photos2 == photos3)   atMost 60s",
            dur=3.4,
            body=[node_state(i, "CONVERGED", GREEN, "1500 users - 15000 photos") for i in range(3)]
                 + [node_progress(i, 1.0) for i in range(3)],
        ),
        Scene(
            step="db2.addUserAndPhotos(500, 10)",
            caption="A further 500-user batch from node 1, then a second convergence window.",
            detail="Awaitility.await().atMost(60, SECONDS).pollInterval(2, SECONDS)",
            dur=3.4,
            body=[node_state(1, "FOLLOWER", GREEN, "+5500 tx"), node_state(0, "LEADER", GREEN, "applying"),
                  node_state(2, "FOLLOWER", BLUE, "applying"), node_progress(2, 0.6, BLUE, "catching up"),
                  client_to(1, "500 users x 10", GREEN, packet=True), raft_link(1, 0, "forward", AMBER),
                  raft_link(0, 2, "replicate")],
        ),
        Scene(
            step="threeNodeReplicationMulti()",
            caption="The sibling method drives the same cluster from a thread pool and adds edge-building load.",
            detail="ExecutorService + addUserAndPhotos(500,10), createFriendships(100), createLike(100); Micrometer meters dumped at the end",
            dur=3.8,
            body=[variants_card([
                "threeNodeReplicationMulti():",
                "  ExecutorService(10), 1 writer thread",
                "  500 users x 10 photos",
                "  createFriendships(100)",
                "  createLike(100)",
                "  counts polled until executor ends",
                "  Metrics.globalRegistry dumped",
            ], title="sibling @Test method"),
                node_state(0, "LEADER", GREEN, "concurrent load"),
                node_state(1, "FOLLOWER", BLUE, "applying"), node_state(2, "FOLLOWER", BLUE, "applying"),
                client_to(0, "pool of writers", GREEN, packet=True)],
        ),
    ]
    emit("load-three-instances-scenario", "LoadThreeInstancesScenarioIT - threeNodeReplication()",
         "Three-node Raft HA under load: 500-user batches per node, then convergence", s,
         "LoadThreeInstancesScenarioIT.java", ["threeNodeReplication", "threeNodeReplicationMulti"],
         "The volume variant: 500-user batches per node, plus a threaded run with friendships and likes.")


# ============================================================================ DropDatabaseScenarioIT
def drop_database():
    layout(3)
    s = boot(3)
    s += [
        Scene(
            step="db0.addUserAndPhotos(30, 10)",
            caption="A fixture of 30 users and 300 photos is written and must reach every node before the drop.",
            detail="until(db0.countUsers() == 30 && db1.countUsers() == 30 && db2.countUsers() == 30)   atMost 120s",
            dur=3.4,
            body=[node_state(i, "CONVERGED", GREEN, "30 users") for i in range(3)]
                 + [node_progress(i, 1.0) for i in range(3)]
                 + [client_to(0, "330 transactions", GREEN, packet=True), raft_link(0, 2, "replicate")],
        ),
        Scene(
            step="close the wrappers FIRST",
            caption="Every RemoteDatabase is closed before the drop: no open connection may point at a database about to vanish.",
            detail="db0.close(); db1.close(); db2.close();   (also closed in the catch block, so a failure cannot leak them)",
            dur=3.0,
            body=[node_state(i, "IDLE", GREY, "no client") for i in range(3)]
                 + [chip(352, 96, "db0.close() / db1.close() / db2.close()", GREY, w=286)],
        ),
        Scene(
            step="drop via node 2 (a replica)",
            caption="The drop is issued on a replica on purpose - that is the forward-to-leader path this test exists to cover.",
            detail='POST /api/v1/server {"command":"drop database playwithpictures"}   ->  assertThat(status).isEqualTo(200)',
            dur=3.8,
            body=[node_state(2, "REPLICA", AMBER, "receives drop"), node_state(0, "LEADER", GREEN, "executes drop"),
                  node_state(1, "FOLLOWER", BLUE, "applying"),
                  client_to(2, "POST drop database", AMBER, packet=True),
                  raft_link(2, 0, "forward to leader", AMBER), raft_link(0, 1, "drop entry")],
        ),
        Scene(
            step="await removal on every node",
            caption="The removal is confirmed from the outside: the database must disappear from every node's database list.",
            detail='until(!databaseExistsOnServer(0) && !databaseExistsOnServer(1) && !databaseExistsOnServer(2))   atMost 60s',
            dur=3.4,
            body=[node_state(i, "DROPPED", GREY, "not listed") for i in range(3)]
                 + [stamp(i, "gone", GREY) for i in range(3)]
                 + [client_to(i, "list databases" if i == 0 else "", BLUE, packet=True) for i in range(3)],
        ),
        Scene(
            step="second drop must be rejected",
            caption="Dropping the same database again has to fail: idempotent success would hide a drop that never happened.",
            detail="assertThat(secondDropStatus).isBetween(400, 499)   -   tearDown skips compareAllDatabases, the DB is gone on purpose",
            dur=3.4,
            body=[node_off(1, "NO DB"), node_off(2, "NO DB"),
                  node_state(0, "LEADER", GREEN, "rejects"), cross_stamp(0, "4xx"),
                  client_to(0, "POST drop database", RED, packet=True)],
        ),
    ]
    emit("drop-database-scenario", "DropDatabaseScenarioIT - dropDatabaseReplicatedAcrossCluster()",
         "Three-node Raft HA: drop database via a replica propagates removal to every peer", s,
         "DropDatabaseScenarioIT.java", ["dropDatabaseReplicatedAcrossCluster"],
         "Drop issued on a replica, so the forward-to-leader path is what is really under test.")


# ============================================================================ ImportDatabaseScenarioIT
def import_database():
    layout(3)
    s = [
        Scene(
            step="stage the fixture on EVERY node",
            caption="The importer reads the archive from the local disk of whichever node leads - and that is not deterministic.",
            detail='withCopyToContainer("raft-import-fixture.jsonl.tgz" -> /home/arcadedb/import-fixture.jsonl.tgz) on all three',
            dur=3.8,
            body=[code_card(92, [
                "leadership is non-deterministic, so",
                "the fixture must exist on all peers:",
                "  /home/arcadedb/",
                "    import-fixture.jsonl.tgz",
            ], title="why on every node", color=AMBER),
                node_state(0, "STAGED", AMBER, "fixture.tgz"), node_state(1, "STAGED", AMBER, "fixture.tgz"),
                node_state(2, "STAGED", AMBER, "fixture.tgz")],
        ),
    ] + boot(3)[1:3] + [
        Scene(
            step="import database, on the leader",
            caption="Import is a leader-only write, so it is sent to the elected leader instead of relying on HTTP forwarding.",
            detail='POST /api/v1/server {"command":"import database RaftImportTest file:///home/arcadedb/import-fixture.jsonl.tgz"}   read timeout 180s',
            dur=3.8,
            body=[node_state(0, "LEADER", GREEN, "importing"), node_state(1, "FOLLOWER", PURPLE, "TX_ENTRY"),
                  node_state(2, "FOLLOWER", PURPLE, "TX_ENTRY"),
                  client_to(0, "import database", GREEN, packet=True),
                  raft_link(0, 1, "importer tx replicate"), raft_link(1, 2, "")],
        ),
        Scene(
            step="await the DB on every node",
            caption="The imported database must appear in each node's database list before any query is attempted.",
            detail="until(databaseExistsOnServer(0, IMPORT_DB) && ... (1) && ... (2))   atMost 180s, pollInterval 2s",
            dur=3.4,
            body=[node_state(i, "HAS DB", GREEN, "RaftImportTest") for i in range(3)]
                 + [client_to(i, "list databases" if i == 0 else "", BLUE, packet=True) for i in range(3)],
        ),
        Scene(
            step="await Person count > 0 per node",
            caption="Each node gets its own RemoteDatabase with a FIXED strategy, so the client never follows an internal Docker address.",
            detail='until(select count(*) as cnt from Person  ->  cnt > 0)   atMost 60s per node',
            dur=3.6,
            body=[node_state(i, "QUERYABLE", GREEN, "Person > 0") for i in range(3)]
                 + [node_progress(i, 1.0) for i in range(3)]
                 + [client_to(i, "count(*) from Person" if i == 0 else "", BLUE, packet=True) for i in range(3)],
        ),
        Scene(
            step="assert equal counts",
            caption="Node 0's count is the reference; the other two must match it exactly.",
            detail="assertThat(counts[0]).isGreaterThan(0); assertThat(counts[1]).isEqualTo(counts[0]); counts[2] likewise",
            dur=3.4,
            body=[code_card(92, ["counts[0] > 0", "counts[1] == counts[0]", "counts[2] == counts[0]"],
                            title="final assertions", color=GREEN)]
                 + [node_state(i, "PASS", GREEN, "same Person count") for i in range(3)]
                 + [stamp(i, "equal") for i in range(3)],
        ),
    ]
    emit("import-database-scenario", "ImportDatabaseScenarioIT - importDatabaseReplicatedAcrossCluster()",
         "Three-node Raft HA: import database replicates data to every peer via TX_ENTRY", s,
         "ImportDatabaseScenarioIT.java", ["importDatabaseReplicatedAcrossCluster"],
         "An import runs on the leader and reaches the peers as ordinary replicated transactions.")


# ============================================================================ RestoreDatabaseScenarioIT
def restore_database():
    layout(3)
    s = boot(3)[:4]
    s += [
        Scene(
            step="fixture + backup on node 0",
            caption="30 users are written and converged, then SQL takes a backup on node 0, into that container.",
            detail='command("sql", "backup database")  ->  backupFile; backupUrl = file:///home/arcadedb/backups/playwithpictures/<file>',
            dur=3.8,
            body=[code_card(92, ['command("sql","backup database")', "-> backupFile (node 0 filesystem)",
                                 "file:///home/arcadedb/backups/..."], title="backup", color=AMBER),
                  node_state(0, "LEADER", GREEN, "backup taken"),
                  node_state(1, "FOLLOWER", BLUE, "30 users"), node_state(2, "FOLLOWER", BLUE, "30 users"),
                  client_to(0, "backup database", GREEN, packet=True)],
        ),
        Scene(
            step="drop the database cluster-wide",
            caption="The wrappers are closed, then the database is dropped everywhere - this is what the restore has to undo.",
            detail='POST "drop database playwithpictures" -> 200, then until(!databaseExistsOnServer(0|1|2))   atMost 60s',
            dur=3.4,
            body=[node_state(i, "DROPPED", GREY, "not listed") for i in range(3)]
                 + [stamp(i, "gone", GREY) for i in range(3)]
                 + [client_to(0, "drop database", RED, packet=True)],
        ),
        Scene(
            step="leadership back to node 0",
            caption="The backup file lives only on node 0, and a restore from a follower cannot write a Raft entry.",
            detail="waitForAllNodesKnowLeader(30); if (leaderIdx != 0) transferLeadershipToNode(node 0, 30)",
            dur=3.8,
            body=[code_card(92, ["waitForAllNodesKnowLeader(30)", "leaderIdx = waitForRaftLeader(30)",
                                 "if (leaderIdx != 0)", "  transferLeadershipToNode(servers, 0)"],
                            title="why node 0 must lead", color=AMBER),
                  node_state(0, "LEADER", GREEN, "holds the backup file"),
                  node_state(1, "FOLLOWER", BLUE), node_state(2, "FOLLOWER", BLUE),
                  raft_link(1, 0, "leadership transfer", AMBER)],
        ),
        Scene(
            step="restore database <backupUrl>",
            caption="The restore runs on node 0 and reaches the peers as a forced snapshot rather than as replayed transactions.",
            detail='POST "restore database playwithpictures file:///home/arcadedb/backups/..."  ->  200',
            dur=3.6,
            body=[node_state(0, "LEADER", GREEN, "restoring"), node_state(1, "FOLLOWER", PURPLE, "forceSnapshot"),
                  node_state(2, "FOLLOWER", PURPLE, "forceSnapshot"),
                  client_to(0, "restore database", GREEN, packet=True),
                  raft_link(0, 1, "snapshot"), raft_link(1, 2, "")],
        ),
        Scene(
            step="await the DB and the 30 users",
            caption="Two waits, in order: the database must be listed again on every node, then every node must count 30 users.",
            detail="until(databaseExistsOnServer(0|1|2))  atMost 120s, then until(db0b/db1b/db2b .countUsers() == 30)  atMost 60s",
            dur=3.6,
            body=[node_state(i, "RESTORED", GREEN, "30 users") for i in range(3)]
                 + [node_progress(i, 1.0) for i in range(3)] + [stamp(i, "30 users") for i in range(3)],
        ),
    ]
    emit("restore-database-scenario", "RestoreDatabaseScenarioIT - restoreDatabaseReplicatedAcrossCluster()",
         "Three-node Raft HA: restore database replicates to every peer via forceSnapshot", s,
         "RestoreDatabaseScenarioIT.java", ["restoreDatabaseReplicatedAcrossCluster"],
         "Backup on node 0, drop everywhere, force leadership back, restore, verify all three.")


# ============================================================================ UserManagementScenarioIT
def user_management():
    layout(3)
    s = boot(3)[:3]
    s += [
        Scene(
            step="waitForAllNodesKnowLeader(60s)",
            caption="An elected leader is not enough: a node still outside the Raft group would learn about the new user far too late.",
            detail="Creating the user in that window commits on the majority while the late peer is absent",
            dur=3.4,
            body=[code_card(92, ["waitForRaftLeader(60)", "  -> a leader exists", "waitForAllNodesKnowLeader(60)",
                                 "  -> every node can name it"], title="two different waits", color=AMBER),
                  node_state(0, "LEADER", GREEN), node_state(1, "FOLLOWER", GREEN, "knows leader"),
                  node_state(2, "FOLLOWER", GREEN, "knows leader")],
        ),
        Scene(
            step="create user alice, on the leader",
            caption="create user is a leader-only write, so it is addressed to the leader rather than forwarded over HTTP.",
            detail='POST /api/v1/server {"command":"create user {name:alice, password:..., databases:{*:[admin]}}"}  ->  200',
            dur=3.6,
            body=[node_state(0, "LEADER", GREEN, "alice created"), node_state(1, "FOLLOWER", PURPLE, "replicating"),
                  node_state(2, "FOLLOWER", PURPLE, "replicating"),
                  client_to(0, "create user alice", GREEN, packet=True),
                  raft_link(0, 1, "security entry"), raft_link(1, 2, "")],
        ),
        Scene(
            step="await alice login on all three",
            caption="Replication is proved by an actual login on each node, probed no faster than the lockout policy allows.",
            detail="pollInterval 10s > lockoutWindow/maxFailures (30s / 5): a faster loop would lock alice out and test the lockout instead",
            dur=3.8,
            body=[code_card(92, ["a probe for a not-yet-replicated user", "counts as a FAILED authentication;",
                                 "5 failures in 30s = 30s lockout", "-> PROBE_INTERVAL_SECONDS = 10"],
                            title="why 10s between probes", color=AMBER)]
                 + [node_state(i, "LOGIN OK", GREEN, "alice -> 200") for i in range(3)]
                 + [stamp(i, "200") for i in range(3)],
        ),
        Scene(
            step="drop user alice, on the leader",
            caption="The user is dropped on the leader; the removal has to travel the same replication path.",
            detail='POST /api/v1/server {"command":"drop user alice"}  ->  assertThat(dropStatus).isEqualTo(200)',
            dur=3.4,
            body=[node_state(0, "LEADER", GREEN, "alice dropped"), node_state(1, "FOLLOWER", PURPLE, "replicating"),
                  node_state(2, "FOLLOWER", PURPLE, "replicating"),
                  client_to(0, "drop user alice", RED, packet=True), raft_link(0, 1, "security entry"),
                  raft_link(1, 2, "")],
        ),
        Scene(
            step="await login rejected everywhere",
            caption="The test passes only once alice is refused everywhere: a stale peer still granting access is the bug.",
            detail="until(!loginOk(0) && !loginOk(1) && !loginOk(2))   atMost 60s, pollInterval 10s",
            dur=3.4,
            body=[node_state(i, "REJECTED", RED, "alice -> 401") for i in range(3)]
                 + [cross_stamp(i, "401") for i in range(3)],
        ),
    ]
    emit("user-management-scenario", "UserManagementScenarioIT - userCreateAndDropReplicatedAcrossCluster()",
         "Three-node Raft HA: create/drop user replicates login authorization to every peer", s,
         "UserManagementScenarioIT.java", ["userCreateAndDropReplicatedAcrossCluster"],
         "Security replication measured the only way that counts: by logging in on each peer.")


# ============================================================================ UserSeedOnPeerAddScenarioIT
def user_seed_on_peer_add():
    layout(3)
    s = boot(3)[:3]
    s += [
        Scene(
            step="create alice, verify on all three",
            caption="The starting state: a user that every node accepts, so any later damage to user state is visible.",
            detail="startContainers() + waitForRaftLeader(60) + waitForAllNodesKnowLeader(60), then create user alice on the leader",
            dur=3.6,
            body=[node_state(i, "LOGIN OK", GREEN, "alice -> 200") for i in range(3)]
                 + [stamp(i, "200") for i in range(3)]
                 + [client_to(0, "create user alice", GREEN, packet=True)],
        ),
        Scene(
            step="POST /api/v1/cluster/peer, empty id",
            caption="The peer-add endpoint is probed with an empty peerId: this test checks the wiring, not a real membership change.",
            detail='POST {"peerId":"", "address":""}  ->  assertThat(status).isEqualTo(400)  ("Missing required fields: peerId, address")',
            dur=4.0,
            body=[code_card(92, ["a real addPeer payload is unreliable:", "  duplicate peerId -> Ratis 500",
                                 "  bogus peerId -> 90s Raft retry loop", "so the validation branch is used"],
                            title="why an empty peerId", color=AMBER),
                  node_state(0, "LEADER", AMBER, "validates input"), cross_stamp(0, "400", AMBER),
                  node_state(1, "FOLLOWER", BLUE), node_state(2, "FOLLOWER", BLUE),
                  client_to(0, "POST cluster/peer", AMBER, packet=True)],
        ),
        Scene(
            step="alice must still log in",
            caption="The real assertion: the best-effort user seed fired by peer-add must not disturb the user state already in place.",
            detail="assertThat(loginOk(0)).isTrue(); loginOk(1); loginOk(2)   -   asserted directly, no Awaitility",
            dur=3.6,
            body=[node_state(i, "LOGIN OK", GREEN, "alice -> 200") for i in range(3)]
                 + [stamp(i, "unchanged") for i in range(3)],
        ),
        Scene(
            step="scope of this smoke test",
            caption="The full seed mechanism (SECURITY_USERS_ENTRY emitted by PostAddPeerHandler) is covered in-process elsewhere.",
            detail="RaftUserSeedOnPeerAdd3NodesIT covers the seed itself; this IT covers the Docker-level endpoint wiring",
            dur=3.4,
            body=[code_card(92, ["covered here:", "  endpoint registered + reachable",
                                 "  accepts JSON, validates input",
                                 "  existing users unaffected", "",
                                 "covered by RaftUserSeedOnPeerAdd3NodesIT:",
                                 "  the SECURITY_USERS_ENTRY seed"], title="what this proves", color=GREEN)],
        ),
    ]
    emit("user-seed-on-peer-add-scenario", "UserSeedOnPeerAddScenarioIT - peerAddEndpointPreservesExistingUsers()",
         "Three-node Raft HA: peer-add endpoint fires the seed without breaking existing users", s,
         "UserSeedOnPeerAddScenarioIT.java", ["peerAddEndpointPreservesExistingUsers"],
         "A wiring smoke test for /api/v1/cluster/peer that must leave user state intact.")


# ============================================================================ LeaderFailoverIT
def leader_failover():
    layout(3)
    s = boot(3)[:4]
    s += [
        Scene(
            step="seed 20 users, assert on all three",
            caption="A known starting point on every node, asserted directly rather than awaited.",
            detail="db0.addUserAndPhotos(20,10); db0/db1/db2.assertThatUserCountIs(20)",
            dur=3.2,
            body=[node_state(i, "READY", GREEN, "20 users") for i in range(3)]
                 + [node_progress(i, 1.0) for i in range(3)] + [stamp(i, "20") for i in range(3)],
        ),
        Scene(
            step="findLeaderIndex(), then stop it",
            caption="The leader is located and its container is stopped outright - a catastrophic failure, not a graceful shutdown.",
            detail="dbs[leaderIdx].close(); containers[leaderIdx].stop();   survivors = (leaderIdx+1)%3 and (leaderIdx+2)%3",
            dur=3.6,
            body=[node_off(0, "KILLED", "container stopped"), cut(0, 1, "leader gone"),
                  node_state(1, "FOLLOWER", AMBER, "no leader"), node_state(2, "FOLLOWER", AMBER, "no leader")],
        ),
        Scene(
            step="election among the survivors",
            caption="The two remaining nodes still hold a majority of three, so Raft elects a new leader from them.",
            detail="waitForRaftLeader(List.of(servers.get(survivor1), servers.get(survivor2)), 60)",
            dur=3.4,
            body=[node_off(0, "DOWN"), node_state(1, "NEW LEADER", GREEN, "isLeader:true"),
                  node_state(2, "FOLLOWER", BLUE), raft_link(1, 2, "term++", AMBER),
                  client_to(1, "GET /api/v1/cluster", BLUE, packet=True)],
        ),
        Scene(
            step="write through the survivor",
            caption="Writes resume on the majority. The count is measured, not assumed: writes issued during the transition may be lost.",
            detail="dbs[survivor1].addUserAndPhotos(10,10); final long actualCount = dbs[survivor1].countUsers();",
            dur=3.6,
            body=[node_off(0, "DOWN"), node_state(1, "LEADER", GREEN, "writing"),
                  node_state(2, "FOLLOWER", BLUE, "applying"), node_progress(1, 1.0), node_progress(2, 0.6, BLUE),
                  client_to(1, "110 transactions", GREEN, packet=True), raft_link(1, 2, "replicate")],
        ),
        Scene(
            step="survivors converge (>= 20, equal)",
            caption="The two survivors must agree, and must not have gone backwards below the pre-failover count.",
            detail="until(usersS1 == usersS2 && usersS1 >= 20L)   atMost 60s   -   then assert survivor2 == convergedCount",
            dur=3.4,
            body=[node_off(0, "DOWN"), node_state(1, "LEADER", GREEN, "converged"),
                  node_state(2, "FOLLOWER", GREEN, "converged"), stamp(1, "equal"), stamp(2, "equal")],
        ),
        Scene(
            step="restart the old leader, resync",
            caption="The killed node is restarted and must reach exactly the converged count via Raft log catch-up.",
            detail="containers[leaderIdx].start(); waitForContainerHealthy(60); new ServerWrapper(...) - the mapped port changed",
            dur=3.8,
            body=[node_state(0, "REJOINING", PURPLE, "log catch-up"), node_state(1, "LEADER", GREEN),
                  node_state(2, "FOLLOWER", BLUE), node_progress(0, 0.55, PURPLE, "resyncing"),
                  raft_link(1, 0, "catch-up entries"),
                  client_to(0, "countUsers()", BLUE, packet=True)],
        ),
        Scene(
            step="all three equal, and the variants",
            caption="After the rejoin all three nodes are asserted against the same converged count.",
            detail="dbRestarted / survivor1 / survivor2 .assertThatUserCountIs((int) convergedCount)",
            dur=4.0,
            body=[variants_card([
                "repeatedLeaderFailures():",
                "  2 kill cycles; the previous node is",
                "  restarted BEFORE the next kill so a",
                "  majority always exists",
                "leaderFailoverDuringWrites():",
                "  kill right after a write; only",
                "  equality is asserted (>= 20)",
            ])] + [node_state(i, "PASS", GREEN, "converged count") for i in range(3)]
                 + [stamp(i, "equal") for i in range(3)],
        ),
    ]
    emit("leader-failover", "LeaderFailoverIT - leaderFailover()",
         "Kill the leader, verify the new election, the writes that follow and the rejoin", s,
         "LeaderFailoverIT.java", ["leaderFailover", "repeatedLeaderFailures", "leaderFailoverDuringWrites"],
         "The leader is stopped outright; the majority must elect, keep writing, and re-absorb it.")


# ============================================================================ RollingRestartIT
def rolling_restart():
    layout(3)
    s = boot(3, persistent=True)[:4]
    s += [
        Scene(
            step="seed 30 users, await on all three",
            caption="Persistent containers are used here: the data has to survive each stop/start cycle.",
            detail="until(users0 == 30 && users1 == 30 && users2 == 30)   atMost 60s",
            dur=3.2,
            body=[node_state(i, "READY", GREEN, "30 users") for i in range(3)]
                 + [node_progress(i, 1.0) for i in range(3)],
        ),
        Scene(
            step="stop arcadedb-0, keep writing",
            caption="One node goes down, the cluster must stay writable: a leader is awaited, then ten more users are written.",
            detail="until(hasLeader(servers)) atMost 30s, then db1.addUserAndPhotos(10,10); until(users1 == 40 && users2 == 40)",
            dur=3.8,
            body=[node_off(0, "STOPPED"), node_state(1, "LEADER", GREEN, "40 users"),
                  node_state(2, "FOLLOWER", GREEN, "40 users"), cut(0, 1, ""),
                  client_to(1, "addUserAndPhotos(10,10)", GREEN, packet=True), raft_link(1, 2, "replicate")],
        ),
        Scene(
            step="start it, new ServerWrapper, resync",
            caption="On restart Testcontainers assigns a new host port, so a fresh ServerWrapper is mandatory before the resync check.",
            detail="arcade0.start(); sleep(15); new ServerWrapper(arcade0); until(db0Restart.countUsers() == 40)   atMost 90s",
            dur=3.8,
            body=[code_card(92, ["a restarted container gets a NEW", "mapped host port, so the old",
                                 "ServerWrapper points at nothing:", "  new ServerWrapper(arcade0)"],
                            title="the restart trap", color=AMBER),
                  node_state(0, "REJOINED", GREEN, "40 users"), node_state(1, "LEADER", GREEN),
                  node_state(2, "FOLLOWER", BLUE), raft_link(1, 0, "catch-up")],
        ),
        Scene(
            step="repeat for arcadedb-1 and -2",
            caption="The same cycle rolls across the other two nodes, ten more users written during each window.",
            detail="stop -> await leader -> write 10 -> assert 50 (then 60) on the live pair -> start -> resync",
            dur=3.8,
            body=[node_state(0, "LEADER", GREEN, "60 users"), node_off(1, "RESTARTING"),
                  node_state(2, "FOLLOWER", GREEN, "60 users"),
                  client_to(0, "addUserAndPhotos(10,10)", GREEN, packet=True), cut(0, 1, "")],
        ),
        Scene(
            step="final 60 on every node, and variants",
            caption="Zero downtime is the claim, so the totals are absolute: 30 seeded plus three batches of 10.",
            detail="db0Restart / db1Restart / db2Restart .assertThatUserCountIs(60)",
            dur=4.0,
            body=[variants_card([
                "rapidRollingRestart():",
                "  stop/start back to back, no writes",
                "  in between; 20 users must survive",
                "rollingRestartWithContinuousWrites():",
                "  writes interleaved with restarts;",
                "  only equality is asserted, because",
                "  a write can be lost mid-election",
            ])] + [node_state(i, "PASS", GREEN, "60 users") for i in range(3)]
                 + [stamp(i, "60") for i in range(3)],
        ),
    ]
    emit("rolling-restart", "RollingRestartIT - rollingRestart()",
         "Restart each node in turn and verify the cluster never stops accepting writes", s,
         "RollingRestartIT.java", ["rollingRestart", "rapidRollingRestart", "rollingRestartWithContinuousWrites"],
         "Zero-downtime maintenance: one node down at a time, writes continuing throughout.")


# ============================================================================ NetworkPartitionIT
def network_partition():
    layout(3)
    s = boot(3, persistent=True)[:4]
    s += [
        Scene(
            step="seed 10 users, check schema + data",
            caption="Schema and data are verified on all three nodes before anything is broken.",
            detail="db0/db1/db2.checkSchema(); until(countUsers() == 10 on all three)   atMost 30s",
            dur=3.2,
            body=[node_state(i, "READY", GREEN, "10 users") for i in range(3)] + [stamp(i, "10") for i in range(3)],
        ),
        Scene(
            step="disconnect the LEADER from the network",
            caption="A Docker network disconnect is a true symmetric partition - the leader lands alone in the minority.",
            detail="disconnectFromNetwork(nodeContainers[leaderIdx])",
            dur=3.6,
            body=[isolated(0, "minority of 1"), cut(0, 1, "partition"),
                  node_state(1, "FOLLOWER", AMBER, "majority of 2"), node_state(2, "FOLLOWER", AMBER, "majority of 2")],
        ),
        Scene(
            step="step-down, then election in the majority",
            caption="A leader that cannot reach a majority steps down by itself; only the two-node side can elect.",
            detail="waitForRaftLeader(List.of(survivor1, survivor2), 60)",
            dur=3.6,
            body=[isolated(0, "stepped down"), node_state(1, "NEW LEADER", GREEN),
                  node_state(2, "FOLLOWER", BLUE), raft_link(1, 2, "term++", AMBER), cut(0, 1, "")],
        ),
        Scene(
            step="write 20 users to the majority",
            caption="The majority partition keeps serving writes while the isolated node is stuck with its pre-partition data.",
            detail="dbs[survivor1].addUserAndPhotos(20,10); until(usersS1 == usersS2 && usersS1 >= 10L)   atMost 60s",
            dur=3.6,
            body=[isolated(0, "stale: 10 users"), node_state(1, "LEADER", GREEN, "30 users"),
                  node_state(2, "FOLLOWER", GREEN, "30 users"), cut(0, 1, ""),
                  client_to(1, "220 transactions", GREEN, packet=True), raft_link(1, 2, "replicate")],
        ),
        Scene(
            step="heal: reconnect AND restart",
            caption="Reconnecting is not enough: the gRPC channels back off for ~2 minutes, so the node is restarted.",
            detail="reconnectToNetwork(...); container.stop(); container.start(); waitForContainerHealthy(90); new ServerWrapper(...)",
            dur=4.0,
            body=[code_card(92, ["after a Docker partition the peer", "gRPC channels stay in backoff (~120s);",
                                 "reconnecting does NOT reset them", "-> stop() + start() the isolated node"],
                            title="why a restart is needed", color=AMBER),
                  node_state(0, "REJOINING", PURPLE, "fresh channels"), node_state(1, "LEADER", GREEN),
                  node_state(2, "FOLLOWER", BLUE), raft_link(1, 0, "catch-up")],
        ),
        Scene(
            step="converge, and the variants",
            caption="All three nodes must reach the count the majority reached while the partition was open.",
            detail="until(restarted == majorityCount && s1 == majorityCount && s2 == majorityCount)   atMost 180s",
            dur=4.0,
            body=[variants_card([
                "singleFollowerPartition():",
                "  isolate a FOLLOWER; leader keeps",
                "  its majority and writes continue",
                "noQuorumScenario():",
                "  isolate TWO nodes; leader steps",
                "  down, a plain INSERT is rejected,",
                "  count stays 10 after the restart",
            ])] + [node_state(i, "PASS", GREEN, "majorityCount") for i in range(3)]
                 + [stamp(i, "equal") for i in range(3)],
        ),
    ]
    emit("network-partition", "NetworkPartitionIT - leaderPartitionWithQuorum()",
         "Isolate the leader, verify the new election in the majority and the convergence after healing", s,
         "NetworkPartitionIT.java", ["leaderPartitionWithQuorum", "singleFollowerPartition", "noQuorumScenario"],
         "Docker network disconnect as a real partition: who may still write, and who catches up.")


# ============================================================================ NetworkPartitionRecoveryIT
def network_partition_recovery():
    layout(3)
    s = boot(3, persistent=True)[:4]
    s += [
        Scene(
            step="seed 20 users on all three",
            caption="The pre-partition state is asserted directly on every node.",
            detail="db0.addUserAndPhotos(20,10); db0/db1/db2.assertThatUserCountIs(20)",
            dur=3.0,
            body=[node_state(i, "READY", GREEN, "20 users") for i in range(3)] + [stamp(i, "20") for i in range(3)],
        ),
        Scene(
            step="isolate a FOLLOWER (2+1 split)",
            caption="A follower is isolated on purpose, so the majority keeps the leader it already has and no election is needed.",
            detail="isolatedIdx = (leaderIdx + 1) % 3;  disconnectFromNetwork(nodeContainers[isolatedIdx])",
            dur=3.6,
            body=[node_state(0, "LEADER", GREEN, "keeps quorum"), isolated(1, "minority of 1"),
                  node_state(2, "FOLLOWER", GREEN, "majority"), cut(0, 1, "partition")],
        ),
        Scene(
            step="write 10 users to the majority",
            caption="The majority keeps writing. The isolated node is not queried: its mapped port may be unreachable.",
            detail="dbs[leaderIdx].addUserAndPhotos(10,10); until(uLeader == uOther && uLeader >= 20L)   atMost 60s",
            dur=3.6,
            body=[node_state(0, "LEADER", GREEN, "30 users"), isolated(1, "stale: 20 users"),
                  node_state(2, "FOLLOWER", GREEN, "30 users"), cut(0, 1, ""),
                  client_to(0, "110 transactions", GREEN, packet=True)],
        ),
        Scene(
            step="heal: reconnect, stop, start",
            caption="The isolated node is restarted so its gRPC channels are rebuilt instead of waiting out the backoff.",
            detail="reconnectToNetwork(); stop(); start(); waitForContainerHealthy(90); new ServerWrapper(...)",
            dur=3.6,
            body=[node_state(0, "LEADER", GREEN), node_state(1, "REJOINING", PURPLE, "fresh channels"),
                  node_state(2, "FOLLOWER", BLUE), raft_link(0, 1, "catch-up entries"),
                  node_progress(1, 0.5, PURPLE, "log catch-up")],
        ),
        Scene(
            step="Raft log catch-up to majorityCount",
            caption="There is no conflict resolution to do: the minority never accepted a write, so catching up is a pure replay.",
            detail="until(restarted == majorityCount && leader == majorityCount && other == majorityCount)   atMost 180s",
            dur=3.6,
            body=[node_state(i, "CONVERGED", GREEN, "majorityCount") for i in range(3)]
                 + [node_progress(i, 1.0) for i in range(3)] + [stamp(i, "equal") for i in range(3)],
        ),
        Scene(
            step="the variants",
            caption="Two siblings push the same shape further: three consecutive cycles, and a one-way isolation that resyncs.",
            detail="multiplePartitionCycles() captures the stable count BEFORE each restart, to avoid reading during a re-election",
            dur=4.0,
            body=[variants_card([
                "multiplePartitionCycles():",
                "  3 x (isolate follower, write 5,",
                "  heal, await leader, converge)",
                "  count captured BEFORE the restart",
                "asymmetricPartitionRecovery():",
                "  isolate a follower, write 15,",
                "  heal and resync to majorityCount",
            ])] + [node_state(i, "PASS", GREEN) for i in range(3)],
        ),
    ]
    emit("network-partition-recovery", "NetworkPartitionRecoveryIT - partitionRecovery()",
         "2+1 split, heal the partition, verify the Raft log catch-up", s,
         "NetworkPartitionRecoveryIT.java",
         ["partitionRecovery", "multiplePartitionCycles", "asymmetricPartitionRecovery"],
         "The minority never wrote, so healing is a replay - no conflict resolution exists or is needed.")


# ============================================================================ SplitBrainIT
def split_brain():
    layout(3)
    s = boot(3, persistent=True)[:4]
    s += [
        Scene(
            step="seed 20 users on all three",
            caption="A known state on every node before the cluster is cut in two.",
            detail="db0.addUserAndPhotos(20,10); db0/db1/db2.assertThatUserCountIs(20)",
            dur=3.0,
            body=[node_state(i, "READY", GREEN, "20 users") for i in range(3)] + [stamp(i, "20") for i in range(3)],
        ),
        Scene(
            step="isolate the LEADER (2+1)",
            caption="The leader is put in the minority - the exact shape that would produce split-brain in a system without quorum.",
            detail="disconnectFromNetwork(nodeContainers[leaderIdx])   -   survivors keep 2 of 3",
            dur=3.6,
            body=[isolated(0, "minority of 1"), cut(0, 1, "2+1 split"),
                  node_state(1, "FOLLOWER", AMBER, "majority"), node_state(2, "FOLLOWER", AMBER, "majority")],
        ),
        Scene(
            step="minority steps down, majority elects",
            caption="Raft prevents split-brain by design: the isolated leader steps down because it cannot reach a majority.",
            detail="waitForRaftLeader(majorityServers, 60); waitForAllNodesKnowLeader(majorityServers, 30)",
            dur=3.8,
            body=[isolated(0, "stepped down"), node_state(1, "NEW LEADER", GREEN),
                  node_state(2, "FOLLOWER", BLUE), cut(0, 1, ""), raft_link(1, 2, "term++", AMBER)],
        ),
        Scene(
            step="majority writes to 30; minority does not",
            caption="Only the majority can commit. A write to the minority is deliberately never attempted.",
            detail="until(dbs[survivor1].countUsers() == 30L && dbs[survivor2].countUsers() == 30L)   atMost 2 minutes",
            dur=4.2,
            body=[code_card(92, ["a write to the minority would:", "  time out after 30s (no quorum)",
                                 "  leave stale uncommitted entries", "  block Raft log reconciliation",
                                 "-> the test only READS the minority"],
                            title="what is skipped, and why", color=AMBER),
                  isolated(0, "read may fail"), node_state(1, "LEADER", GREEN, "30 users"),
                  node_state(2, "FOLLOWER", GREEN, "30 users"), cut(0, 1, "")],
        ),
        Scene(
            step="heal: reconnect and restart",
            caption="The old leader is reconnected and restarted, so its peer channels are rebuilt from scratch.",
            detail="reconnectToNetwork(); stop(); start(); waitForContainerHealthy(90); new ServerWrapper(...)",
            dur=3.6,
            body=[node_state(0, "REJOINING", PURPLE, "truncating its log"), node_state(1, "LEADER", GREEN),
                  node_state(2, "FOLLOWER", BLUE), raft_link(1, 0, "new leader's entries"),
                  node_progress(0, 0.45, PURPLE, "reconciling")],
        ),
        Scene(
            step="reformation, and the variants",
            caption="The old leader truncates its own log and applies the new leader's, which is why this wait is the longest in the suite.",
            detail="until(all three == majorityCount) atMost 3 minutes, pollInterval 5s   -   counts read over plain HTTP",
            dur=4.2,
            body=[variants_card([
                "completePartitionNoQuorum():",
                "  1+1+1; every leader steps down,",
                "  all writes rejected, restart all",
                "clusterReformation():",
                "  3 partition/heal cycles",
                "quorumLossRecovery():",
                "  isolate 2 of 3, write rejected,",
                "  restart all, then write again",
            ])] + [node_state(i, "PASS", GREEN, "majorityCount") for i in range(3)]
                 + [stamp(i, "equal") for i in range(3)],
        ),
    ]
    emit("split-brain", "SplitBrainIT - splitBrainPrevention()",
         "The minority cannot accept writes: the isolated leader steps down and later truncates its log", s,
         "SplitBrainIT.java",
         ["splitBrainPrevention", "completePartitionNoQuorum", "clusterReformation", "quorumLossRecovery"],
         "Quorum enforcement: divergent writes are impossible, so healing is truncate-and-replay.")


# ============================================================================ NetworkDelayIT
def network_delay():
    layout(3, proxy=True)
    s = [
        Scene(
            step="create the Raft and HTTP proxies",
            caption="Every node is reached through Toxiproxy, and the Raft server list points at the proxy rather than at the containers.",
            detail='SERVER_LIST = proxy:8660:8670,proxy:8661:8671,proxy:8662:8672   (raft 866x, http 867x)',
            dur=3.8,
            body=[code_card(92, ['createProxy("raftProxy0",', '  "0.0.0.0:8660", "arcadedb-0:2434")',
                                 'createProxy("httpProxy0",', '  "0.0.0.0:8670", "arcadedb-0:2480")',
                                 "... the same for nodes 1 and 2"], title="useToxiproxy() = true", color=AMBER)]
                 + [node_state(i, "PROXIED", AMBER, "via toxiproxy") for i in range(3)],
        ),
    ] + boot(3)[1:4] + [
        Scene(
            step="seed 10 users with no delay",
            caption="A clean baseline first: replication is verified before any toxic exists.",
            detail="db1.addUserAndPhotos(10,10); db1/db2/db3.assertThatUserCountIs(10)",
            dur=3.2,
            body=[node_state(i, "READY", GREEN, "10 users") for i in range(3)] + [stamp(i, "10") for i in range(3)],
        ),
        Scene(
            step="latency 200ms on every Raft proxy",
            caption="The delay hits the consensus port only: the HTTP command path stays fast, the Raft round trip pays.",
            detail='raftProxyN.toxics().latency("latency_raftN", ToxicDirection.DOWNSTREAM, 200)',
            dur=3.8,
            body=[toxic(i, "latency 200ms") for i in range(3)]
                 + [node_state(0, "LEADER", AMBER, "slow consensus"), node_state(1, "FOLLOWER", AMBER),
                    node_state(2, "FOLLOWER", AMBER)]
                 + [client_to(0, "addUserAndPhotos(20,10)", GREEN, packet=True)],
        ),
        Scene(
            step="write under latency, then converge",
            caption="The write duration is logged rather than asserted; what must hold is that all three nodes still reach 30 users.",
            detail="until(users1 == 30L && users2 == 30L && users3 == 30L)   atMost 60s, pollInterval 2s",
            dur=3.6,
            body=[toxic(i, "latency 200ms") for i in range(3)]
                 + [node_state(i, "CONVERGED", GREEN, "30 users") for i in range(3)]
                 + [node_progress(i, 1.0) for i in range(3)],
        ),
        Scene(
            step="remove the toxics, assert 30",
            caption="The toxics are removed and the final counts are asserted on a healthy network.",
            detail='raftProxyN.toxics().get("latency_raftN").remove();  db1/db2/db3.assertThatUserCountIs(30)',
            dur=3.4,
            body=[node_state(i, "PASS", GREEN, "30 users") for i in range(3)] + [stamp(i, "30") for i in range(3)],
        ),
        Scene(
            step="the variants",
            caption="Three siblings vary where the delay sits and how extreme it gets.",
            detail="asymmetricLeaderDelay - highLatencyWithJitter - extremeLatency",
            dur=4.0,
            body=[variants_card([
                "asymmetricLeaderDelay():",
                "  500ms UP and DOWN on node 0 only,",
                "  writes issued from a follower",
                "highLatencyWithJitter():",
                "  2 nodes, 300ms +/- 150ms jitter",
                "extremeLatency():",
                "  2000ms; some writes time out, so",
                "  the leader's count is measured and",
                "  the peer must merely match it",
            ])] + [node_state(i, "PASS", GREEN) for i in range(3)],
        ),
    ]
    emit("network-delay", "NetworkDelayIT - symmetricDelay()",
         "Toxiproxy latency on the Raft consensus port: replication must still converge", s,
         "NetworkDelayIT.java",
         ["symmetricDelay", "asymmetricLeaderDelay", "highLatencyWithJitter", "extremeLatency"],
         "Latency injected on port 2434 only, so consensus slows down while HTTP stays fast.",
         proxy=True)


# ============================================================================ PacketLossIT
def packet_loss():
    layout(2, proxy=True)
    s = [
        Scene(
            step="create the Raft and HTTP proxies",
            caption="Two nodes, both addressed through Toxiproxy, with the Raft server list pointing at the proxy ports.",
            detail="SERVER_LIST = proxy:8660:8670,proxy:8661:8671   -   useToxiproxy() returns true",
            dur=3.6,
            body=[code_card(92, ['createProxy("raftProxy0",', '  "0.0.0.0:8660", "arcadedb-0:2434")',
                                 'createProxy("httpProxy0",', '  "0.0.0.0:8670", "arcadedb-0:2480")',
                                 "... the same for node 1"], title="useToxiproxy() = true", color=AMBER)]
                 + [node_state(i, "PROXIED", AMBER, "via toxiproxy") for i in range(2)],
        ),
    ] + boot(2)[1:4] + [
        Scene(
            step="seed 10 users, verify both nodes",
            caption="The baseline is established on a clean network.",
            detail="db1.addUserAndPhotos(10,10); db1.assertThatUserCountIs(10); db2.assertThatUserCountIs(10)",
            dur=3.0,
            body=[node_state(i, "READY", GREEN, "10 users") for i in range(2)] + [stamp(i, "10") for i in range(2)],
        ),
        Scene(
            step="5% loss on both Raft proxies",
            caption="limitData with a 5% toxicity drops a slice of the consensus traffic: minor damage the cluster should absorb.",
            detail='raftProxyN.toxics().limitData("packet_loss_raftN", DOWNSTREAM, 0).setToxicity(0.05f)',
            dur=3.8,
            body=[toxic(i, "loss 5%") for i in range(2)]
                 + [node_state(0, "LEADER", AMBER, "lossy consensus"), node_state(1, "FOLLOWER", AMBER, "retrying")]
                 + [client_to(0, "addUserAndPhotos(20,10)", GREEN, packet=True)],
        ),
        Scene(
            step="converge to 30 despite the loss",
            caption="Raft retransmits what is dropped; the wait is longer than usual but the target is still exact.",
            detail="until(users1 == 30L && users2 == 30L)   atMost 180s, pollInterval 2s",
            dur=3.6,
            body=[toxic(i, "loss 5%") for i in range(2)]
                 + [node_state(i, "CONVERGED", GREEN, "30 users") for i in range(2)]
                 + [node_progress(i, 1.0) for i in range(2)],
        ),
        Scene(
            step="remove the toxics, assert 30",
            caption="With the network healthy again, both nodes are asserted against the absolute total.",
            detail='toxics().get("packet_loss_raftN").remove();  db1.assertThatUserCountIs(30); db2 likewise',
            dur=3.2,
            body=[node_state(i, "PASS", GREEN, "30 users") for i in range(2)] + [stamp(i, "30") for i in range(2)],
        ),
        Scene(
            step="the variants",
            caption="Four siblings raise the toxicity, make it one-way, or switch it on and off repeatedly.",
            detail="moderatePacketLoss 20% - highPacketLoss 50% - directionalPacketLoss 30% one-way (3 nodes) - intermittentPacketLoss 25% x3",
            dur=4.2,
            body=[variants_card([
                "moderatePacketLoss():  20%, expect 25",
                "highPacketLoss():      50%; writes may",
                "  fail, so the leader's count is",
                "  measured and the peer must match",
                "directionalPacketLoss(): 30% DOWNSTREAM",
                "  on node 0 only, 3-node cluster",
                "intermittentPacketLoss(): 25% applied",
                "  and removed over 3 cycles",
            ])] + [node_state(i, "PASS", GREEN) for i in range(2)],
        ),
    ]
    emit("packet-loss", "PacketLossIT - lowPacketLoss()",
         "Toxiproxy packet loss on the Raft consensus port: retransmission must still converge", s,
         "PacketLossIT.java",
         ["lowPacketLoss", "moderatePacketLoss", "highPacketLoss", "directionalPacketLoss", "intermittentPacketLoss"],
         "Dropped consensus traffic at 5% to 50%, one-way and intermittent.",
         n=2, proxy=True)


# ---------------------------------------------------------------------------- index.html
INDEX_CSS = """
:root{--bg:#0b1220;--panel:#141f36;--edge:#25344f;--text:#e7eefc;--muted:#8ea3c4;--blue:#41b6f7}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);
 font-family:ui-sans-serif,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
header{padding:28px 32px 14px}
h1{margin:0 0 6px;font-size:22px}
header p{margin:0;color:var(--muted);font-size:13.5px;max-width:860px;line-height:1.5}
main{display:grid;grid-template-columns:320px 1fr;gap:18px;padding:14px 32px 32px;align-items:start}
nav{background:var(--panel);border:1px solid var(--edge);border-radius:14px;padding:8px;position:sticky;top:14px}
nav button{display:block;width:100%;text-align:left;background:none;border:0;color:var(--muted);
 font:inherit;font-size:13px;padding:9px 12px;border-radius:9px;cursor:pointer;line-height:1.35}
nav button:hover{background:#ffffff0d;color:var(--text)}
nav button.on{background:#41b6f71f;border:1px solid #41b6f766;color:var(--text);font-weight:600}
nav button small{display:block;color:var(--muted);font-weight:400;font-size:11px;margin-top:2px;
 font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
section{background:var(--panel);border:1px solid var(--edge);border-radius:14px;padding:18px}
section h2{margin:0 0 4px;font-size:17px}
section p.blurb{margin:0 0 12px;color:var(--muted);font-size:13px;line-height:1.5}
img{width:100%;height:auto;border-radius:12px;display:block;background:var(--bg)}
ul.meta{list-style:none;display:flex;flex-wrap:wrap;gap:8px;padding:0;margin:12px 0 0}
ul.meta li{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:11px;
 color:var(--muted);border:1px solid var(--edge);border-radius:9px;padding:4px 9px}
a{color:var(--blue)}
@media (max-width:900px){main{grid-template-columns:1fr}nav{position:static}}
"""


def write_index():
    items = json.dumps(DIAGRAMS, indent=2)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ArcadeDB e2e-ha test workflows</title>
<style>{INDEX_CSS}</style>
</head>
<body>
<header>
  <h1>ArcadeDB e2e-ha test workflows</h1>
  <p>One looping animation per integration test in <code>e2e-ha</code>: the left rail is the call
     sequence, the stage shows what the Raft cluster is doing, the bottom bar carries the contract being
     exercised. Classes with several <code>@Test</code> methods are drawn from their primary method, with
     the siblings listed on a variants card. Regenerate everything with
     <code>python3 build_all.py</code>.</p>
</header>
<main>
  <nav id="nav"></nav>
  <section>
    <h2 id="title"></h2>
    <p class="blurb" id="blurb"></p>
    <img id="svg" alt="">
    <ul class="meta" id="meta"></ul>
  </section>
</main>
<script>
const DIAGRAMS = {items};
const SRC = {json.dumps(SRC)};
const nav = document.getElementById('nav');
function show(i) {{
  const d = DIAGRAMS[i];
  document.getElementById('title').textContent = d.title;
  document.getElementById('blurb').textContent = d.blurb;
  const img = document.getElementById('svg');
  img.src = d.slug + '.svg?' + Date.now();   // force the animation to restart from scene 1
  img.alt = d.title;
  document.getElementById('meta').innerHTML =
    ['<li>' + d.scenes + ' scenes</li>', '<li>' + d.loop + 's loop</li>',
     '<li>' + d.methods.length + ' @Test method' + (d.methods.length > 1 ? 's' : '') + '</li>',
     '<li><a href="' + SRC + '/' + d.java + '">' + d.java + '</a></li>'].join('');
  [...nav.children].forEach((b, j) => b.classList.toggle('on', i === j));
  location.hash = d.slug;
}}
DIAGRAMS.forEach((d, i) => {{
  const b = document.createElement('button');
  b.innerHTML = d.title.split(' - ')[0] + '<small>' + d.title.split(' - ').slice(1).join(' - ') + '</small>';
  b.onclick = () => show(i);
  nav.appendChild(b);
}});
const from = DIAGRAMS.findIndex(d => d.slug === location.hash.slice(1));
show(from >= 0 ? from : 0);
</script>
</body>
</html>
"""
    with open(os.path.join(HERE, "index.html"), "w") as f:
        f.write(html)
    print(f"  index.html  ({len(DIAGRAMS)} diagrams)")


if __name__ == "__main__":
    for fn in (simple_ha, three_instances, load_three_instances, drop_database, import_database,
               restore_database, user_management, user_seed_on_peer_add, leader_failover,
               rolling_restart, network_partition, network_partition_recovery, split_brain,
               network_delay, packet_loss):
        fn()
    write_index()
