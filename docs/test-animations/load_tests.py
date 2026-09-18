#!/usr/bin/env python3
"""Animated workflows for the load-tests module.

One diagram per test class in load-tests/src/test/java/com/arcadedb/test/load/. These runs are
long, parameterized over a protocol enum, and driven from a thread pool, so the stage here is a
worker pool on the left and live counters inside the server rather than a client box.

Run through build_all.py, which also writes the index page.
"""

from catalog import emit
from workflow_svg import (AMBER, BLUE, GREEN, GREY, PURPLE, Scene, client_to, code_card, counters,
                          layout, node_progress, node_state, raft_link, stamp, variants_card,
                          worker)

SRC = "load-tests/src/test/java/com/arcadedb/test/load/"


# ============================================================================ SingleServerLoadTestIT
def single_server_load():
    layout(1, workers=7)
    scenes = [
        Scene(
            step="@ParameterizedTest over the protocols",
            caption="The whole load run happens once per client protocol, so both wire paths carry the same work.",
            detail="@EnumSource(DatabaseWrapper.Protocol.class) -> HTTP (2480) and GRPC (50051); one container per invocation",
            dur=3.8,
            body=[code_card(92, ["@ParameterizedTest",
                                 "@EnumSource(Protocol.class)",
                                 "  HTTP  -> 2480",
                                 "  GRPC  -> 50051"],
                            title="run twice, same body", color=AMBER),
                  node_state(0, "STARTING", AMBER, "single node, no HA")],
        ),
        Scene(
            step="one container, database and schema",
            caption="A plain single-server container: no Raft, no quorum, nothing between the client and the storage engine.",
            detail="createArcadeContainer(\"arcade\", network) - startContainers() - db.createDatabase() - db.createSchema()",
            dur=3.4,
            body=[node_state(0, "READY", GREEN, "playwithpictures"),
                  counters(0, [("users", 0, "0 / 5000"), ("photos", 0, "0 / 50000"),
                               ("friendships", 0, "0 / 500"), ("likes", 0, "0 / 500")]),
                  client_to(0, "createSchema()", GREEN, packet=True)],
        ),
        Scene(
            step="5 writer tasks on a pool of 10",
            caption="Five tasks each open their own connection and write 1000 users with 10 photos apiece.",
            detail="Executors.newFixedThreadPool(10); each task: new DatabaseWrapper(...) - addUserAndPhotos(1000, 10) - close()",
            dur=4.0,
            body=[worker(k, f"writer {k + 1}", GREEN, frac=0.45, sub="1000 users x 10") for k in range(5)]
                 + [node_state(0, "UNDER LOAD", BLUE, "5 concurrent writers"),
                    counters(0, [("users", 0.45, "2230 / 5000"), ("photos", 0.44, "22K / 50000"),
                                 ("friendships", 0, "0 / 500"), ("likes", 0, "0 / 500")]),
                    client_to(0, "11000 transactions", GREEN, packet=True)],
        ),
        Scene(
            step="one task each for edges",
            caption="Two more tasks build the edges, and they read back ids first - an edge needs vertices that already exist.",
            detail="createFriendships(500) and createLike(500); TypeIdSupplier pages ids out of User / Photo as it goes",
            dur=4.2,
            body=[code_card(92, ["TypeIdSupplier reads ids back",
                                 "from User / Photo, so the edge",
                                 "tasks trail the writers",
                                 "and pause 5s every 10%"],
                            title="why edges come last", color=PURPLE)]
                 + [worker(k, f"writer {k + 1}", GREEN, frac=0.85) for k in range(5)]
                 + [worker(5, "friendships", PURPLE, frac=0.3, sub="FriendOf x500"),
                    worker(6, "likes", PURPLE, frac=0.2, sub="Likes x500"),
                    node_state(0, "UNDER LOAD", BLUE, "7 tasks in flight"),
                    counters(0, [("users", 0.85, "4250 / 5000"), ("photos", 0.8, "40K / 50000"),
                                 ("friendships", 0.3, "150 / 500"), ("likes", 0.2, "100 / 500")])],
        ),
        Scene(
            step="the main thread just watches",
            caption="While the pool drains, the test thread polls the counts every five seconds and logs them.",
            detail="while (!executor.isTerminated()) { db.printUserStats(); countUsers/countPhotos/countFriendships/countLikes; sleep 5s }",
            dur=3.8,
            body=[worker(k, f"writer {k + 1}", GREEN, frac=1.0) for k in range(5)]
                 + [worker(5, "friendships", PURPLE, frac=0.7), worker(6, "likes", PURPLE, frac=0.6),
                    node_state(0, "UNDER LOAD", BLUE, "polled every 5s"),
                    counters(0, [("users", 1.0, "5000 / 5000"), ("photos", 1.0, "50000 / 50000"),
                                 ("friendships", 0.7, "350 / 500"), ("likes", 0.6, "300 / 500")]),
                    client_to(0, "SELECT count(*) x4", BLUE, packet=True)],
        ),
        Scene(
            step="exact counts, not approximations",
            caption="Nothing here is awaited or approximated: the pool has terminated, so every expected row must exist.",
            detail="assertThatUserCountIs(5000) - PhotoCountIs(50000) - FriendshipCountIs(500) - LikesCountIs(500)",
            dur=3.8,
            body=[code_card(92, ["users        1000 x 5 threads",
                                 "photos       users x 10",
                                 "friendships  500",
                                 "likes        500"],
                            title="expected totals", color=GREEN),
                  node_state(0, "PASS", GREEN, "all four counts exact"),
                  counters(0, [("users", 1.0, "5000 / 5000"), ("photos", 1.0, "50000 / 50000"),
                               ("friendships", 1.0, "500 / 500"), ("likes", 1.0, "500 / 500")]),
                  stamp(0, "asserted")],
        ),
        Scene(
            step="elapsed time and the meters",
            caption="The run ends by dumping Micrometer: per-operation timers and the error counters the wrappers increment.",
            detail="Metrics.globalRegistry.getMeters() - arcadedb.test.inserted.users / .photos timers, .error counters",
            dur=3.6,
            body=[code_card(92, ["total time in minutes",
                                 "arcadedb.test.inserted.users",
                                 "arcadedb.test.inserted.photos",
                                 "...error counters"],
                            title="what gets logged", color=AMBER),
                  node_state(0, "DONE", GREEN, "meters dumped")],
        ),
    ]
    emit("single-server-load", "SingleServerLoadTestIT - singleServerLoadTest(Protocol)",
         "Single server, 5 writer threads plus edge builders, per protocol", scenes,
         "The full single-node load run: users, photos, friendships and likes, asserted exactly.",
         group="Load tests", java=SRC + "SingleServerLoadTestIT.java",
         methods=["singleServerLoadTest"], n=1, workers=7)


# ============================================================================ SingleServerSimpleLoadTestIT
def single_server_simple_load():
    layout(1, workers=3)
    scenes = [
        Scene(
            step="the same test, stripped down",
            caption="The simple variant keeps the protocol matrix but drops the edges: one writer thread, vertices only.",
            detail="numOfThreads = 1, numOfUsers = 1000, numOfPhotos = 10 - no friendships, no likes, no metrics dump",
            dur=3.6,
            body=[code_card(92, ["numOfThreads   1",
                                 "numOfUsers     1000",
                                 "numOfPhotos    10",
                                 "pool sized to numOfThreads"],
                            title="parameters", color=AMBER),
                  node_state(0, "READY", GREEN, "single node")],
        ),
        Scene(
            step="one task, its own connection",
            caption="The pool holds exactly one thread, so this measures a single client stream rather than contention.",
            detail="Executors.newFixedThreadPool(numOfThreads); the task opens its own DatabaseWrapper and closes it at the end",
            dur=3.6,
            body=[worker(0, "writer 1", GREEN, frac=0.5, sub="1000 users x 10"),
                  node_state(0, "UNDER LOAD", BLUE, "one writer"),
                  counters(0, [("users", 0.5, "500 / 1000"), ("photos", 0.5, "5000 / 10000")]),
                  client_to(0, "addUserAndPhotos(1000, 10)", GREEN, packet=True)],
        ),
        Scene(
            step="polled every two seconds",
            caption="The watcher loop is tighter here, and it does not guard the count calls - a failure there fails the test.",
            detail="while (!executor.isTerminated()) { countUsers(); countPhotos(); sleep 2s }   (no try/catch, unlike the full test)",
            dur=3.6,
            body=[worker(0, "writer 1", GREEN, frac=0.9),
                  node_state(0, "UNDER LOAD", BLUE, "polled every 2s"),
                  counters(0, [("users", 0.9, "900 / 1000"), ("photos", 0.9, "9000 / 10000")]),
                  client_to(0, "SELECT count(*)", BLUE, packet=True)],
        ),
        Scene(
            step="two exact assertions",
            caption="Both totals must be exact on each protocol arm, which is what makes this the quickest regression signal.",
            detail="db.assertThatUserCountIs(1000); db.assertThatPhotoCountIs(10000)",
            dur=3.4,
            body=[node_state(0, "PASS", GREEN, "1000 users - 10000 photos"),
                  counters(0, [("users", 1.0, "1000 / 1000"), ("photos", 1.0, "10000 / 10000")]),
                  stamp(0, "asserted")],
        ),
    ]
    emit("single-server-simple-load", "SingleServerSimpleLoadTestIT - singleServerLoadTest(Protocol)",
         "One writer thread, vertices only, both protocols", scenes,
         "The short arm: a single client stream writing users and photos, asserted exactly.",
         group="Load tests", java=SRC + "SingleServerSimpleLoadTestIT.java",
         methods=["singleServerLoadTest"], n=1, workers=3)


# ============================================================================ SingleLocalhostServerSimpleLoadTestIT
def single_localhost_load():
    layout(1, workers=7)
    scenes = [
        Scene(
            step="@Disabled, and on purpose",
            caption="This one drives a server you started yourself, so it is disabled until somebody removes the annotation.",
            detail="new ServerWrapper(\"localhost\", 2481, 50051) - no Testcontainers, no container lifecycle in the test at all",
            dur=4.0,
            body=[code_card(92, ["1. mvn clean install -DskipTests",
                                 "2. bin/server.sh with 16G heap",
                                 "3. drop @Disabled, run the test"],
                            title="how to run it", color=AMBER),
                  node_state(0, "EXTERNAL", GREY, "localhost:2481 / 50051")],
        ),
        Scene(
            step="a hand-tuned server",
            caption="The point is the JVM and engine settings the javadoc prescribes, which a container run would not reproduce.",
            detail="-Xms16G -Xmx16G, arcadedb.serverMetrics=true, typeDefaultBuckets=10, bucketReuseSpaceMode=low",
            dur=3.8,
            body=[code_card(92, ["ARCADEDB_OPTS_MEMORY -Xms16G",
                                 "arcadedb.serverMetrics=true",
                                 "arcadedb.typeDefaultBuckets=10",
                                 "arcadedb.bucketReuseSpaceMode=low"],
                            title="server settings", color=PURPLE),
                  node_state(0, "EXTERNAL", BLUE, "16G heap, metrics on")],
        ),
        Scene(
            step="5 writers, 50 photos per user",
            caption="Five times the photo volume of the containerised run: 5000 users and 250000 photos.",
            detail="numOfThreads = 5, numOfUsers = 1000, numOfPhotos = 50 on a pool of 10; HTTP only, no protocol matrix",
            dur=4.0,
            body=[worker(k, f"writer {k + 1}", GREEN, frac=0.5, sub="1000 users x 50") for k in range(5)]
                 + [node_state(0, "UNDER LOAD", BLUE, "5 writers"),
                    counters(0, [("users", 0.5, "2500 / 5000"), ("photos", 0.5, "125K / 250K"),
                                 ("friendships", 0, "0 / 2000"), ("likes", 0, "0 / 2000")])],
        ),
        Scene(
            step="edges submitted 30s later",
            caption="A fixed 30 second sleep gives the writers a head start, so the edge tasks find ids to attach to.",
            detail="TimeUnit.SECONDS.sleep(30) before submitting createFriendships(2000) and createLike(2000)",
            dur=3.8,
            body=[worker(k, f"writer {k + 1}", GREEN, frac=0.9) for k in range(5)]
                 + [worker(5, "friendships", PURPLE, frac=0.25, sub="after sleep(30)"),
                    worker(6, "likes", PURPLE, frac=0.15, sub="after sleep(30)"),
                    node_state(0, "UNDER LOAD", BLUE, "7 tasks in flight"),
                    counters(0, [("users", 0.9, "4500 / 5000"), ("photos", 0.9, "225K / 250K"),
                                 ("friendships", 0.25, "500 / 2000"), ("likes", 0.15, "300 / 2000")])],
        ),
        Scene(
            step="meters first, assertions last",
            caption="The meter dump comes before the assertions here, so a failed run still leaves the timing numbers behind.",
            detail="Metrics dump, then assertThatUserCountIs(5000) / PhotoCountIs(250000) / FriendshipCountIs(2000) / LikesCountIs(2000)",
            dur=3.8,
            body=[node_state(0, "PASS", GREEN, "four exact counts"),
                  counters(0, [("users", 1.0, "5000 / 5000"), ("photos", 1.0, "250K / 250K"),
                               ("friendships", 1.0, "2000 / 2000"), ("likes", 1.0, "2000 / 2000")]),
                  stamp(0, "asserted")],
        ),
    ]
    emit("single-localhost-load", "SingleLocalhostServerSimpleLoadTestIT - singleServerLoadTest()",
         "A disabled benchmark against a server you start and tune by hand", scenes,
         "The manual benchmark: a 16G server on localhost, 250K photos, edges after a head start.",
         group="Load tests", java=SRC + "SingleLocalhostServerSimpleLoadTestIT.java",
         methods=["singleServerLoadTest"], n=1, workers=7)


# ============================================================================ SingleServerTimeSeriesLoadTestIT
def single_server_timeseries_load():
    layout(1, workers=6)
    scenes = [
        Scene(
            step="three ingestion protocols",
            caption="The time series arm runs three times, once per ingestion path, against the same single server.",
            detail="@EnumSource(TimeSeriesDatabaseWrapper.Protocol.class) -> LINE_PROTOCOL, SQL_HTTP, SQL_GRPC",
            dur=3.8,
            body=[code_card(92, ["LINE_PROTOCOL  batched, 500/POST",
                                 "SQL_HTTP       one INSERT per point",
                                 "SQL_GRPC       one INSERT per point"],
                            title="@EnumSource(Protocol)", color=AMBER),
                  node_state(0, "READY", GREEN, "time series schema")],
        ),
        Scene(
            step="5 sensors, 10000 points each",
            caption="Each thread owns one sensor and a disjoint timestamp range, so the threads never write the same point.",
            detail="base = 1_000_000_000 + threadIndex * 100_000_000; ingestSeries(\"sensor-N\", \"region-\" + N % 3, base, 10000)",
            dur=4.0,
            body=[worker(k, f"sensor-{k}", GREEN, frac=0.55, sub=f"region-{k % 3}") for k in range(5)]
                 + [node_state(0, "INGESTING", BLUE, "5 threads"),
                    counters(0, [("points", 0.55, "27K / 50000"), ("sealed buckets", 0.3, "async")]),
                    client_to(0, "10000 points x 5", GREEN, packet=True)],
        ),
        Scene(
            step="the verification series comes after",
            caption="Four hand-written points are added only once the bulk load is done, so their values are deterministic.",
            detail="ingestVerificationSeries(): sensor VERIFY at ts 0/1000/2000/3000 - expectedTotal = 50000 + 4",
            dur=3.8,
            body=[worker(k, f"sensor-{k}", GREEN, frac=1.0) for k in range(5)]
                 + [worker(5, "verification", PURPLE, frac=1.0, sub="4 points, ts 0..3000"),
                    node_state(0, "INGESTED", GREEN, "50004 points"),
                    counters(0, [("points", 1.0, "50004 / 50004"), ("sealed buckets", 0.8, "async")])],
        ),
        Scene(
            step="what is deterministic, and what is not",
            caption="The total is exact, but a tag-filtered aggregate only sees the asynchronously sealed subset.",
            detail="temperature = 15.0 + (i % 20) cycles 15..34, so the extremes settle even though the aggregated count does not",
            dur=4.2,
            body=[code_card(92, ["count(*)        exact -> 50004",
                                 "aggregate count NOT exact",
                                 "aggregate min/max  15.0 / 34.0",
                                 "/latest         exact -> 3000"],
                            title="sealed vs mutable", color=PURPLE),
                  node_state(0, "QUERYING", BLUE, "three assertions")],
        ),
        Scene(
            step="three assertions of three kinds",
            caption="Total count, aggregate extremes over a sealed sensor, and the latest timestamp read from the mutable buffer.",
            detail="assertThatPointCountIs(50004); assertAggregateExtremes(\"sensor-0\", 15.0, 34.0); latestTimestamp(verify) == 3000",
            dur=3.8,
            body=[node_state(0, "PASS", GREEN, "count, extremes, latest"),
                  counters(0, [("points", 1.0, "50004 / 50004"), ("assertions", 1.0, "3 / 3")]),
                  stamp(0, "asserted")],
        ),
    ]
    emit("single-server-timeseries-load", "SingleServerTimeSeriesLoadTestIT - singleServerTimeSeriesLoadTest(Protocol)",
         "50000 points from 5 threads, three ingestion protocols, then exact and approximate checks", scenes,
         "Time series ingestion under load, and which of its query results are deterministic.",
         group="Load tests", java=SRC + "SingleServerTimeSeriesLoadTestIT.java",
         methods=["singleServerTimeSeriesLoadTest"], n=1, workers=6)


# ============================================================================ ThreeNodesLoadTestIT
def three_nodes_load():
    layout(3, workers=5)
    scenes = [
        Scene(
            step="3-node Raft cluster, leader found",
            caption="Load against HA: the cluster is started, a leader is awaited, and the schema is created on that leader.",
            detail="startCluster(); waitForRaftLeader(servers, 10); leaderDb.createDatabase(); createSchema(withMaterializedView)",
            dur=3.8,
            body=[node_state(0, "LEADER", GREEN, "schema created"),
                  node_state(1, "FOLLOWER", BLUE), node_state(2, "FOLLOWER", BLUE),
                  raft_link(0, 1, "schema entry"), raft_link(1, 2, "")],
        ),
        Scene(
            step="awaitSchema on all three",
            caption="Every node must answer the schema query before any load starts, and the wait is measured, not slept.",
            detail="db1/db2/db3.awaitSchema(60) - plus awaitMaterializedView(60) in the view arm - elapsed ms is logged",
            dur=3.6,
            body=[node_state(i, "READY", GREEN, "schema readable") for i in range(3)]
                 + [stamp(i, "ok") for i in range(3)],
        ),
        Scene(
            step="3 writers, all on the leader",
            caption="Every writer targets the leader directly, so the run measures replication rather than request forwarding.",
            detail="3 x addUserAndPhotos(1000, 10) on leaderServer; friendships and likes are configured to 0 in this test",
            dur=4.0,
            body=[worker(k, f"writer {k + 1}", GREEN, frac=0.6, sub="1000 users x 10") for k in range(3)]
                 + [worker(3, "friendships", GREY, frac=0.0, sub="numOfFriendship = 0"),
                    worker(4, "likes", GREY, frac=0.0, sub="numOfLike = 0"),
                    node_state(0, "LEADER", BLUE, "3 writers"),
                    node_state(1, "FOLLOWER", BLUE, "applying"), node_state(2, "FOLLOWER", BLUE, "applying"),
                    node_progress(0, 0.6), node_progress(1, 0.5, BLUE), node_progress(2, 0.45, BLUE),
                    client_to(0, "3000 users x 11 tx", GREEN, packet=True),
                    raft_link(0, 1, "replicate"), raft_link(1, 2, "")],
        ),
        Scene(
            step="watch all three while writing",
            caption="The polling loop reads all three nodes at once, so divergence shows up in the log as it happens.",
            detail="while (!executor.isTerminated()) { users1/2/3, photos1/2/3; sleep 5s }",
            dur=3.6,
            body=[worker(k, f"writer {k + 1}", GREEN, frac=1.0) for k in range(3)]
                 + [node_state(i, "CONVERGING", BLUE, "3000 users") for i in range(3)]
                 + [client_to(i, "count(*)" if i == 0 else "", BLUE, packet=True) for i in range(3)],
        ),
        Scene(
            step="converge, then assert per node",
            caption="Convergence is awaited on equality, then each node is asserted against the absolute expected totals.",
            detail="awaitConvergence(...) atMost 2 min, then per node: users 3000, photos 30000, friendships 0, likes 0",
            dur=4.0,
            body=[node_state(i, "PASS", GREEN, "3000 users - 30000 photos") for i in range(3)]
                 + [stamp(i, "exact") for i in range(3)],
        ),
        Scene(
            step="count by scan as well as by index",
            caption="Every node is counted a second time by scanning the type, because an index and a scan can disagree.",
            detail="assertThatScannedCountIs(\"User\", 3000) and (\"Photo\", 30000) on db1, db2 and db3",
            dur=3.8,
            body=[code_card(92, ["count(*) uses the index",
                                 "a full scan reads the buckets",
                                 "both must give the same number",
                                 "on every node"],
                            title="two ways to count", color=PURPLE)]
                 + [node_state(i, "PASS", GREEN, "scan == index") for i in range(3)],
        ),
        Scene(
            step="the materialized-view arm (#5492)",
            caption="A second method repeats the identical run with a REFRESH INCREMENTAL view, the only difference between the arms.",
            detail="threeNodeReplicationWithMaterializedView() is @Tag(\"slow\"), HTTP only; the view must converge non-empty on every node",
            dur=4.2,
            body=[variants_card([
                "threeNodeReplicationWithMaterializedView",
                "  same load + a REFRESH INCREMENTAL",
                "  view over User, HTTP only",
                "  #5492: 2041/3000 writes, resync loop",
                "  view stood at 3000 / 3000 / 0",
                "  asserts equal AND non-empty, not",
                "  equal to the user count",
            ])] + [node_state(i, "VIEW", PURPLE, "UserStats equal") for i in range(3)],
        ),
        Scene(
            step="log markers, not just row counts",
            caption="Container logs are scanned afterwards: counts alone cannot tell a clean run from one that diverged and resynced.",
            detail="counts \"does not match with existent version\", \"triggering snapshot resync\", \"Snapshot resync completed\" + a canary marker",
            dur=4.0,
            body=[code_card(92, ["record counts cannot distinguish",
                                 "never diverged   from",
                                 "diverged and resynced back",
                                 "-> count the log markers too"],
                            title="#5492 signatures", color=AMBER)]
                 + [node_state(i, "SCANNED", BLUE, "logs dumped") for i in range(3)],
        ),
    ]
    emit("three-nodes-load", "ThreeNodesLoadTestIT - threeNodeReplication(Protocol)",
         "Load against a 3-node Raft cluster, with the #5492 materialized-view A/B", scenes,
         "HA under load: writers on the leader, convergence, per-node exact counts and log signatures.",
         group="Load tests", java=SRC + "ThreeNodesLoadTestIT.java",
         methods=["threeNodeReplication", "threeNodeReplicationWithMaterializedView"], workers=5)


# ============================================================================ ThreeNodesTimeSeriesLoadTestIT
def three_nodes_timeseries_load():
    layout(3, workers=4)
    scenes = [
        Scene(
            step="ingest on one protocol, read on HTTP",
            caption="The ingest connection varies with the protocol matrix; the three readers are always plain HTTP, one per node.",
            detail="ingest = protocol against node 0; r0/r1/r2 = Protocol.SQL_HTTP, one per node, all in one try-with-resources",
            dur=4.0,
            body=[code_card(92, ["ingest   LINE / SQL_HTTP / SQL_GRPC",
                                 "readers  SQL_HTTP, one per node",
                                 "writes auto-forward to the leader",
                                 "all four closed by try-with-res."],
                            title="four connections", color=AMBER),
                  node_state(0, "INGEST + READ", GREEN), node_state(1, "READ", BLUE),
                  node_state(2, "READ", BLUE)],
        ),
        Scene(
            step="schema on every node first",
            caption="The schema is created through the ingest connection and checked on all three readers before any point is written.",
            detail="ingest.createDatabase(); ingest.createSchema(); r0.checkSchema(); r1.checkSchema(); r2.checkSchema()",
            dur=3.6,
            body=[node_state(i, "READY", GREEN, "schema ok") for i in range(3)]
                 + [stamp(i, "schema") for i in range(3)],
        ),
        Scene(
            step="3 sensors, 10000 points each",
            caption="Three threads ingest against node 0 while the writes replicate; 30000 points plus 4 verification points.",
            detail="base = 1_000_000_000 + threadIndex * 100_000_000; ingestSeries(\"sensor-N\", \"region-N\", base, 10000)",
            dur=4.0,
            body=[worker(k, f"sensor-{k}", GREEN, frac=0.6, sub="10000 points") for k in range(3)]
                 + [worker(3, "verification", PURPLE, frac=0.0, sub="after the bulk"),
                    node_state(0, "INGESTING", BLUE, "18K points"),
                    node_state(1, "REPLICATING", BLUE), node_state(2, "REPLICATING", BLUE),
                    node_progress(0, 0.6), node_progress(1, 0.5, BLUE), node_progress(2, 0.45, BLUE),
                    raft_link(0, 1, "replicate"), raft_link(1, 2, "")],
        ),
        Scene(
            step="poll all three, then converge",
            caption="Counts are logged from all three readers during the load, then awaited until each reaches the exact total.",
            detail="until(c0 == 30004 && c1 == 30004 && c2 == 30004) atMost 2 minutes, pollInterval 5s; RemoteException is retried",
            dur=4.0,
            body=[worker(k, f"sensor-{k}", GREEN, frac=1.0) for k in range(3)]
                 + [worker(3, "verification", PURPLE, frac=1.0, sub="ts 0..3000"),
                    node_state(0, "CONVERGED", GREEN, "30004"),
                    node_state(1, "CONVERGED", GREEN, "30004"), node_state(2, "CONVERGED", GREEN, "30004")],
        ),
        Scene(
            step="every assertion, on every node",
            caption="Each reader repeats the full single-server check, so a node that replicated only part of the data fails.",
            detail="per node: assertThatPointCountIs(30004); assertAggregateExtremes(\"sensor-0\", 15.0, 34.0); latest(verify) == 3000",
            dur=4.0,
            body=[node_state(i, "PASS", GREEN, "count, extremes, latest") for i in range(3)]
                 + [stamp(i, "3 checks") for i in range(3)],
        ),
    ]
    emit("three-nodes-timeseries-load", "ThreeNodesTimeSeriesLoadTestIT - threeNodeTimeSeriesReplication(Protocol)",
         "Time series ingestion on a 3-node Raft cluster, verified on every node", scenes,
         "TS load plus replication: ingest on one protocol, read back on HTTP from all three nodes.",
         group="Load tests", java=SRC + "ThreeNodesTimeSeriesLoadTestIT.java",
         methods=["threeNodeTimeSeriesReplication"], workers=4)


ALL = (single_server_load, single_server_simple_load, single_localhost_load,
       single_server_timeseries_load, three_nodes_load, three_nodes_timeseries_load)
