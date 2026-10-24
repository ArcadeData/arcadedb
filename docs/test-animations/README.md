# ArcadeDB test animations

Looping SVGs in three groups: the **Raft protocol** as this codebase implements it, one **test
workflow** per integration test in `e2e-ha`, and one per **load test** in `load-tests`. In every
diagram the left rail is the sequence, the stage is the cluster, and the bottom bar carries the
contract. No scripts and no external fonts, so each one renders in GitHub markdown, an IDE preview
and a browser.

The two suites share one generator, and `e2e-ha` takes its fixtures (`ContainersTestTemplate`,
`DatabaseWrapper`) from `load-tests`, so the diagrams live here rather than being duplicated under
each module.

**Start here: [`index.html`](index.html)** - a local page that lists every diagram and switches
between them. Open it with `open index.html` (any browser; a plain `file://` URL is enough).

A browser is the only reliable viewer: the animation is SMIL, which macOS Preview, Quick Look and
the IntelliJ SVG preview render as a blank stage because they only ever draw frame 0.

## Pausing, stepping and scrubbing

Every diagram carries its own transport bar along the bottom:

| Control | Does |
|---|---|
| play / pause button, <kbd>space</kbd> or <kbd>k</kbd> | freezes the animation where it is |
| prev / next buttons, <kbd>&larr;</kbd> <kbd>&rarr;</kbd> | jumps to the previous or next scene and pauses there |
| the track | click or drag anywhere to scrub; the ticks are the scene boundaries |
| the readout | elapsed time and which scene is on screen |

The controls live inside the SVG, so they work in `index.html` **and** when a single `.svg` is opened
straight from the file system (`open raft-log-replication.svg`). The keyboard shortcuts need the
diagram to have focus, so click it once first.

Two places where the diagrams stay non-interactive, by design: a GitHub markdown preview and this
README render them through `<img>`, which never runs the embedded script. The progress thumb is
animated with SMIL rather than by the script, so it still tracks the animation there - what is lost
is only the ability to stop it.

## Raft protocol

These explain the algorithm, not the tests. Every number, entry type, setting and endpoint on them
comes from `ha-raft/` and `GlobalConfiguration`, not from the Raft paper.

| Diagram | Covers | Grounded in |
|---|---|---|
| [raft-leader-election](raft-leader-election.svg) | randomised timeouts, RequestVote, one vote per term, heartbeats, split votes | `RaftPropertiesBuilder`, `GetClusterHandler` |
| [raft-log-replication](raft-log-replication.svg) | append, AppendEntries, commit on a majority, apply, backfill, batching | `RaftLogEntryType`, `RaftTransactionBroker`, `ArcadeStateMachine` |
| [raft-quorum-and-partitions](raft-quorum-and-partitions.svg) | majority overlap, step-down, minority refusal, truncate-and-replay | `Quorum`, `RaftHAServer`; exercised by `SplitBrainIT` |
| [raft-in-arcadedb](raft-in-arcadedb.svg) | ports, the 8 entry types, leader forwarding, snapshots, tuning knobs | `ha-raft/`, `GlobalConfiguration`, `LeaderCommandForwarder` |

## Load tests

`load-tests/src/test/java/com/arcadedb/test/load/`. These runs are parameterized over a protocol
enum and driven from a thread pool, so the stage is a worker pool on the left and live counters
inside the server.

| Diagram | Test class | Shape |
|---|---|---|
| [single-server-load](single-server-load.svg) | `SingleServerLoadTestIT` | 5 writers + friendships + likes, per protocol, exact counts |
| [single-server-simple-load](single-server-simple-load.svg) | `SingleServerSimpleLoadTestIT` | 1 writer, vertices only, the quickest arm |
| [single-localhost-load](single-localhost-load.svg) | `SingleLocalhostServerSimpleLoadTestIT` | `@Disabled` benchmark against a hand-tuned local server |
| [single-server-timeseries-load](single-server-timeseries-load.svg) | `SingleServerTimeSeriesLoadTestIT` | 50000 points, 3 ingestion protocols, sealed vs mutable reads |
| [three-nodes-load](three-nodes-load.svg) | `ThreeNodesLoadTestIT` | load on a Raft cluster, plus the #5492 materialized-view A/B |
| [three-nodes-timeseries-load](three-nodes-timeseries-load.svg) | `ThreeNodesTimeSeriesLoadTestIT` | TS ingestion replicated, every assertion on every node |

## Test workflows (e2e-ha)

| Diagram | Test class | @Test methods |
|---|---|---|
| [simple-ha-scenario](simple-ha-scenario.svg) | `SimpleHaScenarioIT` | 1 |
| [three-instances-scenario](three-instances-scenario.svg) | `ThreeInstancesScenarioIT` | 1 |
| [load-three-instances-scenario](load-three-instances-scenario.svg) | `LoadThreeInstancesScenarioIT` | 2 |
| [drop-database-scenario](drop-database-scenario.svg) | `DropDatabaseScenarioIT` | 1 |
| [import-database-scenario](import-database-scenario.svg) | `ImportDatabaseScenarioIT` | 1 |
| [restore-database-scenario](restore-database-scenario.svg) | `RestoreDatabaseScenarioIT` | 1 |
| [user-management-scenario](user-management-scenario.svg) | `UserManagementScenarioIT` | 1 |
| [user-seed-on-peer-add-scenario](user-seed-on-peer-add-scenario.svg) | `UserSeedOnPeerAddScenarioIT` | 1 |
| [leader-failover](leader-failover.svg) | `LeaderFailoverIT` | 3 |
| [rolling-restart](rolling-restart.svg) | `RollingRestartIT` | 3 |
| [network-partition](network-partition.svg) | `NetworkPartitionIT` | 3 |
| [network-partition-recovery](network-partition-recovery.svg) | `NetworkPartitionRecoveryIT` | 3 |
| [split-brain](split-brain.svg) | `SplitBrainIT` | 4 |
| [network-delay](network-delay.svg) | `NetworkDelayIT` | 4 |
| [packet-loss](packet-loss.svg) | `PacketLossIT` | 5 |

A class with several `@Test` methods is drawn from its primary method; the siblings are listed on a
variants card inside the animation, so no method is silently dropped.

## Regenerating

```bash
cd e2e-ha/docs/workflows
python3 build_all.py      # rewrites every *.svg, then index.html
```

No dependencies beyond a Python 3 interpreter. The build prints a warning for any caption, detail or
step label too long for the panel it is drawn in - text that overflows is a silent defect in a
generated image, so treat those warnings as failures.

## Adding or editing a diagram

`workflow_svg.py` holds the stage geometry and the primitives, `catalog.py` the diagram registry and
the index page, `raft_protocol.py` the four protocol explainers, `load_tests.py` the load-test
scenes, and `build_all.py` the e2e-ha scenes plus the entry point - one function per test class:

```python
layout(3)                                  # 2- or 3-node stage; layout(2, proxy=True) for toxiproxy
scenes = boot(3) + [                       # boot() = containers, start, election, database + schema
    Scene(step="...", caption="...", detail="...", dur=3.4,
          body=[node_state(0, "LEADER", GREEN, "writing"), raft_link(0, 1, "replicate")]),
]
emit("my-slug", "MyIT - myTest()", "subtitle", scenes, "one-line blurb",
     java="MyIT.java", methods=["myTest"])
```

Primitives: `node_state(i, badge, color, sub)`, `node_off(i)`, `isolated(i)`, `node_progress(i, frac)`,
`stamp(i, verdict)` / `cross_stamp(i, verdict)`, `client_to(i, label)`, `raft_link(i, j, label)`,
`cut(i, j)`, `toxic(i, label)`, `log_strip(i, cells, committed=)`, `node_to_client(i, label)`,
`worker(k, label, frac=)`, `counters(i, rows)`, `code_card(y, lines, title=)`, `variants_card(lines)`,
`chip(x, y, label)`. `layout(n, proxy=, workers=)` picks a 1-, 2- or 3-node stage; `workers=k`
replaces the JUnit box with a `k`-row ExecutorService panel.
Scene bodies are plain SVG strings drawn in order, so put cards first and arrows last.
