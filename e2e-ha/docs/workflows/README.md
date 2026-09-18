# ArcadeDB HA, animated

Looping SVGs in two groups: the **Raft protocol** as this codebase implements it, and one **test
workflow** per integration test in `e2e-ha`. In every diagram the left rail is the sequence, the
stage is the cluster, and the bottom bar carries the contract. No scripts and no external fonts, so
each one renders in GitHub markdown, an IDE preview and a browser.

**Start here: [`index.html`](index.html)** - a local page that lists every diagram and switches
between them. Open it with `open index.html` (any browser; a plain `file://` URL is enough).

A browser is the only reliable viewer: the animation is SMIL, which macOS Preview, Quick Look and
the IntelliJ SVG preview render as a blank stage because they only ever draw frame 0.

## Raft protocol

These explain the algorithm, not the tests. Every number, entry type, setting and endpoint on them
comes from `ha-raft/` and `GlobalConfiguration`, not from the Raft paper.

| Diagram | Covers | Grounded in |
|---|---|---|
| [raft-leader-election](raft-leader-election.svg) | randomised timeouts, RequestVote, one vote per term, heartbeats, split votes | `RaftPropertiesBuilder`, `GetClusterHandler` |
| [raft-log-replication](raft-log-replication.svg) | append, AppendEntries, commit on a majority, apply, backfill, batching | `RaftLogEntryType`, `RaftTransactionBroker`, `ArcadeStateMachine` |
| [raft-quorum-and-partitions](raft-quorum-and-partitions.svg) | majority overlap, step-down, minority refusal, truncate-and-replay | `Quorum`, `RaftHAServer`; exercised by `SplitBrainIT` |
| [raft-in-arcadedb](raft-in-arcadedb.svg) | ports, the 8 entry types, leader forwarding, snapshots, tuning knobs | `ha-raft/`, `GlobalConfiguration`, `LeaderCommandForwarder` |

## Test workflows

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
the index page, `raft_protocol.py` the four protocol explainers, and `build_all.py` the test-workflow
scenes - one function per test class:

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
`code_card(y, lines, title=)`, `variants_card(lines)`, `chip(x, y, label)`.
Scene bodies are plain SVG strings drawn in order, so put cards first and arrows last.
