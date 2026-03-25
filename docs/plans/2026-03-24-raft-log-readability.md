# Raft Log Readability Design

**Goal:** Replace the cryptic Ratis internal log output with clean, operator-friendly cluster event messages using ArcadeDB server names and HTTP addresses.

**Problem:** Ratis logs verbose election protocol chatter (`peer-0@group-D1CE6477116E-LeaderElection36: PRE_VOTE REJECTED...`) that is unreadable for operators. Meanwhile, actual cluster events (leadership changes, role transitions) are not logged at all.

---

## Approach

Two complementary changes:
1. **Suppress Ratis noise** — Set all `org.apache.ratis.*` JUL loggers to WARNING at startup.
2. **Add ArcadeDB-level cluster event logging** — Use Ratis' `notifyLeaderChanged()` callback to log clean messages with server names.

## Events to Log

| Event | Level | Example |
|-------|-------|---------|
| Leadership change | INFO | `Leader elected: node-1 (localhost:2480)` |
| This node's role | INFO | `This node is now LEADER` / `This node is now REPLICA (leader: node-1 (localhost:2480))` |
| Cluster startup | INFO | `Raft cluster joined: 3 nodes [node-1 (localhost:2480), node-2 (localhost:2481), node-3 (localhost:2482)]` |
| Replica lagging | WARNING | `Replica node-2 (localhost:2481) lagging by 150 entries (threshold: 100)` |
| State machine errors | SEVERE | Already logged, no changes |

## Node Display Names

Format: `serverName (host:httpPort)` — e.g., `node-1 (localhost:2480)`.

`RaftHAServer` maintains a `Map<RaftPeerId, String>` built at startup from the cluster peer configuration. The peer ID in Ratis is the server name (already set in `buildPeerList()`), the HTTP address comes from configuration. Exposed via `getPeerDisplayName(RaftPeerId)` with fallback to `peerId.toString()`.

## Leadership Change Detection

Override `BaseStateMachine.notifyLeaderChanged(RaftGroupMemberId, RaftPeerId)` in `ArcadeStateMachine`. Ratis calls this on every leadership transition — zero latency, no polling needed.

`ArcadeStateMachine` receives a reference to `RaftHAServer` (set after construction) to resolve peer IDs to display names.

## Ratis Log Suppression

In `RaftHAServer.start()`, before starting the Ratis server:
```java
java.util.logging.Logger.getLogger("org.apache.ratis").setLevel(Level.WARNING);
```

Covers all sub-loggers. Operators can temporarily lower the level for debugging.

## Replica Lag Monitoring

`ClusterMonitor` already exists with full lag detection API but is not wired in. When this node becomes leader (detected in `notifyLeaderChanged()`), start a scheduled task that queries `RaftServer.Division.getInfo()` for follower commit info and feeds it to `ClusterMonitor`. Cancel the task when leadership is lost.

## Files Changed

- `ArcadeStateMachine.java` — add `notifyLeaderChanged()` override, add `RaftHAServer` reference
- `RaftHAServer.java` — add `getPeerDisplayName()`, peer display map, suppress Ratis loggers, enhance startup log message, add scheduled lag monitor task
- `ClusterMonitor.java` — no changes needed
