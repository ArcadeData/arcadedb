/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.monitor;

import java.util.List;

/**
 * Framework-agnostic source of High-Availability replication health, implemented by the HA server
 * plugin and translated into Micrometer gauges by {@link HAReplicationMetrics}. Kept in the server
 * module (which has no compile dependency on the ha-raft module) so the binder can discover the HA
 * plugin via {@code instanceof} without coupling to Raft types, mirroring how {@link PoolMetrics}
 * translates the engine's framework-agnostic {@code PoolStats} records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface HAReplicationStatsProvider {
  /**
   * Snapshot of the leader's view of replication health.
   *
   * @param leader                    whether this node is currently the Raft leader (the only role
   *                                  for which the follower fields are meaningful)
   * @param maxFollowerLastContactMs  worst (largest) time, in milliseconds, since the leader last
   *                                  successfully exchanged an RPC with any follower. This is the
   *                                  leading indicator of imminent election churn: when it approaches
   *                                  {@code arcadedb.ha.electionTimeoutMin}, a follower is about to
   *                                  start a new election. {@code -1} when not leader or unknown.
   * @param maxFollowerReplicationLag worst (largest) number of committed entries a follower is behind
   *                                  the leader's commit index. {@code -1} when not leader or unknown.
   * @param trackedFollowers          number of followers the leader is currently tracking
   *                                  ({@code 0} when not leader)
   */
  record HAReplicationStats(boolean leader, long maxFollowerLastContactMs, long maxFollowerReplicationLag,
                            int trackedFollowers) {
  }

  /**
   * Per-follower replication health as seen by the leader (issue #4812), used for per-peer metrics,
   * the cluster JSON, and the lagging-follower alert so a constantly-slow node can be pinpointed.
   *
   * @param peerId          the follower's Raft peer id
   * @param matchIndex      the highest log index the leader knows the follower has persisted
   * @param nextIndex       the next log index the leader will send the follower
   * @param replicationLag  committed entries the follower is behind the leader ({@code commit - match})
   * @param lastContactMs   ms since the leader last successfully exchanged an RPC with the follower
   * @param status          classified status: HEALTHY / CATCHING_UP / FALLING_BEHIND / STALLED / UNKNOWN
   * @param laggingForMs    ms the follower has been continuously non-HEALTHY (0 when healthy/unknown)
   */
  record FollowerSample(String peerId, long matchIndex, long nextIndex, long replicationLag,
                        long lastContactMs, String status, long laggingForMs) {
  }

  /**
   * In-flight leader-side phase-2 applies, which hold the Raft snapshot checkpoint back so an entry
   * that is Raft-committed but not yet written locally stays replayable (issue #5407). A ticket that
   * stays held pins log compaction until the node restarts, so this is the signal that explains a
   * Raft log which stops shrinking (issues #5410, #5345).
   *
   * @param pending           number of phase-2 applies currently holding the checkpoint back
   * @param oldestHeldMs      how long (ms) the oldest of them has been held ({@code 0} when none)
   * @param lowestReplayFloor the Raft index past which the log cannot be purged while these are
   *                          held ({@code -1} when none)
   */
  record PendingPhase2Stats(int pending, long oldestHeldMs, long lowestReplayFloor) {
  }

  /**
   * What one database's schema sessions have cost in instalment round trips (issue #6144). A schema session that
   * buffers more WAL than one Raft entry can carry - principally an index rebuild, including the one
   * {@code CHECK DATABASE FIX} performs - ships it in instalments, and each instalment is a quorum round trip taken
   * WHILE THE DATABASE WRITE LOCK IS HELD. That is an accepted trade against the leader holding a whole rebuilt
   * index in heap, but until this was exported the only signal was a detailed-level HA log line, which an operator
   * debugging "every write on this database stalled for a while" could only enable by reproducing the stall.
   * <p>
   * Per database rather than per node: the counters mean nothing aggregated over a multi-database server, where
   * "which database stalled its writers" is the question.
   *
   * @param database     the database these counters describe
   * @param instalments  instalments shipped since the database was opened
   * @param totalTimeMs  total milliseconds spent inside those round trips - the write-lock time they added
   * @param maxTimeMs    the longest single instalment, which is what separates many fast round trips from a few
   *                     that each waited on a slow quorum member
   */
  record SchemaInstalmentSample(String database, long instalments, long totalTimeMs, long maxTimeMs) {
  }

  /**
   * Unreferenced paginated files this node holds, per database (issue #6143). A schema session that ships
   * instalments and then loses leadership cannot send its own compensating removal, so the files its instalments
   * created stay on the other nodes with nothing referencing them. Nothing reads them - the impact is wasted disk -
   * but until this was exported an operator could only find them by reading the data directory by hand, and only
   * the node that ran the session logs anything at all.
   *
   * @param database          the database these files belong to
   * @param unreferencedFiles how many files this node holds that no schema component claims
   */
  record UnreferencedFilesSample(String database, long unreferencedFiles) {
  }

  /**
   * Returns a live snapshot of replication health. Called on each metrics scrape, so implementations
   * must be cheap and non-blocking.
   */
  HAReplicationStats getHAReplicationStats();

  /**
   * Returns the in-flight phase-2 hold state. Cheap and non-blocking - it scans a map sized by
   * concurrent commits. Defaults to "nothing held" for implementations without a Raft state machine.
   */
  default PendingPhase2Stats getPendingPhase2Stats() {
    return new PendingPhase2Stats(0, 0, -1);
  }

  /**
   * Returns a per-follower health sample (leader only; empty otherwise). Cheap and non-blocking - it
   * reads the leader's already-maintained replication bookkeeping.
   */
  default List<FollowerSample> getFollowerSamples() {
    return List.of();
  }

  /**
   * Returns one sample per open replicated database (issue #6144), or empty when HA is disabled. Cheap and
   * non-blocking - it reads counters the schema path already maintains. Unlike the follower samples this is
   * meaningful on every node: a node that was leader when it shipped instalments keeps its counters afterwards.
   */
  default List<SchemaInstalmentSample> getSchemaInstalmentSamples() {
    return List.of();
  }

  /**
   * Returns one sample per open database naming how many paginated files this node holds that no schema component
   * claims (issue #6143), or empty when HA is disabled. Walks the file list, so it is O(files) per call and is
   * refreshed on a timer rather than on every scrape.
   */
  default List<UnreferencedFilesSample> getUnreferencedFilesSamples() {
    return List.of();
  }
}
