/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Monitors replication lag per replica in a Raft cluster.
 * Tracks the difference between the leader's commit index and each replica's match index,
 * emitting warnings when the lag exceeds a configurable threshold.
 */
public class ClusterMonitor {

  private final    long                            lagWarningThreshold;
  private volatile long                            leaderCommitIndex;
  private final    ConcurrentHashMap<String, Long> replicaMatchIndexes = new ConcurrentHashMap<>();

  public ClusterMonitor(final long lagWarningThreshold) {
    this.lagWarningThreshold = lagWarningThreshold;
  }

  public void updateLeaderCommitIndex(final long commitIndex) {
    this.leaderCommitIndex = commitIndex;
  }

  public void updateReplicaMatchIndex(final String replicaId, final long matchIndex) {
    replicaMatchIndexes.put(replicaId, matchIndex);

    final long lag = leaderCommitIndex - matchIndex;
    if (lag > lagWarningThreshold)
      LogManager.instance().log(this, Level.WARNING, "Replica '%s' is lagging behind by %d entries (threshold: %d)",
          replicaId, lag, lagWarningThreshold);
  }

  public void removeReplica(final String replicaId) {
    replicaMatchIndexes.remove(replicaId);
  }

  public Map<String, Long> getReplicaLags() {
    if (replicaMatchIndexes.isEmpty())
      return Collections.emptyMap();

    final Map<String, Long> lags = new ConcurrentHashMap<>();
    for (final Map.Entry<String, Long> entry : replicaMatchIndexes.entrySet())
      lags.put(entry.getKey(), leaderCommitIndex - entry.getValue());
    return lags;
  }

  public boolean isReplicaLagging(final String replicaId) {
    final Long matchIndex = replicaMatchIndexes.get(replicaId);
    if (matchIndex == null)
      return false;
    return (leaderCommitIndex - matchIndex) > lagWarningThreshold;
  }

  public long getLeaderCommitIndex() {
    return leaderCommitIndex;
  }

  public long getLagWarningThreshold() {
    return lagWarningThreshold;
  }
}
