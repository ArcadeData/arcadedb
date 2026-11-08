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

package com.arcadedb.containers.ha.chaos;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Records every cluster operation as {@code "op:node"} so tests can assert on the sequence of actions.
 */
final class FakeNodeControl implements NodeControl {
  final List<String>     calls            = new ArrayList<>();
  int                    leader           = 0;
  boolean                leaderAvailable  = true;
  RuntimeException       failure;
  /** Reported by {@link #unexpectedExit} once {@code checksBeforeExit} checks have passed. */
  String                 unexpectedExit;
  /** Reported by {@link #outOfMemory()} once {@code checksBeforeOutOfMemory} checks have passed. */
  String                 outOfMemory;
  int                    checksBeforeOutOfMemory;
  /** Returned by {@link #leaderView()}. */
  String                 leaderView       = "";
  int                    checksBeforeExit;
  /** In-place restarts per node, reported by {@link #inPlaceRestarts}. */
  final int[]            recovered        = new int[5];
  final int[]            reformatted      = new int[5];
  /** An unpaused node restarts Ratis in place reformatting (or recovering) its storage. */
  boolean                reformatOnUnpause;
  /** A started node reformats its Raft storage while it comes back up, as the new server process counts it. */
  boolean                reformatOnStart;
  /** Like {@link #reformatOnStart}, for one node only (-1 for none). */
  int                    reformatOnStartOfNode = -1;
  boolean                recoverOnUnpause;
  /** An unpaused node's server process restarts: its counts drop to zero. */
  boolean                processRestartsOnUnpause;
  /** A reconnected node's server process restarts too, which no fault of that kind performs. */
  boolean                processRestartsOnReconnect;
  /** After an unpause, reading the in-place restart counts fails, as for a node that no longer answers HTTP. */
  boolean                countsUnreadableAfterUnpause;
  boolean                restartCountsUnreadable;
  /** Applied-index lag per node behind the leader, reported by {@link #raftPositions()} for {@code readsBeforeCaughtUp} reads. */
  final long[]           lag              = new long[5];
  int                    readsBeforeCaughtUp = Integer.MAX_VALUE;
  /** Node whose position is reported as unreadable (-1 for none). */
  int                    unreadablePositionOfNode = -1;
  int                    positionReads;
  /** Time one {@link #findLeader()} takes while positions are being polled, and how many lookups that happened. */
  long                   findLeaderMillis;
  int                    leaderLookupsDuringWait;
  /** Time one {@link #raftPositions()} read takes, as for nodes that answer slowly. */
  long                   positionReadMillis;
  /** Leader reported by {@link #findLeader()} is -1 from this read on, as during a leader change. */
  int                    noLeaderFromRead = Integer.MAX_VALUE;

  private void log(final String call) {
    if (failure != null)
      throw failure;
    calls.add(call);
  }

  @Override
  public void kill(final int node) {
    log("kill:" + node);
    // The counts live in the server process: a new process starts them from zero
    recovered[node] = 0;
    reformatted[node] = 0;
  }

  @Override
  public void stopGracefully(final int node) {
    log("stop:" + node);
    recovered[node] = 0;
    reformatted[node] = 0;
  }

  @Override
  public void start(final int node) {
    log("start:" + node);
    if (reformatOnStart || reformatOnStartOfNode == node)
      ++reformatted[node];
  }

  @Override
  public void pause(final int node) {
    log("pause:" + node);
  }

  @Override
  public void unpause(final int node) {
    log("unpause:" + node);
    if (reformatOnUnpause)
      ++reformatted[node];
    if (recoverOnUnpause)
      ++recovered[node];
    if (processRestartsOnUnpause) {
      recovered[node] = 0;
      reformatted[node] = 0;
    }
    if (countsUnreadableAfterUnpause)
      restartCountsUnreadable = true;
  }

  @Override
  public void disconnect(final int node) {
    log("disconnect:" + node);
  }

  @Override
  public void reconnect(final int node) {
    log("reconnect:" + node);
    // A server process restart nobody asked for, as an OOM kill and a restart policy would make one
    if (processRestartsOnReconnect) {
      recovered[node] = 0;
      reformatted[node] = 0;
    }
  }

  @Override
  public void addLatency(final int node, final int latencyMs, final int jitterMs) {
    log("latency:" + node + ":" + latencyMs + ":" + jitterMs);
  }

  @Override
  public void addLoss(final int node, final float toxicity) {
    log("loss:" + node + ":" + toxicity);
  }

  @Override
  public void clearToxics(final int node) {
    log("clearToxics:" + node);
  }

  @Override
  public int findLeader() {
    if (positionReads > 0) {
      ++leaderLookupsDuringWait;
      if (findLeaderMillis > 0)
        try {
          Thread.sleep(findLeaderMillis);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
    }
    return leaderAvailable && positionReads < noLeaderFromRead ? leader : -1;
  }

  @Override
  public boolean awaitLeader(final Duration timeout) {
    calls.add("awaitLeader");
    return leaderAvailable;
  }

  @Override
  public RaftPosition[] raftPositions() {
    ++positionReads;
    if (positionReadMillis > 0)
      try {
        Thread.sleep(positionReadMillis);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    final boolean lagging = readsBeforeCaughtUp-- > 0;
    final RaftPosition[] positions = new RaftPosition[5];
    for (int node = 0; node < positions.length; node++)
      positions[node] = node == unreadablePositionOfNode ? null : new RaftPosition(1_000_000 - (lagging ? lag[node] : 0), 1_000_000);
    return positions;
  }

  @Override
  public InPlaceRestarts inPlaceRestarts(final int node) {
    if (restartCountsUnreadable)
      throw new ChaosFailure(ResultKind.HARNESS, "Could not read the in-place restart counts of node " + node);
    return new InPlaceRestarts(recovered[node], reformatted[node]);
  }

  @Override
  public String outOfMemory() {
    if (outOfMemory == null)
      return null;
    if (checksBeforeOutOfMemory > 0) {
      --checksBeforeOutOfMemory;
      return null;
    }
    return outOfMemory;
  }

  @Override
  public String leaderView() {
    return leaderView;
  }

  @Override
  public String unexpectedExit(final ClusterState state) {
    if (unexpectedExit == null)
      return null;
    if (checksBeforeExit > 0) {
      --checksBeforeExit;
      return null;
    }
    return unexpectedExit;
  }
}
