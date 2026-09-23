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
  int                    checksBeforeExit;

  private void log(final String call) {
    if (failure != null)
      throw failure;
    calls.add(call);
  }

  @Override
  public void kill(final int node) {
    log("kill:" + node);
  }

  @Override
  public void stopGracefully(final int node) {
    log("stop:" + node);
  }

  @Override
  public void start(final int node) {
    log("start:" + node);
  }

  @Override
  public void pause(final int node) {
    log("pause:" + node);
  }

  @Override
  public void unpause(final int node) {
    log("unpause:" + node);
  }

  @Override
  public void disconnect(final int node) {
    log("disconnect:" + node);
  }

  @Override
  public void reconnect(final int node) {
    log("reconnect:" + node);
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
    return leaderAvailable ? leader : -1;
  }

  @Override
  public boolean awaitLeader(final Duration timeout) {
    calls.add("awaitLeader");
    return leaderAvailable;
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
