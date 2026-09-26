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

import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.server.protocol.TermIndex;

import java.lang.reflect.Field;
import java.util.List;

/**
 * The real {@link DatabaseReconciler} with only the leader snapshot-marker read answered locally. Since issue #8374 a
 * leader-driven install reads that marker on every path and fails when it cannot, so a unit test that drives an
 * install against a fake leader address, and is about something else, installs this to keep the install offline.
 */
final class NoNetworkMarkerReconciler extends DatabaseReconciler {
  private final TermIndex marker;

  private NoNetworkMarkerReconciler(final ArcadeDBServer server, final TermIndex marker) {
    this.marker = marker;
    setServer(server);
  }

  @Override
  LeaderDatabaseQuery.BootstrapState fetchSnapshotMarker(final String leaderHttpAddr, final String leaderHttpsAddr,
      final String clusterToken) {
    return new LeaderDatabaseQuery.BootstrapState(List.of(), marker);
  }

  /** Swaps it into {@code sm}, reporting no marker so the install registers the approximate term as before. */
  static void installInto(final ArcadeStateMachine sm, final ArcadeDBServer server) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("reconciler");
    f.setAccessible(true);
    f.set(sm, new NoNetworkMarkerReconciler(server, null));
  }
}
