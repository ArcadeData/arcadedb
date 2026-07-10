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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.*;

/**
 * Regression test for issue #4837: on a K8s pod shutdown {@link RaftHAPlugin#stopService()} used to
 * call {@code leaveCluster()} and then {@code stop()}, while {@link RaftHAServer#stop()} also calls
 * {@code leaveCluster()} in its {@code HA_K8S} branch. The graceful leave therefore ran twice: the
 * second call found self already removed and threw {@link com.arcadedb.exception.ConfigurationException},
 * doing redundant leader-transfer/reconfig work and logging a spurious WARNING on every termination.
 * <p>
 * The fix makes {@link RaftHAServer#stop()} the single owner of the K8s graceful leave (it is also
 * reached via {@code disconnectCluster()}); {@code stopService()} must not issue its own leave. This
 * test asserts that contract deterministically with a mocked {@link RaftHAServer}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4837DoubleLeaveTest {

  @Test
  void stopServiceDoesNotLeaveClusterItselfInK8s() throws Exception {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    final RaftHAServer raft = mock(RaftHAServer.class);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_K8S, true);

    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.configure(server, config);
    injectRaftHAServer(plugin, raft);

    plugin.stopService();

    // stop() is the single owner of the graceful leave; stopService() must delegate to it exactly once
    // and must NOT issue its own leaveCluster() (which caused the double-leave of issue #4837).
    verify(raft, times(1)).stop();
    verify(raft, never()).leaveCluster();
    verify(raft, never()).leaveCluster(anyBoolean());
  }

  @Test
  void stopServiceDoesNotLeaveClusterItselfWhenNotK8s() throws Exception {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    final RaftHAServer raft = mock(RaftHAServer.class);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_K8S, false);

    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.configure(server, config);
    injectRaftHAServer(plugin, raft);

    plugin.stopService();

    verify(raft, times(1)).stop();
    verify(raft, never()).leaveCluster();
    verify(raft, never()).leaveCluster(anyBoolean());
  }

  private static void injectRaftHAServer(final RaftHAPlugin plugin, final RaftHAServer raft) throws Exception {
    final Field field = RaftHAPlugin.class.getDeclaredField("raftHAServer");
    field.setAccessible(true);
    field.set(plugin, raft);
  }
}
