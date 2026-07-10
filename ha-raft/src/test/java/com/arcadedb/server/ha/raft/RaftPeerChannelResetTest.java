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

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.util.PeerProxyMap;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Verifies the Ratis-coupling seam of the issue #4696 channel-reset recovery:
 * {@link RaftHAServer#resetPeerAppenderChannel(RaftServerRpc, RaftPeerId)} must close and re-create
 * the leader's outbound gRPC proxy for the unreachable peer (via {@code PeerProxyMap.resetProxy}),
 * which is what forces a fresh DNS re-resolution, and must degrade gracefully when the RPC layer is
 * not the expected proxy-based Ratis implementation.
 */
class RaftPeerChannelResetTest {

  @Test
  @SuppressWarnings({ "unchecked", "rawtypes" })
  void resetsTheProxyForTheGivenPeer() {
    final RaftServerRpcWithProxy rpc = mock(RaftServerRpcWithProxy.class);
    final PeerProxyMap proxies = mock(PeerProxyMap.class);
    when(rpc.getProxies()).thenReturn(proxies);

    final RaftPeerId peerId = RaftPeerId.valueOf("vcc-superx-arcadedb-2_2434");

    final boolean applied = RaftHAServer.resetPeerAppenderChannel(rpc, peerId);

    assertThat(applied).isTrue();
    verify(proxies).resetProxy(peerId);
  }

  @Test
  void returnsFalseWhenProxiesAreNull() {
    // Defensive: a proxy-based RPC whose proxy map is not yet available is a no-op, not an NPE.
    final RaftServerRpcWithProxy<?, ?> rpc = mock(RaftServerRpcWithProxy.class);
    when(rpc.getProxies()).thenReturn(null);

    final boolean applied = RaftHAServer.resetPeerAppenderChannel(rpc, RaftPeerId.valueOf("peer-1"));

    assertThat(applied).isFalse();
  }

  @Test
  void returnsFalseWhenRpcIsNotProxyBased() {
    // A plain RaftServerRpc (not RaftServerRpcWithProxy) has no proxy map to reset: the reset is a
    // no-op and the caller logs at FINE instead of throwing.
    final RaftServerRpc rpc = mock(RaftServerRpc.class);

    final boolean applied = RaftHAServer.resetPeerAppenderChannel(rpc, RaftPeerId.valueOf("peer-1"));

    assertThat(applied).isFalse();
    verifyNoInteractions(rpc);
  }
}
