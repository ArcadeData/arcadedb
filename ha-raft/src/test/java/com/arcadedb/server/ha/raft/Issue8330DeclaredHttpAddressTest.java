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

import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.server.ha.raft.RaftPeerAddressResolver.JoinTarget;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8330: the HTTP port a {@code connect cluster} target declares has to be what the joining node's own
 * HTTP-address map holds while the membership change COMMITS, not something written after it.
 * <p>
 * The membership change is not the end of an admission. The configuration entry it commits makes the leader
 * schedule the security seed at once ({@code MembershipSecuritySeeder}), and the group and API-token entries of
 * that seed pass the #7511 capability gate only after the new peer has answered a capability probe - a probe
 * sent to whatever HTTP address the map holds for that peer at that instant. The declared address used to be
 * written only after {@code addPeer} returned, and {@code addPeer} itself first wrote one DERIVED from the Raft
 * port plus another peer's HTTP offset, which on a cluster whose ports are not in step names a socket nobody
 * listens on. Every probe of the seed's retry budget then failed with {@code ConnectException} and the verb
 * answered 503 with {@code failedSeeds: [groups, API tokens]}, although the peer was up and answering on the
 * port the operator had named.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8330DeclaredHttpAddressTest {

  private static final int DEFAULT_RAFT_PORT = 2434;

  /** What {@code getLivePeers()} answers once a test sets it, standing in for a change that committed. */
  private final AtomicReference<List<RaftPeer>> livePeersOverride = new AtomicReference<>();

  /** A cluster whose Raft and HTTP ports are NOT in step, so a derived address is detectably wrong. */
  private FakeRaftHAServer stubServer(final Map<RaftPeerId, String> httpAddresses, final AdminApi admin) {
    final FakeRaftHAServer server = FakeRaftHAServer.detached();
    final RaftClient client = mock(RaftClient.class);
    server.returns("getClient", client);
    when(client.admin()).thenReturn(admin);
    final RaftPeer a = RaftPeer.newBuilder().setId(RaftPeerId.valueOf("A")).setAddress("localhost:28654").build();
    final RaftPeer b = RaftPeer.newBuilder().setId(RaftPeerId.valueOf("B")).setAddress("localhost:15712").build();
    final List<RaftPeer> live = List.of(a, b);
    server.on("getLivePeers", args -> livePeersOverride.get() != null ? livePeersOverride.get() : live);
    server.on("getCommittedPeersOrNull", args -> livePeersOverride.get() != null ? livePeersOverride.get() : live);
    server.raftGroup(RaftGroup.valueOf(RaftGroupId.randomId(), a, b));
    httpAddresses.put(a.getId(), "localhost:2480");
    httpAddresses.put(b.getId(), "localhost:2481");
    server.httpAddresses(httpAddresses);
    return server;
  }

  /**
   * The address is in the map when Ratis is asked to commit - which is when the leader's seed starts probing -
   * and it is still the declared one afterwards, not replaced by the derived {@code raftPort + offset} guess.
   */
  @Test
  void aDeclaredHttpAddressIsInPlaceBeforeTheMembershipChangeCommitsAndIsNotOverwritten() throws Exception {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();
    final AdminApi admin = mock(AdminApi.class);
    final AtomicReference<String> seenAtCommit = new AtomicReference<>();
    final RaftPeerId joining = RaftPeerId.valueOf("localhost_22898");

    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(true);
    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      seenAtCommit.set(httpAddresses.get(joining));
      return reply;
    });

    final FakeRaftHAServer server = stubServer(httpAddresses, admin);
    final JoinTarget target = RaftPeerAddressResolver.parseJoinTarget("localhost:22898:2482", DEFAULT_RAFT_PORT, "");
    assertThat(target.peer().getId()).isEqualTo(joining);

    new RaftClusterManager(server).addPeer(target.peer(), target.name(), target.httpAddress());

    assertThat(seenAtCommit.get()).as("the address a seed started by this commit would probe").isEqualTo("localhost:2482");
    assertThat(httpAddresses.get(joining)).as("the address left behind once the change returned").isEqualTo("localhost:2482");
  }

  /**
   * Without a declared port nothing changes: the address is still derived after the commit, as it always was, so a
   * homogeneous cluster (a Kubernetes StatefulSet) keeps the behaviour it relies on.
   */
  @Test
  void withoutADeclaredPortTheAddressIsStillDerivedAfterTheCommit() throws Exception {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();
    final AdminApi admin = mock(AdminApi.class);
    final AtomicReference<String> seenAtCommit = new AtomicReference<>("unset");
    final RaftPeerId joining = RaftPeerId.valueOf("localhost_22898");

    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(true);
    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      seenAtCommit.set(httpAddresses.get(joining));
      return reply;
    });

    final FakeRaftHAServer server = stubServer(httpAddresses, admin);
    final JoinTarget target = RaftPeerAddressResolver.parseJoinTarget("localhost:22898", DEFAULT_RAFT_PORT, "");
    assertThat(target.httpAddress()).isNull();

    new RaftClusterManager(server).addPeer(target.peer(), target.name(), target.httpAddress());

    assertThat(seenAtCommit.get()).isNull();
    // Offset of peer A: 2480 - 28654 = -26174, applied to 22898.
    assertThat(httpAddresses.get(joining)).isEqualTo("localhost:" + (22898 + 2480 - 28654));
  }

  /**
   * A change that does not commit leaves no address behind for a peer that never became a member, and puts back
   * the one that was there before - a re-issued join of a peer the map already knew must not lose it either.
   */
  @Test
  void aMembershipChangeThatFailsRestoresThePreviousAddress() throws Exception {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();
    final AdminApi admin = mock(AdminApi.class);
    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      throw new GroupMismatchException("group-AAAA does not match group-BBBB");
    });
    final FakeRaftHAServer server = stubServer(httpAddresses, admin);

    final JoinTarget fresh = RaftPeerAddressResolver.parseJoinTarget("localhost:22898:2482", DEFAULT_RAFT_PORT, "");
    assertThatThrownBy(() -> new RaftClusterManager(server, 60_000L).addPeer(fresh.peer(), fresh.name(), fresh.httpAddress()))
        .isInstanceOf(ConfigurationException.class);
    assertThat(httpAddresses).doesNotContainKey(fresh.peer().getId());

    final RaftPeerId knownId = RaftPeerId.valueOf("localhost_24001");
    httpAddresses.put(knownId, "localhost:2483");
    final JoinTarget known = RaftPeerAddressResolver.parseJoinTarget("localhost:24001:2489", DEFAULT_RAFT_PORT, "");
    assertThatThrownBy(() -> new RaftClusterManager(server, 60_000L).addPeer(known.peer(), known.name(), known.httpAddress()))
        .isInstanceOf(ConfigurationException.class);
    assertThat(httpAddresses.get(knownId)).isEqualTo("localhost:2483");
  }

  /**
   * A failure is not proof the change did not commit: when the peer is in the configuration by the time the failure
   * surfaces, the declared address stays, rather than leaving a member with only the derived guess.
   */
  @Test
  void aFailureAfterThePeerBecameAMemberKeepsTheDeclaredAddress() throws Exception {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();
    final AdminApi admin = mock(AdminApi.class);
    final FakeRaftHAServer server = stubServer(httpAddresses, admin);
    final JoinTarget target = RaftPeerAddressResolver.parseJoinTarget("localhost:22898:2482", DEFAULT_RAFT_PORT, "");

    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      final List<RaftPeer> withJoining = new ArrayList<>(server.getLivePeers());
      withJoining.add(target.peer());
      livePeersOverride.set(withJoining);
      throw new GroupMismatchException("group-AAAA does not match group-BBBB");
    });

    assertThatThrownBy(() -> new RaftClusterManager(server, 60_000L).addPeer(target.peer(), target.name(), target.httpAddress()))
        .isInstanceOf(ConfigurationException.class);
    assertThat(httpAddresses.get(target.peer().getId())).isEqualTo("localhost:2482");
  }

  /** The rollback withdraws only its own write: an entry something else put in while the change was in flight stays. */
  @Test
  void theRollbackKeepsAnEntryWrittenWhileTheChangeWasInFlight() throws Exception {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();
    final AdminApi admin = mock(AdminApi.class);
    final FakeRaftHAServer server = stubServer(httpAddresses, admin);

    final RaftPeerId knownId = RaftPeerId.valueOf("localhost_24001");
    httpAddresses.put(knownId, "localhost:2483");
    final JoinTarget known = RaftPeerAddressResolver.parseJoinTarget("localhost:24001:2489", DEFAULT_RAFT_PORT, "");

    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      httpAddresses.put(knownId, "localhost:2487");
      throw new GroupMismatchException("group-AAAA does not match group-BBBB");
    });

    assertThatThrownBy(() -> new RaftClusterManager(server, 60_000L).addPeer(known.peer(), known.name(), known.httpAddress()))
        .isInstanceOf(ConfigurationException.class);
    assertThat(httpAddresses.get(knownId)).isEqualTo("localhost:2487");
  }
}
