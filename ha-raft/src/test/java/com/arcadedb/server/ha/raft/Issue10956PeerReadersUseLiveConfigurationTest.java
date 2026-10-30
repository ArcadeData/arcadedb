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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10956 (#10933, #10937): two HA readers still enumerated the declared server list where the live configuration is the
 * authority.
 * <ul>
 *   <li>#10933: the presence matrix of {@code GET /api/v1/cluster?presence=true} iterated the group parsed from
 *       {@code arcadedb.ha.serverList}, so a pod that joined at runtime was neither a node of the matrix nor reported
 *       unreachable, and every database it lacked read as present everywhere.</li>
 *   <li>#10937: the two preconditions of {@code addPeer} (the duplicate-address refusal and the reachability probe) read
 *       {@code getLivePeers()}, which substitutes the declared list while the division is unreadable: a declared peer that is
 *       not a member read as one, both checks skipped it, and the add went on for real.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10956PeerReadersUseLiveConfigurationTest {

  @Nested
  class PresenceMatrix {
    private static final String SERVER_LIST = "arcadedb-0.svc:2434:2480,arcadedb-1.svc:2434:2480,arcadedb-2.svc:2434:2480";

    private static final RaftPeerId POD_0  = RaftPeerId.valueOf("arcadedb-0.svc_2434");
    private static final RaftPeerId POD_1  = RaftPeerId.valueOf("arcadedb-1.svc_2434");
    private static final RaftPeerId POD_2  = RaftPeerId.valueOf("arcadedb-2.svc_2434");
    private static final RaftPeer   JOINED = RaftPeer.newBuilder().setId("arcadedb-3.svc_2434").setAddress("arcadedb-3.svc:2434")
        .build();

    private final ArcadeDBServer server = TestServerHelper.unstartedServer("ArcadeDB_0");

    /** The joined pod is a node of the matrix, and the database it lacks is reported missing on it. */
    @Test
    void aPeerThatJoinedAtRuntimeIsANodeOfTheMatrix() {
      final RaftHAServer raft = serverWithLiveConfiguration(List.of(declared(POD_0), declared(POD_1), declared(POD_2), JOINED));
      raft.getHttpAddresses().put(JOINED.getId(), "arcadedb-3.svc:2480");

      final JSONObject matrix = GetClusterHandler.buildPresenceMatrix(server, raft, POD_0, membershipOf(raft),
          (s, peerId, dial, token) -> peerId.equals(JOINED.getId().toString()) ? Set.of("A") : Set.of("A", "B"));

      assertThat(strings(matrix.getJSONArray("nodes"))).containsExactly(POD_0.toString(), POD_1.toString(), POD_2.toString(),
          JOINED.getId().toString());
      assertThat(strings(matrix.getJSONArray("unreachable"))).isEmpty();
      assertThat(strings(matrix.getJSONArray("notInConfiguration"))).isEmpty();
      final JSONObject b = database(matrix, "B");
      assertThat(strings(b.getJSONArray("missing"))).as("the joined pod lacks B, and says so")
          .contains(JOINED.getId().toString());
      assertThat(strings(b.getJSONArray("present"))).containsExactlyInAnyOrder(POD_1.toString(), POD_2.toString());
    }

    /** A joined pod that does not answer is reported unreachable, never silently absent from both lists. */
    @Test
    void aJoinedPeerThatDoesNotAnswerIsUnreachable() {
      final RaftHAServer raft = serverWithLiveConfiguration(List.of(declared(POD_0), declared(POD_1), declared(POD_2), JOINED));
      raft.getHttpAddresses().put(JOINED.getId(), "arcadedb-3.svc:2480");

      final JSONObject matrix = GetClusterHandler.buildPresenceMatrix(server, raft, POD_0, membershipOf(raft),
          (s, peerId, dial, token) -> {
            if (peerId.equals(JOINED.getId().toString()))
              throw new IOException("connection refused");
            return Set.of("A");
          });

      assertThat(strings(matrix.getJSONArray("nodes"))).contains(JOINED.getId().toString());
      assertThat(strings(matrix.getJSONArray("unreachable"))).containsExactly(JOINED.getId().toString());
      assertThat(strings(database(matrix, "A").getJSONArray("missing"))).as("an unreachable node is not counted missing")
          .doesNotContain(JOINED.getId().toString());
    }

    /**
     * The other direction does not regress: a declared pod the configuration dropped is still queried, and is flagged as not a
     * member so what it lacks is not read as a replication gap.
     */
    @Test
    void aDeclaredPeerTheConfigurationDroppedIsFlagged() {
      final RaftHAServer raft = serverWithLiveConfiguration(List.of(declared(POD_0), declared(POD_1), JOINED));
      raft.getHttpAddresses().put(JOINED.getId(), "arcadedb-3.svc:2480");

      final JSONObject matrix = GetClusterHandler.buildPresenceMatrix(server, raft, POD_0, membershipOf(raft),
          (s, peerId, dial, token) -> Set.of("A"));

      assertThat(strings(matrix.getJSONArray("nodes"))).contains(POD_2.toString(), JOINED.getId().toString());
      assertThat(strings(matrix.getJSONArray("notInConfiguration"))).containsExactly(POD_2.toString());
    }

    private ClusterMembership membershipOf(final RaftHAServer raft) {
      // What GetClusterHandler samples once per request and hands to both the peers array and the matrix
      return ClusterMembership.of(raft.getRaftGroup().getPeers(), raft.getLivePeers());
    }

    /** A detached server built from {@link #SERVER_LIST}, naming itself pod 0, whose live Raft configuration is {@code live}. */
    private RaftHAServer serverWithLiveConfiguration(final Collection<RaftPeer> live) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
      return new RaftHAServer(server, config) {
        @Override
        public Collection<RaftPeer> getLivePeers() {
          return live;
        }

        @Override
        Collection<RaftPeer> getCommittedPeersOrNull() {
          return live;
        }
      };
    }

    private static RaftPeer declared(final RaftPeerId id) {
      return RaftPeer.newBuilder().setId(id).setAddress(id.toString().replace('_', ':')).build();
    }

    private static JSONObject database(final JSONObject matrix, final String name) {
      final JSONArray databases = matrix.getJSONArray("databases");
      for (int i = 0; i < databases.length(); i++)
        if (name.equals(databases.getJSONObject(i).getString("name")))
          return databases.getJSONObject(i);
      throw new AssertionError("database " + name + " is not a row of the matrix: " + matrix);
    }

    private static List<String> strings(final JSONArray array) {
      final List<String> result = new ArrayList<>(array.length());
      for (int i = 0; i < array.length(); i++)
        result.add(array.getString(i));
      return result;
    }
  }

  /**
   * The declared list ({@code getLivePeers()} while the division is unreadable) names E, a peer that never joined; the committed
   * configuration, which the compare-and-set of the add is evaluated against, is A, B, C.
   */
  @Nested
  class AddPeerPreconditions {
    private final List<SetConfigurationRequest.Arguments> sent = new CopyOnWriteArrayList<>();
    private       FakeRaftHAServer                        server;

    @BeforeEach
    void setUp() {
      server = FakeRaftHAServer.detached();
      server.raftGroup(RaftGroup.valueOf(RaftGroupId.randomId()));
      server.client(recordingClient());
      server.livePeers(List.of(peer("A"), peer("B"), peer("C"), RaftPeer.newBuilder().setId("E").setAddress("peer-e.invalid:2434")
          .build()));
      server.committedPeers(List.of(peer("A"), peer("B"), peer("C")));
    }

    /** E is not a member, so the probe runs and refuses it at once, instead of the add waiting out the membership budget. */
    @Test
    void anUnreachablePeerTheDeclaredListNamesIsProbedAndRefused() {
      server.getConfiguration().setValue(GlobalConfiguration.HA_ADD_PEER_PROBE_TIMEOUT, 2_000L);

      assertThatThrownBy(() -> server.addPeer("E", "peer-e.invalid:2434")).isInstanceOf(UnreachablePeerException.class);
      assertThat(sent).as("no membership change is attempted").isEmpty();
    }

    /**
     * The duplicate-address refusal compares with the members, not with the declared list: F on the address the declared list
     * gives E (not a member) is no duplicate, and the change is built from the committed configuration.
     */
    @Test
    void theDuplicateAddressCheckComparesWithTheMembersOnly() {
      server.getConfiguration().setValue(GlobalConfiguration.HA_ADD_PEER_PROBE_TIMEOUT, 0L);

      server.addPeer("F", "peer-e.invalid:2434");

      assertThat(sent).hasSize(1);
      assertThat(sent.getFirst().getServersInCurrentConf()).extracting(p -> p.getId().toString()).containsExactly("A", "B", "C");
      assertThat(sent.getFirst().getServersInNewConf()).extracting(p -> p.getId().toString())
          .containsExactly("A", "B", "C", "F");
    }

    /** A member re-added under its own id is still the idempotent no-op, probe skipped, whatever its address answers. */
    @Test
    void aMemberReAddedIsStillTheIdempotentNoOp() {
      server.getConfiguration().setValue(GlobalConfiguration.HA_ADD_PEER_PROBE_TIMEOUT, 2_000L);
      server.committedPeers(List.of(peer("A"), peer("B"), peer("C"),
          RaftPeer.newBuilder().setId("E").setAddress("peer-e.invalid:2434").build()));

      server.addPeer("E", "peer-e.invalid:2434");

      assertThat(sent).as("already a member: nothing to change").isEmpty();
    }

    private RaftPeer peer(final String id) {
      return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress("h-" + id + ".invalid:2434").build();
    }

    /** A Raft client whose admin API records each configuration change and answers it as committed. */
    private RaftClient recordingClient() {
      final AdminApi admin = new AdminApi() {
        @Override
        public RaftClientReply setConfiguration(final SetConfigurationRequest.Arguments arguments) {
          sent.add(arguments);
          return RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("A"))
              .setGroupId(RaftGroupId.randomId()).setCallId(sent.size()).setSuccess(true).build();
        }

        @Override
        public RaftClientReply transferLeadership(final RaftPeerId expectedLeader, final RaftPeerId newLeader,
            final long timeoutMs) {
          throw new UnsupportedOperationException("not part of a membership change");
        }
      };
      return (RaftClient) Proxy.newProxyInstance(RaftClient.class.getClassLoader(), new Class<?>[] { RaftClient.class },
          (proxy, method, args) -> switch (method.getName()) {
            case "admin" -> admin;
            case "close" -> null;
            case "toString" -> "recording RaftClient";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new UnsupportedOperationException(method.getName());
          });
    }
  }
}
