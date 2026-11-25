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
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.ha.raft.PostSecuritySeedHandler.DeclaredPeerHttpAddress;
import com.arcadedb.server.ha.raft.RaftPeerAddressResolver.JoinTarget;
import com.arcadedb.server.http.HttpServer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static com.arcadedb.server.ha.raft.PeerCapabilityProbeTest.SharedAddressCapabilityProbe.newDetachedServer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * How a node learns and resolves the HTTP address of its peers: declared addresses, candidates, runtime learning,
 * shared-endpoint port offsets and the live-configuration readers.
 */
class PeerHttpAddressDiscoveryTest {

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
   */
  @Nested
  class DeclaredHttpAddress {

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
    // Regression for #8330: a declared http address is in place before the membership change commits and is not overwritten
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
     * Without a declared port nothing is written, before the commit or after it (issue #9255). The node serving the admission
     * used to write a {@code raftPort + offset} guess here while every other node derived the peer's Raft host plus its own
     * HTTP port, so two nodes dialled one member on two addresses (issue #9230). Every node now derives the same address until
     * the member is heard from, and then records the one it answers on.
     */
    // Regression for #8330: without a declared port no address is guessed at the commit
    @Test
    void withoutADeclaredPortNoAddressIsGuessedAtTheCommit() throws Exception {
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
      assertThat(httpAddresses).as("no guess left behind once the change returned").doesNotContainKey(joining);
    }

    /**
     * A change that does not commit leaves no address behind for a peer that never became a member, and puts back
     * the one that was there before - a re-issued join of a peer the map already knew must not lose it either.
     */
    // Regression for #8330: a membership change that fails restores the previous address
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
    // Regression for #8330: a failure after the peer became a member keeps the declared address
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
    // Regression for #8330: the rollback keeps an entry written while the change was in flight
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

  /**
   * Issue #8689: a {@code connect cluster host:raftPort:httpPort} served by a FOLLOWER wrote the declared HTTP address
   * into the follower's own map only. The security seed runs on the leader, and its #7511 capability gate probes the
   * new peer at the address the LEADER's map holds - for a brand-new or previously removed peer, a derived guess that
   * on a cluster whose ports are not in step names the wrong listener. The verb then answered 503 with
   * {@code failedSeeds: [groups, API tokens]} for a peer that was up and declared correctly.
   * <p>
   * The fix carries the declaration in the seed request to the leader, which records it - only for a committed member,
   * never over its own server list - and answers the request with a seed that started after the record. These pin each
   * link of that chain without a live cluster; {@code ClusterRejoinMembershipIT} drives it end to end.
   */
  @Nested
  class FollowerDeclaredHttpAddress {

    private static final RaftPeerId LEADER  = RaftPeerId.valueOf("localhost_28654");
    private static final RaftPeerId MEMBER  = RaftPeerId.valueOf("localhost_15712");
    private static final RaftPeerId JOINED  = RaftPeerId.valueOf("localhost_22898");
    private static final String     ADDRESS = "localhost:2482";

    private static List<RaftPeer> committed(final RaftPeerId... ids) {
      final List<RaftPeer> peers = new ArrayList<>(ids.length);
      for (final RaftPeerId id : ids)
        peers.add(RaftPeer.newBuilder().setId(id).setAddress(id.toString().replace('_', ':')).build());
      return peers;
    }

    // ------------------------------------------------------------------ the leader records the declaration

    /** The defect's shape on the leader: no entry for the joined peer, so the declared one is recorded. */
    // Regression for #8689: a committed member with no entry gets the declared address
    @Test
    void aCommittedMemberWithNoEntryGetsTheDeclaredAddress() {
      final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();

      assertThat(PeerAdmission.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, MEMBER, JOINED), httpAddresses,
          Map.of(), JOINED, ADDRESS)).isTrue();
      assertThat(httpAddresses.get(JOINED)).isEqualTo(ADDRESS);
    }

    /** A derived or runtime-written entry is replaced, as a connect cluster served by this node would have replaced it. */
    // Regression for #8689: a derived entry is replaced
    @Test
    void aDerivedEntryIsReplaced() {
      final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>(Map.of(JOINED, "localhost:22944"));

      assertThat(PeerAdmission.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, JOINED), httpAddresses, Map.of(),
          JOINED, ADDRESS)).isTrue();
      assertThat(httpAddresses.get(JOINED)).isEqualTo(ADDRESS);
    }

    /** The same address again changes nothing, and says so: the caller must not throw away a seed for a no-op. */
    // Regression for #8689: the same address again is not a change
    @Test
    void theSameAddressAgainIsNotAChange() {
      final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>(Map.of(JOINED, ADDRESS));

      assertThat(PeerAdmission.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, JOINED), httpAddresses, Map.of(),
          JOINED, ADDRESS)).isFalse();
    }

    /** The address this node's own server list declared is the operator's word here, and another node does not outrank it. */
    // Regression for #8689: an entry this nodes server list declared is kept
    @Test
    void anEntryThisNodesServerListDeclaredIsKept() {
      final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>(Map.of(JOINED, "localhost:2490"));

      assertThat(PeerAdmission.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, JOINED), httpAddresses,
          Map.of(JOINED, "localhost:2490"), JOINED, ADDRESS)).isFalse();
      assertThat(httpAddresses.get(JOINED)).isEqualTo("localhost:2490");
    }

    /**
     * Once this node has dropped its server-list entry (removePeer does), there is nothing left to override, and the
     * newest declaration wins - exactly what a {@code connect cluster} served by this node does, since
     * {@code RaftClusterManager.addPeer} writes a declared address unconditionally. Restoring the boot-time value instead
     * would make the follower-served admission resolve the peer differently from the leader-served one.
     */
    // Regression for #8689: a dropped server list entry does not outrank a newer declaration
    @Test
    void aDroppedServerListEntryDoesNotOutrankANewerDeclaration() {
      final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();

      assertThat(PeerAdmission.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, JOINED), httpAddresses,
          Map.of(JOINED, "localhost:2490"), JOINED, ADDRESS)).isTrue();
      assertThat(httpAddresses.get(JOINED)).isEqualTo(ADDRESS);
    }

    /** A seed request cannot plant an address for a node that is not a member. */
    // Regression for #8689: a peer outside the committed configuration is refused
    @Test
    void aPeerOutsideTheCommittedConfigurationIsRefused() {
      final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();

      assertThat(PeerAdmission.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, MEMBER), httpAddresses, Map.of(),
          JOINED, ADDRESS)).isFalse();
      assertThat(PeerAdmission.recordAdmittedPeerHttpAddress(LEADER, null, httpAddresses, Map.of(), JOINED, ADDRESS))
          .as("an unreadable configuration is not proof of membership").isFalse();
      assertThat(httpAddresses).isEmpty();
    }

    /** Nor can it rewrite this node's own address. */
    // Regression for #8689: this nodes own address is never recorded
    @Test
    void thisNodesOwnAddressIsNeverRecorded() {
      final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();

      assertThat(PeerAdmission.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, MEMBER), httpAddresses, Map.of(),
          LEADER, ADDRESS)).isFalse();
      assertThat(httpAddresses).isEmpty();
    }

    // ------------------------------------------------------------------ the request fields

    // Regression for #8689: a request without a declaration reads as none
    @Test
    void aRequestWithoutADeclarationReadsAsNone() {
      assertThat(PostSecuritySeedHandler.readDeclaredPeerHttpAddress(new JSONObject().put("reason", "an admission")))
          .isNull();
    }

    // Regression for #8689: a request with a declaration reads it
    @Test
    void aRequestWithADeclarationReadsIt() {
      assertThat(PostSecuritySeedHandler.readDeclaredPeerHttpAddress(new JSONObject()
          .put(PostSecuritySeedHandler.ADMITTED_PEER_ID, JOINED.toString())
          .put(PostSecuritySeedHandler.DECLARED_HTTP_ADDRESS, ADDRESS)))
          .isEqualTo(new DeclaredPeerHttpAddress(JOINED.toString(), ADDRESS));
    }

    /** Each malformed shape is the caller's mistake: a 400, never a value written into the dial map. */
    // Regression for #8689: a malformed declaration is refused
    @Test
    void aMalformedDeclarationIsRefused() {
      for (final JSONObject bad : List.of(
          new JSONObject().put(PostSecuritySeedHandler.DECLARED_HTTP_ADDRESS, ADDRESS),
          new JSONObject().put(PostSecuritySeedHandler.ADMITTED_PEER_ID, JOINED.toString()),
          declaration("localhost"),
          declaration("localhost:"),
          declaration(":2482"),
          declaration("localhost:http"),
          declaration("localhost:0"),
          declaration("localhost:65536"),
          declaration("evil.example/x:80"),
          declaration("root@evil.example:80"),
          declaration("evil.example?x:80"),
          declaration("local host:80"),
          declaration("[::1:2480"),
          declaration("host%2Fx:80"),
          declaration("[fe80::1%eth0]:2480")))
        assertThatThrownBy(() -> PostSecuritySeedHandler.readDeclaredPeerHttpAddress(bad)).as("%s", bad)
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** A catch-up is a node repairing itself: whatever its body carries, it records nothing. An admission does. */
    // Regression for #8689: only an admission records its declaration
    @Test
    void onlyAnAdmissionRecordsItsDeclaration() {
      final DeclaredPeerHttpAddress declared = new DeclaredPeerHttpAddress(JOINED.toString(), ADDRESS);

      assertThat(PostSecuritySeedHandler.declarationToRecord(declared, true)).isNull();
      assertThat(PostSecuritySeedHandler.declarationToRecord(declared, false)).isEqualTo(declared);
      assertThat(PostSecuritySeedHandler.declarationToRecord(null, false)).isNull();
    }

    /** The forms a real peer address takes are accepted, a bracketed IPv6 literal included. */
    // Regression for #8689: every peer address form is accepted
    @Test
    void everyPeerAddressFormIsAccepted() {
      for (final String address : List.of("localhost:2482", "10.0.0.7:2480", "arcadedb-2.arcadedb.default.svc:2480",
          "[::1]:2480"))
        assertThat(PostSecuritySeedHandler.readDeclaredPeerHttpAddress(declaration(address)).httpAddress()).as(address)
            .isEqualTo(address);
    }

    /**
     * The leader answers only after the seed in flight and then a fresh one, so the deadline has room for two whole
     * retry budgets; anything less lets the fresh seed time out behind one that spent its budget on the stale address.
     */
    // Regression for #8689: a declared admission is given room for two seeds
    @Test
    void aDeclaredAdmissionIsGivenRoomForTwoSeeds() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT, 7_000L);

      assertThat(ClusterSecuritySeedQuery.declaredAdmissionReportTimeoutMs(configuration))
          .isGreaterThan(2 * 7_000L)
          .isGreaterThan(ClusterSecuritySeedQuery.reportTimeoutMs(configuration));
      assertThat(ClusterSecuritySeedQuery.CLIENT_HEADROOM_MS)
          .as("the admitting node waits longer than the leader, so it reads the leader's answer").isPositive();
    }

    private static JSONObject declaration(final String address) {
      return new JSONObject().put(PostSecuritySeedHandler.ADMITTED_PEER_ID, JOINED.toString())
          .put(PostSecuritySeedHandler.DECLARED_HTTP_ADDRESS, address);
    }

    // ------------------------------------------------------------------ the seed that answers

    /** Stands in for the cluster-wide seed: the first run fails as the derived-address probe did, every later one commits. */
    private static class FailFirstSeed implements MembershipSecuritySeeder.SecuritySeed {
      final AtomicInteger  calls   = new AtomicInteger();
      final CountDownLatch started = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);

      @Override
      public List<String> seed(final long retryBudgetMs) {
        if (calls.incrementAndGet() == 1) {
          started.countDown();
          try {
            release.await(10, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return List.of("groups", "API tokens");
        }
        return List.of();
      }
    }

    /**
     * A seed still in flight when the address was recorded probed the old one: the request waits it out and is answered
     * by a fresh seed, instead of folding into the failure the declaration was sent to prevent.
     */
    // Regression for #8689: a seed in flight is not the answer after the address changed
    @Test
    void aSeedInFlightIsNotTheAnswerAfterTheAddressChanged() throws Exception {
      final FailFirstSeed seed = new FailFirstSeed();
      final ExecutorService executor = Executors.newSingleThreadExecutor();
      final ExecutorService caller = Executors.newSingleThreadExecutor();
      try (final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> 3_000L, seed,
          executor)) {
        final CompletableFuture<List<String>> membershipSeed = CompletableFuture.supplyAsync(
            () -> seeder.seedNowAndReport("the membership change", 10_000L, false), caller);
        assertThat(seed.started.await(10, TimeUnit.SECONDS)).isTrue();

        final CompletableFuture<List<String>> answer = new CompletableFuture<>();
        final Thread admission = new Thread(() -> answer.complete(seeder.seedAfterInFlightAndReport("the admission", 10_000L)));
        admission.start();
        // Released only once the admission is parked on the in-flight seed, so the wait-out path is what is exercised -
        // released earlier, the in-flight seed would already be done and a plain fresh seed would pass this test too.
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS)
            .until(() -> admission.getState() == Thread.State.TIMED_WAITING);
        assertThat(seed.calls.get()).as("nothing ran for the admission while the in-flight seed was running").isEqualTo(1);
        seed.release.countDown();

        assertThat(membershipSeed.get(10, TimeUnit.SECONDS)).containsExactly("groups", "API tokens");
        assertThat(answer.get(10, TimeUnit.SECONDS)).as("the fresh seed's outcome").isEmpty();
        assertThat(seed.calls.get()).isEqualTo(2);
      } finally {
        caller.shutdownNow();
        executor.shutdownNow();
      }
    }

    /** And one that already finished, failing, is not reused either - the admission's ordinary reuse window does not apply. */
    // Regression for #8689: a seed that just failed is not reused after the address changed
    @Test
    void aSeedThatJustFailedIsNotReusedAfterTheAddressChanged() {
      final FailFirstSeed seed = new FailFirstSeed();
      seed.release.countDown();
      try (final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> 3_000L, seed,
          Runnable::run)) {
        assertThat(seeder.seedNowAndReport("the membership change", 10_000L, false)).containsExactly("groups",
            "API tokens");

        assertThat(seeder.seedAfterInFlightAndReport("the admission", 10_000L)).isEmpty();
        assertThat(seed.calls.get()).isEqualTo(2);
      }
    }

    // ------------------------------------------------------------------ the admitting node sends it

    /** The join and the seed request of a plugin with no Raft server, recorded rather than run. */
    private static class RecordingPlugin extends RaftHAPlugin {
      final List<String> seeds = new ArrayList<>();

      RecordingPlugin() {
        configure(null, new ContextConfiguration());
      }

      @Override
      public void connectCluster(final String serverAddress) {
        // The membership change is not what is under test.
      }

      @Override
      public Optional<List<String>> seedSecurityStateForAdmission(final String admittedPeer) {
        seeds.add(admittedPeer + " declaring nothing");
        return Optional.of(List.of());
      }

      @Override
      Optional<List<String>> seedSecurityStateForAdmission(final String admittedPeer,
          final DeclaredPeerHttpAddress declared) throws IOException {
        seeds.add(admittedPeer + " declaring " + declared.peerId() + " at " + declared.httpAddress());
        return Optional.of(List.of());
      }
    }

    /** A declared HTTP port reaches the seed request, under the id the join gave the peer. */
    // Regression for #8689: connect cluster sends the declared address with the seed request
    @Test
    void connectClusterSendsTheDeclaredAddressWithTheSeedRequest() {
      final RecordingPlugin plugin = new RecordingPlugin();

      assertThat(plugin.connectClusterAndReportSeed("localhost:22898:2482")).contains(List.of());
      assertThat(plugin.seeds).containsExactly("localhost:22898:2482 declaring localhost_22898 at localhost:2482");
    }

    /** Without a declared port there is nothing to send, and the request is the one it always was. */
    // Regression for #8689: connect cluster without a declared port sends no declaration
    @Test
    void connectClusterWithoutADeclaredPortSendsNoDeclaration() {
      final RecordingPlugin plugin = new RecordingPlugin();

      plugin.connectClusterAndReportSeed("localhost:22898");
      assertThat(plugin.seeds).containsExactly("localhost:22898 declaring nothing");
    }
  }

  /**
   * Issue #9255: the candidate HTTP addresses a node holds for its peers, and the two wire members that carry them - the
   * caller's self-description in a capability request and the relayed addresses in the reply.
   */
  @Nested
  class HttpAddressCandidates {

    private static final String PEER = "hostB_2434";

    // Regression for #9255: an offered address is due until its probe fails
    @Test
    void anOfferedAddressIsDueUntilItsProbeFails() {
      final AtomicLong now = new AtomicLong(1_000L);
      final PeerHttpAddressCandidates candidates = new PeerHttpAddressCandidates();
      candidates.setClock(now::get);

      candidates.offer(PEER, "hostB:2490");
      candidates.offer(PEER, "hostB:2491");
      assertThat(candidates.due(PEER)).containsExactly("hostB:2490", "hostB:2491");

      candidates.failed(PEER, "hostB:2490");
      assertThat(candidates.due(PEER)).containsExactly("hostB:2491");

      // A relay re-offers the same stale address every round: that must not undo the back-off
      candidates.offer(PEER, "hostB:2490");
      assertThat(candidates.due(PEER)).containsExactly("hostB:2491");

      now.addAndGet(PeerHttpAddressCandidates.RETRY_AFTER_FAILURE_MS);
      assertThat(candidates.due(PEER)).containsExactly("hostB:2490", "hostB:2491");
    }

    // Regression for #9255: a peer holds a bounded number of candidates and the oldest goes first
    @Test
    void aPeerHoldsABoundedNumberOfCandidatesAndTheOldestGoesFirst() {
      final PeerHttpAddressCandidates candidates = new PeerHttpAddressCandidates();
      for (int i = 0; i < PeerHttpAddressCandidates.MAX_PER_PEER + 2; i++)
        candidates.offer(PEER, "hostB:" + (2490 + i));

      assertThat(candidates.all(PEER)).hasSize(PeerHttpAddressCandidates.MAX_PER_PEER).doesNotContain("hostB:2490", "hostB:2491")
          .contains("hostB:" + (2490 + PeerHttpAddressCandidates.MAX_PER_PEER + 1));
    }

    // Regression for #9255: a confirmed or departed peer takes its candidates with it
    @Test
    void aConfirmedOrDepartedPeerTakesItsCandidatesWithIt() {
      final PeerHttpAddressCandidates candidates = new PeerHttpAddressCandidates();
      candidates.offer(PEER, "hostB:2490");
      candidates.offer("hostC_2434", "hostC:2490");

      candidates.confirmed(PEER);
      assertThat(candidates.due(PEER)).isEmpty();

      candidates.retainOnly(Set.of(PEER));
      assertThat(candidates.all("hostC_2434")).isEmpty();
    }

    // Regression for #9255: the reply relays addresses sorted and keeps the old shape when there are none
    @Test
    void theReplyRelaysAddressesSortedAndKeepsTheOldShapeWhenThereAreNone() throws IOException {
      final JSONObject withRelay = PostCapabilitiesHandler.advertisement("hostA_2434", Set.of("x"), false, Set.of(),
          Map.of("hostC_2434", "hostC:2490", "hostB_2434", "hostB:2490"));
      assertThat(withRelay.getJSONObject(PostCapabilitiesHandler.PEER_HTTP_ADDRESSES).keySet()).containsExactly("hostB_2434",
          "hostC_2434");

      final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("hostA_2434", withRelay.toString(), "test");
      assertThat(parsed.peerHttpAddresses()).containsExactlyInAnyOrderEntriesOf(
          Map.of("hostB_2434", "hostB:2490", "hostC_2434", "hostC:2490"));

      // Byte-identical to what a node wrote before the field, so the document of a node that relays nothing does not move
      assertThat(PostCapabilitiesHandler.advertisement("hostA_2434", Set.of("x"), false, Set.of(), Map.of()).toString())
          .isEqualTo(PostCapabilitiesHandler.advertisement("hostA_2434", Set.of("x"), false, Set.of()).toString());
      assertThat(PeerCapabilityQuery.parse("hostA_2434",
          PostCapabilitiesHandler.advertisement("hostA_2434", Set.of("x")).toString(), "test").peerHttpAddresses()).isEmpty();
    }

    /**
     * A candidate is probed with no HTTPS endpoint, which dials plain HTTP whatever the SSL setting: the answer confirms the
     * HTTP address it is recorded as, never an HTTPS listener.
     */
    // Regression for #9255: a candidate is always dialled on its http address
    @Test
    void aCandidateIsAlwaysDialledOnItsHttpAddress() {
      assertThat(PeerCapabilityQuery.chooseUrl("hostB:2490", null, true)).isEqualTo("http://hostB:2490/api/v1/cluster/capabilities");
      assertThat(PeerCapabilityQuery.chooseUrl("hostB:2490", null, false)).isEqualTo("http://hostB:2490/api/v1/cluster/capabilities");
    }

    /** A malformed relayed entry is dropped, never the answer it came with: the relay is only ever a hint. */
    // Regression for #9255: a malformed relayed address is dropped and the answer kept
    @Test
    void aMalformedRelayedAddressIsDroppedAndTheAnswerKept() throws IOException {
      final JSONObject reply = PostCapabilitiesHandler.advertisement("hostA_2434", Set.of("x"))
          .put(PostCapabilitiesHandler.PEER_HTTP_ADDRESSES, new JSONObject().put("hostB_2434", "not an address")
              .put("hostC_2434", "hostC:2490"));

      final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("hostA_2434", reply.toString(), "test");
      assertThat(parsed.capabilities()).containsExactly("x");
      assertThat(parsed.peerHttpAddresses()).containsExactly(Map.entry("hostC_2434", "hostC:2490"));
      assertThat(List.of(PeerCapabilityQuery.isPeerAddress("hostB:2490"), PeerCapabilityQuery.isPeerAddress("[::1]:2490"),
          PeerCapabilityQuery.isPeerAddress("hostB"), PeerCapabilityQuery.isPeerAddress(""))).containsExactly(true, true, false,
          false);
    }
  }

  /**
   * Issue #9255 (#9229, #9230): every node learns a member's HTTP address the same way, whichever node admitted it and
   * whether or not anyone declared its port. A member that does not answer on the address this node resolves for it is
   * asked on the candidates offered for it - the port it pushed in its own capability requests, an address another member
   * relayed - and the first one it answers on, under its own id, becomes its address on this node.
   * <p>
   * The cluster below differs by host and declares no HTTP port, with every peer's listener on 2490 and this node's on 2480:
   * the derived address of each peer (its Raft host plus THIS node's port) names nothing at all, which is the shape where
   * neither the old leader-side derive nor the follower's {@code raftPort + offset} guess was right.
   */
  @Nested
  class HttpAddressLearning {
    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private static final String SERVER_LIST = "hostA:2434,hostB:2434,hostC:2434";
    private static final String LOCAL       = "hostA_2434";
    private static final String PEER_B      = "hostB_2434";
    private static final String PEER_C      = "hostC_2434";

    // Regression for #9255: a peer unreachable on its derived address is learnt from the port it pushed
    @Test
    void aPeerUnreachableOnItsDerivedAddressIsLearntFromThePortItPushed() {
      final RaftHAServer raft = newDetachedServer(SERVER_LIST);
      final ListeningPeers peers = new ListeningPeers().listen("hostB:2490", PEER_B).listen("hostC:2490", PEER_C);
      raft.setCapabilityProber(peers);

      // What PostCapabilitiesHandler hands over when B's capability request arrives: B resolves no address for itself here,
      // but its listener's port pairs with the Raft host this node already knows it by
      raft.offerCallerHttpAddress(PEER_B, null, 2490);
      raft.refreshPeerCapabilities();

      assertThat(peers.calls).as("the derived address first, then the candidate").containsSubsequence(PEER_B + "@hostB:2480",
          PEER_B + "@hostB:2490");
      assertThat(raft.getPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isEqualTo("hostB:2490");
      assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_B)).isNotNull();
      assertThat(raft.getPeerHttpAddressCandidates(PEER_B)).as("nothing offered is needed once the peer answered").isEmpty();
      // C pushed nothing and nobody relayed it: still unknown, and still derived
      assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_C)).isNull();
      assertThat(raft.getRelayablePeerHttpAddresses()).containsExactly(Map.entry(PEER_B, "hostB:2490"));
    }

    /** A member's relay is enough, within the same round: C relays the address it confirmed for B. */
    // Regression for #9255: a peer is learnt from the address another member relays
    @Test
    void aPeerIsLearntFromTheAddressAnotherMemberRelays() {
      final RaftHAServer raft = newDetachedServer(SERVER_LIST);
      raft.offerCallerHttpAddress(PEER_C, "hostC:2490", -1);
      final ListeningPeers peers = new ListeningPeers().listen("hostB:2490", PEER_B)
          .listen("hostC:2490", PEER_C, Map.of(PEER_B, "hostB:2490"));
      raft.setCapabilityProber(peers);

      raft.refreshPeerCapabilities();

      assertThat(raft.getPeerHttpAddress(RaftPeerId.valueOf(PEER_C))).isEqualTo("hostC:2490");
      assertThat(raft.getPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isEqualTo("hostB:2490");
      assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_B)).isNotNull();
    }

    /**
     * An offered address is believed only when the peer it was offered for answers on it: an address that another node
     * answers is not recorded, and is not dialled again on the next round.
     */
    // Regression for #9255: a candidate answered by another peer is never recorded
    @Test
    void aCandidateAnsweredByAnotherPeerIsNeverRecorded() {
      final RaftHAServer raft = newDetachedServer(SERVER_LIST);
      final ListeningPeers peers = new ListeningPeers().listen("hostB:2490", PEER_C);
      raft.setCapabilityProber(peers);

      raft.offerPeerHttpAddress(PEER_B, "hostB:2490");
      raft.refreshPeerCapabilities();

      assertThat(raft.getHttpAddresses()).doesNotContainKey(RaftPeerId.valueOf(PEER_B));
      assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_B)).isNull();

      peers.calls.clear();
      raft.refreshPeerCapabilities();
      assertThat(peers.calls).as("the failed candidate is left alone for a while").doesNotContain(PEER_B + "@hostB:2490");
    }

    /** The address this node's own server list declares is the operator's: nothing heard from another node replaces it. */
    // Regression for #9255: a server list declaration in force is never replaced
    @Test
    void aServerListDeclarationInForceIsNeverReplaced() {
      final RaftHAServer raft = newDetachedServer("hostA:2434:2480,hostB:2434:2470,hostC:2434:2490");
      final ListeningPeers peers = new ListeningPeers().listen("hostB:2490", PEER_B);
      raft.setCapabilityProber(peers);

      raft.offerPeerHttpAddress(PEER_B, "hostB:2490");
      assertThat(raft.getPeerHttpAddressCandidates(PEER_B)).isEmpty();
      raft.refreshPeerCapabilities();
      assertThat(raft.getPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isEqualTo("hostB:2470");

      // Once the entry is gone - the peer was removed and admitted again - the declaration no longer holds
      raft.getHttpAddresses().remove(RaftPeerId.valueOf(PEER_B));
      raft.offerPeerHttpAddress(PEER_B, "hostB:2490");
      raft.refreshPeerCapabilities();
      assertThat(raft.getPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isEqualTo("hostB:2490");
    }

    /** Neither this node nor a stranger outside the configuration can be offered an address. */
    // Regression for #9255: an offer for this node or for a non member is dropped
    @Test
    void anOfferForThisNodeOrForANonMemberIsDropped() {
      final RaftHAServer raft = newDetachedServer(SERVER_LIST);

      raft.offerCallerHttpAddress(LOCAL, "hostA:2499", 2499);
      raft.offerCallerHttpAddress("stranger_2434", "stranger:2490", 2490);
      raft.offerPeerHttpAddress(PEER_B, "not an address");
      // A candidate is probed with the cluster token: only a host this node already knows the peer by may be offered
      raft.offerPeerHttpAddress(PEER_B, "elsewhere:2490");

      assertThat(raft.getPeerHttpAddressCandidates(LOCAL)).isEmpty();
      assertThat(raft.getPeerHttpAddressCandidates("stranger_2434")).isEmpty();
      assertThat(raft.getPeerHttpAddressCandidates(PEER_B)).isEmpty();
    }

    /** On a cluster that asked for SSL no candidate is probed: the probe would carry the cluster token over plain HTTP. */
    // Regression for #9255: a cluster with ssl probes no candidate over plain http
    @Test
    void aClusterWithSslProbesNoCandidateOverPlainHttp() {
      final RaftHAServer raft = newDetachedServer(SERVER_LIST, true);
      final ListeningPeers peers = new ListeningPeers().listen("hostB:2490", PEER_B);
      raft.setCapabilityProber(peers);

      raft.offerCallerHttpAddress(PEER_B, null, 2490);
      raft.refreshPeerCapabilities();

      assertThat(peers.calls).doesNotContain(PEER_B + "@hostB:2490");
      assertThat(raft.getHttpAddresses()).doesNotContainKey(RaftPeerId.valueOf(PEER_B));
    }

    /** What this node says about itself in every capability request. */
    // Regression for #9255: the capability request carries this nodes own endpoint
    @Test
    void theCapabilityRequestCarriesThisNodesOwnEndpoint() {
      final Map<String, String> headers = newDetachedServer(SERVER_LIST).capabilityRequestHeaders();

      assertThat(headers).containsExactly(Map.entry(PostCapabilitiesHandler.CALLER_PEER_ID_HEADER, LOCAL),
          Map.entry(PostCapabilitiesHandler.CALLER_HTTP_ADDRESS_HEADER, "hostA:2480"),
          Map.entry(PostCapabilitiesHandler.CALLER_HTTP_PORT_HEADER, "2480"));
    }

    /** A peer answering on the address this node derives is confirmed there and relayed from then on. */
    // Regression for #9255: a peer answering on its derived address is recorded there
    @Test
    void aPeerAnsweringOnItsDerivedAddressIsRecordedThere() {
      final RaftHAServer raft = newDetachedServer(SERVER_LIST);
      raft.setCapabilityProber(new ListeningPeers().listen("hostB:2480", PEER_B).listen("hostC:2480", PEER_C));

      raft.refreshPeerCapabilities();

      assertThat(raft.getRelayablePeerHttpAddresses()).containsExactlyInAnyOrderEntriesOf(
          Map.of(PEER_B, "hostB:2480", PEER_C, "hostC:2480"));
    }

    /** Every peer of the list listens on one of these addresses; a probe anywhere else finds nothing. */
    private static final class ListeningPeers implements RaftHAServer.CapabilityProber {
      private final List<String>                     calls     = new ArrayList<>();
      private final Map<String, String>              listeners = new HashMap<>();
      private final Map<String, Map<String, String>> relays    = new HashMap<>();

      ListeningPeers listen(final String address, final String peerId) {
        return listen(address, peerId, Map.of());
      }

      ListeningPeers listen(final String address, final String peerId, final Map<String, String> relayed) {
        listeners.put(address, peerId);
        relays.put(address, relayed);
        return this;
      }

      @Override
      public PeerCapabilityQuery.Advertisement probe(final String expectedPeerId, final String httpAddress,
          final String httpsAddress, final String clusterToken) throws IOException {
        calls.add(expectedPeerId + "@" + httpAddress);
        final String answering = listeners.get(httpAddress);
        if (answering == null)
          throw new IOException("connection refused: " + httpAddress);
        // The binding PeerCapabilityQuery.parse enforces on a real reply
        if (expectedPeerId != null && !expectedPeerId.equals(answering))
          throw new IOException("capability query to " + httpAddress + " for peer '" + expectedPeerId
              + "' was answered by peer '" + answering + "'");
        return new PeerCapabilityQuery.Advertisement(answering, "test", Set.of(PeerCapabilities.SCHEMA_DELTA), false, Set.of(),
            relays.get(httpAddress));
      }
    }

    private static RaftHAServer newDetachedServer(final String serverList) {
      return newDetachedServer(serverList, false);
    }

    private static RaftHAServer newDetachedServer(final String serverList, final boolean ssl) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);
      config.setValue(GlobalConfiguration.NETWORK_USE_SSL, ssl);
      final FakeArcadeDBServer arcadeServer = FakeArcadeDBServer.create("ArcadeDB_0", new ContextConfiguration());
      final HttpServer httpServer = HTTP_SERVERS.listeningOn(arcadeServer, 2480);
      arcadeServer.httpServer(httpServer);
      return new RaftHAServer(arcadeServer, config);
    }
  }

  /**
   * Issue #7332: the shared-endpoint recovery #7256 added could not fire on the deployment shape its own
   * documentation named.
   * <p>
   * With no {@code http} port declared, a peer's HTTP endpoint is derived as ITS Raft host plus THIS node's HTTP
   * port. On a cluster whose peers differ by port rather than by host that address is this node's own for every
   * peer, so {@code sharedEndpointOf} withheld it every single time - correctly, dialling ourselves comes straight
   * back - and the second pass never had an endpoint to probe. Every piece of code was individually right and the
   * feature still could not work where it was written to work.
   * <p>
   * The signal left on that shape is the port offset: a multi-node cluster on one host is configured by moving both
   * ports together, so {@code localHttpPort + (peerRaftPort - localRaftPort)} names the peer's listener when the
   * convention holds and names nothing when it does not. A guess is admissible here for exactly one reason, and the
   * tests below are mostly about keeping that reason true: the candidate reaches only
   * {@link PeerDialAddress#sharedEndpoint()}, never {@link PeerDialAddress#httpAddress()}, so the only caller that
   * can act on it is the read-only probe that credits whoever ANSWERS rather than whoever it addressed.
   */
  @Nested
  class SharedEndpointPortOffset {

    /** Local node first (raft 2434), then two peers differing only by Raft port. No http port anywhere. */
    private static final String COLLAPSED_BY_PORT = "localhost:2434,localhost:2435,localhost:2436";
    private static final String LOCAL             = "localhost_2434";
    private static final String PEER_A            = "localhost_2435";
    private static final String PEER_B            = "localhost_2436";

    private CapturingTestLogger logger;

    @BeforeEach
    void installLogger() {
      logger = CapturingTestLogger.install();
    }

    @AfterEach
    void uninstallLogger() {
      logger.uninstall();
    }

    // ---------------------------------------------------------------------------------------------------------
    // The candidate itself
    // ---------------------------------------------------------------------------------------------------------

    /** The whole point: on the target shape every peer now has an endpoint worth asking. */
    // Regression for #7332: each peer collapsed onto this nodes own address gets its own candidate
    @Test
    void eachPeerCollapsedOntoThisNodesOwnAddressGetsItsOwnCandidate() {
      final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, 2480);

      assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(PEER_A))).isEqualTo("localhost:2481");
      assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isEqualTo("localhost:2482");
    }

    /** Never this node itself, whichever way it is asked. */
    // Regression for #7332: there is no candidate for this node or for a peer that does not exist
    @Test
    void thereIsNoCandidateForThisNodeOrForAPeerThatDoesNotExist() {
      final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, 2480);

      assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(LOCAL))).isNull();
      assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf("nobody_9999"))).isNull();
      assertThat(raft.getPortOffsetPeerHttpAddress(null)).isNull();
    }

    /**
     * A declared HTTP port is a statement about which node owns which port, and guessing over it would be replacing
     * an operator's answer with ours. A declared address that STILL collides is a configuration fault to report -
     * which the refusal already does, naming HA_SERVER_LIST - not one to work around.
     */
    // Regression for #7332: a peer that declared its http port is never guessed at
    @Test
    void aPeerThatDeclaredItsHttpPortIsNeverGuessedAt() {
      final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2490,localhost:2436:2490", 2480);

      assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(PEER_A))).isNull();
      assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isNull();
    }

    /**
     * Peers that differ by HOST share a Raft port, so the derivation never collapsed them onto one another in the
     * first place and there is nothing to recover. An offset of zero would also name our own port.
     */
    // Regression for #7332: peers that differ by host rather than by port have no offset to carry
    @Test
    void peersThatDifferByHostRatherThanByPortHaveNoOffsetToCarry() {
      final RaftHAServer raft = newDetachedServer("hostA:2434,hostB:2434,hostC:2434", 2480);

      assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf("hostB_2434"))).isNull();
      assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf("hostC_2434"))).isNull();
    }

    /** No local HTTP listener yet means no offset to carry it over, rather than a port derived from -1. */
    // Regression for #7332: there is no candidate before the local http listener is up
    @Test
    void thereIsNoCandidateBeforeTheLocalHttpListenerIsUp() {
      final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, -1);

      assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(PEER_A))).isNull();
    }

    /** Arithmetic that leaves the port range is not a port, and must not silently wrap into one that looks real. */
    // Regression for #7332: an offset that leaves the port range yields no candidate
    @Test
    void anOffsetThatLeavesThePortRangeYieldsNoCandidate() {
      // 2480 + (1 - 65000) is negative; 2480 + (65535 - 100) is past 65535.
      assertThat(newDetachedServer("localhost:65000,localhost:1", 2480)
          .getPortOffsetPeerHttpAddress(RaftPeerId.valueOf("localhost_1"))).isNull();
      assertThat(newDetachedServer("localhost:100,localhost:65535", 2480)
          .getPortOffsetPeerHttpAddress(RaftPeerId.valueOf("localhost_65535"))).isNull();
    }

    // ---------------------------------------------------------------------------------------------------------
    // How it reaches the probe, and how it must not reach anything else
    // ---------------------------------------------------------------------------------------------------------

    /** The candidate is offered as a shared endpoint and never as a dial address. */
    // Regression for #7332: the candidate is offered only through shared endpoint
    @Test
    void theCandidateIsOfferedOnlyThroughSharedEndpoint() {
      final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, 2480);

      final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf(PEER_A), "peer");

      assertThat(dial.refused())
          .as("a resync, a verify or a forwarded write must still refuse: nothing here identifies the peer")
          .isTrue();
      assertThat(dial.httpAddress()).isNull();
      assertThat(dial.httpsAddress()).isNull();
      assertThat(dial.sharedEndpoint().httpAddress()).isEqualTo("localhost:2481");
      assertThat(dial.sharedEndpoint().httpsAddress())
          .as("an offset between two Raft ports says nothing about a third port")
          .isNull();
    }

    /**
     * End to end: the round that used to make no second-pass probe at all now asks each candidate and credits the
     * peer that answers. This is the assertion the issue is actually about - everything above is the shape of it.
     */
    // Regression for #7332: the second pass now probes and credits a peer on the collapsed shape
    @Test
    void theSecondPassNowProbesAndCreditsAPeerOnTheCollapsedShape() {
      final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, 2480);
      final ScriptedProber prober = new ScriptedProber();
      prober.sharedAnswers.put("localhost:2481",
          new PeerCapabilityQuery.Advertisement(PEER_A, "26.10.1", Set.of(PeerCapabilities.SCHEMA_DELTA)));
      raft.setCapabilityProber(prober);

      raft.refreshPeerCapabilities();

      assertThat(prober.calls)
          .as("one probe per candidate, on the shared-endpoint route (expectedPeerId null)")
          .containsExactlyInAnyOrder("null@localhost:2481", "null@localhost:2482");
      assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_A))
          .as("the peer that identified itself behind a collapsed address is negotiable after all")
          .isNotNull();
      assertThat(raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA))
          .as("and the one nothing answered for is still a 'no', so the mechanism still fails the cheap way")
          .containsExactly(PEER_B);
    }

    /**
     * The second pass's diagnostic goes through {@code describeProbeFailure} like the first pass's does. A bare
     * {@code SocketTimeoutException} carries no message, so {@code getMessage()} printed "null" for precisely the
     * failure that helper was written to describe.
     */
    // Regression for #7332: a second pass probe failure with no message still says what
    @Test
    void aSecondPassProbeFailureWithNoMessageStillSaysWhat() {
      final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, 2480);
      raft.setCapabilityProber(new ScriptedProber());

      raft.refreshPeerCapabilities();

      assertThat(logger.formattedAt(Level.FINE))
          .filteredOn(line -> line.contains("No peer identified itself at the shared address"))
          .as("the reason survives; it used to read 'null', which is the one thing a reason must never be")
          .isNotEmpty()
          .allSatisfy(line -> assertThat(line).contains("SocketTimeoutException").doesNotContain(": null"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // The port parser the offset rests on
    // ---------------------------------------------------------------------------------------------------------

    // Regression for #7332: extract port reads the last colon and refuses everything else
    @Test
    void extractPortReadsTheLastColonAndRefusesEverythingElse() {
      assertThat(PeerEndpoints.extractPort("localhost:2424")).isEqualTo(2424);
      assertThat(PeerEndpoints.extractPort("[::1]:2424")).as("a bracketed IPv6 literal keeps its own colons")
          .isEqualTo(2424);
      assertThat(PeerEndpoints.extractPort("fe80::1:2424")).as("an unbracketed one is separated by the LAST colon")
          .isEqualTo(2424);
      assertThat(PeerEndpoints.extractPort("[::1]")).as("a bracketed literal with no port has none").isEqualTo(-1);
      assertThat(PeerEndpoints.extractPort("localhost")).isEqualTo(-1);
      assertThat(PeerEndpoints.extractPort("localhost:")).isEqualTo(-1);
      assertThat(PeerEndpoints.extractPort("localhost:http")).isEqualTo(-1);
      assertThat(PeerEndpoints.extractPort("")).isEqualTo(-1);
      assertThat(PeerEndpoints.extractPort(null)).isEqualTo(-1);
    }

    /**
     * A prober that answers from a per-address script on the shared-endpoint route and fails with a MESSAGE-LESS
     * exception everywhere else, which is what the diagnostic assertion above needs.
     */
    private static final class ScriptedProber implements RaftHAServer.CapabilityProber {
      private final List<String>                                     calls         = new ArrayList<>();
      private final Map<String, PeerCapabilityQuery.Advertisement>    sharedAnswers = new HashMap<>();

      @Override
      public PeerCapabilityQuery.Advertisement probe(final String expectedPeerId, final String httpAddress,
          final String httpsAddress, final String clusterToken) throws IOException {
        calls.add(expectedPeerId + "@" + httpAddress);
        final PeerCapabilityQuery.Advertisement answer = expectedPeerId == null ? sharedAnswers.get(httpAddress) : null;
        if (answer == null)
          throw new SocketTimeoutException();
        return answer;
      }
    }
  }

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
   */
  @Nested
  class PeerReadersUseLiveConfiguration {

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
      // Regression for #10956: a peer that joined at runtime is a node of the matrix
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
      // Regression for #10956: a joined peer that does not answer is unreachable
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
      // Regression for #10956: a declared peer the configuration dropped is flagged
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
      // Regression for #10956: an unreachable peer the declared list names is probed and refused
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
      // Regression for #10956: the duplicate address check compares with the members only
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
      // Regression for #10956: a member re added is still the idempotent no op
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
}
