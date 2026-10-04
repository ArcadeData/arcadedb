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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.raft.PostSecuritySeedHandler.DeclaredPeerHttpAddress;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8689: a {@code connect cluster host:raftPort:httpPort} served by a FOLLOWER wrote the declared HTTP address
 * into the follower's own map only. The security seed runs on the leader, and its #7511 capability gate probes the
 * new peer at the address the LEADER's map holds - for a brand-new or previously removed peer, a derived guess that
 * on a cluster whose ports are not in step names the wrong listener. The verb then answered 503 with
 * {@code failedSeeds: [groups, API tokens]} for a peer that was up and declared correctly.
 * <p>
 * The fix carries the declaration in the seed request to the leader, which records it - only for a committed member,
 * never over its own server list - and answers the request with a seed that started after the record. These pin each
 * link of that chain without a live cluster; {@code Issue7401ConnectClusterJoinsPeerIT} drives it end to end.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8689FollowerDeclaredHttpAddressTest {

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
  @Test
  void aCommittedMemberWithNoEntryGetsTheDeclaredAddress() {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();

    assertThat(RaftHAServer.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, MEMBER, JOINED), httpAddresses,
        Map.of(), JOINED, ADDRESS)).isTrue();
    assertThat(httpAddresses.get(JOINED)).isEqualTo(ADDRESS);
  }

  /** A derived or runtime-written entry is replaced, as a connect cluster served by this node would have replaced it. */
  @Test
  void aDerivedEntryIsReplaced() {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>(Map.of(JOINED, "localhost:22944"));

    assertThat(RaftHAServer.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, JOINED), httpAddresses, Map.of(),
        JOINED, ADDRESS)).isTrue();
    assertThat(httpAddresses.get(JOINED)).isEqualTo(ADDRESS);
  }

  /** The same address again changes nothing, and says so: the caller must not throw away a seed for a no-op. */
  @Test
  void theSameAddressAgainIsNotAChange() {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>(Map.of(JOINED, ADDRESS));

    assertThat(RaftHAServer.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, JOINED), httpAddresses, Map.of(),
        JOINED, ADDRESS)).isFalse();
  }

  /** The address this node's own server list declared is the operator's word here, and another node does not outrank it. */
  @Test
  void anEntryThisNodesServerListDeclaredIsKept() {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>(Map.of(JOINED, "localhost:2490"));

    assertThat(RaftHAServer.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, JOINED), httpAddresses,
        Map.of(JOINED, "localhost:2490"), JOINED, ADDRESS)).isFalse();
    assertThat(httpAddresses.get(JOINED)).isEqualTo("localhost:2490");
  }

  /** A seed request cannot plant an address for a node that is not a member. */
  @Test
  void aPeerOutsideTheCommittedConfigurationIsRefused() {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();

    assertThat(RaftHAServer.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, MEMBER), httpAddresses, Map.of(),
        JOINED, ADDRESS)).isFalse();
    assertThat(RaftHAServer.recordAdmittedPeerHttpAddress(LEADER, null, httpAddresses, Map.of(), JOINED, ADDRESS))
        .as("an unreadable configuration is not proof of membership").isFalse();
    assertThat(httpAddresses).isEmpty();
  }

  /** Nor can it rewrite this node's own address. */
  @Test
  void thisNodesOwnAddressIsNeverRecorded() {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();

    assertThat(RaftHAServer.recordAdmittedPeerHttpAddress(LEADER, committed(LEADER, MEMBER), httpAddresses, Map.of(),
        LEADER, ADDRESS)).isFalse();
    assertThat(httpAddresses).isEmpty();
  }

  // ------------------------------------------------------------------ the request fields

  @Test
  void aRequestWithoutADeclarationReadsAsNone() {
    assertThat(PostSecuritySeedHandler.readDeclaredPeerHttpAddress(new JSONObject().put("reason", "an admission")))
        .isNull();
  }

  @Test
  void aRequestWithADeclarationReadsIt() {
    assertThat(PostSecuritySeedHandler.readDeclaredPeerHttpAddress(new JSONObject()
        .put(PostSecuritySeedHandler.ADMITTED_PEER_ID, JOINED.toString())
        .put(PostSecuritySeedHandler.DECLARED_HTTP_ADDRESS, ADDRESS)))
        .isEqualTo(new DeclaredPeerHttpAddress(JOINED.toString(), ADDRESS));
  }

  /** Each malformed shape is the caller's mistake: a 400, never a value written into the dial map. */
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
        declaration("[::1:2480")))
      assertThatThrownBy(() -> PostSecuritySeedHandler.readDeclaredPeerHttpAddress(bad)).as("%s", bad)
          .isInstanceOf(IllegalArgumentException.class);
  }

  /** The forms a real peer address takes are accepted, a bracketed IPv6 literal included. */
  @Test
  void everyPeerAddressFormIsAccepted() {
    for (final String address : List.of("localhost:2482", "10.0.0.7:2480", "arcadedb-2.arcadedb.default.svc:2480",
        "[::1]:2480", "[fe80::1%eth0]:2480"))
      assertThat(PostSecuritySeedHandler.readDeclaredPeerHttpAddress(declaration(address)).httpAddress()).as(address)
          .isEqualTo(address);
  }

  /**
   * The leader answers only after the seed in flight and then a fresh one, so the deadline has room for two whole
   * retry budgets; anything less lets the fresh seed time out behind one that spent its budget on the stale address.
   */
  @Test
  void aDeclaredAdmissionIsGivenRoomForTwoSeeds() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT, 7_000L);

    assertThat(ClusterSecuritySeedQuery.declaredAdmissionReportTimeoutMs(configuration) - 2 * 7_000L)
        .isEqualTo(ClusterSecuritySeedQuery.reportTimeoutMs(configuration) - 7_000L);
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

      final CompletableFuture<List<String>> answer = CompletableFuture.supplyAsync(
          () -> seeder.seedAfterInFlightAndReport("the admission", 10_000L));
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
    Optional<List<String>> seedSecurityStateForAdmission(final String admittedPeer, final String admittedPeerId,
        final String declaredHttpAddress) throws IOException {
      seeds.add(admittedPeer + " declaring " + admittedPeerId + " at " + declaredHttpAddress);
      return Optional.of(List.of());
    }
  }

  /** A declared HTTP port reaches the seed request, under the id the join gave the peer. */
  @Test
  void connectClusterSendsTheDeclaredAddressWithTheSeedRequest() {
    final RecordingPlugin plugin = new RecordingPlugin();

    assertThat(plugin.connectClusterAndReportSeed("localhost:22898:2482")).contains(List.of());
    assertThat(plugin.seeds).containsExactly("localhost:22898:2482 declaring localhost_22898 at localhost:2482");
  }

  /** Without a declared port there is nothing to send, and the request is the one it always was. */
  @Test
  void connectClusterWithoutADeclaredPortSendsNoDeclaration() {
    final RecordingPlugin plugin = new RecordingPlugin();

    plugin.connectClusterAndReportSeed("localhost:22898");
    assertThat(plugin.seeds).containsExactly("localhost:22898 declaring nothing");
  }
}
