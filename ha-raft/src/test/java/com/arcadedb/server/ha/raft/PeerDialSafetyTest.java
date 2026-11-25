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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.PeerRequest;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.http.HttpRequest;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Safety of peer-to-peer dials: refusing unreachable peers, the plaintext dial scheme, divergence-probe leader
 * identity, the peer-query body deadline and the single dial builder.
 */
class PeerDialSafetyTest {

  /**
   * Issue #7514, the two halves of the refusal that do not need a live cluster to pin.
   * <ul>
   *   <li>{@link PeerReachability} - what the pre-flight probe answers, and the cases in which it
   *       deliberately answers nothing at all;</li>
   *   <li>the message a membership change that ran out of budget is reported with, which is what the caller
   *       sees when the probe passed and the change still did not commit.</li>
   * </ul>
   * The entry points themselves are driven end to end by {@code UnreachablePeerFailsFastIT}.
   */
  @Nested
  class UnreachablePeerRefusal {

    private static final long PROBE_TIMEOUT_MS = 2_000;

    /**
     * The probe is one-sided: something listening means it reports nothing, and the request proceeds exactly
     * as it did before the fix. Proving that is what keeps the probe from being able to turn a working add
     * into a refusal.
     */
    // Regression for #7514: a listening port is not reported unreachable
    @Test
    void aListeningPortIsNotReportedUnreachable() throws Exception {
      try (final ServerSocket listening = new ServerSocket(0)) {
        assertThat(PeerReachability.unreachableReason("localhost:" + listening.getLocalPort(), PROBE_TIMEOUT_MS))
            .isNull();
      }
    }

    /** The reported case: the address parses, the host is up, and nothing is bound to the port. */
    // Regression for #7514: a port nothing is bound to is reported with a reason
    @Test
    void aPortNothingIsBoundToIsReportedWithAReason() throws Exception {
      final String reason = PeerReachability.unreachableReason(addressNothingListensOn(), PROBE_TIMEOUT_MS);

      assertThat(reason).isNotNull().isNotBlank();
    }

    /**
     * The budget covers the whole probe, name resolution included. Before the review of PR #7562 it did not:
     * {@code new InetSocketAddress(host, port)} resolves in its constructor, before {@code Socket.connect}'s
     * timeout argument applies to anything, so a slow or unreachable resolver held the caller for as long as
     * the platform's resolver took - the held-worker-thread problem this whole change exists to remove,
     * reintroduced through DNS.
     * <p>
     * Driven with a name that cannot resolve. The bound is a tripwire, not a latency budget: the point is
     * that the caller is released on the probe's own budget rather than on the resolver's.
     */
    // Regression for #7514: the budget covers name resolution and not only the handshake
    @Test
    void theBudgetCoversNameResolutionAndNotOnlyTheHandshake() {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();

      final String reason = PeerReachability.unreachableReason(
          "issue7514-no-such-host-" + System.nanoTime() + ".invalid:2434", PROBE_TIMEOUT_MS);

      watch.assertGaveUpWithin(PROBE_TIMEOUT_MS * 5,
          "a probe budget that covers name resolution from one that starts only after the resolver answers");
      assertThat(reason).isNotNull().isNotBlank();
    }

    /**
     * "Not probed" must not read as "unreachable". {@code RaftPeerAddressResolver} always produces
     * {@code host:port}, so these are the defensive half of the contract - and getting them wrong would refuse
     * an add for an address the probe simply could not dial.
     */
    // Regression for #7514: an address with nothing to dial is not reported unreachable
    @Test
    void anAddressWithNothingToDialIsNotReportedUnreachable() {
      assertThat(PeerReachability.unreachableReason(null, PROBE_TIMEOUT_MS)).isNull();
      assertThat(PeerReachability.unreachableReason("localhost", PROBE_TIMEOUT_MS)).isNull();
      assertThat(PeerReachability.unreachableReason("localhost:", PROBE_TIMEOUT_MS)).isNull();
      assertThat(PeerReachability.unreachableReason("localhost:raft", PROBE_TIMEOUT_MS)).isNull();
      assertThat(PeerReachability.unreachableReason("localhost:0", PROBE_TIMEOUT_MS)).isNull();
      assertThat(PeerReachability.unreachableReason("localhost:70000", PROBE_TIMEOUT_MS)).isNull();
    }

    /**
     * The refusal has to be an {@link IllegalArgumentException}: that is the single type
     * {@code AbstractServerHttpHandler} maps to HTTP 400 and {@code ArcadeDbGrpcAdminService} maps to gRPC
     * {@code INVALID_ARGUMENT}, so it is what makes the gRPC {@code ConnectCluster} RPC answer the same way
     * the HTTP verb does without a second mapping arm. It also has to name the address and the escape hatch,
     * because a probe budget that is too short for the network is the one way this refusal can be wrong.
     */
    // Regression for #7514: the refusal maps to invalid argument and http400
    @Test
    void theRefusalMapsToInvalidArgumentAndHttp400() {
      final UnreachablePeerException refusal = new UnreachablePeerException("localhost_2436", "localhost:2436",
          "Connection refused", "localhost_2435", PROBE_TIMEOUT_MS);

      assertThat(refusal).isInstanceOf(IllegalArgumentException.class);
      assertThat(refusal.getMessage())
          .contains("localhost_2436")
          .contains("localhost:2436")
          .contains("Connection refused")
          .contains(GlobalConfiguration.HA_ADD_PEER_PROBE_TIMEOUT.getKey());
    }

    /**
     * Neither add-peer route is leader-routed, so the probe runs on whichever node the request landed on and
     * answers from THAT node's view of the network. Naming it is what separates "the peer is down" from "this
     * follower cannot see a peer the leader can", which are otherwise the same refusal.
     */
    // Regression for #7514: the refusal names the node that probed
    @Test
    void theRefusalNamesTheNodeThatProbed() {
      assertThat(new UnreachablePeerException("localhost_2436", "localhost:2436", "Connection refused",
          "localhost_2435", PROBE_TIMEOUT_MS).getMessage()).contains("probed from node 'localhost_2435'");

      assertThat(new UnreachablePeerException("localhost_2436", "localhost:2436", "Connection refused", null,
          PROBE_TIMEOUT_MS).getMessage()).doesNotContain("probed from node");
    }

    /**
     * The two branches on which the probe must not refuse, both of which would otherwise be regressions:
     * re-adding a peer that is already a member has to stay the idempotent no-op {@code connect cluster}
     * documents (even for a member that is down), and the operator's escape hatch has to actually restore the
     * pre-probe behaviour rather than merely shortening the wait.
     */
    // Regression for #7514: the probe is skipped for a member and when the operator turned it off
    @Test
    void theProbeIsSkippedForAMemberAndWhenTheOperatorTurnedItOff() throws Exception {
      final String unreachable = addressNothingListensOn();

      assertThat(PeerReachability.addRefusalReason(true, PROBE_TIMEOUT_MS, unreachable))
          .as("already a member").isNull();
      assertThat(PeerReachability.addRefusalReason(false, 0, unreachable))
          .as("probe disabled with 0").isNull();
      assertThat(PeerReachability.addRefusalReason(false, -1, unreachable))
          .as("probe disabled with a negative budget").isNull();
      assertThat(PeerReachability.addRefusalReason(false, PROBE_TIMEOUT_MS, unreachable))
          .as("neither skip applies").isNotNull();
    }

    /**
     * When the probe passes and the configuration change still does not commit, the report leads with a
     * sentence and keeps the Ratis text as a labelled tail - instead of being nothing but the serialized
     * {@code SetConfigurationRequest} the issue quotes.
     * <p>
     * Driven with a zero budget so the give-up branch is reached at once rather than after 90 s.
     */
    // Regression for #7514: a membership change that ran out of budget is reported in a sentence
    @Test
    void aMembershipChangeThatRanOutOfBudgetIsReportedInASentence() throws Exception {
      final String ratisText = "Failed SetConfigurationRequest:client-48FB->localhost_2435@group-E6A6, cid=11, "
          + "seq=null, RW, null, ADD, servers:[localhost_2436|localhost:2436], listeners:[] for 60 attempts "
          + "with RetryLimited(maxAttempts=60, sleepTime=1s)";

      final FakeRaftHAServer server = FakeRaftHAServer.detached();
      final RaftClient client = mock(RaftClient.class);
      final AdminApi admin = mock(AdminApi.class);
      final RaftClientReply reply = mock(RaftClientReply.class);

      server.client(client);
      when(client.admin()).thenReturn(admin);
      when(reply.isSuccess()).thenReturn(false);
      when(reply.getException()).thenReturn(new RaftException(ratisText));
      when(admin.setConfiguration(org.mockito.ArgumentMatchers.<SetConfigurationRequest.Arguments>any()))
          .thenReturn(reply);
      server.livePeers(List.of(peer("A"), peer("B")));
      server.committedPeers(List.of(peer("A"), peer("B")));
      server.httpAddresses(new HashMap<>());
      server.raftGroup(RaftGroup.valueOf(RaftGroupId.randomId()));

      assertThatThrownBy(() -> new RaftClusterManager(server, 0).addPeer("D", "localhost:2447"))
          .isInstanceOf(ConfigurationException.class)
          .hasMessageContaining("Failed to add peer D at localhost:2447")
          .hasMessageContaining("did not commit within 0 ms")
          .hasMessageContaining("same cluster name and cluster token")
          .hasMessageContaining("Raft reported: " + ratisText);
    }

    /** The removal path shares the helper and gets its own sentence, not the add's. */
    // Regression for #7514: a removal that ran out of budget is reported in its own sentence
    @Test
    void aRemovalThatRanOutOfBudgetIsReportedInItsOwnSentence() throws Exception {
      final FakeRaftHAServer server = FakeRaftHAServer.detached();
      final RaftClient client = mock(RaftClient.class);
      final AdminApi admin = mock(AdminApi.class);
      final RaftClientReply reply = mock(RaftClientReply.class);

      server.client(client);
      when(client.admin()).thenReturn(admin);
      when(reply.isSuccess()).thenReturn(false);
      when(reply.getException()).thenReturn(new RaftException("no leader"));
      when(admin.setConfiguration(org.mockito.ArgumentMatchers.<SetConfigurationRequest.Arguments>any()))
          .thenReturn(reply);
      server.livePeers(List.of(peer("A"), peer("B"), peer("C")));
      server.committedPeers(List.of(peer("A"), peer("B"), peer("C")));
      server.httpAddresses(new HashMap<>());

      assertThatThrownBy(() -> new RaftClusterManager(server, 0).removePeer("C"))
          .isInstanceOf(ConfigurationException.class)
          .hasMessageContaining("Failed to remove peer C")
          .hasMessageContaining("voting majority")
          .hasMessageContaining("Raft reported: no leader");
    }

    // -----------------------------------------------------------------------------------------------

    private static RaftPeer peer(final String id) {
      return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress("localhost:2444").build();
    }

    private static String addressNothingListensOn() throws IOException {
      try (final ServerSocket socket = new ServerSocket(0)) {
        return "localhost:" + socket.getLocalPort();
      }
    }
  }

  /**
   * The scheme the two dials that were still hardcoded to {@code http://} now choose (issue #7546, folded into
   * issue #7563): the bootstrap-state probe and the remote-shutdown command.
   * <p>
   * Both carry the cluster token - the probe in {@code X-ArcadeDB-Cluster-Token}, the shutdown in
   * {@code Authorization: Bearer} - so on a cluster with {@code arcadedb.ssl.enabled} set they were putting it
   * on the wire in clear text while every sibling dial was already encrypted. What is pinned here is the
   * DECISION in isolation; that a decision of {@code https} is then carried by a real TLS socket, against a
   * certificate issued for the name dialled and validated through the configured truststore, is pinned by
   * {@link HaTlsPeerDialIT} against a live two-node cluster.
   * <p>
   * The three rows of each table are the three states an operator can be in, and the middle one is the one that
   * must NOT be a refusal: an SSL cluster that never declared the optional 5th field of
   * {@code arcadedb.ha.serverList} has only the plain listener to be reached on, and every sibling
   * ({@code LeaderDial}, {@link PeerCapabilityQuery#chooseUrl}, {@link LeaderDatabaseQuery#chooseEndpoint})
   * falls back there rather than breaking the cluster.
   */
  @Nested
  class PlaintextPeerDialScheme {

    private static final String HTTP_ADDR  = "peer-1:2480";
    private static final String HTTPS_ADDR = "peer-1:2490";

    private static final RaftPeerId PEER = RaftPeerId.valueOf("peer-1");
    /** Small enough that the retryable control finishes fast, large enough to fit several backoffs. */
    private static final long       BUDGET_MS  = 300L;
    private static final long       BACKOFF_MS = 10L;

    // ------------------------------------------------------------------ bootstrap-state probe

    // Regression for #7563: the bootstrap probe takes the encrypted endpoint when ssl is on and one resolves
    @Test
    void theBootstrapProbeTakesTheEncryptedEndpointWhenSslIsOnAndOneResolves() {
      assertThat(BootstrapElection.chooseUrl(HTTP_ADDR, HTTPS_ADDR, true))
          .isEqualTo("https://peer-1:2490/api/v1/cluster/bootstrap-state");
    }

    // Regression for #7563: the bootstrap probe falls back to the plain listener when no https endpoint resolves
    @Test
    void theBootstrapProbeFallsBackToThePlainListenerWhenNoHttpsEndpointResolves() {
      assertThat(BootstrapElection.chooseUrl(HTTP_ADDR, null, true))
          .as("an SSL cluster that declared no 'https' ports must still be able to bootstrap")
          .isEqualTo("http://peer-1:2480/api/v1/cluster/bootstrap-state");
    }

    // Regression for #7563: the bootstrap probe ignores an https endpoint when ssl is off
    @Test
    void theBootstrapProbeIgnoresAnHttpsEndpointWhenSslIsOff() {
      assertThat(BootstrapElection.chooseUrl(HTTP_ADDR, HTTPS_ADDR, false))
          .isEqualTo("http://peer-1:2480/api/v1/cluster/bootstrap-state");
    }

    // Regression for #7563: a peer with no address at all yields no url rather than a malformed one
    @Test
    void aPeerWithNoAddressAtAllYieldsNoUrlRatherThanAMalformedOne() {
      assertThat(BootstrapElection.chooseUrl(null, null, true)).isNull();
      assertThat(BootstrapElection.chooseUrl(null, null, false)).isNull();
    }

    // Regression for #7563: the encrypted probe carries exactly the credentials the plain one did
    @Test
    void theEncryptedProbeCarriesExactlyTheCredentialsThePlainOneDid() {
      final HttpRequest request = BootstrapElection.bootstrapStateRequestTo(
          BootstrapElection.chooseUrl(HTTP_ADDR, HTTPS_ADDR, true), true, "the-token", 1234L);

      assertThat(request.uri().getScheme()).isEqualTo("https");
      assertThat(request.method()).isEqualTo("POST");
      assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).hasValue("the-token");
      assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).hasValue("root");
    }

    // ------------------------------------------------------------------ remote shutdown

    // Regression for #7563: the remote shutdown takes the encrypted endpoint when ssl is on and one resolves
    @Test
    void theRemoteShutdownTakesTheEncryptedEndpointWhenSslIsOnAndOneResolves() {
      assertThat(RaftHAPlugin.shutdownUrl(new PeerDialAddress(HTTP_ADDR, HTTPS_ADDR, null, null), true))
          .isEqualTo("https://peer-1:2490/api/v1/server");
    }

    // Regression for #7563: the remote shutdown falls back to the plain listener when no https endpoint resolves
    @Test
    void theRemoteShutdownFallsBackToThePlainListenerWhenNoHttpsEndpointResolves() {
      assertThat(RaftHAPlugin.shutdownUrl(new PeerDialAddress(HTTP_ADDR, null, null, null), true))
          .isEqualTo("http://peer-1:2480/api/v1/server");
    }

    // Regression for #7563: the remote shutdown ignores an https endpoint when ssl is off
    @Test
    void theRemoteShutdownIgnoresAnHttpsEndpointWhenSslIsOff() {
      assertThat(RaftHAPlugin.shutdownUrl(new PeerDialAddress(HTTP_ADDR, HTTPS_ADDR, null, null), false))
          .isEqualTo("http://peer-1:2480/api/v1/server");
    }

    // ------------------------------------------------------------------ "cannot happen twice" is FATAL

    /**
     * The branch itself: an HTTPS probe with no client to send it on answers {@code FATAL}, not
     * {@code RETRYABLE} (code review on PR #7838), and - the other half of the security property - it answers
     * rather than quietly falling back to the plain listener.
     * <p>
     * Driven through {@code queryPeer}, which is where the decision is made. The no-client branch answers
     * before the election object is otherwise touched, so a bare instance is enough to reach it.
     */
    // Regression for #7563: an https probe with no client is fatal rather than retried or downgraded
    @Test
    void anHttpsProbeWithNoClientIsFatalRatherThanRetriedOrDowngraded() throws Exception {
      final BootstrapElection election = new BootstrapElection(null, null);

      final BootstrapElection.ProbeOutcome outcome = election.queryPeer(PEER,
          "https://peer-1:2490/api/v1/cluster/bootstrap-state", Set.of("db"), 1_000L, null).get();

      assertThat(outcome.result())
          .as("the client is built once per fan-out and never rebuilt, so retrying cannot change this")
          .isEqualTo(BootstrapElection.ProbeResult.FATAL);
      assertThat(outcome.detail()).contains("truststore");
    }

    /**
     * Why that distinction is worth a test: {@code collectRemoteStatesWithRetry} drops a {@code FATAL} peer
     * immediately and re-probes a {@code RETRYABLE} one until {@code HA_BOOTSTRAP_TIMEOUT_MS} runs out, so
     * reporting the outcome above as retryable cost the full budget - two minutes by default - on every
     * election of a cluster with a misconfigured {@code arcadedb.ssl.trustStore}, to reach the conclusion that
     * was available on the first attempt.
     * <p>
     * The assertion counts <b>probe attempts</b> rather than elapsed time: the count is what the two outcomes
     * actually differ by, and it says the same thing on a loaded runner as on an idle one.
     */
    // Regression for #7563: an outcome that cannot change on retry is not retried
    @Test
    void anOutcomeThatCannotChangeOnRetryIsNotRetried() {
      final AtomicInteger fatalAttempts = new AtomicInteger();
      final List<RaftPeerId> fatalAssumedEmpty = new ArrayList<>();
      BootstrapElection.collectRemoteStatesWithRetry(
          Map.of(PEER, "https://peer-1:2490/api/v1/cluster/bootstrap-state"),
          (peerId, url, attemptMs) -> {
            fatalAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(
                BootstrapElection.ProbeOutcome.fatal("no HTTPS client could be built from the cluster truststore"));
          },
          BUDGET_MS, BUDGET_MS, BACKOFF_MS, () -> true, fatalAssumedEmpty);

      assertThat(fatalAttempts)
          .as("a peer whose probe can never succeed must be probed once, not until the budget runs out")
          .hasValue(1);
      assertThat(fatalAssumedEmpty)
          .as("it still reaches the SEVERE line that names the peers the election had to assume empty")
          .containsExactly(PEER);

      // The control: the SAME budget, the same backoff, an outcome that says "try again" - and it does.
      final AtomicInteger retryableAttempts = new AtomicInteger();
      BootstrapElection.collectRemoteStatesWithRetry(
          Map.of(PEER, "https://peer-1:2490/api/v1/cluster/bootstrap-state"),
          (peerId, url, attemptMs) -> {
            retryableAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(BootstrapElection.ProbeOutcome.retryable("HTTP 503"));
          },
          BUDGET_MS, BUDGET_MS, BACKOFF_MS, () -> true, new ArrayList<>());

      assertThat(retryableAttempts.get())
          .as("a transient failure is still retried, so the two outcomes are genuinely distinguished")
          .isGreaterThan(1);
    }
  }

  /**
   * Which HTTPS endpoint the bootstrap-divergence probe of {@code ArcadeStateMachine.verifyBootstrapDivergence}
   * may dial, once that probe is allowed to choose a scheme at all (issues #7546 / #7563).
   * <p>
   * The plain-HTTP half of that call site asks the two questions issue #6202 requires of an address acted on
   * unattended - it must identify one peer, and it must not be our own - through
   * {@link RaftHAServer#getUnambiguousPeerHttpAddress}. The encrypted half has to be asked the same two
   * separately: the endpoints come from two independent fields of {@code arcadedb.ha.serverList} (the 3rd and
   * the 5th), each with its own derive fallback, so an HTTP verdict says nothing about the HTTPS one.
   * <p>
   * {@link RaftHAServer#getLeaderHttpsAddress()} answers only the second of the two, and says so: it resolves
   * through the raw resolver on the argument that "an address that names the wrong node is caught by the
   * receiving node's one-hop refusal, since the forward carries FORWARDED_TO_LEADER_HEADER". That argument is
   * the leader <em>forward</em>'s, and it does not transfer here - {@code POST /api/v1/cluster/bootstrap-state}
   * answers with the receiving node's own state and refuses nothing - so the probe needs the guard the
   * forward can do without.
   * <p>
   * {@link PeerDialAddress#resolve} is where both questions are already answered for both schemes, from one
   * peer identity. What this test pins is that on a topology the HTTP guard waves through, the two are not
   * interchangeable.
   */
  @Nested
  class DivergenceProbeLeaderIdentity {

    /** This node is the first entry; the two others declare distinct http ports and SHARE one https port. */
    private static final String SHARED_HTTPS_PORT_CLUSTER =
        "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2491";

    private static final RaftPeerId LEADER = RaftPeerId.valueOf("localhost_2435");

    /**
     * The gap: the HTTP address of the leader identifies it alone, so the call site proceeds, while the HTTPS
     * address two peers answer to identifies neither - and the scheme selection prefers it.
     */
    // Regression for #7563: the leader https address of this cluster identifies no peer yet the http one does
    @Test
    void theLeaderHttpsAddressOfThisClusterIdentifiesNoPeerYetTheHttpOneDoes() {
      final RaftHAServer raft = newDetachedServer(SHARED_HTTPS_PORT_CLUSTER);

      assertThat(raft.getUnambiguousPeerHttpAddress(LEADER))
          .as("distinct http ports: the plain guard has nothing to object to")
          .isEqualTo("localhost:2481");
      assertThat(raft.isOwnHttpAddress("localhost:2481")).isFalse();

      assertThat(raft.getUnambiguousPeerHttpsAddress(LEADER))
          .as("two peers answer to localhost:2491, so it identifies neither")
          .isNull();
      assertThat(raft.getPeerHttpsAddress(LEADER))
          .as("the raw resolver hands it out all the same")
          .isEqualTo("localhost:2491");
    }

    /**
     * The composition both proposed fixes use - the raw resolver behind {@code getLeaderHttpsAddress()}, whose
     * only filter is "is it our own" - keeps the shared address, so the probe would dial a node that may not be
     * the leader and read its answer as the leader's.
     */
    // Regression for #7563: the self check alone does not withhold an address shared with another peer
    @Test
    void theSelfCheckAloneDoesNotWithholdAnAddressSharedWithAnotherPeer() {
      final RaftHAServer raft = newDetachedServer(SHARED_HTTPS_PORT_CLUSTER);

      assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, raft.getPeerHttpsAddress(LEADER),
          raft.getLocalHttpsAddress()))
          .as("localhost:2491 is not ours - it is localhost_2436's just as much as the leader's")
          .isEqualTo("localhost:2491");
    }

    /** The guard that does withhold it, from one peer identity and for both schemes at once. */
    // Regression for #7563: the shared https endpoint is withheld when resolved through peer dial address
    @Test
    void theSharedHttpsEndpointIsWithheldWhenResolvedThroughPeerDialAddress() {
      final RaftHAServer raft = newDetachedServer(SHARED_HTTPS_PORT_CLUSTER);

      final PeerDialAddress dial = PeerDialAddress.resolve(raft, LEADER, "leader");

      assertThat(dial.refused()).as("the plain listener is still reachable, so this is no refusal").isFalse();
      assertThat(dial.httpAddress()).isEqualTo("localhost:2481");
      assertThat(dial.httpsAddress())
          .as("withheld, so the probe falls back to the guarded plain endpoint instead of dialling an unknown node")
          .isNull();
    }

    /** And the guard costs a correctly declared SSL cluster nothing: its own https endpoint is handed over. */
    // Regression for #7563: a leader with an https endpoint of its own is still probed over tls
    @Test
    void aLeaderWithAnHttpsEndpointOfItsOwnIsStillProbedOverTls() {
      final RaftHAServer raft = newDetachedServer(
          "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2492");

      final PeerDialAddress dial = PeerDialAddress.resolve(raft, LEADER, "leader");

      assertThat(dial.httpAddress()).isEqualTo("localhost:2481");
      assertThat(dial.httpsAddress()).isEqualTo("localhost:2491");
    }

    /**
     * A {@link RaftHAServer} built from {@code serverList} with Ratis never started: the peer group and both
     * declared address maps are populated by the constructor, which is all these accessors read.
     */
    private static RaftHAServer newDetachedServer(final String serverList) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);
      config.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");

      return new RaftHAServer(arcadeServer, config);
    }
  }

  /**
   * Regression test for issue #8472, the peer-to-peer queries #8325 left out: each one bounded the peer with the JDK
   * request timeout alone and read its answer with {@code BodyHandlers.ofString()}. On JDK 21-25 that timeout stops at
   * the response headers, so a peer that sent {@code 200} with a {@code Content-Length} and then stalled inside its body
   * parked the calling thread with no bound at all. Each entry point is driven through its own code here, against a peer
   * that does exactly that; without the fix every one of them waits on it until the hang detector gives up.
   * <p>
   * Every site dials its peer over plain HTTP or over HTTPS, and the HTTPS branch builds or borrows a client of its own
   * - one it may then close, which is where a hang could hide behind a deadline that did fire - so each site is driven
   * over both, the HTTPS one against a peer that completes the handshake and then stalls. The HTTPS branch of
   * {@code LeaderDatabaseQuery}, which #8325 bounded without a test of its own, is driven here as well.
   */
  @Nested
  class PeerQueryBodyDeadline {

    /** The tripwire between "the deadline fired" and "the call is unbounded" (forever, without the fix). */
    private static final long GAVE_UP_BOUND_MS = 15_000L;
    /** The deadline every query under test carries. */
    private static final long TIMEOUT_MS       = 1_000L;

    private static RaftTestPki pki;

    @BeforeAll
    static void generatePki() throws Exception {
      pki = RaftTestPki.create(Path.of("target", "test-pki-8472"), "stall");
    }

    // ---- PeerAuthSessionQuery.validate: the HTTP worker authenticating a token another node issued ----------------

    // Regression for #8472: an auth session query whose issuer stalls inside its body is given up on
    @Test
    void anAuthSessionQueryWhoseIssuerStallsInsideItsBodyIsGivenUpOn() throws Exception {
      try (final StallingBodyPeer issuer = new StallingBodyPeer()) {
        final RaftPeerId issuerId = RaftPeerId.valueOf("issuer");
        final FakeRaftHAServer raft = raftDialling(issuerId, plainServer(), issuer.address(), null, null);
        assertGivenUpOn(issuer, () -> PeerAuthSessionQuery.validate(raft, issuerId, "session-token", TIMEOUT_MS));
      }
    }

    // Regression for #8472: an auth session query whose issuer stalls inside its body over tls is given up on
    @Test
    void anAuthSessionQueryWhoseIssuerStallsInsideItsBodyOverTlsIsGivenUpOn() throws Exception {
      final TrustedHttpClientCache clients = new TrustedHttpClientCache();
      try (final StallingBodyPeer issuer = new StallingBodyPeer(tls())) {
        final RaftPeerId issuerId = RaftPeerId.valueOf("issuer");
        // The plain address is never dialled: SSL is on and the issuer has an HTTPS endpoint of its own.
        final FakeRaftHAServer raft = raftDialling(issuerId, tlsServer(), "127.0.0.1:1", issuer.address(), clients);
        assertGivenUpOn(issuer, () -> PeerAuthSessionQuery.validate(raft, issuerId, "session-token", TIMEOUT_MS));
      } finally {
        clients.close();
      }
    }

    /**
     * {@code PeerAuthSessionQuery.revokeEverywhere}: the logout fan-out. Its caller was always bounded, but a send the
     * deadline gave up on was left running, holding a connection to a peer stalled inside its body for as long as the
     * peer kept it open - one more on every logout (found in review of the #8472 fix).
     */
    // Regression for #8472: a revocation whose peer stalls inside its body releases the connection
    @Test
    void aRevocationWhosePeerStallsInsideItsBodyReleasesTheConnection() throws Exception {
      try (final StallingBodyPeer peer = new StallingBodyPeer()) {
        final RaftPeerId peerId = RaftPeerId.valueOf("peer-1");
        final FakeRaftHAServer raft = raftDialling(peerId, plainServer(), peer.address(), null, null);
        raft.raftGroup(RaftGroup.valueOf(RaftGroupId.randomId(), RaftPeer.newBuilder().setId(peerId).build()));

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        peer.callWithin(() -> {
          PeerAuthSessionQuery.revokeEverywhere(raft, "session-token", TIMEOUT_MS);
          return null;
        });
        watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s fan-out deadline from an unbounded wait");

        assertThat(peer.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
      }
    }

    // ---- PeerCapabilityQuery: the capability probe ------------------------------------------------------------------

    // Regression for #8472: a capability probe whose peer stalls inside its body is given up on
    @Test
    void aCapabilityProbeWhosePeerStallsInsideItsBodyIsGivenUpOn() throws Exception {
      try (final StallingBodyPeer peer = new StallingBodyPeer()) {
        assertGivenUpOn(peer,
            () -> PeerCapabilityQuery.fetch("peer-1", peer.address(), null, "test-token", TIMEOUT_MS, null, null));
      }
    }

    // Regression for #8472: a shared endpoint capability probe whose peer stalls inside its body is given up on
    @Test
    void aSharedEndpointCapabilityProbeWhosePeerStallsInsideItsBodyIsGivenUpOn() throws Exception {
      try (final StallingBodyPeer peer = new StallingBodyPeer()) {
        assertGivenUpOn(peer,
            () -> PeerCapabilityQuery.fetchFromSharedEndpoint(peer.address(), null, "test-token", TIMEOUT_MS, null, null));
      }
    }

    // Regression for #8472: a capability probe whose peer stalls inside its body over tls is given up on
    @Test
    void aCapabilityProbeWhosePeerStallsInsideItsBodyOverTlsIsGivenUpOn() throws Exception {
      final TrustedHttpClientCache clients = new TrustedHttpClientCache();
      try (final StallingBodyPeer peer = new StallingBodyPeer(tls())) {
        final ArcadeDBServer server = tlsServer();
        assertGivenUpOn(peer, () -> PeerCapabilityQuery.fetch("peer-1", "127.0.0.1:1", peer.address(), "test-token",
            TIMEOUT_MS, server, clients));
      } finally {
        clients.close();
      }
    }

    /** The bound must not cost a healthy peer its answer: the capability probe still reads a whole reply. */
    // Regression for #8472: a capability probe still reads a whole answer
    @Test
    void aCapabilityProbeStillReadsAWholeAnswer() throws Exception {
      try (final AnsweringPeer peer = new AnsweringPeer(
          "{\"peerId\":\"peer-1\",\"version\":\"x\",\"capabilities\":[\"a\"]}")) {
        final PeerCapabilityQuery.Advertisement ad = PeerCapabilityQuery.fetch("peer-1", peer.address(), null,
            "test-token", TIMEOUT_MS * 10, null, null);
        assertThat(ad.peerId()).isEqualTo("peer-1");
        assertThat(ad.capabilities()).containsExactly("a");
      }
    }

    // ---- BootstrapElection.fetchBootstrapState: the state-machine thread re-verifying a diverged database ----------

    /** A failed probe answers {@code null} rather than throwing, and the caller retries on a later tick. */
    // Regression for #8472: a bootstrap state probe whose peer stalls inside its body answers no result
    @Test
    void aBootstrapStateProbeWhosePeerStallsInsideItsBodyAnswersNoResult() throws Exception {
      try (final StallingBodyPeer leader = new StallingBodyPeer()) {
        assertAnsweredNothing(leader, () -> BootstrapElection.fetchBootstrapState(null, null, leader.address(), null,
            "test-token", Set.of("db"), TIMEOUT_MS));
      }
    }

    /** Over HTTPS the probe builds a client per call and closes it after the deadline, which must not wait it out. */
    // Regression for #8472: a bootstrap state probe whose peer stalls inside its body over tls answers no result
    @Test
    void aBootstrapStateProbeWhosePeerStallsInsideItsBodyOverTlsAnswersNoResult() throws Exception {
      try (final StallingBodyPeer leader = new StallingBodyPeer(tls())) {
        final ArcadeDBServer server = tlsServer();
        assertAnsweredNothing(leader, () -> BootstrapElection.fetchBootstrapState(server, null, "127.0.0.1:1",
            leader.address(), "test-token", Set.of("db"), TIMEOUT_MS));
      }
    }

    /**
     * {@code BootstrapElection.concludePass}: the fan-out telling every peer a bootstrap pass finished. Its HTTPS client
     * is built per pass and closed once the fan-out's deadline expires, and {@code close()} waits for every exchange
     * still running on it - so one that nothing cancelled kept the leader's bootstrap thread on a peer stalled inside
     * its body, past the deadline that had already fired (found in review of the #8472 fix).
     */
    // Regression for #8472: a pass conclusion whose peer stalls inside its body over tls is given up on
    @Test
    void aPassConclusionWhosePeerStallsInsideItsBodyOverTlsIsGivenUpOn() throws Exception {
      try (final StallingBodyPeer peer = new StallingBodyPeer(tls())) {
        final RaftPeerId peerId = RaftPeerId.valueOf("peer-1");
        final FakeRaftHAServer raft = FakeRaftHAServer.detached();
        raft.localPeerId(RaftPeerId.valueOf("self"));
        raft.livePeers(List.of(RaftPeer.newBuilder().setId(peerId).build()));
        raft.httpAddresses(Map.of(peerId, "127.0.0.1:1"));
        // The fan-out reads the guarded accessor, not the raw resolver (issue #8033).
        raft.peerHttpsAddress(peerId, peer.address());
        raft.clusterToken("test-token");
        final BootstrapElection election = new BootstrapElection(raft, tlsServer());
        election.probeAttemptTimeoutMs = TIMEOUT_MS;

        final Method conclude = BootstrapElection.class.getDeclaredMethod("concludePass", String.class, List.class);
        conclude.setAccessible(true);

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        peer.callWithin(() -> conclude.invoke(election, "pass-1", List.of()));
        watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s fan-out deadline from a close() waiting on the stalled body");

        assertThat(peer.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
      }
    }

    // ---- LeaderDatabaseQuery.fetch over HTTPS: the branch #8325 bounded without a test of its own -------------------

    // Regression for #8472: a leader database query whose peer stalls inside its body over tls is given up on
    @Test
    void aLeaderDatabaseQueryWhosePeerStallsInsideItsBodyOverTlsIsGivenUpOn() throws Exception {
      try (final StallingBodyPeer leader = new StallingBodyPeer(tls())) {
        final ArcadeDBServer server = tlsServer();
        assertGivenUpOn(leader,
            () -> LeaderDatabaseQuery.fetch(null, "127.0.0.1:1", leader.address(), "test-token", TIMEOUT_MS, server));
      }
    }

    // ------------------------------------------------------------------------------------------------------------

    private static void assertGivenUpOn(final StallingBodyPeer peer, final Callable<?> call) throws Exception {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> peer.callWithin(call)).isInstanceOf(HttpTimeoutException.class);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s deadline from the unbounded ofString() body read");

      assertThat(peer.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
    }

    private static void assertAnsweredNothing(final StallingBodyPeer peer, final Callable<?> call) throws Exception {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final Object answer = peer.callWithin(call);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s deadline from the unbounded ofString() body read");

      assertThat(answer).isNull();
      assertThat(peer.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
    }

    private static SSLContext tls() throws Exception {
      return RaftTestPki.serverContext(pki);
    }

    private static ArcadeDBServer plainServer() {
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, new ContextConfiguration());
      return server;
    }

    /** A server with SSL on that trusts the stalling peer's certificate, so the dial reaches the stall. */
    private static ArcadeDBServer tlsServer() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);
      cfg.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, pki.trustStore().toAbsolutePath().toString());
      cfg.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, RaftTestPki.password());
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
      return server;
    }

    private static FakeRaftHAServer raftDialling(final RaftPeerId peerId, final ArcadeDBServer server,
        final String httpAddress, final String httpsAddress, final TrustedHttpClientCache clients) {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached(server);
      raft.localPeerId(RaftPeerId.valueOf("self"));
      raft.localHttpAddress("127.0.0.1:2");
      raft.peerHttpAddress(peerId, httpAddress);
      raft.peerHttpsAddress(peerId, httpsAddress);
      raft.httpsClients(clients);
      raft.clusterToken("test-token");
      return raft;
    }

    /** A peer that answers every request with a complete {@code 200}. */
    private static final class AnsweringPeer implements AutoCloseable {
      private final HttpServer server;

      AnsweringPeer(final String body) throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
          exchange.getRequestBody().readAllBytes();
          final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
        server.start();
      }

      String address() {
        return server.getAddress().getAddress().getHostAddress() + ":" + server.getAddress().getPort();
      }

      @Override
      public void close() {
        server.stop(0);
      }
    }
  }

  /**
   * Issue #9999: two helpers extracted to make the peer-to-peer dials spell their credential and their plain-HTTP fallback
   * notice in one place had 2 and 5 adopters out of 11, and the sites left alone had drifted - a snapshot download sending
   * the token with no principal, a probe spelling the root principal as a literal, and seven dials that fell back to plain
   * HTTP on an SSL cluster without a word, the follower's write forward among them.
   * <p>
   * The two helpers are now one builder, {@link PeerRequest} (server module since issue #10068). That no dial of this module or of
   * the server starts its request, or attaches either header, outside it is held by {@code PeerDialsGoThroughOneBuilderCensusTest}
   * (census module), over the compiled classes (#11288); this class pins the drifts the issue found, through their dials.
   */
  @Nested
  class PeerDialsGoThroughOneBuilder {

    @BeforeEach
    @AfterEach
    void rearmTheNotice() {
      PeerRequest.rearmForTests();
    }

    // ------------------------------------------------------------------ the drifts the issue found, through their dials

    /** Drift 2: the bootstrap-state probe spelled the root principal as the literal "root". */
    // Regression for #9999: the bootstrap probe acts as the root principal every other dial names
    @Test
    void theBootstrapProbeActsAsTheRootPrincipalEveryOtherDialNames() {
      final HttpRequest request = BootstrapElection.bootstrapStateRequestTo("http://arcadedb-1:2480/api/v1/cluster/bootstrap-state",
          false, "s3cr3t", 1234L);

      assertThat(request.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).hasValue("s3cr3t");
      assertThat(request.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).hasValue(RaftHAServer.FORWARDED_ROOT_USER);
    }

    /** The probe used to send the principal even with no token: a claim with no proof, which the receiver ignores. */
    // Regression for #9999: the bootstrap probe sends no principal without a token
    @Test
    void theBootstrapProbeSendsNoPrincipalWithoutAToken() {
      final HttpRequest request = BootstrapElection.bootstrapStateRequestTo("http://arcadedb-1:2480/api/v1/cluster/bootstrap-state",
          false, null, 1234L);

      assertThat(request.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).isEmpty();
      assertThat(request.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).isEmpty();
    }

    /**
     * The bootstrap-state probe is also how the unverified-closed-copy check and the leader-serves check ask, which never
     * said the fallback: they now say it as the request is built, not at a fan-out some callers skipped.
     */
    // Regression for #9999: a plain bootstrap probe on an ssl cluster says the notice
    @Test
    void aPlainBootstrapProbeOnAnSslClusterSaysTheNotice() {
      BootstrapElection.bootstrapStateRequestTo("http://arcadedb-1:2480/api/v1/cluster/bootstrap-state", true, "s3cr3t", 1234L);

      assertThat(PeerRequest.saidForTests()).isTrue();
    }

    /** The remote shutdown, the first dial on the shared builder, keeps saying it. */
    // Regression for #9999: a plain shutdown on an ssl cluster says the notice
    @Test
    void aPlainShutdownOnAnSslClusterSaysTheNotice() {
      final HttpRequest request = RaftHAPlugin.shutdownRequest("http://arcadedb-1:2480" + RaftHAPlugin.SERVER_COMMAND_ROUTE,
          true, "s3cr3t");

      assertThat(PeerRequest.saidForTests()).isTrue();
      assertThat(request.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).hasValue(RaftHAServer.FORWARDED_ROOT_USER);
    }

    /** An encrypted dial has nothing to say. */
    // Regression for #9999: an encrypted probe says nothing
    @Test
    void anEncryptedProbeSaysNothing() {
      BootstrapElection.bootstrapStateRequestTo("https://arcadedb-1:2490/api/v1/cluster/bootstrap-state", true, "s3cr3t", 1234L);

      assertThat(PeerRequest.saidForTests()).isFalse();
    }
  }
}
