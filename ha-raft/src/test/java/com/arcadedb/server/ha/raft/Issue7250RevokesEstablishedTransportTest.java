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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.io.grpc.Attributes;
import org.apache.ratis.thirdparty.io.grpc.Grpc;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #7250, a follow-up to #7225.
 * <p>
 * #7225 made the allowlist unlearn a host that left the Raft configuration, so a removed peer stopped being
 * <i>admitted</i>. It did not touch one that was already <i>connected</i>: {@code ServerTransportFilter.transportReady}
 * runs once per transport and gRPC hands it no reference to the transport it admitted, so a peer connected at the
 * moment it was removed kept that HTTP/2 connection - and everything it could send on it - until one side dropped it.
 * <p>
 * The filter now attaches a {@link PeerTransportSession} to every transport it admits, revokes the sessions whose
 * address a resolution stopped admitting, and {@link PeerAllowlistCallInterceptor} enforces that per RPC: in-flight
 * calls are closed with {@code PERMISSION_DENIED} and later ones are refused. What is NOT closed is the socket -
 * gRPC's public API offers no way to close one established transport - which is the residual risk the tracking doc
 * records.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7250RevokesEstablishedTransportTest {

  private static final Metadata NO_HEADERS = new Metadata();

  // ---------------------------------------------------------------------------
  // Row 1: the reported case - a peer removed from the Raft configuration
  // ---------------------------------------------------------------------------

  @Test
  void aPeerRemovedFromTheRaftConfigurationLosesItsEstablishedTransport() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("arcadedb-0", List.of("10.1.13.1"));
    dns.table.put("arcadedb-3", List.of("10.1.13.4"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("arcadedb-0"), 30_000L, 0L,
        300_000L, clock::get, dns);
    final PeerAllowlistCallInterceptor interceptor = new PeerAllowlistCallInterceptor();

    // Scale-up: pod 3 joins and connects. A long-lived appendEntries stream is running on that transport.
    filter.setMemberHosts(List.of("arcadedb-0", "arcadedb-3"));
    final Attributes transport = admit(filter, "10.1.13.4");
    final RecordingServerCall stream = startCall(interceptor, transport);
    assertThat(stream.closedStatus()).as("an admitted peer's RPC runs").isNull();

    // Scale-down: the health monitor tick reports the smaller membership.
    filter.setMemberHosts(List.of("arcadedb-0"));

    assertThat(filter.isAllowed("10.1.13.4")).as("#7225: the removed peer can no longer connect").isFalse();
    assertThat(stream.closedStatus()).as("#7250: and the RPC it already had running is cut")
        .isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  /** Row 7: after a revocation, an RPC started on the same still-open transport is refused. */
  @Test
  void aNewRpcOnARevokedTransportIsRefused() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    dns.table.put("gone", List.of("10.0.0.9"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 300_000L,
        clock::get, dns);
    final PeerAllowlistCallInterceptor interceptor = new PeerAllowlistCallInterceptor();

    filter.setMemberHosts(List.of("gone"));
    final Attributes transport = admit(filter, "10.0.0.9");
    filter.setMemberHosts(List.of());

    final AtomicInteger handlerInvocations = new AtomicInteger();
    final RecordingServerCall refused = new RecordingServerCall(transport);
    interceptor.interceptCall(refused, NO_HEADERS, countingHandler(handlerInvocations));

    assertThat(refused.closedStatus()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(handlerInvocations)
        .as("a refused call must never reach the Raft service handler on the other side of the interceptor")
        .hasValue(0);
  }

  // ---------------------------------------------------------------------------
  // Row 3: the peer's DNS record no longer carries the address it connected from
  // ---------------------------------------------------------------------------

  @Test
  void aPeerWhoseAddressChangedLosesTheTransportOpenedOnTheOldAddress() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 0L,
        clock::get, dns);
    final PeerAllowlistCallInterceptor interceptor = new PeerAllowlistCallInterceptor();

    final Attributes transport = admit(filter, "10.0.0.1");
    final RecordingServerCall stream = startCall(interceptor, transport);

    // The pod is recreated with a new IP; the name now resolves elsewhere. Whoever still holds the old address is
    // not the peer any more.
    dns.table.put("peerA", List.of("10.0.0.2"));
    clock.addAndGet(60_000L);
    filter.proactiveRefresh();

    assertThat(filter.getAllowedIps()).contains("10.0.0.2").doesNotContain("10.0.0.1");
    assertThat(stream.closedStatus()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  // ---------------------------------------------------------------------------
  // Row 4: the sticky last-known-good retention expiring
  // ---------------------------------------------------------------------------

  @Test
  void aTransportHeldOpenOnAnExpiredStickyAddressIsRevoked() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 1_000L, 0L, 10_000L,
        clock::get, dns);
    final PeerAllowlistCallInterceptor interceptor = new PeerAllowlistCallInterceptor();

    final Attributes transport = admit(filter, "10.0.0.1");
    final RecordingServerCall stream = startCall(interceptor, transport);

    // DNS stops answering for the peer. Inside the sticky window the transport keeps its access, which is the whole
    // point of the retention: a transient outage must not partition a live cluster.
    dns.table.remove("peerA");
    clock.addAndGet(5_000L);
    filter.refresh();
    assertThat(stream.closedStatus()).as("a sticky entry still covers the address").isNull();

    // Past the retention the address is nobody's any more.
    clock.addAndGet(20_000L);
    filter.refresh();
    assertThat(filter.getAllowedIps()).doesNotContain("10.0.0.1");
    assertThat(stream.closedStatus()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  // ---------------------------------------------------------------------------
  // Row 5: a transport admitted while the filter was failing open at startup
  // ---------------------------------------------------------------------------

  @Test
  void aTransportAdmittedUnderTheStartupFailOpenIsRevokedOnceTheGraceEnds() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    // peerA does not resolve yet: below quorum, so the filter fails open (issues #4471/#4828).
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 1_000L, 60_000L, 0L,
        clock::get, dns);
    final PeerAllowlistCallInterceptor interceptor = new PeerAllowlistCallInterceptor();

    final Attributes transport = admit(filter, "10.0.0.55");
    final RecordingServerCall stream = startCall(interceptor, transport);

    // Still inside the grace window and still below quorum: cutting the transport here would re-create exactly the
    // self-inflicted partition the fail-open exists to prevent.
    clock.addAndGet(10_000L);
    filter.refresh();
    assertThat(stream.closedStatus()).as("the fail-open window must keep admitting what it admitted").isNull();

    // The peer resolves, the quorum latch trips and the fail-open ends. 10.0.0.55 was never a peer.
    dns.table.put("peerA", List.of("10.0.0.1"));
    clock.addAndGet(10_000L);
    filter.refresh();

    assertThat(filter.isQuorumResolved()).isTrue();
    assertThat(stream.closedStatus()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  // ---------------------------------------------------------------------------
  // Row 9 and the lifetime contract
  // ---------------------------------------------------------------------------

  @Test
  void aLoopbackTransportIsNeverTrackedAndNeverRevoked() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 0L,
        clock::get, dns);
    final PeerAllowlistCallInterceptor interceptor = new PeerAllowlistCallInterceptor();

    final Attributes transport = filter.transportReady(remoteAttributes("127.0.0.1"));
    assertThat(transport.get(PeerAddressAllowlistFilter.TRANSPORT_SESSION))
        .as("loopback is admitted before the allowlist is consulted, so it gets no session").isNull();
    assertThat(filter.getSessions()).isEmpty();

    final AtomicInteger handlerInvocations = new AtomicInteger();
    final RecordingServerCall call = new RecordingServerCall(transport);
    interceptor.interceptCall(call, NO_HEADERS, countingHandler(handlerInvocations));

    // Every membership shrink there is, and the loopback call is still running.
    filter.setMemberHosts(List.of());
    filter.refresh();
    assertThat(handlerInvocations).hasValue(1);
    assertThat(call.closedStatus()).isNull();
  }

  /**
   * The session is registered before the admission decision is taken, so that a revocation racing that decision
   * cannot miss it. This pins the other half of that ordering: the rejection path has to take the session back out
   * again, and the miss-triggered re-resolution inside {@code isAllowed} sweeps it on the way through - so the
   * rejected connection walks the whole register/revoke/unregister path, not just the throw.
   */
  @Test
  void aRejectedTransportIsRegisteredForTheRaceButNotLeftBehind() {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 0L,
        clock::get, dns);

    assertThatThrownBy(() -> filter.transportReady(remoteAttributes("192.168.7.7")))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("192.168.7.7");
    assertThat(filter.getSessions()).as("a transport that was never admitted must not be tracked").isEmpty();
  }

  /** {@code getSessions()} is a test hook, and a test hook that hands out the live set is a way to corrupt it. */
  @Test
  void theSessionSetIsNotExposedForMutation() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 0L,
        clock::get, dns);
    admit(filter, "10.0.0.1");

    assertThatThrownBy(() -> filter.getSessions().clear()).isInstanceOf(UnsupportedOperationException.class);
  }

  /**
   * A handler that throws before the RPC starts leaves no {@code onComplete} or {@code onCancel} to deregister the
   * call, so the interceptor has to do it itself - otherwise a long-lived connection accumulates one dead entry per
   * failed RPC, and every later revocation walks them.
   */
  @Test
  void aHandlerThatThrowsDoesNotLeaveTheCallRegistered() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 0L,
        clock::get, dns);
    final PeerAllowlistCallInterceptor interceptor = new PeerAllowlistCallInterceptor();

    final Attributes transport = admit(filter, "10.0.0.1");
    final PeerTransportSession session = transport.get(PeerAddressAllowlistFilter.TRANSPORT_SESSION);

    assertThatThrownBy(() -> interceptor.interceptCall(new RecordingServerCall(transport), NO_HEADERS,
        (call, headers) -> {
          throw new IllegalStateException("the Raft service refused to start this RPC");
        })).isInstanceOf(IllegalStateException.class);

    assertThat(session.getLiveCallCount()).isZero();
  }

  @Test
  void aTerminatedTransportIsForgottenSoTheSessionSetDoesNotGrowForever() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 0L,
        clock::get, dns);
    final PeerAllowlistCallInterceptor interceptor = new PeerAllowlistCallInterceptor();

    final Attributes transport = admit(filter, "10.0.0.1");
    final PeerTransportSession session = transport.get(PeerAddressAllowlistFilter.TRANSPORT_SESSION);
    startCall(interceptor, transport);
    assertThat(filter.getSessions()).hasSize(1);
    assertThat(session.getLiveCallCount()).isEqualTo(1);

    filter.transportTerminated(transport);

    assertThat(filter.getSessions()).isEmpty();
    assertThat(session.getLiveCallCount()).as("a dead transport's calls must not be walked by later revocations")
        .isZero();
  }

  @Test
  void aCompletedRpcIsDeregisteredFromItsSession() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 0L,
        clock::get, dns);
    final PeerAllowlistCallInterceptor interceptor = new PeerAllowlistCallInterceptor();

    final Attributes transport = admit(filter, "10.0.0.1");
    final PeerTransportSession session = transport.get(PeerAddressAllowlistFilter.TRANSPORT_SESSION);

    final ServerCall.Listener<byte[]> listener = interceptor.interceptCall(new RecordingServerCall(transport),
        NO_HEADERS, countingHandler(new AtomicInteger()));
    assertThat(session.getLiveCallCount()).isEqualTo(1);
    listener.onComplete();
    assertThat(session.getLiveCallCount()).isZero();

    final ServerCall.Listener<byte[]> cancelled = interceptor.interceptCall(new RecordingServerCall(transport),
        NO_HEADERS, countingHandler(new AtomicInteger()));
    assertThat(session.getLiveCallCount()).isEqualTo(1);
    cancelled.onCancel();
    assertThat(session.getLiveCallCount()).isZero();
  }

  /**
   * A revocation and the handler's own completion race for the single-shot {@code ServerCall.close}. The wrapper has
   * to arbitrate, or one of the two throws {@code IllegalStateException} inside gRPC.
   */
  @Test
  void aCallTheHandlerAlreadyClosedIsNotClosedTwiceByARevocation() throws UnknownHostException {
    final AtomicLong clock = new AtomicLong(0);
    final PeerAddressAllowlistFilterTest.FakeResolver dns = new PeerAddressAllowlistFilterTest.FakeResolver();
    dns.table.put("gone", List.of("10.0.0.9"));
    dns.table.put("peerA", List.of("10.0.0.1"));
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(List.of("peerA"), 30_000L, 0L, 0L,
        clock::get, dns);
    final PeerAllowlistCallInterceptor interceptor = new PeerAllowlistCallInterceptor();

    filter.setMemberHosts(List.of("gone"));
    final Attributes transport = admit(filter, "10.0.0.9");

    final RecordingServerCall raw = new RecordingServerCall(transport);
    final List<ServerCall<byte[], byte[]>> handed = new ArrayList<>();
    interceptor.interceptCall(raw, NO_HEADERS, (call, headers) -> {
      handed.add(call);
      return new ServerCall.Listener<>() {
      };
    });
    // The handler finishes the RPC normally, exactly as a completing appendEntries would.
    handed.getFirst().close(Status.OK, new Metadata());
    assertThat(raw.closes).isEqualTo(1);

    filter.setMemberHosts(List.of());

    assertThat(raw.closes).as("the revocation must not close a call the handler already finished").isEqualTo(1);
    assertThat(raw.closedStatus()).isEqualTo(Status.Code.OK);
  }

  // ---------------------------------------------------------------------------
  // Row 2: the production wiring in RaftHAServer
  // ---------------------------------------------------------------------------

  @Test
  void theProductionReconciliationRevokesTheRemovedPeersEstablishedTransport() throws UnknownHostException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "127.0.0.1:2434:2480");
    config.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_STARTUP_GRACE_MS, 0L);

    final ArcadeDBServer arcadeServer = mock(ArcadeDBServer.class);
    when(arcadeServer.getServerName()).thenReturn("arcadedb-0");
    final RaftHAServer server = new RaftHAServer(arcadeServer, config);
    server.buildParameters(config);

    final PeerAddressAllowlistFilter filter = server.allowlistFilterForTest();
    final PeerAllowlistCallInterceptor interceptor = server.allowlistInterceptorForTest();
    assertThat(filter).isNotNull();
    assertThat(interceptor).as("the per-RPC half of the allowlist is installed with the filter").isNotNull();

    // A peer joins at runtime and connects; its host is a literal address so it needs no DNS.
    server.reconcileAllowlistMembership(List.of(peer("arcadedb-1", "10.20.30.40:2434")));
    assertThat(filter.isAllowed("10.20.30.40")).isTrue();
    final Attributes transport = admit(filter, "10.20.30.40");
    final RecordingServerCall stream = startCall(interceptor, transport);

    // DELETE /api/v1/cluster/peer/{id}: the next tick carries the committed configuration without it.
    server.reconcileAllowlistMembership(List.of(peer("arcadedb-0", "127.0.0.1:2434")));

    assertThat(filter.isAllowed("10.20.30.40")).isFalse();
    assertThat(stream.closedStatus()).as("removing a peer revokes its reach, not only its ability to reconnect")
        .isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  void aDisabledAllowlistInstallsNeitherHalf() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "127.0.0.1:2434:2480");
    config.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, false);

    final ArcadeDBServer arcadeServer = mock(ArcadeDBServer.class);
    when(arcadeServer.getServerName()).thenReturn("arcadedb-0");
    final RaftHAServer server = new RaftHAServer(arcadeServer, config);
    server.buildParameters(config);

    assertThat(server.allowlistFilterForTest()).isNull();
    assertThat(server.allowlistInterceptorForTest()).isNull();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static RaftPeer peer(final String id, final String address) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress(address).build();
  }

  /** The transport attributes gRPC hands {@code transportReady} for a connection from {@code ip}. */
  private static Attributes remoteAttributes(final String ip) throws UnknownHostException {
    return Attributes.newBuilder()
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(InetAddress.getByName(ip), 51_234)).build();
  }

  /** Runs a connection from {@code ip} through the filter and returns the effective transport attributes. */
  private static Attributes admit(final PeerAddressAllowlistFilter filter, final String ip)
      throws UnknownHostException {
    final Attributes effective = filter.transportReady(remoteAttributes(ip));
    assertThat(effective.get(PeerAddressAllowlistFilter.TRANSPORT_SESSION))
        .as("an admitted transport carries the handle a revocation needs").isNotNull();
    return effective;
  }

  /** Starts one long-lived RPC on {@code transport} and returns the underlying call, to inspect how it ends. */
  private static RecordingServerCall startCall(final PeerAllowlistCallInterceptor interceptor,
      final Attributes transport) {
    final RecordingServerCall call = new RecordingServerCall(transport);
    interceptor.interceptCall(call, NO_HEADERS, countingHandler(new AtomicInteger()));
    return call;
  }

  /** A handler that counts its invocations and never finishes the call, standing in for a live Raft stream. */
  private static ServerCallHandler<byte[], byte[]> countingHandler(final AtomicInteger invocations) {
    return (call, headers) -> {
      invocations.incrementAndGet();
      return new ServerCall.Listener<>() {
      };
    };
  }

  /**
   * A {@link ServerCall} that records how it was finished. {@code ServerCall} is an abstract class with no
   * final methods, so the real thing can be stood in for without a mocking framework - and without a gRPC server.
   */
  private static final class RecordingServerCall extends ServerCall<byte[], byte[]> {
    private final Attributes attributes;
    private       Status     closedWith;
    private       int        closes;

    private RecordingServerCall(final Attributes attributes) {
      this.attributes = attributes;
    }

    private Status.Code closedStatus() {
      return closedWith == null ? null : closedWith.getCode();
    }

    @Override
    public void request(final int numMessages) {
      // no flow control in this stub
    }

    @Override
    public void sendHeaders(final Metadata headers) {
      // no headers in this stub
    }

    @Override
    public void sendMessage(final byte[] message) {
      // no messages in this stub
    }

    @Override
    public void close(final Status status, final Metadata trailers) {
      closes++;
      closedWith = status;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public Attributes getAttributes() {
      return attributes;
    }

    @Override
    public MethodDescriptor<byte[], byte[]> getMethodDescriptor() {
      return null;
    }
  }
}
