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

import org.apache.ratis.thirdparty.io.grpc.Status;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One inbound Raft gRPC transport that {@link PeerAddressAllowlistFilter} admitted, and the RPCs currently running on
 * it (issue #7250).
 * <p>
 * {@code ServerTransportFilter} is a one-shot admission gate: it is consulted when a transport is established and
 * never again, and it is handed no reference to the transport it admitted. So #7225 could make a removed peer stop
 * being <i>admitted</i> but could not make one that was already <i>connected</i> stop talking. This object is the
 * missing handle. The filter creates one per admitted transport, stashes it in the transport {@link
 * org.apache.ratis.thirdparty.io.grpc.Attributes}, and {@link PeerAllowlistCallInterceptor} finds it again on every
 * RPC through {@code ServerCall.getAttributes()} - which is where the transport attributes are merged in.
 * <p>
 * <b>Lifetime.</b> The filter holds sessions in a set it clears from {@code transportTerminated}, and a session holds
 * only the calls currently in flight - the interceptor deregisters each one when it completes or is cancelled. So
 * neither the session nor a call outlives its transport, which is the part the issue asked to design carefully.
 * <p>
 * <b>What a revocation actually revokes.</b> gRPC's public API hands out no way to close one established transport
 * on demand (checked across {@code ServerTransportFilter} and every public method of {@code NettyServerBuilder}), so
 * revoking is not the same act as disconnecting. What is revoked is everything the socket can carry: in-flight calls
 * are closed with {@link #REVOKED} and every subsequent call on the transport is refused by the interceptor, which
 * sits in the server-wide interceptor chain that every RPC passes. The socket then goes when it falls idle: the one
 * connection-lifetime knob {@code NettyServerBuilder} does expose is builder-wide, and
 * {@link RaftGrpcServicesCustomizer} sets it from {@code arcadedb.ha.grpcMaxConnectionIdleMs} (issue #7316).
 */
final class PeerTransportSession {

  /** Status a revoked peer sees, on the in-flight RPCs and on every one it starts afterwards. */
  static final Status REVOKED = Status.PERMISSION_DENIED
      .withDescription("The Raft gRPC peer allowlist no longer admits this address");

  private final String                             remoteIp;
  // The calls in flight on this transport. Concurrent because the interceptor registers and deregisters from gRPC
  // handler threads while a reconciliation thread may be closing them. Sized for the handful a Raft connection
  // actually has in flight rather than the default 16, since there is one of these per open connection.
  private final Set<RevocableServerCall<?, ?>>     liveCalls = ConcurrentHashMap.newKeySet(4);
  private volatile boolean                         revoked;
  // Set once transportReady has decided to admit this transport. A session is registered BEFORE that decision, so
  // that a concurrent revocation cannot miss it, which means a rejected connection can be swept too; this flag is
  // what keeps such a session out of the revocation log, where it would report a revocation of something that was
  // never admitted (the rejection itself is already logged by isAllowed).
  private volatile boolean                         admitted;

  PeerTransportSession(final String remoteIp) {
    this.remoteIp = remoteIp;
  }

  String getRemoteIp() {
    return remoteIp;
  }

  boolean isRevoked() {
    return revoked;
  }

  /** Called by {@code transportReady} once it has decided to admit this transport, and never unset. */
  void markAdmitted() {
    admitted = true;
  }

  boolean isAdmitted() {
    return admitted;
  }

  /**
   * Marks this transport revoked. Idempotent, and the return value is what keeps a repeated reconciliation from
   * logging - and re-walking the call set of - a transport that was already cut.
   * <p>
   * The {@code synchronized} makes the check-and-set atomic in this object rather than in its caller. It buys
   * nothing today: {@code grep -rn "\.revoke()" ha-raft/src} finds one caller,
   * {@code PeerAddressAllowlistFilter.doResolve()}, which already runs under that filter's monitor. It is here so
   * that the guarantee this method's return value advertises belongs to this method, and a second caller does not
   * have to discover that it was really the filter's lock all along.
   * <p>
   * <b>Revocation is terminal for this transport, deliberately.</b> Nothing un-revokes a session when its address
   * becomes admissible again, so a peer whose name resolved elsewhere for one tick reconnects rather than resuming
   * on the connection it had - which Ratis does on its own, and which the allowlist then admits normally. The
   * alternative, letting a later DNS answer undo a revocation, would make the revocation only as durable as the
   * least trustworthy resolution in the window, and this class exists to revoke.
   *
   * @return true when this invocation is the one that flipped the session from live to revoked
   */
  synchronized boolean revoke() {
    if (revoked)
      return false;
    revoked = true;
    return true;
  }

  void register(final RevocableServerCall<?, ?> call) {
    liveCalls.add(call);
  }

  void unregister(final RevocableServerCall<?, ?> call) {
    liveCalls.remove(call);
  }

  /**
   * Closes every RPC currently in flight on this transport. Removing before closing rather than after keeps the set
   * from being walked twice for the same call when a handler completes concurrently.
   *
   * @return how many calls this invocation actually closed
   */
  int closeLiveCalls() {
    int closed = 0;
    for (final RevocableServerCall<?, ?> call : liveCalls) {
      liveCalls.remove(call);
      if (call.closeAsRevoked())
        closed++;
    }
    return closed;
  }

  /** Drops the call references without touching them; the transport is gone, so there is nothing left to close. */
  void forgetCalls() {
    liveCalls.clear();
  }

  /** How many RPCs are currently in flight on this transport. Exposed for testing. */
  int getLiveCallCount() {
    return liveCalls.size();
  }

  @Override
  public String toString() {
    return "PeerTransportSession{remoteIp=" + remoteIp + ", admitted=" + admitted + ", revoked=" + revoked
        + ", liveCalls=" + liveCalls.size() + "}";
  }
}
