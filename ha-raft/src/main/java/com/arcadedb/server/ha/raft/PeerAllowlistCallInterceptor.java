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

import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;

/**
 * Enforces {@link PeerAddressAllowlistFilter}'s admission decision for the whole life of a transport rather than only
 * at the moment it is established (issue #7250).
 * <p>
 * {@code ServerTransportFilter.transportReady} runs once per connection, and gRPC gives it no handle on the
 * connection, so #7225's unlearning of a removed peer stopped that peer from <i>reconnecting</i> and left an
 * already-open HTTP/2 connection - and every RPC it can still carry - untouched. This interceptor closes that window
 * from the only surface gRPC does expose: every RPC on the server passes the interceptor chain
 * ({@code ServerImpl} applies the builder's interceptors to every call regardless of service registration order), so
 * a transport whose {@link PeerTransportSession} has been revoked can be refused per call.
 * <p>
 * It also registers each call with its session, which is what lets a revocation cut the RPCs that are already
 * running - a Raft {@code appendEntries} stream is long-lived, so refusing only new calls would revoke nothing on a
 * connected follower.
 * <p>
 * Calls with no session in their attributes are passed straight through. That is not a hole: the filter creates no
 * session for a loopback connection (it returns before the allowlist check) nor for a transport whose remote address
 * gRPC did not report, and both are addresses the filter itself admits unconditionally.
 */
final class PeerAllowlistCallInterceptor implements ServerInterceptor {

  @Override
  public <Q, R> ServerCall.Listener<Q> interceptCall(final ServerCall<Q, R> call, final Metadata headers,
      final ServerCallHandler<Q, R> next) {
    final PeerTransportSession session = call.getAttributes().get(PeerAddressAllowlistFilter.TRANSPORT_SESSION);
    if (session == null)
      return next.startCall(call, headers);

    if (session.isRevoked())
      return refuse(call);

    final RevocableServerCall<Q, R> revocable = new RevocableServerCall<>(call);
    session.register(revocable);
    // Re-read after registering: a revocation that ran between the check above and the registration has already
    // walked the call set, so without this the call would start on a transport that is no longer admitted.
    if (session.isRevoked()) {
      session.unregister(revocable);
      revocable.closeAsRevoked();
      return new NoOpListener<>();
    }

    final ServerCall.Listener<Q> delegate;
    try {
      delegate = next.startCall(revocable, headers);
    } catch (final RuntimeException | Error e) {
      // The handler never started, so neither onComplete nor onCancel will arrive to deregister the call. Without
      // this the session would keep one dead entry per failed RPC for the life of the connection.
      session.unregister(revocable);
      throw e;
    }

    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
      @Override
      public void onComplete() {
        session.unregister(revocable);
        super.onComplete();
      }

      @Override
      public void onCancel() {
        session.unregister(revocable);
        super.onCancel();
      }
    };
  }

  private static <Q, R> ServerCall.Listener<Q> refuse(final ServerCall<Q, R> call) {
    call.close(PeerTransportSession.REVOKED, new Metadata());
    return new NoOpListener<>();
  }

  /**
   * Listener for a call that was refused before the handler ever saw it. Declared rather than an anonymous subclass
   * so both refusal paths return the same thing.
   */
  private static final class NoOpListener<Q> extends ServerCall.Listener<Q> {
  }
}
