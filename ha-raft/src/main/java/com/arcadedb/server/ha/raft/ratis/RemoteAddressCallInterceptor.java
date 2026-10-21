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
package com.arcadedb.server.ha.raft.ratis;

import org.apache.ratis.thirdparty.io.grpc.Context;
import org.apache.ratis.thirdparty.io.grpc.Contexts;
import org.apache.ratis.thirdparty.io.grpc.Grpc;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;

import java.net.SocketAddress;

/**
 * Makes the remote address of the connection an inbound Raft RPC arrived on readable by {@link GuardedRaftServer}
 * (issue #10225).
 * <p>
 * gRPC exposes that address only to an interceptor, through the call's attributes, so this one copies it into the call's
 * {@link Context}, which gRPC attaches around every listener callback of the call. Ratis's server protocol service invokes
 * the {@code RaftServer} synchronously from those callbacks, so the guard reads it there with {@link #remoteAddress()}.
 * <p>
 * <b>It is reported, not trusted</b> (issue #10299). Until then the guard's listener took a refused request that named
 * this node as its sender for this node's own when it came from an address of this host. An address names a host, not a
 * node: every local process passes, so does anything a local proxy or service-mesh sidecar relays, and this node's own
 * request that crossed a hop rewriting its source fails. Whether a request is this node's own is now settled by a probe
 * of this node's channel (see {@code RaftHAServer.onMisdirectedRaftRequest}); the address only tells the operator where a
 * claim it did not confirm came from, and a {@code null} - an RPC Ratis served outside the call's context - costs no more
 * than that line.
 * <p>
 * Costs one {@link Context} per call, and an {@code appendEntries} stream is one call for its whole life.
 */
public final class RemoteAddressCallInterceptor implements ServerInterceptor {

  private static final Context.Key<SocketAddress> REMOTE_ADDRESS = Context.key("arcadedb.raft.remoteAddress");

  @Override
  public <Q, R> ServerCall.Listener<Q> interceptCall(final ServerCall<Q, R> call, final Metadata headers,
      final ServerCallHandler<Q, R> next) {
    final SocketAddress remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    if (remote == null)
      return next.startCall(call, headers);
    return Contexts.interceptCall(Context.current().withValue(REMOTE_ADDRESS, remote), call, headers, next);
  }

  /**
   * The remote address of the connection the RPC being served on this thread arrived on, or {@code null} outside such a
   * call or when gRPC did not report one.
   */
  public static SocketAddress remoteAddress() {
    return REMOTE_ADDRESS.get();
  }

  /** Runs {@code task} as if it were serving an RPC that arrived from {@code remote}. For tests. */
  static void runAsIfFrom(final SocketAddress remote, final Runnable task) {
    Context.current().withValue(REMOTE_ADDRESS, remote).run(task);
  }
}
