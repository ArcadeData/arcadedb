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
 * The guard sees only the request protos Ratis hands the {@code RaftServer}, and the requestor id in their header is
 * whatever the sender wrote: on its own it cannot tell this node's own request, which came back to it because the
 * address it holds for a peer leads here, from a request that merely claims to be from this node. The connection's
 * remote address is what the sender cannot write. gRPC exposes it only to an interceptor, through the call's
 * attributes, so this one copies it into the call's {@link Context}, which gRPC attaches around every listener callback
 * of the call. Ratis's server protocol service invokes the {@code RaftServer} synchronously from those callbacks
 * ({@code requestVote}, {@code startLeaderElection}, {@code readIndex}, and {@code onNext} of the {@code appendEntries}
 * and {@code installSnapshot} streams), so the guard reads it there with {@link #remoteAddress()}.
 * <p>
 * <b>That synchronous dispatch is an assumption about Ratis, not a guarantee.</b> A Ratis upgrade that moves one of those
 * calls onto an executor of its own leaves the guard reading {@code null} there, and a self-dial on that RPC is then
 * refused and reported but no longer repaired - safe, but #10016's recovery silently gone for it.
 * {@code Issue10016MisdirectedRaftRequestIT} pins it for {@code requestVote} and the {@code appendEntries} stream, the two
 * RPCs a self-dial carries.
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
