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
package com.arcadedb.server.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Publishes, for the duration of each call, whether the call's transport is fit to carry secret
 * material back to the caller (issue #7309).
 * <p>
 * Only one RPC needs this: {@code CreateApiToken} returns a plaintext API token, the single response
 * field in this service that the server can never reproduce and that authenticates its holder.
 * {@code RemoteGrpcServer} already refuses to <i>send</i> credentials over a cleartext channel to a
 * non-loopback host, but that guard runs in ArcadeDB's own Java client, can be opted out of with
 * {@code allowInsecureCredentials}, and is not running at all when the caller is grpcurl or a Python
 * stub. The guard that actually holds has to sit where the secret leaves the process.
 * <p>
 * <b>What counts as fit.</b> Either the transport is encrypted - gRPC exposes an
 * {@link Grpc#TRANSPORT_ATTR_SSL_SESSION} for TLS connections and nothing for cleartext ones - or the
 * peer is on the loopback interface, where the bytes never reach a network. That is the same pair of
 * conditions {@code RemoteGrpcServer.isLoopbackHost} applies from the other end, read from the live
 * connection here rather than from configuration, so a TLS setting that did not actually take effect
 * cannot vouch for a plaintext socket.
 * <p>
 * <b>What "loopback" assumes.</b> That the peer on the other end of a loopback socket is the client
 * itself, and not a TLS-terminating reverse proxy forwarding a remote caller to cleartext 127.0.0.1.
 * From inside this process the two are indistinguishable - a proxied remote caller presents exactly
 * the same address - so such a deployment gets the token minted for it. That is the same trust model
 * "trust loopback" HTTP setups already run on, and it is stated here rather than left to be
 * rediscovered, because this class exists precisely to close a transport-trust gap: an operator who
 * fronts the gRPC port with a proxy has moved the trust boundary to the proxy and needs to protect it
 * there.
 * <p>
 * <b>What it deliberately does not do</b> is refuse the call. It records a fact; the handler decides.
 * An interceptor that closed the call would have to know which methods carry secrets, putting that
 * list one refactor away from disagreeing with the service that defines them.
 */
public class GrpcTransportSecurityInterceptor implements ServerInterceptor {

  /**
   * True when the current call arrived over a transport that protects the response body.
   * <p>
   * Read it with {@code SECRET_SAFE_TRANSPORT_KEY.get()}, which returns {@code null} when this
   * interceptor did not run. A null is NOT "unknown, carry on": a caller handling secret material must
   * treat it as unfit, so that removing this interceptor turns the minting of tokens off rather than
   * turning the gate off.
   */
  public static final Context.Key<Boolean> SECRET_SAFE_TRANSPORT_KEY = Context.key("secret-safe-transport");

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers, final ServerCallHandler<ReqT, RespT> next) {

    final boolean encrypted = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION) != null;
    final boolean safe = encrypted || isLoopbackPeer(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));

    return Contexts.interceptCall(Context.current().withValue(SECRET_SAFE_TRANSPORT_KEY, safe), call, headers, next);
  }

  /**
   * Whether the peer is on this machine's loopback interface. Fails closed on anything it cannot read
   * as an IP socket - an in-process or unix-domain transport reports an address type this does not
   * recognise, and answering "safe" for an address whose shape is unknown is the wrong direction to
   * guess in.
   */
  private static boolean isLoopbackPeer(final SocketAddress remoteAddress) {
    if (!(remoteAddress instanceof final InetSocketAddress inetAddress))
      return false;

    // Already-resolved is the normal case for an accepted connection; getAddress() returning null
    // means the address is unresolved, and an unresolved peer is not a peer known to be local.
    final InetAddress address = inetAddress.getAddress();
    return address != null && address.isLoopbackAddress();
  }
}
