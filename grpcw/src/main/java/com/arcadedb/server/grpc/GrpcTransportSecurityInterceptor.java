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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.http.handler.PostApiTokenHandler;
import com.arcadedb.utility.IPAddressBlocklist;
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
import java.util.Objects;
import java.util.function.Supplier;

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
 * <b>A listed reverse proxy can vouch for the leg it terminated</b> (issue #7821), on the same terms as the
 * HTTP mint ({@code PostApiTokenHandler.isTransportSafeForSecrets}, issue #7804) and read from the same
 * setting, {@code arcadedb.server.apiTokenTrustedProxies}, so the two control planes keep one trust boundary
 * (#7309, #7372). gRPC metadata travels as HTTP/2 headers, so the proxy's report arrives as the
 * {@code x-forwarded-proto} metadata key: Envoy adds it to gRPC requests on its own, and nginx's
 * {@code grpc_pass} adds it with {@code grpc_set_header X-Forwarded-Proto $scheme}. It is read only when the
 * direct peer is on the list - the caller asking for a token is exactly the caller who would forge it - and
 * every value must report {@code https}, so a client-supplied value a proxy appended to rather than replaced
 * cannot outvote the proxy's own. With the list empty, the shipped default, the metadata is never read and
 * the decision is exactly the TLS-or-loopback test below.
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

  /**
   * The metadata key a TLS-terminating proxy reports the client's scheme in. gRPC lowercases every metadata
   * key, so this matches an {@code X-Forwarded-Proto} header whatever case the proxy wrote it in.
   */
  static final Metadata.Key<String> X_FORWARDED_PROTO_KEY = Metadata.Key.of("x-forwarded-proto",
      Metadata.ASCII_STRING_MARSHALLER);

  private static final TrustedProxies NO_TRUSTED_PROXIES = new TrustedProxies("", IPAddressBlocklist.parse(null));

  private final Supplier<String>   trustedProxiesSetting;
  private volatile TrustedProxies trustedProxies = NO_TRUSTED_PROXIES;

  /** The last parse of the setting, kept with the text it came from so a changed setting is re-parsed. */
  private record TrustedProxies(String csv, IPAddressBlocklist list) {
  }

  /** No proxy is trusted: the decision is TLS or loopback only. */
  public GrpcTransportSecurityInterceptor() {
    this(() -> "");
  }

  /**
   * @param trustedProxiesSetting supplies the current value of {@code arcadedb.server.apiTokenTrustedProxies}.
   *                              It is read on every call that is neither TLS nor loopback, so a SET SERVER
   *                              SETTING takes effect on the next call, as it does for the HTTP mint
   */
  public GrpcTransportSecurityInterceptor(final Supplier<String> trustedProxiesSetting) {
    this.trustedProxiesSetting = Objects.requireNonNull(trustedProxiesSetting, "trustedProxiesSetting");
  }

  /**
   * The interceptor the gRPC server registers: it trusts the proxies listed in
   * {@link GlobalConfiguration#SERVER_API_TOKEN_TRUSTED_PROXIES} of {@code configuration}, read live so that
   * SET SERVER SETTING takes effect on the next call, as it does for the HTTP mint (issue #7821).
   */
  public static GrpcTransportSecurityInterceptor forConfiguration(final ContextConfiguration configuration) {
    Objects.requireNonNull(configuration, "configuration");
    return new GrpcTransportSecurityInterceptor(
        () -> configuration.getValueAsString(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES));
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers, final ServerCallHandler<ReqT, RespT> next) {

    final boolean encrypted = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION) != null;
    final boolean safe = encrypted || isSafePeer(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR), headers);

    return Contexts.interceptCall(Context.current().withValue(SECRET_SAFE_TRANSPORT_KEY, safe), call, headers, next);
  }

  /**
   * Whether a cleartext peer is one whose leg does not reach a network the operator does not own: the
   * loopback interface, or a proxy the operator listed that reports https for every hop. Fails closed on
   * anything it cannot read as an IP socket - an in-process or unix-domain transport reports an address
   * type this does not recognise, and answering "safe" for an address whose shape is unknown is the wrong
   * direction to guess in.
   */
  private boolean isSafePeer(final SocketAddress remoteAddress, final Metadata headers) {
    if (!(remoteAddress instanceof final InetSocketAddress inetAddress))
      return false;

    // Already-resolved is the normal case for an accepted connection; getAddress() returning null
    // means the address is unresolved, and an unresolved peer is not a peer known to be local.
    final InetAddress address = inetAddress.getAddress();
    if (address == null)
      return false;

    if (address.isLoopbackAddress())
      return true;

    // An empty list is the shipped default and means "no proxy vouches for anything", so the metadata stays
    // unread; isBlocked() asks whether the peer is in the operator's set, not whether it is blocked.
    final IPAddressBlocklist proxies = currentTrustedProxies();
    if (proxies.isEmpty() || !proxies.isBlocked(address))
      return false;

    final Iterable<String> values = headers != null ? headers.getAll(X_FORWARDED_PROTO_KEY) : null;
    return values != null && PostApiTokenHandler.forwardedProtoIsFullyEncrypted(String.join(",", values));
  }

  /**
   * The parsed list for the setting's current text, re-parsed only when the text changes: this runs on
   * every cleartext call from a remote peer, not only on mints, so it must not re-parse an unchanged list.
   * A setting that cannot be read trusts nobody.
   */
  private IPAddressBlocklist currentTrustedProxies() {
    String csv;
    try {
      csv = trustedProxiesSetting.get();
    } catch (final RuntimeException e) {
      csv = null;
    }
    if (csv == null || csv.isBlank())
      return NO_TRUSTED_PROXIES.list();

    final TrustedProxies cached = trustedProxies;
    if (csv.equals(cached.csv()))
      return cached.list();

    // A benign race: two calls seeing a change at once both parse it and store equal results.
    final TrustedProxies parsed = new TrustedProxies(csv, PostApiTokenHandler.parseTrustedProxies(csv));
    trustedProxies = parsed;
    return parsed.list();
  }
}
