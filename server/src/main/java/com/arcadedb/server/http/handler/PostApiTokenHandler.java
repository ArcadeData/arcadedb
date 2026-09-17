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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.IPAddressBlocklist;
import io.undertow.server.HttpServerExchange;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.logging.Level;

/**
 * {@code POST /server/api-tokens}: mints a token and returns it, plaintext, exactly once. Minting and
 * the validation of the permission document live in {@link ServerControlPlane#createApiToken} so the
 * gRPC {@code CreateApiToken} RPC applies the same rules (issue #7309).
 * <p>
 * <b>Transport precondition.</b> The token is the one field this API returns that the server can never
 * reproduce and that authenticates its holder, so it must not be written back over a connection that
 * puts it on the wire in the clear. gRPC has refused that since 26.10.1
 * ({@code GrpcTransportSecurityInterceptor}); this route now applies the same rule - HTTPS, or a
 * loopback peer - but only when {@link GlobalConfiguration#SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT}
 * is on. It is off by default because Studio's own token UI mints over this route, so refusing by
 * default would break every Studio served over plain HTTP from a remote host: tightening a route that
 * has always behaved this way is a compatibility decision, and it is the operator's to take. With the
 * setting off, an unprotected mint is logged at WARNING rather than passing unremarked (issue #7372).
 * <p>
 * Issue #7804 settled the two questions that left open. The default flips in 27.1.1, stated on the
 * setting itself so an operator reads the window rather than discovering it; and a TLS-terminating
 * reverse proxy can vouch for the leg it terminated, but only from a peer address the operator listed
 * in {@link GlobalConfiguration#SERVER_API_TOKEN_TRUSTED_PROXIES} - see {@link #isTransportSafeForSecrets}.
 * Studio renders the 412 through {@code apiTokenTransportRefusal()} in {@code studio-security.js}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostApiTokenHandler extends AbstractServerHttpHandler {
  static final String X_FORWARDED_PROTO = "X-Forwarded-Proto";

  private final ServerControlPlane controlPlane;

  public PostApiTokenHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final ContextConfiguration configuration = httpServer.getServer().getConfiguration();

    // Parsed per request rather than cached so SET SERVER SETTING takes effect on the next mint. Minting is
    // an administrative operation measured in units per day, and the list is a handful of entries, so the
    // parse is not on any path where it could be measured.
    final ExecutionResponse refusal = checkTransport(exchange.getRequestScheme(), exchange.getSourceAddress(),
        joinForwardedProto(exchange.getRequestHeaders().get(X_FORWARDED_PROTO)),
        parseTrustedProxies(configuration.getValueAsString(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES)),
        configuration.getValueAsBoolean(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT));
    if (refusal != null)
      return refusal;

    if (payload == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Request body is required").toString());

    final JSONObject tokenJson;
    try {
      tokenJson = controlPlane.createApiToken(
          payload.getString("name", ""),
          payload.getString("database", "*"),
          payload.getLong("expiresAt", 0),
          payload.getJSONObject("permissions", new JSONObject()));
    } catch (final ServerControlPlane.AlreadyExistsException e) {
      return new ExecutionResponse(409, new JSONObject().put("error", e.getMessage()).toString());
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, new JSONObject().put("error", e.getMessage()).toString());
    }

    final JSONObject response = new JSONObject();
    response.put("result", tokenJson);
    return new ExecutionResponse(201, response.toString());
  }

  /**
   * Decides what to do about the connection this mint arrived on: {@code null} to let it proceed, or the
   * refusal to answer with. Kept whole and static because the case that matters - a cleartext request from
   * a peer that is not on this machine - cannot be produced by a test that talks to 127.0.0.1, and a branch
   * no test can reach is a branch nobody knows the behaviour of.
   *
   * @param requireSecure the value of {@link GlobalConfiguration#SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT}
   */
  static ExecutionResponse checkTransport(final String scheme, final InetSocketAddress peer,
      final boolean requireSecure) {
    return checkTransport(scheme, peer, null, null, requireSecure);
  }

  /**
   * As above, additionally letting a reverse proxy the operator has listed vouch for the leg it terminated
   * (issue #7804).
   *
   * @param forwardedProto the {@code X-Forwarded-Proto} header as received, or null
   * @param trustedProxies the peers whose {@code forwardedProto} is worth reading, or null for none
   */
  static ExecutionResponse checkTransport(final String scheme, final InetSocketAddress peer,
      final String forwardedProto, final IPAddressBlocklist trustedProxies, final boolean requireSecure) {
    if (isTransportSafeForSecrets(scheme, peer, forwardedProto, trustedProxies))
      return null;

    if (requireSecure)
      // 412 rather than 403: the credentials are exactly right and the same request over HTTPS succeeds.
      // What is wrong is the connection it arrived on, which is the caller's to fix by reconnecting, not an
      // authorization decision to appeal.
      return new ExecutionResponse(412, new JSONObject().put("error",
          "API tokens can only be minted over HTTPS or from a loopback client. Connect over TLS, set "
              + GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES.getKey()
              + " if a reverse proxy terminates TLS in front of this server, or set "
              + GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey() + "=false to allow it")
          .toString());

    LogManager.instance().log(PostApiTokenHandler.class, Level.WARNING,
        "Minting an API token over an unprotected transport (scheme=%s, peer=%s): the token is readable on the "
            + "wire. Set %s=true to refuse this", null, scheme, peer,
        GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey());
    return null;
  }

  /**
   * Parses {@link GlobalConfiguration#SERVER_API_TOKEN_TRUSTED_PROXIES} into the matcher
   * {@link #isTransportSafeForSecrets} consults. A list that does not parse yields an empty matcher, which
   * trusts nobody: a typo in an allow-list has to deny, never widen. The operator is told at SEVERE, because
   * the symptom on its own - mints refused from behind the proxy they did configure - does not point here.
   * <p>
   * {@link IPAddressBlocklist} is reused rather than copied: it is the codebase's one CIDR parser, it already
   * rejects hostnames instead of resolving them, and duplicating a second matcher is how GHSA-67m7-7w7g-mpmh
   * got its bypass. Read {@code isBlocked} here as "this address is in the set" - the class is a set matcher;
   * only its callers decide whether membership allows or denies.
   * <p>
   * One consequence of that reuse is worth stating, since the class was written for a block-list and this is
   * the one allow-list using it: it also matches an IPv6 address that merely <em>encodes</em> a listed IPv4
   * one (IPv4-mapped, 6to4, Teredo, NAT64), so an entry of {@code 10.0.0.5} matches a peer arriving as
   * {@code ::ffff:10.0.0.5}. That is the intended reading - it is the same host - and it does not widen the
   * trust boundary, because a peer still has to complete a TCP handshake from the address it claims; an
   * attacker able to do that from the operator's proxy address is already on the path and can read the
   * cleartext leg without forging anything.
   */
  static IPAddressBlocklist parseTrustedProxies(final String csv) {
    try {
      return IPAddressBlocklist.parse(csv);
    } catch (final IllegalArgumentException e) {
      LogManager.instance().log(PostApiTokenHandler.class, Level.SEVERE,
          "Ignoring the whole of %s ('%s'): %s. No reverse proxy is trusted to vouch for the transport of an API "
              + "token mint until the list parses", null, GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES.getKey(),
          csv, e.getMessage());
      return IPAddressBlocklist.parse(null);
    }
  }

  /**
   * Flattens every {@code X-Forwarded-Proto} header on the request into the one comma-separated list
   * {@link #forwardedProtoIsFullyEncrypted} checks, so that all of them are read and not just the first.
   * <p>
   * This is the difference between trusting the proxy and trusting whoever reached it. A proxy configured to
   * <em>append</em> rather than overwrite leaves a client-supplied header in place and adds its own after it,
   * so the request arrives carrying two values and the client wrote the first one. Reading only that first
   * value would let a cleartext client send {@code X-Forwarded-Proto: https} and have it believed, which is
   * the exact forgery the trusted-proxy list exists to prevent. Requiring every value instead makes an
   * injected one useless: an injected {@code https} still leaves the proxy's own honest {@code http} in the
   * list, and the mint is refused.
   *
   * @param values every value of the header, in order, or null when the header is absent
   */
  static String joinForwardedProto(final Collection<String> values) {
    if (values == null || values.isEmpty())
      return null;

    return String.join(",", values);
  }

  /**
   * Whether every hop that reported a scheme in {@code X-Forwarded-Proto} encrypted its leg. The header carries
   * one entry per proxy, oldest (the client's own leg) first; one cleartext hop anywhere in the chain put the
   * response on a wire in the clear, and it does not matter which one. A blank or absent header reports nothing,
   * and nothing is not https.
   */
  static boolean forwardedProtoIsFullyEncrypted(final String forwardedProto) {
    if (forwardedProto == null || forwardedProto.isBlank())
      return false;

    for (final String hop : forwardedProto.split(","))
      if (!"https".equalsIgnoreCase(hop.trim()))
        return false;

    return true;
  }

  /**
   * Whether a response body written back on this connection stays out of reach of anything on the path.
   * Either the transport is encrypted, or the peer is on the loopback interface, where the bytes never
   * reach a network - the same pair of conditions {@code GrpcTransportSecurityInterceptor} applies.
   * <p>
   * Read from the live connection, and from {@code X-Forwarded-Proto} only when the peer that sent it is one
   * the operator listed in {@link GlobalConfiguration#SERVER_API_TOKEN_TRUSTED_PROXIES}. The header is written
   * by whoever connected, and a caller asking for a token is exactly the caller who would forge it, so on its
   * own it proves nothing; what the list adds is an operator saying which peers are their own infrastructure
   * (issue #7804). With the list empty - the default - the header is never consulted and this is exactly the
   * two-condition test #7372 shipped.
   * <p>
   * Fails closed on an address it cannot read as a resolved IP socket: answering "safe" for an address
   * whose shape is unknown is the wrong direction to guess in.
   */
  static boolean isTransportSafeForSecrets(final String scheme, final InetSocketAddress peer) {
    return isTransportSafeForSecrets(scheme, peer, null, null);
  }

  /**
   * @param forwardedProto the {@code X-Forwarded-Proto} header as received, or null
   * @param trustedProxies the peers whose {@code forwardedProto} is worth reading, or null for none
   *
   * @see #isTransportSafeForSecrets(String, InetSocketAddress)
   */
  static boolean isTransportSafeForSecrets(final String scheme, final InetSocketAddress peer,
      final String forwardedProto, final IPAddressBlocklist trustedProxies) {
    if ("https".equalsIgnoreCase(scheme))
      return true;

    if (peer == null)
      return false;

    final InetAddress address = peer.getAddress();
    if (address == null)
      return false;

    if (address.isLoopbackAddress())
      return true;

    // An empty list is the shipped default and means "no proxy vouches for anything", so the header stays
    // unread; isBlocked() here asks whether the peer is in the operator's set, not whether it is blocked.
    return trustedProxies != null && !trustedProxies.isEmpty() && trustedProxies.isBlocked(address)
        && forwardedProtoIsFullyEncrypted(forwardedProto);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }
}
