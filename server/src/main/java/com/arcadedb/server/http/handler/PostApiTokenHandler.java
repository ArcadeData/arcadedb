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
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ApiTokenTrustedProxies;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.IPAddressBlocklist;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;

import java.io.IOException;
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
 * in {@link GlobalConfiguration#SERVER_API_TOKEN_TRUSTED_PROXIES} - see {@link #isTransportSafeForSecrets}. The proxy
 * reports the scheme through {@code X-Forwarded-Proto} or the RFC 7239 {@code Forwarded} header (issue #7822). The gRPC
 * mint reads the same list, through the {@code x-forwarded-proto} and {@code forwarded} metadata keys, with the same
 * {@link ApiTokenTrustedProxies} helpers (issue #7821).
 * Studio renders the 412 through {@code apiTokenTransportRefusal()} in {@code studio-security.js}.
 * <p>
 * <b>On an HA cluster the mint is forwarded to the leader</b>, as {@code /server/users} is (issue #8109), and the
 * transport is then checked on BOTH legs the plaintext token travels back over. The follower checks the client's
 * connection before forwarding, because the leader cannot see it - on a cluster whose nodes share a host the hop
 * even arrives from loopback, which would pass for a safe client. The leader checks the hop it received the forward
 * on, because that leg carries the token too: with the cluster's inter-node HTTP in cleartext, a mint the client
 * made over HTTPS still puts the token on a wire in the clear, and the refusal says so rather than telling the client
 * to reconnect over a TLS connection it is already using.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostApiTokenHandler extends AbstractServerHttpHandler {
  static final String X_FORWARDED_PROTO = "X-Forwarded-Proto";
  /** The RFC 7239 header, read with the same rule as {@link #X_FORWARDED_PROTO} (issue #7822). */
  static final String FORWARDED         = "Forwarded";

  private final ServerControlPlane controlPlane;

  public PostApiTokenHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws IOException {
    checkRootUser(user);

    final ContextConfiguration configuration = httpServer.getServer().getConfiguration();

    // Parsed per request rather than cached so SET SERVER SETTING takes effect on the next mint. Minting is
    // an administrative operation measured in units per day, and the list is a handful of entries, so the
    // parse is not on any path where it could be measured.
    final ExecutionResponse refusal = checkTransport(exchange.getRequestScheme(), exchange.getSourceAddress(),
        forwardedProtoOf(exchange.getRequestHeaders()),
        parseTrustedProxies(configuration.getValueAsString(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES)),
        configuration.getValueAsBoolean(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT));
    if (refusal != null)
      return refusalForThisLeg(refusal);

    // After the transport check, which is about the client's own connection and only this node can see it, and
    // before any validation of the request, which the leader does (issue #8109).
    final ExecutionResponse forwarded = httpServer.getLeaderCommandForwarder()
        .forwardIfReplica(exchange, user, LeaderCommandForwarder.currentPathWithQuery(exchange),
            payload != null ? payload.toString() : null);
    if (forwarded != null)
      return forwarded;

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
   * The transport refusal to answer on the connection this mint arrived on: {@code refusal} as {@link #checkTransport}
   * built it for a client, or - when that connection is a trusted hop from a follower that forwarded the mint - the
   * refusal the leader answers a mint a follower forwarded to it over a connection that is not safe for secrets
   * (issue #8109). The client's own leg was already checked by the follower, so "connect over TLS" would send the
   * client after the wrong connection: what is unprotected is the hop between the two nodes.
   */
  static ExecutionResponse refusalForThisLeg(final ExecutionResponse refusal) {
    return LeaderForwardContext.isAlreadyForwarded() ? forwardedHopRefusal() : refusal;
  }

  private static ExecutionResponse forwardedHopRefusal() {
    return new ExecutionResponse(412, new JSONObject().put("error",
        "API tokens can only be minted over HTTPS or from a loopback client, and this mint reached the cluster "
            + "leader from another node over a connection that is neither: the token would cross the network between "
            + "the two nodes in the clear. Enable HTTPS between the cluster nodes, send the request to the leader "
            + "directly, or set " + GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey()
            + "=false to allow it").toString());
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
   * @param forwardedProto every scheme the proxies reported, as {@link #forwardedProtoOf} flattens it, or null
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

  /** @see ApiTokenTrustedProxies#parse(String) */
  static IPAddressBlocklist parseTrustedProxies(final String csv) {
    return ApiTokenTrustedProxies.parse(csv);
  }

  /**
   * Every scheme the request's proxies reported, from {@code X-Forwarded-Proto} and from the RFC 7239
   * {@code Forwarded} header alike, as the one comma-separated list {@link #forwardedProtoIsFullyEncrypted} checks.
   * The handler reads the headers through this method, so a test that drives it drives the live path.
   *
   * @see ApiTokenTrustedProxies#reportedForwardedProto(Iterable, Iterable)
   */
  static String forwardedProtoOf(final HeaderMap headers) {
    return ApiTokenTrustedProxies.reportedForwardedProto(headers.get(X_FORWARDED_PROTO), headers.get(FORWARDED));
  }

  /** @see ApiTokenTrustedProxies#joinForwardedProto(Iterable) */
  static String joinForwardedProto(final Collection<String> values) {
    return ApiTokenTrustedProxies.joinForwardedProto(values);
  }

  /** @see ApiTokenTrustedProxies#forwardedProtoIsFullyEncrypted(String) */
  static boolean forwardedProtoIsFullyEncrypted(final String forwardedProto) {
    return ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted(forwardedProto);
  }

  /**
   * Whether a response body written back on this connection stays out of reach of anything on the path.
   * Either the transport is encrypted, or the peer is on the loopback interface, where the bytes never
   * reach a network - the same pair of conditions {@code GrpcTransportSecurityInterceptor} applies.
   * <p>
   * Read from the live connection, and from {@code X-Forwarded-Proto} or {@code Forwarded} (see
   * {@link #forwardedProtoOf}) only when the peer that sent it is one
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
   * @param forwardedProto every scheme the proxies reported, as {@link #forwardedProtoOf} flattens it, or null
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
