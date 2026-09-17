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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.net.InetAddress;
import java.net.InetSocketAddress;
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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostApiTokenHandler extends AbstractServerHttpHandler {
  private final ServerControlPlane controlPlane;

  public PostApiTokenHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final ExecutionResponse refusal = checkTransport(exchange.getRequestScheme(), exchange.getSourceAddress(),
        httpServer.getServer().getConfiguration()
            .getValueAsBoolean(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT));
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
    if (isTransportSafeForSecrets(scheme, peer))
      return null;

    if (requireSecure)
      // 412 rather than 403: the credentials are exactly right and the same request over HTTPS succeeds.
      // What is wrong is the connection it arrived on, which is the caller's to fix by reconnecting, not an
      // authorization decision to appeal.
      return new ExecutionResponse(412, new JSONObject().put("error",
          "API tokens can only be minted over HTTPS or from a loopback client. Connect over TLS, or set "
              + GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey() + "=false to allow it")
          .toString());

    LogManager.instance().log(PostApiTokenHandler.class, Level.WARNING,
        "Minting an API token over an unprotected transport (scheme=%s, peer=%s): the token is readable on the "
            + "wire. Set %s=true to refuse this", null, scheme, peer,
        GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey());
    return null;
  }

  /**
   * Whether a response body written back on this connection stays out of reach of anything on the path.
   * Either the transport is encrypted, or the peer is on the loopback interface, where the bytes never
   * reach a network - the same pair of conditions {@code GrpcTransportSecurityInterceptor} applies.
   * <p>
   * Read from the live connection, never from {@code X-Forwarded-Proto}: that header is written by the
   * client and a caller asking for a token is exactly the caller who could set it. A TLS-terminating
   * proxy that forwards from a non-loopback address therefore reads as unprotected here; an operator
   * fronting a cleartext listener has moved the trust boundary to the proxy and states that by leaving
   * {@link GlobalConfiguration#SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT} off.
   * <p>
   * Fails closed on an address it cannot read as a resolved IP socket: answering "safe" for an address
   * whose shape is unknown is the wrong direction to guess in.
   */
  static boolean isTransportSafeForSecrets(final String scheme, final InetSocketAddress peer) {
    if ("https".equalsIgnoreCase(scheme))
      return true;

    if (peer == null)
      return false;

    final InetAddress address = peer.getAddress();
    return address != null && address.isLoopbackAddress();
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }
}
