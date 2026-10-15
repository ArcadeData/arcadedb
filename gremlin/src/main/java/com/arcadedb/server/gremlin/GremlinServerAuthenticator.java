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
package com.arcadedb.server.gremlin;

import com.arcadedb.query.RunningQuery;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class GremlinServerAuthenticator implements Authenticator {
  /**
   * The credentials member the HTTP basic authentication handler fills with the peer of the channel (TinkerPop's
   * {@code CredentialGraphTokens.PROPERTY_ADDRESS}), spelled out so the module does not depend on the Groovy DSL for it.
   */
  static final String ADDRESS_CREDENTIAL = "address";

  /**
   * Numbers the connections that authenticate once and live on (SASL), so two clients of one credential are two clients
   * (issue #10105). Unique within the process, which is what {@code list queries} needs: the numbering restarts at 1 with
   * the JVM, so it is not an identity to correlate across restarts.
   */
  private static final AtomicLong CONNECTIONS = new AtomicLong();

  private ArcadeDBServer server;

  @Override
  public boolean requireAuthentication() {
    return true;
  }

  @Override
  public void setup(final Map<String, Object> config) {
    this.server = (ArcadeDBServer) config.get("server");
  }

  public Authenticator.SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
    // The peer is known here, before the credentials are: the user that comes out of the negotiation carries it. A WebSocket
    // connection authenticates once and lives on, so the number is the connection: two of one credential are two clients.
    // The port is missing, TinkerPop passes the address only
    return new GremlinServerAuthenticator.PlainTextSaslAuthenticator(new RunningQuery.Client(newConnectionId(),
        peer(remoteAddress != null ? remoteAddress.getHostAddress() : null)));
  }

  @Override
  public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
    // The HTTP basic authentication handler authenticates EVERY request and has no connection to number: it passes the peer of
    // the channel, "host:port", which is what ties the requests of one keep-alive connection together and tells two apart.
    // The handler fills this member itself, after it has parsed the credentials out of the Authorization header (exactly
    // user:password), so a client cannot supply it. It is attribution for "list queries", never an authorization input
    final String peer = peer(credentials.get(ADDRESS_CREDENTIAL));
    return authenticate(credentials, peer != null ?
        new RunningQuery.Client(null, peer) :
        new RunningQuery.Client(newConnectionId(), null));
  }

  private AuthenticatedUser authenticate(final Map<String, String> credentials, final RunningQuery.Client client) {
    final String userName = credentials.get("username");
    final String userPassword = credentials.get("password");
    final String databaseName = credentials.get("databaseName");

    final ServerSecurityUser user = server.getSecurity().authenticate(userName, userPassword, databaseName);

    return new ClientAuthenticatedUser(user.getName(), client);
  }

  private static String newConnectionId() {
    return Long.toHexString(CONNECTIONS.incrementAndGet());
  }

  /**
   * The peer in the shape {@code SocketAddress.toString()} gives ({@code /host[:port]}), which is what the channelizer's own
   * publication carries, or {@code null} when the transport reported none.
   */
  private static String peer(final String address) {
    if (address == null || address.isBlank())
      return null;
    return address.startsWith("/") ? address : "/" + address;
  }

  private class PlainTextSaslAuthenticator implements Authenticator.SaslNegotiator {
    private final RunningQuery.Client client;
    private       boolean             complete;
    private       String              username;
    private       String              password;

    private PlainTextSaslAuthenticator(final RunningQuery.Client client) {
      this.client = client;
      this.complete = false;
    }

    public byte[] evaluateResponse(final byte[] clientResponse) throws AuthenticationException {
      this.decodeCredentials(clientResponse);
      this.complete = true;
      return null;
    }

    public boolean isComplete() {
      return this.complete;
    }

    public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
      if (!this.complete) {
        throw new AuthenticationException("SASL negotiation not complete");
      } else {
        try {
          final Map<String, String> credentials = new HashMap<>();
          credentials.put("username", this.username);
          credentials.put("password", this.password);
          return GremlinServerAuthenticator.this.authenticate(credentials, client);
        } catch (final Exception e) {
          throw new AuthenticationException(e);
        }
      }
    }

    private void decodeCredentials(final byte[] bytes) throws AuthenticationException {
      byte[] user = null;
      byte[] pass = null;
      int end = bytes.length;

      for (int i = bytes.length - 1; i >= 0; --i) {
        if (bytes[i] == 0) {
          if (pass == null) {
            pass = Arrays.copyOfRange(bytes, i + 1, end);
          } else if (user == null) {
            user = Arrays.copyOfRange(bytes, i + 1, end);
          }

          end = i;
        }
      }

      if (null == user) {
        throw new AuthenticationException("Authentication ID must not be null");
      } else if (null == pass) {
        throw new AuthenticationException("Password must not be null");
      } else {
        this.username = new String(user, StandardCharsets.UTF_8);
        this.password = new String(pass, StandardCharsets.UTF_8);
      }
    }
  }
}
