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

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;

import java.net.*;
import java.nio.charset.*;
import java.util.*;

public class GremlinServerAuthenticator implements org.apache.tinkerpop.gremlin.server.auth.Authenticator {
  private ArcadeDBServer server;

  public GremlinServerAuthenticator() {
  }

  @Override
  public boolean requireAuthentication() {
    return true;
  }

  @Override
  public void setup(final Map<String, Object> config) {
    this.server = (ArcadeDBServer) config.get("server");
  }

  public Authenticator.SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
    return new GremlinServerAuthenticator.PlainTextSaslAuthenticator();
  }

  @Override
  public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
    final String userName = credentials.get("username");
    final String userPassword = credentials.get("password");
    final String databaseName = credentials.get("databaseName");

    final ServerSecurityUser user = server.getSecurity().authenticate(userName, userPassword, databaseName);

    return new AuthenticatedUser(user.getName());
  }

  private class PlainTextSaslAuthenticator implements Authenticator.SaslNegotiator {
    private boolean complete;
    private String  username;
    private String  password;

    private PlainTextSaslAuthenticator() {
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
          Map<String, String> credentials = new HashMap();
          credentials.put("username", this.username);
          credentials.put("password", this.password);
          return GremlinServerAuthenticator.this.authenticate(credentials);
        } catch (Exception e) {
          throw new AuthenticationException(e);
        }
      }
    }

    private void decodeCredentials(byte[] bytes) throws AuthenticationException {
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
