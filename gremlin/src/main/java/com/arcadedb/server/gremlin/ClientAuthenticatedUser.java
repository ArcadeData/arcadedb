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
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;

/**
 * The user a Gremlin connection authenticated as, together with the {@link RunningQuery.Client} that connection is
 * (issue #10105) and the {@link ServerSecurityUser} the login resolved, so every request of a connection that lives on can be
 * checked against the user as it is now, not only against the name it logged in with.
 * <p>
 * TinkerPop binds the authenticated user to the connection and hands it to the authorizer with every request, whichever
 * channelizer carries the connection. That makes it the one place the identity of the client can travel independently of
 * the pipeline: {@link ArcadeGremlinAuthorizer} publishes the client beside the principal, on the thread that submits the
 * evaluation, and {@link GremlinPrincipalPropagatingExecutorService} carries both to the pool thread that evaluates it.
 * Before, the client was published only by {@link ConcealingWebSocketChannelizer}'s own handler, so a server configured
 * with another channelizer listed every statement with no client.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ClientAuthenticatedUser extends AuthenticatedUser {
  private final    RunningQuery.Client client;
  private volatile ServerSecurityUser  principal;

  public ClientAuthenticatedUser(final ServerSecurityUser principal, final RunningQuery.Client client) {
    super(principal.getName());
    this.principal = principal;
    this.client = client;
  }

  /** The connection this user authenticated on, as {@code list queries} names it. */
  public RunningQuery.Client getClient() {
    return client;
  }

  /**
   * Resolves the user this connection is now: the one registered under its name, which carries the database grants in
   * force. The connection keeps the user it authenticated as until a request finds it changed, so the next request is compared
   * with the latest one seen.
   *
   * @throws ServerSecurityException when the user no longer exists or its password is not the one the connection logged in with
   */
  public ServerSecurityUser revalidate(final ServerSecurity security) {
    final ServerSecurityUser current = security.revalidate(principal);
    principal = current;
    return current;
  }
}
