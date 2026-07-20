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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.util.Map;

/**
 * Enforces per-database authorization for the Gremlin wire protocol. The {@link GremlinServerAuthenticator}
 * only validates credentials; without this gate any valid server credential could read, write, and drop
 * ANY database by naming it as the traversal-source alias, bypassing ArcadeDB's {@code canAccessToDatabase}
 * model entirely (GHSA-c287-v325-j5jx).
 * <p>
 * Runs as a TinkerPop {@link Authorizer} on every bytecode and string request, before the traversal is
 * executed, and rejects the request with an {@link AuthorizationException} when the authenticated user is
 * not granted access to the targeted database - mirroring the check the HTTP and BOLT transports perform.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeGremlinAuthorizer implements Authorizer {
  private ArcadeDBServer server;

  @Override
  public void setup(final Map<String, Object> config) {
    this.server = (ArcadeDBServer) config.get("server");
  }

  /**
   * Authorizes a bytecode (traversal) request. The {@code aliases} map values are the traversal-source
   * names targeted by the request, i.e. the database names the traversal will run against.
   */
  @Override
  public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode, final Map<String, String> aliases)
      throws AuthorizationException {
    final ServerSecurityUser securityUser = publishPrincipal(user);
    if (aliases != null)
      for (final String alias : aliases.values())
        checkDatabaseAccess(securityUser, alias);
    return bytecode;
  }

  /**
   * Authorizes a string (script) request. The target traversal source(s) are carried in the request's
   * {@code aliases} argument. A bare script may reference a globally-bound graph directly, with no alias;
   * that case is backstopped by the principal binding performed on the execution thread (the engine's own
   * per-database ACL then denies unauthorized access), which is why the principal is always published here.
   */
  @Override
  public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
    // Session-based requests execute on a per-session executor that the principal-binding pool does not
    // cover, so the engine's per-database/per-type ACLs cannot be enforced for them. Reject them:
    // ArcadeDB does not use Gremlin sessions and TinkerPop's SessionOpProcessor is deprecated
    // (GHSA-c287-v325-j5jx). The processor is also stripped from the server settings as defense-in-depth.
    if ("session".equals(msg.getProcessor()) || msg.getArgs().containsKey(Tokens.ARGS_SESSION))
      throw new AuthorizationException("Gremlin sessions are not supported");

    final ServerSecurityUser securityUser = publishPrincipal(user);
    final Object aliasesArg = msg.getArgs().get(Tokens.ARGS_ALIASES);
    if (aliasesArg instanceof Map<?, ?> aliases)
      for (final Object alias : aliases.values())
        checkDatabaseAccess(securityUser, String.valueOf(alias));
  }

  /**
   * Resolves the authenticated principal and publishes it to {@link GremlinAuthContext} so the execution
   * pool can bind it into the engine. Fails closed: an unknown/disabled user is rejected rather than left
   * unbound (an unbound principal would make the engine's permission gates no-op).
   */
  private ServerSecurityUser publishPrincipal(final AuthenticatedUser user) throws AuthorizationException {
    final ServerSecurityUser securityUser = server.getSecurity().getUser(user.getName());
    if (securityUser == null)
      throw new AuthorizationException("Unknown or disabled user '" + user.getName() + "'");
    GremlinAuthContext.set(securityUser);
    return securityUser;
  }

  private void checkDatabaseAccess(final ServerSecurityUser securityUser, final String traversalSourceAlias)
      throws AuthorizationException {
    if (traversalSourceAlias == null)
      return;

    final String databaseName = ArcadeGraphManager.resolveDatabaseName(traversalSourceAlias);
    if (!securityUser.canAccessToDatabase(databaseName))
      throw new AuthorizationException(
          "User '" + securityUser.getName() + "' is not authorized to access database '" + databaseName + "'");
  }
}
