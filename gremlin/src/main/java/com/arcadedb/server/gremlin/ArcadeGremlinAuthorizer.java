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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.util.Iterator;
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
 * <p>
 * It also reserves to the server administrator every request that makes the server evaluate Groovy: a string script
 * in any language other than {@value #GREMLIN_LANG} (TinkerPop evaluates a script with no language as
 * {@code gremlin-groovy}) and a bytecode traversal carrying a lambda, whose body travels as Groovy source. Groovy is
 * arbitrary JVM code, so its reach is the host, not the database the request targets. Every other user keeps
 * bytecode traversals and {@value #GREMLIN_LANG} string scripts, both of which are parsed by the Gremlin grammar
 * rather than executed as code.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeGremlinAuthorizer implements Authorizer {
  static final         String         GREMLIN_LANG = "gremlin-lang";
  // DEEPER THAN ANY TRAVERSAL A CLIENT BUILDS ON PURPOSE: BEYOND IT THE REQUEST IS TREATED AS CARRYING A LAMBDA
  private static final int            MAX_LAMBDA_SCAN_DEPTH = 64;
  private              ArcadeDBServer server;

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

    if (!ServerSecurityUser.isServerAdministrator(securityUser.getName()) && containsLambda(bytecode, 0))
      throw new AuthorizationException(
          "User '" + securityUser.getName() + "' is not authorized to use lambdas: they are evaluated as Groovy code, which is reserved to the server administrator");
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

    final Object language = msg.getArgs().get(Tokens.ARGS_LANGUAGE);
    if (!ServerSecurityUser.isServerAdministrator(securityUser.getName()) && !GREMLIN_LANG.equals(language))
      throw new AuthorizationException("User '" + securityUser.getName() + "' is not authorized to evaluate "
          + (language != null ? "'" + language + "'" : "Groovy") + " scripts, which are reserved to the server administrator. Submit the script with language '"
          + GREMLIN_LANG + "' or send a bytecode traversal");

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

  /**
   * Whether a lambda travels anywhere in the request: as a step argument, in a nested traversal, behind a
   * {@link Bytecode.Binding} or a {@link GValue} parameter, inside a collection, map or array argument, or in the configuration of a strategy passed to
   * {@code withStrategies()}. TinkerPop's {@code BytecodeHelper.getLambdaLanguage} only follows nested bytecode, so it
   * would miss the other carriers. Fails closed: a structure nested deeper than {@value #MAX_LAMBDA_SCAN_DEPTH} levels
   * counts as carrying a lambda.
   */
  static boolean containsLambda(final Object value, final int depth) {
    if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean)
      return false;
    if (depth > MAX_LAMBDA_SCAN_DEPTH || value instanceof Lambda)
      return true;

    final int next = depth + 1;
    if (value instanceof Bytecode bytecode) {
      for (final Bytecode.Instruction instruction : bytecode.getInstructions())
        for (final Object argument : instruction.getArguments())
          if (containsLambda(argument, next))
            return true;
    } else if (value instanceof Bytecode.Binding<?> binding)
      return containsLambda(binding.value(), next);
    else if (value instanceof GValue<?> parameter)
      return containsLambda(parameter.get(), next);
    else if (value instanceof Traversal<?, ?> traversal)
      return containsLambda(traversal.asAdmin().getBytecode(), next);
    else if (value instanceof TraversalStrategy<?> strategy) {
      final var configuration = strategy.getConfiguration();
      if (configuration != null)
        for (final Iterator<String> keys = configuration.getKeys(); keys.hasNext(); )
          if (containsLambda(configuration.getProperty(keys.next()), next))
            return true;
    } else if (value instanceof Iterable<?> iterable) {
      for (final Object element : iterable)
        if (containsLambda(element, next))
          return true;
    } else if (value instanceof Map<?, ?> map) {
      for (final Map.Entry<?, ?> entry : map.entrySet())
        if (containsLambda(entry.getKey(), next) || containsLambda(entry.getValue(), next))
          return true;
    } else if (value instanceof Object[] array) {
      for (final Object element : array)
        if (containsLambda(element, next))
          return true;
    }
    return false;
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
