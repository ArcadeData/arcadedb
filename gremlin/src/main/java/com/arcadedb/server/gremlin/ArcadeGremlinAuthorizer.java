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
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.CardinalityValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
 * <p>
 * A bytecode request is a list of instructions, each naming by its operator a method of the traversal or of the traversal
 * source, which TinkerPop's translator invokes by name. A request is accepted only when every operator in it, nested traversals
 * included, is a step of the traversal DSL or an option of the traversal source, which is all a client driver records. The check
 * runs for every user, before the translation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeGremlinAuthorizer implements Authorizer {
  static final         String GREMLIN_LANG = "gremlin-lang";
  // DEEPER THAN ANY TRAVERSAL A CLIENT BUILDS ON PURPOSE: BEYOND IT THE REQUEST IS TREATED AS CARRYING A LAMBDA OR AN UNSUPPORTED INSTRUCTION
  private static final int    MAX_SCAN_DEPTH = 64;
  private static final int    OPERATOR_PREVIEW_LENGTH = 48;
  // THE OPERATORS A CLIENT DRIVER RECORDS: THE SYMBOLS OF THE TRAVERSAL DSL AND OF THE TRAVERSAL SOURCE, READ FROM TINKERPOP SO A STEP
  // IT ADDS IS ACCEPTED AND A METHOD IT ADDS THAT IS NOT A STEP IS NOT. CardinalityValueTraversal IS THE ONE SOURCE INSTRUCTION A NESTED
  // TRAVERSAL CARRIES
  private static final Set<String> STEP_OPERATORS   = symbols(GraphTraversal.Symbols.class, Traversal.Symbols.class);
  private static final Set<String> SOURCE_OPERATORS = sourceOperators();

  private static final Rule LAMBDA                  = value -> value instanceof Lambda;
  private static final Rule UNSUPPORTED_INSTRUCTION = value -> value instanceof Bytecode bytecode && unsupportedOperator(bytecode) != null;

  private ArcadeDBServer server;

  /** What the request scan looks for: a value that breaks the rule on its own, whatever is nested in it. */
  @FunctionalInterface
  private interface Rule {
    boolean refuses(Object value);
  }

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

    final String unsupported = unsupportedInstructionMessage(bytecode, 0);
    if (unsupported != null)
      throw new AuthorizationException(unsupported);

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
   * <p>
   * A connection authenticates once and lives on, so a user this server authenticated is checked against the principal it logged in
   * as: a user that was deleted, or whose password is no longer the one the connection proved, is refused on the next request, as
   * the other long-lived connections of the server do.
   */
  private ServerSecurityUser publishPrincipal(final AuthenticatedUser user) throws AuthorizationException {
    final ServerSecurityUser securityUser;
    if (user instanceof ClientAuthenticatedUser connection) {
      try {
        securityUser = connection.revalidate(server.getSecurity());
      } catch (final ServerSecurityException e) {
        throw new AuthorizationException("The credentials of this connection are no longer valid", e);
      }
    } else
      securityUser = server.getSecurity().getUser(user.getName());
    if (securityUser == null)
      throw new AuthorizationException("Unknown or disabled user '" + user.getName() + "'");
    GremlinAuthContext.set(securityUser);
    // The connection of the request travels with its user (issue #10105), on the same thread and for the same pool task, so it
    // does not depend on the channelizer that carries the connection to publish it. A user this server did not authenticate on a
    // connection carries none, and what the previous request left on the thread must not be taken for its client. The
    // channelizer's own publication, downstream of this one, replaces it with the more precise channel and port
    GremlinAuthContext.setClient(user instanceof ClientAuthenticatedUser connection ? connection.getClient() : null);
    return securityUser;
  }

  /**
   * Whether a lambda travels anywhere in the request: as a step argument, in a nested traversal, behind a
   * {@link Bytecode.Binding} or a {@link GValue} parameter, inside a predicate ({@link P}, including {@code and}/{@code or}), inside a collection, map or array argument, or in the configuration of a strategy passed to
   * {@code withStrategies()}. TinkerPop's {@code BytecodeHelper.getLambdaLanguage} only follows nested bytecode, so it
   * would miss the other carriers. Fails closed: a structure nested deeper than {@value #MAX_SCAN_DEPTH} levels
   * counts as carrying a lambda.
   */
  static boolean containsLambda(final Object value, final int depth) {
    return scan(value, depth, LAMBDA) != null;
  }

  /**
   * What the client is told when an instruction a client driver never records travels anywhere in the request, looked for in the
   * same places as {@link #containsLambda}: a bytecode whose step operators are not steps of the traversal DSL, or whose source
   * operators are not options of the traversal source. The operator is client input, so only the start of it goes into the message.
   * Fails closed: a structure nested deeper than {@value #MAX_SCAN_DEPTH} levels counts as carrying one.
   *
   * @return the message, or {@code null} when every instruction of the request is one a driver records
   */
  static String unsupportedInstructionMessage(final Object value, final int depth) {
    final Object refused = scan(value, depth, UNSUPPORTED_INSTRUCTION);
    if (refused == null)
      return null;

    final String operator = refused instanceof Bytecode bytecode ? unsupportedOperator(bytecode) : null;
    if (operator == null)
      return "The traversal is nested too deeply";
    return "Traversal instruction '" + (operator.length() > OPERATOR_PREVIEW_LENGTH ? operator.substring(0, OPERATOR_PREVIEW_LENGTH) + "..." : operator)
        + "' is not supported";
  }

  /**
   * Looks for a value that breaks {@code rule} in {@code value} and in everything nested in it.
   *
   * @return the value that breaks the rule, or the structure that is nested too deeply, or {@code null} when there is none
   */
  private static Object scan(final Object value, final int depth, final Rule rule) {
    if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean)
      return null;
    if (depth > MAX_SCAN_DEPTH || rule.refuses(value))
      return value;

    final int next = depth + 1;
    if (value instanceof Bytecode bytecode) {
      for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
        final Object refused = scanEach(instruction.getArguments(), next, rule);
        if (refused != null)
          return refused;
      }
    } else if (value instanceof Bytecode.Binding<?> binding)
      return scan(binding.value(), next, rule);
    else if (value instanceof GValue<?> parameter)
      return scan(parameter.get(), next, rule);
    else if (value instanceof ConnectiveP<?> connective)
      return scanEach(connective.getPredicates(), next, rule);
    else if (value instanceof P<?> predicate) {
      final Object refused = scan(predicate.getBiPredicate(), next, rule);
      return refused != null ? refused : scan(predicate.getValue(), next, rule);
    } else if (value instanceof Traversal<?, ?> traversal)
      return scan(traversal.asAdmin().getBytecode(), next, rule);
    else if (value instanceof TraversalStrategy<?> strategy) {
      final var configuration = strategy.getConfiguration();
      if (configuration != null)
        for (final Iterator<String> keys = configuration.getKeys(); keys.hasNext(); ) {
          final Object refused = scan(configuration.getProperty(keys.next()), next, rule);
          if (refused != null)
            return refused;
        }
    } else if (value instanceof Iterable<?> iterable)
      return scanEach(iterable, next, rule);
    else if (value instanceof Map<?, ?> map) {
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        final Object refused = scan(entry.getKey(), next, rule);
        if (refused != null)
          return refused;
        final Object refusedValue = scan(entry.getValue(), next, rule);
        if (refusedValue != null)
          return refusedValue;
      }
    } else if (value instanceof Object[] array)
      return scanEach(array, next, rule);
    return null;
  }

  private static Object scanEach(final Iterable<?> values, final int depth, final Rule rule) {
    for (final Object value : values) {
      final Object refused = scan(value, depth, rule);
      if (refused != null)
        return refused;
    }
    return null;
  }

  private static Object scanEach(final Object[] values, final int depth, final Rule rule) {
    if (values == null)
      return null;
    for (final Object value : values) {
      final Object refused = scan(value, depth, rule);
      if (refused != null)
        return refused;
    }
    return null;
  }

  /** The first operator of the bytecode that a client driver does not record, or {@code null} when there is none. */
  private static String unsupportedOperator(final Bytecode bytecode) {
    for (final Bytecode.Instruction instruction : bytecode.getSourceInstructions())
      if (!isSupported(SOURCE_OPERATORS, instruction.getOperator()))
        return String.valueOf(instruction.getOperator());
    for (final Bytecode.Instruction instruction : bytecode.getStepInstructions())
      if (!isSupported(STEP_OPERATORS, instruction.getOperator()))
        return String.valueOf(instruction.getOperator());
    return null;
  }

  private static boolean isSupported(final Set<String> operators, final String operator) {
    return operator != null && operators.contains(operator);
  }

  /** The values of the public string constants of {@code holders}: the operator names TinkerPop defines. */
  private static Set<String> symbols(final Class<?>... holders) {
    final Set<String> names = new HashSet<>();
    for (final Class<?> holder : holders)
      for (final Field field : holder.getFields())
        if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class)
          try {
            names.add((String) field.get(null));
          } catch (final IllegalAccessException e) {
            // FAIL CLOSED: WITHOUT THE NAMES, A REQUEST COULD NOT BE TOLD FROM ONE THAT NAMES A METHOD THAT IS NOT A STEP
            throw new ExceptionInInitializerError(e);
          }
    return Collections.unmodifiableSet(names);
  }

  private static Set<String> sourceOperators() {
    final Set<String> operators = new HashSet<>(symbols(TraversalSource.Symbols.class, GraphTraversalSource.Symbols.class));
    operators.add(CardinalityValueTraversal.class.getSimpleName());
    return Collections.unmodifiableSet(operators);
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
