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
package com.arcadedb.gremlin.query;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.gremlin.ArcadeGremlin;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.HashMap;
import java.util.Map;

public class GremlinQueryEngine implements QueryEngine {
  static final  String      ENGINE_NAME = "gremlin";
  private final ArcadeGraph arcadeGraph;

  protected GremlinQueryEngine(final ArcadeGraph arcadeGraph) {
    this.arcadeGraph = arcadeGraph;
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    final ArcadeGremlin arcadeGremlin = arcadeGraph.gremlin(query);
    arcadeGremlin.setParameters(parameters);
    if (!arcadeGremlin.parse().isIdempotent())
      throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");
    // A statement that only reads: it streams, and has no use for a transaction of its own
    return execute(query, parameters);
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Object... parameters) {
    if (parameters.length % 2 != 0)
      throw new IllegalArgumentException("Command parameters must be as pairs `<key>, <value>`");

    final Map<String, Object> map = new HashMap<>(parameters.length / 2);
    for (int i = 0; i < parameters.length; i += 2)
      map.put((String) parameters[i], parameters[i + 1]);
    return query(query, configuration, map);
  }

  /**
   * Runs a statement. When the caller has no transaction open and the statement may write, it is ONE transaction, as an openCypher
   * statement is since #10190 and as it is over HTTP, where the handler opens one around every command (issue #10220): the traversal
   * runs to its end inside this call, its rows are kept, the transaction is committed once, and a failure takes everything back.
   * <p>
   * {@link ArcadeGraph} keeps TinkerPop's AUTO model, where the first write of a traversal opens a transaction nothing ever closes, and
   * the traversal runs while its result is read. Left alone, that is a transaction open in the caller's thread, which the next
   * unrelated {@code commit()} publishes together with the half of a traversal that failed, and a result closed unread is a write that
   * never ran. The model stays for the graph API; the entry point of the query languages, {@code Database.command()}, owns the
   * transaction of the statement instead.
   * <ul>
   * <li>A statement that only reads is untouched: it streams, as before.</li>
   * <li>Inside a transaction of the caller (an explicit {@code begin()}, {@code transaction()}, the HTTP handler) the statement joins
   * it and the caller commits or rolls back, as for every language.</li>
   * <li>A conflict with a concurrent transaction runs the whole statement again, up to {@code arcadedb.txRetries} times. What a script
   * does OUTSIDE the database (a Groovy script calling another system) is repeated by it, as it is by the retry of the HTTP handler and
   * of {@code Database.transaction()}.</li>
   * <li>The rows of a write are held in memory until the commit, as the records it writes are.</li>
   * </ul>
   */
  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    final DatabaseInternal transactionDatabase = databaseToOpenTheTransactionOn(query, parameters);
    if (transactionDatabase == null)
      return execute(query, parameters);

    final InternalResultSet[] answer = new InternalResultSet[1];
    transactionDatabase.transaction(() -> answer[0] = executeToTheEnd(query, parameters), false,
        transactionDatabase.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES));
    return answer[0];
  }

  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Object... parameters) {
    if (parameters.length % 2 != 0)
      throw new IllegalArgumentException("Command parameters must be as pairs `<key>, <value>`");

    final Map<String, Object> map = new HashMap<>(parameters.length / 2);
    for (int i = 0; i < parameters.length; i += 2)
      map.put((String) parameters[i], parameters[i + 1]);
    return command(query, null, map);
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    try {
      final ArcadeGremlin arcadeGremlin = arcadeGraph.gremlin(query);

      return arcadeGremlin.parse();

    } catch (final CommandParsingException e) {
      throw e;
    } catch (final Exception e) {
      throw new CommandParsingException("Error on parsing Gremlin query", e);
    }
  }

  /**
   * The database the statement opens its own transaction on, or null when it runs as it is: the caller holds a transaction, or the
   * statement only reads. It is the instance the database is served through, which under HA is the replicated wrapper: a commit on the
   * raw instance is applied on the leader and never replicated (issues #5492, #5655), see {@code OpenCypherQueryEngine}.
   */
  private DatabaseInternal databaseToOpenTheTransactionOn(final String query, final Map<String, Object> parameters) {
    if (!(arcadeGraph.getDatabase() instanceof DatabaseInternal database))
      return null;

    final DatabaseInternal transactionDatabase = database.getWrappedDatabaseInstance();
    return !transactionDatabase.isTransactionActive() && mayWrite(query, parameters) ? transactionDatabase : null;
  }

  /**
   * Whether the statement may write, read from what it builds without running it (see {@link ArcadeGremlin#parse}): a Groovy script,
   * which only running tells what it does, may. A statement that cannot be analysed is taken for a write too: in a transaction whatever
   * it does is taken back, and the execution then reports the same failure, with the classification it has always given it. The refusal
   * of Groovy to anybody but the server administrator is the one of the execution, which this does not replace.
   */
  private boolean mayWrite(final String query, final Map<String, Object> parameters) {
    try {
      final ArcadeGremlin arcadeGremlin = arcadeGraph.gremlin(query);
      arcadeGremlin.setParameters(parameters);
      return !arcadeGremlin.parse().isIdempotent();
    } catch (final RuntimeException e) {
      return true;
    }
  }

  /**
   * Runs the statement and reads every row of its result, so a traversal runs here, inside the transaction, and not when the caller
   * reads. A retry of the transaction runs it again from scratch, with its own copy of the parameters.
   */
  private InternalResultSet executeToTheEnd(final String query, final Map<String, Object> parameters) {
    final InternalResultSet answer = new InternalResultSet();
    try (final ResultSet result = executeRetryably(query, parameters)) {
      while (result.hasNext())
        answer.add(result.next());
      // $profileExecution: the plan describes the run that was drained, which is over now
      result.getExecutionPlan().ifPresent(answer::setPlan);
    }
    return answer;
  }

  /**
   * {@link #execute}, except that a conflict an eager terminal step ({@code iterate()}, {@code toList()}) reports wrapped in the
   * exception of the execution is given back as the conflict, which is what the retry of the transaction looks for.
   */
  private ResultSet executeRetryably(final String query, final Map<String, Object> parameters) {
    try {
      return execute(query, parameters);
    } catch (final CommandExecutionException e) {
      // BOUNDED: A CAUSE CHAIN CAN LOOP BACK ON ITSELF
      Throwable cause = e.getCause();
      for (int depth = 0; cause != null && depth < 16; depth++, cause = cause.getCause())
        if (cause instanceof NeedRetryException retry)
          throw retry;
      throw e;
    }
  }

  private ResultSet execute(final String query, final Map<String, Object> parameters) {
    try {
      final ArcadeGremlin arcadeGremlin = arcadeGraph.gremlin(query);
      arcadeGremlin.setParameters(parameters);
      return arcadeGremlin.execute();
    } catch (final CommandParsingException | CommandExecutionException e) {
      // Preserve the classification already assigned by ArcadeGremlin.execute(): a genuine parse failure
      // stays a CommandParsingException (HTTP 400), a runtime execution error (e.g. NoSuchElementException
      // from an eager .next() on an empty traversal) stays a CommandExecutionException (HTTP 500). Blindly
      // rewrapping everything as CommandParsingException misreported valid queries as syntax errors.
      // See issues #5201 (parse) and #5219 (runtime error misclassified as parse).
      throw e;
    } catch (final Exception e) {
      throw new CommandExecutionException("Error on executing Gremlin query", e);
    }
  }
}
