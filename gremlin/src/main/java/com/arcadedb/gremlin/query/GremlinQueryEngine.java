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
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
    if (!parse(arcadeGremlin).isIdempotent())
      throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");
    // A statement that only reads: it streams, and has no use for a transaction of its own
    return arcadeGraph.getDatabase() instanceof DatabaseInternal database && !database.isTransactionActive() ?
        executeLeavingNoTransaction(database, query, parameters) :
        execute(query, parameters);
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
   * <li>A statement that only reads still streams. It has no transaction either: a read that goes through the generic graph step
   * ({@code g.V().limit(1)}, {@code g.V(id)}) opens the transaction of the AUTO model while it runs, and that one is ended when the
   * result is exhausted or closed (rolled back, it wrote nothing), so it is not left to the next statement of the thread to join.</li>
   * <li>Inside a transaction of the caller (an explicit {@code begin()}, {@code transaction()}, the HTTP handler) the statement joins
   * it and the caller commits or rolls back, as for every language.</li>
   * <li>A conflict with a concurrent transaction runs the whole statement again, up to {@code arcadedb.txRetries} times. What a script
   * does OUTSIDE the database (a Groovy script calling another system) is repeated by it, as it is by the retry of the HTTP handler and
   * of {@code Database.transaction()}.</li>
   * <li>The rows of a write are held in memory until the commit, as the records it writes are.</li>
   * <li>What a script commits itself ({@code g.tx().commit()}) is committed: the statement owns the transaction it opened, not what a
   * script ends on purpose. The bulk readers of {@code io().read()} do the same, a commit every {@code batchSize} records (10000)
   * through the graph the traversal is bound to, so for a file larger than a batch "a failure takes everything back" holds from the
   * last batch only. A conflict raised after a batch was published is not run again ({@code TransactionContext.isPartiallyCommitted}),
   * so no record is applied twice.</li>
   * </ul>
   */
  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    // A REMOTE DATABASE HAS NO LOCAL TRANSACTION, AND THE TRANSACTION OF THE CALLER IS THE CALLER'S: THE STATEMENT RUNS AS IT IS
    if (!(arcadeGraph.getDatabase() instanceof DatabaseInternal database) || database.isTransactionActive())
      return execute(query, parameters);

    if (!mayWrite(query, parameters))
      return executeLeavingNoTransaction(database, query, parameters);

    // THE INSTANCE THE DATABASE IS SERVED THROUGH, WHICH UNDER HA IS THE REPLICATED WRAPPER: A COMMIT ON THE RAW INSTANCE IS APPLIED
    // ON THE LEADER AND NEVER REPLICATED (ISSUES #5492, #5655), SEE OpenCypherQueryEngine
    final DatabaseInternal transactionDatabase = database.getWrappedDatabaseInstance();
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

      return parse(arcadeGremlin);

    } catch (final CommandParsingException e) {
      throw e;
    } catch (final Exception e) {
      throw new CommandParsingException("Error on parsing Gremlin query", e);
    }
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
      return !parse(arcadeGremlin).isIdempotent();
    } catch (final RuntimeException e) {
      return true;
    }
  }

  /**
   * {@link ArcadeGremlin#parse()} that leaves the transaction state of the thread as it found it. The analysis runs nothing, except a
   * terminated traversal used as an argument ({@code g.V(g.V().limit(1).id().next())}), which runs as the script is built: it reads
   * through the generic graph step, which opens the transaction of the AUTO model and leaves it open. When the thread held no
   * transaction, whatever is open afterwards was opened by the analysis, and it is ended here (rolled back, the analysis never
   * writes): a statement run after it must not nest above it, and the next write of the thread must not join it.
   */
  private AnalyzedQuery parse(final ArcadeGremlin arcadeGremlin) {
    if (!(arcadeGraph.getDatabase() instanceof DatabaseInternal database) || database.isTransactionActive())
      return arcadeGremlin.parse();

    try {
      return arcadeGremlin.parse();
    } finally {
      endTransaction(database);
    }
  }

  /**
   * Runs a statement that only reads, with no transaction open on the thread. The traversal may open the transaction of the AUTO model
   * as it runs (the generic graph step does, {@code g.V().limit(1)}), while the statement is evaluated (an eager terminal step) or
   * as its rows are pulled, and nothing would ever end it: the next statement of the thread would join it, and a write would be left
   * in it. The result ends the transaction a pull opened when it is exhausted or closed, see {@link TransactionEndingResultSet}.
   */
  private ResultSet executeLeavingNoTransaction(final DatabaseInternal database, final String query,
      final Map<String, Object> parameters) {
    final ResultSet result;
    try {
      result = execute(query, parameters);
    } catch (final Throwable e) {
      // THE THREAD HELD NO TRANSACTION WHEN THE STATEMENT BEGAN: WHAT IS OPEN NOW WAS OPENED BY IT
      endTransaction(database);
      throw e;
    }
    return new TransactionEndingResultSet(database, result);
  }

  private static void endTransaction(final DatabaseInternal database) {
    if (database.isTransactionActive())
      database.rollbackAllNested();
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

  /**
   * The rows of a read, which ends the transaction the traversal opened for itself. The thread held none when the statement began, so
   * a transaction that is open after a pull (or after the statement was evaluated) and was not open before it, was opened by the
   * traversal: it belongs to this result, and is rolled back when the rows are exhausted, when a pull fails, or when the result is
   * closed. A transaction the caller begins between two pulls is the caller's: it is open before the pull, so it is never claimed, and
   * a result closed or exhausted on another thread than the one whose transaction it claimed leaves that one alone.
   * <p>
   * While no transaction was claimed, a pull costs two lookups of the transaction of the thread.
   *
   * @author lvca (l.garulli@arcadedata.com)
   */
  private static final class TransactionEndingResultSet implements ResultSet {
    private final DatabaseInternal database;
    private final ResultSet        delegate;
    // THE THREAD WHOSE TRANSACTION THE TRAVERSAL OPENED, NULL WHILE IT OPENED NONE
    private       Thread           owner;

    private TransactionEndingResultSet(final DatabaseInternal database, final ResultSet delegate) {
      this.database = database;
      this.delegate = delegate;
      // AN EAGER TERMINAL STEP RAN THE TRAVERSAL WHILE THE STATEMENT WAS EVALUATED, BEFORE THE FIRST PULL
      claimIfOpened(false);
    }

    @Override
    public boolean hasNext() {
      final boolean openBefore = owner != null || database.isTransactionActive();
      boolean hasNext = false;
      try {
        hasNext = delegate.hasNext();
      } finally {
        claimIfOpened(openBefore);
        if (!hasNext)
          // EXHAUSTED, OR THE PULL FAILED
          end();
      }
      return hasNext;
    }

    @Override
    public Result next() {
      final boolean openBefore = owner != null || database.isTransactionActive();
      boolean failed = true;
      try {
        final Result next = delegate.next();
        failed = false;
        return next;
      } finally {
        claimIfOpened(openBefore);
        if (failed)
          end();
      }
    }

    @Override
    public void close() {
      try {
        delegate.close();
      } finally {
        end();
      }
    }

    @Override
    public Optional<ExecutionPlan> getExecutionPlan() {
      return delegate.getExecutionPlan();
    }

    @Override
    public Optional<QueryStatistics> getStatistics() {
      return delegate.getStatistics();
    }

    private void claimIfOpened(final boolean openBefore) {
      if (!openBefore && owner == null && database.isTransactionActive())
        owner = Thread.currentThread();
    }

    private void end() {
      if (owner == Thread.currentThread()) {
        owner = null;
        endTransaction(database);
      }
    }
  }
}
