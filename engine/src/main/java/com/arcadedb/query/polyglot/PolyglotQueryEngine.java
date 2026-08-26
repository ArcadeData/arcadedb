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
package com.arcadedb.query.polyglot;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.polyglot.JavascriptFunctionDefinition;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;
import org.graalvm.polyglot.Value;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class PolyglotQueryEngine implements QueryEngine {
  /**
   * The shared script context. Volatile and guarded by {@link #engineLock} rather than being its own monitor:
   * {@link #unregisterFunctions()} replaces it, and {@code synchronized} on a field that can be reassigned lets two
   * commands run concurrently on two different monitors over what is meant to be a serialised context (issue #6759).
   */
  private volatile GraalPolyglotEngine       polyglotEngine;
  /** Serialises every access to {@link #polyglotEngine}; stable for the life of this engine. */
  private final Object                       engineLock      = new Object();
  private final String                       language;
  private final long                         timeout;
  private final DatabaseInternal             database;
  private       List<String>                 allowedPackages = null;
  private final ExecutorService              userCodeExecutor;
  private final ArrayBlockingQueue<Runnable> userCodeExecutorQueue;

  /** #5418: names the user-code workers and marks them DAEMON (see the constructor). */
  private static final AtomicLong USER_CODE_THREAD_SEQ = new AtomicLong();

  /** How long {@link #cancelRunningScript()} waits for a timed-out script to unwind before reporting it wedged. */
  private static final long INTERRUPT_GRACE_MS = 5_000;

  private static final AnalyzedQuery ANALYZED_QUERY = new AnalyzedQuery() {
    @Override
    public boolean isIdempotent() {
      return false;
    }

    @Override
    public boolean isDDL() {
      return false;
    }
  };

  public static class PolyglotQueryEngineFactory implements QueryEngineFactory {
    private final String       language;
    private       List<String> allowedPackages = null;

    public PolyglotQueryEngineFactory(final String language) {
      this.language = language;
    }

    public PolyglotQueryEngineFactory setAllowedPackages(final List<String> allowedPackages) {
      this.allowedPackages = allowedPackages;
      return this;
    }

    @Override
    public String getLanguage() {
      return language;
    }

    @Override
    public QueryEngine getInstance(final DatabaseInternal database) {
      return new PolyglotQueryEngine(database, language, allowedPackages);
    }

    public static Iterable<String> getSupportedLanguages() {
      return GraalPolyglotEngine.getSupportedLanguages();
    }
  }

  protected PolyglotQueryEngine(final DatabaseInternal database, final String language, final List<String> allowedPackages) {
    this.language = language;
    this.database = database;
    this.allowedPackages = allowedPackages;
    this.polyglotEngine = GraalPolyglotEngine.newBuilder(database, PolyglotEngineManager.getInstance().getSharedEngine())
        .setLanguage(language).setAllowedPackages(allowedPackages).build();
    this.userCodeExecutorQueue = new ArrayBlockingQueue<>(10000);
    // #5418: named DAEMON workers. With the JDK default factory these were non-daemon core threads that never
    // time out, so a single scripted query on a Database the embedder later leaks pinned its JVM alive.
    this.userCodeExecutor = new ThreadPoolExecutor(8, 8, 30, TimeUnit.SECONDS, userCodeExecutorQueue, r -> {
      final Thread t = new Thread(r, "ArcadeDB-PolyglotUserCode-" + USER_CODE_THREAD_SEQ.incrementAndGet());
      t.setDaemon(true);
      return t;
    }, new ThreadPoolExecutor.CallerRunsPolicy());
    this.timeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.POLYGLOT_COMMAND_TIMEOUT);
  }

  @Override
  public String getLanguage() {
    return language;
  }

  /**
   * Executing a polyglot script grants the caller arbitrary in-JVM capabilities (host method access and file I/O reachable
   * through the bound {@code database} object), so it must be gated behind database-administrator privileges. Without this
   * check any authenticated user - including a read-only one - could escalate through the scripting engine and read host
   * files outside the database scope (GHSA-48qw-824m-86pr). The check runs on the calling thread (which carries the bound
   * {@link SecurityDatabaseUser}) and is a no-op in embedded mode and in internal/system contexts where no user is bound.
   */
  private void checkScriptingPermissions() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SECURITY);
  }

  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Object... parameters) {
    if (parameters == null || parameters.length == 0)
      return command(query, configuration, (Map) null);
    throw new UnsupportedOperationException(
        "Execution of a command with positional parameter is not supported for polyglot engine");
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    checkScriptingPermissions();
    // Which context THIS invocation is evaluating in, or null while it is still queued behind another one. Read by
    // the timeout path so it never cancels a context its own task never entered (PR #6783 review).
    final AtomicReference<GraalPolyglotEngine> running = new AtomicReference<>();
    try {
      return executeUserCode(() -> {

        synchronized (engineLock) {
          final GraalPolyglotEngine engine = polyglotEngine;
          running.set(engine);
          // Parameters are scoped to THIS evaluation: the context is shared by every caller on the database, so a
          // parameter left bound would be readable by the next command, from any caller (issue #6759).
          final Map<String, Value> displaced =
              parameters == null || parameters.isEmpty() ? null : engine.setAttributes(parameters);
          try {
            final Value result = engine.eval(query);

            if (result.isHostObject()) {
              final Object host = result.asHostObject();
              if (host instanceof ResultSet)
                return host;

              final InternalResultSet resultSet = new InternalResultSet();
              if (host instanceof Iterable iterable) {
                for (final Object o : iterable)
                  resultSet.add(extractResult(o));
              } else
                resultSet.add(extractResult(host));

              return resultSet;

            }

            final InternalResultSet resultSet = new InternalResultSet();

            final Object value = JavascriptFunctionDefinition.jsValueToJava(result);

            resultSet.add(new ResultInternal(database).setProperty("value", value));
            return resultSet;
          } finally {
            if (displaced != null)
              engine.restoreAttributes(displaced);
            running.compareAndSet(engine, null);
          }
        }

      }, running, timeout);

    } catch (final CommandExecutionException e) {
      throw e;
    } catch (final ExecutionException e) {
      // USE THE UNDERLYING CAUSE BYPASSING THE NOT RELEVANT EXECUTION EXCEPTION
      throw new CommandExecutionException("Error on executing user code", e.getCause());
    } catch (final Exception e) {
      throw new CommandExecutionException("Error on executing user code", e);
    }
  }

  @Override
  public QueryEngine registerFunctions(final String function) {
    checkScriptingPermissions();
    synchronized (engineLock) {
      try {
        polyglotEngine.eval(function);
      } catch (final CommandExecutionException e) {
        throw e;
      } catch (final Exception e) {
        throw new CommandExecutionException("Error on executing user code", e);
      }
    }
    return this;
  }

  @Override
  public QueryEngine unregisterFunctions() {
    synchronized (engineLock) {
      final GraalPolyglotEngine previous = this.polyglotEngine;
      this.polyglotEngine = GraalPolyglotEngine.newBuilder(database, PolyglotEngineManager.getInstance().getSharedEngine())
          .setLanguage(language).setAllowedPackages(allowedPackages).build();
      // The replaced context owns native GraalVM state; dropping the reference without closing it leaks it for the
      // life of the JVM.
      if (previous != null)
        previous.close();
    }
    return this;
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    // analyze() evaluates the script to determine its characteristics, so it is gated identically to command().
    checkScriptingPermissions();
    final AtomicReference<GraalPolyglotEngine> running = new AtomicReference<>();
    try {
      executeUserCode(() -> {
        synchronized (engineLock) {
          final GraalPolyglotEngine engine = polyglotEngine;
          running.set(engine);
          try {
            engine.eval(query);
          } finally {
            running.compareAndSet(engine, null);
          }
        }
        return null;
      }, running, timeout);
    } catch (final CommandExecutionException e) {
      throw e;
    } catch (final ExecutionException e) {
      // USE THE UNDERLYING CAUSE BYPASSING THE NOT RELEVANT EXECUTION EXCEPTION
      throw new CommandExecutionException("Error on executing user code", e.getCause());
    } catch (final Exception e) {
      throw new CommandExecutionException("Error on analyzing user code", e);
    }

    return ANALYZED_QUERY;
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Map<String, Object> parameters) {
    throw new UnsupportedOperationException(
        "Execution of a query (idempotent) is not supported for polyglot engine. Use command instead");
  }

  @Override
  public ResultSet query(final String query, ContextConfiguration configuration, final Object... parameters) {
    throw new UnsupportedOperationException(
        "Execution of a query (idempotent) is not supported for polyglot engine. Use command instead");
  }

  @Override
  public void close() {
    userCodeExecutor.shutdown();
    userCodeExecutorQueue.clear();
    polyglotEngine.close();
  }

  private ResultSet executeUserCode(final Callable task, final AtomicReference<GraalPolyglotEngine> running,
      final long executionTimeoutMs) throws Exception {
    // IF NOT INITIALIZED, EXECUTE AS SOON AS THE SERVICE STARTS
    final Future future = userCodeExecutor.submit(task);
    if (future == null)
      return null;

    try {
      final Object result = executionTimeoutMs > 0 ? future.get(executionTimeoutMs, TimeUnit.MILLISECONDS) : future.get();
      if (result instanceof Exception exception)
        throw exception;

      return (ResultSet) result;

    } catch (final TimeoutException e) {
      // cancel(true) interrupts the HOST thread, which a guest loop is free to ignore. It returns false when the
      // task had in fact just completed - in which case there is nothing left in the context to cancel, and
      // interrupting would only risk hitting whichever command acquired the lock next.
      if (future.cancel(true))
        cancelRunningScript(running.get());
      throw e;
    }
  }

  /**
   * Cancels whatever the timed-out script left running inside the shared context.
   * <p>
   * {@code Future.cancel(true)} only sets the host thread's interrupt flag. A guest program is stopped by GraalVM's
   * own cancellation ({@code Context.interrupt}), and it has to be stopped, because the task holds
   * {@link #engineLock} - the monitor every command on this database serialises on - until it unwinds. A script that
   * outlives its timeout would otherwise block the engine for every caller (issue #6759).
   */
  private void cancelRunningScript(final GraalPolyglotEngine engine) {
    // null means this invocation was still QUEUED behind another one when it timed out: its own script never
    // entered the context, so there is nothing of ours to cancel - and interrupting anyway would abort whichever
    // OTHER caller's script is currently holding the lock (PR #6783 review). Reading the recorded instance rather
    // than the field also means a context unregisterFunctions() replaced in the meantime is left alone.
    if (engine == null)
      return;
    try {
      if (!engine.interrupt(Duration.ofMillis(INTERRUPT_GRACE_MS)))
        LogManager.instance().log(this, Level.WARNING,
            "Timed-out %s script did not unwind within %dms: it is blocked in a host call that cannot be "
                + "interrupted and still holds the shared script context of database '%s'",
            language, INTERRUPT_GRACE_MS, database.getName());
    } catch (final Exception ex) {
      LogManager.instance().log(this, Level.WARNING, "Error while cancelling a timed-out %s script", ex, language);
    }
  }

  private ResultInternal extractResult(final Object o) {
    if (o instanceof Document document)
      return new ResultInternal(document);
    else if (o instanceof Identifiable identifiable)
      return new ResultInternal(identifiable);
    else if (o instanceof Map map)
      return new ResultInternal(map);

    return new ResultInternal(database).setProperty("value", o);
  }

}
