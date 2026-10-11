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
package com.arcadedb.postgres;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.CommandContext;

import java.math.BigDecimal;
import java.util.Set;

/**
 * PostgreSQL's backend signalling functions (issue #9689), so a client stops another connection's statement the way it
 * would on PostgreSQL:
 * <ul>
 * <li>{@code pg_backend_pid()}: the process id of the calling connection, the one its BackendKeyData carried, also from
 * work of its statement that runs on another thread;</li>
 * <li>{@code pg_cancel_backend(pid)}: stops the statement that connection is running, which fails with
 * {@code 57014 query_canceled} - what the connection's own {@code CancelRequest} does;</li>
 * <li>{@code pg_terminate_backend(pid)}: stops its statement and closes the connection, rolling back its transaction.</li>
 * </ul>
 * Both signalling functions answer {@code true} once the signal is sent, and {@code false} for a process id that names no
 * connection the caller may signal: as everywhere a running statement is listed or stopped, the server administrator may
 * signal every connection and any other user only their own, and somebody else's connection is not told apart from one
 * that does not exist. Only connections of the caller's own server can be signalled, when a JVM runs several. The rule
 * is the server's registry's own ({@code RunningQueryRegistry.isVisible}), the one {@code list queries} and
 * {@code SHOW TRANSACTIONS} apply, so the surfaces cannot disagree.
 * <p>
 * Registered in the JVM-wide SQL function factory once the Postgres plugin starts, so they are also callable from SQL sent
 * over any other protocol: there {@code pg_backend_pid()} answers {@code null} (the caller is not a Postgres connection)
 * and the signalling functions still reach Postgres connections. They stay registered after the plugin stops, where they
 * find no connection and answer {@code false}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PostgresBackendFunctions {
  private PostgresBackendFunctions() {
  }

  /** Registers the functions once per JVM: the SQL function factory is shared by every server. */
  static void register() {
    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    synchronized (PostgresBackendFunctions.class) {
      // One by one: whatever happened to an earlier registration, each function ends up registered exactly once
      final Set<String> registered = factory.getFunctionNames();
      if (!registered.contains(BackendPid.NAME))
        factory.register(new BackendPid());
      if (!registered.contains(CancelBackend.NAME))
        factory.register(new CancelBackend());
      if (!registered.contains(TerminateBackend.NAME))
        factory.register(new TerminateBackend());
    }
  }

  /** {@code pg_backend_pid()}: the calling connection's process id, or null outside a Postgres connection. */
  static final class BackendPid extends SQLFunctionAbstract {
    static final String NAME = "pg_backend_pid";

    BackendPid() {
      super(NAME);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      if (Thread.currentThread() instanceof PostgresNetworkExecutor executor)
        return executor.getProcessId();
      // Work of a Postgres statement that runs on another thread (a parallel step, a script on the polyglot pool) carries
      // the statement's entry, and the entry the connection's process id
      final RunningQuery statement = context != null ? context.getRunningQuery() : RunningQuery.current();
      if (statement != null && "postgres".equals(statement.getProtocol()) && statement.getConnectionId() != null)
        try {
          return Integer.parseInt(statement.getConnectionId());
        } catch (final NumberFormatException e) {
          return null;
        }
      return null;
    }

    @Override
    public int getMaxArgs() {
      return 0;
    }

    @Override
    public String getSyntax() {
      return "pg_backend_pid()";
    }
  }

  /** {@code pg_cancel_backend(pid)}. */
  static final class CancelBackend extends SQLFunctionAbstract {
    static final String NAME = "pg_cancel_backend";

    CancelBackend() {
      super(NAME);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      if (params == null || params.length != 1 || params[0] == null)
        // NULL in, NULL out: as PostgreSQL answers a strict function called with NULL
        return null;
      final String caller = caller(context);
      final PostgresNetworkExecutor backend = signalled(params, caller, context);
      if (backend == null)
        return false;
      backend.cancelRunningStatement(caller);
      return true;
    }

    @Override
    public int getMinArgs() {
      return 1;
    }

    @Override
    public int getMaxArgs() {
      return 1;
    }

    @Override
    public String getSyntax() {
      return "pg_cancel_backend(<pid>)";
    }
  }

  /** {@code pg_terminate_backend(pid)}. */
  static final class TerminateBackend extends SQLFunctionAbstract {
    static final String NAME = "pg_terminate_backend";

    TerminateBackend() {
      super(NAME);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      if (params == null || params.length != 1 || params[0] == null)
        // NULL in, NULL out: as PostgreSQL answers a strict function called with NULL
        return null;
      final String caller = caller(context);
      final PostgresNetworkExecutor backend = signalled(params, caller, context);
      if (backend == null)
        return false;
      // The statement first, so its work stops at its next check rather than running on against a closed socket
      backend.cancelRunningStatement(caller);
      // Ends the connection's thread, which rolls back what its transaction left open. Safe from this thread: close()
      // only raises the shutdown flag and closes the socket, and the connection's own thread - blocked reading, or
      // writing - fails out of its I/O and runs its cleanup (the rollback included) itself
      backend.close();
      return true;
    }

    @Override
    public int getMinArgs() {
      return 1;
    }

    @Override
    public int getMaxArgs() {
      return 1;
    }

    @Override
    public String getSyntax() {
      return "pg_terminate_backend(<pid>)";
    }
  }

  private static boolean isIntegral(final Number number) {
    if (number instanceof Double || number instanceof Float) {
      final double value = number.doubleValue();
      return !Double.isInfinite(value) && value == Math.rint(value);
    }
    if (number instanceof BigDecimal decimal)
      return decimal.signum() == 0 || decimal.stripTrailingZeros().scale() <= 0;
    return true;
  }

  private static String caller(final CommandContext context) {
    return context != null && context.getDatabase() != null ? context.getDatabase().getCurrentUserName() : null;
  }

  /** The connection {@code params} names, if the caller may signal it; otherwise {@code null}. */
  private static PostgresNetworkExecutor signalled(final Object[] params, final String caller, final CommandContext context) {
    if (params == null || params.length != 1 || params[0] == null)
      return null;
    if (!(params[0] instanceof Number pid) || !isIntegral(pid))
      // A fractional pid is not rounded to some other connection's: refused, as PostgreSQL refuses pg_cancel_backend(1.5)
      throw new CommandExecutionException("The process id must be an integer, found: " + params[0]);
    final long value = pid.longValue();
    // Process ids are positive, as on the wire a CancelRequest quotes them
    if (value <= 0 || value > Integer.MAX_VALUE)
      return null;
    final PostgresNetworkExecutor backend = PostgresNetworkExecutor.getBackend((int) value);
    if (backend == null || caller == null)
      return null;
    // Only a connection of the caller's own server: process ids are unique in the JVM, but a JVM may run several servers,
    // each with its own users, and the caller's server is the one whose registry its own statement is in
    final RunningQuery callerStatement = context != null ? context.getRunningQuery() : RunningQuery.current();
    final RunningQueryRegistry registry = backend.getServer().getRunningQueries();
    if (callerStatement == null || callerStatement.getRegistry() != registry)
      return null;
    return registry.isVisible(caller, backend.getUserName()) ? backend : null;
  }
}
