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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.opencypher.ast.CypherAdminStatement;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@code SHOW TRANSACTIONS} and {@code TERMINATE TRANSACTIONS} (issue #9689): the Neo4j shape of the server's
 * {@code list queries} / {@code terminate query} commands, over the same registry of running statements and with the
 * same visibility rule - the server administrator sees and stops every statement, any other user only their own. So a
 * Bolt client, or a tool written for Neo4j, finds and stops a statement without knowing ArcadeDB's HTTP API.
 * <p>
 * A transaction here is a running statement: its id is the statement's id in {@code list queries}, so either surface
 * stops what the other lists. The columns are Neo4j's, by the same names and types; {@code SHOW TRANSACTIONS} returns the
 * default ones, and its {@code YIELD} reaches the others ({@code protocol}, {@code language}, {@code metaData} with the
 * client's tag, {@code sessionId}, {@code statusDetails}). As in Neo4j, a terminate does not wait for the statement to
 * stop: it stops at its next check, and {@code SHOW TRANSACTIONS} lists it until it has.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CypherTransactionsCommand {
  static final List<String> SHOW_DEFAULT_COLUMNS   = List.of("database", "transactionId", "currentQueryId", "connectionId",
      "clientAddress", "username", "currentQuery", "startTime", "status", "elapsedTime");
  static final List<String> SHOW_ALL_COLUMNS       = List.of("database", "transactionId", "currentQueryId", "connectionId",
      "clientAddress", "username", "currentQuery", "startTime", "status", "elapsedTime", "protocol", "language", "metaData",
      "sessionId", "statusDetails");
  static final List<String> TERMINATE_COLUMNS      = List.of("transactionId", "username", "message");
  static final String       TERMINATED_MESSAGE     = "Transaction terminated.";
  static final String       NOT_FOUND_MESSAGE      = "Transaction not found.";

  private final DatabaseInternal     database;
  private final ExpressionEvaluator  evaluator;
  private final String               query;
  private final Map<String, Object>  parameters;
  private final RunningQueryRegistry registry;
  private final String               user;

  CypherTransactionsCommand(final DatabaseInternal database, final ExpressionEvaluator evaluator, final String query,
      final Map<String, Object> parameters) {
    this.database = database;
    this.evaluator = evaluator;
    this.query = query;
    this.parameters = parameters;

    // The registry is the server's, reached through the entry of the statement running this command: every protocol
    // registers its statements there, and an embedded database has no server to list the statements of
    final RunningQuery current = RunningQuery.current();
    if (current == null)
      throw new CommandExecutionException("SHOW TRANSACTIONS and TERMINATE TRANSACTIONS require a server");
    this.registry = current.getRegistry();
    final String databaseUser = database.getCurrentUserName();
    this.user = databaseUser != null ? databaseUser : current.getUser();
  }

  ResultSet execute(final CypherAdminStatement statement) {
    final InternalResultSet resultSet = new InternalResultSet();
    final ShowCommandTail.Table table;
    if (statement.getKind() == CypherAdminStatement.Kind.SHOW_TRANSACTIONS) {
      final CypherAdminStatement terminate = statement.getComposedTerminate();
      if (terminate == null)
        table = show(statement, query);
      else {
        // SHOW ... TERMINATE ...: each part has its own tail, and the TERMINATE stops what the SHOW's rows name
        final int terminateAt = ShowCommandTail.composedCommandStart(query);
        if (terminateAt < 0)
          throw new CommandExecutionException("Cannot locate TERMINATE TRANSACTIONS in: " + query);
        final ShowCommandTail.Table shown = show(statement, query.substring(0, terminateAt));
        final Set<String> ids = new LinkedHashSet<>();
        for (final List<Object> row : shown.rows()) {
          final ResultInternal bound = new ResultInternal(database);
          for (int i = 0; i < shown.fields().size(); i++)
            bound.setProperty(shown.fields().get(i), i < row.size() ? row.get(i) : null);
          collectIds(terminate, bound, ids);
        }
        table = terminate(ids, query.substring(terminateAt));
      }
    } else {
      final Set<String> ids = new LinkedHashSet<>();
      collectIds(statement, new ResultInternal(database), ids);
      table = terminate(ids, query);
    }

    for (final List<Object> row : table.rows()) {
      final ResultInternal result = new ResultInternal();
      for (int i = 0; i < table.fields().size(); i++)
        result.setProperty(table.fields().get(i), i < row.size() ? row.get(i) : null);
      resultSet.add(result);
    }
    return resultSet;
  }

  private ShowCommandTail.Table show(final CypherAdminStatement statement, final String showText) {
    Set<String> only = null;
    if (statement.getTransactionIds() != null || statement.getTransactionIdsExpression() != null) {
      only = new LinkedHashSet<>();
      collectIds(statement, new ResultInternal(database), only);
    }

    // A bare SHOW answers Neo4j's default columns; YIELD reaches every column
    final boolean hasTail = ShowCommandTail.hasTail(showText);
    final List<String> columns = hasTail ? SHOW_ALL_COLUMNS : SHOW_DEFAULT_COLUMNS;
    final List<List<Object>> rows = new ArrayList<>();
    for (final RunningQuery running : registry.getRunning())
      if (registry.isVisible(user, running) && (only == null || only.contains(running.getId())))
        rows.add(hasTail ? allColumns(running) : allColumns(running).subList(0, SHOW_DEFAULT_COLUMNS.size()));
    return ShowCommandTail.apply(database, showText, columns, rows, parameters);
  }

  private static List<Object> allColumns(final RunningQuery running) {
    final long elapsedMs = running.getElapsedMillis();
    final String terminatedBy = running.getTerminatedBy();
    final Map<String, Object> metaData = running.getTag() != null ? Map.of("tag", running.getTag()) : Map.of();
    return Arrays.asList(//
        running.getDatabase(),//
        running.getId(),//
        running.getId(),//
        running.getConnectionId() != null ? running.getConnectionId() : "",//
        running.getClientAddress() != null ? running.getClientAddress() : "",//
        running.getUser(),//
        running.getText() != null ? running.getText() : "",//
        Instant.ofEpochMilli(running.getStartedAt()).toString(),//
        terminatedBy == null ? "Running" : "Terminating",//
        new CypherDuration(0, 0, elapsedMs / 1000, (elapsedMs % 1000) * 1_000_000L),//
        running.getProtocol(),//
        running.getLanguage(),//
        metaData,//
        running.getSessionId(),//
        terminatedBy == null ? null : "Terminated by " + terminatedBy);
  }

  private ShowCommandTail.Table terminate(final Set<String> ids, final String terminateText) {
    final List<List<Object>> rows = new ArrayList<>(ids.size());
    for (final String id : ids) {
      final RunningQuery running = registry.get(id);
      // Another user's statement is "not found", as one that does not exist: its existence is not given away
      if (running != null && registry.isVisible(user, running)) {
        running.terminate(user);
        rows.add(Arrays.asList(id, running.getUser(), TERMINATED_MESSAGE));
      } else
        rows.add(Arrays.asList(id, user, NOT_FOUND_MESSAGE));
    }
    return ShowCommandTail.apply(database, terminateText, TERMINATE_COLUMNS, rows, parameters);
  }

  /** The ids a SHOW or TERMINATE names: its string literals, or what its expression yields against {@code row}. */
  private void collectIds(final CypherAdminStatement statement, final ResultInternal row, final Set<String> ids) {
    if (statement.getTransactionIds() != null) {
      ids.addAll(statement.getTransactionIds());
      return;
    }
    if (statement.getTransactionIdsExpression() == null)
      throw new CommandExecutionException("TERMINATE TRANSACTIONS needs the ids of the transactions to terminate");

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    addIds(evaluator.evaluate(statement.getTransactionIdsExpression(), row, context), ids);
  }

  private static void addIds(final Object value, final Set<String> ids) {
    if (value == null)
      return;
    if (value instanceof String id)
      ids.add(id);
    else if (value instanceof Collection<?> collection)
      for (final Object element : collection)
        addIds(element, ids);
    else if (value instanceof Object[] array)
      for (final Object element : array)
        addIds(element, ids);
    else
      throw new CommandExecutionException("Transaction ids must be strings, found: " + value.getClass().getSimpleName());
  }
}
