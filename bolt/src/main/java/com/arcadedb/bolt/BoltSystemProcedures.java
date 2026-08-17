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
package com.arcadedb.bolt;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.opencypher.procedures.db.DbLabels;
import com.arcadedb.query.opencypher.procedures.db.DbPropertyKeys;
import com.arcadedb.query.opencypher.procedures.db.DbRelationshipTypes;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.stream.Stream;

/**
 * Serves the three schema-introspection procedures Neo4j clients call over Bolt -
 * {@code db.labels()}, {@code db.relationshipTypes()} and {@code db.propertyKeys()} - out of
 * {@link CypherProcedureRegistry}, the same registry entries the native Cypher {@code CALL} path executes.
 * <p>
 * The Bolt executor intercepts these calls instead of running them through the query engine, because the
 * Neo4j tooling that sends them also sends a combined {@code UNION} form the engine does not parse. The
 * interception used to carry its own copy of what each procedure returns, which drifted from the registry
 * versions (issue #6151): relationship types were not filtered for Cypher's composite {@code A~B} label
 * types, and property keys came back sorted rather than in schema order. Everything here now asks the
 * registry, so there is one implementation of each procedure and one place to fix it.
 * </p>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
final class BoltSystemProcedures {
  private static final Object[] NO_ARGS       = new Object[0];
  private static final String   LABELS        = DbLabels.NAME.toLowerCase(Locale.ROOT);
  private static final String   RELATIONSHIPS = DbRelationshipTypes.NAME.toLowerCase(Locale.ROOT);
  private static final String   PROPERTY_KEYS = DbPropertyKeys.NAME.toLowerCase(Locale.ROOT);

  /**
   * The field names and the rows a served query answers with, in the shape the Bolt executor streams them.
   *
   * @param fields the record field names, i.e. the procedure's yield fields
   * @param rows   one entry per record, each holding one value per field
   */
  record Served(List<String> fields, List<List<Object>> rows) {
  }

  private BoltSystemProcedures() {
    // Utility class - prevent instantiation
  }

  /**
   * Normalizes a query for the Bolt executor's system-query matching: trimmed, lowercased and with every
   * whitespace run collapsed to a single space.
   * <p>
   * The lowercasing is {@link Locale#ROOT}-bound on purpose: under a Turkish default locale {@code toLowerCase()}
   * maps {@code I} to {@code ı}, so an upper-case {@code CALL DB.RELATIONSHIPTYPES()} would stop matching.
   * </p>
   *
   * @param query the raw query text
   *
   * @return the normalized form used by every {@code contains}/{@code startsWith} check
   */
  static String normalize(final String query) {
    return query.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
  }

  /**
   * Answers whether the normalized query mentions any of the three schema procedures.
   *
   * @param normalized a query normalized by {@link #normalize(String)}
   *
   * @return true if the Bolt executor should try to serve it here
   */
  static boolean isSchemaProcedureQuery(final String normalized) {
    return normalized.contains(LABELS) || normalized.contains(RELATIONSHIPS) || normalized.contains(PROPERTY_KEYS);
  }

  /**
   * Serves a schema-procedure query out of the registry.
   * <p>
   * Recognizes the single-procedure calls and the combined form Neo4j Desktop sends, which unions the three
   * procedures and collects each into a list under a single {@code result} field.
   * </p>
   *
   * @param database   the database the connection is bound to, may be null when none is selected yet
   * @param normalized a query normalized by {@link #normalize(String)}
   *
   * @return the records to stream, or null when the query must be left to the Cypher engine - which happens
   * when the call carries arguments (the registry rejects those with the same error the native {@code CALL}
   * path reports), when the procedure is not registered, and when running it raises anything at all
   */
  static Served serveSchemaProcedure(final Database database, final String normalized) {
    final boolean labels = normalized.contains(LABELS);
    final boolean relationships = normalized.contains(RELATIONSHIPS);
    final boolean propertyKeys = normalized.contains(PROPERTY_KEYS);

    try {
      if (labels && relationships && propertyKeys)
        return serveCombined(database, normalized);

      if (labels)
        return serveOne(database, normalized, LABELS);
      if (relationships)
        return serveOne(database, normalized, RELATIONSHIPS);
      if (propertyKeys)
        return serveOne(database, normalized, PROPERTY_KEYS);
    } catch (final Exception e) {
      // The Bolt executor calls its system-query interception before the try/catch that classifies query
      // errors (CommandParsingException vs. retryable conflict vs. plain failure), so an exception escaping
      // here would reach the connection loop's catch-all unclassified. Running a registry procedure is a
      // wider surface than the direct schema iteration this branch used to do, so it declines instead: all
      // three procedures are read-only, which makes re-running the query through the engine free of side
      // effects and gets the client the engine's own, properly classified error.
      LogManager.instance().log(BoltSystemProcedures.class, Level.FINE,
          "Error serving schema procedure from the registry, leaving the query to the engine", e);
      return null;
    }

    // Defensive: the caller reaches here only through isSchemaProcedureQuery(), which tests the same three
    // substrings, so no name can be missing. Kept so the two ever diverging declines the query rather than
    // answering it with nothing.
    return null;
  }

  /**
   * Serves the combined query Neo4j Desktop sends, one row per procedure, each row holding the list of that
   * procedure's values.
   */
  private static Served serveCombined(final Database database, final String normalized) {
    final CypherProcedure labels = procedureFor(normalized, LABELS);
    final CypherProcedure relationships = procedureFor(normalized, RELATIONSHIPS);
    final CypherProcedure propertyKeys = procedureFor(normalized, PROPERTY_KEYS);
    if (labels == null || relationships == null || propertyKeys == null)
      return null;

    final List<List<Object>> rows = new ArrayList<>(3);
    if (database != null) {
      rows.add(List.of(column(database, labels)));
      rows.add(List.of(column(database, relationships)));
      rows.add(List.of(column(database, propertyKeys)));
    }
    return new Served(List.of("result"), rows);
  }

  /**
   * Serves a single procedure call, one record per row the procedure yields.
   */
  private static Served serveOne(final Database database, final String normalized, final String procedureName) {
    final CypherProcedure procedure = procedureFor(normalized, procedureName);
    if (procedure == null)
      return null;

    final List<String> fields = procedure.getYieldFields();
    final List<List<Object>> rows = new ArrayList<>();
    if (database != null) {
      try (final Stream<Result> results = execute(database, procedure)) {
        results.forEach(result -> {
          final List<Object> row = new ArrayList<>(fields.size());
          for (final String field : fields)
            row.add(result.getProperty(field));
          rows.add(row);
        });
      }
    }
    return new Served(fields, rows);
  }

  /**
   * Looks the procedure up, refusing any call that carries arguments so that the query engine gets it.
   * <p>
   * The refusal is not a statement about these procedures' arity - it is that this path only has the query
   * TEXT and cannot evaluate an argument, so whatever a call passes has to be interpreted where arguments
   * are evaluated. Today all three declare zero arguments and the engine answers with the registry's arity
   * error; a procedure that later grew an optional argument would be executed there with it, which is the
   * same outcome, reached the same way.
   * </p>
   */
  private static CypherProcedure procedureFor(final String normalized, final String procedureName) {
    if (callHasArguments(normalized, procedureName))
      return null;
    return CypherProcedureRegistry.get(procedureName);
  }

  /**
   * Reads every value of a single-field procedure into one list, for the combined query's collected form.
   */
  private static List<Object> column(final Database database, final CypherProcedure procedure) {
    final String field = procedure.getYieldFields().getFirst();
    final List<Object> values = new ArrayList<>();
    try (final Stream<Result> results = execute(database, procedure)) {
      results.forEach(result -> values.add(result.getProperty(field)));
    }
    return values;
  }

  private static Stream<Result> execute(final Database database, final CypherProcedure procedure) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return procedure.execute(NO_ARGS, null, context);
  }

  /**
   * Detects a call to the named procedure that passes at least one argument, e.g. {@code CALL db.labels('x')}.
   * <p>
   * The Bolt interception matches procedure names by substring and cannot evaluate arguments, so a call that
   * carries any is handed to the Cypher engine rather than answered here.
   * </p>
   * <p>
   * This is not a Cypher tokenizer and must not be mistaken for one: it walks to the first {@code )} after the
   * name, so a nested paren or a stray {@code )} later in the query reads as an argument list. Every error it
   * can make is in the same direction - it declines a call it could have served, and the engine answers it -
   * which is why the crude scan is enough for the fixed procedure names this interception covers.
   * </p>
   *
   * @param normalized    a query normalized by {@link #normalize(String)}
   * @param procedureName the lower-case procedure name to look for
   *
   * @return true if the name is followed by a non-empty argument list
   */
  static boolean callHasArguments(final String normalized, final String procedureName) {
    final int length = normalized.length();
    for (int found = normalized.indexOf(procedureName); found >= 0;
        found = normalized.indexOf(procedureName, found + 1)) {
      int pos = found + procedureName.length();
      while (pos < length && normalized.charAt(pos) == ' ')
        ++pos;
      if (pos >= length || normalized.charAt(pos) != '(')
        continue;
      final int close = normalized.indexOf(')', pos);
      final String args = close < 0 ? normalized.substring(pos + 1) : normalized.substring(pos + 1, close);
      if (!args.isBlank())
        return true;
    }
    return false;
  }
}
