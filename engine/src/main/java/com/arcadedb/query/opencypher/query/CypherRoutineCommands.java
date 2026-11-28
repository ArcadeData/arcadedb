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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.function.CypherBuiltinFunctions;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.ast.CypherShowRoutinesStatement;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * {@code SHOW PROCEDURES} and {@code SHOW FUNCTIONS} answered from what is registered (issue #13541): {@link CypherProcedureRegistry}
 * for the procedures, and for the functions the hard-coded Cypher ones ({@link CypherBuiltinFunctions}) and
 * {@link CypherFunctionRegistry}. Every transport answers the same, and a client that asks the server whether a procedure exists
 * ({@code SHOW PROCEDURES YIELD name WHERE name = 'db.labels'}) is told what is there.
 * <p>
 * The columns are Neo4j's, in its order: a bare {@code SHOW} answers the default ones and a {@code YIELD} reaches all of them. What
 * the registries do not know is {@code null} rather than made up: the category of a function, the declared type of an argument.
 * A name the engine resolves under the {@code apoc.} prefix is listed under that name too, since that is how a client that probes
 * for APOC asks.
 */
public final class CypherRoutineCommands {
  static final List<String> PROCEDURE_DEFAULT_COLUMNS = List.of("name", "description", "mode", "worksOnSystem");
  static final List<String> PROCEDURE_ALL_COLUMNS     = List.of("name", "description", "mode", "worksOnSystem", "signature",
      "argumentDescription", "returnDescription", "admin", "rolesExecution", "rolesBoostedExecution", "isDeprecated", "deprecatedBy",
      "option");
  static final List<String> FUNCTION_DEFAULT_COLUMNS  = List.of("name", "category", "description");
  static final List<String> FUNCTION_ALL_COLUMNS      = List.of("name", "category", "description", "signature", "isBuiltIn",
      "argumentDescription", "returnDescription", "aggregating", "rolesExecution", "rolesBoostedExecution", "isDeprecated",
      "deprecatedBy");

  private static final String APOC_PREFIX = "apoc.";
  // The functions of the Cypher language that the engine runs through the SQL bridge, so CypherBuiltinFunctions does not list them
  private static final List<String> BRIDGED_AGGREGATES = List.of("count", "sum", "stDev", "stDevP");
  // Neo4j's spelling of the functions it spells in camel case. The engine matches names lower-cased, so only these need a table
  private static final Map<String, String> CANONICAL_NAMES = canonicalNames("duration.inDays", "duration.inMonths",
      "duration.inSeconds", "elementId", "endNode", "isEmpty", "isNaN", "nullIf", "percentileCont", "percentileDisc",
      "point.withinBBox", "randomUUID", "startNode", "toBoolean", "toBooleanList", "toBooleanOrNull", "toFloat", "toFloatList",
      "toFloatOrNull", "toInteger", "toIntegerList", "toIntegerOrNull", "toLower", "toString", "toStringList", "toStringOrNull",
      "toUpper", "valueType", "stDev", "stDevP");
  private static final Set<String> AGGREGATING_BUILT_INS = Set.of("avg", "collect", "collect_list", "count", "max", "min",
      "percentilecont", "percentile_cont", "percentiledisc", "percentile_disc", "stdev", "stdevp", "sum");

  private CypherRoutineCommands() {
  }

  static ResultSet execute(final DatabaseInternal database, final CypherShowRoutinesStatement statement, final String query,
      final Map<String, Object> parameters) {
    final ShowCommandTail.Table untailed = table(statement, ShowCommandTail.hasTail(query));
    final ShowCommandTail.Table table = ShowCommandTail.apply(database, query, untailed.fields(), untailed.rows(), parameters);
    final InternalResultSet resultSet = new InternalResultSet();
    for (final List<Object> row : table.rows()) {
      final ResultInternal result = new ResultInternal();
      for (int i = 0; i < table.fields().size(); i++)
        result.setProperty(table.fields().get(i), i < row.size() ? row.get(i) : null);
      resultSet.add(result);
    }
    return resultSet;
  }

  /**
   * The table a {@code SHOW PROCEDURES} / {@code SHOW FUNCTIONS} command produces before its {@code YIELD}/{@code WHERE} tail is
   * applied, or null when the command is not one of them. For a wire protocol that serves the {@code SHOW} commands itself and
   * applies the tail on its own, so it reports the columns even when no row comes back.
   */
  public static ShowCommandTail.Table untailedTable(final DatabaseInternal database, final String query) {
    final String stripped = OpenCypherQueryEngine.stripLeadingComments(query);
    final String normalized = stripped.toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    if (!normalized.startsWith("show "))
      return null;
    final CypherStatement parsed;
    try {
      parsed = database.getCypherStatementCache().getAsWritten(stripped).statement().statement();
    } catch (final CommandParsingException e) {
      // not ours to report: run as a query, the command fails there with the error any other client gets
      return null;
    }
    if (!(parsed instanceof CypherShowRoutinesStatement show))
      return null;
    return table(show, ShowCommandTail.hasTail(stripped));
  }

  private static ShowCommandTail.Table table(final CypherShowRoutinesStatement statement, final boolean allColumns) {
    if (statement.getKind() == CypherShowRoutinesStatement.Kind.PROCEDURES)
      return new ShowCommandTail.Table(allColumns ? PROCEDURE_ALL_COLUMNS : PROCEDURE_DEFAULT_COLUMNS, procedureRows(allColumns));
    return new ShowCommandTail.Table(allColumns ? FUNCTION_ALL_COLUMNS : FUNCTION_DEFAULT_COLUMNS,
        functionRows(statement.getFunctionFilter(), allColumns));
  }

  // ----------------------------------------------------------------------------------------------------------------- PROCEDURES

  private static List<List<Object>> procedureRows(final boolean allColumns) {
    final Map<String, CypherProcedure> byName = new TreeMap<>(Comparator.comparing((String n) -> n.toLowerCase(Locale.ROOT))
        .thenComparing(Comparator.naturalOrder()));
    for (final CypherProcedure procedure : CypherProcedureRegistry.getAllProcedures())
      for (final String name : namesOf(procedure.getName()))
        byName.putIfAbsent(name, procedure);

    final List<List<Object>> rows = new ArrayList<>(byName.size());
    for (final Map.Entry<String, CypherProcedure> entry : byName.entrySet()) {
      final CypherProcedure procedure = entry.getValue();
      final String mode = procedure.isSchemaProcedure() ? "SCHEMA" : procedure.isWriteProcedure() ? "WRITE" : "READ";
      final String name = entry.getKey();
      if (!allColumns) {
        rows.add(Arrays.asList(name, procedure.getDescription(), mode, false));
        continue;
      }
      final List<Map<String, Object>> returned = new ArrayList<>();
      final List<String> signatureColumns = new ArrayList<>();
      for (final String field : procedure.getYieldFields()) {
        returned.add(described(field, "ANY"));
        signatureColumns.add(field + " :: ANY");
      }
      // the registry knows how many arguments a procedure takes, not what they are called or what type they are
      final String signature = name + (procedure.getMaxArgs() == 0 ? "()" : "(...)") + " :: ("
          + String.join(", ", signatureColumns) + ")";
      rows.add(Arrays.asList(name, procedure.getDescription(), mode, false, signature, List.of(), returned, false, null, null, false,
          null, Map.of("deprecated", false)));
    }
    return rows;
  }

  // ------------------------------------------------------------------------------------------------------------------ FUNCTIONS

  private static List<List<Object>> functionRows(final CypherShowRoutinesStatement.FunctionFilter filter, final boolean allColumns) {
    final Map<String, List<Object>> byName = new TreeMap<>(Comparator.comparing((String n) -> n.toLowerCase(Locale.ROOT))
        .thenComparing(Comparator.naturalOrder()));

    if (filter != CypherShowRoutinesStatement.FunctionFilter.USER_DEFINED) {
      final Set<String> builtIns = new java.util.TreeSet<>(CypherBuiltinFunctions.getNames());
      for (final String bridged : BRIDGED_AGGREGATES)
        builtIns.add(bridged.toLowerCase(Locale.ROOT));
      for (final String lowerName : builtIns) {
        final String name = CANONICAL_NAMES.getOrDefault(lowerName, lowerName);
        byName.putIfAbsent(name, functionRow(name, null, name + "(...)", true, AGGREGATING_BUILT_INS.contains(lowerName), allColumns));
      }
    }

    if (filter != CypherShowRoutinesStatement.FunctionFilter.BUILT_IN) {
      // The functions of the registry are the ones ArcadeDB adds to the language, as APOC does on Neo4j: not built in
      for (final StatelessFunction function : CypherFunctionRegistry.getAllFunctions())
        for (final String name : namesOf(function.getName()))
          byName.putIfAbsent(name, functionRow(name, function.getDescription(), function.getSyntax(), false,
              function.aggregateResults(), allColumns));
    }
    return new ArrayList<>(byName.values());
  }

  private static List<Object> functionRow(final String name, final String description, final String signature, final boolean builtIn,
      final boolean aggregating, final boolean allColumns) {
    if (!allColumns)
      return Arrays.asList(name, null, description);
    return Arrays.asList(name, null, description, signature, builtIn, List.of(), null, aggregating, null, null, false, null);
  }

  // --------------------------------------------------------------------------------------------------------------------- HELPERS

  /**
   * The names a registered routine answers to: its own and, when it has not got one, the {@code apoc.} one the engine resolves
   * it under. The {@code db.} and {@code dbms.} ones are Neo4j's own and have no APOC namesake.
   */
  private static List<String> namesOf(final String name) {
    if (name.startsWith(APOC_PREFIX) || name.startsWith("db.") || name.startsWith("dbms."))
      return List.of(name);
    return List.of(name, APOC_PREFIX + name);
  }

  private static Map<String, Object> described(final String name, final String type) {
    final Map<String, Object> description = new LinkedHashMap<>();
    description.put("name", name);
    description.put("description", "");
    description.put("isDeprecated", false);
    description.put("type", type);
    return description;
  }

  private static Map<String, String> canonicalNames(final String... names) {
    final Map<String, String> byLowerCase = new HashMap<>();
    for (final String name : names)
      byLowerCase.put(name.toLowerCase(Locale.ROOT), name);
    return Map.copyOf(byLowerCase);
  }
}
