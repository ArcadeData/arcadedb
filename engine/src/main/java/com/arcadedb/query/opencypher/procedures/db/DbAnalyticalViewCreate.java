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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewCommands;
import com.arcadedb.query.opencypher.procedures.ProcedureSchemaTypes;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.NumberUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * {@code CALL db.analyticalView.create(name [, vertexTypes [, edgeTypes [, config]]]) YIELD name, created, status}: the
 * Cypher form of {@code CREATE GRAPH ANALYTICAL VIEW} (issue #11438), sharing its body
 * ({@link GraphAnalyticalViewCommands#create}).
 * <p>
 * The types are a name or a list of names, null for every type; a name that is not a vertex type, or an edge type, is
 * refused by name, as the {@code algo.*} procedures refuse it. The config keys mirror the SQL clauses: {@code properties}
 * (an empty list copies no vertex property, an absent key all of them), {@code edgeProperties}, {@code updateMode}
 * ({@code OFF}, {@code SYNCHRONOUS}, {@code ASYNCHRONOUS}), {@code compactionThreshold}, {@code cch} (the weight
 * properties to keep a contraction hierarchy for) and {@code ifNotExists}.
 * <p>
 * As the SQL statement, it returns while the view is still BUILDING: {@code status} says where the build is, and
 * {@code db.analyticalView.list()} follows it. The definition is a schema change, so a rollback of the caller's
 * transaction does not undo it: the view and its definition stay together.
 */
public class DbAnalyticalViewCreate extends AbstractDbAnalyticalViewProcedure {
  public static final String NAME = "db.analyticalView.create";

  private static final Set<String> CONFIG_KEYS = Set.of("properties", "edgeProperties", "updateMode", "compactionThreshold", "cch",
      "ifNotExists");

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Creates a Graph Analytical View over the given vertex and edge types (null for all), as CREATE GRAPH ANALYTICAL VIEW "
        + "does; config keys: properties, edgeProperties, updateMode, compactionThreshold, cch, ifNotExists";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("name", "created", "status");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);
    final DatabaseInternal database = context.getDatabase();
    final String viewName = extractName(args[0]);
    final String[] vertexTypes = args.length > 1 ? emptyAsNull(extractNames(args[1], "vertexTypes")) : null;
    final String[] edgeTypes = args.length > 2 ? emptyAsNull(extractNames(args[2], "edgeTypes")) : null;
    final Map<String, Object> config = extractConfig(args.length > 3 ? args[3] : null, CONFIG_KEYS);

    final Schema schema = database.getSchema();
    if (vertexTypes != null)
      ProcedureSchemaTypes.requireVertexTypes(NAME, schema, vertexTypes);
    if (edgeTypes != null)
      ProcedureSchemaTypes.requireEdgeTypes(NAME, schema, edgeTypes);

    final Object updateModeArg = config.get("updateMode");
    if (updateModeArg != null && !(updateModeArg instanceof String))
      throw new IllegalArgumentException(NAME + "(): updateMode must be a string, got " + describe(updateModeArg));
    final GraphAnalyticalView.UpdateMode updateMode = updateModeArg != null ?
        GraphAnalyticalViewCommands.parseUpdateMode((String) updateModeArg) :
        GraphAnalyticalView.UpdateMode.OFF;

    final GraphAnalyticalViewCommands.Definition definition = new GraphAnalyticalViewCommands.Definition(viewName, vertexTypes,
        edgeTypes, extractNames(config.get("properties"), "properties"), extractNames(config.get("edgeProperties"), "edgeProperties"),
        updateMode, extractCompactionThreshold(config.get("compactionThreshold")), extractNames(config.get("cch"), "cch"));

    final GraphAnalyticalView view = GraphAnalyticalViewCommands.create(database, definition, extractBoolean(config, "ifNotExists"));

    final ResultInternal result = new ResultInternal();
    result.setProperty("name", viewName);
    result.setProperty("created", view != null);
    result.setProperty("status", view != null ? view.getStatus().name() : GraphAnalyticalViewCommands.statusOf(database, viewName));
    return Stream.of(result);
  }

  private int extractCompactionThreshold(final Object value) {
    if (value == null)
      return -1;
    if (!(value instanceof Number number))
      throw new IllegalArgumentException(NAME + "(): compactionThreshold must be a number, got " + describe(value));
    final int threshold;
    try {
      threshold = NumberUtils.toIntExact(number);
    } catch (final ArithmeticException e) {
      throw new IllegalArgumentException(NAME + "(): compactionThreshold is out of range for an int: " + number, e);
    }
    if (threshold < 0)
      throw new IllegalArgumentException(NAME + "(): compactionThreshold must be at least 0, got " + threshold);
    return threshold;
  }

  private static String[] emptyAsNull(final String[] names) {
    return names == null || names.length == 0 ? null : names;
  }
}
