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
package com.arcadedb.query.opencypher.procedures.merge;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: merge.node(labels, matchProps, createProps = {})
 * <p>
 * Merges a node with the specified labels. If a node with the given labels
 * and matching properties exists, it returns the existing node. Otherwise,
 * it creates a new node with both matchProps and createProps.
 * </p>
 * <p>
 * {@code createProps} is optional and defaults to an empty map, matching APOC's
 * {@code apoc.merge.node(labels :: LIST<STRING>, identProps :: MAP, onCreateProps = {} :: MAP, onMatchProps = {} ::
 * MAP)} - a call that omits it is the shape APOC's own documentation example uses (issue #8102). APOC's fourth
 * argument, {@code onMatchProps}, is not implemented here at all and is tracked by issue #8117.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL merge.node(['Person'], {name: 'John'}, {age: 30}) YIELD node
 * RETURN node
 *
 * CALL merge.node(['Person'], {name: 'John'}) YIELD node
 * RETURN node
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MergeNode implements CypherProcedure {
  public static final String NAME = "merge.node";

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Two, not three: APOC declares {@code onCreateProps} with a default, so a call that omits it is a call this
   * procedure has to accept (issue #8102), exactly as {@code refactor.mergeNodes} had to for its own trailing
   * {@code config} (issue #7427). {@link #extractOptionalMap} supplies the absent map in its place.
   */
  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Merges a node with specified labels. Creates the node if it doesn't exist.";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node");
  }

  @Override
  public boolean isWriteProcedure() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    // Extract arguments
    final List<String> labels = extractLabels(args[0]);
    final Map<String, Object> matchProps = extractMap(args[1], "matchProps");
    final Map<String, Object> createProps = extractOptionalMap(args, 2, "createProps");

    if (labels.isEmpty()) {
      throw new IllegalArgumentException(getName() + "(): at least one label is required");
    }

    final Database database = context.getDatabase();

    // Get or create the composite type name
    final String typeName = Labels.getCompositeTypeName(labels);

    // Ensure vertex type exists (also creates base types if needed)
    Labels.ensureCompositeType(database.getSchema(), labels);

    // Try to find existing node matching the criteria
    final Vertex existingNode = findMatchingNode(database, typeName, labels, matchProps);

    if (existingNode != null)
      // Return existing node
      return createResultStream(existingNode);

    // Create new node with both matchProps and createProps
    final MutableVertex newNode = database.newVertex(typeName);

    // Apply matchProps
    if (matchProps != null) {
      for (final Map.Entry<String, Object> entry : matchProps.entrySet()) {
        newNode.set(entry.getKey(), entry.getValue());
      }
    }

    // Apply createProps
    if (createProps != null) {
      for (final Map.Entry<String, Object> entry : createProps.entrySet()) {
        newNode.set(entry.getKey(), entry.getValue());
      }
    }

    newNode.save();

    return createResultStream(newNode);
  }

  /**
   * Finds an existing vertex with the given type/labels that matches all the specified properties.
   * <p>
   * The type name and every match-property key are caller-supplied - {@code merge.node(labels, matchProps, ...)}
   * takes them straight from the procedure arguments - so both are emitted through {@link Identifier#quote}
   * rather than spliced between raw back-ticks. Inside a back-tick quoted identifier a backslash escapes the
   * character after it, so a raw splice of {@code a\b} named the property {@code ab} (a lookup that matches
   * nothing, hence a duplicate node) and a raw splice of {@code a\} swallowed the closing back-tick and ran the
   * rest of the statement into the identifier. Same defect and same helper as the Postgres COPY path in #7858
   * (issue #8072).
   */
  private Vertex findMatchingNode(final Database database, final String typeName,
                                  final List<String> labels, final Map<String, Object> matchProps) {
    // Build query to find matching node
    final StringBuilder query = new StringBuilder("SELECT FROM ");
    query.append(Identifier.quote(typeName));

    final List<Object> queryParams = new ArrayList<>();

    if (matchProps != null && !matchProps.isEmpty()) {
      query.append(" WHERE ");
      boolean first = true;
      for (final Map.Entry<String, Object> entry : matchProps.entrySet()) {
        if (!first) {
          query.append(" AND ");
        }
        query.append(Identifier.quote(entry.getKey())).append(" = ?");
        queryParams.add(entry.getValue());
        first = false;
      }
    }

    query.append(" LIMIT 1");

    try (final ResultSet resultSet = database.query("sql", query.toString(), queryParams.toArray())) {
      if (resultSet.hasNext()) {
        final Result result = resultSet.next();
        if (result.isVertex()) {
          return result.getVertex().get();
        }
      }
    }

    return null;
  }

  private Stream<Result> createResultStream(final Vertex node) {
    final ResultInternal result = new ResultInternal();
    result.setProperty("node", node);
    return Stream.of(result);
  }

  @SuppressWarnings("unchecked")
  private List<String> extractLabels(final Object arg) {
    switch (arg) {
      case null -> {
        return List.of();
      }
      case List<?> list -> {
        final List<String> result = new ArrayList<>();
        for (final Object item : list) {
          if (item != null) {
            result.add(item.toString());
          }
        }
        return result;
      }
      case String s -> {
        return List.of(s);
      }
      default -> {
      }
    }

    throw new IllegalArgumentException(
        getName() + "(): labels must be a list or string, got " + arg.getClass().getSimpleName());
  }

  /**
   * The map at {@code index}, or {@code null} when the caller stopped short of that argument - which
   * {@link #execute} applies exactly as it applies an explicitly passed {@code null}, by setting no property from
   * it. {@code validateArgs} has already run, so a shorter array means the argument is one APOC declares with a
   * default and not a malformed call (issue #8102).
   */
  private Map<String, Object> extractOptionalMap(final Object[] args, final int index, final String paramName) {
    return index < args.length ? extractMap(args[index], paramName) : null;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> extractMap(final Object arg, final String paramName) {
    if (arg == null)
      return null;

    if (!(arg instanceof Map)) {
      throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must be a map, got " + arg.getClass().getSimpleName());
    }
    return (Map<String, Object>) arg;
  }
}
