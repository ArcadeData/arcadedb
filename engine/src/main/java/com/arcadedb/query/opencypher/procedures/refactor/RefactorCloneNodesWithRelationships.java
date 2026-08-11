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
package com.arcadedb.query.opencypher.procedures.refactor;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: refactor.cloneNodesWithRelationships(nodes, config)
 * <p>
 * Clones each given node - a new vertex of the same type carrying the same properties - together with
 * every relationship touching it in either direction. A relationship whose other endpoint is also being
 * cloned reconnects the two clones rather than the originals; a relationship to a node outside the given
 * list reconnects the clone to that same original node. The source nodes and their relationships are
 * left untouched.
 * </p>
 * <p>
 * {@code config.skipProperties}, when given, is a list of property names excluded from the clone.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Person {name:'A'}), (b:Person {name:'B'})
 * CALL apoc.refactor.cloneNodesWithRelationships([a, b], {})
 * YIELD input, output, error
 * RETURN input, output, error
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class RefactorCloneNodesWithRelationships implements CypherProcedure {
  public static final String NAME = "refactor.cloneNodesWithRelationships";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Clones each given node together with every relationship touching it.";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("input", "output", "error");
  }

  @Override
  public boolean isWriteProcedure() {
    return true;
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final List<Vertex> nodes = extractVertices(args[0]);
    final Set<String> skipProperties = extractSkipProperties(args[1]);
    final Database database = context.getDatabase();

    final Map<RID, Vertex> cloneOf = new LinkedHashMap<>();
    final List<Result> results = new ArrayList<>();

    for (final Vertex original : nodes) {
      try {
        final MutableVertex clone = database.newVertex(original.getTypeName());
        for (final String propertyName : original.getPropertyNames()) {
          if (skipProperties.contains(propertyName))
            continue;
          clone.set(propertyName, original.get(propertyName));
        }
        clone.save();
        cloneOf.put(original.getIdentity(), clone);
        results.add(row(original, clone, null));
      } catch (final Exception e) {
        results.add(row(original, null, e.getMessage()));
      }
    }

    final Set<RID> processedEdges = new HashSet<>();
    for (final Vertex original : nodes) {
      if (!cloneOf.containsKey(original.getIdentity()))
        continue;

      for (final Edge edge : original.getEdges()) {
        if (!processedEdges.add(edge.getIdentity()))
          continue;
        cloneEdge(edge, cloneOf);
      }
    }

    return results.stream();
  }

  private void cloneEdge(final Edge edge, final Map<RID, Vertex> cloneOf) {
    final RID originalOut = edge.getOut();
    final RID originalIn = edge.getIn();

    final Vertex newOutVertex = cloneOf.containsKey(originalOut) ? cloneOf.get(originalOut) : originalOut.asVertex(true);
    final Identifiable newInTarget = cloneOf.containsKey(originalIn) ? cloneOf.get(originalIn) : originalIn;

    final MutableEdge newEdge = newOutVertex.newEdge(edge.getTypeName(), newInTarget);
    final Map<String, Object> properties = edge.propertiesAsMap();
    if (!properties.isEmpty())
      newEdge.set(properties);
    newEdge.save();
  }

  private Result row(final Vertex input, final Vertex output, final String error) {
    final ResultInternal result = new ResultInternal();
    result.setProperty("input", input);
    result.setProperty("output", output);
    result.setProperty("error", error);
    return result;
  }

  private List<Vertex> extractVertices(final Object arg) {
    if (!(arg instanceof List<?> list))
      throw new IllegalArgumentException(getName() + "(): nodes must be a list, got " +
          (arg == null ? "null" : arg.getClass().getSimpleName()));

    final List<Vertex> vertices = new ArrayList<>();
    for (final Object item : list) {
      if (!(item instanceof Vertex vertex))
        throw new IllegalArgumentException(getName() + "(): every element of nodes must be a node, got " +
            (item == null ? "null" : item.getClass().getSimpleName()));
      vertices.add(vertex);
    }
    return vertices;
  }

  @SuppressWarnings("unchecked")
  private Set<String> extractSkipProperties(final Object configArg) {
    if (!(configArg instanceof Map))
      return Collections.emptySet();

    final Object skip = ((Map<String, Object>) configArg).get("skipProperties");
    if (!(skip instanceof List))
      return Collections.emptySet();

    final Set<String> result = new HashSet<>();
    for (final Object item : (List<?>) skip)
      if (item != null)
        result.add(item.toString());
    return result;
  }
}
