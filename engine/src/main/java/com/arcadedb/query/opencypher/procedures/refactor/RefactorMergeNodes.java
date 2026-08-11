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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: refactor.mergeNodes(nodes, config)
 * <p>
 * Merges a list of nodes into the first one (the survivor). Every incoming and outgoing edge of the
 * other nodes (the absorbed nodes) is rewired onto the survivor - an edge that connected two nodes
 * both being merged becomes a self-relationship on the survivor - and the absorbed nodes are then
 * deleted.
 * </p>
 * <p>
 * {@code config.properties} controls how a property present on both the survivor and an absorbed node
 * is resolved: {@code "overwrite"} (the absorbed node's value wins, the default), {@code "discard"}
 * (the survivor's original value is kept) or {@code "combine"} (both values are kept as a list). A
 * property present only on an absorbed node is always copied onto the survivor.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Person {name:'A'}), (b:Person {name:'B'})
 * CALL apoc.refactor.mergeNodes([a, b], {properties: 'combine'})
 * YIELD node
 * RETURN node
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class RefactorMergeNodes implements CypherProcedure {
  public static final String NAME = "refactor.mergeNodes";

  private static final Set<String> VALID_POLICIES = Set.of("overwrite", "discard", "combine");

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
    return "Merges a list of nodes into the first one, rewiring their edges and deleting the absorbed nodes.";
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
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final List<Vertex> nodes = RefactorProcedureArgs.extractVertices(getName(), args[0]);
    if (nodes.size() < 2)
      throw new CommandSemanticException(getName() + "(): at least two distinct nodes are required to merge");

    final Map<String, Object> config = RefactorProcedureArgs.extractConfig(getName(), args[1]);
    final String globalPolicy = extractPropertiesPolicy(config);

    final Vertex survivor = nodes.get(0);
    final MutableVertex survivorMutable = survivor.modify();

    for (int i = 1; i < nodes.size(); i++) {
      final Vertex absorbed = nodes.get(i);

      mergeProperties(survivorMutable, absorbed, globalPolicy);
      survivorMutable.save();

      rewireEdges(absorbed, survivorMutable);

      absorbed.modify().delete();
    }

    return createResultStream(survivorMutable);
  }

  private void mergeProperties(final MutableVertex survivor, final Vertex absorbed, final String policy) {
    for (final String propertyName : absorbed.getPropertyNames()) {
      final Object absorbedValue = absorbed.get(propertyName);

      if (!survivor.getPropertyNames().contains(propertyName)) {
        survivor.set(propertyName, absorbedValue);
        continue;
      }

      switch (policy) {
        case "overwrite" -> survivor.set(propertyName, absorbedValue);
        case "discard" -> {
          // keep the survivor's original value
        }
        case "combine" -> {
          final Object survivorValue = survivor.get(propertyName);
          final List<Object> combined = new ArrayList<>();
          if (survivorValue instanceof List<?> list)
            combined.addAll(list);
          else
            combined.add(survivorValue);
          if (absorbedValue instanceof List<?> list)
            combined.addAll(list);
          else
            combined.add(absorbedValue);
          survivor.set(propertyName, combined);
        }
        // unreachable in practice - extractPropertiesPolicy validates policy against VALID_POLICIES
        // before mergeProperties is ever called; kept as a defensive fallback against the two drifting
        // apart under a future edit, e.g. a new call site that skips extractPropertiesPolicy
        default -> throw new CommandSemanticException(getName() + "(): unknown properties policy '" + policy + "'");
      }
    }
  }

  private void rewireEdges(final Vertex absorbed, final MutableVertex survivor) {
    final List<Edge> edgesToRewire = new ArrayList<>();
    for (final Edge edge : absorbed.getEdges())
      edgesToRewire.add(edge);

    for (final Edge edge : edgesToRewire) {
      final MutableEdge mutableEdge = edge.modify();
      if (mutableEdge.getOut().equals(absorbed.getIdentity()))
        mutableEdge.set("@out", survivor.getIdentity());
      if (mutableEdge.getIn().equals(absorbed.getIdentity()))
        mutableEdge.set("@in", survivor.getIdentity());
    }
  }

  private Stream<Result> createResultStream(final Vertex node) {
    final ResultInternal result = new ResultInternal();
    result.setProperty("node", node);
    return Stream.of(result);
  }

  private String extractPropertiesPolicy(final Map<String, Object> config) {
    final Object raw = config.get("properties");
    if (raw == null)
      return "overwrite";
    final String policy = raw.toString();
    if (!VALID_POLICIES.contains(policy))
      throw new CommandSemanticException(getName() + "(): unknown properties policy '" + policy + "'");
    return policy;
  }
}
