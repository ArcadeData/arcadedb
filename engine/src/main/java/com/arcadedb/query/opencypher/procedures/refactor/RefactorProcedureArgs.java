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

import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Argument-extraction helpers shared by the {@code refactor.*} procedures, which all take a
 * {@code (nodes, config)} argument shape.
 */
final class RefactorProcedureArgs {
  private RefactorProcedureArgs() {
  }

  /**
   * Parses {@code arg} into a list of distinct nodes, collapsing a repeated identity to its first
   * occurrence (keeping insertion order). Neither {@code mergeNodes} nor {@code cloneNodesWithRelationships}
   * has a sound way to handle the same node appearing twice - {@code mergeNodes} would re-absorb an
   * already-deleted record, and {@code cloneNodesWithRelationships} would silently orphan one of the two
   * resulting clones (its edges all land on whichever clone {@code cloneOf} was overwritten with last) -
   * so both are better served by treating the list as a set of nodes than by either crashing or producing
   * an output the caller has no way to detect is incomplete.
   */
  static List<Vertex> extractVertices(final String procedureName, final Object arg) {
    if (!(arg instanceof List<?> list))
      throw new IllegalArgumentException(procedureName + "(): nodes must be a list, got " +
          (arg == null ? "null" : arg.getClass().getSimpleName()));

    final Map<RID, Vertex> byIdentity = new LinkedHashMap<>();
    for (final Object item : list) {
      if (!(item instanceof Vertex vertex))
        throw new IllegalArgumentException(procedureName + "(): every element of nodes must be a node, got " +
            (item == null ? "null" : item.getClass().getSimpleName()));
      byIdentity.putIfAbsent(vertex.getIdentity(), vertex);
    }
    return new ArrayList<>(byIdentity.values());
  }

  /**
   * Returns the {@code config} of a {@code (nodes, config = {})} call, defaulting to an empty map when the caller
   * omitted the trailing argument.
   * <p>
   * APOC declares the config with a default - {@code apoc.refactor.cloneNodesWithRelationships(nodes :: LIST<NODE>,
   * config = {} :: MAP)}, and the same for {@code apoc.refactor.mergeNodes} - so Cypher migrated from Neo4j calls
   * them without it. Both procedures therefore declare {@code getMinArgs() == 1}, which is what makes the slot
   * possibly absent here (issue #7427). Both of this method's call sites run {@code validateArgs} first, so
   * {@code args} is non-null and carries at least the nodes.
   * <p>
   * An explicitly passed {@code null} resolves to the same empty map, via {@link #extractConfig}.
   */
  static Map<String, Object> extractOptionalConfig(final String procedureName, final Object[] args) {
    return args.length < 2 ? Collections.emptyMap() : extractConfig(procedureName, args[1]);
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> extractConfig(final String procedureName, final Object arg) {
    if (arg == null)
      return Collections.emptyMap();
    if (!(arg instanceof Map))
      throw new IllegalArgumentException(procedureName + "(): config must be a map, got " + arg.getClass().getSimpleName());
    return (Map<String, Object>) arg;
  }
}
