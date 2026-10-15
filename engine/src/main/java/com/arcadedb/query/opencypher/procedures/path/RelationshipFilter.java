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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.ProcedureSchemaTypes;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The {@code relationshipFilter} of the {@code path.*} procedures, in APOC's syntax (issue #10025):
 * <ul>
 *   <li>{@code TYPE} follows {@code TYPE} in both directions, {@code TYPE>} only outgoing, {@code <TYPE} only incoming;</li>
 *   <li>{@code >} alone follows every outgoing relationship, {@code <} alone every incoming one;</li>
 *   <li>{@code |} separates the alternatives of one hop, each with its own direction ({@code ROAD>|<RAIL});</li>
 *   <li>{@code ,} separates the hops of a repeating sequence: {@code NEXT>,<FOLLOWS} walks {@code NEXT} outgoing on the
 *   first hop, {@code FOLLOWS} incoming on the second, {@code NEXT} outgoing on the third, and so on.</li>
 * </ul>
 * A null or blank filter follows every relationship in both directions. Every name must be an edge type of the schema:
 * before #10025 {@code 'ROAD>'} was read as the type name {@code "ROAD>"}, matched no edge and answered an empty result.
 * <p>
 * A hop is stored as two arrays, one per direction, so a walk asks for exactly the edges it follows: {@code null} means
 * the direction is not followed at all, an empty array means every type in that direction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class RelationshipFilter {
  private static final String[] ALL_TYPES = AbstractPathProcedure.NO_TYPES;

  /** No filter: every relationship, both directions. */
  static final RelationshipFilter ANY = new RelationshipFilter(new String[][] { ALL_TYPES }, new String[][] { ALL_TYPES });

  // One entry per hop of the sequence; a filter without a ',' has exactly one
  private final String[][] outTypes;
  private final String[][] inTypes;

  private RelationshipFilter(final String[][] outTypes, final String[][] inTypes) {
    this.outTypes = outTypes;
    this.inTypes = inTypes;
  }

  /**
   * Parses a filter given as a string (APOC's form) or as a list of strings, each of which is one alternative of the
   * single hop (the list form ArcadeDB accepted before #10025, kept: {@code ['ROAD>', '<RAIL']} is {@code 'ROAD>|<RAIL'}).
   * The list form never forms a sequence: a comma inside one of its elements is part of a type name.
   * <p>
   * A type follows its subtypes, as {@code MATCH ()-[:ROAD]->()} does: a hierarchy of edge types has no counterpart in
   * Neo4j, and naming the parent means the whole hierarchy.
   */
  static RelationshipFilter parse(final String procedureName, final Object filter, final Schema schema) {
    final List<String> steps = FilterSteps.split(procedureName, "relationshipFilter", filter);
    return steps == null ? ANY : ofSteps(procedureName, steps, schema);
  }

  /** Builds the filter from its hops, one string per hop: the {@code sequence} configuration hands them over already split. */
  static RelationshipFilter ofSteps(final String procedureName, final List<String> steps, final Schema schema) {
    // A filter that is blank as a whole means "everything", as a missing one does
    if (FilterSteps.isBlank(steps))
      return ANY;

    final String[][] out = new String[steps.size()][];
    final String[][] in = new String[steps.size()][];
    for (int i = 0; i < steps.size(); i++)
      parseStep(procedureName, steps.get(i), schema, i, out, in);
    return new RelationshipFilter(out, in);
  }

  private static void parseStep(final String procedureName, final String step, final Schema schema, final int index,
      final String[][] out, final String[][] in) {
    final Set<String> outNames = new LinkedHashSet<>();
    final Set<String> inNames = new LinkedHashSet<>();
    boolean outAll = false;
    boolean inAll = false;
    boolean any = false;

    for (final String rawEntry : step.split("\\|")) {
      final String entry = rawEntry.trim();
      if (entry.isEmpty())
        continue;
      any = true;

      final boolean incoming = entry.charAt(0) == '<';
      final boolean outgoing = entry.charAt(entry.length() - 1) == '>';
      if (incoming && outgoing && entry.length() > 1) {
        final String inner = entry.substring(1, entry.length() - 1).trim();
        throw new IllegalArgumentException(procedureName + "(): relationship filter '" + entry + "' carries both direction markers: "
            + (inner.isEmpty() ? "leave the filter empty" : "write '" + inner + "'") + " to follow it in both directions");
      }

      final String name = entry.substring(incoming ? 1 : 0, entry.length() - (outgoing ? 1 : 0)).trim();
      if (name.isEmpty()) {
        // '>' or '<' alone: every type in that direction
        if (outgoing)
          outAll = true;
        else
          inAll = true;
        continue;
      }

      ProcedureSchemaTypes.requireEdgeTypes(procedureName, schema, name);
      if (!incoming)
        outNames.add(name);
      if (!outgoing)
        inNames.add(name);
    }

    if (!any) {
      // An empty hop inside a sequence ('A>,,<B'): any relationship, both directions
      out[index] = ALL_TYPES;
      in[index] = ALL_TYPES;
      return;
    }
    out[index] = outAll ? ALL_TYPES : outNames.isEmpty() ? null : outNames.toArray(new String[0]);
    in[index] = inAll ? ALL_TYPES : inNames.isEmpty() ? null : inNames.toArray(new String[0]);
  }

  /**
   * The types the hop leaving a node at {@code depth} follows in {@code direction}: {@code null} when that direction is
   * not followed, an empty array for every type.
   */
  String[] typesFor(final int depth, final Vertex.DIRECTION direction) {
    final String[][] byStep = direction == Vertex.DIRECTION.OUT ? outTypes : inTypes;
    return byStep[depth % byStep.length];
  }

  /** Whether the hop leaving a node at {@code depth} follows {@code edge}, by its type, in {@code direction}. */
  boolean follows(final int depth, final Vertex.DIRECTION direction, final Edge edge) {
    final String[] types = typesFor(depth, direction);
    if (types == null)
      return false;
    if (types.length == 0)
      return true;
    final DocumentType edgeType = edge.getType();
    for (final String type : types)
      if (edgeType.instanceOf(type))
        return true;
    return false;
  }

  /**
   * Whether the hop leaving a node at {@code depth} follows the same types in both directions, so a walk can read both
   * sides of the adjacency in one pass.
   */
  boolean isSymmetric(final int depth) {
    final String[] out = typesFor(depth, Vertex.DIRECTION.OUT);
    final String[] in = typesFor(depth, Vertex.DIRECTION.IN);
    return out != null && in != null && Arrays.equals(out, in);
  }
}
