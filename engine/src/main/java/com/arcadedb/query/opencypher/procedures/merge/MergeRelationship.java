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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.LightEdge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.executor.CypherVertexReload;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: merge.relationship(startNode, relType, matchProps, createProps, endNode, onMatchProps = {})
 * <p>
 * Merges a relationship between two nodes. If a relationship with the specified type
 * and matching properties exists, it returns the existing relationship. Otherwise,
 * it creates a new relationship with both matchProps and createProps.
 * </p>
 * <p>
 * The trailing {@code onMatchProps} is applied to the relationship on the match branch only, mirroring how
 * {@code createProps} is applied on the create branch - it is how a caller says "set these properties when the
 * relationship already existed". It is optional and defaults to an empty map, matching APOC's
 * {@code apoc.merge.relationship(startNode :: NODE, relationshipType :: STRING, identProps :: MAP, onCreateProps ::
 * MAP, endNode :: NODE, onMatchProps = {} :: MAP)}, whose six-argument form ArcadeDB used to reject outright
 * (issue #8103).
 * </p>
 * <p>
 * This is the key use case from issue #3256:
 * <pre>
 * UNWIND $batch AS row
 * MATCH (a), (b) WHERE elementId(a) = row.source_id AND elementId(b) = row.target_id
 * CALL merge.relationship(a, row.rel_type, {}, row.props, b)
 * YIELD rel
 * RETURN elementId(rel) as id
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MergeRelationship implements CypherProcedure {
  public static final String NAME = "merge.relationship";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 5;
  }

  /**
   * Six, not five: APOC's sixth parameter {@code onMatchProps} is now implemented, so the full-arity call is one this
   * procedure accepts (issue #8103). It stays optional - {@link #getMinArgs()} is unchanged - and an omitted slot
   * means "change nothing on match", which is what the five-argument form has always done.
   */
  @Override
  public int getMaxArgs() {
    return 6;
  }

  @Override
  public String getDescription() {
    return "Merges a relationship between two nodes. Creates the relationship if it doesn't exist.";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("rel");
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
    final Vertex startNode = extractVertex(args[0], "startNode");
    final String relType = extractString(args[1], "relType");
    final Map<String, Object> matchProps = extractMap(args[2], "matchProps");
    final Map<String, Object> createProps = extractMap(args[3], "createProps");
    final Vertex endNode = extractVertex(args[4], "endNode");
    // Absent whenever the caller used the five-argument form, which getMinArgs() still allows.
    final Map<String, Object> onMatchProps = args.length < 6 ? null : extractMap(args[5], "onMatchProps");

    final Database database = context.getDatabase();

    // Ensure edge type exists
    if (!database.getSchema().existsType(relType))
      database.getSchema().createEdgeType(relType);

    // The vertex instances the row carries were loaded before the rows ahead of it applied their merges, and
    // appending an edge rewrites the edge-list head pointer of BOTH endpoints - out on the start, in on the
    // end. Everything below reads those pointers: the existence check missed an edge a previous row had
    // already created and merged a duplicate, and appending against a stale head fails outright with
    // "Edge list IN head of vertex ... changed by a concurrent transaction" (issue #7174). Re-read both,
    // exactly as MergeStep does for the anchor of a MERGE clause (issue #6461).
    final Vertex latestStartNode = CypherVertexReload.latest(database, startNode);
    final Vertex latestEndNode = CypherVertexReload.latest(database, endNode);

    // Try to find existing relationship matching the criteria
    final Edge existingEdge = findMatchingEdge(latestStartNode, latestEndNode, relType, matchProps);

    if (existingEdge != null)
      // Return the existing relationship, after applying onMatchProps to it - the match branch's counterpart of the
      // createProps applied below.
      return createResultStream(applyOnMatchProps(existingEdge, onMatchProps));

    // Create new relationship with both matchProps and createProps
    // Note: using bidirectional=true so the edge can be traversed from both ends
    final MutableEdge newEdge = latestStartNode.newEdge(relType, latestEndNode);

    // Apply matchProps
    if (matchProps != null) {
      for (final Map.Entry<String, Object> entry : matchProps.entrySet()) {
        newEdge.set(entry.getKey(), entry.getValue());
      }
    }

    // Apply createProps
    if (createProps != null) {
      for (final Map.Entry<String, Object> entry : createProps.entrySet()) {
        newEdge.set(entry.getKey(), entry.getValue());
      }
    }

    newEdge.save();

    return createResultStream(newEdge);
  }

  /**
   * Applies {@code onMatchProps} to the relationship the merge matched and returns the instance the caller should
   * see - the saved mutable edge when anything was written, the argument itself otherwise.
   * <p>
   * A {@code null} or empty map writes nothing, so the five-argument form and an explicit {@code {}} both leave the
   * matched relationship exactly as it was rather than paying for a no-op record update. APOC applies this map on
   * the match branch only, which is why the create branch below does not consult it (issue #8103).
   * <p>
   * An edge of a {@code LIGHTWEIGHT} type has no record and therefore no properties, so there is nothing to apply
   * the map to. {@code ImmutableLightEdge.modify()} does refuse - "Lightweight edges cannot be modified" - but names
   * neither this procedure nor the argument that asked for the write, so the refusal is raised here instead. The
   * create branch already refuses the same configuration the same way, from {@code MutableLightEdge.set()}.
   */
  private Edge applyOnMatchProps(final Edge existingEdge, final Map<String, Object> onMatchProps) {
    if (onMatchProps == null || onMatchProps.isEmpty())
      return existingEdge;

    if (existingEdge instanceof LightEdge)
      throw new IllegalStateException(getName() + "(): edge type '" + existingEdge.getTypeName()
          + "' is declared LIGHTWEIGHT, so its edges cannot have properties and onMatchProps cannot be applied");

    final MutableEdge mutableEdge = existingEdge.modify();
    for (final Map.Entry<String, Object> entry : onMatchProps.entrySet())
      mutableEdge.set(entry.getKey(), entry.getValue());
    mutableEdge.save();

    return mutableEdge;
  }

  /**
   * Finds an existing edge between startNode and endNode with the given type
   * that matches all the specified properties.
   */
  private Edge findMatchingEdge(final Vertex startNode, final Vertex endNode,
                                final String relType, final Map<String, Object> matchProps) {
    // startNode must be the re-read instance, not the one the row carries - see execute() (issue #7174).
    for (final Edge edge : startNode.getEdges(Vertex.DIRECTION.OUT, relType)) {
      try {
        // Check if edge connects to the endNode
        if (!edge.getIn().equals(endNode.getIdentity()))
          continue;

        // Check if all matchProps match
        if (matchProps == null || matchProps.isEmpty())
          return edge; // No props to match, found a match

        boolean allMatch = true;
        for (final Map.Entry<String, Object> entry : matchProps.entrySet()) {
          final Object edgeValue = edge.get(entry.getKey());
          final Object matchValue = entry.getValue();

          if (matchValue == null) {
            if (edgeValue != null) {
              allMatch = false;
              break;
            }
          } else if (!matchValue.equals(edgeValue)) {
            allMatch = false;
            break;
          }
        }

        if (allMatch) {
          return edge;
        }
      } catch (final RecordNotFoundException e) {
        GhostEdgeReporter.reportSkipped(e);
      }
    }

    return null;
  }

  private Stream<Result> createResultStream(final Edge edge) {
    final ResultInternal result = new ResultInternal();
    result.setProperty("rel", edge);
    return Stream.of(result);
  }

  private Vertex extractVertex(final Object arg, final String paramName) {
    if (arg == null)
      throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");

    if (!(arg instanceof Vertex))
      throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must be a node, got " + arg.getClass().getSimpleName());

    return (Vertex) arg;
  }

  private String extractString(final Object arg, final String paramName) {
    if (arg == null)
      throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");

    return arg.toString();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> extractMap(final Object arg, final String paramName) {
    if (arg == null)
      return null;

    if (!(arg instanceof Map))
      throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must be a map, got " + arg.getClass().getSimpleName());

    return (Map<String, Object>) arg;
  }
}
