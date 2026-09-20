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

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeIdentitySet;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.RidHashSet;

import java.util.*;

/**
 * Abstract base class for path expansion procedures.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractPathProcedure implements CypherProcedure {
  // PRIVATE, NOT protected: `final` on an array fixes the reference and nothing else, so a shared mutable array
  // handed to subclasses is one stray write away from corrupting every caller of every path procedure
  private static final String[]           NO_TYPES        = new String[0];
  private static final Vertex.DIRECTION[] BOTH_DIRECTIONS = { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN };

  protected Vertex extractVertex(final Object arg, final String paramName) {
    if (arg == null)
      throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");

    if (arg instanceof Vertex v)
      return v;

    if (arg instanceof Document doc && doc instanceof Vertex v)
      return v;

    throw new IllegalArgumentException(
        getName() + "(): " + paramName + " must be a node, got " + arg.getClass().getSimpleName());
  }

  @SuppressWarnings("unchecked")
  protected String[] extractRelTypes(final Object arg) {
    if (arg == null)
      return null;

    if (arg instanceof String s) {
      final String trimmedSource = s.trim();
      if (trimmedSource.isEmpty())
        return null;

      // Handle pipe- or comma-separated format "REL1|REL2" or "REL1,REL2"
      if (trimmedSource.contains("|") || trimmedSource.contains(",")) {
        final String[] types = Arrays.stream(trimmedSource.split("[,|]")).map(String::trim).filter(t -> !t.isEmpty()).toArray(String[]::new);
        return types.length == 0 ? null : types;
      }

      return new String[]{trimmedSource};
    }
    if (arg instanceof Collection<?> coll)
      return coll.stream().map(Object::toString).toArray(String[]::new);

    return new String[]{arg.toString()};
  }

  @SuppressWarnings("unchecked")
  protected String[] extractLabels(final Object arg) {
    if (arg == null)
      return null;

    if (arg instanceof String s) {
      final String trimmedSource = s.trim();
      if (trimmedSource.isEmpty())
        return null;

      // Handle pipe- or comma-separated format "Label1|Label2" or "Label1,Label2"
      if (trimmedSource.contains("|") || trimmedSource.contains(",")) {
        final String[] labels = Arrays.stream(trimmedSource.split("[,|]")).map(String::trim).filter(t -> !t.isEmpty()).toArray(String[]::new);
        return labels.length == 0 ? null : labels;
      }

      return new String[]{trimmedSource};
    }
    if (arg instanceof Collection<?> coll)
      return coll.stream().map(Object::toString).toArray(String[]::new);

    return new String[]{arg.toString()};
  }

  protected Vertex.DIRECTION parseDirection(final String direction) {
    if (direction == null || direction.isEmpty() || "BOTH".equalsIgnoreCase(direction))
      return Vertex.DIRECTION.BOTH;

    return Vertex.DIRECTION.valueOf(direction.toUpperCase(Locale.ENGLISH));
  }

  protected Map<String, Object> buildPath(final List<Object> elements) {
    final List<Object> nodes = new ArrayList<>();
    final List<Object> relationships = new ArrayList<>();

    for (final Object element : elements) {
      if (element instanceof Vertex) {
        nodes.add(element);
      } else if (element instanceof Edge) {
        relationships.add(element);
      }
    }

    final Map<String, Object> path = new HashMap<>();
    path.put("_type", "path");
    path.put("nodes", nodes);
    path.put("relationships", relationships);
    path.put("length", relationships.size());
    return path;
  }

  /**
   * Walks the component reachable from {@code startNode} breadth-first, collecting the vertices - and, only when
   * {@code reachableEdges} is non-null, the edges - it reaches.
   * <p>
   * Shared by {@code path.subgraphAll}, {@code path.subgraphNodes} and anything else that needs a reachable
   * component, because the expensive part is not the walk but what the walk loads, and getting that wrong once is
   * enough (issue #7976). Three rules keep the loaded set down to what the caller asked for:
   * <ul>
   *   <li><b>A neighbour is identified by its RID, not by its record.</b> Both RIDs of an adjacency entry sit
   *   inline in the edge segment, so the "have I been here already?" test - which rejects the large majority of
   *   entries in any graph that is not a tree - costs two primitive comparisons and touches no record at all. The
   *   previous walk asked the edge for {@code getInVertex()}/{@code getOutVertex()} FIRST and deduplicated
   *   afterwards, so a component with E adjacency entries and V vertices loaded ~2E vertex records instead of V,
   *   plus an edge record for every one of them.</li>
   *   <li><b>A label filter is answered from the schema, not from the record.</b> A vertex's label is its type, and
   *   its type is determined by the bucket its RID names, so a neighbour excluded by {@code labelFilter} is never
   *   loaded.</li>
   *   <li><b>Edges are collected only when the caller yields them.</b> Under a plain {@code YIELD nodes} the walk
   *   uses the neighbour-RID iterator, which reads the adjacency entries without materialising a single edge.</li>
   * </ul>
   * An adjacency entry whose far endpoint has no record is dropped whole - neither the vertex nor the edge reaching
   * it is reported - and the missing endpoint is remembered, so the second edge pointing at the same ghost costs a
   * set probe instead of another failed load and another report. The one entry not covered by that guarantee is a
   * neighbour the {@code labelFilter} excludes: it is never loaded, so nothing here knows whether it exists, and
   * loading it to find out would undo the very saving the filter is there to make.
   * The walk is level-synchronous rather than a queue of (vertex, level) pairs: the level is a property of the
   * wave, so tracking it per entry allocates one wrapper per vertex to carry a number the loop already knows.
   *
   * @param startNode      the vertex the component is measured from; always the first entry of the result
   * @param relTypes       edge types to follow, or {@code null}/empty for all of them
   * @param labelFilter    vertex labels to accept, or {@code null}/empty for all of them
   * @param maxLevel       maximum number of hops from {@code startNode}
   * @param reachableNodes collects the reached vertices, in breadth-first order
   * @param reachableEdges collects the traversed edges, or {@code null} to skip building them entirely
   */
  protected void collectReachableComponent(final Vertex startNode, final String[] relTypes, final String[] labelFilter,
      final int maxLevel, final List<Vertex> reachableNodes, final List<Edge> reachableEdges) {
    final Database database = startNode.getDatabase();
    final String[] edgeTypes = relTypes != null ? relTypes : NO_TYPES;
    final boolean collectEdges = reachableEdges != null;

    final RidHashSet visitedNodes = new RidHashSet();
    final RidHashSet ghostNodes = new RidHashSet(16);
    final EdgeIdentitySet visitedEdges = collectEdges ? new EdgeIdentitySet() : null;

    visitedNodes.add(startNode.getIdentity());
    reachableNodes.add(startNode);

    List<Vertex> frontier = new ArrayList<>();
    frontier.add(startNode);

    for (int level = 0; level < maxLevel && !frontier.isEmpty(); ++level) {
      final List<Vertex> nextFrontier = new ArrayList<>();

      for (final Vertex current : frontier) {
        if (collectEdges) {
          // THE EDGES ARE PART OF THE ANSWER: MATERIALISE THEM, BUT STILL TAKE THE NEIGHBOUR FROM THE EDGE'S RID
          // RATHER THAN FROM ITS RECORD, SO AN ALREADY-VISITED NEIGHBOUR COSTS NOTHING
          for (final Vertex.DIRECTION direction : BOTH_DIRECTIONS) {
            for (final Edge edge : current.getEdges(direction, edgeTypes)) {
              try {
                // READING THE ENDPOINT IS WHAT FORCES A LAZILY LOADED EDGE, SO A GHOST EDGE RECORD SURFACES HERE
                final RID neighborId = direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();

                // AND THE EDGE IS RECORDED ONLY ONCE ITS FAR ENDPOINT HAS ANSWERED, SO relationships NEVER CARRIES AN
                // EDGE WHOSE VERTEX nodes HAD TO LEAVE OUT
                if (visitNeighbor(database, neighborId, labelFilter, visitedNodes, ghostNodes, reachableNodes, nextFrontier)
                    && visitedEdges.add(edge.getIdentity()))
                  reachableEdges.add(edge);
              } catch (final RecordNotFoundException e) {
                GhostEdgeReporter.reportSkipped(e);
              }
            }
          }
        } else {
          // ONLY THE NODES ARE ASKED FOR: WALK THE ADJACENCY ENTRIES WITHOUT LOADING A SINGLE EDGE RECORD
          for (final RID neighborId : current.getConnectedVertexRIDs(Vertex.DIRECTION.BOTH, edgeTypes))
            visitNeighbor(database, neighborId, labelFilter, visitedNodes, ghostNodes, reachableNodes, nextFrontier);
        }
      }

      frontier = nextFrontier;
    }
  }

  /**
   * Adds a neighbour to the walk unless it has been seen already or its label is filtered out. The vertex record is
   * loaded only once both tests have passed, which is what keeps the walk's loads proportional to the component
   * rather than to its adjacency entries.
   * <p>
   * A RID with no record behind it is recorded in {@code ghostNodes} rather than in {@code visitedNodes}: the two
   * answer different questions, and conflating them would mark a missing vertex as reached - so the FIRST edge to
   * a ghost would be dropped and every later one silently kept, which is worse than either consistent outcome.
   *
   * @return whether the neighbour names a vertex the walk can stand behind: one it loaded, one it had already
   * loaded, or one the label filter took out of the answer without ever looking. {@code false} says the endpoint is
   * missing, which is the caller's cue to leave the edge reaching it out of the result too.
   */
  private boolean visitNeighbor(final Database database, final RID neighborId, final String[] labelFilter,
      final RidHashSet visitedNodes, final RidHashSet ghostNodes, final List<Vertex> reachableNodes,
      final List<Vertex> nextFrontier) {
    if (neighborId == null)
      return false;

    if (visitedNodes.contains(neighborId))
      return true;

    if (ghostNodes.contains(neighborId))
      return false;

    if (!matchesLabels(database, neighborId, labelFilter))
      return true;

    final Vertex neighbor;
    try {
      neighbor = neighborId.asVertex();
    } catch (final RecordNotFoundException e) {
      ghostNodes.add(neighborId);
      GhostEdgeReporter.reportSkipped(e);
      return false;
    }

    visitedNodes.add(neighborId);
    reachableNodes.add(neighbor);
    nextFrontier.add(neighbor);
    return true;
  }

  /**
   * {@link #matchesLabels(Vertex, String[])} answered from the RID alone. A vertex's label is its type name, and a
   * bucket belongs to exactly one type, so the schema knows the answer without the record being read.
   */
  protected boolean matchesLabels(final Database database, final RID vertexId, final String[] labels) {
    if (labels == null || labels.length == 0)
      return true;

    final DocumentType type = database.getSchema().getTypeByBucketId(vertexId.getBucketId());
    if (type == null)
      return false;

    final String vertexType = type.getName();
    for (final String label : labels)
      if (vertexType.equals(label))
        return true;

    return false;
  }

  protected boolean matchesLabels(final Vertex vertex, final String[] labels) {
    if (labels == null || labels.length == 0)
      return true;

    final String vertexType = vertex.getTypeName();
    for (final String label : labels) {
      if (vertexType.equals(label))
        return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> extractConfig(final Object arg) {
    if (arg == null)
      return new HashMap<>();
    if (arg instanceof Map)
      return (Map<String, Object>) arg;
    return new HashMap<>();
  }
}
