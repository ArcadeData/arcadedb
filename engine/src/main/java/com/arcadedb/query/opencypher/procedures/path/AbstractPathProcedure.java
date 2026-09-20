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
  /**
   * The "no edge-type filter" argument. {@code getEdges}, {@code getConnectedVertexRIDs} and their iterators all read
   * a null and an empty array the same way, so this is a sentinel, not a behaviour change - and sharing one empty
   * array is safe where sharing {@link #BOTH_DIRECTIONS} would not be, because an array of length zero has nothing a
   * stray write could reach.
   */
  protected static final String[] NO_TYPES = new String[0];

  // Private, not protected: `final` on an array fixes the reference and nothing else, so a shared array with
  // elements in it, handed to subclasses, is one stray write away from corrupting every caller of every path walk
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
   * set probe instead of another failed load and another report. That holds for a neighbour the {@code labelFilter}
   * excludes too, but only when the edges are being collected: a filtered-out neighbour is read solely to learn
   * whether it exists, and only a caller that will be handed the edge reaching it has any use for the answer. Under
   * a plain {@code YIELD nodes} such a neighbour is still never read, which is the filter's whole value.
   * <p>
   * An edge to a filtered-out neighbour that DOES exist stays in the result while the neighbour itself does not.
   * That asymmetry predates this walk and is being decided on its own in issue #7982, not changed here.
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
          // The edges are part of the answer: materialise them, but still take the neighbour from the edge's RID
          // rather than from its record, so an already-visited neighbour costs nothing
          for (final Vertex.DIRECTION direction : BOTH_DIRECTIONS) {
            for (final Edge edge : current.getEdges(direction, edgeTypes)) {
              try {
                // Reading the endpoint is what forces a lazily loaded edge, so a ghost edge record surfaces here
                final RID neighborId = direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();

                // and the edge is recorded only once its far endpoint has answered, so relationships never carries an
                // edge whose vertex nodes had to leave out
                if (visitNeighbor(database, neighborId, labelFilter, true, visitedNodes, ghostNodes, reachableNodes,
                    nextFrontier) && visitedEdges.add(edge.getIdentity()))
                  reachableEdges.add(edge);
              } catch (final RecordNotFoundException e) {
                GhostEdgeReporter.reportSkipped(e);
              }
            }
          }
        } else {
          // Only the nodes are asked for: walk the adjacency entries without loading a single edge record
          for (final RID neighborId : current.getConnectedVertexRIDs(Vertex.DIRECTION.BOTH, edgeTypes))
            visitNeighbor(database, neighborId, labelFilter, false, visitedNodes, ghostNodes, reachableNodes, nextFrontier);
        }
      }

      frontier = nextFrontier;
    }
  }

  /**
   * The neighbour vertex behind a RID, or {@code null} when that RID names no record.
   * <p>
   * A missing endpoint is remembered in {@code ghostNodes}, so the second edge into the same ghost costs a set probe
   * instead of another failed load and another report - which matters on a vertex with a high ghost fan-in, where
   * "report every encounter" means one failed load per edge. It is a set of its own rather than the walk's visited
   * set because the two answer different questions: a ghost was never reached, and marking it as though it had been
   * makes a later edge to it look ordinary (issue #7976).
   */
  protected Vertex resolveNeighbor(final RID neighborId, final RidHashSet ghostNodes) {
    if (ghostNodes.contains(neighborId))
      return null;

    try {
      return neighborId.asVertex();
    } catch (final RecordNotFoundException e) {
      ghostNodes.add(neighborId);
      GhostEdgeReporter.reportSkipped(e);
      return null;
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
   * @param endpointMustExist when true, a neighbour the label filter excludes is still resolved, because the caller
   * is collecting the edges and an edge is only reported where its endpoint is there; when false the filter's whole
   * value is that such a neighbour is never read
   *
   * @return whether the neighbour names a vertex the walk can stand behind: one it loaded, one it had already
   * loaded, or - only when {@code endpointMustExist} is false - one the label filter took out of the answer without
   * ever looking. {@code false} says the endpoint is missing, which is the caller's cue to leave the edge reaching
   * it out of the result too.
   */
  private boolean visitNeighbor(final Database database, final RID neighborId, final String[] labelFilter,
      final boolean endpointMustExist, final RidHashSet visitedNodes, final RidHashSet ghostNodes,
      final List<Vertex> reachableNodes, final List<Vertex> nextFrontier) {
    // Before any set is touched: RidHashSet reads the bucket and offset off the RID without checking it for null
    if (neighborId == null)
      return false;

    if (visitedNodes.contains(neighborId))
      return true;

    if (ghostNodes.contains(neighborId))
      return false;

    if (!matchesLabels(database, neighborId, labelFilter)) {
      // The filter says this neighbour is not part of the answer's nodes. It does NOT say it exists - the label comes
      // from the schema, and no record was read. That is the whole point when only the nodes are wanted; but a caller
      // collecting the edges is promised an edge only where its endpoint is there, so for that caller the endpoint
      // has to be resolved even though the answer will never carry it. Resolving it once is enough: it lands in
      // visitedNodes (handled, deliberately not in reachableNodes) or in ghostNodes, and either way the next edge
      // into it is answered from a set.
      if (!endpointMustExist)
        return true;

      if (resolveNeighbor(neighborId, ghostNodes) == null)
        return false;

      visitedNodes.add(neighborId);
      return true;
    }

    final Vertex neighbor = resolveNeighbor(neighborId, ghostNodes);
    if (neighbor == null)
      return false;

    visitedNodes.add(neighborId);
    reachableNodes.add(neighbor);
    nextFrontier.add(neighbor);
    return true;
  }

  /**
   * Tells whether a vertex's label is one the caller accepts, answered from the RID alone: a vertex's label is its
   * type name, and a bucket belongs to exactly one type, so the schema knows the answer without the record being
   * read. That is the whole point - it replaced a {@code Vertex}-based overload that could only answer once the
   * record was in hand, which meant loading precisely the vertices the filter exists to leave out.
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

  @SuppressWarnings("unchecked")
  protected Map<String, Object> extractConfig(final Object arg) {
    if (arg == null)
      return new HashMap<>();
    if (arg instanceof Map)
      return (Map<String, Object>) arg;
    return new HashMap<>();
  }
}
