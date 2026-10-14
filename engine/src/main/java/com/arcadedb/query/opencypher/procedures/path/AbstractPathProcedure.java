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
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.schema.Schema;
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

  /**
   * The edges of {@code vertex} in {@code direction} over {@code relTypes} (all when null or empty), completed with the
   * incoming edges of the unidirectional types among them (issue #8629): a path procedure walks the graph, not what a
   * vertex happens to store, and answers the same whichever way an edge type is stored.
   */
  protected static Iterable<Edge> edgesOf(final CommandContext context, final Vertex vertex, final Vertex.DIRECTION direction,
      final String[] relTypes) {
    final String[] types = relTypes != null ? relTypes : NO_TYPES;
    if (direction == Vertex.DIRECTION.OUT)
      return vertex.getEdges(direction, types);
    return () -> IncomingEdgeLookup.getEdges(context, vertex, direction, types);
  }

  /**
   * The edges the hop leaving {@code vertex}, a node {@code depth} hops away from the start, follows in
   * {@code direction}, or {@code null} when the relationship filter does not follow that direction on this hop.
   */
  protected static Iterable<Edge> hopEdges(final CommandContext context, final Vertex vertex, final Vertex.DIRECTION direction,
      final RelationshipFilter relationships, final int depth) {
    final String[] types = relationships.typesFor(depth, direction);
    return types == null ? null : edgesOf(context, vertex, direction, types);
  }

  // Private, not protected: `final` on an array fixes the reference and nothing else, so a shared array with
  // elements in it, handed to subclasses, is one stray write away from corrupting every caller of every path walk
  private static final Vertex.DIRECTION[] BOTH_DIRECTIONS = { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN };

  /** The direction of the {@code index}-th side a walk reads (0 = outgoing, 1 = incoming), without sharing an array. */
  protected static Vertex.DIRECTION walkDirection(final int index) {
    return index == 0 ? Vertex.DIRECTION.OUT : Vertex.DIRECTION.IN;
  }

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

  /**
   * The relationship and label filters of a walk configured by an APOC-style map: {@code relationshipFilter} and
   * {@code labelFilter}, or {@code sequence}, which takes precedence over both, plus {@code filterStartNode} and
   * {@code beginSequenceAtStart} (issue #10025).
   *
   * @param minLevel the walk's minimum level, which decides whether a termination node closer than that stops the walk
   */
  protected Filters extractFilters(final Map<String, Object> config, final Database database, final int minLevel) {
    final Schema schema = database.getSchema();
    final boolean filterStartNode = extractBoolean(config, "filterStartNode", false);
    final boolean beginSequenceAtStart = extractBoolean(config, "beginSequenceAtStart", true);

    final Object sequence = config.get("sequence");
    if (sequence != null) {
      if (!(sequence instanceof String sequenceString))
        throw new IllegalArgumentException(getName() + "(): sequence must be a string, got " + sequence.getClass().getSimpleName());
      if (!sequenceString.isBlank())
        return extractSequence(sequenceString, schema, filterStartNode, beginSequenceAtStart, minLevel);
    }

    return new Filters(RelationshipFilter.parse(getName(), config.get("relationshipFilter"), schema),
        LabelFilter.parse(getName(), config.get("labelFilter"), schema, filterStartNode, beginSequenceAtStart, minLevel));
  }

  /** The filters of {@code path.expand}'s positional form, which has neither a sequence nor the start-node options. */
  protected Filters extractFilters(final Object relationshipFilter, final Object labelFilter, final Database database,
      final int minLevel) {
    final Schema schema = database.getSchema();
    return new Filters(RelationshipFilter.parse(getName(), relationshipFilter, schema),
        LabelFilter.parse(getName(), labelFilter, schema, false, true, minLevel));
  }

  /**
   * APOC's {@code sequence}: label and relationship filters alternating, one pair per step of a repeating sequence. It
   * starts with the label filter of the start node, or - when the sequence begins one hop away from it - with the
   * relationship filter of the first hop, and so has as many filters of one kind as of the other.
   */
  private Filters extractSequence(final String sequence, final Schema schema, final boolean filterStartNode,
      final boolean beginSequenceAtStart, final int minLevel) {
    final String[] entries = sequence.split(",", -1);
    if (entries.length % 2 != 0)
      throw new IllegalArgumentException(getName() + "(): sequence '" + sequence + "' must alternate label and relationship filters, "
          + (beginSequenceAtStart ?
          "starting with a label filter and ending with a relationship filter" :
          "starting with a relationship filter and ending with a label filter (beginSequenceAtStart is false)"));

    final List<String> labelSteps = new ArrayList<>(entries.length / 2);
    final List<String> relationshipSteps = new ArrayList<>(entries.length / 2);
    for (int i = 0; i < entries.length; i++)
      ((i % 2 == 0) == beginSequenceAtStart ? labelSteps : relationshipSteps).add(entries[i]);

    return new Filters(RelationshipFilter.ofSteps(getName(), relationshipSteps, schema),
        LabelFilter.ofSteps(getName(), labelSteps, schema, filterStartNode, beginSequenceAtStart, minLevel));
  }

  private boolean extractBoolean(final Map<String, Object> config, final String key, final boolean defaultValue) {
    final Object value = config.get(key);
    if (value == null)
      return defaultValue;
    if (value instanceof Boolean b)
      return b;
    throw new IllegalArgumentException(getName() + "(): " + key + " must be a boolean, got " + value.getClass().getSimpleName());
  }

  /**
   * The minimum level of the procedures that report each reachable node once ({@code path.subgraphNodes},
   * {@code path.subgraphAll}, {@code path.spanningTree}): 0 includes the start node, 1 leaves it out. Nothing else means
   * anything for a walk that reaches every node once, by its shortest route, and APOC refuses the rest the same way.
   */
  protected int extractComponentMinLevel(final Map<String, Object> config) {
    final Object value = config.get("minLevel");
    if (value == null)
      return 0;
    if (!(value instanceof Number number) || (number.longValue() != 0 && number.longValue() != 1))
      throw new IllegalArgumentException(getName() + "(): minLevel can only be 0 or 1, got " + value);
    return number.intValue();
  }

  /** The two filters of a walk, parsed once per call. */
  protected record Filters(RelationshipFilter relationships, LabelFilter labels) {
  }

  protected Vertex.DIRECTION parseDirection(final String direction) {
    if (direction == null || direction.isEmpty() || "BOTH".equalsIgnoreCase(direction))
      return Vertex.DIRECTION.BOTH;

    return Vertex.DIRECTION.valueOf(direction.toUpperCase(Locale.ENGLISH));
  }

  /**
   * The path of the walked {@code [v0, e0, v1, ...]} elements: a {@link TraversalPath}, the value every Cypher path is, so
   * the path functions read it and a Bolt client receives a path (issue #9913).
   */
  protected TraversalPath buildPath(final List<Object> elements) {
    return TraversalPath.ofWalk(elements);
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
   * it is reported - but this walk never pays to find that out up front: a traversed edge is always appended to
   * {@code reachableEdges} the moment it is seen (deduplicated by identity), and a closing pass at the end of the
   * walk drops every edge whose endpoint did not, for whatever reason, make it into {@code reachableNodes} - a
   * ghost endpoint and one the {@code labelFilter} excludes are the same case from the closing pass's point of
   * view, and neither has to be resolved just to learn that (issue #7982: {@code relationships} used to keep an
   * edge whose {@code labelFilter}-excluded endpoint was never in {@code nodes}, which also meant loading exactly
   * the vertex record the filter exists to avoid loading). Under a plain {@code YIELD nodes} a filtered-out
   * neighbour is still never read, which remains the filter's whole value there.
   * <p>
   * A node the label filter walks THROUGH without reporting it - one that is not an end node when the filter names end
   * or termination nodes - is visited but kept out of {@code reachableNodes}, so the closing pass drops the edges that
   * reach it too (APOC answers the same).
   * <p>
   * The walk is level-synchronous rather than a queue of (vertex, level) pairs: the level is a property of the
   * wave, so tracking it per entry allocates one wrapper per vertex to carry a number the loop already knows - and the
   * level is also what picks the step of a relationship or label sequence.
   *
   * @param startNode      the vertex the component is measured from; the first entry of the result when it is reported
   * @param filters        the relationships to follow and the nodes to report
   * @param minLevel       0 to report the start node, 1 to leave it out
   * @param maxLevel       maximum number of hops from {@code startNode}
   * @param reachableNodes collects the reported vertices, in breadth-first order
   * @param reachableEdges collects the traversed edges, or {@code null} to skip building them entirely
   */
  protected void collectReachableComponent(final Vertex startNode, final Filters filters, final int minLevel, final int maxLevel,
      final List<Vertex> reachableNodes, final List<Edge> reachableEdges, final CommandContext context) {
    final Database database = startNode.getDatabase();
    final RelationshipFilter relationships = filters.relationships();
    final LabelFilter labels = filters.labels();
    final boolean collectEdges = reachableEdges != null;

    final RidHashSet visitedNodes = new RidHashSet();
    final RidHashSet ghostNodes = new RidHashSet(16);
    final EdgeIdentitySet visitedEdges = collectEdges ? new EdgeIdentitySet() : null;

    final LabelFilter.Verdict startVerdict = labels.evaluate(database, startNode.getIdentity(), 0);
    if (startVerdict == LabelFilter.Verdict.EXCLUDE_AND_PRUNE)
      // The start node itself is denied (filterStartNode): there is no component to report
      return;

    visitedNodes.add(startNode.getIdentity());
    if (startVerdict.includes && minLevel == 0)
      reachableNodes.add(startNode);

    List<Vertex> frontier = new ArrayList<>();
    if (startVerdict.continues)
      frontier.add(startNode);

    for (int level = 0; level < maxLevel && !frontier.isEmpty(); ++level) {
      final List<Vertex> nextFrontier = new ArrayList<>();
      final int depth = level + 1;

      if (collectEdges) {
        for (final Vertex current : frontier) {
          // The edges are part of the answer: materialise them, but still take the neighbour from the edge's RID
          // rather than from its record, so an already-visited neighbour costs nothing
          for (final Vertex.DIRECTION direction : BOTH_DIRECTIONS) {
            final Iterable<Edge> edges = hopEdges(context, current, direction, relationships, level);
            if (edges == null)
              continue;
            for (final Edge edge : edges) {
              try {
                // Reading the endpoint is what forces a lazily loaded edge, so a ghost edge record surfaces here
                final RID neighborId = direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();

                // Recorded unconditionally: the closing pass after the walk is what decides whether the endpoint
                // earned this edge a place in the answer, so no endpoint has to be resolved here just for that
                if (visitedEdges.add(edge.getIdentity()))
                  reachableEdges.add(edge);

                visitNeighbor(database, neighborId, depth, labels, minLevel, visitedNodes, ghostNodes, reachableNodes, nextFrontier);
              } catch (final RecordNotFoundException e) {
                GhostEdgeReporter.reportSkipped(e);
              }
            }
          }
        }
      } else {
        // Only the nodes are asked for: walk the adjacency entries without loading a single edge record. Both sides
        // are read in one pass when the hop follows the same types both ways and none of them is stored one way only.
        // THE INCOMING SIDE OF A UNIDIRECTIONAL EDGE TYPE IS ON THE OTHER END ONLY (ISSUE #8629): READ FROM THE EDGES THAT
        // END IN THE VERTEX
        final String[] outTypes = relationships.typesFor(level, Vertex.DIRECTION.OUT);
        final String[] inTypes = relationships.typesFor(level, Vertex.DIRECTION.IN);
        final boolean incomingSideMissing =
            inTypes != null && IncomingEdgeLookup.isNeeded(context, database, Vertex.DIRECTION.IN, inTypes);
        final boolean bothInOnePass = relationships.isSymmetric(level) && !incomingSideMissing;

        for (final Vertex current : frontier) {
          if (bothInOnePass) {
            for (final RID neighborId : current.getConnectedVertexRIDs(Vertex.DIRECTION.BOTH, outTypes))
              visitNeighbor(database, neighborId, depth, labels, minLevel, visitedNodes, ghostNodes, reachableNodes, nextFrontier);
            continue;
          }

          if (outTypes != null)
            for (final RID neighborId : current.getConnectedVertexRIDs(Vertex.DIRECTION.OUT, outTypes))
              visitNeighbor(database, neighborId, depth, labels, minLevel, visitedNodes, ghostNodes, reachableNodes, nextFrontier);

          if (inTypes == null)
            continue;
          if (incomingSideMissing) {
            for (final Edge edge : edgesOf(context, current, Vertex.DIRECTION.IN, inTypes)) {
              try {
                visitNeighbor(database, edge.getOut(), depth, labels, minLevel, visitedNodes, ghostNodes, reachableNodes, nextFrontier);
              } catch (final RecordNotFoundException e) {
                GhostEdgeReporter.reportSkipped(e);
              }
            }
          } else
            for (final RID neighborId : current.getConnectedVertexRIDs(Vertex.DIRECTION.IN, inTypes))
              visitNeighbor(database, neighborId, depth, labels, minLevel, visitedNodes, ghostNodes, reachableNodes, nextFrontier);
        }
      }

      frontier = nextFrontier;
    }

    if (collectEdges) {
      // Close relationships over the final node set: cheap, because reachableNodes is already the RID set the
      // answer commits to, and this is what keeps an edge from naming a vertex the same result does not contain
      // (issue #7982) - whether that vertex was excluded by labelFilter or never existed in the first place
      final RidHashSet finalNodeIds = new RidHashSet(reachableNodes.size());
      for (final Vertex node : reachableNodes)
        finalNodeIds.add(node.getIdentity());

      reachableEdges.removeIf(edge -> !finalNodeIds.contains(edge.getOut()) || !finalNodeIds.contains(edge.getIn()));
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
   * Adds a neighbour to the walk unless it has been seen already or the label filter turns it away. The vertex record is
   * loaded only once both tests have passed, which is what keeps the walk's loads proportional to the component
   * rather than to its adjacency entries: a neighbour the {@code labelFilter} excludes is never resolved, here or
   * later, because a closing pass over {@code reachableNodes} (not this method) is what keeps a collected edge from
   * naming such a neighbour (issue #7982) - so there is nothing this method needs to know about that neighbour
   * beyond "not part of {@code nodes}".
   * <p>
   * A neighbour turned away is not marked visited: in a label sequence the same node can be welcome at another depth.
   * <p>
   * A RID with no record behind it is recorded in {@code ghostNodes} rather than in {@code visitedNodes}: the two
   * answer different questions, and conflating them would mark a missing vertex as reached - so the FIRST edge to
   * a ghost would be dropped and every later one silently kept, which is worse than either consistent outcome.
   */
  private void visitNeighbor(final Database database, final RID neighborId, final int depth, final LabelFilter labels,
      final int minLevel, final RidHashSet visitedNodes, final RidHashSet ghostNodes, final List<Vertex> reachableNodes,
      final List<Vertex> nextFrontier) {
    // Before any set is touched: RidHashSet reads the bucket and offset off the RID without checking it for null
    if (neighborId == null || visitedNodes.contains(neighborId) || ghostNodes.contains(neighborId))
      return;

    final LabelFilter.Verdict verdict = labels.evaluate(database, neighborId, depth);
    if (verdict == LabelFilter.Verdict.EXCLUDE_AND_PRUNE)
      return;

    final Vertex neighbor = resolveNeighbor(neighborId, ghostNodes);
    if (neighbor == null)
      return;

    visitedNodes.add(neighborId);
    if (verdict.includes && depth >= minLevel)
      reachableNodes.add(neighbor);
    if (verdict.continues)
      nextFrontier.add(neighbor);
  }
}
