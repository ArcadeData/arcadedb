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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeEntryRIDIterator;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.NumberUtils;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Abstract base class for path expansion procedures.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractPathProcedure implements CypherProcedure {
  /**
   * The "no edge-type filter" argument. {@code getEdges}, {@code getConnectedVertexRIDs} and their iterators all read
   * a null and an empty array the same way, so this is a sentinel, not a behaviour change - and sharing one empty
   * array is safe where sharing one with elements would not be, because an array of length zero has nothing a stray
   * write could reach.
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

  /**
   * The direction of the {@code index}-th side a walk reads (0 = outgoing, 1 = incoming), without sharing an array: `final`
   * on an array fixes the reference and nothing else, so a shared array with elements in it, handed to subclasses, is one
   * stray write away from corrupting every caller of every path walk.
   */
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
   * The relationship, label and node filters of a walk configured by an APOC-style map: {@code relationshipFilter} and
   * {@code labelFilter}, or {@code sequence}, which takes precedence over both, plus {@code filterStartNode} and
   * {@code beginSequenceAtStart} (issue #10025), and the node lists {@code endNodes}, {@code terminatorNodes},
   * {@code allowlistNodes} and {@code denylistNodes} (issue #10049).
   *
   * @param minLevel the walk's minimum level, which decides whether a termination node closer than that stops the walk
   */
  protected Filters extractFilters(final Map<String, Object> config, final Database database, final int minLevel) {
    final Schema schema = database.getSchema();
    final boolean filterStartNode = extractBoolean(config, "filterStartNode", false);
    final boolean beginSequenceAtStart = extractBoolean(config, "beginSequenceAtStart", true);
    final NodeFilter nodes = NodeFilter.parse(getName(), config, filterStartNode, minLevel);

    final Object sequence = config.get("sequence");
    if (sequence != null) {
      if (!(sequence instanceof String sequenceString))
        throw new IllegalArgumentException(getName() + "(): sequence must be a string, got " + sequence.getClass().getSimpleName());
      if (!sequenceString.isBlank())
        return extractSequence(sequenceString, schema, filterStartNode, beginSequenceAtStart, minLevel, nodes);
    }

    return new Filters(RelationshipFilter.parse(getName(), config.get("relationshipFilter"), schema),
        LabelFilter.parse(getName(), config.get("labelFilter"), schema, filterStartNode, beginSequenceAtStart, minLevel), nodes);
  }

  /** The filters of {@code path.expand}'s positional form, which has neither a sequence nor the start-node options. */
  protected Filters extractPositionalFilters(final Object relationshipFilter, final Object labelFilter, final Database database,
      final int minLevel) {
    final Schema schema = database.getSchema();
    return new Filters(RelationshipFilter.parse(getName(), relationshipFilter, schema),
        LabelFilter.parse(getName(), labelFilter, schema, false, true, minLevel), NodeFilter.NONE);
  }

  /**
   * APOC's {@code sequence}: label and relationship filters alternating, one pair per step of a repeating sequence. It
   * starts with the label filter of the start node, or - when the sequence begins one hop away from it - with the
   * relationship filter of the first hop, and so has as many filters of one kind as of the other.
   */
  private Filters extractSequence(final String sequence, final Schema schema, final boolean filterStartNode,
      final boolean beginSequenceAtStart, final int minLevel, final NodeFilter nodes) {
    final String[] entries = sequence.split(",", -1);
    if (entries.length % 2 != 0)
      throw new IllegalArgumentException(getName() + "(): sequence '" + sequence + "' must alternate label and relationship filters, "
          + (beginSequenceAtStart ?
          "starting with a label filter and ending with a relationship filter" :
          "starting with a relationship filter and ending with a label filter (beginSequenceAtStart is false)"));

    final List<String> labelSteps = new ArrayList<>(entries.length / 2);
    final List<String> relationshipSteps = new ArrayList<>(entries.length / 2);
    for (int i = 0; i < entries.length; i++) {
      // Even entries are label filters when the sequence begins at the start node, relationship filters otherwise
      final boolean labelEntry = (i % 2 == 0) == beginSequenceAtStart;
      (labelEntry ? labelSteps : relationshipSteps).add(entries[i]);
    }

    return new Filters(RelationshipFilter.ofSteps(getName(), relationshipSteps, schema),
        LabelFilter.ofSteps(getName(), labelSteps, schema, filterStartNode, beginSequenceAtStart, minLevel), nodes);
  }

  protected boolean extractBoolean(final Map<String, Object> config, final String key, final boolean defaultValue) {
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
    if (!(value instanceof Number number) || (number.doubleValue() != 0 && number.doubleValue() != 1))
      throw new IllegalArgumentException(getName() + "(): minLevel can only be 0 or 1, got " + value);
    return number.intValue();
  }

  /**
   * The filters of a walk, parsed once per call: the relationships to follow, and the label filter and node lists that
   * decide which of the nodes reached are answers and which the walk goes on past.
   */
  protected record Filters(RelationshipFilter relationships, LabelFilter labels, NodeFilter nodes) {
    /**
     * The verdict on the node {@code vertexId} names, reached {@code depth} hops from the start node: an answer only if the
     * label filter and the node lists both include it, and walked past only if both let it, as the evaluators of an APOC
     * traversal combine. Answered from the RID alone, so a node a filter turns away is never loaded.
     */
    LabelFilter.Verdict evaluate(final Database database, final RID vertexId, final int depth) {
      final LabelFilter.Verdict verdict = labels.evaluate(database, vertexId, depth);
      if (nodes.isEmpty() || verdict == LabelFilter.Verdict.EXCLUDE_AND_PRUNE)
        return verdict;
      return verdict.and(nodes.evaluate(vertexId, depth));
    }
  }

  /** An integer option of the configuration map: a missing one is {@code defaultValue}. */
  protected int extractInt(final Map<String, Object> config, final String key, final int defaultValue) {
    final Object value = config.get(key);
    if (value == null)
      return defaultValue;
    if (!(value instanceof Number number))
      throw new IllegalArgumentException(getName() + "(): " + key + " must be a number, got " + value.getClass().getSimpleName());
    return NumberUtils.saturateToInt(number);
  }

  /** {@code maxLevel}: no bound when absent, as APOC's default of -1 reads, and for any negative value. */
  protected int extractMaxLevel(final Map<String, Object> config) {
    final int maxLevel = extractInt(config, "maxLevel", -1);
    return maxLevel < 0 ? Integer.MAX_VALUE : maxLevel;
  }

  /** {@code limit}: no bound when absent, as APOC's default of -1 reads, and for any negative value. */
  protected int extractLimit(final Map<String, Object> config) {
    final int limit = extractInt(config, "limit", -1);
    return limit < 0 ? Integer.MAX_VALUE : limit;
  }

  /**
   * The {@code uniqueness} option, checked. Every procedure that reads it checks it - including those that walk
   * {@link PathUniqueness#NODE_GLOBAL} whatever it says, as APOC's do - so a value that is no uniqueness is refused wherever
   * it is written.
   */
  protected PathUniqueness extractUniqueness(final Map<String, Object> config, final PathUniqueness defaultValue) {
    return PathUniqueness.parse(getName(), config.get("uniqueness"), defaultValue);
  }

  /**
   * The rows of a walk that yields paths. The walk is not run to the end here: it goes on as the query asks for rows.
   *
   * <p>
   * The heap the walk holds is given back when it ends, or when the stream is closed, which the {@code CALL} step does when the
   * rows run out or the query's result set is closed with rows still unread (issue #10150). A consumer that reads the stream
   * itself and stops without closing leaves it to the query's heap tracker, which gives back whatever a query still holds once
   * it is unreachable.
   *
   * @param optional APOC's {@code optional}: when the walk answers nothing, one row with a null path
   */
  protected Stream<Result> pathRows(final PathExpansion paths, final boolean optional) {
    if (optional && !paths.hasNext())
      return Stream.of(nullRow("path"));
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(paths, Spliterator.ORDERED | Spliterator.NONNULL), false)
        .map(elements -> {
          final ResultInternal result = new ResultInternal();
          result.setProperty("path", buildPath(elements));
          return (Result) result;
        }).onClose(paths::release);
  }

  /** The single row of an {@code optional} walk that found nothing: the field is there, and null. */
  protected static Result nullRow(final String field) {
    final ResultInternal result = new ResultInternal();
    result.setProperty(field, null);
    return result;
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
   * Walks the component reachable from {@code startNode} breadth-first, collecting the vertices it reaches - and, only when
   * {@code reachableEdges} is non-null, the relationships between them.
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
   *   <li><b>The label filter and the node lists are answered from the RID, not from the record.</b> A vertex's label is
   *   its type, and its type is determined by the bucket its RID names, so a neighbour excluded by {@code labelFilter}
   *   or by a node list is never loaded.</li>
   *   <li><b>Relationships are collected only when the caller yields them.</b> Under a plain {@code YIELD nodes} the walk
   *   uses the neighbour-RID iterator, which reads the adjacency entries without materialising a single edge.</li>
   *   <li><b>An entry reaches its neighbour only if the edge it names still has its record</b> (issue #10148). The bucket is
   *   asked about the edge RID the entry holds once per node reached, never per entry and without reading the record, and a
   *   dangling entry is reported and skipped without spending the node, so the walk, {@code path.expandConfig} and
   *   {@code path.spanningTree} agree on the component, and a node of {@code path.subgraphAll} is always reached by one of the
   *   relationships it returns.</li>
   * </ul>
   * The walk is {@link PathUniqueness#NODE_GLOBAL}: a node is spent the moment it is first reached, whatever the filters make of
   * it there, as it is in APOC. A node met again where a label sequence would welcome it is not a new node.
   * <p>
   * The relationships are not the ones the walk followed. They are every relationship, of any type, between two of the
   * nodes the walk reports: that is what {@code apoc.path.subgraphAll} answers, and what "the subgraph" of a set of nodes
   * is (issue #10048). They are found after the walk, from the outgoing relationships of each reported node - every
   * relationship has its source among them or is not wanted - by the RID of the far end alone, so a relationship that leads
   * out of the subgraph costs one adjacency entry and no record. An adjacency entry whose far endpoint has no record is
   * not in the node set, so the relationship reaching it is not reported (issue #7982).
   * <p>
   * The walk is level-synchronous rather than a queue of (vertex, level) pairs: the level is a property of the
   * wave, so tracking it per entry allocates one wrapper per vertex to carry a number the loop already knows - and the
   * level is also what picks the step of a relationship or label sequence.
   *
   * @param startNode      the vertex the component is measured from; the first entry of the result when it is reported
   * @param filters        the relationships to follow and the nodes to report
   * @param minLevel       0 to report the start node, 1 to leave it out
   * @param maxLevel       maximum number of hops from {@code startNode}
   * @param limit          the most nodes to report
   * @param reachableNodes collects the reported vertices, in breadth-first order
   * @param reachableEdges collects the relationships between them, or {@code null} to skip building them entirely
   */
  protected void collectReachableComponent(final Vertex startNode, final Filters filters, final int minLevel, final int maxLevel,
      final int limit, final List<Vertex> reachableNodes, final List<Edge> reachableEdges, final CommandContext context) {
    if (limit <= 0)
      return;

    final Database database = startNode.getDatabase();
    final RelationshipFilter relationships = filters.relationships();

    final ComponentWalk walk = new ComponentWalk(database, filters, minLevel, limit, reachableNodes);

    final LabelFilter.Verdict startVerdict = filters.evaluate(database, startNode.getIdentity(), 0);
    if (startVerdict == LabelFilter.Verdict.EXCLUDE_AND_PRUNE)
      // The start node itself is denied (filterStartNode): there is no component to report
      return;

    walk.visitedNodes.add(startNode.getIdentity());
    if (startVerdict.includes && minLevel == 0)
      reachableNodes.add(startNode);

    List<Vertex> frontier = new ArrayList<>();
    if (startVerdict.continues)
      frontier.add(startNode);

    for (int level = 0; level < maxLevel && !frontier.isEmpty() && reachableNodes.size() < limit; ++level) {
      walk.startLevel(level + 1);

      // The adjacency entries are walked on the RIDs they hold, so no record is read to follow one. The outgoing list and then the
      // incoming one, the order the single pass over BOTH read them in: each list is walked by an iterator that can name the edge of
      // its entry, which a pass over both lists cannot, and the walk is the same entry for entry. THE INCOMING SIDE OF A
      // UNIDIRECTIONAL EDGE TYPE IS ON THE OTHER END ONLY (ISSUE #8629): READ FROM THE EDGES THAT END IN THE VERTEX
      final String[] outTypes = relationships.typesFor(level, Vertex.DIRECTION.OUT);
      final String[] inTypes = relationships.typesFor(level, Vertex.DIRECTION.IN);
      final boolean incomingSideMissing =
          inTypes != null && IncomingEdgeLookup.isNeeded(context, database, Vertex.DIRECTION.IN, inTypes);

      for (final Vertex current : frontier) {
        if (reachableNodes.size() >= limit)
          break;

        if (outTypes != null)
          walk.followEntries(current, Vertex.DIRECTION.OUT, outTypes);

        if (inTypes == null)
          continue;
        if (incomingSideMissing)
          walk.followEdges(Vertex.DIRECTION.IN, edgesOf(context, current, Vertex.DIRECTION.IN, inTypes).iterator());
        else
          walk.followEntries(current, Vertex.DIRECTION.IN, inTypes);
      }

      frontier = walk.endLevel();
    }

    if (reachableEdges != null)
      collectRelationshipsBetween(reachableNodes, reachableEdges);
  }

  /**
   * Every relationship, of any type, whose two ends are among {@code nodes}. Each is found once, from its source.
   * <p>
   * The edges are read off the adjacency lists knowing their endpoints, so a relationship to a node outside the set is
   * turned away on the RID in the list entry and its record is never read: the cost of this pass is the out-degree of the
   * reported nodes in entries, not in records. The record of a relationship that is kept is read here, which is what finds an
   * entry whose edge record is gone: it is reported and skipped, as in every other path walk.
   */
  private static void collectRelationshipsBetween(final List<Vertex> nodes, final List<Edge> edges) {
    if (nodes.isEmpty())
      return;

    final RidHashSet nodeIds = new RidHashSet(Math.max(16, nodes.size() * 2));
    for (final Vertex node : nodes)
      nodeIds.add(node.getIdentity());

    for (final Vertex node : nodes) {
      final Iterator<Edge> outgoing = node instanceof VertexInternal internal ?
          ((DatabaseInternal) node.getDatabase()).getGraphEngine().getEdgesKnowingEndpoints(internal, Vertex.DIRECTION.OUT) :
          node.getEdges(Vertex.DIRECTION.OUT).iterator();
      while (outgoing.hasNext()) {
        try {
          final Edge edge = outgoing.next();
          final RID target = edge.getIn();
          // The record is read now, for the edges that are kept: an entry whose edge record is gone is not noticed by
          // following the list, and the caller must not be handed a relationship it cannot read
          if (target != null && nodeIds.contains(target))
            edges.add(edge.asEdge(true));
        } catch (final RecordNotFoundException e) {
          GhostEdgeReporter.reportSkipped(e);
        }
      }
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
  protected static Vertex resolveNeighbor(final RID neighborId, final RidHashSet ghostNodes) {
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
   * The state of one {@link #collectReachableComponent} walk, and the following of the adjacency entries of the nodes of a level.
   * <p>
   * A neighbour is reached by an adjacency entry only if the edge that entry names still has its record (issue #10148): the walk
   * decides reachability on the same evidence as the relationships it reports and as the walks that read the edge
   * ({@code path.expandConfig}, {@code path.spanningTree}), so every procedure of the family agrees on the component. A ghost
   * entry does not spend the node it names, so another, live, entry reaches it later. The check comes after the test for a node
   * met already, which turns away the large majority of the entries and costs a set probe, so it is paid once per node reached and
   * not once per entry, and it asks the bucket about the edge RID the entry holds: no record is read.
   * <p>
   * A neighbour is spent the moment it is first reached, before the filters have their say: one turned away is not a new node when
   * the walk meets it again, even where a label sequence would welcome it. APOC's node-global uniqueness is recorded when a branch
   * is created, ahead of the evaluators, and answers the same. A RID with no record behind it is spent too, and reported once, by
   * {@link #resolveNeighbor}. The vertex record is loaded only once all of that has passed, which is what keeps the walk's loads
   * proportional to the component rather than to its adjacency entries.
   */
  private static final class ComponentWalk {
    private final Database     database;
    private final Filters      filters;
    private final int          minLevel;
    private final int          limit;
    private final List<Vertex> reachableNodes;
    private final RidHashSet   visitedNodes = new RidHashSet();
    private final RidHashSet   ghostNodes    = new RidHashSet(16);
    private       List<Vertex> nextFrontier;
    private       int          depth;

    private ComponentWalk(final Database database, final Filters filters, final int minLevel, final int limit,
        final List<Vertex> reachableNodes) {
      this.database = database;
      this.filters = filters;
      this.minLevel = minLevel;
      this.limit = limit;
      this.reachableNodes = reachableNodes;
    }

    /** Starts the level {@code depth} hops from the start node: the nodes it reaches are its frontier. */
    private void startLevel(final int depth) {
      this.depth = depth;
      this.nextFrontier = new ArrayList<>();
    }

    /** Ends the level, answering the nodes it reached that the walk goes on from. */
    private List<Vertex> endLevel() {
      return nextFrontier;
    }

    /** Follows the entries of the adjacency list of {@code current} in {@code direction} (outgoing or incoming) over {@code types}. */
    private void followEntries(final Vertex current, final Vertex.DIRECTION direction, final String[] types) {
      if (!(current instanceof VertexInternal internal)) {
        followEdges(direction, current.getEdges(direction, types).iterator());
        return;
      }

      final EdgeEntryRIDIterator entries = ((DatabaseInternal) database).getGraphEngine()
          .getConnectedVertexEntries(internal, direction, types);
      while (entries.hasNext() && reachableNodes.size() < limit) {
        final RID neighborId = entries.next();
        // Before any set is touched: RidHashSet reads the bucket and offset off the RID without checking it for null
        if (neighborId == null || visitedNodes.contains(neighborId))
          continue;
        // The edge pointer of the entry is decoded now, for the nodes not met yet: the walk skips it otherwise
        reach(entries.getEdgeRID(), neighborId);
      }
    }

    /**
     * Follows edges that were read as records (those of an incoming side stored on the other end only): the far end is read off
     * the edge, which for a record loaded lazily is what finds one whose record is gone.
     */
    private void followEdges(final Vertex.DIRECTION direction, final Iterator<Edge> edges) {
      while (edges.hasNext() && reachableNodes.size() < limit) {
        final RID edgeId;
        final RID neighborId;
        try {
          final Edge edge = edges.next();
          neighborId = direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
          edgeId = edge.getIdentity();
        } catch (final RecordNotFoundException e) {
          GhostEdgeReporter.reportSkipped(e);
          continue;
        }
        if (neighborId == null || visitedNodes.contains(neighborId))
          continue;
        reach(edgeId, neighborId);
      }
    }

    /** Reaches {@code neighborId} through the entry naming {@code edgeId}, unless that edge is a ghost or the filters turn it away. */
    private void reach(final RID edgeId, final RID neighborId) {
      if (!GhostEdgeReporter.isLiveEntry(database, edgeId))
        return;

      if (!visitedNodes.add(neighborId))
        return;

      final LabelFilter.Verdict verdict = filters.evaluate(database, neighborId, depth);
      if (verdict == LabelFilter.Verdict.EXCLUDE_AND_PRUNE)
        return;

      final Vertex neighbor = resolveNeighbor(neighborId, ghostNodes);
      if (neighbor == null)
        return;

      if (verdict.includes && depth >= minLevel)
        reachableNodes.add(neighbor);
      if (verdict.continues)
        nextFrontier.add(neighbor);
    }
  }
}
