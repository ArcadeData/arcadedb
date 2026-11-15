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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Schema;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * A one-hop pattern that an expression asks about once per row, answered off the edge list of the vertex the row binds
 * (issue #11891): {@code COUNT { (p)-[:K]->() }}, {@code EXISTS { (p)-[:K]->() }}, the pattern predicate
 * {@code WHERE (p)-[:K]->()} and {@code size([(p)-[:K]->(q) | q])}. Each of them otherwise plans and runs a subquery, or
 * walks the edges loading their records, for what is a degree.
 * <p>
 * Only a pattern whose answer is fully decided by the edge list qualifies: one fixed-length hop, no path variable, no
 * relationship variable, no property map or inline {@code WHERE} on the relationship or on either node, no label on the
 * bound node, and a far node whose variable the row does not bind (a bound far node constrains the hop to one vertex).
 * A label on the far node is still checked, on the bucket of the neighbour's RID, so no record is loaded for it. Either
 * end may be the bound one: {@code COUNT { (:P)-[:K]->(p) }} is the in-degree of {@code p}.
 * <p>
 * The counts are the ones a relationship pattern matches: an undirected self loop is one relationship, and the incoming
 * side of a unidirectional edge type comes from the query's lookup (see
 * {@link IncomingEdgeLookup#countPatternEdges}). The edge list is read from the copy of the vertex the transaction holds,
 * so the edges created or deleted earlier in the transaction are counted as they stand.
 * <p>
 * No record is loaded, so no read permission is asked by loading one: where the path this replaces loads the relationship
 * records (a pattern predicate, a pattern comprehension), each relationship's bucket is asked whether the record is there,
 * which asks the same permission and skips an entry whose record is gone as that path does, see {@link #count}.
 */
public final class OneHopPattern {
  /** The answer for a row the edge list cannot decide: the caller evaluates the expression the general way. */
  public static final long NOT_ANSWERED = -1L;

  /** A pattern that never qualifies, so the analysis is cached as a value rather than re-run. */
  static final OneHopPattern UNSUPPORTED = new OneHopPattern(null, null, new String[0]);

  // ROWS ANSWERED HERE SINCE THE JVM STARTED, READ BY THE TESTS THAT CHECK THE FAST PATH IS TAKEN
  private static final LongAdder ROWS_ANSWERED = new LongAdder();

  /**
   * One way of reading the hop: from the vertex bound to {@code anchor}, towards a far node that carries
   * {@code farLabels} (any vertex when empty).
   */
  private record Side(String anchor, String far, Vertex.DIRECTION direction, List<String> farLabels, boolean farDisjunction) {
  }

  private final Side     fromFirst;
  private final Side     fromLast;
  private final String[] edgeTypes;

  private OneHopPattern(final Side fromFirst, final Side fromLast, final String[] edgeTypes) {
    this.fromFirst = fromFirst;
    this.fromLast = fromLast;
    this.edgeTypes = edgeTypes;
  }

  /**
   * The body of a {@code COUNT { }} or {@code EXISTS { }} that is one {@code MATCH} of a qualifying pattern and nothing
   * else, or {@link #UNSUPPORTED}.
   */
  static OneHopPattern ofBody(final CypherStatement body) {
    if (body == null || body instanceof UnionStatement)
      return UNSUPPORTED;
    final List<ClauseEntry> clauses = body.getClausesInOrder();
    if (clauses == null || clauses.size() != 1 || clauses.getFirst().getType() != ClauseEntry.ClauseType.MATCH)
      return UNSUPPORTED;
    if (body.getWhereClause() != null || body.getReturnClause() != null || body.getOrderByClause() != null
        || body.getSkip() != null || body.getLimit() != null)
      return UNSUPPORTED;
    final MatchClause match = clauses.getFirst().getTypedClause();
    if (match.isOptional() || match.hasWhereClause() || match.getPathPatterns().size() != 1)
      return UNSUPPORTED;
    return of(match.getPathPatterns().getFirst());
  }

  /** A qualifying pattern, or {@link #UNSUPPORTED}. */
  static OneHopPattern of(final PathPattern path) {
    if (path == null || path.getClass() != PathPattern.class || path.hasPathVariable() || path.getRelationshipCount() != 1
        || path.getNodeCount() != 2 || path.getEffectivePathMode() == PathMode.ACYCLIC)
      return UNSUPPORTED;

    final RelationshipPattern relationship = path.getRelationship(0);
    if (relationship.getClass() != RelationshipPattern.class || relationship.getVariable() != null
        || relationship.hasProperties() || relationship.hasWhereExpression())
      return UNSUPPORTED;
    // [*1..1] is the hop itself; any other length is a walk
    if (relationship.isVariableLength()
        && (relationship.getEffectiveMinHops() != 1 || relationship.getEffectiveMaxHops() != 1))
      return UNSUPPORTED;

    final NodePattern first = path.getNode(0);
    final NodePattern last = path.getNode(1);
    if (!isPlain(first) || !isPlain(last))
      return UNSUPPORTED;
    // (p)-[:K]->(p) asks for the self loops alone
    if (first.getVariable() != null && first.getVariable().equals(last.getVariable()))
      return UNSUPPORTED;

    final Vertex.DIRECTION direction = relationship.getDirection().toArcadeDirection();
    final Side fromFirst = canAnchor(first) ?
        new Side(first.getVariable(), last.getVariable(), direction, last.getLabels(), last.isLabelDisjunction()) :
        null;
    final Side fromLast = canAnchor(last) ?
        new Side(last.getVariable(), first.getVariable(), reverse(direction), first.getLabels(), first.isLabelDisjunction()) :
        null;
    if (fromFirst == null && fromLast == null)
      return UNSUPPORTED;
    return new OneHopPattern(fromFirst, fromLast, relationship.getTypes().toArray(new String[0]));
  }

  /** A node that constrains nothing but, possibly, its labels. */
  private static boolean isPlain(final NodePattern node) {
    return !node.hasProperties() && !node.hasExplicitProperties() && !node.hasWhereExpression() && !node.hasDynamicLabels()
        && (node.getVariable() == null || !node.getVariable().isEmpty());
  }

  /** A node the hop can be read from: named, so a row can bind it, and constraining nothing on the vertex it binds. */
  private static boolean canAnchor(final NodePattern node) {
    return node.getVariable() != null && !node.hasLabels();
  }

  private static Vertex.DIRECTION reverse(final Vertex.DIRECTION direction) {
    return switch (direction) {
      case OUT -> Vertex.DIRECTION.IN;
      case IN -> Vertex.DIRECTION.OUT;
      default -> direction;
    };
  }

  /**
   * How many relationships the pattern matches for {@code row}, or with {@code any} 1 when it matches one and 0 when it
   * matches none; {@link #NOT_ANSWERED} when the row is not one this can decide (no vertex bound to an end, the far end
   * bound too, or a far-end label over the unidirectional lookup).
   *
   * @param loadsEdges whether the path this replaces loads the relationship records it walks, which asks the current user's
   *                   permission to read their type and skips an entry whose record is gone (it never loads the far end: its
   *                   label is read off the type of its bucket). The relationships are then walked here too, each record's
   *                   presence asked of its bucket without loading it.
   */
  long count(final Result row, final CommandContext context, final boolean any, final boolean loadsEdges) {
    if (row == null)
      return NOT_ANSWERED;
    Side side = fromFirst;
    Vertex vertex = side != null ? anchorOf(side, row) : null;
    if (vertex == null) {
      side = fromLast;
      vertex = side != null ? anchorOf(side, row) : null;
      if (vertex == null)
        return NOT_ANSWERED;
    }

    final long answer;
    if (loadsEdges) {
      // THE RELATIONSHIP RIDS ARE READ OFF THE STORED LISTS: THE UNIDIRECTIONAL LOOKUP IS LEFT TO THE GENERAL PATH
      if (!(vertex instanceof VertexInternal internal) || !(vertex.getDatabase() instanceof DatabaseInternal database)
          || IncomingEdgeLookup.isNeeded(context, database, side.direction, edgeTypes))
        return NOT_ANSWERED;
      answer = countPresentEdges(internal, database, side, any);
    } else if (side.farLabels.isEmpty()) {
      if (any && !IncomingEdgeLookup.isNeeded(context, vertex.getDatabase(), side.direction, edgeTypes))
        answer = vertex.getConnectedVertexRIDs(side.direction, edgeTypes).iterator().hasNext() ? 1L : 0L;
      else {
        final long degree = IncomingEdgeLookup.countPatternEdges(context, vertex, side.direction, edgeTypes);
        answer = any ? Math.min(degree, 1L) : degree;
      }
    } else {
      // THE STORED LISTS HOLD THE NEIGHBOUR RIDS; THE INCOMING SIDE OF A UNIDIRECTIONAL TYPE IS NOT IN THEM
      if (IncomingEdgeLookup.isNeeded(context, vertex.getDatabase(), side.direction, edgeTypes))
        return NOT_ANSWERED;
      answer = countLabelled(vertex, side, any);
    }
    ROWS_ANSWERED.increment();
    return answer;
  }

  private static Vertex anchorOf(final Side side, final Result row) {
    if (!(row.getProperty(side.anchor) instanceof Vertex vertex))
      return null;
    return side.far != null && row.hasProperty(side.far) ? null : vertex;
  }

  /**
   * The neighbours whose type carries the far end's labels, told from the bucket of their RID. An undirected self loop sits
   * in both lists of its vertex and is one relationship, so every second sighting of the vertex itself is skipped.
   */
  private long countLabelled(final Vertex vertex, final Side side, final boolean any) {
    final Schema schema = vertex.getDatabase().getSchema();
    final RID self = side.direction == Vertex.DIRECTION.BOTH ? vertex.getIdentity() : null;
    int selfSeen = 0;
    long count = 0;
    for (final RID neighbor : vertex.getConnectedVertexRIDs(side.direction, edgeTypes)) {
      if (self != null && self.equals(neighbor) && (++selfSeen & 1) == 0)
        continue;
      if (Labels.matches(schema.getTypeByBucketId(neighbor.getBucketId()), side.farLabels, side.farDisjunction)) {
        ++count;
        if (any)
          break;
      }
    }
    return count;
  }

  /**
   * {@link #countLabelled} over the relationships whose record is there, for the paths that load each relationship they walk:
   * an edge-list entry whose edge record is gone (a ghost) is skipped as those paths skip it, and asking the bucket whether
   * the record is there asks the current user's permission to read it, as loading it does. No record is loaded; a
   * lightweight relationship has no record and is taken as it is.
   */
  private long countPresentEdges(final VertexInternal vertex, final DatabaseInternal database, final Side side,
      final boolean any) {
    final Schema schema = database.getSchema();
    final RID identity = vertex.getIdentity();
    final boolean undirected = side.direction == Vertex.DIRECTION.BOTH;
    int selfSeen = 0;
    long count = 0;
    for (final Iterator<Edge> edges = database.getGraphEngine().getEdgesKnowingEndpoints(vertex, side.direction, edgeTypes);
        edges.hasNext(); ) {
      final Edge edge = edges.next();
      final RID far = side.direction == Vertex.DIRECTION.IN || (undirected && !identity.equals(edge.getOut())) ?
          edge.getOut() :
          edge.getIn();
      if (undirected && identity.equals(far) && (++selfSeen & 1) == 0)
        continue;
      final RID edgeRid = edge.getIdentity();
      if (edgeRid.getPosition() >= 0 && !schema.getBucketById(edgeRid.getBucketId()).existsRecord(edgeRid))
        continue;
      if (side.farLabels.isEmpty() || Labels.matches(schema.getTypeByBucketId(far.getBucketId()), side.farLabels,
          side.farDisjunction)) {
        ++count;
        if (any)
          break;
      }
    }
    return count;
  }

  /** The rows answered off an edge list since the JVM started. */
  static long rowsAnswered() {
    return ROWS_ANSWERED.sum();
  }
}
