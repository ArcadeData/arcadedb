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
import com.arcadedb.query.opencypher.executor.SelfLoops;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;

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
 * No record is loaded, so no read permission is asked by loading one. The answer asks the permissions of the path it
 * replaces instead (see {@link Question}): where that path loads the relationship records (a pattern predicate, a pattern
 * comprehension), each relationship's bucket is asked whether the record is there, which asks the same permission and skips
 * an entry whose record is gone as that path does; where it is refused a labelled type whatever the graph holds (the
 * count push-down), the type is asked up front; and where it has nothing bound to start from and scans (a predicate or a
 * comprehension bound by their last node only) it keeps its path, see {@link #count}.
 */
public final class OneHopPattern {
  /** The answer for a row the edge list cannot decide: the caller evaluates the expression the general way. */
  public static final long NOT_ANSWERED = -1L;

  /** A pattern that never qualifies, so the analysis is cached as a value rather than re-run. */
  static final OneHopPattern UNSUPPORTED = new OneHopPattern(null, null, new String[0]);

  /**
   * What an expression asks of the hop, which fixes what the path it replaces does and therefore which permissions the
   * answer off the edge list has to ask.
   */
  enum Question {
    /** {@code COUNT { }}: how many; the count push-down it replaces loads no relationship record. */
    COUNT(false, false),
    /** {@code EXISTS { }}: whether there is one; its body loads no relationship record either. */
    EXISTS(true, false),
    /** {@code size([...])}: how many; the comprehension it replaces loads every relationship record it walks. */
    SIZE(false, true),
    /** The pattern predicate: whether there is one; the walk it replaces loads the relationship records up to the first match. */
    PREDICATE(true, true);

    private final boolean any;
    private final boolean loadsEdges;

    Question(final boolean any, final boolean loadsEdges) {
      this.any = any;
      this.loadsEdges = loadsEdges;
    }
  }

  /** Edge-list entries between two asks of the statement's deadline and termination: one less than a power of two. */
  private static final long GUARD_INTERVAL_MASK = 1023L;

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
    // Any variable-length relationship, [*1..1] included, is a walk, which skips an edge-list entry whose edge record is gone
    // where the fixed hop's count follows it: the general path answers it
    if (relationship.isVariableLength())
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
   * How many relationships the pattern matches for {@code row}, or for a question that asks whether there is one 1 when it
   * matches one and 0 when it matches none; {@link #NOT_ANSWERED} when the row is not one this can decide: no vertex bound to
   * an end, the far end bound too, a far-end label over the unidirectional lookup, or a shape whose replaced path asks
   * permissions this does not (below).
   * <p>
   * What the answer asks is what the replaced path asks, in both directions, because a user refused there must still be
   * refused and one answered there must not start to be:
   * <ul>
   *   <li>{@link Question#SIZE} and {@link Question#PREDICATE} walk the relationships, each record's presence asked of its
   *   bucket without loading it, which asks the current user's permission to read it and skips an entry whose record is gone
   *   (the far end is never loaded: its label is read off the type of its bucket). That is the walk of a row that binds the
   *   FIRST node. A row that binds the last node only has nothing to start from on the replaced path, which scans the first
   *   node's types (every type when it has no label) and loads the relationships of the vertices it finds: it is refused
   *   for a type that scan reads, and not for a relationship type that scan never walks. A walk off the bound vertex
   *   reproduces neither, so such a row keeps its path.</li>
   *   <li>{@link Question#COUNT} over a labelled node is refused by the count push-down for a type its user cannot read,
   *   whatever the graph holds, so that type is asked up front, for the bodies the push-down takes (see
   *   {@link #checkLabelReadable}).</li>
   *   <li>{@link Question#COUNT} and {@link Question#EXISTS} otherwise load nothing and ask nothing.</li>
   * </ul>
   */
  long count(final Result row, final CommandContext context, final Question question) {
    if (row == null)
      return NOT_ANSWERED;
    Side side = fromFirst;
    Vertex vertex = side != null ? anchorOf(side, row) : null;
    if (vertex == null) {
      if (fromLast == null || question.loadsEdges)
        return NOT_ANSWERED;
      side = fromLast;
      vertex = anchorOf(side, row);
      if (vertex == null)
        return NOT_ANSWERED;
    }

    final boolean any = question.any;
    final long answer;
    if (question.loadsEdges) {
      // THE RELATIONSHIP RIDS ARE READ OFF THE STORED LISTS: THE UNIDIRECTIONAL LOOKUP IS LEFT TO THE GENERAL PATH
      if (!(vertex instanceof VertexInternal internal) || !(vertex.getDatabase() instanceof DatabaseInternal database)
          || IncomingEdgeLookup.isNeeded(context, database, side.direction, edgeTypes))
        return NOT_ANSWERED;
      answer = countPresentEdges(internal, database, side, any, context);
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
      if (question == Question.COUNT)
        checkLabelReadable(vertex, side);
      answer = countLabelled(vertex, side, any, context);
    }
    ROWS_ANSWERED.increment();
    return answer;
  }

  /**
   * Asks the permission the count push-down asks of a labelled node, before anything is read, so the refusal does not depend
   * on whether the graph holds a neighbour of that type (it would be an existence oracle for the type otherwise).
   * <p>
   * The push-down takes a body of exactly one relationship type, not declared unidirectional, and plain labels (one name
   * each, no disjunction), and before it runs it counts the type of each label to tell a type that holds nothing from one
   * that does: counting reads every bucket of the type, subtypes included, which refuses a user who may not read one of
   * them. A body it does not take (several types or none, a unidirectional type, a conjunction or a disjunction of labels)
   * is never counted that way, so it is not asked here either.
   */
  private void checkLabelReadable(final Vertex vertex, final Side side) {
    if (edgeTypes.length != 1 || side.farLabels.size() != 1 || !(vertex.getDatabase() instanceof DatabaseInternal database)
        || IncomingEdgeLookup.isAnyUnidirectional(database.getSchema(), edgeTypes))
      return;
    final DocumentType type = database.getSchema().getTypeOrNull(side.farLabels.getFirst());
    // An edge type named as a label is not counted: it keeps no record when its edges are lightweight
    if (type == null || type.getType() == Edge.RECORD_TYPE)
      return;
    for (final int bucketId : type.getBucketIds(true))
      database.checkPermissionsOnFile(bucketId, SecurityDatabaseUser.ACCESS.READ_RECORD);
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
  private long countLabelled(final Vertex vertex, final Side side, final boolean any, final CommandContext context) {
    final Schema schema = vertex.getDatabase().getSchema();
    final RID self = side.direction == Vertex.DIRECTION.BOTH ? vertex.getIdentity() : null;
    int selfSeen = 0;
    long count = 0;
    long walked = 0;
    WorkGuard guard = null;
    for (final RID neighbor : vertex.getConnectedVertexRIDs(side.direction, edgeTypes)) {
      // A supernode is one row that walks millions of entries: the statement's deadline and its termination are asked now and
      // then (the guard is made only by a walk long enough to need it, so a short one allocates nothing)
      if ((++walked & GUARD_INTERVAL_MASK) == 0) {
        if (guard == null)
          guard = WorkGuard.forCommandDeadline(context);
        guard.check();
      }
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
   * lightweight relationship has no record and is taken as it is. An undirected self loop sits in both lists of its vertex:
   * two of them are read K, L, K, L, so each is told apart by its identity before its presence is asked, not by parity.
   */
  private long countPresentEdges(final VertexInternal vertex, final DatabaseInternal database, final Side side,
      final boolean any, final CommandContext context) {
    final Schema schema = database.getSchema();
    final RID identity = vertex.getIdentity();
    final boolean undirected = side.direction == Vertex.DIRECTION.BOTH;
    long count = 0;
    long walked = 0;
    WorkGuard guard = null;
    final Iterator<Edge> entries = database.getGraphEngine().getEdgesKnowingEndpoints(vertex, side.direction, edgeTypes);
    for (final Iterator<Edge> edges = undirected ? SelfLoops.deduplicatingEdges(entries) : entries; edges.hasNext(); ) {
      final Edge edge = edges.next();
      if ((++walked & GUARD_INTERVAL_MASK) == 0) {
        if (guard == null)
          guard = WorkGuard.forCommandDeadline(context);
        guard.check();
      }
      final RID far = side.direction == Vertex.DIRECTION.IN || (undirected && !identity.equals(edge.getOut())) ?
          edge.getOut() :
          edge.getIn();
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
