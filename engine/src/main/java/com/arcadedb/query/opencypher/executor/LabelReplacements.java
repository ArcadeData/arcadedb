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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * Carries out a Cypher label write and remembers what it displaced.
 * <p>
 * ArcadeDB derives a record's type from the bucket it lives in, so there is no in-place retype: changing the label
 * set of a vertex means writing a new record under the new type and deleting the old one. Both the new vertex and
 * every edge re-attached to it get fresh RIDs, which leaves every other reference to the original - the same node
 * bound to a second alias in the row, the same node reached again by a later row of the same clause - pointing at a
 * record that no longer exists. Touching one of those fails with {@code RecordNotFoundException}, either directly or
 * as an "edge list not fully visible" complaint from the vertex delete that follows its dangling edge-list chunk
 * (issues #6312 and #6313).
 * <p>
 * One instance is held per running <b>statement</b> - {@link #of(CommandContext)} keeps it on the command context -
 * so a node replaced while processing one row is redirected to its live replacement on every later row, and the write
 * becomes idempotent instead of fatal. The edges re-attached on the way are tracked the same, lightweight ones
 * included: they have no record to address, but their identity is the (type, out vertex, in vertex) triple, and that
 * is what moved.
 * <p>
 * The scope is the statement and not the step because a {@code CALL { }} body is <b>re-planned for every outer
 * row</b> ({@code SubqueryStep.executeInnerQuery} builds a fresh {@code CypherExecutionPlan} each time): a per-step
 * instance there lives for exactly one row, so the second outer row met the vertex the first one had deleted and the
 * label write followed a RID that was gone (issue #6977). {@link #inherit} is what carries the enclosing statement's
 * map into such a nested plan's own context.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LabelReplacements {
  private static final Object[] NO_PROPERTIES = new Object[0];
  /**
   * Where {@link #of} parks the statement-wide instance. Held as a context <i>cached value</i> rather than as a
   * context variable: cached values are engine-internal, while variables are reachable from a query through
   * {@code $CONTEXT} and fall back to the database's global variables when missing.
   */
  private static final String   CONTEXT_KEY   = "$cypher.labelReplacements";

  private final Map<RID, Vertex> vertices = new HashMap<>();
  private final Map<RID, Edge>   edges    = new HashMap<>();

  /**
   * The replacement map of the statement {@code context} belongs to, created on first use.
   * <p>
   * Every label write of one statement shares it: the three steps that perform one ({@code SET}, {@code REMOVE} and
   * {@code MERGE}'s {@code ON CREATE}/{@code ON MATCH SET}) and, through {@link #inherit}, every plan nested inside
   * it. Allocated only when a write step actually asks for it, so a read-only statement never builds one.
   * <p>
   * A {@code null} context has no statement to share through, so the caller gets a private map rather than an
   * exception: it is still correct for a single step, only unshared. No call site passes one today - every step
   * that asks is constructed with the plan's context - and a new one that did would get the pre-#6977 behaviour
   * back for itself, which is why the fallback is spelled out here rather than left to be inferred.
   */
  public static LabelReplacements of(final CommandContext context) {
    if (context == null)
      return new LabelReplacements();
    if (context.getCachedValue(CONTEXT_KEY) instanceof LabelReplacements existing)
      return existing;
    final LabelReplacements created = new LabelReplacements();
    context.setCachedValue(CONTEXT_KEY, created);
    return created;
  }

  /**
   * Makes a nested plan's context answer {@link #of} with the enclosing statement's map, so a label write inside a
   * {@code CALL { }} body is seen by the next outer row even though the body is re-planned for each one.
   */
  public static void inherit(final CommandContext inner, final CommandContext outer) {
    if (inner == null || outer == null)
      return;
    inner.setCachedValue(CONTEXT_KEY, of(outer));
  }

  public boolean isEmpty() {
    return vertices.isEmpty();
  }

  /**
   * Returns an independent snapshot of the current replacement state.
   * <p>
   * Mirrors {@code QueryStatistics.copy()}: a step that retries its own auto-commit mini-transaction
   * (issue #6367) needs to undo whatever a failed, rolled-back attempt recorded here before re-running,
   * exactly as it resets its statistics counters, and for the same reason - {@link #replace} both
   * writes records (a new vertex, its re-attached edges, the deleted original) and remembers the move
   * in {@link #vertices}/{@link #edges} in the same call. A transaction rollback undoes the writes but
   * leaves an untouched Java map still pointing at a vertex that no longer exists, which {@link #resolve}
   * would then hand back to the retried attempt as if it were live.
   */
  public Snapshot copy() {
    return new Snapshot(new HashMap<>(vertices), new HashMap<>(edges));
  }

  /**
   * Restores the replacement state to the given snapshot, discarding any entries recorded since it was
   * taken - the counterpart of {@code QueryStatistics.restore(snapshot)}.
   */
  public void restore(final Snapshot snapshot) {
    vertices.clear();
    vertices.putAll(snapshot.vertices);
    edges.clear();
    edges.putAll(snapshot.edges);
  }

  /**
   * Opaque, immutable capture of {@link #vertices}/{@link #edges} at one point in time. The constructor is
   * private and {@link #copy()} always defensively copies, so a caller can never hold a snapshot that
   * changes under it as later replacements are recorded.
   */
  public static final class Snapshot {
    private final Map<RID, Vertex> vertices;
    private final Map<RID, Edge>   edges;

    private Snapshot(final Map<RID, Vertex> vertices, final Map<RID, Edge> edges) {
      this.vertices = vertices;
      this.edges = edges;
    }
  }

  /**
   * Follows the replacement chain of a vertex - a node can be relabelled more than once in the same clause - and
   * returns the live record, or the argument itself when it was never replaced.
   */
  public Vertex resolve(final Vertex vertex) {
    if (vertex == null || vertices.isEmpty())
      return vertex;
    final RID rid = vertex.getIdentity();
    if (rid == null)
      return vertex;
    Vertex current = vertices.get(rid);
    if (current == null)
      return vertex;
    Vertex next;
    while ((next = vertices.get(current.getIdentity())) != null)
      current = next;
    return current;
  }

  /**
   * Points every alias of a row that still refers to a replaced vertex or to one of its re-attached edges at the
   * live record. Called before a row is written to and again after each replacement, so all aliases of the same
   * node observe the same state.
   * <p>
   * A row does not hold records only under their own name: a path variable, a {@code collect()} list or a map holds
   * them too, and one left pointing at the deleted original answers with the state the node had before the write -
   * the labels it no longer has - or fails outright the moment something re-reads it. Those are rebuilt around the
   * live records, and only when something inside them actually moved.
   * <p>
   * The descent into collections is charged to every row of the step once any row has triggered a replacement, not
   * only to the rows that carry the replaced node - the map is the only thing that knows a node moved, and it is
   * step-wide. That is a deliberate trade: a clause with no label write at all pays a single
   * {@link Map#isEmpty()} check per row and never walks anything, and correctness after a write is not something a
   * large {@code collect()} list should be able to opt out of.
   */
  public void redirect(final Result row) {
    if (row == null || vertices.isEmpty())
      return;
    for (final String name : row.getPropertyNames()) {
      final Object value = row.getProperty(name);
      final Object live = redirectValue(value);
      if (live != value)
        ((ResultInternal) row).setProperty(name, live);
    }
  }

  /**
   * Returns the live form of a row value, or the value itself - by identity, so the caller can tell whether
   * anything moved - when nothing inside it was replaced.
   */
  private Object redirectValue(final Object value) {
    if (value instanceof Vertex vertex)
      return resolve(vertex);

    if (value instanceof Edge edge) {
      if (edges.isEmpty())
        return value;
      final RID rid = edge.getIdentity();
      final Edge live = rid != null ? edges.get(rid) : null;
      return live != null ? live : value;
    }

    if (value instanceof TraversalPath path)
      return redirectPath(path);

    if (value instanceof List<?> list) {
      List<Object> rebuilt = null;
      for (int i = 0; i < list.size(); i++) {
        final Object element = list.get(i);
        final Object live = redirectValue(element);
        if (live != element && rebuilt == null)
          rebuilt = new ArrayList<>(list);
        if (rebuilt != null)
          rebuilt.set(i, live);
      }
      return rebuilt != null ? rebuilt : value;
    }

    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> rebuilt = null;
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        final Object element = entry.getValue();
        final Object live = redirectValue(element);
        if (live != element && rebuilt == null)
          rebuilt = new LinkedHashMap<>(map);
        if (rebuilt != null)
          rebuilt.put(entry.getKey(), live);
      }
      return rebuilt != null ? rebuilt : value;
    }

    return value;
  }

  /**
   * Rebuilds a path around the live records when one of its members was replaced, keeping its shape.
   */
  private TraversalPath redirectPath(final TraversalPath path) {
    final List<Vertex> pathVertices = path.getVertices();
    if (pathVertices.isEmpty())
      return path;
    final List<Edge> pathEdges = path.getEdges();

    boolean moved = false;
    final List<Vertex> liveVertices = new ArrayList<>(pathVertices.size());
    for (final Vertex vertex : pathVertices) {
      final Vertex live = resolve(vertex);
      moved |= live != vertex;
      liveVertices.add(live);
    }
    final List<Edge> liveEdges = new ArrayList<>(pathEdges.size());
    for (final Edge edge : pathEdges) {
      final Object live = redirectValue(edge);
      moved |= live != edge;
      liveEdges.add((Edge) live);
    }
    if (!moved)
      return path;

    final TraversalPath rebuilt = new TraversalPath(liveVertices.get(0));
    for (int i = 0; i < liveEdges.size(); i++)
      rebuilt.addStep(liveEdges.get(i), liveVertices.get(i + 1));
    return rebuilt;
  }

  /**
   * Rewrites {@code vertex} under {@code newTypeName}, carrying over its properties and its edges (with their own
   * properties, which a plain re-link would silently drop), deletes the original and records the replacement.
   * <p>
   * <b>This is a write proportional to the vertex's degree, and it changes RIDs.</b> Every incident edge is
   * re-created in both directions, which also mutates each neighbour's edge list, so {@code SET n:Label} on a
   * vertex with a million edges rewrites a million edge records for what the user wrote as a metadata change - and
   * it undoes the locality the striping and append-merge paths build. The vertex's RID changes and so does every
   * edge's, which leaves anything holding one across the write - an application-side reference, a stored link from
   * a non-graph type, an external index, a client that cached an earlier result - pointing at a deleted record. The
   * query's own row follows the move ({@link #redirect}); nothing can make an outside reference follow it.
   * <p>
   * None of that is fixable while a label is a type and a type is a bucket, so the two knobs of issue #6335 make it
   * visible rather than silent: {@code arcadedb.opencypher.labelWriteDegreeWarning} logs what the write cost, and
   * {@code arcadedb.opencypher.labelWriteDegreeLimit} (off by default) refuses it outright above a degree, before
   * any record has moved.
   *
   * @return the vertex that now holds the identity of the original
   */
  public MutableVertex replace(final Vertex vertex, final String newTypeName) {
    final Database database = vertex.getDatabase();
    final RID originalRid = vertex.getIdentity();
    final String originalTypeName = vertex.getTypeName();

    final int limit = database.getConfiguration().getValueAsInteger(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT);
    if (limit > 0) {
      final long degree = countEdgesToMigrate(vertex, originalRid);
      if (degree > limit)
        throw new CommandExecutionException(
            "Label write on " + originalRid + " (" + originalTypeName + " -> " + newTypeName + ") rejected: the vertex has "
                + degree + " edges and arcadedb.opencypher.labelWriteDegreeLimit is " + limit
                + ". A label change rewrites the vertex and every one of its edges under the new type, so it costs a write "
                + "per edge and changes every RID involved. Model the distinction as a property, or raise the limit if the "
                + "cost is acceptable");
    }

    final MutableVertex newVertex = database.newVertex(newTypeName);
    for (final String property : vertex.getPropertyNames())
      newVertex.set(property, vertex.get(property));
    newVertex.save();

    long migratedEdges = 0;
    // Outgoing first, so a self-loop is migrated exactly once: the incoming pass below skips it rather than
    // re-creating it as a second edge between the same pair.
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT)) {
      final RID in = edge.getIn();
      final Identifiable target = originalRid.equals(in) ? newVertex : in;
      track(edge, newVertex.newEdge(edge.getTypeName(), target, propertiesOf(edge)));
      ++migratedEdges;
    }
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN)) {
      if (originalRid.equals(edge.getOut()))
        continue;
      track(edge, edge.getVertex(Vertex.DIRECTION.OUT).newEdge(edge.getTypeName(), newVertex, propertiesOf(edge)));
      ++migratedEdges;
    }

    vertex.delete();
    vertices.put(originalRid, newVertex);

    // Counted while migrating rather than measured up front: the count is exact and costs nothing, and a report of
    // what the write actually did is what explains a stall that has already happened.
    final int warnAbove = database.getConfiguration()
        .getValueAsInteger(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_WARNING);
    if (warnAbove > 0 && migratedEdges >= warnAbove)
      LogManager.instance().log(this, Level.WARNING,
          "Cypher label write on %s (%s -> %s) rewrote the vertex and %d incident edges: a label change moves the record to "
              + "a new type, so it is O(degree) and every RID involved (the vertex and all its edges) changed. Set "
              + "arcadedb.opencypher.labelWriteDegreeWarning to 0 to silence this, or "
              + "arcadedb.opencypher.labelWriteDegreeLimit to refuse it",
          originalRid, originalTypeName, newTypeName, migratedEdges);

    return newVertex;
  }

  /**
   * The number of edges {@link #replace} would re-create, counted the same way the migration counts them so that the
   * limit refuses on the number the warning would have reported.
   * <p>
   * Not {@code countEdges(BOTH)}: that sums the two edge lists independently, so a self-loop - which is in both -
   * counts twice, while the migration deliberately re-creates it once. The IN pass walks the connected RIDs rather
   * than the edges, so the skip costs a list walk and not a record load per edge.
   */
  private static long countEdgesToMigrate(final Vertex vertex, final RID originalRid) {
    long count = vertex.countEdges(Vertex.DIRECTION.OUT);
    for (final RID neighbour : vertex.getConnectedVertexRIDs(Vertex.DIRECTION.IN))
      if (!originalRid.equals(neighbour))
        ++count;
    return count;
  }

  private void track(final Edge original, final Edge replacement) {
    final RID rid = original.getIdentity();
    // Every edge reached through a vertex is keyed here, lightweight ones included: a lightweight edge has no
    // record and therefore no address, but it does have an identity - its LightEdgeRID carries the
    // (type, out vertex, in vertex) triple and hashes on it, so it keys this map exactly like the address of a
    // heavy edge does, and the re-attached copy hanging off the replacement vertex is a different key rather than
    // the same one. Only the null is guarded: an edge that came out of getEdges() always has a bucket.
    if (rid != null)
      edges.put(rid, replacement);
  }

  /**
   * Flattens an edge's properties into the alternating name/value array {@code Vertex.newEdge()} expects. Returns
   * an empty array for a lightweight edge, whose type rejects any property at all.
   */
  private static Object[] propertiesOf(final Edge edge) {
    final Set<String> names = edge.getPropertyNames();
    if (names.isEmpty())
      return NO_PROPERTIES;
    final List<Object> flattened = new ArrayList<>(names.size() * 2);
    for (final String name : names) {
      flattened.add(name);
      flattened.add(edge.get(name));
    }
    return flattened.toArray();
  }
}
