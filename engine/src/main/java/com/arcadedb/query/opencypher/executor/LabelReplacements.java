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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.LightEdgeRID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.utility.LongRangeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
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
 * <p>
 * <b>What the wider scope costs.</b> The map is keyed by RID, and a RID is a physical position, so a slot freed
 * by the delete inside {@link #replace} could be handed to a record created later - which {@link #resolve} would then
 * answer for with the replacement of the record that used to live there. Inside one transaction that cannot happen: a
 * slot freed by a transaction is not reused by it. Across the committed batches of {@code CALL { ... } IN TRANSACTIONS}
 * it did: the engine handed a slot a committed delete freed to a later insert, a replacement (issue #11764, where two
 * replacement chains could form a cycle), a plain {@code CREATE} of the statement, or a concurrent transaction (issue
 * #12336), and a binding of that new record, read back by a later MATCH, was redirected to the old record's replacement:
 * same RID, same bucket, same type, nothing to tell the two apart. So the RID is never reused while the map can hold it:
 * every key is held in its bucket ({@link LocalBucket#holdPosition}) from the moment it is recorded until the statement
 * ends ({@link #endOfStatement}), and no insert of any transaction takes a held slot. The old bindings keep resolving
 * through the map and a record created later always has a RID of its own, so the map needs no fencing, and the outer
 * rows of a batch no refresh. The holds cost what the map already costs: one entry per replaced record (and per
 * re-attached heavy edge) in its bucket, for the length of the statement, and a page whose next append slot is held is
 * passed over until the statement ends.
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
  // EVERY REPLACEMENT RECORD (VERTEX OR RE-ATTACHED EDGE) TO THE RECORD AT THE START OF ITS CHAIN, AS THE ROWS READ IT: THE
  // IDENTITY A DISTINCT KEYS IT BY, SO A NODE HELD AS THE ORIGINAL BY ONE ROW AND AS A REPLACEMENT BY ANOTHER IS ONE VALUE
  // (ISSUE #10614). A VERTEX MAPS TO A VERTEX AND AN EDGE TO AN EDGE
  private final Map<RID, Identifiable> origins = new HashMap<>();
  // THE SAME FOR A RE-ATTACHED LIGHTWEIGHT EDGE, BY THE OBJECT THE REDIRECT HANDS OUT: ITS IDENTITY IS THE (TYPE, OUT, IN)
  // TRIPLE, WHICH A PARALLEL TWIN CREATED LATER SHARES, SO KEYING IT BY RID WOULD MERGE TWO RELATIONSHIPS UNDER DISTINCT
  private final Map<Edge, Edge>        lightweightOrigins = new IdentityHashMap<>();
  // The context of the statement this map belongs to, which alone ends it (see endOfStatement); null for a private map
  private final CommandContext   owner;
  // The database the held positions live in, set by the first hold
  private       Database         database;
  // Set once the holds were given back: each one is released exactly once, since a hold is counted per holder in the bucket
  private       boolean          positionsReleased;
  // HOW MANY VERTICES {@link #replace} REWROTE: A CHANGE DETECTOR, SO A CALLER RE-WALKS ITS ROW ONLY WHEN SOMETHING MOVED
  private       long             replacements;

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
    final LabelReplacements created = new LabelReplacements(context);
    context.setCachedValue(CONTEXT_KEY, created);
    return created;
  }

  /**
   * A private map, owned by no statement: the caller ends it with {@link #releaseHeldPositions()} when nothing can hold one
   * of the replaced RIDs anymore.
   */
  public LabelReplacements() {
    this(null);
  }

  private LabelReplacements(final CommandContext owner) {
    this.owner = owner;
  }

  /**
   * Ends the replacement map of the statement {@code context} runs, if {@code context} is the statement's own: the positions
   * of the records it replaced are handed back to their buckets, for any later insert to reuse. A nested plan's context, which
   * only borrows the enclosing statement's map ({@link #inherit}), ends nothing. Called once the statement has produced every
   * row, when no binding of a replaced record is left to resolve; a statement that writes is drained before it answers.
   */
  public static void endOfStatement(final CommandContext context) {
    if (context != null && context.getCachedValue(CONTEXT_KEY) instanceof LabelReplacements replacements && replacements.owner == context)
      replacements.releaseHeldPositions();
  }

  /**
   * Hands back the position of every record this map replaced, which {@link #replace} held so that no insert could reuse the
   * RID while a binding may still name it (issue #12336). The map itself is left as it is.
   */
  public void releaseHeldPositions() {
    if (database == null || positionsReleased)
      return;
    positionsReleased = true;
    for (final RID rid : vertices.keySet())
      releasePosition(rid);
    for (final RID rid : edges.keySet())
      releasePosition(rid);
  }

  /**
   * The map of the statement {@code context} runs, or {@code null} when no step of it can write a label: what a DISTINCT asks
   * before keying a row (see {@link #distinctKey}), without creating a map no write step asked for. A step that can write a
   * label creates the map when it is first pulled, so it exists before the first row of the statement is keyed.
   */
  public static LabelReplacements existing(final CommandContext context) {
    return context != null && context.getCachedValue(CONTEXT_KEY) instanceof LabelReplacements replacements ? replacements : null;
  }

  /**
   * The value a DISTINCT keys {@code value} by (issue #10614): a replacement record - a relabelled vertex, an edge re-attached
   * to one - is keyed by the RID of the record its chain started from, inside a list, a map or a path too, and everything else
   * is {@code value} itself. A row projected before a later row's label write holds the original, a row projected after it the
   * replacement: one node, which Neo4j answers once, but the DISTINCT told the two apart by RID - and before any SKIP or LIMIT,
   * which a second pass over the answered rows could not undo. Keying by the origin matches the original too, since a DISTINCT
   * reads a record, and a path, by RID. A value holding no replacement is answered as it is, so the keys a DISTINCT built
   * before the first replacement, or before this map existed, still compare equal to the ones it builds after.
   */
  @SuppressWarnings("PMD.CompareObjectsWithEquals") // identity says whether the element was replaced: a copy is built only then
  public Object distinctKey(final Object value) {
    if (origins.isEmpty())
      return value;

    if (value instanceof Vertex || value instanceof Edge) {
      final Identifiable origin = originOf((Identifiable) value);
      return origin != null ? origin.getIdentity() : value;
    }

    if (value instanceof TraversalPath path) {
      // Rebuilt around the original records, whose RIDs are what a path compares and hashes by
      final List<Vertex> pathVertices = path.getVertices();
      final List<Edge> pathEdges = path.getEdges();
      boolean moved = false;
      final List<Vertex> keyVertices = new ArrayList<>(pathVertices.size());
      for (final Vertex vertex : pathVertices) {
        final Identifiable origin = originOf(vertex);
        moved |= origin != null;
        keyVertices.add(origin != null ? (Vertex) origin : vertex);
      }
      final List<Edge> keyEdges = new ArrayList<>(pathEdges.size());
      for (final Edge edge : pathEdges) {
        final Identifiable origin = originOf(edge);
        moved |= origin != null;
        keyEdges.add(origin != null ? (Edge) origin : edge);
      }
      if (!moved || keyVertices.isEmpty())
        return value;
      final TraversalPath key = new TraversalPath(keyVertices.get(0));
      for (int i = 0; i < keyEdges.size(); i++)
        key.addStep(keyEdges.get(i), keyVertices.get(i + 1));
      return key;
    }

    // A lazy range holds numbers only: walking it would materialise what it exists not to
    if (value instanceof List<?> list && !(value instanceof LongRangeList)) {
      List<Object> rebuilt = null;
      for (int i = 0; i < list.size(); i++) {
        final Object element = list.get(i);
        final Object key = distinctKey(element);
        if (key != element && rebuilt == null)
          rebuilt = new ArrayList<>(list);
        if (rebuilt != null)
          rebuilt.set(i, key);
      }
      return rebuilt != null ? rebuilt : value;
    }

    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> rebuilt = null;
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        final Object element = entry.getValue();
        final Object key = distinctKey(element);
        if (key != element && rebuilt == null)
          rebuilt = new LinkedHashMap<>(map);
        if (rebuilt != null)
          rebuilt.put(entry.getKey(), key);
      }
      return rebuilt != null ? rebuilt : value;
    }

    return value;
  }

  /**
   * Points every row a statement is about to answer at the live records, when the statement replaced any (issue #10614).
   * A row leaves the write step as soon as it is written, so a LATER row's label write can delete a record it still holds:
   * a row waiting in a barrier further down (an ORDER BY, a DISTINCT, the drain of the statement itself) kept the deleted
   * vertex and the deleted re-attached edges of its path, and the client was answered with records that no longer exist -
   * the Bolt serializer failed on them with a {@code RecordNotFoundException}. Called once the statement has produced every
   * row and before {@link #endOfStatement}, while the map still answers for the replaced RIDs. Free for a statement that
   * replaced nothing: no map was ever created, or it is empty.
   */
  public static void redirectRows(final CommandContext context, final Iterable<? extends Result> rows) {
    final LabelReplacements replacements = existing(context);
    if (replacements == null || replacements.isEmpty())
      return;
    for (final Result row : rows)
      replacements.redirect(row);
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

  /**
   * Increases every time a vertex is replaced, and never goes back (a {@link #restore} does not rewind it): a caller that
   * compares two readings knows whether anything moved in between, and skips the walk of a row when nothing did.
   */
  public long getReplacementCount() {
    return replacements;
  }

  /**
   * The live form of a single value - a vertex, an edge, or a path, list or map holding them - or the value itself when
   * nothing inside it was replaced. What {@link #redirect} does for each value of a row, for a caller binding one value.
   */
  public Object resolveValue(final Object value) {
    return vertices.isEmpty() ? value : redirectValue(value);
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
    return new Snapshot(new HashMap<>(vertices), new HashMap<>(edges), new HashMap<>(origins), new IdentityHashMap<>(lightweightOrigins));
  }

  /**
   * Restores the replacement state to the given snapshot, discarding any entries recorded since it was
   * taken - the counterpart of {@code QueryStatistics.restore(snapshot)}.
   */
  public void restore(final Snapshot snapshot) {
    // The rolled-back attempt deleted nothing in the end, so the positions it held are given back
    if (database != null && !positionsReleased) {
      for (final RID rid : vertices.keySet())
        if (!snapshot.vertices.containsKey(rid))
          releasePosition(rid);
      for (final RID rid : edges.keySet())
        if (!snapshot.edges.containsKey(rid))
          releasePosition(rid);
    }
    vertices.clear();
    vertices.putAll(snapshot.vertices);
    edges.clear();
    edges.putAll(snapshot.edges);
    // The replacements of the rolled-back attempt never existed: their RIDs may be handed to unrelated records later
    origins.clear();
    origins.putAll(snapshot.origins);
    lightweightOrigins.clear();
    lightweightOrigins.putAll(snapshot.lightweightOrigins);
  }

  /**
   * Opaque, immutable capture of {@link #vertices}/{@link #edges} at one point in time. The constructor is
   * private and {@link #copy()} always defensively copies, so a caller can never hold a snapshot that
   * changes under it as later replacements are recorded.
   */
  public static final class Snapshot {
    private final Map<RID, Vertex> vertices;
    private final Map<RID, Edge>   edges;
    private final Map<RID, Identifiable> origins;
    private final Map<Edge, Edge>        lightweightOrigins;

    private Snapshot(final Map<RID, Vertex> vertices, final Map<RID, Edge> edges, final Map<RID, Identifiable> origins,
        final Map<Edge, Edge> lightweightOrigins) {
      this.vertices = vertices;
      this.edges = edges;
      this.origins = origins;
      this.lightweightOrigins = lightweightOrigins;
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
   * Follows the replacement chain of an edge and returns the live record, or the argument itself when it was never
   * re-attached. An edge moves once per endpoint relabelled: relabelling its source re-creates it, and relabelling its
   * target later re-creates that copy again, so the first replacement is itself deleted by then and a path redirected to it
   * reported a relationship incident to neither of its live endpoints (issue #11763).
   */
  public Edge resolveEdge(final Edge edge) {
    if (edge == null || edges.isEmpty())
      return edge;
    final RID rid = edge.getIdentity();
    if (rid == null)
      return edge;
    Edge current = edges.get(rid);
    if (current == null)
      return edge;
    Edge next;
    while ((next = edges.get(current.getIdentity())) != null)
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
    // The ResultInternal cast below is unguarded where SubqueryStep.refreshDocumentBindings guards its own, and
    // deliberately so rather than by oversight. Both plan shapes feed the write steps ResultInternal rows -
    // the optimizer is not disabled by a REMOVE/SET after the MATCH, but its NodeByLabelScan/ExpandAll chain
    // materializes ResultInternal, and the one Result that is not (GAVResult, from the fused GAV chain) has
    // no path into a label write today. Should one appear, failing loudly is the answer that matches what
    // this method is for: skipping silently would leave the row pointing at the record the write deleted,
    // which is the defect the class exists to prevent, whereas the refresh next door is an optimisation that
    // is allowed to decline.
    for (final String name : row.getPropertyNames()) {
      final Object value = row.getProperty(name);
      final Object live = redirectValue(value);
      if (live != value)
        ((ResultInternal) row).setProperty(name, live);
    }
    // A RETURN of a single node or relationship also carries it as the row's element, which is what a client is answered
    // with (the HTTP JSON flattens the row through it): it follows the move too (issue #10614)
    if (row.isElement() && row instanceof ResultInternal internal) {
      final Document element = internal.getElement().orElse(null);
      final Object live = redirectValue(element);
      // redirectValue answers a vertex with a vertex and an edge with an edge, and any other document with itself
      if (live != element)
        internal.setElement((Document) live);
    }
  }

  /**
   * Returns the live form of a row value, or the value itself - by identity, so the caller can tell whether
   * anything moved - when nothing inside it was replaced.
   */
  private Object redirectValue(final Object value) {
    if (value instanceof Vertex vertex)
      return resolve(vertex);

    if (value instanceof Edge edge)
      return resolveEdge(edge);

    if (value instanceof TraversalPath path)
      return redirectPath(path);

    // A lazy range holds numbers only, and every row served after a label write is walked: it is not materialised for that
    if (value instanceof List<?> list && !(value instanceof LongRangeList)) {
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
   * Rewrites {@code matched} under {@code newTypeName}, carrying over its properties and its edges (with their own
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
   * <p>
   * The copy is taken from the latest committed record, not from the image the row read (issue #8538): a vertex deleted
   * concurrently since that read therefore fails the write with a retryable {@link ConcurrentModificationException},
   * rather than being resurrected from a stale copy.
   *
   * @return the vertex that now holds the identity of the original
   */
  public MutableVertex replace(final Vertex matched, final String newTypeName) {
    // #8538: the rewrite copies the vertex's properties and deletes it, so it must copy the LATEST committed record,
    // not the image the row read: a commit landing in between would otherwise vanish with the deleted original, and the
    // delete, taken on a page loaded fresh, has no older image left to refuse it against. modify() pins the page and
    // reloads a record whose page moved on, so a commit landing AFTER this point fails the commit-time version check.
    final Vertex vertex;
    try {
      vertex = matched.modify();
    } catch (final RecordNotFoundException e) {
      // A retryable conflict, not a hard error: the retry re-runs the statement against the committed delete (a MERGE
      // then takes its ON CREATE branch)
      throw new ConcurrentModificationException(
          "Vertex " + matched.getIdentity() + " was deleted by a concurrent transaction before its label write. Please retry the operation");
    }
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

    final MutableVertex newVertex = copyOf(database, newTypeName, vertex);

    long migratedEdges = 0;
    // Outgoing first, so a self-loop is migrated exactly once: the incoming pass below skips it rather than
    // re-creating it as a second edge between the same pair.
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT)) {
      final RID in = edge.getIn();
      final Identifiable target = originalRid.equals(in) ? newVertex : in;
      track(edge, newVertex.newEdge(edge.getTypeName(), target, propertiesOf(edge)));
      ++migratedEdges;
    }
    final boolean anyUnidirectional = database.getSchema().hasUnidirectionalEdgeTypes();
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN)) {
      // A pointer a bulk load stored for a unidirectional type is migrated with the edges below, which answer for it
      if (originalRid.equals(edge.getOut()) || (anyUnidirectional && isUnidirectional(database, edge)))
        continue;
      track(edge, edge.getVertex(Vertex.DIRECTION.OUT).newEdge(edge.getTypeName(), newVertex, propertiesOf(edge)));
      ++migratedEdges;
    }
    // The edges of a unidirectional type that end in the vertex are stored on their source only: the delete below would
    // drop them, so they are migrated like the others (issue #8629). The same edges the delete finds.
    if (anyUnidirectional)
      for (final Edge edge : IncomingEdgeLookup.getIncomingUnidirectionalEdges((DatabaseInternal) database, originalRid)) {
        if (originalRid.equals(edge.getOut()))
          continue;
        track(edge, edge.getVertex(Vertex.DIRECTION.OUT).newEdge(edge.getTypeName(), newVertex, propertiesOf(edge)));
        ++migratedEdges;
      }

    vertex.delete();
    if (vertices.put(originalRid, newVertex) == null)
      holdPosition(database, originalRid);
    final Identifiable vertexOrigin = originOf(vertex);
    origins.put(newVertex.getIdentity(), vertexOrigin != null ? vertexOrigin : vertex);
    ++replacements;

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
    final Database database = vertex.getDatabase();
    if (!database.getSchema().hasUnidirectionalEdgeTypes()) {
      for (final RID neighbour : vertex.getConnectedVertexRIDs(Vertex.DIRECTION.IN))
        if (!originalRid.equals(neighbour))
          ++count;
      return count;
    }
    // The incoming edges of a unidirectional type are counted from the edges that end in the vertex, and a pointer a
    // bulk load stored for one is left out of the stored ones, as the migration does (issue #8629)
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN))
      if (!originalRid.equals(edge.getOut()) && !isUnidirectional(database, edge))
        ++count;
    for (final Edge edge : IncomingEdgeLookup.getIncomingUnidirectionalEdges((DatabaseInternal) database, originalRid))
      if (!originalRid.equals(edge.getOut()))
        ++count;
    return count;
  }

  private static boolean isUnidirectional(final Database database, final Edge edge) {
    return database.getSchema().getTypeByBucketId(edge.getIdentity().getBucketId()) instanceof EdgeType type
        && !type.isBidirectional();
  }

  private void track(final Edge original, final Edge replacement) {
    final RID rid = original.getIdentity();
    // Every edge reached through a vertex is keyed here, lightweight ones included: a lightweight edge has no
    // record and therefore no address, but it does have an identity - its LightEdgeRID carries the
    // (type, out vertex, in vertex) triple and hashes on it, so it keys this map exactly like the address of a
    // heavy edge does, and the re-attached copy hanging off the replacement vertex is a different key rather than
    // the same one. Only the null is guarded: an edge that came out of getEdges() always has a bucket.
    if (rid == null)
      return;
    if (edges.put(rid, replacement) == null)
      holdPosition(original.getDatabase(), rid);
    final Identifiable origin = originOf(original);
    final Identifiable root = origin != null ? origin : original;
    final RID replacementRid = replacement.getIdentity();
    if (replacementRid instanceof LightEdgeRID)
      lightweightOrigins.put(replacement, (Edge) root);
    else if (replacementRid != null)
      origins.put(replacementRid, root);
  }

  /**
   * The record at the start of the replacement chain {@code record} belongs to, or {@code null} when it is no replacement.
   */
  private Identifiable originOf(final Identifiable record) {
    final RID rid = record.getIdentity();
    if (rid instanceof LightEdgeRID)
      return record instanceof Edge edge ? lightweightOrigins.get(edge) : null;
    return rid != null ? origins.get(rid) : null;
  }

  /**
   * Keeps the slot of a replaced record from being handed to a new record until the statement ends (issue #12336): the map
   * answers for its RID, so the RID must keep naming that record alone. A lightweight edge has no slot to hold: its identity
   * is the (type, out vertex, in vertex) triple, and the re-attached copy names the new vertex.
   */
  private void holdPosition(final Database database, final RID rid) {
    if (rid.getPosition() < 0 || positionsReleased)
      return;
    final LocalBucket bucket = bucketOf(database, rid);
    if (bucket == null)
      return;
    this.database = database;
    bucket.holdPosition(rid.getPosition());
  }

  private void releasePosition(final RID rid) {
    if (rid.getPosition() < 0)
      return;
    final LocalBucket bucket = bucketOf(database, rid);
    if (bucket != null)
      bucket.releaseHeldPosition(rid.getPosition());
  }

  private static LocalBucket bucketOf(final Database database, final RID rid) {
    final Bucket bucket = database.getSchema().getBucketByIdIfExists(rid.getBucketId());
    return bucket instanceof LocalBucket localBucket ? localBucket : null;
  }

  /**
   * A new vertex of {@code typeName} carrying {@code source}'s properties. It never lands on the slot of a record this
   * statement replaced: those are held ({@link #holdPosition}), so the RID keeps naming the record the map answers for.
   */
  private static MutableVertex copyOf(final Database database, final String typeName, final Vertex source) {
    final MutableVertex created = database.newVertex(typeName);
    for (final String property : source.getPropertyNames())
      created.set(property, source.get(property));
    return created.save();
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
