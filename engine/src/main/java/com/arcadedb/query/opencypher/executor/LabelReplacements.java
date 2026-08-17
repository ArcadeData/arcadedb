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

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * One instance is held per running {@code SET}/{@code REMOVE} step, so a node replaced while processing one row is
 * redirected to its live replacement on every later row, and the write becomes idempotent instead of fatal. The
 * edges re-attached on the way are tracked the same, lightweight ones included: they have no record to address, but
 * their identity is the (type, out vertex, in vertex) triple, and that is what moved.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LabelReplacements {
  private static final Object[] NO_PROPERTIES = new Object[0];

  private final Map<RID, Vertex> vertices = new HashMap<>();
  private final Map<RID, Edge>   edges    = new HashMap<>();

  public boolean isEmpty() {
    return vertices.isEmpty();
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
   *
   * @return the vertex that now holds the identity of the original
   */
  public MutableVertex replace(final Vertex vertex, final String newTypeName) {
    final Database database = vertex.getDatabase();
    final RID originalRid = vertex.getIdentity();

    final MutableVertex newVertex = database.newVertex(newTypeName);
    for (final String property : vertex.getPropertyNames())
      newVertex.set(property, vertex.get(property));
    newVertex.save();

    // Outgoing first, so a self-loop is migrated exactly once: the incoming pass below skips it rather than
    // re-creating it as a second edge between the same pair.
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT)) {
      final RID in = edge.getIn();
      final Identifiable target = originalRid.equals(in) ? newVertex : in;
      track(edge, newVertex.newEdge(edge.getTypeName(), target, propertiesOf(edge)));
    }
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN)) {
      if (originalRid.equals(edge.getOut()))
        continue;
      track(edge, edge.getVertex(Vertex.DIRECTION.OUT).newEdge(edge.getTypeName(), newVertex, propertiesOf(edge)));
    }

    vertex.delete();
    vertices.put(originalRid, newVertex);
    return newVertex;
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
