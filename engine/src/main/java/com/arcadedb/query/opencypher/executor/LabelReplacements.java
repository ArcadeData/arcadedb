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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.HashMap;
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
 * redirected to its live replacement on every later row, and the write becomes idempotent instead of fatal.
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
   */
  public void redirect(final Result row) {
    if (row == null || vertices.isEmpty())
      return;
    for (final String name : row.getPropertyNames()) {
      final Object value = row.getProperty(name);
      if (value instanceof Vertex vertex) {
        final Vertex live = resolve(vertex);
        if (live != vertex)
          ((ResultInternal) row).setProperty(name, live);
      } else if (value instanceof Edge edge && !edges.isEmpty()) {
        final RID rid = edge.getIdentity();
        final Edge live = rid != null ? edges.get(rid) : null;
        if (live != null && live != edge)
          ((ResultInternal) row).setProperty(name, live);
      }
    }
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
    // A lightweight edge has no record of its own: its RID carries a negative position and stands for the pair of
    // endpoints rather than for an address, so there is no identity to redirect from.
    if (rid != null && rid.getBucketId() > -1 && rid.getPosition() > -1)
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
