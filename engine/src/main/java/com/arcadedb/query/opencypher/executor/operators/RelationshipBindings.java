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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeIdentitySet;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;

import java.util.Set;

/** Shared extraction of edge identities from scalar, list, and path relationship bindings. */
final class RelationshipBindings {
  private RelationshipBindings() {
  }

  static EdgeIdentitySet collectEdgeIdentities(final Result row, final Set<String> variables) {
    if (variables == null || variables.isEmpty())
      return null;

    EdgeIdentitySet identities = null;
    for (final String variable : variables)
      identities = addEdges(identities, row.getProperty(variable));
    return identities;
  }

  static boolean overlaps(final EdgeIdentitySet used, final TraversalPath path) {
    if (used == null || path == null)
      return false;
    for (final Edge edge : path.getEdges())
      if (used.contains(edge.getIdentity()))
        return true;
    return false;
  }

  /**
   * Adds one relationship binding to a clause-level set and reports overlap with a different
   * binding. Repeated edges inside the same binding are collapsed first so WALK mode is not
   * mistaken for reuse between separate MATCH pattern relationships.
   */
  static boolean addBindingAndDetectOverlap(final EdgeIdentitySet used, final Object binding) {
    final EdgeIdentitySet bindingEdges = new EdgeIdentitySet();
    boolean overlap = false;
    if (binding instanceof Edge edge)
      return addEdgeAndDetectOverlap(used, bindingEdges, edge);
    if (binding instanceof TraversalPath path)
      for (final Edge edge : path.getEdges())
        overlap |= addEdgeAndDetectOverlap(used, bindingEdges, edge);
    if (binding instanceof Iterable<?> iterable)
      for (final Object item : iterable)
        if (item instanceof Edge edge)
          overlap |= addEdgeAndDetectOverlap(used, bindingEdges, edge);
    return overlap;
  }

  private static EdgeIdentitySet addEdges(EdgeIdentitySet target, final Object binding) {
    if (binding instanceof Edge edge) {
      if (target == null)
        target = new EdgeIdentitySet();
      target.add(edge.getIdentity());
    } else if (binding instanceof TraversalPath path) {
      for (final Edge edge : path.getEdges()) {
        if (target == null)
          target = new EdgeIdentitySet();
        target.add(edge.getIdentity());
      }
    } else if (binding instanceof Iterable<?> iterable) {
      for (final Object item : iterable)
        if (item instanceof Edge edge) {
          if (target == null)
            target = new EdgeIdentitySet();
          target.add(edge.getIdentity());
        }
    }
    return target;
  }

  private static boolean addEdgeAndDetectOverlap(final EdgeIdentitySet used,
      final EdgeIdentitySet bindingEdges, final Edge edge) {
    if (!bindingEdges.add(edge.getIdentity()))
      return false;
    final boolean overlap = used.contains(edge.getIdentity());
    used.add(edge.getIdentity());
    return overlap;
  }
}
