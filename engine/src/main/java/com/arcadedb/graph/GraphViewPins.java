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
package com.arcadedb.graph;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The graph views one statement reads, each pinned at the snapshot it served when the statement first asked (issue #10415).
 * <p>
 * A view publishes a base numbered afresh on every rebuild, so a dense node id is only meaningful in the snapshot that handed it
 * out. A statement reads a view many times - an operator maps a vertex to its id, reads its neighbours, maps them back, and the
 * next operator of the plan takes those ids on - and a rebuild landing between two of those reads made the later ones read the
 * earlier ids as other vertices, without failing. Pinned here, every operator of the statement reads the same snapshot whatever
 * the view does meanwhile: the pin is taken when the statement first reads the view, never at plan time (plans are cached and
 * outlive any snapshot), and dropped with the statement.
 * <p>
 * Held by the context a statement executes with - opened by the SQL {@code SELECT} and {@code MATCH} statements and by every
 * top-level Cypher plan - and shared with the contexts derived from it: sub-queries, nested plans, parallel workers. A statement
 * of a script opens its own, so a statement that follows a {@code COMMIT} reads what was committed. Where no statement opened
 * one, {@link #pin(CommandContext, GraphTraversalProvider)} pins for the caller alone, which keeps that caller's reads together.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GraphViewPins {
  private final ConcurrentHashMap<GraphTraversalProvider, GraphTraversalProvider> pins = new ConcurrentHashMap<>(4);

  /**
   * {@code provider} as this statement reads it: pinned on first use, the same pin on every later one.
   *
   * @return null for a null provider
   */
  public GraphTraversalProvider pin(final GraphTraversalProvider provider) {
    if (provider == null)
      return null;
    final GraphTraversalProvider pinned = pins.get(provider);
    if (pinned != null)
      return pinned;
    final GraphTraversalProvider fresh = pinnedOrLive(provider);
    if (fresh == provider)
      return provider;
    final GraphTraversalProvider raced = pins.putIfAbsent(provider, fresh);
    return raced != null ? raced : fresh;
  }

  /**
   * {@code provider} as the statement {@code context} belongs to reads it, or pinned for the caller alone when no statement
   * opened a scope.
   *
   * @return null for a null provider
   */
  public static GraphTraversalProvider pin(final CommandContext context, final GraphTraversalProvider provider) {
    if (provider == null)
      return null;
    final GraphViewPins pins = context != null ? context.getGraphViewPins() : null;
    return pins != null ? pins.pin(provider) : pinAlone(provider);
  }

  /**
   * {@code provider} pinned for a caller that reads it on its own, outside any statement scope: a count that walks the records
   * and asks the view per vertex, a function evaluated once. Every read of that caller then belongs to one snapshot.
   *
   * @return null for a null provider
   */
  public static GraphTraversalProvider pinAlone(final GraphTraversalProvider provider) {
    return provider != null ? pinnedOrLive(provider) : null;
  }

  /**
   * The provider pinned, or the provider itself when it has nothing to pin yet: a view that was never built has no snapshot,
   * and a read of it fails just as it did before this pin existed. That fallback is not kept in the scope, and needs not be:
   * a caller handed the live view of a view with no snapshot reads nothing from it, every read failing the same way, so a later
   * caller of the statement that pins the view once it is built cannot mix two snapshots. A view whose restore from disk is
   * deferred is not such a view: {@code pinned()} waits for the restore. The only {@link IllegalStateException} {@code pinned()}
   * raises is the one for a view with no snapshot.
   */
  private static GraphTraversalProvider pinnedOrLive(final GraphTraversalProvider provider) {
    try {
      return provider.pinned();
    } catch (final IllegalStateException e) {
      return provider;
    }
  }
}
