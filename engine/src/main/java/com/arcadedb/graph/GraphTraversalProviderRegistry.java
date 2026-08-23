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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.log.LogManager;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Registry for {@link GraphTraversalProvider}s, keyed by {@link Database}.
 * <p>
 * The query planner queries this registry to find providers that can accelerate
 * graph traversals for a given database. Uses a {@link WeakHashMap} so that
 * entries for closed databases are eligible for GC after all providers are unregistered.
 * <p>
 * <b>Lifecycle note:</b> Registered providers (e.g. {@code GraphAnalyticalView}) typically
 * hold a strong reference back to the Database, which prevents the WeakHashMap key from being
 * GC-collected while any provider is registered. This is intentional — providers are explicitly
 * unregistered via {@link #unregister} during {@code drop()}/{@code shutdown()}, which removes
 * the strong reference chain and allows GC. The WeakHashMap acts as a safety net for any
 * leaked entries, not as the primary cleanup mechanism.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphTraversalProviderRegistry {
  private static final WeakHashMap<Database, CopyOnWriteArrayList<GraphTraversalProvider>> REGISTRY = new WeakHashMap<>();

  // Fast-path flag: when false, findProvider() returns null without acquiring the lock.
  // Updated under synchronized(REGISTRY) on every register/unregister/clearAll.
  // TOCTOU note: the volatile read in findProvider() can be transiently stale (e.g., a provider
  // was just registered but hasAnyProviders still reads false). This is acceptable because the
  // consequence is a missed optimization opportunity for a single query, not a correctness issue —
  // the next query will see the updated flag. The alternative (always locking) would add contention
  // on every query plan compilation across all databases, even when no providers exist.
  private static volatile boolean hasAnyProviders = false;

  /**
   * Registers a traversal provider for a database.
   */
  public static void register(final Database database, final GraphTraversalProvider provider) {
    final Database key = unwrap(database);
    synchronized (REGISTRY) {
      REGISTRY.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(provider);
      hasAnyProviders = true;
    }
  }

  /**
   * Unregisters a traversal provider from a database.
   */
  public static void unregister(final Database database, final GraphTraversalProvider provider) {
    final Database key = unwrap(database);
    synchronized (REGISTRY) {
      final CopyOnWriteArrayList<GraphTraversalProvider> list = REGISTRY.get(key);
      if (list != null) {
        list.remove(provider);
        if (list.isEmpty())
          REGISTRY.remove(key);
      }
      hasAnyProviders = !REGISTRY.isEmpty();
    }
  }

  /**
   * Returns all registered providers for a database (unmodifiable snapshot).
   */
  public static List<GraphTraversalProvider> getProviders(final Database database) {
    if (!hasAnyProviders)
      return Collections.emptyList();
    final Database key = unwrap(database);
    synchronized (REGISTRY) {
      final CopyOnWriteArrayList<GraphTraversalProvider> list = REGISTRY.get(key);
      // CopyOnWriteArrayList's iterator already returns a snapshot — no need to copy into a new ArrayList
      return list != null ? Collections.unmodifiableList(list) : Collections.emptyList();
    }
  }

  /**
   * Finds the first ready provider that covers all the given edge types.
   *
   * @param database  the database
   * @param edgeTypes the edge types needed (null or empty = all types)
   * @return a matching ready provider, or null if none found
   */
  public static GraphTraversalProvider findProvider(final Database database, final String... edgeTypes) {
    // Fast path: single volatile read avoids lock, unwrap, and WeakHashMap lookup
    // when no providers are registered (the common case for most databases)
    if (!hasAnyProviders)
      return null;

    final CopyOnWriteArrayList<GraphTraversalProvider> list;
    synchronized (REGISTRY) {
      list = REGISTRY.get(unwrap(database));
    }
    if (list == null)
      return null;
    // CopyOnWriteArrayList iteration is safe outside the lock. Loop condition (not an explicit break/return
    // in the body) stops at the first match, so isReady() - which now dispatches a GraphAnalyticalView's
    // deferred restore-from-disk as a side effect, see #6641 - is never called on a provider past that point.
    GraphTraversalProvider found = null;
    final Iterator<GraphTraversalProvider> iterator = list.iterator();
    while (found == null && iterator.hasNext()) {
      final GraphTraversalProvider provider = iterator.next();
      // Type coverage first, readiness second: coversEdgeType() is a pure, side-effect-free config check,
      // while isReady()'s dispatch is not. Checking coverage first means isReady() - and its cost - only
      // ever runs on a provider that could actually be selected, not on every registered one #6632's
      // "a view a session never actually needs shouldn't cost anything" goal for a multi-view database.
      final boolean covers;
      if (edgeTypes == null || edgeTypes.length == 0)
        covers = provider.coversEdgeType(null);
      else {
        boolean allCovered = true;
        for (final String et : edgeTypes)
          if (!provider.coversEdgeType(et)) {
            allCovered = false;
            break;
          }
        covers = allCovered;
      }
      if (covers && provider.isReady())
        found = provider;
    }
    if (found != null && found.isStale())
      LogManager.instance().log(GraphTraversalProviderRegistry.class, Level.FINE,
          "Using stale GraphTraversalProvider '%s' for query acceleration (data may not reflect latest commits)", found.getName());
    return found;
  }

  /**
   * Waits until all registered providers for a database are ready (or the timeout expires).
   * <p>
   * Use this after opening a database with persisted GAVs to ensure all CSR structures
   * are built before running performance-critical queries.
   * <pre>
   *   Database db = new DatabaseFactory(path).open();
   *   GraphTraversalProviderRegistry.awaitAll(db, 60, TimeUnit.SECONDS);
   *   // Now all GAVs are ready — queries will use CSR acceleration
   * </pre>
   *
   * @return true if all providers became ready within the timeout, false if some timed out
   */
  public static boolean awaitAll(final Database database, final long timeout, final TimeUnit unit) {
    if (!hasAnyProviders)
      return true;
    final CopyOnWriteArrayList<GraphTraversalProvider> list;
    synchronized (REGISTRY) {
      list = REGISTRY.get(unwrap(database));
    }
    if (list == null || list.isEmpty())
      return true;

    final long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    for (final GraphTraversalProvider provider : list) {
      // A GraphAnalyticalView always goes through awaitReady(), regardless of isReady(): isReady() is
      // accurate (see #6641 - it reports not-ready while a deferred restore-from-disk, #6632, is still
      // unresolved rather than optimistically READY) but deliberately non-blocking, and this method's
      // whole point is to actually wait for the deferred read to resolve, not just to ask whether it
      // already has. awaitReady() is the one call that both triggers that read and blocks for it. It is
      // cheap/idempotent once nothing is pending (its own trigger call no-ops and the wait loop returns
      // immediately), so this costs nothing extra for an already-settled view.
      if (provider instanceof GraphAnalyticalView) {
        final long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0)
          return false;
        if (!((GraphAnalyticalView) provider).awaitReady(remainingNanos, TimeUnit.NANOSECONDS))
          return false;
        continue;
      }
      if (provider.isReady())
        continue;
    }
    return true;
  }

  /**
   * Removes all providers for a database.
   */
  public static void clearAll(final Database database) {
    synchronized (REGISTRY) {
      REGISTRY.remove(unwrap(database));
      hasAnyProviders = !REGISTRY.isEmpty();
    }
  }

  private static Database unwrap(final Database database) {
    return DatabaseInternal.unwrap(database);
  }
}
