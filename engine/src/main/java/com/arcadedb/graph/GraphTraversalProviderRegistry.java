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
import com.arcadedb.log.LogManager;

import java.util.Collections;
import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
    // CopyOnWriteArrayList iteration is safe outside the lock
    for (final GraphTraversalProvider provider : list) {
      if (!provider.isReady())
        continue;
      if (edgeTypes == null || edgeTypes.length == 0) {
        if (provider.coversEdgeType(null)) {
          if (provider.isStale())
            LogManager.instance().log(GraphTraversalProviderRegistry.class, Level.FINE,
                "Using stale GraphTraversalProvider '%s' for query acceleration (data may not reflect latest commits)", provider.getName());
          return provider;
        }
      } else {
        boolean allCovered = true;
        for (final String et : edgeTypes)
          if (!provider.coversEdgeType(et)) {
            allCovered = false;
            break;
          }
        if (allCovered) {
          if (provider.isStale())
            LogManager.instance().log(GraphTraversalProviderRegistry.class, Level.FINE,
                "Using stale GraphTraversalProvider '%s' for query acceleration (data may not reflect latest commits)", provider.getName());
          return provider;
        }
      }
    }
    return null;
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
