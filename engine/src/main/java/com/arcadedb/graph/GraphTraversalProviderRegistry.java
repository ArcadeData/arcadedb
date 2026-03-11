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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry for {@link GraphTraversalProvider}s, keyed by {@link Database}.
 * <p>
 * The query planner queries this registry to find providers that can accelerate
 * graph traversals for a given database. Uses a {@link WeakHashMap} so that
 * providers are automatically cleaned up when the database is garbage-collected.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphTraversalProviderRegistry {
  private static final Map<Database, CopyOnWriteArrayList<GraphTraversalProvider>> REGISTRY =
      Collections.synchronizedMap(new WeakHashMap<>());

  /**
   * Registers a traversal provider for a database.
   */
  public static void register(final Database database, final GraphTraversalProvider provider) {
    REGISTRY.computeIfAbsent(unwrap(database), k -> new CopyOnWriteArrayList<>()).add(provider);
  }

  /**
   * Unregisters a traversal provider from a database.
   */
  public static void unregister(final Database database, final GraphTraversalProvider provider) {
    final CopyOnWriteArrayList<GraphTraversalProvider> list = REGISTRY.get(unwrap(database));
    if (list != null) {
      list.remove(provider);
      if (list.isEmpty())
        REGISTRY.remove(unwrap(database));
    }
  }

  /**
   * Returns all registered providers for a database (unmodifiable snapshot).
   */
  public static List<GraphTraversalProvider> getProviders(final Database database) {
    final CopyOnWriteArrayList<GraphTraversalProvider> list = REGISTRY.get(unwrap(database));
    return list != null ? Collections.unmodifiableList(new ArrayList<>(list)) : Collections.emptyList();
  }

  /**
   * Finds the first ready provider that covers all the given edge types.
   *
   * @param database  the database
   * @param edgeTypes the edge types needed (null or empty = all types)
   * @return a matching ready provider, or null if none found
   */
  public static GraphTraversalProvider findProvider(final Database database, final String... edgeTypes) {
    final CopyOnWriteArrayList<GraphTraversalProvider> list = REGISTRY.get(unwrap(database));
    if (list == null)
      return null;
    for (final GraphTraversalProvider provider : list) {
      if (!provider.isReady())
        continue;
      if (edgeTypes == null || edgeTypes.length == 0) {
        if (provider.coversEdgeType(null))
          return provider;
      } else {
        boolean allCovered = true;
        for (final String et : edgeTypes)
          if (!provider.coversEdgeType(et)) {
            allCovered = false;
            break;
          }
        if (allCovered)
          return provider;
      }
    }
    return null;
  }

  /**
   * Removes all providers for a database.
   */
  public static void clearAll(final Database database) {
    REGISTRY.remove(unwrap(database));
  }

  /**
   * Unwraps a database to its underlying instance (e.g., ServerDatabase → LocalDatabase).
   * Ensures consistent identity for WeakHashMap lookups regardless of wrapper layers.
   */
  private static Database unwrap(final Database database) {
    if (database instanceof DatabaseInternal) {
      final DatabaseInternal internal = ((DatabaseInternal) database).getWrappedDatabaseInstance();
      if (internal != null && internal != database)
        return unwrap(internal);
    }
    return database;
  }
}
