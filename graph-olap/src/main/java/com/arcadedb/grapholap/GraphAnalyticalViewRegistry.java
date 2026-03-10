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
package com.arcadedb.grapholap;

import com.arcadedb.database.Database;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that associates named {@link GraphAnalyticalView} instances with databases.
 * <p>
 * Uses a {@link WeakHashMap} keyed by {@link Database}, so GAV references are automatically
 * cleaned up when the database is garbage-collected. Within each database, views are stored
 * in a {@link ConcurrentHashMap} keyed by name for thread-safe access.
 * <p>
 * The registry is used by the builder when a name is specified, and by the query planner
 * to discover available GAVs for a database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAnalyticalViewRegistry {
  private static final Map<Database, ConcurrentHashMap<String, GraphAnalyticalView>> REGISTRY =
      Collections.synchronizedMap(new WeakHashMap<>());

  /**
   * Registers a named GAV for a database.
   *
   * @throws IllegalArgumentException if a GAV with the same name is already registered
   */
  public static void register(final Database database, final String name, final GraphAnalyticalView view) {
    final ConcurrentHashMap<String, GraphAnalyticalView> views =
        REGISTRY.computeIfAbsent(database, k -> new ConcurrentHashMap<>());
    final GraphAnalyticalView existing = views.putIfAbsent(name, view);
    if (existing != null)
      throw new IllegalArgumentException("GraphAnalyticalView '" + name + "' is already registered");
  }

  /**
   * Returns the named GAV for a database, or null if not registered.
   */
  public static GraphAnalyticalView get(final Database database, final String name) {
    final ConcurrentHashMap<String, GraphAnalyticalView> views = REGISTRY.get(database);
    return views != null ? views.get(name) : null;
  }

  /**
   * Returns all registered GAVs for a database (unmodifiable).
   */
  public static Map<String, GraphAnalyticalView> getAll(final Database database) {
    final ConcurrentHashMap<String, GraphAnalyticalView> views = REGISTRY.get(database);
    return views != null ? Collections.unmodifiableMap(views) : Collections.emptyMap();
  }

  /**
   * Returns all registered GAVs for a database that are ready for use.
   */
  public static Map<String, GraphAnalyticalView> getReady(final Database database) {
    final ConcurrentHashMap<String, GraphAnalyticalView> views = REGISTRY.get(database);
    if (views == null)
      return Collections.emptyMap();
    final ConcurrentHashMap<String, GraphAnalyticalView> ready = new ConcurrentHashMap<>();
    for (final Map.Entry<String, GraphAnalyticalView> entry : views.entrySet())
      if (entry.getValue().isReady())
        ready.put(entry.getKey(), entry.getValue());
    return Collections.unmodifiableMap(ready);
  }

  /**
   * Unregisters a named GAV.
   */
  public static void unregister(final Database database, final String name) {
    final ConcurrentHashMap<String, GraphAnalyticalView> views = REGISTRY.get(database);
    if (views != null) {
      views.remove(name);
      if (views.isEmpty())
        REGISTRY.remove(database);
    }
  }

  /**
   * Closes and unregisters all GAVs for a database.
   */
  public static void closeAll(final Database database) {
    final ConcurrentHashMap<String, GraphAnalyticalView> views = REGISTRY.remove(database);
    if (views != null)
      for (final GraphAnalyticalView view : views.values())
        view.close();
  }
}
