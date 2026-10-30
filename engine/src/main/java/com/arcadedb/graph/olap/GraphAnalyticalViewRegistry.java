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
package com.arcadedb.graph.olap;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.AlreadyExistsException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that associates named {@link GraphAnalyticalView} instances with databases.
 * <p>
 * Keyed by {@link Database}; within each database, views are stored in a {@link HashMap}
 * keyed by name.
 * <p>
 * <b>Lifecycle note:</b> entries are cleaned up <i>explicitly</i>, never by garbage collection.
 * Each {@link GraphAnalyticalView} holds a strong reference back to its owning {@link Database}
 * (issue #4586), so a {@link java.util.WeakHashMap} here would never collect a key while a view
 * is registered - the weak-key contract would be silently defeated by the value-to-key reference.
 * A plain {@link ConcurrentHashMap} is used instead to make the explicit-cleanup contract clear.
 * Cleanup is guaranteed by {@link #shutdownAll}/{@link #dropAll}, invoked from
 * {@code LocalDatabase.closeInternal()} on every database close, and by {@link #unregister} when a
 * single view is dropped. The open-database set itself ({@code DatabaseFactory.ACTIVE_INSTANCES})
 * already strongly pins live databases, so this registry can never be the deciding factor for GC.
 * <p>
 * All compound operations are synchronized on the {@code REGISTRY} lock to keep the per-database
 * inner maps consistent under concurrency, so those inner maps do not need to be concurrent.
 * <p>
 * The registry is used by the builder when a name is specified, and by the query planner
 * to discover available GAVs for a database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAnalyticalViewRegistry {
  private static final ConcurrentHashMap<Database, HashMap<String, GraphAnalyticalView>> REGISTRY = new ConcurrentHashMap<>();
  // Why the background build of a view failed, kept after the view left the registry (which frees its name for a new build)
  // so the schema:graphAnalyticalViews row can say FAILED and why, instead of NOT_BUILT or nothing (issue #11011). Guarded by
  // REGISTRY, like the inner maps, and cleaned up with them
  private static final HashMap<Database, HashMap<String, Throwable>>                    FAILURES = new HashMap<>();

  /**
   * Registers a named GAV for a database.
   *
   * @throws IllegalArgumentException if a GAV with the same name is already registered
   */
  public static void register(final Database database, final String name, final GraphAnalyticalView view) {
    synchronized (REGISTRY) {
      final HashMap<String, GraphAnalyticalView> views =
          REGISTRY.computeIfAbsent(unwrap(database), k -> new HashMap<>());
      final GraphAnalyticalView existing = views.putIfAbsent(name, view);
      if (existing != null)
        throw new AlreadyExistsException(AlreadyExistsException.Kind.GRAPH_ANALYTICAL_VIEW,
            "GraphAnalyticalView '" + name + "' is already registered");
      // A new build under the name replaces what the failed one left
      clearFailure(database, name);
    }
  }

  /** Records why the background build of the view {@code name}, no longer registered, failed. */
  public static void recordFailure(final Database database, final String name, final Throwable error) {
    synchronized (REGISTRY) {
      FAILURES.computeIfAbsent(unwrap(database), k -> new HashMap<>()).put(name, error);
    }
  }

  /** Why the background build of the view {@code name} failed, or null when it did not or the view was built since. */
  public static Throwable getFailure(final Database database, final String name) {
    synchronized (REGISTRY) {
      final HashMap<String, Throwable> failures = FAILURES.get(unwrap(database));
      return failures != null ? failures.get(name) : null;
    }
  }

  /** Forgets the failure of the view {@code name}: it is built again, or its definition is gone. */
  public static void clearFailure(final Database database, final String name) {
    synchronized (REGISTRY) {
      final Database unwrapped = unwrap(database);
      final HashMap<String, Throwable> failures = FAILURES.get(unwrapped);
      if (failures != null && failures.remove(name) != null && failures.isEmpty())
        FAILURES.remove(unwrapped);
    }
  }

  /**
   * Returns the named GAV for a database, or null if not registered.
   */
  public static GraphAnalyticalView get(final Database database, final String name) {
    synchronized (REGISTRY) {
      final HashMap<String, GraphAnalyticalView> views = REGISTRY.get(unwrap(database));
      return views != null ? views.get(name) : null;
    }
  }

  /**
   * Returns all registered GAVs for a database (unmodifiable).
   */
  public static Map<String, GraphAnalyticalView> getAll(final Database database) {
    synchronized (REGISTRY) {
      final HashMap<String, GraphAnalyticalView> views = REGISTRY.get(unwrap(database));
      return views != null ? Collections.unmodifiableMap(views) : Collections.emptyMap();
    }
  }

  /**
   * Returns all registered GAVs for a database that are ready for use.
   * <p>
   * Unlike {@link com.arcadedb.graph.GraphTraversalProviderRegistry#findProvider}, this has no coverage
   * filter to check before {@code isReady()} - it means to list every view, not find one usable for a
   * specific query. A future caller relying on this to be a cheap, side-effect-free read should know it
   * is not one: {@code isReady()} dispatches a pending deferred restore-from-disk in the background for
   * every view it is called on (see #6641), so calling this eagerly resolves every registered view's
   * restore, not only the ones that turn out ready.
   */
  public static Map<String, GraphAnalyticalView> getReady(final Database database) {
    synchronized (REGISTRY) {
      final HashMap<String, GraphAnalyticalView> views = REGISTRY.get(unwrap(database));
      if (views == null)
        return Collections.emptyMap();
      final Map<String, GraphAnalyticalView> ready = new HashMap<>();
      for (final Map.Entry<String, GraphAnalyticalView> entry : views.entrySet())
        if (entry.getValue().isReady())
          ready.put(entry.getKey(), entry.getValue());
      return Collections.unmodifiableMap(ready);
    }
  }

  /**
   * Unregisters a named GAV.
   */
  public static void unregister(final Database database, final String name) {
    synchronized (REGISTRY) {
      final Database unwrapped = unwrap(database);
      final HashMap<String, GraphAnalyticalView> views = REGISTRY.get(unwrapped);
      if (views != null) {
        views.remove(name);
        if (views.isEmpty())
          REGISTRY.remove(unwrapped);
      }
    }
  }

  /**
   * Shuts down all GAVs for a database without removing schema definitions.
   * Called during database close to release resources while preserving persistence for next open.
   */
  public static void shutdownAll(final Database database) {
    final HashMap<String, GraphAnalyticalView> views;
    synchronized (REGISTRY) {
      views = REGISTRY.remove(unwrap(database));
      FAILURES.remove(unwrap(database));
    }
    if (views != null)
      for (final GraphAnalyticalView view : views.values())
        view.shutdown();
  }

  /**
   * Drops all GAVs for a database, including their schema definitions.
   */
  public static void dropAll(final Database database) {
    final HashMap<String, GraphAnalyticalView> views;
    synchronized (REGISTRY) {
      views = REGISTRY.remove(unwrap(database));
      FAILURES.remove(unwrap(database));
    }
    if (views != null)
      for (final GraphAnalyticalView view : views.values())
        view.drop();
  }

  /**
   * Removes all registry entries for a database without calling close() on them.
   * Used during restore to clear stale entries from a previous database lifecycle.
   */
  public static void clearAll(final Database database) {
    synchronized (REGISTRY) {
      REGISTRY.remove(unwrap(database));
      FAILURES.remove(unwrap(database));
    }
  }

  private static Database unwrap(final Database database) {
    return DatabaseInternal.unwrap(database);
  }
}
