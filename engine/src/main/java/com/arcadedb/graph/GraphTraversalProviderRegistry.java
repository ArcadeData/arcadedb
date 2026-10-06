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
import com.arcadedb.database.TransactionContext;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.log.LogManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
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

  /** How often {@link #awaitRestoring} looks again at the restore and runs the caller's abort check. */
  private static final long RESTORE_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

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
   * Returns true if any provider is registered for any database, without the lock/unwrap/WeakHashMap lookup
   * {@link #findProvider} needs once it establishes that. Lets a caller that would otherwise do non-trivial work
   * (e.g. building an edge-types array) just to pass it to {@link #findProvider} skip that work first in the
   * overwhelmingly common case where no provider is registered at all - the same single-volatile-read short
   * circuit {@link #findProvider} itself takes, exposed for callers that need it before they can call that method.
   */
  public static boolean hasAnyProviders() {
    return hasAnyProviders;
  }

  /**
   * Finds the first ready provider that covers all the given edge types <b>and every vertex type</b>.
   * <p>
   * A provider built over a subset of the vertex types maps only the vertices of those types, so the edges that leave
   * it are missing from its adjacency: a traversal handed such a provider answers the covered part of the graph (issue
   * #9301: 0 rows where the edges exist). Callers walking edges therefore get only a provider that can answer for
   * every vertex the walk can reach; a caller that handles an uncovered vertex type itself asks for
   * {@link #findProviderAllowingPartialVertexCoverage} instead.
   * <p>
   * None is returned while the calling thread's transaction on {@code database} holds uncommitted changes: a
   * provider serves the committed graph only, so a query reading through it would not see its own transaction's
   * writes - a count push-down counted the committed edges and missed the one the transaction had just created
   * (found with issue #8335). Every caller already falls back to the records when no provider is found.
   *
   * @param database  the database
   * @param edgeTypes the edge types needed (null or empty = all types)
   * @return a matching ready provider, or null if none found
   */
  public static GraphTraversalProvider findProvider(final Database database, final String... edgeTypes) {
    return find(database, true, edgeTypes);
  }

  /**
   * Like {@link #findProvider}, but the provider may cover only some of the vertex types. For a caller that checks the
   * vertex types it walks against {@link GraphTraversalProvider#coversVertexType(String)} (or reads the records of a
   * vertex the provider does not map) itself; every other caller must use {@link #findProvider}.
   */
  public static GraphTraversalProvider findProviderAllowingPartialVertexCoverage(final Database database,
      final String... edgeTypes) {
    return find(database, false, edgeTypes);
  }

  private static GraphTraversalProvider find(final Database database, final boolean requireAllVertexTypes,
      final String[] edgeTypes) {
    // Fast path: single volatile read avoids lock, unwrap, and WeakHashMap lookup
    // when no providers are registered (the common case for most databases)
    if (!hasAnyProviders)
      return null;

    if (hasUncommittedChanges(database))
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
      if (coversEdgeTypes(provider, edgeTypes) && (!requireAllVertexTypes || provider.coversVertexType(null))
          && provider.isReady())
        found = provider;
    }
    if (found != null && found.isStale())
      LogManager.instance().log(GraphTraversalProviderRegistry.class, Level.FINE,
          "Using stale GraphTraversalProvider '%s' for query acceleration (data may not reflect latest commits)", found.getName());
    return found;
  }

  /**
   * Whether {@code provider} covers every requested edge type; {@code null} or empty asks for the provider's
   * whole-graph coverage. A pure configuration check with no side effects, unlike {@link GraphTraversalProvider#isReady()},
   * which can dispatch a deferred restore - which is why callers check coverage first.
   */
  static boolean coversEdgeTypes(final GraphTraversalProvider provider, final String[] edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return provider.coversEdgeType(null);
    for (final String edgeType : edgeTypes)
      if (!provider.coversEdgeType(edgeType))
        return false;
    return true;
  }

  /**
   * Waits for the providers that could serve a request and are still {@link GraphTraversalProvider#isRestoring()
   * restoring}, so a caller whose first {@link #findProvider} came back empty only because it dispatched a deferred
   * restore can ask again once that restore has settled, instead of falling back to a record-by-record scan.
   * <p>
   * Only a provider that covers every requested edge type and every vertex type is considered: a view that cannot
   * serve the request is neither waited for nor, by this method, touched. The whole-graph fallback in
   * {@code AbstractAlgoProcedure#findReadyProvider} applies the same vertex-type rule, so a view this method skips is
   * never one that lookup would accept. A provider that is merely rebuilding after
   * a commit does not count as restoring. Every provider is waited for under one shared deadline, not one budget per
   * provider, and {@code abortCheck} runs at every poll so the caller's own command timeout and interrupt end the
   * wait (it is expected to throw to abort).
   * <p>
   * Does nothing while the calling thread has uncommitted changes, because {@link #findProvider} refuses to hand out
   * a provider in that case no matter how long it waits.
   *
   * @param edgeTypes  the edge types the request needs; {@code null} or empty for the whole graph
   * @param timeoutMs  the total time to wait, in milliseconds; zero or less does not wait
   * @param abortCheck called between polls; throws to abort the wait; may be null for a wait nothing can abort
   * except its own budget and a thread interrupt (which ends it and is left set for the caller)
   *
   * @return true if at least one covering provider was restoring when the call started; false if there was nothing to
   * wait for. Informational: a restore can end between the caller's first lookup and this call, so callers look
   * again either way
   */
  public static boolean awaitRestoring(final Database database, final String[] edgeTypes, final long timeoutMs,
      final Runnable abortCheck) {
    if (timeoutMs <= 0 || !hasAnyProviders || hasUncommittedChanges(database))
      return false;

    List<GraphTraversalProvider> restoring = null;
    for (final GraphTraversalProvider provider : getProviders(database))
      if (coversEdgeTypes(provider, edgeTypes) && provider.coversVertexType(null) && provider.isRestoring()) {
        if (restoring == null)
          restoring = new ArrayList<>(2);
        restoring.add(provider);
      }
    if (restoring == null)
      return false;

    final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (anyRestoring(restoring)) {
      if (abortCheck != null)
        abortCheck.run();
      final long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0)
        break;
      LockSupport.parkNanos(Math.min(remainingNanos, RESTORE_POLL_NANOS));
      // parkNanos returns at once while the interrupt flag is set. A WorkGuard abort hook consumes the flag and throws,
      // so with it this line is never reached; a hook that leaves the flag alone (or none) would otherwise make the loop
      // spin until the deadline. The flag stays set: the caller sees the interrupt at its next check.
      if (Thread.currentThread().isInterrupted())
        break;
    }
    return true;
  }

  private static boolean anyRestoring(final List<GraphTraversalProvider> providers) {
    for (int i = 0; i < providers.size(); i++)
      if (providers.get(i).isRestoring())
        return true;
    return false;
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

  /**
   * True while {@link #findProvider} withholds every provider from the calling thread: some provider is registered and
   * the thread's transaction on {@code database} holds uncommitted changes. A caller that caches a plan built around a
   * provider must neither reuse nor cache one in that state: a cached plan would keep reading the view past the
   * transaction's writes, and a plan built now, without a view, would keep the acceleration from clean executions.
   */
  public static boolean isWithheld(final Database database) {
    return hasAnyProviders && hasUncommittedChanges(database);
  }

  private static boolean hasUncommittedChanges(final Database database) {
    if (!(database instanceof DatabaseInternal internal))
      return false;
    final TransactionContext transaction = internal.getTransactionIfExists();
    return transaction != null && transaction.isActive() && transaction.hasChanges();
  }

  private static Database unwrap(final Database database) {
    return DatabaseInternal.unwrap(database);
  }
}
