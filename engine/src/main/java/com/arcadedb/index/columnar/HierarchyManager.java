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
package com.arcadedb.index.columnar;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.StripedEdgeList;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.IntHashSet;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * The hierarchy of one COLUMNAR index (one per declaring type and property, see {@link ColumnarIndexManager#hierarchy}): the current
 * {@link HierarchyView} of its tag graph, rebuilt when the graph changes. Never persisted: the first view after an open is built from
 * the graph.
 * <p>
 * <b>Generation.</b> The files of the tag graph are watched ({@link ColumnarIndexManager#watchFiles}): the buckets of
 * {@code vertexType} (polymorphic), their edge-list buckets in {@code parentDirection} and the super-node stripe pools of those types
 * (a lightweight edge writes edge-list pages only), and the buckets of {@code parentEdge} (polymorphic). Every mark on one of them
 * bumps the watcher's generation synchronously on the committing thread (commit, Raft apply, WAL replay), which is the only work of
 * the commit path. {@link #generation()} is that counter plus the number of changes of the watched file set, so it never goes back.
 * <p>
 * <b>Views.</b> A view is labelled with the generation read BEFORE the builder's scans: a change committed during the build bumps it
 * past the label, and the next {@link #view()} rebuilds (the ordering protocol of dirty pages, spec 6.2). A query needs a view at least
 * as new as the generation it read at start; the build is single-flight ({@link #buildLock}), in a READ_COMMITTED transaction of its
 * own (never the caller's: the shared view holds committed state only) and with no user (the shared view is every user's: a caller's
 * result-set limit or read timeout must not truncate it; access to the tag buckets is the facet layer's check). The READ triggers of
 * the tag type therefore also run with no user during a shared build, as they do in the background rebuild (triggers are schema DDL,
 * so the querying user gains nothing from it; bypassing read events for the build is a tuning item). That transaction is
 * nested when the caller has one, so a caller already as deeply nested as allowed gets no view while the shared one needs a rebuild
 * ({@link #NESTING_TOO_DEEP}); it ends without counting as a rollback in the database statistics. A coalesced watcher run, on
 * {@link ColumnarApplyPool}, only queues {@link #rebuildIfStale()} on {@link ColumnarMaintenancePool} (at most one queued), so queries
 * normally find a fresh view; that rebuild counts as a running maintenance job of the database (its close waits for it, and it stops
 * at its next step once the manager is closed) and never waits for a build already running, which queues it again when it ends. A
 * caller whose own transaction changed a watched file gets a private view built through that transaction and the caller's user
 * (its result-set limit applies to it, and to it only), never cached and never logging the cycle WARNING.
 * <p>
 * <b>Sources.</b> The watched file set is recomputed at every rebuild (a stripe pool appears at a super-node's first promotion), when
 * the schema version moved, and on a {@code HIERARCHY_SOURCE_CHANGED} event naming {@code vertexType} or {@code parentEdge}; a change of
 * it (a bucket added to the tag type, another key type) makes the current view stale. A dropped or renamed {@code vertexType},
 * {@code key} or {@code parentEdge} makes the hierarchy unavailable until the index is recreated (the event names only the old type).
 */
public final class HierarchyManager {
  /** Reached by {@link #rebuildIfStale()} once it found the view stale, before it builds (test hook). */
  static final String REBUILD          = "hierarchy.rebuild";
  /** Reached by a scheduled rebuild that found another build running: it does not wait, the running build re-queues it (test hook). */
  static final String REBUILD_DEFERRED = "hierarchy.rebuildDeferred";
  /** Why {@link #view()} has no view for a caller whose transactions are nested as deep as allowed while the view needs a rebuild. */
  public static final String NESTING_TOO_DEEP = "transaction nesting too deep for the hierarchy view";

  /** The watched files, their set for {@link TransactionContext#hasChangesInFiles}, and what a change of the sources is told by. */
  private record FileSet(int[] ids, IntHashSet set, String fingerprint) {
    static final FileSet NONE = new FileSet(new int[0], new IntHashSet(), null);
  }

  private final LocalDatabase        database;
  private final ColumnarIndexManager columnar;
  private final String               typeName;
  private final String               property;
  private final HierarchyDefinition  def;
  private final String               indexName;
  /** The file ids of the bucket indexes of the type index at creation: the index is gone when none is left. */
  private final int[]                indexFileIds;
  private final Runnable             onChange      = this::scheduleRebuild;
  private final ReentrantLock        buildLock     = new ReentrantLock();
  private final Object               sourcesLock   = new Object();
  private final AtomicBoolean        rebuildQueued = new AtomicBoolean();
  /** A scheduled rebuild found {@link #buildLock} busy: the holder queues one more when it releases it. */
  private final AtomicBoolean        rebuildWanted = new AtomicBoolean();
  private final AtomicLong           sourceChanges = new AtomicLong();
  private final AtomicLong           rebuilds      = new AtomicLong();
  private volatile HierarchyView     current;
  private volatile FileSet           files         = FileSet.NONE;
  private volatile long              sourcesSchemaVersion = Long.MIN_VALUE;
  private volatile String            unavailableReason;
  private volatile boolean           closed;

  HierarchyManager(final DatabaseInternal db, final ColumnarIndexManager columnar, final String typeName, final String property,
      final HierarchyDefinition def) {
    this(db, columnar, typeName, property, def, null);
  }

  /** @param invalid why the definition could not be read (null when it could): the manager is then unavailable and watches nothing */
  HierarchyManager(final DatabaseInternal db, final ColumnarIndexManager columnar, final String typeName, final String property,
      final HierarchyDefinition def, final String invalid) {
    this.database = (LocalDatabase) db.getEmbedded();
    this.columnar = columnar;
    this.typeName = typeName;
    this.property = property;
    this.def = def;
    final DocumentType type = database.getSchema().getTypeOrNull(typeName);
    final TypeIndex index = type == null ? null : type.getColumnarIndex(property);
    this.indexName = index == null ? typeName + "." + property : index.getName();
    final List<IndexInternal> subIndexes = index == null ? List.of() : index.getSubIndexes();
    this.indexFileIds = new int[subIndexes.size()];
    for (int i = 0; i < indexFileIds.length; i++)
      indexFileIds[i] = subIndexes.get(i).getFileId();
    if (invalid != null || def == null)
      unavailableReason = invalid != null ? invalid : "no hierarchy definition";
    else
      refreshSources(schemaVersion());
  }

  /**
   * The watched file ids, sorted; empty when the hierarchy is unavailable. A facet reads the hierarchy only for a user who may read
   * the records of every one of them (the shared view is built with no user).
   */
  public int[] watchedFileIds() {
    return files.ids().clone();
  }

  /** The page-change generation of the tag graph: never goes back while the manager is open. */
  long generation() {
    return columnar.watchGeneration(onChange) + sourceChanges.get();
  }

  /**
   * The view for the calling thread's query: a private one, built through the caller's transaction and never cached, when that
   * transaction changed a watched file; otherwise the shared view, rebuilt first when it is older than the generation read now. Null
   * when the hierarchy is not available; {@link #unavailableReason()} then says why (also {@link #NESTING_TOO_DEEP}, for this caller
   * only).
   */
  public HierarchyView view() {
    refreshIfSchemaChanged();
    if (closed || unavailableReason != null)
      return null;
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null && tx.isActive() && tx.hasChangesInFiles(files.set()))
      return HierarchyBuilder.build(database, def, generation(), indexName, false, null, null);
    final long wanted = generation();
    final HierarchyView fresh = current;
    if (fresh != null && fresh.generation() >= wanted)
      return fresh;
    if (isCallerNestedTooDeep())
      return null;
    buildLock.lock();
    try {
      return buildLocked(wanted);
    } finally {
      buildLock.unlock();
      requeueIfWanted();
    }
  }

  /**
   * Builds the shared view when it is older than the generation; the coalesced watcher queues it on the maintenance pool. It never
   * waits for a build already running: that build queues it again when it releases the lock.
   */
  void rebuildIfStale() {
    if (!isAvailable())
      return;
    final long wanted = generation();
    final HierarchyView fresh = current;
    if (fresh != null && fresh.generation() >= wanted)
      return;
    ColumnarTestHooks.reach(REBUILD);
    if (!buildLock.tryLock()) {
      rebuildWanted.set(true);
      // the holder may have released the lock before it could see the flag: one more try
      if (!buildLock.tryLock()) {
        ColumnarTestHooks.reach(REBUILD_DEFERRED);
        return;
      }
      rebuildWanted.set(false);
    }
    try {
      buildLocked(wanted);
    } finally {
      buildLock.unlock();
      requeueIfWanted();
    }
  }

  /** {@code HIERARCHY_SOURCE_CHANGED} naming {@code vertexType} or {@code parentEdge}: sources recomputed, a rebuild queued. */
  void onSchemaEvent(final ColumnarSchemaEvent event) {
    if (closed || def == null || event.kind() != ColumnarSchemaEvent.Kind.HIERARCHY_SOURCE_CHANGED)
      return;
    if (!def.vertexType().equals(event.typeName()) && !def.parentEdge().equals(event.typeName()))
      return;
    refreshSources(schemaVersion());
    scheduleRebuild();
  }

  /** False when a source is gone, the manager is closed, or the calling thread is nested too deep to build the stale shared view. */
  public boolean isAvailable() {
    refreshIfSchemaChanged();
    return unavailableReason == null && !closed && !isCallerNestedTooDeep();
  }

  /** Why the hierarchy is not available to the calling thread, null when it is. */
  public String unavailableReason() {
    refreshIfSchemaChanged();
    final String reason = unavailableReason;
    if (reason != null)
      return reason;
    if (closed)
      return "the hierarchy of index '" + indexName + "' is closed";
    return isCallerNestedTooDeep() ? NESTING_TOO_DEEP : null;
  }

  /** The number of shared views built (test observability; private views are not counted). */
  long rebuildCount() {
    return rebuilds.get();
  }

  /** Stops watching; no view is served or built from now on. Idempotent. */
  void close() {
    synchronized (sourcesLock) {
      closed = true;
      columnar.unwatchFiles(onChange);
    }
    current = null;
  }

  /** Whether the type index this manager belongs to is still in the schema (one of its bucket indexes at creation is left). */
  boolean indexExists() {
    final DocumentType type = database.getSchema().getTypeOrNull(typeName);
    final TypeIndex index = type == null ? null : type.getColumnarIndex(property);
    if (index == null)
      return false;
    for (final IndexInternal sub : index.getSubIndexes())
      for (final int fileId : indexFileIds)
        if (sub.getFileId() == fileId)
          return true;
    return false;
  }

  // ------------------------------------------------------------------------------------------------------------------- internals

  /** Run by the coalesced watcher on {@link ColumnarApplyPool}: queues one rebuild on {@link ColumnarMaintenancePool}, never builds. */
  private void scheduleRebuild() {
    if (closed || unavailableReason != null || !rebuildQueued.compareAndSet(false, true))
      return;
    try {
      ColumnarMaintenancePool.getInstance().getExecutorService().execute(this::runScheduledRebuild);
    } catch (final RejectedExecutionException e) {
      rebuildQueued.set(false);
    }
  }

  private void runScheduledRebuild() {
    // cleared first: a change during this rebuild queues one more
    rebuildQueued.set(false);
    // counted as a running maintenance job: the database close waits for it (it stops at its next step once this manager is closed)
    columnar.runDatabaseMaintenanceBody(() -> {
      try {
        rebuildIfStale();
      } catch (final RuntimeException e) {
        if (!closed)
          LogManager.instance().log(this, Level.WARNING, "Background rebuild of the hierarchy of index '%s' of database '%s' failed", e,
              indexName, database.getName());
      }
    });
  }

  /** After {@link #buildLock} is released: queues the rebuild a scheduled run deferred while the lock was held. */
  private void requeueIfWanted() {
    if (rebuildWanted.getAndSet(false))
      scheduleRebuild();
  }

  /** Under {@link #buildLock}: the shared view at least as new as {@code wanted}, built when the current one is older. */
  private HierarchyView buildLocked(final long wanted) {
    final HierarchyView raced = current;
    if (raced != null && raced.generation() >= wanted)
      return raced;
    // every rebuild recomputes the sources: a stripe pool bucket created at a super-node's first promotion is watched from now on
    refreshSources(schemaVersion());
    if (closed || unavailableReason != null)
      return null;
    // read before the scans: a change committed during the build bumps the generation past the label
    final long generation = generation();
    final HierarchyView previous = current;
    final HierarchyView built = ColumnarApplier.inDatabaseContext(database, () -> {
      final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());
      // no user: the view is every user's, so no user's result-set limit or read timeout may truncate it
      final SecurityDatabaseUser user = context.getCurrentUser();
      context.setCurrentUser(null);
      try {
        // a transaction of its own (nested when the caller has one): the shared view never sees uncommitted state
        database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
        try {
          return HierarchyBuilder.build(database, def, generation, indexName, true, previous, () -> closed);
        } finally {
          // pops only the build's own transaction; not counted as a rollback in the database statistics
          database.rollbackUncounted();
        }
      } finally {
        context.setCurrentUser(user);
      }
    });
    current = built;
    // a close that cleared the view meanwhile wins: nothing is left published in a closed manager
    if (closed) {
      current = null;
      return null;
    }
    rebuilds.incrementAndGet();
    return built;
  }


  /**
   * The calling thread's transactions are nested as deeply as allowed and it would get the shared view, which is stale: its build
   * would need one more nested transaction. A caller that changed a watched file gets a private view, which needs none.
   */
  private boolean isCallerNestedTooDeep() {
    final HierarchyView fresh = current;
    if (fresh != null && fresh.generation() >= generation())
      return false;
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    if (context == null || context.getTransactionDepth() < context.getMaxNested())
      return false;
    final TransactionContext tx = database.getTransactionIfExists();
    return tx != null && tx.isActive() && !tx.hasChangesInFiles(files.set());
  }

  private void refreshIfSchemaChanged() {
    if (closed || unavailableReason != null)
      return;
    final long version = schemaVersion();
    if (version != sourcesSchemaVersion)
      refreshSources(version);
  }

  /**
   * Checks the sources and recomputes the watched file set: a missing source makes the hierarchy unavailable (and stops watching), a
   * new file set or key type replaces the watched files, keeping their generation, and moves {@link #generation()} past the current
   * view.
   *
   * @param version the schema version read before the schema is
   */
  private void refreshSources(final long version) {
    synchronized (sourcesLock) {
      if (closed || unavailableReason != null)
        return;
      final Schema schema = database.getSchema();
      final DocumentType tags = schema.getTypeOrNull(def.vertexType());
      final Property key = tags instanceof VertexType ? tags.getPolymorphicPropertyIfExists(def.key()) : null;
      final DocumentType edges = schema.getTypeOrNull(def.parentEdge());
      final String missing = !(tags instanceof VertexType) ? def.vertexType()
          : key == null ? def.vertexType() + "." + def.key() : !(edges instanceof EdgeType) ? def.parentEdge() : null;
      if (missing != null) {
        unavailableReason = "hierarchy source " + missing + " was renamed or dropped";
        columnar.unwatchFiles(onChange);
        files = FileSet.NONE;
        current = null;
        return;
      }

      final int[] ids = sourceFileIds(schema, tags, edges);
      final String fingerprint = Arrays.toString(ids) + "|" + key.getType();
      final FileSet previous = files;
      if (!fingerprint.equals(previous.fingerprint())) {
        final IntHashSet set = new IntHashSet(ids.length);
        for (final int id : ids)
          set.add(id);
        columnar.replaceWatchedFiles(onChange, ids);
        files = new FileSet(ids, set, fingerprint);
        // the first file set is not a change: no view exists yet
        if (previous.fingerprint() != null)
          sourceChanges.incrementAndGet();
      }
      sourcesSchemaVersion = version;
    }
  }

  /** The buckets of the tag types with their edge-list buckets in parentDirection and stripe pools, and the edge buckets, sorted. */
  private int[] sourceFileIds(final Schema schema, final DocumentType tags, final DocumentType edges) {
    final IntHashSet ids = new IntHashSet();
    final GraphEngine graph = database.getGraphEngine();
    for (final int bucketId : tags.getBucketIds(true)) {
      ids.add(bucketId);
      final Bucket edgeList = schema.getBucketByNameIfExists(graph.getEdgesBucketName(bucketId, def.parentDirection()));
      if (edgeList != null)
        ids.add(edgeList.getFileId());
    }
    addStripePools(schema, tags, database.getConfiguration().getValueAsInteger(GlobalConfiguration.GRAPH_SUPERNODE_STRIPES), ids);
    for (final int bucketId : edges.getBucketIds(true))
      ids.add(bucketId);
    final int[] sorted = ids.toArray();
    Arrays.sort(sorted);
    return sorted;
  }

  private static void addStripePools(final Schema schema, final DocumentType type, final int stripes, final IntHashSet ids) {
    for (final Bucket stripe : StripedEdgeList.getStripePool(schema, type.getName(), stripes))
      if (stripe != null)
        ids.add(stripe.getFileId());
    for (final DocumentType subType : type.getSubTypes())
      addStripePools(schema, subType, stripes, ids);
  }

  private long schemaVersion() {
    return database.getSchema().getEmbedded().getVersion();
  }
}
