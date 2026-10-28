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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The hierarchy manager of a COLUMNAR index: a view rebuilt on the page-change generation of the tag graph's files (vertex buckets,
 * their edge-list buckets, the edge buckets), single-flight, in the background on the maintenance pool or on demand; a private view for
 * a transaction that changed the tag graph itself; unavailable once a hierarchy source is renamed or dropped.
 */
class HierarchyManagerTest extends TestHelper {
  private static final long         SEED       = 4242L;
  private static final List<String> TAG_VALUES = List.of("a/b/c", "a/d", "zzz");
  private static final String       INDEX_NAME = ColumnarTestData.TYPE + "[tagsbyitem]_columnar";
  private static final String       CYCLE_LOG  = "on a cycle of parent links";
  /** A hang detector, never a latency bound. */
  private static final long         HANG_MS    = 10_000L;

  private ColumnarTestData data;
  private HierarchyManager manager;

  @Override
  protected void beginTest() {
    data = new ColumnarTestData(SEED);
    data.tagHierarchy = hierarchyJSON();
    data.tagValues = TAG_VALUES;
    data.createSchema(database);
    data.insertAssets(database, 30);
    database.transaction(() -> {
      for (final String path : List.of("a", "a/b", "a/b/c", "a/d", "x"))
        database.newVertex(ColumnarTestData.TAG_TYPE).set("path", path).save();
    });
    database.transaction(() -> {
      edge("a", "a/b");
      edge("a/b", "a/b/c");
      edge("a", "a/d");
    });
    manager = columnar().hierarchy(ColumnarTestData.TYPE, "tags");
    assertThat(manager).isNotNull();
  }

  @Test
  void committedEdgeChangeIsVisibleToTheNextView() {
    final HierarchyView before = manager.view();
    assertThat(parentValues(before, "a/d")).containsExactly("a");
    database.transaction(() -> reparent("a/d", "x"));
    // no wait: the generation was bumped by the commit's marks before the commit returned
    final HierarchyView after = manager.view();
    assertThat(after.generation()).isGreaterThan(before.generation());
    assertThat(parentValues(after, "a/d")).containsExactly("x");
  }

  @Test
  void lightweightEdgeChangeBumpsGeneration() {
    database.command("sql", "CREATE EDGE TYPE LightParentOf LIGHTWEIGHT");
    database.transaction(() -> {
      tag("a").newEdge("LightParentOf", tag("a/d"));
      // x gets its OUT list now, so the edge below writes edge-list pages only, never the vertex record
      tag("x").newEdge("LightParentOf", tag("a/b"));
    });
    final HierarchyManager light = new HierarchyManager((DatabaseInternal) database, columnar(), ColumnarTestData.TYPE, "tags",
        new HierarchyDefinition(ColumnarTestData.TAG_TYPE, "path", "LightParentOf", Vertex.DIRECTION.IN));
    try {
      final HierarchyView before = light.view();
      assertThat(parentValues(before, "a/d")).containsExactly("a");

      final Set<Integer> marked = ConcurrentHashMap.newKeySet();
      ColumnarIndexManager.onMarkForTesting = marked::add;
      try {
        database.transaction(() -> tag("x").newEdge("LightParentOf", tag("a/d")));
      } finally {
        ColumnarIndexManager.onMarkForTesting = null;
      }
      final int tagBucket = database.getSchema().getType(ColumnarTestData.TAG_TYPE).getBucketIds(false).getFirst();
      final int inEdges = database.getSchema()
          .getBucketByName(((DatabaseInternal) database).getGraphEngine().getEdgesBucketName(tagBucket, Vertex.DIRECTION.IN)).getFileId();
      // only edge-list pages changed: the bump comes from watching them
      assertThat(marked).doesNotContain(tagBucket).contains(inEdges);
      assertThat(light.generation()).isGreaterThan(before.generation());
      assertThat(parentValues(light.view(), "a/d")).containsExactly("a", "x");
    } finally {
      light.close();
    }
  }

  @Test
  void tagKeyRenameRebuilds() {
    final HierarchyView before = manager.view();
    database.transaction(() -> database.command("sql", "UPDATE Tag SET path = 'a/e' WHERE path = 'a/d'"));
    final HierarchyView after = manager.view();
    assertThat(after.generation()).isGreaterThan(before.generation());
    assertThat(after.nodeOfValue("a/d")).isEqualTo(-1);
    assertThat(parentValues(after, "a/e")).containsExactly("a");
  }

  @Test
  void unrelatedWritesDoNotRebuild() {
    final HierarchyView view = manager.view();
    final long rebuilds = manager.rebuildCount();
    final long generation = manager.generation();
    data.insertAssets(database, 100);
    assertThat(manager.generation()).isEqualTo(generation);
    assertThat(manager.view()).isSameAs(view);
    assertThat(manager.rebuildCount()).isEqualTo(rebuilds);
  }

  @Test
  void ownUncommittedChangeIsPrivate() throws Exception {
    final HierarchyView shared = manager.view();
    final long rebuilds = manager.rebuildCount();
    database.begin();
    try {
      reparent("a/d", "x");
      // read-your-own-writes: the transaction changed watched files
      assertThat(parentValues(manager.view(), "a/d")).containsExactly("x");
      final AtomicReference<List<String>> other = new AtomicReference<>();
      runInOtherThread(() -> other.set(parentValues(manager.view(), "a/d")));
      // the shared view never sees the uncommitted change
      assertThat(other.get()).containsExactly("a");
    } finally {
      database.rollback();
    }
    // the private view was not cached nor counted
    assertThat(manager.view()).isSameAs(shared);
    assertThat(manager.rebuildCount()).isEqualTo(rebuilds);
  }

  @Test
  void concurrentQueriesBuildOnce() throws Exception {
    manager.view();
    // no background rebuild while the 8 queries race: they alone decide who builds
    try (final AutoCloseable hold = columnar().suspendApply()) {
      database.transaction(() -> reparent("a/d", "x"));
      final long before = manager.rebuildCount();
      final CountDownLatch start = new CountDownLatch(1);
      final ExecutorService pool = Executors.newFixedThreadPool(8);
      try {
        final List<Future<HierarchyView>> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++)
          futures.add(pool.submit(() -> {
            start.await();
            return manager.view();
          }));
        start.countDown();
        final List<HierarchyView> views = new ArrayList<>();
        for (final Future<HierarchyView> future : futures)
          views.add(future.get(HANG_MS, TimeUnit.MILLISECONDS));
        assertThat(manager.rebuildCount()).isEqualTo(before + 1);
        for (final HierarchyView view : views) {
          assertThat(view).isSameAs(views.getFirst());
          assertThat(parentValues(view, "a/d")).containsExactly("x");
        }
      } finally {
        pool.shutdownNow();
      }
    }
  }

  @Test
  void newBucketOnTagTypeIsWatched() throws Exception {
    // a view of a generation past 0, so a file set swap that restarted the counter would go back below it
    database.transaction(() -> reparent("a/d", "x"));
    final HierarchyView before = manager.view();
    assertThat(before.generation()).isGreaterThan(1);
    database.command("sql", "CREATE BUCKET Tag_extra");
    database.command("sql", "ALTER TYPE Tag BUCKET +Tag_extra");
    // the HIERARCHY_SOURCE_CHANGED event of "Tag" reached the manager
    assertThat(columnar().awaitCaughtUp(HANG_MS)).isTrue();
    final int extra = database.getSchema().getBucketByName("Tag_extra").getFileId();
    assertThat(manager.watchedFileIds()).contains(extra);
    // the view built before the ALTER is stale: the generation never goes back and moved past it
    assertThat(manager.generation()).isGreaterThan(before.generation());
    final HierarchyView afterAlter = manager.view();
    assertThat(afterAlter).isNotSameAs(before);
    assertThat(afterAlter.generation()).isGreaterThan(before.generation());

    final long generation = manager.generation();
    database.transaction(() -> database.newVertex(ColumnarTestData.TAG_TYPE).set("path", "n").save("Tag_extra"));
    assertThat(manager.generation()).isGreaterThan(generation);
    assertThat(manager.view().nodeOfValue("n")).isGreaterThanOrEqualTo(0);
  }

  @Test
  void tagTypeWithItsOwnColumnarIndexStillBumps() throws Exception {
    database.command("sql", "CREATE INDEX ON Tag (path) COLUMNAR");
    assertThat(columnar().awaitCaughtUp(HANG_MS)).isTrue();
    final int tagBucket = database.getSchema().getType(ColumnarTestData.TAG_TYPE).getBucketIds(false).getFirst();
    assertThat(columnar().isProjected(tagBucket)).isTrue();

    // a key update writes the Tag bucket only: a projected file, whose marks must reach its watchers too
    final HierarchyView before = manager.view();
    database.transaction(() -> database.command("sql", "UPDATE Tag SET path = 'a/e' WHERE path = 'a/d'"));
    assertThat(manager.generation()).isGreaterThan(before.generation());
    final HierarchyView renamed = manager.view();
    assertThat(parentValues(renamed, "a/e")).containsExactly("a");

    database.transaction(() -> reparent("a/e", "x"));
    assertThat(manager.generation()).isGreaterThan(renamed.generation());
    assertThat(parentValues(manager.view(), "a/e")).containsExactly("x");
  }

  @Test
  void droppedTagTypeMakesHierarchyUnavailable() throws Exception {
    manager.view();
    database.transaction(() -> database.command("sql", "DELETE FROM ParentOf"));
    database.command("sql", "DROP TYPE ParentOf UNSAFE");
    assertThat(manager.isAvailable()).isFalse();
    assertThat(manager.unavailableReason()).isEqualTo("hierarchy source ParentOf was renamed or dropped");
    assertThat(manager.view()).isNull();
    // the schema event arrives later and changes nothing
    assertThat(columnar().awaitCaughtUp(HANG_MS)).isTrue();
    assertThat(manager.isAvailable()).isFalse();
  }

  @Test
  void renamedTagTypeMakesHierarchyUnavailable() throws Exception {
    manager.view();
    database.command("sql", "ALTER TYPE Tag NAME Label");
    assertThat(columnar().awaitCaughtUp(HANG_MS)).isTrue();
    assertThat(manager.isAvailable()).isFalse();
    assertThat(manager.unavailableReason()).isEqualTo("hierarchy source Tag was renamed or dropped");
    assertThat(manager.view()).isNull();
    // until the index is recreated, whatever happens to the type
    assertThat(columnar().hierarchy(ColumnarTestData.TYPE, "tags").isAvailable()).isFalse();
  }

  @Test
  void watchedFilesAreMarkedOnWalReplay() throws Exception {
    manager.view();
    // the page flush is held while the change commits and the database is killed: the change survives in the WAL only
    ((DatabaseInternal) database).getPageManager().suspendFlushAndExecute(database, () -> {
      database.transaction(() -> reparent("a/d", "x"));
      ((DatabaseInternal) database).kill();
    });
    // the killed database closed its managers
    assertThat(manager.isAvailable()).isFalse();
    database.close();
    database = factory.open();

    // the hierarchy is never persisted: the first view after the open is built from the graph the WAL replay restored
    final HierarchyManager reopened = columnar().hierarchy(ColumnarTestData.TYPE, "tags");
    assertThat(reopened).isNotSameAs(manager);
    assertThat(parentValues(reopened.view(), "a/d")).containsExactly("x");
    assertThat(parentValues(reopened.view(), "a/b")).containsExactly("a");
  }

  @Test
  void sharedViewIgnoresTheCallerResultSetLimit() {
    // a database with a security manager, so the context user's limits apply to the scans
    database.close();
    factory.setSecurity(db -> {
    });
    try {
      database = factory.open();
      final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());
      final SecurityDatabaseUser limited = limitedUser(2);
      context.setCurrentUser(limited);
      try {
        // the limit is real: a plain scan of the tags stops at 2
        assertThat(countIterated(ColumnarTestData.TAG_TYPE)).isEqualTo(2);

        final HierarchyView view = columnar().hierarchy(ColumnarTestData.TYPE, "tags").view();
        // the shared view is every user's: built with no user, all 5 tags and their links
        assertThat(view.nodeCount()).isEqualTo(5);
        assertThat(parentValues(view, "a/b/c")).containsExactly("a/b");
        assertThat(context.getCurrentUser()).isSameAs(limited);
      } finally {
        context.setCurrentUser(null);
      }
    } finally {
      database.close();
      factory.setSecurity(null);
      database = factory.open();
    }
  }

  @Test
  void callerTransactionIsIntactAfterASharedBuild() {
    manager.view();
    final long assets = countIterated(ColumnarTestData.TYPE);
    database.begin();
    try {
      // the caller writes outside the watched files: its view is the shared one
      database.newDocument(ColumnarTestData.TYPE).set("brand", "caller-brand").save();
      runInOtherThreadUnchecked(() -> database.transaction(() -> reparent("a/d", "x")));
      final long rebuilds = manager.rebuildCount();
      assertThat(parentValues(manager.view(), "a/d")).containsExactly("x");
      assertThat(manager.rebuildCount()).isEqualTo(rebuilds + 1);

      // the build's own transaction is gone: the caller's is the only one, still active, its write still visible
      assertThat(DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).getTransactionDepth()).isEqualTo(1);
      assertThat(database.isTransactionActive()).isTrue();
      assertThat(countBrand("caller-brand")).isEqualTo(1);
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
    assertThat(countIterated(ColumnarTestData.TYPE)).isEqualTo(assets + 1);
    assertThat(countBrand("caller-brand")).isEqualTo(1);
  }

  @Test
  void repeatableReadCallerDoesNotLeakItsSnapshot() {
    manager.view();
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      // the snapshot holds the tag pages and a/d's edge list as they are now
      assertThat(parentsInTransaction("a/d")).containsExactly("a");
      runInOtherThreadUnchecked(() -> database.transaction(() -> reparent("a/d", "x")));
      assertThat(parentsInTransaction("a/d")).containsExactly("a");

      final long rebuilds = manager.rebuildCount();
      final HierarchyView view = manager.view();
      assertThat(manager.rebuildCount()).isEqualTo(rebuilds + 1);
      // built from the committed state, not from the caller's snapshot
      assertThat(parentValues(view, "a/d")).containsExactly("x");
    } finally {
      database.rollback();
    }
    assertThat(parentValues(manager.view(), "a/d")).containsExactly("x");
  }

  @Test
  void closeWaitsForARunningBackgroundRebuild() throws Exception {
    manager.view();
    final long rebuilds = manager.rebuildCount();
    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean once = new AtomicBoolean();
    ColumnarTestHooks.killPoint = name -> {
      if (HierarchyBuilder.BETWEEN_SCANS.equals(name) && Thread.currentThread().getName().startsWith("ArcadeDB-ColumnarMaintenance-")
          && once.compareAndSet(false, true)) {
        reached.countDown();
        try {
          release.await(HANG_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    final Thread closer = new Thread(() -> database.close());
    try {
      try {
        database.transaction(() -> reparent("a/d", "x"));
        // the background rebuild is inside the builder, between its two scans
        assertThat(reached.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
        closer.start();
        // a short wait expected to time out: the close waits for the running rebuild
        closer.join(500);
        assertThat(closer.isAlive()).isTrue();
      } finally {
        release.countDown();
        ColumnarTestHooks.killPoint = null;
      }
      closer.join(HANG_MS);
      assertThat(closer.isAlive()).isFalse();
      // the rebuild was cancelled by the close: nothing was published
      assertThat(manager.rebuildCount()).isEqualTo(rebuilds);
    } finally {
      // the fixture's database back, whatever failed above (a close that never started is done here)
      if (closer.isAlive())
        closer.join(HANG_MS);
      if (database.isOpen())
        database.close();
      database = factory.open();
    }
  }

  @Test
  void busyBuildLockDefersTheScheduledRebuild() throws Exception {
    manager.view();
    final long rebuilds = manager.rebuildCount();
    final CountDownLatch building = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch deferred = new CountDownLatch(1);
    final AtomicReference<Thread> query = new AtomicReference<>();
    ColumnarTestHooks.killPoint = name -> {
      if (HierarchyManager.REBUILD_DEFERRED.equals(name))
        deferred.countDown();
      else if (HierarchyBuilder.BETWEEN_SCANS.equals(name) && Thread.currentThread() == query.get()) {
        building.countDown();
        try {
          release.await(HANG_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    try {
      final Thread queryThread;
      try (final AutoCloseable hold = columnar().suspendApply()) {
        database.transaction(() -> reparent("a/d", "x"));
        // a query builds and holds the build lock
        queryThread = new Thread(() -> manager.view());
        query.set(queryThread);
        queryThread.start();
        assertThat(building.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
        // a change the running build will miss
        database.transaction(() -> reparent("a/d", "a"));
      }
      // the apply pass runs the watcher: the scheduled rebuild finds the lock busy and does not wait for it
      assertThat(deferred.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
      release.countDown();
      queryThread.join(HANG_MS);
      assertThat(queryThread.isAlive()).isFalse();
      // the running build re-queued it on release: a second build without any query
      assertThat(waitFor(() -> manager.rebuildCount() >= rebuilds + 2)).isTrue();
    } finally {
      release.countDown();
      ColumnarTestHooks.killPoint = null;
    }
    assertThat(parentValues(manager.view(), "a/d")).containsExactly("a");
  }

  @Test
  void privateBuildsNeverLogTheCycleWarning() throws Exception {
    manager.view();
    try (final AutoCloseable hold = columnar().suspendApply()) {
      database.begin();
      try {
        edge("a/b/c", "a");
        final List<String> warnings = new ArrayList<>();
        final HierarchyView first = capturingWarnings(warnings, () -> manager.view());
        final HierarchyView second = capturingWarnings(warnings, () -> manager.view());
        assertThat(first.cycleNodes()).isEqualTo(3);
        assertThat(second.cycleNodes()).isEqualTo(3);
        assertThat(cycleWarnings(warnings)).isEmpty();
      } finally {
        database.rollback();
      }
    }
  }

  @Test
  void callerAtMaxNestingGetsTheHierarchyUnavailable() {
    manager.view();
    database.transaction(() -> reparent("a/d", "x"));
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());
    database.begin();
    while (context.getTransactionDepth() < context.getMaxNested())
      database.begin();
    try {
      assertThat(context.getTransactionDepth()).isEqualTo(context.getMaxNested());
      // the shared view needs a rebuild, which needs a transaction of its own: no exception, the hierarchy is unavailable
      assertThat(manager.view()).isNull();
      assertThat(manager.isAvailable()).isFalse();
      assertThat(manager.unavailableReason()).isEqualTo(HierarchyManager.NESTING_TOO_DEEP);
    } finally {
      while (database.isTransactionActive())
        database.rollback();
    }
    assertThat(manager.isAvailable()).isTrue();
    assertThat(parentValues(manager.view(), "a/d")).containsExactly("x");
  }

  @Test
  void sharedBuildIsNotCountedAsARollback() throws Exception {
    manager.view();
    // no apply pass meanwhile: its implicit transactions are rolled back, which would count
    try (final AutoCloseable hold = columnar().suspendApply()) {
      database.transaction(() -> reparent("a/d", "x"));
      final long rollbacks = rollbacks();
      final long rebuilds = manager.rebuildCount();
      assertThat(parentValues(manager.view(), "a/d")).containsExactly("x");
      assertThat(manager.rebuildCount()).isEqualTo(rebuilds + 1);
      assertThat(rollbacks()).isEqualTo(rollbacks);
    }
  }

  @Test
  void backgroundRebuildAfterCommit() {
    manager.view();
    final long before = manager.rebuildCount();
    final Set<String> rebuildThreads = ConcurrentHashMap.newKeySet();
    ColumnarTestHooks.killPoint = name -> {
      if (HierarchyManager.REBUILD.equals(name))
        rebuildThreads.add(Thread.currentThread().getName());
    };
    try {
      database.transaction(() -> reparent("a/d", "x"));
      // no query: the coalesced onChange scheduled rebuildIfStale on the maintenance pool
      assertThat(waitFor(() -> manager.rebuildCount() > before)).isTrue();
    } finally {
      ColumnarTestHooks.killPoint = null;
    }
    assertThat(rebuildThreads).isNotEmpty().allMatch(name -> name.startsWith("ArcadeDB-ColumnarMaintenance-"));
    assertThat(parentValues(manager.view(), "a/d")).containsExactly("x");
  }

  @Test
  void cycleWarningOnlyWhenTheCycleSetChanges() throws Exception {
    manager.view();
    // no background rebuild: every rebuild below is the one view() makes, under the capturing logger
    try (final AutoCloseable hold = columnar().suspendApply()) {
      database.transaction(() -> edge("a/b/c", "a"));
      final List<String> warnings = new ArrayList<>();
      HierarchyView view = capturingWarnings(warnings, () -> manager.view());
      assertThat(view.cycleNodes()).isEqualTo(3);
      assertThat(cycleWarnings(warnings)).hasSize(1);
      assertThat(cycleWarnings(warnings).getFirst()).contains("'" + INDEX_NAME + "'");

      // a rebuild with the same cycle: no new WARNING
      warnings.clear();
      final long generation = view.generation();
      database.transaction(() -> database.newVertex(ColumnarTestData.TAG_TYPE).set("path", "y").save());
      view = capturingWarnings(warnings, () -> manager.view());
      assertThat(view.generation()).isGreaterThan(generation);
      assertThat(view.cycleNodes()).isEqualTo(3);
      assertThat(cycleWarnings(warnings)).isEmpty();

      // another set of cycle nodes: one WARNING
      database.transaction(() -> edge("y", "y"));
      view = capturingWarnings(warnings, () -> manager.view());
      assertThat(view.cycleNodes()).isEqualTo(4);
      assertThat(cycleWarnings(warnings)).hasSize(1);
    }
  }

  @Test
  void writeDuringBuildLeavesTheViewStale() throws Exception {
    manager.view();
    final AtomicBoolean once = new AtomicBoolean();
    final AtomicReference<Throwable> writerError = new AtomicReference<>();
    try (final AutoCloseable hold = columnar().suspendApply()) {
      database.transaction(() -> reparent("a/d", "x"));
      // between the builder's two scans another thread commits a new tag under "a"
      ColumnarTestHooks.killPoint = name -> {
        if (HierarchyBuilder.BETWEEN_SCANS.equals(name) && once.compareAndSet(false, true))
          try {
            runInOtherThread(() -> database.transaction(() -> {
              final Vertex late = database.newVertex(ColumnarTestData.TAG_TYPE).set("path", "late").save();
              tag("a").newEdge(ColumnarTestData.PARENT_EDGE, late);
            }));
          } catch (final Throwable e) {
            writerError.set(e);
          }
      };
      final HierarchyView during;
      try {
        during = manager.view();
      } finally {
        ColumnarTestHooks.killPoint = null;
      }
      assertThat(once.get()).isTrue();
      assertThat(writerError.get()).isNull();
      assertThat(during.nodeOfValue("late")).isEqualTo(-1);
      // the generation was read before the scans: the commit during the build left the view stale
      assertThat(manager.generation()).isGreaterThan(during.generation());
      final HierarchyView next = manager.view();
      assertThat(next).isNotSameAs(during);
      assertThat(parentValues(next, "late")).containsExactly("a");
    }
  }

  @Test
  void badPersistedDefinitionMakesHierarchyUnavailable() throws Exception {
    database.close();
    final Path schemaFile = Path.of(getDatabasePath(), "schema.json");
    final String json = Files.readString(schemaFile);
    final String edited = json.replaceAll("\"parentEdge\"\\s*:\\s*\"ParentOf\"", "\"parentEdge\":\"ParentOf\",\"parentDirection\":\"SIDEWAYS\"");
    assertThat(edited).isNotEqualTo(json);
    Files.writeString(schemaFile, edited);

    // the open does not read the definition
    database = factory.open();
    final HierarchyManager bad = columnar().hierarchy(ColumnarTestData.TYPE, "tags");
    assertThat(bad).isNotNull();
    assertThat(bad.isAvailable()).isFalse();
    assertThat(bad.unavailableReason()).contains("parentDirection").contains("SIDEWAYS");
    assertThat(bad.view()).isNull();
    assertThat(bad.watchedFileIds()).isEmpty();

    // reconcile and apply passes are unaffected
    data.insertAssets(database, 10);
    database.transaction(() -> reparent("a/d", "x"));
    assertThat(columnar().awaitCaughtUp(HANG_MS)).isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + ColumnarTestData.TYPE)) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(40L);
    }
  }

  @Test
  void managerIsClosedWithItsIndexAndItsDatabase() throws Exception {
    final ColumnarIndexManager columnar = columnar();
    final int listeners = columnar.schemaListenerCountForTesting();
    assertThat(columnar.watcherCountForTesting()).isEqualTo(1);

    database.command("sql", "DROP INDEX `" + INDEX_NAME + "`");
    assertThat(columnar.awaitCaughtUp(HANG_MS)).isTrue();
    assertThat(manager.isAvailable()).isFalse();
    assertThat(columnar.watcherCountForTesting()).isZero();
    assertThat(columnar.hierarchy(ColumnarTestData.TYPE, "tags")).isNull();

    // recreated: a new manager, and still one schema listener for all of them
    database.command("sql",
        "CREATE INDEX ON " + ColumnarTestData.TYPE + " (tags BY ITEM) COLUMNAR METADATA " + new JSONObject().put("hierarchy", hierarchyJSON()));
    assertThat(columnar.awaitCaughtUp(HANG_MS)).isTrue();
    final HierarchyManager recreated = columnar.hierarchy(ColumnarTestData.TYPE, "tags");
    assertThat(recreated).isNotNull().isNotSameAs(manager);
    assertThat(recreated.isAvailable()).isTrue();
    assertThat(parentValues(recreated.view(), "a/d")).containsExactly("a");
    assertThat(columnar.watcherCountForTesting()).isEqualTo(1);
    assertThat(columnar.schemaListenerCountForTesting()).isEqualTo(listeners);

    database.close();
    assertThat(recreated.isAvailable()).isFalse();
    assertThat(columnar.watcherCountForTesting()).isZero();
    database = factory.open();
  }

  // ------------------------------------------------------------------------------------------------------------------- helpers

  private static JSONObject hierarchyJSON() {
    return new JSONObject().put("vertexType", ColumnarTestData.TAG_TYPE).put("key", "path").put("parentEdge", ColumnarTestData.PARENT_EDGE);
  }

  private ColumnarIndexManager columnar() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private Vertex tag(final String path) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + ColumnarTestData.TAG_TYPE + " WHERE path = ?", path)) {
      return rs.next().getVertex().orElseThrow();
    }
  }

  private void edge(final String from, final String to) {
    tag(from).newEdge(ColumnarTestData.PARENT_EDGE, tag(to));
  }

  /** Replaces every parent of {@code child} with {@code newParent}. */
  private void reparent(final String child, final String newParent) {
    final Vertex vertex = tag(child);
    final List<Edge> edges = new ArrayList<>();
    for (final Edge e : vertex.getEdges(Vertex.DIRECTION.IN, ColumnarTestData.PARENT_EDGE))
      edges.add(e);
    for (final Edge e : edges)
      e.delete();
    tag(newParent).newEdge(ColumnarTestData.PARENT_EDGE, vertex);
  }

  private static List<String> parentValues(final HierarchyView view, final String value) {
    final int node = view.nodeOfValue(value);
    assertThat(node).isGreaterThanOrEqualTo(0);
    final List<String> parents = new ArrayList<>();
    for (int p = view.parentOffsets()[node]; p < view.parentOffsets()[node + 1]; p++)
      parents.add((String) view.value(view.parents()[p]));
    return parents;
  }

  /** {@link #runInOtherThread} for a body that throws nothing checked. */
  private static void runInOtherThreadUnchecked(final Runnable body) {
    try {
      runInOtherThread(body);
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** The records a plain polymorphic scan of the type hands out (the context user's result-set limit applies). */
  private long countIterated(final String type) {
    long count = 0;
    for (final Iterator<?> it = database.iterateType(type, true); it.hasNext(); it.next())
      count++;
    return count;
  }

  private long countBrand(final String brand) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + ColumnarTestData.TYPE + " WHERE brand = ?", brand)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /** The parent keys of a tag as the caller's transaction reads them. */
  private List<String> parentsInTransaction(final String child) {
    final List<String> parents = new ArrayList<>();
    for (final Vertex parent : tag(child).getVertices(Vertex.DIRECTION.IN, ColumnarTestData.PARENT_EDGE))
      parents.add(parent.getString("path"));
    return parents;
  }

  private long rollbacks() {
    return ((Number) ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getStats().get("txRollbacks")).longValue();
  }

  /** A user that may do everything and whose scans stop after {@code limit} records. */
  private static SecurityDatabaseUser limitedUser(final long limit) {
    return new SecurityDatabaseUser() {
      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public String getName() {
        return "limited";
      }

      @Override
      public long getResultSetLimit() {
        return limit;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }

  /** Runs {@code body} on a new thread and waits for it, rethrowing what it threw. */
  private static void runInOtherThread(final Runnable body) throws Exception {
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        body.run();
      } catch (final Throwable e) {
        error.set(e);
      }
    });
    thread.start();
    thread.join(HANG_MS);
    assertThat(thread.isAlive()).isFalse();
    if (error.get() instanceof Exception e)
      throw e;
    if (error.get() instanceof Error e)
      throw e;
  }

  /** Polls {@code condition} until it holds, for {@link #HANG_MS} at most. */
  private static boolean waitFor(final BooleanSupplier condition) {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(HANG_MS);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() - deadline >= 0)
        return false;
      try {
        Thread.sleep(5);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return true;
  }

  private static List<String> cycleWarnings(final List<String> warnings) {
    return warnings.stream().filter(w -> w.contains(CYCLE_LOG)).toList();
  }

  /** Runs {@code action} with the WARNINGs of every logger call collected into {@code warnings}, formatted. */
  private static <T> T capturingWarnings(final List<String> warnings, final Supplier<T> action) {
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(warnings));
    try {
      return action.get();
    } finally {
      LogManager.instance().setLogger(previous);
    }
  }

  private static final class CapturingLogger implements Logger {
    private final List<String> warnings;

    private CapturingLogger(final List<String> warnings) {
      this.warnings = warnings;
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
        final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
        final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      log(requester, level, message, exception, context,
          new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17 });
    }

    @Override
    public synchronized void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      if (level == Level.WARNING && message != null)
        warnings.add(args == null || args.length == 0 ? message : String.format(message, args));
    }

    @Override
    public void flush() {
    }
  }
}
