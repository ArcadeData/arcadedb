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
package com.arcadedb.query.facet;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestAccess;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.ColumnarTestHooks;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.index.columnar.HierarchyManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HIERARCHY facets end to end, through SQL, against {@link FacetOracle#hierarchicalCounts} (SQL rows and a vertex walk that share no
 * code with the collector): a 4 x 4 tag forest of 340 paths, 2000 assets with up to 4 tags each drawn from the paths and 5 orphan
 * values.
 */
class HierarchyFacetIntegrationTest extends TestHelper {
  private static final String       TYPE    = ColumnarTestData.TYPE;
  private static final String       WHERE   = "fileSize > 1000";
  private static final List<String> ORPHANS = List.of("orphan-0", "orphan-1", "orphan-2", "orphan-3", "orphan-4");
  private static final String       QUERY   = "SELECT FROM " + TYPE + " WHERE " + WHERE + " FACETS t: tags HIERARCHY TOP 1000";
  /** A hang detector, never a latency bound. */
  private static final long         HANG_MS = 60_000L;

  private static JSONObject hierarchyJSON() {
    return new JSONObject().put("vertexType", ColumnarTestData.TAG_TYPE).put("key", "path").put("parentEdge", ColumnarTestData.PARENT_EDGE);
  }

  private ColumnarTestData data(final long seed, final int buckets) {
    final ColumnarTestData data = new ColumnarTestData(seed);
    data.tagHierarchy = hierarchyJSON();
    data.buckets = buckets;
    data.maxTags = 4;
    return data;
  }

  /** The schema, the tag forest, then 2000 assets tagged from it and from the orphan values. */
  private ColumnarTestData load(final long seed, final int buckets) {
    final ColumnarTestData data = data(seed, buckets);
    data.createSchema(database);
    tagAndInsert(data, seed);
    return data;
  }

  private void tagAndInsert(final ColumnarTestData data, final long seed) {
    final List<String> values = new ArrayList<>(ColumnarTestData.createTagTree(database, seed, 4, 4));
    values.addAll(ORPHANS);
    data.tagValues = values;
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private FacetResults facets(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getFacets();
    }
  }

  private FacetResult facet(final String sql) {
    return facets(sql).get("t");
  }

  private Map<Object, Long> expected(final String where) {
    return FacetOracle.hierarchicalCounts(database, TYPE, "tags", where, ColumnarTestData.TAG_TYPE, "path", ColumnarTestData.PARENT_EDGE);
  }

  /** The buckets as value to count, the matches without a value under {@link FacetOracle#MISSING}, as the oracle keys them. */
  private static Map<Object, Long> toMap(final FacetResult result) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      assertThat(counts.put(bucket.value(), bucket.count())).as("value " + bucket.value() + " returned twice").isNull();
    if (result.missing() > 0)
      counts.put(FacetOracle.MISSING, result.missing());
    return counts;
  }

  private static FacetBucket bucket(final FacetResult result, final Object value) {
    for (final FacetBucket bucket : result.buckets())
      if (value.equals(bucket.value()))
        return bucket;
    throw new AssertionError("No bucket " + value + " in " + result.buckets());
  }

  private void assertEqualsReference(final FacetResult result, final String where) {
    assertThat(result.kind()).isEqualTo("hierarchy");
    assertThat(result.source()).as(result.reason()).isIn("index", "scan");
    assertThat(toMap(result)).as("WHERE " + where).isEqualTo(expected(where));
    assertThat(result.otherCount()).isZero();
  }

  private Vertex tag(final String path) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + ColumnarTestData.TAG_TYPE + " WHERE path = ?", path)) {
      return rs.next().getVertex().orElseThrow();
    }
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

  // ------------------------------------------------------------------------------------------------------------------- tests

  @ParameterizedTest
  @ValueSource(longs = { 1L, 7L, 42L })
  @Timeout(300)
  void hierarchicalCountsEqualReference(final long seed) {
    load(seed, 1);
    final FacetResult result = facet(QUERY);
    assertThat(result.source()).isEqualTo("index");
    assertEqualsReference(result, WHERE);
    // every matched document reaches a root: the roots of the forest and the orphans hold the matches with a value between them
    assertThat(bucket(result, "t0/t1/t2").attributes().getString("parent")).isEqualTo("t0/t1");
    assertThat(bucket(result, "t0/t1/t2").attributes().getInt("depth")).isEqualTo(2);
    assertThat(bucket(result, "orphan-3").attributes().getBoolean("orphan")).isTrue();
    // without WHERE as well
    assertEqualsReference(facet("SELECT FROM " + TYPE + " FACETS t: tags HIERARCHY TOP 1000"), null);
  }

  @Test
  @Timeout(300)
  void twoTagsInOneSubtreeCountedOnce() {
    load(42L, 1);
    database.transaction(() -> database.newDocument(TYPE).set("author", "solo").set("tags", List.of("t1/t2/t3", "t1/t2/t0", "t1")).save());
    FacetOracle.awaitCaughtUp(database);
    final FacetResult result = facet("SELECT FROM " + TYPE + " WHERE author = 'solo' FACETS t: tags HIERARCHY TOP 1000");
    assertThat(toMap(result)).isEqualTo(Map.of("t1", 1L, "t1/t2", 1L, "t1/t2/t3", 1L, "t1/t2/t0", 1L));
    assertEqualsReference(result, "author = 'solo'");
  }

  @Test
  @Timeout(300)
  void treeChangeIsReflectedWithoutReapply() {
    load(7L, 1);
    assertEqualsReference(facet(QUERY), WHERE);
    final long applied = manager().getStats().applyPagesCount(TYPE);
    database.transaction(() -> reparent("t0/t1", "t2"));
    final FacetResult result = facet(QUERY);
    assertEqualsReference(result, WHERE);
    assertThat(bucket(result, "t0/t1").attributes().getString("parent")).isEqualTo("t2");
    // no document page re-applied: the Tag type has no COLUMNAR index
    assertThat(manager().getStats().applyPagesCount(TYPE)).isEqualTo(applied);
  }

  /** Review focus 1: a tag value counted as an orphan root first, then under its real ancestors once its Tag exists. */
  @Test
  @Timeout(300)
  void orphanBecomesMappedWhenTagIsCreatedLater() {
    load(1L, 1);
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument(TYPE).set("fileSize", 5_000L).set("tags", List.of("late/x")).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final FacetResult before = facet(QUERY);
    assertEqualsReference(before, WHERE);
    assertThat(bucket(before, "late/x").attributes().getBoolean("orphan")).isTrue();
    final long applied = manager().getStats().applyPagesCount(TYPE);

    database.transaction(() -> {
      final Vertex late = database.newVertex(ColumnarTestData.TAG_TYPE).set("path", "late").save();
      final Vertex x = database.newVertex(ColumnarTestData.TAG_TYPE).set("path", "late/x").save();
      late.newEdge(ColumnarTestData.PARENT_EDGE, x);
    });
    final FacetResult after = facet(QUERY);
    assertEqualsReference(after, WHERE);
    assertThat(bucket(after, "late/x").attributes().has("orphan")).isFalse();
    assertThat(bucket(after, "late/x").attributes().getString("parent")).isEqualTo("late");
    assertThat(bucket(after, "late").count()).isEqualTo(3L);
    // no document re-applied: the documents keep their values, the hierarchy maps them anew
    assertThat(manager().getStats().applyPagesCount(TYPE)).isEqualTo(applied);
  }

  @Test
  @Timeout(300)
  void cycleInTagGraphDoesNotLoop() {
    load(42L, 1);
    // a leaf becomes the parent of the root of its own branch
    database.transaction(() -> tag("t0/t1/t2/t3").newEdge(ColumnarTestData.PARENT_EDGE, tag("t0")));
    final FacetResult result = facet(QUERY);
    assertEqualsReference(result, WHERE);
    // every member of the cycle counts a document of its branch once: t0 and the leaf hold the same documents
    assertThat(bucket(result, "t0").count()).isEqualTo(bucket(result, "t0/t1/t2/t3").count());
    assertThat(bucket(result, "t0/t1").attributes().getInt("depth")).isEqualTo(-1);
  }

  @Test
  @Timeout(300)
  void patchedDocumentsWhileApplyIsSuspended() throws Exception {
    final ColumnarTestData data = load(7L, 1);
    try (final AutoCloseable hold = manager().suspendApply()) {
      final long before = count();
      data.insertAssets(database, 50);
      assertThat(count()).isEqualTo(before + 50);
      final FacetResults results = facets("SELECT FROM " + TYPE + " FACETS t: tags HIERARCHY TOP 1000");
      assertEqualsReference(results.get("t"), null);
      assertThat(results.exact()).isTrue();
      // pending marks are page granular: at least the new records are patched
      assertThat(results.patched()).isGreaterThanOrEqualTo(50);
    }
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + TYPE)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  @Timeout(300)
  void ownTransactionTreeChangeIsSeen() {
    load(42L, 1);
    final Map<Object, Long> committed = expected(WHERE);
    database.begin();
    try {
      reparent("t3/t3", "t1");
      final FacetResult result = facet(QUERY);
      assertEqualsReference(result, WHERE);
      assertThat(bucket(result, "t3/t3").attributes().getString("parent")).isEqualTo("t1");
      assertThat(toMap(result)).isNotEqualTo(committed);
    } finally {
      database.rollback();
    }
    final FacetResult afterRollback = facet(QUERY);
    assertThat(toMap(afterRollback)).isEqualTo(committed);
    assertThat(bucket(afterRollback, "t3/t3").attributes().getString("parent")).isEqualTo("t3");
  }

  @Test
  @Timeout(300)
  void unavailableHierarchySource() {
    load(1L, 1);
    assertEqualsReference(facet(QUERY), WHERE);
    // the edges first, so the drop leaves no dangling adjacency entry behind (the fixture checks the database integrity)
    database.transaction(() -> database.command("sql", "DELETE FROM " + ColumnarTestData.PARENT_EDGE));
    database.command("sql", "DROP TYPE " + ColumnarTestData.PARENT_EDGE + " UNSAFE");
    final FacetResults results = facets(QUERY + ", b: brand TERMS TOP 5");
    final FacetResult result = results.get("t");
    assertThat(result.source()).isEqualTo(FacetResult.UNAVAILABLE);
    assertThat(result.reason()).isEqualTo("hierarchy source " + ColumnarTestData.PARENT_EDGE + " was renamed or dropped");
    assertThat(result.buckets()).isEmpty();
    // the query and its other facets are answered
    assertThat(results.get("b").source()).isEqualTo("index");
  }

  /** {@code view()} throwing CancellationException (the manager closed during the build): the facet is unavailable, never the query. */
  @Test
  @Timeout(300)
  void managerClosedDuringTheBuildIsUnavailable() throws Exception {
    load(1L, 1);
    assertEqualsReference(facet(QUERY), WHERE);
    final HierarchyManager hierarchy = manager().hierarchy(TYPE, "tags");
    final Thread query = Thread.currentThread();
    final AtomicBoolean closed = new AtomicBoolean();
    // held: the coalesced background rebuild cannot run first, so this query builds the stale view itself
    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> reparent("t0/t1", "t2"));
      ColumnarTestHooks.killPoint = name -> {
        if ("hierarchy.betweenScans".equals(name) && Thread.currentThread() == query && closed.compareAndSet(false, true))
          ColumnarTestAccess.closeHierarchy(hierarchy);
      };
      try {
        final FacetResults results = facets(QUERY + ", b: brand TERMS TOP 5");
        assertThat(closed.get()).as("the query built the view").isTrue();
        final FacetResult result = results.get("t");
        assertThat(result.source()).isEqualTo(FacetResult.UNAVAILABLE);
        assertThat(result.reason()).isEqualTo(hierarchy.unavailableReason()).contains("is closed");
        assertThat(results.get("b").source()).isEqualTo("index");
      } finally {
        ColumnarTestHooks.killPoint = null;
      }
    }
  }

  /**
   * Final review A-M4: a stale view honours the statement's TIMEOUT. The query leaves the build to the maintenance pool and waits for it
   * until its deadline (round 2, N1); past it the facet is unavailable ({@link HierarchyManager#BEING_REBUILT}), the other facets are
   * answered, and the pool publishes the view for the next query.
   */
  @Test
  @Timeout(300)
  void buildPastTheStatementDeadlineIsUnavailable() throws Exception {
    load(1L, 1);
    assertEqualsReference(facet(QUERY), WHERE);
    final AtomicBoolean slept = new AtomicBoolean();
    // held: the coalesced background rebuild is not queued by the watcher, only by the query
    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> reparent("t0/t1", "t2"));
      ColumnarTestHooks.killPoint = name -> {
        if ("hierarchy.betweenScans".equals(name) && Thread.currentThread().getName().startsWith("ArcadeDB-ColumnarMaintenance-")
            && slept.compareAndSet(false, true))
          try {
            // past the statement's TIMEOUT below
            Thread.sleep(2_500);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
      };
      try {
        final FacetResults results = facets(QUERY + ", b: brand TERMS TOP 5 TIMEOUT 2000 RETURN");
        assertThat(slept.get()).as("the maintenance pool builds the view").isTrue();
        final FacetResult result = results.get("t");
        assertThat(result.source()).isEqualTo(FacetResult.UNAVAILABLE);
        assertThat(result.reason()).isEqualTo(HierarchyManager.BEING_REBUILT);
        assertThat(results.get("b").source()).isEqualTo("index");
      } finally {
        ColumnarTestHooks.killPoint = null;
      }
    }
    // the build left to the background, then the query without TIMEOUT: the view of the new tree
    assertEqualsReference(facet(QUERY), WHERE);
  }

  /** Final review round 2 (N5): the private view of the caller's own tag changes, not built before the TIMEOUT, says so. */
  @Test
  @Timeout(300)
  void privateBuildPastTheStatementDeadlineSaysSo() {
    load(1L, 1);
    final Thread query = Thread.currentThread();
    final AtomicBoolean slept = new AtomicBoolean();
    ColumnarTestHooks.killPoint = name -> {
      if ("hierarchy.betweenScans".equals(name) && Thread.currentThread() == query && slept.compareAndSet(false, true))
        try {
          Thread.sleep(2_500);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
    };
    database.begin();
    try {
      reparent("t0/t1", "t2");
      final FacetResult result = facets(QUERY + " TIMEOUT 2000 RETURN").get("t");
      assertThat(slept.get()).as("the query built its private view").isTrue();
      assertThat(result.source()).isEqualTo(FacetResult.UNAVAILABLE);
      assertThat(result.reason()).isEqualTo(HierarchyManager.PRIVATE_NOT_BUILT);
    } finally {
      ColumnarTestHooks.killPoint = null;
      database.rollback();
    }
  }

  @Test
  @Timeout(300)
  void multiBucketTypeEqualsReference() {
    load(42L, 3);
    assertThat(database.getSchema().getType(TYPE).getBuckets(false)).hasSize(3);
    final FacetResult result = facet(QUERY);
    assertThat(result.source()).isEqualTo("index");
    assertEqualsReference(result, WHERE);
  }

  @Test
  @Timeout(300)
  void polymorphicSubtypeEqualsReference() {
    final ColumnarTestData data = data(7L, 1);
    data.createSchema(database);
    database.command("sql", "CREATE DOCUMENT TYPE Image EXTENDS " + TYPE);
    FacetOracle.awaitCaughtUp(database);
    tagAndInsert(data, 7L);
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Image").set("fileSize", 2_000L + i).set("tags", List.of("t" + (i % 4) + "/t" + (i % 3), ORPHANS.get(i % 5)))
            .save();
    });
    FacetOracle.awaitCaughtUp(database);
    final FacetResult result = facet(QUERY);
    assertThat(result.source()).isEqualTo("index");
    assertEqualsReference(result, WHERE);
    assertThat(expected("@type = 'Image'")).containsKey("t1");
    assertEqualsReference(facet("SELECT FROM Image FACETS t: tags HIERARCHY TOP 1000"), "@type = 'Image'");
  }

  /**
   * Review I2: the index with the hierarchy only on a subtype. A match in a bucket of the unindexed supertype has no hierarchy to roll up
   * along (spec 7.6: it is declared in the index METADATA), so the facet is unavailable and says which type lacks it; the query and its
   * other facets are answered, and matches only in the indexed subtype are counted as usual.
   */
  @Test
  @Timeout(300)
  void indexOnlyOnASubtypeNamesTheTypeWithoutHierarchy() {
    ColumnarTestData.createTagTree(database, 3L, 3, 3);
    database.command("sql", "CREATE DOCUMENT TYPE Media");
    database.command("sql", "CREATE PROPERTY Media.tags LIST OF STRING");
    database.command("sql", "CREATE DOCUMENT TYPE Photo EXTENDS Media");
    database.command("sql", "CREATE INDEX ON Photo (tags BY ITEM) COLUMNAR METADATA " + new JSONObject().put("hierarchy", hierarchyJSON()));
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      for (int i = 0; i < 40; i++) {
        database.newDocument("Media").set("tags", List.of("t" + (i % 3) + "/t" + (i % 2))).save();
        database.newDocument("Photo").set("tags", List.of("t" + (i % 3) + "/t" + (i % 3) + "/t1", "orphan-" + (i % 2))).save();
      }
    });
    FacetOracle.awaitCaughtUp(database);

    final FacetResults both = facets("SELECT FROM Media FACETS t: tags HIERARCHY TOP 1000, n: tags TERMS TOP 5");
    assertThat(both.get("t").source()).isEqualTo(FacetResult.UNAVAILABLE);
    assertThat(both.get("t").reason()).isEqualTo("no hierarchy on Media.tags");
    assertThat(both.get("n").source()).isEqualTo("scan");

    final FacetResult photos = facet("SELECT FROM Media WHERE @type = 'Photo' FACETS t: tags HIERARCHY TOP 1000");
    assertThat(photos.source()).isEqualTo("index");
    assertThat(toMap(photos)).isEqualTo(
        FacetOracle.hierarchicalCounts(database, "Photo", "tags", null, ColumnarTestData.TAG_TYPE, "path", ColumnarTestData.PARENT_EDGE));
    assertThat(bucket(photos, "t1").count()).isEqualTo(13L);
  }

  @Test
  @Timeout(300)
  void scannedBucketEqualsOracle() throws Exception {
    final ColumnarTestData data = data(13L, 1);
    data.createSchema(database);
    tagAndInsert(data, 13L);
    database.command("sql", "DROP INDEX `" + TYPE + "[tagsbyitem]_columnar`");
    FacetOracle.awaitCaughtUp(database);

    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if ("rebuild.scanned".equals(name)) {
        reached.countDown();
        try {
          release.await(HANG_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    try {
      database.command("sql", "CREATE INDEX ON " + TYPE + " (tags BY ITEM) COLUMNAR METADATA " + new JSONObject().put("hierarchy", hierarchyJSON()));
      assertThat(reached.await(HANG_MS, TimeUnit.MILLISECONDS)).as("the create rebuild reached the end of its scan").isTrue();
      final FacetResult result = facet(QUERY);
      assertThat(result.source()).isEqualTo("scan");
      assertEqualsReference(result, WHERE);
      assertThat(bucket(result, "orphan-1").attributes().getBoolean("orphan")).isTrue();
    } finally {
      ColumnarTestHooks.killPoint = null;
      release.countDown();
    }
    FacetOracle.awaitCaughtUp(database);
    final FacetResult published = facet(QUERY);
    assertThat(published.source()).isEqualTo("index");
    assertEqualsReference(published, WHERE);
  }

  /** Review focus 10: the shared view is built with no user, so the facet asks for READ on every watched file of the tag graph. */
  @Test
  @Timeout(300)
  void userWithoutReadOnTagIsRefused() {
    load(1L, 1);
    final Set<Integer> tagBuckets = new HashSet<>(database.getSchema().getType(ColumnarTestData.TAG_TYPE).getBucketIds(true));
    final Set<Integer> edgeBuckets = new HashSet<>(database.getSchema().getType(ColumnarTestData.PARENT_EDGE).getBucketIds(true));
    // the edge lists of the tag buckets in the parent direction: a lightweight edge writes only those
    final Set<Integer> edgeLists = new HashSet<>();
    for (final int bucketId : tagBuckets)
      edgeLists.add(database.getSchema()
          .getBucketByName(((DatabaseInternal) database).getGraphEngine().getEdgesBucketName(bucketId, Vertex.DIRECTION.IN)).getFileId());
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());
    for (final Set<Integer> hidden : List.of(tagBuckets, edgeBuckets, edgeLists)) {
      context.setCurrentUser(userWithoutReadOn(hidden));
      try {
        final FacetResults results = facets(QUERY + ", b: brand TERMS TOP 5");
        final FacetResult result = results.get("t");
        assertThat(result.source()).as("hidden " + hidden).isEqualTo(FacetResult.UNAVAILABLE);
        assertThat(result.reason()).isEqualTo("hierarchy source not readable");
        assertThat(result.buckets()).isEmpty();
        assertThat(results.get("b").source()).isEqualTo("index");
      } finally {
        context.setCurrentUser(null);
      }
    }
    // the same query for a user who may read the tags
    assertEqualsReference(facet(QUERY), WHERE);
  }

  /** A user who may do everything but read the records of {@code hidden}. */
  private static SecurityDatabaseUser userWithoutReadOn(final Set<Integer> hidden) {
    return new SecurityDatabaseUser() {
      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != ACCESS.READ_RECORD || !hidden.contains(fileId);
      }

      @Override
      public String getName() {
        return "noTags";
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }
}
