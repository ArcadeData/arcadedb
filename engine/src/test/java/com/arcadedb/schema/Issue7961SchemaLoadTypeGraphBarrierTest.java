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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7961, the half of issue #7213 that fix left open.
 * <p>
 * #7213 staged the by-NAME lookup maps, so a component reaches {@code indexMap}/{@code bucketMap} only once its
 * {@code onAfterSchemaLoad()} has run. The LOGICAL schema graph was not staged: {@code readConfiguration()} opened
 * with {@code types.clear()} and rebuilt the {@link LocalDocumentType} objects in place, and
 * {@code LocalDocumentType.addIndexInternal()} binds a bucket-level index into its type WHILE that runs - which is
 * by construction before the hook pass. So a reader resolving an index through its TYPE rather than by name -
 * {@code getType(t).getAllIndexes()}, which is what SQL query planning uses to pick an index for a {@code WHERE}
 * clause - could still obtain an {@code LSMVectorIndex} whose vectors had not been loaded: a search that silently
 * finds nothing. The same reader could also observe a type mid-rebuild, missing properties, buckets or indexes,
 * on every {@code load()} and every {@code loadIncremental()}.
 * <p>
 * These tests hold a load inside the hook pass and look at the TYPE GRAPH from another thread. The window is opened
 * the same way #7213's tests open it, with a bucket whose schema hook parks; what is asserted is what the second
 * thread can reach THROUGH a type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7961SchemaLoadTypeGraphBarrierTest extends TestHelper {

  private static final String TYPE_NAME  = "Issue7961Doc";
  private static final String INDEX_NAME = TYPE_NAME + "[embedding]";

  /** Set while a test wants one named bucket to block in its schema hook; {@code null} the rest of the time. */
  private static final AtomicReference<BlockingHook> BLOCK = new AtomicReference<>();

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE', idPropertyName: 'name' }");
    });

    database.transaction(() -> {
      database.newDocument(TYPE_NAME).set("name", "a").set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      database.newDocument(TYPE_NAME).set("name", "b").set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
      database.newDocument(TYPE_NAME).set("name", "c").set("embedding", new float[] { 0.0f, 0.0f, 1.0f }).save();
    });
  }

  /**
   * The full rebuild. The type graph is replaced in one reference swap at the barrier, so a reader either sees the
   * graph the previous load published - whole, with its fully initialised indexes - or the new one, also whole.
   * What it may never see is the graph that is being assembled.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aFullLoadDoesNotPublishATypeBeforeItsIndexesSchemaHooksHaveRun() throws Exception {
    final LocalSchema schema = schema();
    final String blockingBucketName = firstBucketNameOf(schema);
    final DocumentType typeBefore = schema.getType(TYPE_NAME);
    final IndexInternal indexBefore = bucketLevelVectorIndex(schema);

    final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
    final AtomicReference<Throwable> loadFailure = new AtomicReference<>();
    final Thread loader = new Thread(() -> {
      try {
        schema.load(ComponentFile.MODE.READ_WRITE, true);
      } catch (final Throwable t) {
        loadFailure.set(t);
      }
    }, "issue7961-full-load");

    try {
      loader.start();
      assertThat(hook.entered.await(60, TimeUnit.SECONDS))
          .as("the load must reach the onAfterSchemaLoad() pass, which is the window under test")
          .isTrue();

      // THE ASSERTION THE ISSUE IS ABOUT. Before the fix `types` had been cleared and refilled in place by this
      // point, so the type resolved to a HALF-REBUILT object carrying the freshly built vector index - whose
      // onAfterSchemaLoad(), and therefore whose vector load, was still pending.
      assertThat(schema.getType(TYPE_NAME))
          .as("a type whose indexes' schema hooks have not run must not have replaced the published one")
          .isSameAs(typeBefore);
      assertThat(typeIndexes(schema))
          .as("and an index resolved THROUGH that type must be the previous, fully loaded instance")
          .contains(indexBefore);

      // The user-visible form of the same claim: the query planner picks its index through the type, so the search
      // has to keep answering while the rebuild is in flight.
      assertThat(neighbors())
          .as("a search that resolves its index through the type must keep working across a reload")
          .containsExactly("a");
    } finally {
      hook.release.countDown();
      loader.join(TimeUnit.MINUTES.toMillis(1));
      restoreRealBuckets(schema);
    }

    assertThat(loadFailure.get()).isNull();
    assertThat(loader.isAlive()).isFalse();

    // The other side of the barrier: the LOADING thread has to see the graph it is assembling, or
    // readConfiguration() could not wire up the types it has just built.
    assertThat(hook.typeVisibleToTheLoadingThread.get())
        .as("the loading thread must see its own type graph at the very moment another thread cannot")
        .isTrue();

    // ...and the barrier publishes: a new graph, a new type object, a new index behind it, and a working search.
    assertThat(schema.getType(TYPE_NAME)).isNotSameAs(typeBefore);
    assertThat(bucketLevelVectorIndex(schema)).isNotSameAs(indexBefore);
    assertThat(neighbors()).containsExactly("a");
  }

  /**
   * The incremental refresh, which is the path an HA follower takes for every applied schema entry (#6988). It
   * rebuilds the logical schema through the very same {@code readConfiguration()}, so the same window exists there
   * - once per applied entry, on a database that is serving reads throughout.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void anIncrementalRefreshDoesNotPublishATypeMidRebuild() throws Exception {
    final LocalSchema schema = schema();
    final DocumentType typeBefore = schema.getType(TYPE_NAME);
    final int vectorIndexFileId = bucketLevelVectorIndex(schema).getFileIds().getFirst();

    // A bucket file the FileManager knows and no component is registered for: what a follower sees when an earlier
    // entry created the file, and what puts this bucket in loadIncremental's `toInstantiate` pass.
    final String blockingBucketName = "issue7961blockingbucket";
    final int blockingBucketFileId = database.getSchema().createBucket(blockingBucketName).getFileId();
    schema.removeFile(blockingBucketFileId);

    final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
    final AtomicReference<Throwable> loadFailure = new AtomicReference<>();
    final Thread loader = new Thread(() -> {
      try {
        schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(vectorIndexFileId));
      } catch (final Throwable t) {
        loadFailure.set(t);
      }
    }, "issue7961-incremental-load");

    try {
      loader.start();
      assertThat(hook.entered.await(60, TimeUnit.SECONDS))
          .as("the refresh must reach the onAfterSchemaLoad() pass, which is the window under test")
          .isTrue();

      assertThat(schema.getType(TYPE_NAME))
          .as("a refresh must keep serving the published type until the replacement graph is ready")
          .isSameAs(typeBefore);
      assertThat(schema.getType(TYPE_NAME).getAllIndexes(false))
          .as("and that type must still carry its indexes - never be observed mid-rebuild without them")
          .isNotEmpty();
      assertThat(neighbors())
          .as("so a search resolving its index through the type keeps answering")
          .containsExactly("a");
    } finally {
      hook.release.countDown();
      loader.join(TimeUnit.MINUTES.toMillis(1));
      restoreRealBuckets(schema);
    }

    assertThat(loadFailure.get()).isNull();
    assertThat(schema.getType(TYPE_NAME)).isNotSameAs(typeBefore);
    assertThat(neighbors()).containsExactly("a");
  }

  /**
   * The barrier must not be a way for a load to lose the graph: an ordinary reload with nothing blocking has to
   * leave every type published, wired to its buckets, properties and indexes.
   */
  @Test
  void anUnblockedLoadPublishesTheWholeGraph() throws Exception {
    final LocalSchema schema = schema();

    schema.load(ComponentFile.MODE.READ_WRITE, true);

    final DocumentType type = schema.getType(TYPE_NAME);
    assertThat(type).isNotNull();
    assertThat(type.getPropertyNames()).contains("name", "embedding");
    assertThat(type.getBuckets(false)).isNotEmpty();
    assertThat(type.getAllIndexes(false)).isNotEmpty();
    assertThat(schema.getTypes()).extracting(DocumentType::getName).contains(TYPE_NAME);
    assertThat(neighbors()).containsExactly("a");
  }

  /**
   * A type dropped from {@code schema.json} between two loads must be GONE after the second, which is the property
   * a swap has and a merge would not: the graph is replaced, not added to.
   */
  @Test
  void aReloadReplacesTheGraphRatherThanMergingIntoIt() throws Exception {
    final LocalSchema schema = schema();

    database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE Issue7961Temporary"));
    assertThat(schema.existsType("Issue7961Temporary")).isTrue();

    database.transaction(() -> database.command("sql", "DROP TYPE Issue7961Temporary"));
    schema.load(ComponentFile.MODE.READ_WRITE, true);

    assertThat(schema.existsType("Issue7961Temporary"))
        .as("a reload must not resurrect a type the schema no longer carries")
        .isFalse();
    assertThat(schema.existsType(TYPE_NAME)).isTrue();
    assertThat(neighbors()).containsExactly("a");
  }

  // ---------- fixture ----------

  private BlockingHook installBlockingBucket(final LocalSchema schema, final String bucketName) {
    final BlockingHook hook = new BlockingHook(bucketName);
    BLOCK.set(hook);
    schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new BlockingBucketFactoryHandler());
    return hook;
  }

  private void restoreRealBuckets(final LocalSchema schema) {
    BLOCK.set(null);
    schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new LocalBucket.PaginatedComponentFactoryHandler());
  }

  private static final class BlockingHook {
    private final String                   bucketName;
    private final CountDownLatch           entered                       = new CountDownLatch(1);
    private final CountDownLatch           release                       = new CountDownLatch(1);
    /** What the LOADING thread saw for its own type while parked in the hook - the other side of the barrier. */
    private final AtomicReference<Boolean> typeVisibleToTheLoadingThread = new AtomicReference<>();

    private BlockingHook(final String bucketName) {
      this.bucketName = bucketName;
    }
  }

  private static final class BlockingBucketFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      final BlockingHook hook = BLOCK.get();
      if (hook != null && hook.bucketName.equals(name))
        return new BlockingBucket(hook, database, name, filePath, id, mode, pageSize, version);
      return new LocalBucket(database, name, filePath, id, mode, pageSize, version);
    }
  }

  /**
   * A real bucket in every respect except that its schema hook parks until the test lets it through. That is the
   * only way to observe the window from another thread: it exists entirely inside one call to {@code load()}.
   */
  private static final class BlockingBucket extends LocalBucket {
    private final BlockingHook hook;

    private BlockingBucket(final BlockingHook hook, final DatabaseInternal database, final String name,
        final String filePath, final int id, final ComponentFile.MODE mode, final int pageSize, final int version)
        throws IOException {
      super(database, name, filePath, id, mode, pageSize, version);
      this.hook = hook;
    }

    @Override
    public void onAfterSchemaLoad() {
      // Recorded rather than asserted here: an assertion error thrown out of a load hook would surface as a broken
      // load rather than as a failed test.
      final LocalSchema schema = ((DatabaseInternal) getDatabase()).getSchema().getEmbedded();
      hook.typeVisibleToTheLoadingThread.set(
          schema.existsType(TYPE_NAME) && !schema.getType(TYPE_NAME).getAllIndexes(false).isEmpty());

      hook.entered.countDown();
      try {
        hook.release.await(1, TimeUnit.MINUTES);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      super.onAfterSchemaLoad();
    }
  }

  /** The nearest neighbour of the first fixture vector. Answers nothing at all when the index loaded no vectors. */
  private List<String> neighbors() {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(`vector.neighbors`(?, ?, ?)))",
        INDEX_NAME, new float[] { 1.0f, 0.0f, 0.0f }, 1)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  /** Every bucket-level index reachable THROUGH the type, which is the path this issue is about. */
  private static List<IndexInternal> typeIndexes(final LocalSchema schema) {
    final List<IndexInternal> indexes = new ArrayList<>();
    for (final TypeIndex typeIndex : schema.getType(TYPE_NAME).getAllIndexes(true))
      indexes.addAll(List.of(typeIndex.getIndexesOnBuckets()));
    return indexes;
  }

  private static IndexInternal bucketLevelVectorIndex(final LocalSchema schema) {
    return ((TypeIndex) schema.getIndexByName(INDEX_NAME)).getIndexesOnBuckets()[0];
  }

  private static String firstBucketNameOf(final LocalSchema schema) {
    return schema.getType(TYPE_NAME).getBuckets(false).getFirst().getName();
  }

  private LocalSchema schema() {
    return ((DatabaseInternal) database).getSchema().getEmbedded();
  }
}
