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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.trigger.JavaTrigger;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: schema load consistency.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class SchemaLoadConsistencyTest {
  /**
   * Regression for #7961: schema load type graph barrier.
   *
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
  @Nested
  class SchemaLoadTypeGraphBarrierTest extends TestHelper {

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

  /**
   * Regression for #7962: schema load file id barrier.
   *
   * Regression test for issue #7962, the FILE-ID half of the publication barrier #7213 built by name.
   * <p>
   * #7213 staged {@code indexMap}/{@code bucketMap}, so {@code getIndexByName()} keeps answering with the previous,
   * fully initialised instance until a replacement has run {@code onAfterSchemaLoad()}. The file-id array was left
   * out of that, deliberately: {@code readConfiguration()} and the load hooks resolve SIBLING components through it
   * while the load runs - an LSM mutable index reads its compacted sub-index by file id in {@code onAfterLoad()},
   * and the dictionary must be file-id-resolvable before it may write a missing header page - so staging it means
   * giving the loading thread an overlay of its own. Until then {@code getFileById()} handed a concurrent caller the
   * replacement while its schema hook was still pending, which for an {@code LSMVectorIndexMutable} is an index with
   * no vectors loaded.
   * <p>
   * These tests hold a load inside that exact window, in the same way {@code SchemaLoadTest.SchemaLoadPublicationBarrierTest}
   * does - a bucket whose {@code onAfterSchemaLoad()} parks - and then look at the file-id lookup from another
   * thread while recording what the LOADING thread sees at the same instant.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaLoadFileIdBarrierTest extends TestHelper {

    private static final String TYPE_NAME  = "Issue7962Doc";
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
      });
    }

    /**
     * THE ASSERTION THE ISSUE IS ABOUT. On the incremental path the replaced vector index used to take over its
     * file-id slot the instant it was built, so a concurrent {@code getFileById()} reached an index whose
     * {@code onAfterSchemaLoad()} - and therefore its vector load - had not run. It must answer with the previous
     * instance instead, exactly as {@code getIndexByName()} already did.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void anIncrementalRefreshKeepsThePreviousComponentInTheFileIdSlot() throws Exception {
      final LocalSchema schema = schema();
      final IndexInternal vectorIndex = bucketLevelVectorIndex(schema);
      final int vectorIndexFileId = vectorIndex.getFileIds().getFirst();
      final Component componentBefore = schema.getFileById(vectorIndexFileId);

      // A bucket file the FileManager knows and no component is registered for: loadIncremental's `toInstantiate`
      // pass, whose hooks run before the replaced index' one.
      final String blockingBucketName = "issue7962blockingbucket";
      final int blockingBucketFileId = database.getSchema().createBucket(blockingBucketName).getFileId();
      schema.removeFile(blockingBucketFileId);
      assertThat(schema.getFileByIdIfExists(blockingBucketFileId)).isNull();

      final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
      hook.watchedFileId = vectorIndexFileId;
      hook.watchedNewFileId = blockingBucketFileId;

      final AtomicReference<Throwable> loadFailure = new AtomicReference<>();
      final AtomicReference<Boolean> incremental = new AtomicReference<>();
      final Thread loader = new Thread(() -> {
        try {
          incremental.set(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(vectorIndexFileId)));
        } catch (final Throwable t) {
          loadFailure.set(t);
        }
      }, "issue7962-incremental-load");

      try {
        loader.start();
        assertThat(hook.entered.await(60, TimeUnit.SECONDS))
            .as("the refresh must reach the onAfterSchemaLoad() pass, which is the window under test")
            .isTrue();

        assertThat(schema.getFileById(vectorIndexFileId))
            .as("a replaced component must keep its file-id slot pointing at the previous, fully loaded instance "
                + "until the replacement has run its schema hook")
            .isSameAs(componentBefore);
        assertThat(schema.getFileByIdIfExists(blockingBucketFileId))
            .as("a brand-new component is not reachable by file id before its hook has run either")
            .isNull();
        assertThat(schema.getFileByName(blockingBucketName))
            .as("nor by the name scan over the same array")
            .isNull();
      } finally {
        hook.release.countDown();
        loader.join(TimeUnit.MINUTES.toMillis(1));
        restoreRealBuckets(schema);
      }

      assertThat(loadFailure.get()).isNull();
      assertThat(incremental.get()).as("the refresh must have been expressible incrementally").isTrue();

      // The other side of the barrier, and the reason the load does not trip over its own staging: the LOADING
      // thread has to resolve its own new components by file id, or readConfiguration() and the load hooks could
      // not find the siblings they read.
      assertThat(hook.replacedVisibleToTheLoadingThread.get())
          .as("the loading thread must resolve the REPLACEMENT by file id at the very moment another thread cannot")
          .isTrue();
      assertThat(hook.addedVisibleToTheLoadingThread.get())
          .as("and must resolve the component it has just added, too")
          .isTrue();

      // ...and the barrier publishes.
      assertThat(schema.getFileById(vectorIndexFileId)).isNotSameAs(componentBefore);
      assertThat(schema.getFileByIdIfExists(blockingBucketFileId)).isNotNull();
      assertThat(schema.getFileByName(blockingBucketName)).isNotNull();
      assertThat(neighbors()).containsExactly("a");
    }

    /**
     * #7961 keeps the PREVIOUS type graph published across a full reload precisely so a query that resolves its index
     * through a type keeps working, and reading a record through it resolves the record's bucket BY FILE ID. So the
     * full load must keep its file-id slots resolvable for the whole window - since issue #7963 to the previous
     * generation's components, which it no longer empties on the way in. This test is that guarantee, measured from
     * inside the window.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void aFullLoadKeepsItsFileIdsResolvableSoThePublishedTypeGraphStaysReadable() throws Exception {
      final LocalSchema schema = schema();
      final int vectorIndexFileId = bucketLevelVectorIndex(schema).getFileIds().getFirst();
      final String blockingBucketName = firstBucketNameOf(schema);
      final int blockingBucketFileId = schema.getBucketByName(blockingBucketName).getFileId();

      final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
      hook.watchedFileId = vectorIndexFileId;
      hook.watchedNewFileId = blockingBucketFileId;

      final AtomicReference<Throwable> loadFailure = new AtomicReference<>();
      final Thread loader = new Thread(() -> {
        try {
          schema.load(ComponentFile.MODE.READ_WRITE, true);
        } catch (final Throwable t) {
          loadFailure.set(t);
        }
      }, "issue7962-full-load");

      try {
        loader.start();
        assertThat(hook.entered.await(60, TimeUnit.SECONDS)).isTrue();

        assertThat(schema.getFileByIdIfExists(vectorIndexFileId))
            .as("a full load must keep its file-id slots resolvable while it runs")
            .isNotNull();
        assertThat(neighbors())
            .as("a search through the still-published previous type graph reads its records by file id, so the "
                + "array must not be withheld from it")
            .containsExactly("a");
      } finally {
        hook.release.countDown();
        loader.join(TimeUnit.MINUTES.toMillis(1));
        restoreRealBuckets(schema);
      }

      assertThat(loadFailure.get()).isNull();
      assertThat(schema.getFileByIdIfExists(vectorIndexFileId)).isNotNull();
      assertThat(schema.getFileByIdIfExists(blockingBucketFileId)).isNotNull();
      assertThat(neighbors()).containsExactly("a");
    }

    /**
     * The overlay IS the rollback: an incremental refresh that dies inside its hook pass must leave every file-id
     * slot exactly as it found it. Before the overlay the slots were taken immediately and had to be put back by
     * hand, and this is what proves the hand-written undo is no longer needed rather than merely deleted.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void arefreshThatThrowsInsideTheWindowLeavesEveryFileIdSlotUntouched() throws Exception {
      final LocalSchema schema = schema();
      final int vectorIndexFileId = bucketLevelVectorIndex(schema).getFileIds().getFirst();
      final Component componentBefore = schema.getFileById(vectorIndexFileId);

      final String blockingBucketName = "issue7962failingbucket";
      final int blockingBucketFileId = database.getSchema().createBucket(blockingBucketName).getFileId();
      schema.removeFile(blockingBucketFileId);

      final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
      hook.failInsteadOfBlocking = true;
      try {
        assertThatThrownBy(
            () -> schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(vectorIndexFileId)))
            .as("the fixture must actually break this refresh")
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("issue7962");
      } finally {
        restoreRealBuckets(schema);
      }

      assertThat(schema.getFileById(vectorIndexFileId))
          .as("the replaced slot must still hold the instance the aborted refresh found there")
          .isSameAs(componentBefore);
      assertThat(schema.getFileByIdIfExists(blockingBucketFileId))
          .as("and a slot the aborted refresh filled must be empty again")
          .isNull();

      // The window has to be closed too, which only a second load can show.
      schema.load(ComponentFile.MODE.READ_WRITE, true);
      assertThat(neighbors()).containsExactly("a");
    }

    /** A plain refresh must still publish every file-id slot it staged. */
    @Test
    void anUnblockedRefreshPublishesEveryFileIdItStaged() throws Exception {
      final LocalSchema schema = schema();

      final String addedBucket = "issue7962plainbucket";
      final int addedFileId = database.getSchema().createBucket(addedBucket).getFileId();
      schema.removeFile(addedFileId);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isTrue();

      assertThat(schema.getFileByIdIfExists(addedFileId)).isNotNull();
      assertThat(schema.getFileById(addedFileId).getName()).isEqualTo(addedBucket);
      assertThat(schema.getFileByName(addedBucket)).isNotNull();
      assertThat(neighbors()).containsExactly("a");
    }

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
      private final String         bucketName;
      private final CountDownLatch entered = new CountDownLatch(1);
      private final CountDownLatch release = new CountDownLatch(1);
      private volatile boolean     failInsteadOfBlocking;
      /** The file id of a component this load REPLACES, as the loading thread should see it. */
      private volatile int         watchedFileId    = -1;
      /** The file id of a component this load ADDS, likewise. */
      private volatile int         watchedNewFileId = -1;

      private final AtomicReference<Boolean> replacedVisibleToTheLoadingThread = new AtomicReference<>();
      private final AtomicReference<Boolean> addedVisibleToTheLoadingThread    = new AtomicReference<>();

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

    /** A real bucket whose schema hook parks until the test lets it through - the only way to see the window. */
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
        // Recorded rather than asserted: an assertion error thrown out of a load hook would surface as a broken load
        // rather than as a failed test.
        final LocalSchema schema = ((DatabaseInternal) getDatabase()).getSchema().getEmbedded();
        if (hook.watchedFileId >= 0)
          hook.replacedVisibleToTheLoadingThread.set(schema.getFileByIdIfExists(hook.watchedFileId) != null);
        if (hook.watchedNewFileId >= 0)
          hook.addedVisibleToTheLoadingThread.set(schema.getFileByIdIfExists(hook.watchedNewFileId) != null);

        hook.entered.countDown();
        if (hook.failInsteadOfBlocking)
          throw new IllegalStateException("issue7962 deliberate failure inside the schema hook pass");
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

  /**
   * Regression for #7963: full schema load keeps previous generation.
   *
   * Regression test for issue #7963.
   * <p>
   * {@link LocalSchema#load(ComponentFile.MODE, boolean)} used to open by emptying the by-name maps and the file-id
   * array, so every bucket and index in the database was unresolvable for the whole rebuild. On an HA follower that
   * rebuild runs on a live node whenever {@code loadIncremental()} refuses a schema entry (a retired file, a compacted
   * index, a bloom filter), and concurrent queries failed with "Bucket with id 'N' was not found" (a record read
   * resolving its bucket by file id) or "Index with name '...' was not found", although nothing was wrong.
   * <p>
   * The full load now stages everything it builds and publishes at the same barrier as the type graph (#7961): until
   * then every other thread keeps resolving the previous generation, whole.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FullSchemaLoadKeepsPreviousGenerationTest extends TestHelper {

    private static final String TYPE_NAME  = "Issue7963Doc";
    private static final String INDEX_NAME = TYPE_NAME + "[embedding]";

    private static final AtomicReference<BlockingHook> BLOCK = new AtomicReference<>();

    private RID firstRid;

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
        firstRid = database.newDocument(TYPE_NAME).set("name", "a").set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save()
            .getIdentity();
        database.newDocument(TYPE_NAME).set("name", "b").set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
      });
    }

    /** THE ASSERTION THE ISSUE IS ABOUT: from inside the window, every lookup answers with the previous generation. */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void aFullLoadKeepsServingThePreviousGenerationUntilItPublishes() throws Exception {
      final LocalSchema schema = schema();
      final String blockingBucketName = firstBucketNameOf(schema);
      final Bucket bucketBefore = schema.getBucketByName(blockingBucketName);
      final int bucketFileId = bucketBefore.getFileId();
      final Index indexBefore = schema.getIndexByName(INDEX_NAME);
      final IndexInternal bucketIndexBefore = bucketLevelVectorIndex(schema);
      final int vectorIndexFileId = bucketIndexBefore.getFileIds().getFirst();
      final Component vectorFileBefore = schema.getFileById(vectorIndexFileId);
      final int indexesBefore = schema.getIndexes().length;
      final int bucketsBefore = schema.getBuckets().size();

      final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
      final Thread loader = startFullLoad(schema, hook);

      try {
        assertThat(hook.entered.await(60, TimeUnit.SECONDS))
            .as("the load must reach the onAfterSchemaLoad() pass, which is the window under test")
            .isTrue();

        // By file id: the path of the reported "Bucket with id 'N' was not found", a record read resolving its bucket.
        assertThat(schema.getBucketById(bucketFileId, false)).isSameAs(bucketBefore);
        assertThat(schema.getFileByIdIfExists(vectorIndexFileId)).isSameAs(vectorFileBefore);
        assertThat(database.lookupByRID(firstRid, true).asDocument().getString("name")).isEqualTo("a");

        // By name.
        assertThat(schema.getBucketByName(blockingBucketName)).isSameAs(bucketBefore);
        assertThat(schema.getIndexByName(INDEX_NAME)).isSameAs(indexBefore);
        assertThat(schema.getIndexByName(bucketIndexBefore.getName())).isSameAs(bucketIndexBefore);
        assertThat(schema.getFileByName(blockingBucketName)).isSameAs(bucketBefore);

        // The bulk accessors carry their own merge logic, so they get their own assertion.
        assertThat(schema.getIndexes()).hasSize(indexesBefore);
        assertThat(schema.getBuckets()).hasSize(bucketsBefore);

        // And end to end: a scan and a vector search, both resolved through the previous generation.
        assertThat(countType()).isEqualTo(2L);
        assertThat(neighbors()).containsExactly("a");
      } finally {
        hook.release.countDown();
        loader.join(TimeUnit.MINUTES.toMillis(1));
        restoreRealBuckets(schema);
      }

      assertThat(hook.loadFailure.get()).isNull();
      assertThat(loader.isAlive()).isFalse();

      // The loading thread saw only what it built, never the previous generation it is replacing.
      assertThat(hook.loadingThreadSawTheNewBucket.get()).isTrue();

      // Published: new instances everywhere, all of them working.
      assertThat(schema.getBucketByName(blockingBucketName)).isNotSameAs(bucketBefore);
      assertThat(schema.getBucketById(bucketFileId)).isSameAs(schema.getBucketByName(blockingBucketName));
      assertThat(schema.getIndexByName(INDEX_NAME)).isNotSameAs(indexBefore);
      assertThat(schema.getFileById(vectorIndexFileId)).isNotSameAs(vectorFileBefore);
      assertThat(schema.getIndexes()).hasSize(indexesBefore);
      assertThat(schema.getBuckets()).hasSize(bucketsBefore);
      assertThat(countType()).isEqualTo(2L);
      assertThat(neighbors()).containsExactly("a");
    }

    /** A load that dies inside the window has published nothing, so the previous generation is still whole. */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void aFullLoadThatFailsLeavesThePreviousGenerationPublished() throws Exception {
      final LocalSchema schema = schema();
      final String blockingBucketName = firstBucketNameOf(schema);
      final Bucket bucketBefore = schema.getBucketByName(blockingBucketName);
      final Index indexBefore = schema.getIndexByName(INDEX_NAME);
      final Dictionary dictionaryBefore = schema.getDictionary();

      final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
      hook.failInsteadOfBlocking = true;
      final Thread loader = startFullLoad(schema, hook);
      try {
        loader.join(TimeUnit.MINUTES.toMillis(1));
      } finally {
        restoreRealBuckets(schema);
      }

      assertThat(hook.loadFailure.get()).isNotNull();
      assertThat(schema.getBucketByName(blockingBucketName)).isSameAs(bucketBefore);
      assertThat(schema.getBucketById(bucketBefore.getFileId())).isSameAs(bucketBefore);
      assertThat(schema.getIndexByName(INDEX_NAME)).isSameAs(indexBefore);
      // The dictionary is swapped in before the barrier, so the abort path has to put the previous one back: by name
      // and by file id, the same instance (PR #8228 review).
      assertThat(schema.getDictionary()).isSameAs(dictionaryBefore);
      assertThat(schema.getFileById(dictionaryBefore.getFileId())).isSameAs(dictionaryBefore);
      assertThat(countType()).isEqualTo(2L);
      assertThat(neighbors()).containsExactly("a");

      // ...and a later load still works: the refusal machinery was released.
      schema.load(ComponentFile.MODE.READ_WRITE, true);
      assertThat(countType()).isEqualTo(2L);
      assertThat(neighbors()).containsExactly("a");
    }

    /** Publication REPLACES the previous generation: a name the new one does not carry does not survive it. */
    @Test
    void aFullLoadDropsNamesTheNewGenerationDoesNotCarry() throws Exception {
      final LocalSchema schema = schema();
      final LocalBucket anyBucket = (LocalBucket) schema.getBucketByName(firstBucketNameOf(schema));
      schema.bucketMap.put("issue7963Ghost", anyBucket);
      schema.indexMap.put("issue7963GhostIndex", bucketLevelVectorIndex(schema));

      schema.load(ComponentFile.MODE.READ_WRITE, true);

      assertThat(schema.existsBucket("issue7963Ghost")).isFalse();
      assertThat(schema.existsIndex("issue7963GhostIndex")).isFalse();
      assertThat(countType()).isEqualTo(2L);
      assertThat(neighbors()).containsExactly("a");
    }

    /**
     * A commit raises the page count before the flush thread writes the page, so the file can be shorter than what is
     * committed. The component published by a full load must not count fewer pages than the one it replaces, or the
     * next allocation reuses a committed page number (PR #8228 review).
     */
    @Test
    void aFullLoadCarriesOverThePageCountThePreviousGenerationCommitted() throws Exception {
      final LocalSchema schema = schema();
      final String bucketName = firstBucketNameOf(schema);
      final LocalBucket bucketBefore = (LocalBucket) schema.getBucketByName(bucketName);
      final int pagesOnDisk = (int) (bucketBefore.getComponentFile().getSize() / bucketBefore.getPageSize());
      final int committedPages = Math.max(pagesOnDisk, bucketBefore.getCommittedPageCount()) + 1;
      // Stands in for a page committed and not flushed yet: counted by the published instance, absent from the file.
      bucketBefore.updatePageCount(committedPages);

      schema.load(ComponentFile.MODE.READ_WRITE, true);

      final LocalBucket bucketAfter = (LocalBucket) schema.getBucketByName(bucketName);
      assertThat(bucketAfter).isNotSameAs(bucketBefore);
      assertThat(bucketAfter.getCommittedPageCount()).isEqualTo(committedPages);
    }

    private Thread startFullLoad(final LocalSchema schema, final BlockingHook hook) {
      final Thread loader = new Thread(() -> {
        try {
          schema.load(ComponentFile.MODE.READ_WRITE, true);
        } catch (final Throwable t) {
          hook.loadFailure.set(t);
        }
      }, "issue7963-full-load");
      loader.start();
      return loader;
    }

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
      private final String                     bucketName;
      private final CountDownLatch             entered                      = new CountDownLatch(1);
      private final CountDownLatch             release                      = new CountDownLatch(1);
      private final AtomicReference<Throwable> loadFailure                  = new AtomicReference<>();
      private final AtomicReference<Boolean>   loadingThreadSawTheNewBucket = new AtomicReference<>();
      private volatile boolean                 failInsteadOfBlocking;

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

    /** A real bucket whose schema hook parks until the test lets it through - the only way to see the window. */
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
        // Recorded rather than asserted: an assertion error thrown out of a load hook would surface as a broken load.
        final LocalSchema schema = ((DatabaseInternal) getDatabase()).getSchema().getEmbedded();
        hook.loadingThreadSawTheNewBucket.set(schema.getBucketByName(getName()) == this
            && schema.getBucketById(getFileId()) == this);

        hook.entered.countDown();
        if (hook.failInsteadOfBlocking)
          throw new IllegalStateException("issue7963 deliberate failure inside the schema hook pass");
        try {
          hook.release.await(1, TimeUnit.MINUTES);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        super.onAfterSchemaLoad();
      }
    }

    private long countType() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

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

  /**
   * Regression for #8230: aborted schema load keeps schema members.
   *
   * Issue #8230, follow-up to #7963.
   * <ol>
   * <li>A {@code schema.json} that cannot be parsed used to be swallowed by {@code readConfiguration()}, so the empty
   * graph it had assembled was published over a perfectly good previous generation.</li>
   * <li>A full load that aborts after {@code readConfiguration()} restored the types (#7963) but not triggers,
   * materialized views, continuous aggregates, function libraries and extensions: those were replaced in place, and the
   * previous generation's trigger listeners were torn down.</li>
   * </ol>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AbortedSchemaLoadKeepsSchemaMembersTest extends TestHelper {
    private static final String TARGET = "Target8230";
    private static final String AUDIT  = "Audit8230";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE " + TARGET);
      database.command("sql", "CREATE PROPERTY " + TARGET + ".name STRING");
      database.command("sql", "CREATE DOCUMENT TYPE " + AUDIT);
      database.command("sql", "CREATE PROPERTY " + AUDIT + ".what STRING");
      database.command("sql",
          "CREATE TRIGGER audit8230 AFTER CREATE ON TYPE " + TARGET + " EXECUTE SQL \"INSERT INTO " + AUDIT
              + " SET what = 'created'\"");
      database.command("sql", "CREATE MATERIALIZED VIEW View8230 AS SELECT name FROM " + TARGET + " REFRESH INCREMENTAL");
      database.command("sql", "DEFINE FUNCTION lib8230.twice 'return x * 2' PARAMETERS [x] LANGUAGE js");
      schema().setExtension("ext8230", new JSONObject().put("k", "v"));
      schema().saveConfiguration();
    }

    /** A full load that aborts after readConfiguration() leaves every member of the previous generation live. */
    @Test
    void anAbortedFullLoadKeepsTheTriggersViewsLibrariesAndExtensionsOfThePreviousGeneration() throws Exception {
      final LocalSchema schema = schema();
      final MaterializedViewImpl viewBefore = (MaterializedViewImpl) schema.getMaterializedView("View8230");
      assertThat(viewBefore.getChangeListener()).isNotNull();

      schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new FailingBucketFactoryHandler());
      try {
        assertThatThrownBy(() -> schema.load(ComponentFile.MODE.READ_WRITE, true)).hasMessageContaining("issue8230");
      } finally {
        schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new LocalBucket.PaginatedComponentFactoryHandler());
      }

      assertThat(schema.existsTrigger("audit8230")).isTrue();
      insertTarget("after-abort");
      assertThat(auditRows()).as("the trigger of the previous generation still fires, exactly once").isEqualTo(1);

      assertThat(schema.getMaterializedView("View8230")).isSameAs(viewBefore);
      assertThat(viewBefore.getChangeListener()).as("and the view is still refreshed").isNotNull();
      assertThat(schema.hasFunctionLibrary("lib8230")).isTrue();
      assertThat(schema.getExtension("ext8230")).isNotNull();

      // A later load still works, and does not stack a second listener.
      schema.load(ComponentFile.MODE.READ_WRITE, true);
      insertTarget("after-reload");
      assertThat(auditRows()).isEqualTo(2);
      assertThat(schema.existsTrigger("audit8230")).isTrue();
      assertThat(schema.hasFunctionLibrary("lib8230")).isTrue();
      assertThat(schema.getExtension("ext8230")).isNotNull();
      assertThat(((MaterializedViewImpl) schema.getMaterializedView("View8230")).getChangeListener()).isNotNull();
    }

    /** A schema.json that cannot be parsed must leave the previous generation published, and must not be overwritten. */
    @Test
    void aSchemaFileThatCannotBeParsedLeavesThePreviousGenerationPublished() throws Exception {
      final LocalSchema schema = schema();
      final File schemaFile = new File(database.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
      final File prevFile = new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME);
      final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      root.getJSONObject("settings").remove("dateFormat");
      final String broken = root.toString();
      Files.writeString(schemaFile.toPath(), broken, StandardCharsets.UTF_8);
      // The previous copy is the fallback of a CORRUPT file only; a parseable file with a bad value is not one.
      Files.deleteIfExists(prevFile.toPath());

      assertThatThrownBy(() -> schema.load(ComponentFile.MODE.READ_WRITE, true)).isInstanceOf(SchemaException.class);

      assertThat(schema.existsType(TARGET)).isTrue();
      assertThat(schema.existsTrigger("audit8230")).isTrue();
      insertTarget("after-bad-file");
      assertThat(auditRows()).isEqualTo(1);
      assertThat(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8))
          .as("the file the operator has to fix is left as it was found").isEqualTo(broken);
    }

    private void insertTarget(final String name) {
      database.transaction(() -> database.newDocument(TARGET).set("name", name).save());
    }

    private long auditRows() {
      return database.countType(AUDIT, false);
    }

    private LocalSchema schema() {
      return ((DatabaseInternal) database).getSchema().getEmbedded();
    }

    private static final class FailingBucketFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
      @Override
      public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
          final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
        return new FailingBucket(database, name, filePath, id, mode, pageSize, version);
      }
    }

    /** A real bucket whose schema hook fails: the load dies AFTER readConfiguration() has restored the members. */
    private static final class FailingBucket extends LocalBucket {
      private FailingBucket(final DatabaseInternal database, final String name, final String filePath, final int id,
          final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
        super(database, name, filePath, id, mode, pageSize, version);
      }

      @Override
      public void onAfterSchemaLoad() {
        throw new IllegalStateException("issue8230 deliberate failure inside the schema hook pass");
      }
    }
  }

  /**
   * Regression for #8310: schema reload retires superseded vector index.
   *
   * Regression test for issue #8310.
   * <p>
   * A schema reload builds a new instance of every index it re-reads, publishes it, and until this fix left the
   * instance it replaced exactly as it was: nothing ever stopped its background work, because
   * {@code LocalDatabase} releases only the indexes the schema still lists on close and drop, and the superseded one
   * is no longer listed. An {@code LSMVectorIndex} carries an inactivity rebuild timer - one daemon {@code Timer}
   * thread per instance - armed by any write, so the retired instance woke up once the window expired and ran a full
   * graph build of its own: against the live database, contending for the JVM-wide {@code REBUILD_SEMAPHORE} and
   * persisting a graph file the NEW instance also owns, or, when the database had been dropped in between, against a
   * closed one. The CI log of issue #8178 shows the second shape: {@code SchemaLoadTest.SchemaLoadPublicationBarrierTest}
   * passed, and fifteen seconds later - the default inactivity window - a timer from one of its reloads failed with
   * {@code DatabaseIsClosedException} on a database the test had already dropped.
   * <p>
   * The same leak runs on an HA follower on every schema entry it applies, since each one reloads the schema.
   */
  @Nested
  class SchemaReloadRetiresSupersededVectorIndexTest extends TestHelper {

    private static final String TYPE_NAME           = "Issue8310Doc";
    private static final String INDEX_NAME          = TYPE_NAME + "[embedding]";
    private static final String TIMER_THREAD_PREFIX = "VectorIndex-InactivityTimer-";

    @Override
    protected void beginTest() {
      // Long enough that the timer cannot fire while a test runs: what is asserted is that it is CANCELLED, not that
      // it has not fired yet. Set on this database only, not JVM-wide, so no other test class is affected.
      ((DatabaseInternal) database).getConfiguration()
          .setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 60_000);

      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
        database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
        database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (embedding) LSM_VECTOR "
            + "METADATA { dimensions: 3, similarity: 'COSINE', idPropertyName: 'name' }");
      });

      // A handful of writes, far below the rebuild threshold: this is what arms the inactivity timer.
      database.transaction(() -> {
        database.newDocument(TYPE_NAME).set("name", "a").set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
        database.newDocument(TYPE_NAME).set("name", "b").set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
        database.newDocument(TYPE_NAME).set("name", "c").set("embedding", new float[] { 0.0f, 0.0f, 1.0f }).save();
      });
    }

    @Test
    void aFullLoadRetiresTheVectorIndexItSupersedes() throws Exception {
      final LocalSchema schema = schema();
      final IndexInternal before = bucketLevelVectorIndex(schema);
      assertThat(timerThreadsOf(before)).as("the fixture must arm the inactivity timer it then expects gone").hasSize(1);

      schema.load(ComponentFile.MODE.READ_WRITE, true);

      assertSupersededAndRetired(schema, before);
    }

    /**
     * The path an HA follower takes for every applied schema entry (#6988), where a touched index file is re-read into
     * a replacement instance while everything else keeps its instance.
     */
    @Test
    void anIncrementalRefreshRetiresTheVectorIndexItReplaces() throws Exception {
      final LocalSchema schema = schema();
      final IndexInternal before = bucketLevelVectorIndex(schema);
      assertThat(timerThreadsOf(before)).as("the fixture must arm the inactivity timer it then expects gone").hasSize(1);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(before.getFileIds().getFirst())))
          .as("the refresh must have been expressible incrementally")
          .isTrue();

      assertSupersededAndRetired(schema, before);
    }

    /**
     * The other half of the contract: an incremental refresh that does not touch the index keeps serving the SAME
     * instance, and that one must be left running. Releasing by "was published before" rather than by "is not
     * published any more" would stop the live index's timer here and silently end its background rebuilds.
     */
    @Test
    void aRefreshThatKeepsTheInstanceLeavesItRunning() throws Exception {
      final LocalSchema schema = schema();
      final IndexInternal before = bucketLevelVectorIndex(schema);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isTrue();

      assertThat(bucketLevelVectorIndex(schema)).as("an untouched index keeps its instance").isSameAs(before);
      assertThat(before.isValid()).as("the instance still serving must not be released").isTrue();
      assertThat(timerThreadsOf(before)).as("and its inactivity timer must still be armed").hasSize(1);
      assertThat(neighbors()).containsExactly("a");
    }

    /**
     * Cancelling the timer is half of it: a write that resolved the retired instance before the swap and commits after
     * it would arm a fresh one. Driven on the live instance, retired directly, because that is the only way to route
     * an ordinary committed write to an instance that has been retired.
     */
    @Test
    void aWriteToARetiredInstanceDoesNotArmItsTimerAgain() {
      final IndexInternal index = bucketLevelVectorIndex(schema());
      index.onSuperseded();
      Awaitility.await("the retired instance's inactivity timer thread exits")
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(20))
          .untilAsserted(() -> assertThat(timerThreadsOf(index)).isEmpty());

      database.transaction(() ->
          database.newDocument(TYPE_NAME).set("name", "d").set("embedding", new float[] { 1.0f, 1.0f, 0.0f }).save());

      assertThat(timerThreadsOf(index)).as("a retired instance must not arm the timer again").isEmpty();
      assertThat(neighbors()).as("and the write itself still lands").containsExactly("a");
    }

    /**
     * The same leak one door over: a dropped index leaves the schema too, and {@code drop()} marked it invalid but
     * left the timer armed, so it fired later and started a build over files the drop had just deleted.
     */
    @Test
    void droppingTheIndexStopsItsTimer() {
      final IndexInternal index = bucketLevelVectorIndex(schema());
      assertThat(timerThreadsOf(index)).as("the fixture must arm the inactivity timer it then expects gone").hasSize(1);

      database.command("sql", "DROP INDEX `" + INDEX_NAME + "`");

      Awaitility.await("the dropped index's inactivity timer thread exits")
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(20))
          .untilAsserted(() -> assertThat(timerThreadsOf(index)).isEmpty());
    }

    /**
     * {@code REBUILD INDEX} used to replace the instance by dropping the old one, which had to leave no timer behind. It is rebuilt in
     * place now: the instance stays, and the compaction that ends the rebuild moves it to a file of another name, which must not leave
     * the timer armed under the previous name either.
     */
    @Test
    void rebuildingTheIndexLeavesNoTimerUnderThePreviousName() {
      final IndexInternal before = bucketLevelVectorIndex(schema());
      assertThat(timerThreadsOf(before)).as("the fixture must arm the inactivity timer it then expects gone").hasSize(1);
      final String timerBefore = TIMER_THREAD_PREFIX + before.getName();

      database.command("sql", "REBUILD INDEX `" + INDEX_NAME + "`");

      assertThat(bucketLevelVectorIndex(schema())).as("rebuilt in place").isSameAs(before);
      Awaitility.await("the inactivity timer thread under the previous name exits")
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(20))
          .untilAsserted(() -> assertThat(Thread.getAllStackTraces().keySet().stream()
              .filter(t -> t.isAlive() && t.getName().equals(timerBefore))).isEmpty());
      assertThat(neighbors()).as("the rebuilt index answers").containsExactly("a");
    }

    private void assertSupersededAndRetired(final LocalSchema schema, final IndexInternal before) {
      final IndexInternal after = bucketLevelVectorIndex(schema);
      assertThat(after).as("the reload must have replaced the instance").isNotSameAs(before);
      assertThat(after.isValid()).as("the instance now published is live").isTrue();

      // Retired, not invalidated: a query that resolved it just before the swap may still be running on it.
      assertThat(before.isValid()).as("the superseded instance must keep answering an in-flight query").isTrue();
      // A cancelled Timer's thread exits asynchronously, so give it a bounded window. The published instance has the
      // same name, and therefore the same thread name, but has taken no write since the reload and armed nothing.
      Awaitility.await("the superseded instance's inactivity timer thread exits")
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(20))
          .untilAsserted(() -> assertThat(timerThreadsOf(before)).isEmpty());

      assertThat(neighbors()).as("the published instance still answers").containsExactly("a");
    }

    private static List<Thread> timerThreadsOf(final IndexInternal index) {
      final String name = TIMER_THREAD_PREFIX + index.getName();
      final List<Thread> result = new ArrayList<>();
      for (final Thread t : Thread.getAllStackTraces().keySet())
        if (t.isAlive() && t.getName().equals(name))
          result.add(t);
      return result;
    }

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

    private static IndexInternal bucketLevelVectorIndex(final LocalSchema schema) {
      return ((TypeIndex) schema.getIndexByName(INDEX_NAME)).getIndexesOnBuckets()[0];
    }

    private LocalSchema schema() {
      return ((DatabaseInternal) database).getSchema().getEmbedded();
    }
  }

  /**
   * Regression for #8717: schema reload keeps live counts.
   *
   * Regression test for issue #8717: a follower applying a replicated schema entry re-reads the schema through
   * {@code loadIncremental()} (or the {@code load()} fallback), and that used to re-apply the per-bucket record counts
   * of {@code statistics.json} on top of the live counters. The file is written only by a graceful close, so every
   * insert since then vanished from {@code count(*)}, although the records were intact.
   * <p>
   * The stored counts are a start-up shortcut: they are applied when the schema is first loaded, and never again on a
   * live database. A full reload, which rebuilds the bucket instances, carries each live counter over to the new instance.
   */
  @Nested
  class SchemaReloadKeepsLiveCountsTest extends TestHelper {

    private static final String TYPE     = "Counted";
    private static final int    INITIAL  = 25;
    private static final int    INSERTED = 17;

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(TYPE, 1);
      insert(INITIAL);
      assertThat(bucket().count()).isEqualTo(INITIAL);
    }

    @Test
    void anIncrementalSchemaReloadKeepsTheCountsOfInsertsMadeSinceTheLastClose() throws Exception {
      // A graceful close writes statistics.json with INITIAL, the state the follower had at its last restart.
      reopenDatabase();
      assertThat(bucket().getCachedRecordCount()).isEqualTo(INITIAL);

      insert(INSERTED);
      assertThat(bucket().count()).isEqualTo(INITIAL + INSERTED);

      schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());

      assertThat(bucket().getCachedRecordCount()).isEqualTo(INITIAL + INSERTED);
      assertThat(bucket().count()).isEqualTo(INITIAL + INSERTED);
    }

    @Test
    void aFullSchemaReloadKeepsTheCountsOfInsertsMadeSinceTheLastClose() throws Exception {
      reopenDatabase();
      insert(INSERTED);
      assertThat(bucket().count()).isEqualTo(INITIAL + INSERTED);
      final JSONArray live = new JSONArray().put(new JSONObject().put("id", 0).put("free", 1234));
      bucket().setPageStatistics(live);

      schema().load(ComponentFile.MODE.READ_WRITE, true);

      // The full load rebuilds the bucket instance: it inherits the live counter rather than the stale file value, and
      // is not left unknown either, which would cost a rescan of every bucket on the next count(*).
      assertThat(bucket().getCachedRecordCount()).isEqualTo(INITIAL + INSERTED);
      assertThat(bucket().count()).isEqualTo(INITIAL + INSERTED);
      // The live page hints are carried over too, not the stale ones of the close.
      assertThat(bucket().getStatistics().getJSONArray("pages").toString()).isEqualTo(live.toString());
    }

    @Test
    void aReloadDoesNotResurrectAStaleCountOverAnUnknownCounter() throws Exception {
      // After a crash recovery the counter is unknown (-1) and the file still holds the older close's value.
      reopenDatabase();
      insert(INSERTED);
      bucket().setCachedRecordCount(-1);

      schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());

      assertThat(bucket().getCachedRecordCount()).isEqualTo(-1L);
      assertThat(bucket().count()).isEqualTo(INITIAL + INSERTED);
    }

    @Test
    void anIncrementalSchemaReloadKeepsTheLivePageFreeSpaceHints() throws Exception {
      reopenDatabase();
      // Stand-in for the hints this bucket built since the close: statistics.json still carries the ones of the close.
      final JSONArray live = new JSONArray().put(new JSONObject().put("id", 0).put("free", 1234));
      bucket().setPageStatistics(live);

      schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());

      assertThat(bucket().getStatistics().getJSONArray("pages").toString()).isEqualTo(live.toString());
    }

    private void insert(final int count) {
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          database.newDocument(TYPE).set("id", i).save();
      });
    }

    private LocalSchema schema() {
      return (LocalSchema) database.getSchema().getEmbedded();
    }

    private LocalBucket bucket() {
      return (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
    }
  }

  /**
   * Regression for #9886: aborted schema load keeps time series files open.
   *
   * Issue #9886, the failed-load arm: a full load that dies after {@code readConfiguration()} has built a TimeSeries
   * engine per type discards that staged graph, and its engines sit on the same registered files as the engines of the
   * generation that stays published. Closing them closed the live type's files, so the next write failed with
   * "file ... is closed".
   */
  @Nested
  class AbortedSchemaLoadKeepsTimeSeriesFilesOpenTest extends TestHelper {
    private static final String TYPE = "SensorReading9886";

    @Override
    protected void beginTest() {
      // A record bucket for the failing hook below to live on: a TimeSeries type has none
      database.command("sql", "CREATE DOCUMENT TYPE Doc9886");
      database.command("sql",
          "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE) SHARDS 1");
      database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET ts = 1000, sensor_id = 'A', temperature = 22.5"));
    }

    @Test
    void anAbortedFullLoadLeavesThePublishedTimeSeriesFilesOpen() {
      final LocalSchema schema = ((DatabaseInternal) database).getSchema().getEmbedded();
      final TimeSeriesEngine before = ((LocalTimeSeriesType) schema.getType(TYPE)).getEngine();

      schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new FailingBucketFactoryHandler());
      try {
        assertThatThrownBy(() -> schema.load(ComponentFile.MODE.READ_WRITE, true)).hasMessageContaining("issue9886");
      } finally {
        schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new LocalBucket.PaginatedComponentFactoryHandler());
      }

      final TimeSeriesEngine engine = ((LocalTimeSeriesType) schema.getType(TYPE)).getEngine();
      assertThat(engine).as("the aborted load keeps the previous generation published").isSameAs(before);
      assertThat(engine.getTagDictionary().getComponentFile().isOpen()).as("tag dictionary file").isTrue();
      assertThat(engine.getShard(0).getMutableBucket().getComponentFile().isOpen()).as("mutable bucket file").isTrue();

      database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET ts = 2000, sensor_id = 'B', temperature = 23.5"));
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE)) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2L);
      }
    }

    private static final class FailingBucketFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
      @Override
      public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
          final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
        return new FailingBucket(database, name, filePath, id, mode, pageSize, version);
      }
    }

    /** A real bucket whose schema hook fails: the load dies AFTER readConfiguration() has built the TimeSeries engines. */
    private static final class FailingBucket extends LocalBucket {
      private FailingBucket(final DatabaseInternal database, final String name, final String filePath, final int id,
          final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
        super(database, name, filePath, id, mode, pageSize, version);
      }

      @Override
      public void onAfterSchemaLoad() {
        throw new IllegalStateException("issue9886 deliberate failure inside the schema hook pass");
      }
    }
  }

  /**
   * Regression for #9997: schema reload before read count.
   *
   * Issue #9997: the database counts the BEFORE READ listeners of its own registry and of every type's, and a read skips
   * the type lookup while the count is zero. A schema reload in place replaces every type with a new instance and
   * re-attaches the triggers to the new ones: the retired instances must give their count back, or each reload would
   * leave the database one listener further from the fast path. A load that aborts must leave the count as it was: it
   * attaches no trigger to the types it staged (they are registered at publication, on the published types), the
   * published generation's listeners stay registered, and dropping the trigger afterwards still reaches zero.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaReloadBeforeReadCountTest extends TestHelper {
    private static final String        TYPE  = "Reloaded9997";
    static final         AtomicInteger FIRED = new AtomicInteger();

    public static class CountingBeforeReadTrigger implements JavaTrigger {
      @Override
      public boolean execute(final Database database, final Record record, final Record oldRecord) {
        return true;
      }

      @Override
      public boolean executeBeforeRead(final Database database, final RID rid) {
        FIRED.incrementAndGet();
        return true;
      }
    }

    @BeforeEach
    void resetTrigger() {
      FIRED.set(0);
    }

    @Test
    void aSchemaReloadInPlaceSucceededOrAbortedKeepsTheCountExact() throws IOException {
      database.getSchema().createDocumentType(TYPE);
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          database.newDocument(TYPE).set("id", i).save();
      });
      database.command("sql", "CREATE TRIGGER countReload BEFORE READ ON TYPE " + TYPE + " EXECUTE JAVA '"
          + CountingBeforeReadTrigger.class.getName() + "'");
      final LocalSchema schema = ((DatabaseInternal) database).getSchema().getEmbedded();
      final DocumentType before = schema.getType(TYPE);

      schema.load(ComponentFile.MODE.READ_WRITE, true);
      schema.load(ComponentFile.MODE.READ_WRITE, true);
      assertThat(schema.getType(TYPE)).as("the reload replaced the type instance").isNotSameAs(before);
      assertThat(scan()).isEqualTo(5);
      assertThat(FIRED.get()).as("one listener after two reloads").isEqualTo(5);

      schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new FailingBucketFactoryHandler());
      try {
        assertThatThrownBy(() -> schema.load(ComponentFile.MODE.READ_WRITE, true)).hasMessageContaining("issue9997");
      } finally {
        schema.getComponentFactory().registerComponent(LocalBucket.BUCKET_EXT, new LocalBucket.PaginatedComponentFactoryHandler());
      }
      assertThat(anyBeforeReadListener()).isTrue();
      assertThat(scan()).isEqualTo(5);
      assertThat(FIRED.get()).isEqualTo(10);

      database.command("sql", "DROP TRIGGER countReload");
      assertThat(anyBeforeReadListener()).as("no retired or staged registry still counts").isFalse();
      assertThat(scan()).isEqualTo(5);
      assertThat(FIRED.get()).isEqualTo(10);
    }

    private boolean anyBeforeReadListener() {
      return ((RecordEventsRegistry) database.getEvents()).hasBeforeReadListenersInDatabase();
    }

    private int scan() {
      int rows = 0;
      try (final ResultSet rs = database.query("sql", "SELECT id FROM " + TYPE)) {
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
      }
      return rows;
    }

    private static final class FailingBucketFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
      @Override
      public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
          final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
        return new FailingBucket(database, name, filePath, id, mode, pageSize, version);
      }
    }

    /** A real bucket whose schema hook fails: the load dies after readConfiguration() staged the types. */
    private static final class FailingBucket extends LocalBucket {
      private FailingBucket(final DatabaseInternal database, final String name, final String filePath, final int id,
          final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
        super(database, name, filePath, id, mode, pageSize, version);
      }

      @Override
      public void onAfterSchemaLoad() {
        throw new IllegalStateException("issue9997 deliberate failure inside the schema hook pass");
      }
    }
  }
}
