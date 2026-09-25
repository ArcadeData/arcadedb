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
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.Index;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7213.
 * <p>
 * Both {@link LocalSchema#load(ComponentFile.MODE, boolean)} and {@link LocalSchema#loadIncremental} have to make a
 * component resolvable BY NAME before {@link LocalSchema#readConfiguration()} runs - that is how the logical schema
 * binds an index to its type - and {@code readConfiguration()} in turn has to run before the {@code
 * onAfterSchemaLoad()} pass, because a schema hook reads what it set. Publishing straight into the live lookup maps
 * therefore left a window in which {@code getIndexByName()} answered with an index whose schema hook had not run.
 * {@code LSMVectorIndexMutable} is the component that makes that visible: it overrides {@code onAfterSchemaLoad()}
 * ONLY, to load the index' vectors once {@code readConfiguration()} has set its dimensions, so an instance obtained
 * inside the window is an index with no vectors - a search that silently finds nothing rather than one that fails.
 * <p>
 * These tests hold a load inside that exact window and look at the schema from another thread. The window is opened
 * by a bucket component whose {@code onAfterSchemaLoad()} blocks, and the bucket is one created BEFORE the vector
 * index, so its file id is lower and the hook pass - which walks the file-id-ordered component list on the full
 * load, and {@code toInstantiate} before {@code toReplace} on the incremental one - reaches it while the vector
 * index' own hook is still pending. That is precisely the state the issue describes.
 */
class Issue7213SchemaLoadPublicationBarrierTest extends TestHelper {

  private static final String TYPE_NAME  = "Issue7213Doc";
  private static final String INDEX_NAME = TYPE_NAME + "[embedding]";

  /** Set while a test wants one named bucket to block in its schema hook; {@code null} the rest of the time. */
  private static final AtomicReference<BlockingHook> BLOCK = new AtomicReference<>();

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".embedding ARRAY_OF_FLOATS");
      // The vector index is created AFTER the type's bucket, so the bucket's file id is the lower of the two.
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
   * The full rebuild. Inside the window the answer is the PREVIOUS generation's instance (issue #7963; before it,
   * "not found") - what it must never be is the freshly built vector index, which at that moment has loaded no
   * vectors.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aFullLoadDoesNotPublishAnIndexBeforeItsSchemaHookHasRun() throws Exception {
    final LocalSchema schema = schema();
    final String blockingBucketName = firstBucketNameOf(schema);
    final Index indexBefore = schema.getIndexByName(INDEX_NAME);
    final IndexInternal bucketLevelIndexBefore = bucketLevelVectorIndex(schema);
    final String bucketLevelIndexName = bucketLevelIndexBefore.getName();
    final Bucket bucketBefore = schema.getBucketByName(blockingBucketName);
    final int indexesBefore = schema.getIndexes().length;
    final int bucketsBefore = schema.getBuckets().size();

    final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
    final AtomicReference<Throwable> loadFailure = new AtomicReference<>();
    final Thread loader = new Thread(() -> {
      try {
        schema.load(ComponentFile.MODE.READ_WRITE, true);
      } catch (final Throwable t) {
        loadFailure.set(t);
      }
    }, "issue7213-full-load");

    try {
      loader.start();
      assertThat(hook.entered.await(60, TimeUnit.SECONDS))
          .as("the load must reach the onAfterSchemaLoad() pass, which is the window under test")
          .isTrue();

      // THE ASSERTION THE ISSUE IS ABOUT. Before the fix the rebuilt LSMVectorIndex was already in indexMap here,
      // with onAfterSchemaLoad() - and therefore its vector load - still pending.
      assertThat(schema.getIndexByName(INDEX_NAME))
          .as("an index whose onAfterSchemaLoad() has not run must not be reachable by name")
          .isSameAs(indexBefore);
      assertThat(schema.getIndexByName(bucketLevelIndexName))
          .as("neither is the bucket-level component the wrapper is built over")
          .isSameAs(bucketLevelIndexBefore);
      assertThat(schema.getBucketByName(blockingBucketName))
          .as("the bucket lookup map publishes on the same barrier as the index one")
          .isSameAs(bucketBefore);
      // getIndexes()/getBuckets() carry their own merge logic, so they get their own assertion rather than being
      // assumed to follow existsIndex()/existsBucket().
      assertThat(schema.getIndexes())
          .as("the bulk index accessor must not expose the staged components either")
          .hasSize(indexesBefore)
          .contains(indexBefore, bucketLevelIndexBefore);
      assertThat(new ArrayList<Bucket>(schema.getBuckets()))
          .as("nor must the bulk bucket accessor")
          .hasSize(bucketsBefore)
          .contains(bucketBefore);
    } finally {
      hook.release.countDown();
      loader.join(TimeUnit.MINUTES.toMillis(1));
      restoreRealBuckets(schema);
    }

    assertThat(loadFailure.get()).isNull();
    assertThat(loader.isAlive()).isFalse();

    // The other side of the same barrier, and the reason the load does not trip over its own staging: while the
    // second thread above could see only the previous index, the LOADING thread saw the new one - it has to, or readConfiguration()
    // would not find the components it has just built.
    assertThat(hook.indexVisibleToTheLoadingThread.get())
        .as("the loading thread must see through the barrier at the very moment another thread cannot")
        .isTrue();

    // ...and the barrier publishes: the rebuilt index is reachable again, is a NEW instance, and has actually
    // loaded its vectors, which is what makes the search below answer at all.
    assertThat(schema.existsIndex(INDEX_NAME)).isTrue();
    assertThat(schema.getIndexByName(INDEX_NAME)).isNotSameAs(indexBefore);
    assertThat(schema.existsBucket(blockingBucketName)).isTrue();
    assertNearestNeighbourIsA();
  }

  /**
   * The incremental refresh, which is the path an HA follower takes for every applied schema entry (#6988). Here the
   * guarantee is stronger than on the full load: the index it replaces keeps answering with the instance the
   * previous load published - fully initialized - until the replacement has run its own schema hook.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void anIncrementalRefreshKeepsTheOldIndexUntilTheNewOnesSchemaHookHasRun() throws Exception {
    final LocalSchema schema = schema();
    final Index indexBefore = schema.getIndexByName(INDEX_NAME);
    final int vectorIndexFileId = bucketLevelVectorIndex(schema).getFileIds().getFirst();

    // A bucket file the FileManager knows and no component is registered for: exactly what a follower sees when an
    // earlier entry created the file, and what puts this bucket in loadIncremental's `toInstantiate` pass, which
    // runs its hooks before the replaced index' one.
    final String blockingBucketName = "issue7213blockingbucket";
    final int blockingBucketFileId = database.getSchema().createBucket(blockingBucketName).getFileId();
    schema.removeFile(blockingBucketFileId);
    assertThat(schema.getFileByIdIfExists(blockingBucketFileId)).isNull();

    final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
    final AtomicReference<Throwable> loadFailure = new AtomicReference<>();
    final AtomicReference<Boolean> incremental = new AtomicReference<>();
    final Thread loader = new Thread(() -> {
      try {
        incremental.set(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(vectorIndexFileId)));
      } catch (final Throwable t) {
        loadFailure.set(t);
      }
    }, "issue7213-incremental-load");

    try {
      loader.start();
      assertThat(hook.entered.await(60, TimeUnit.SECONDS))
          .as("the refresh must reach the onAfterSchemaLoad() pass, which is the window under test")
          .isTrue();

      // THE ASSERTION THE ISSUE IS ABOUT. Before the fix replaceLoadedComponent had already swapped the new,
      // vector-less LSMVectorIndex into indexMap by this point.
      assertThat(schema.getIndexByName(INDEX_NAME))
          .as("a replaced index must keep answering with the previous, fully loaded instance until the "
              + "replacement has run its schema hook")
          .isSameAs(indexBefore);
      // Identity is the mechanism; this is the guarantee a user would notice. The previous instance has its vectors
      // loaded, so the search still answers while the replacement is mid-build.
      assertThat(neighbors())
          .as("the previously published index must stay searchable until the replacement is ready")
          .containsExactly("a");
    } finally {
      hook.release.countDown();
      loader.join(TimeUnit.MINUTES.toMillis(1));
      restoreRealBuckets(schema);
    }

    assertThat(loadFailure.get()).isNull();
    assertThat(incremental.get()).as("the refresh must have been expressible incrementally").isTrue();

    assertThat(schema.getIndexByName(INDEX_NAME))
        .as("the replacement is published once its schema hook has run")
        .isNotSameAs(indexBefore);
    assertThat(schema.existsBucket(blockingBucketName)).isTrue();
    assertNearestNeighbourIsA();
  }

  /**
   * The barrier must not be a way for a load to lose components: a plain incremental refresh that blocks nothing
   * still has to leave every name it staged published, and the logical schema wired to it.
   */
  @Test
  void anUnblockedRefreshPublishesEverythingItStaged() throws Exception {
    final LocalSchema schema = schema();

    final String addedBucket = "issue7213plainbucket";
    final int addedFileId = database.getSchema().createBucket(addedBucket).getFileId();
    schema.removeFile(addedFileId);

    assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isTrue();

    assertThat(schema.existsBucket(addedBucket)).isTrue();
    assertThat(schema.getBucketByName(addedBucket).getFileId()).isEqualTo(addedFileId);
    assertThat(schema.existsIndex(INDEX_NAME)).isTrue();
    assertThat(schema.getType(TYPE_NAME).getAllIndexes(false)).isNotEmpty();
    assertNearestNeighbourIsA();
  }

  /**
   * A second load arriving while one is staging must be refused, and the refusal must cost the schema nothing.
   * Racing two threads into {@code beginStagedPublication()} would test the refusal only by luck, so this pins it
   * the deterministic way: hold the first load inside its hook pass, then try the second from the test thread and
   * look at what the refusal left behind. The "left behind" half is the point - a refusal that had already run
   * {@code load()}'s clears would have emptied the live schema for the load still legitimately in flight.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aSecondLoadIsRefusedWhileOneIsStagingAndCostsTheSchemaNothing() throws Exception {
    final LocalSchema schema = schema();
    final String blockingBucketName = firstBucketNameOf(schema);
    final Index indexBefore = schema.getIndexByName(INDEX_NAME);

    final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
    final AtomicReference<Throwable> loadFailure = new AtomicReference<>();
    final Thread loader = new Thread(() -> {
      try {
        schema.load(ComponentFile.MODE.READ_WRITE, true);
      } catch (final Throwable t) {
        loadFailure.set(t);
      }
    }, "issue7213-first-load");

    try {
      loader.start();
      assertThat(hook.entered.await(60, TimeUnit.SECONDS)).isTrue();

      assertThatThrownBy(() -> schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of()))
          .as("an incremental refresh cannot start while a full load is staging")
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("already in flight");

      assertThatThrownBy(() -> schema.load(ComponentFile.MODE.READ_WRITE, true))
          .as("neither can a second full load")
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("already in flight");
    } finally {
      hook.release.countDown();
      loader.join(TimeUnit.MINUTES.toMillis(1));
      restoreRealBuckets(schema);
    }

    // The refused loads must not have touched anything the first one was in the middle of rebuilding.
    assertThat(loadFailure.get()).as("the refusals must not have broken the load that owned the window").isNull();
    assertThat(schema.existsIndex(INDEX_NAME)).isTrue();
    assertThat(schema.getIndexByName(INDEX_NAME)).isNotSameAs(indexBefore);
    assertThat(schema.existsBucket(blockingBucketName)).isTrue();
    assertNearestNeighbourIsA();
  }

  /**
   * The {@code finally} on both load paths is a guarantee, not an implementation detail: a load that dies inside the
   * hook pass must leave the staging window closed, or the next load refuses to start and the database never comes
   * back. Nothing else in the suite kills a load between {@code beginStagedPublication()} and its commit.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aLoadThatThrowsInsideTheWindowLeavesNothingStaged() throws Exception {
    final LocalSchema schema = schema();
    final String blockingBucketName = firstBucketNameOf(schema);
    final IndexInternal bucketLevelIndexBefore = bucketLevelVectorIndex(schema);
    // Armed by the fixture's writes. The failed load below must leave it armed: a load that dies publishes nothing,
    // so the instances still published are the live ones, and retiring them (issue #8310) would silently end their
    // background rebuilds.
    assertThat(inactivityTimerThreadsOf(bucketLevelIndexBefore)).hasSize(1);

    final BlockingHook hook = installBlockingBucket(schema, blockingBucketName);
    hook.failInsteadOfBlocking = true;
    try {
      assertThatThrownBy(() -> schema.load(ComponentFile.MODE.READ_WRITE, true))
          .as("the fixture must actually break this load")
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("issue7213");
    } finally {
      restoreRealBuckets(schema);
    }

    assertThat(bucketLevelVectorIndex(schema))
        .as("a load that fails leaves the previous generation published (issue #7963)")
        .isSameAs(bucketLevelIndexBefore);
    assertThat(inactivityTimerThreadsOf(bucketLevelIndexBefore))
        .as("and leaves it running: only what the failed load built itself is retired (issue #8310)")
        .hasSize(1);

    // The window has to be closed, which a second load is the only honest way to observe: beginStagedPublication()
    // refuses outright when one is still open.
    schema.load(ComponentFile.MODE.READ_WRITE, true);

    assertThat(schema.existsIndex(INDEX_NAME)).isTrue();
    assertThat(schema.existsBucket(blockingBucketName)).isTrue();
    assertNearestNeighbourIsA();
  }

  /**
   * Registers a bucket factory handler that answers a blocking component for {@code bucketName} and the real
   * {@link LocalBucket} for every other bucket file, so only one component in the load stalls.
   */
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
    /** Throw out of the schema hook instead of parking in it, for the load-dies-mid-window case. */
    private volatile boolean     failInsteadOfBlocking;
    /** What the LOADING thread saw for the vector index while parked in the hook - the other side of the barrier. */
    private final AtomicReference<Boolean> indexVisibleToTheLoadingThread = new AtomicReference<>();

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
      hook.indexVisibleToTheLoadingThread.set(
          ((DatabaseInternal) getDatabase()).getSchema().getEmbedded().existsIndex(INDEX_NAME));

      hook.entered.countDown();
      if (hook.failInsteadOfBlocking)
        throw new IllegalStateException("issue7213 deliberate failure inside the schema hook pass");
      try {
        hook.release.await(1, TimeUnit.MINUTES);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      super.onAfterSchemaLoad();
    }
  }

  /**
   * The search every test ends on. On failure it says what state the vector index the query hit was in, because the
   * two ways this can go wrong look identical from the result alone - an empty list - and differ in exactly what
   * those counters show: a graph never built ({@code graphNodeCount} 0) against locations never loaded (issue #8178).
   */
  private void assertNearestNeighbourIsA() {
    final List<String> found = neighbors();
    assertThat(found)
        .as(() -> "nearest neighbour of the first fixture vector; vector index stats: "
            + bucketLevelVectorIndex(schema()).getStats())
        .containsExactly("a");
  }

  private static List<Thread> inactivityTimerThreadsOf(final IndexInternal index) {
    final String name = "VectorIndex-InactivityTimer-" + index.getName();
    final List<Thread> result = new ArrayList<>();
    for (final Thread t : Thread.getAllStackTraces().keySet())
      if (t.isAlive() && t.getName().equals(name))
        result.add(t);
    return result;
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

  /**
   * The bucket-level {@code LSMVectorIndex} behind the {@code MyType[myProperty]} wrapper: the component whose file
   * an entry writes into, and whose {@code onAfterSchemaLoad()} loads the vectors.
   */
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
