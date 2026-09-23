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
import com.arcadedb.engine.Component;
import com.arcadedb.exception.SchemaException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
 * These tests hold a load inside that exact window, in the same way {@code Issue7213SchemaLoadPublicationBarrierTest}
 * does - a bucket whose {@code onAfterSchemaLoad()} parks - and then look at the file-id lookup from another
 * thread while recording what the LOADING thread sees at the same instant.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7962SchemaLoadFileIdBarrierTest extends TestHelper {

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
