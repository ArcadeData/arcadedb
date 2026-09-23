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
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Component;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue7963FullSchemaLoadKeepsPreviousGenerationTest extends TestHelper {

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
