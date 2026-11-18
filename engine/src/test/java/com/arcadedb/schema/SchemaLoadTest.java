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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBloomFilter;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the load of the schema from its files, full and incremental, one nested class per fixed issue.
 */
class SchemaLoadTest {
  /**
   * Regression for #6988: the incremental schema load.
   * <p>
   * Regression test for issue #6988.
   * <p>
   * {@link LocalSchema#load(ComponentFile.MODE, boolean)} is a from-scratch rebuild: it drops every
   * {@code Component} instance in the database and re-instantiates one per file. The HA follower apply path ran it
   * once per committed DDL entry, which made building a schema cost O(entries x total files) - quadratic in the
   * number of types (issue #6982: a 1209-type schema took about 2h53m to replicate).
   * <p>
   * These tests pin the invariant that makes the fix a fix, and they do it by IDENTITY rather than by elapsed time:
   * an incremental refresh must leave every untouched {@code Component} instance exactly as it found it, while the
   * full rebuild replaces all of them. That distinction is what "O(changed files)" means in practice, and unlike a
   * wall-clock budget it cannot pass or fail on how loaded the machine is.
   */
  @Nested
  class IncrementalSchemaLoad extends TestHelper {

    private static final int TYPE_COUNT = 12;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        for (int i = 0; i < TYPE_COUNT; i++) {
          final DocumentType type = database.getSchema().createDocumentType("Issue6988Type_" + i);
          type.createProperty("id", Type.STRING);
          type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
        }
      });
    }

    @Test
    void incrementalRefreshKeepsEveryUntouchedComponentInstance() throws Exception {
      final LocalSchema schema = schema();
      final Map<Integer, Component> before = componentsByFileId(schema);
      assertThat(before).as("the fixture must register several components to make the comparison meaningful").hasSizeGreaterThan(TYPE_COUNT);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of()))
          .as("an entry that adds and removes nothing is expressible incrementally")
          .isTrue();

      final Map<Integer, Component> after = componentsByFileId(schema);
      assertThat(after.keySet()).isEqualTo(before.keySet());
      for (final Map.Entry<Integer, Component> entry : before.entrySet())
        assertThat(after.get(entry.getKey()))
            .as("component for file %d must not be re-instantiated by an incremental refresh", entry.getKey())
            .isSameAs(entry.getValue());

      // The logical schema is still refreshed from schema.json, exactly as the full rebuild refreshes it.
      for (int i = 0; i < TYPE_COUNT; i++) {
        final String typeName = "Issue6988Type_" + i;
        assertThat(schema.existsType(typeName)).isTrue();
        assertThat(schema.getType(typeName).existsProperty("id")).isTrue();
        assertThat(schema.getType(typeName).getAllIndexes(false)).isNotEmpty();
      }
    }

    @Test
    void fullLoadReplacesEveryComponentInstance() throws Exception {
      final LocalSchema schema = schema();
      final Map<Integer, Component> before = componentsByFileId(schema);

      schema.load(ComponentFile.MODE.READ_WRITE, true);

      final Map<Integer, Component> after = componentsByFileId(schema);
      assertThat(after.keySet()).isEqualTo(before.keySet());
      // This is the cost issue #6988 is about: every file in the database pays for every single applied entry.
      for (final Map.Entry<Integer, Component> entry : before.entrySet())
        assertThat(after.get(entry.getKey()))
            .as("the full rebuild is expected to re-instantiate the component for file %d", entry.getKey())
            .isNotSameAs(entry.getValue());
    }

    @Test
    void incrementalRefreshRegistersFilesThatHaveNoComponentYet() throws Exception {
      final LocalSchema schema = schema();

      final Bucket newBucket = database.getSchema().createBucket("issue6988newbucket");
      final int addedFileId = newBucket.getFileId();

      // Reproduce the state an HA follower is in when a committed schema entry reaches the refresh: the file exists
      // and is registered in the FileManager (createNewFiles made it), but no Component is registered for it yet.
      schema.removeFile(addedFileId);
      assertThat(schema.getFileByIdIfExists(addedFileId)).isNull();

      final Map<Integer, Component> before = componentsByFileId(schema);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of()))
          .as("a plain bucket addition is expressible incrementally")
          .isTrue();

      final Component added = schema.getFileByIdIfExists(addedFileId);
      assertThat(added).as("the unregistered file must be registered by the incremental refresh").isNotNull();
      assertThat(added.getName()).isEqualTo("issue6988newbucket");
      assertThat(schema.getBucketByName("issue6988newbucket").getFileId()).isEqualTo(addedFileId);

      for (final Map.Entry<Integer, Component> entry : before.entrySet())
        assertThat(schema.getFileByIdIfExists(entry.getKey()))
            .as("component for file %d must not be re-instantiated when another file is added", entry.getKey())
            .isSameAs(entry.getValue());
    }

    /**
     * The case that makes deriving the new components from the {@code FileManager} - rather than from the entry's
     * {@code filesToAdd} - a correctness requirement rather than a convenience.
     * <p>
     * A schema change split across several Raft entries (#5443) creates its files in a leading chunk whose apply
     * deliberately skips the refresh, and publishes the schema in the last one, whose {@code filesToAdd} does not name
     * them. If the refresh registered only the publishing entry's own files, {@link LocalSchema#readConfiguration()}
     * would find the index name in {@code schema.json} with nothing behind it, drop the reference AND persist the
     * drop (the #4083 self-heal path) - so the follower would lose the index for good, silently.
     */
    @Test
    void incrementalRefreshRecoversAnIndexFileLeftUnregisteredByAnEarlierEntry() throws Exception {
      final LocalSchema schema = schema();

      final String typeName = "Issue6988Type_0";
      final IndexInternal index = firstBucketIndexOf(schema, typeName);
      final String indexName = index.getName();
      final int indexFileId = index.getComponent().getFileId();

      // Exactly the state such a chunk leaves behind: the file is in the FileManager, nothing is registered for it.
      schema.removeFile(indexFileId);
      schema.indexMap.remove(indexName);
      assertThat(schema.getFileByIdIfExists(indexFileId)).isNull();

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isTrue();

      assertThat(schema.getFileByIdIfExists(indexFileId))
          .as("the index file left behind by an earlier entry must be registered by this refresh")
          .isNotNull();
      assertThat(schema.existsIndex(indexName)).isTrue();
      assertThat(schema.getType(typeName).getAllIndexes(false))
          .as("the index must still be attached to its type, not dropped from the schema and saved away")
          .isNotEmpty();
    }

    /**
     * A component this entry wrote pages into has to pick up what those pages changed - an LSM mutable index' page 0
     * carries its key types, its sub-index file id and its mutable page count - but it must NOT do so by re-running
     * {@code onAfterLoad()} on the live instance. Those hooks write plain, non-volatile fields that
     * {@code LSMTreeIndex.getKeyTypes()}/{@code getBinaryKeyTypes()}/{@code convertKeys()} read WITHOUT the index
     * lock, and their safety rests on publish-once, mutate-never-after: it is why a compaction's
     * {@code splitIndex()} swaps in a NEW instance instead of updating the old one. On a follower the Raft apply
     * thread runs concurrently with query threads, so this test pins the shape rather than trying to race it: the
     * refreshed component must be a different object, and the surviving ones must not be.
     */
    @Test
    void aTouchedIndexComponentIsRebuiltRatherThanRefreshedInPlace() throws Exception {
      final LocalSchema schema = schema();

      final int indexFileId = firstBucketIndexOf(schema, "Issue6988Type_0").getComponent().getFileId();
      final Component indexBefore = schema.getFileByIdIfExists(indexFileId);
      final Map<Integer, Component> before = componentsByFileId(schema);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(indexFileId))).isTrue();

      assertThat(schema.getFileByIdIfExists(indexFileId))
          .as("a touched index component must be REBUILT, never mutated in place while readers may hold it")
          .isNotSameAs(indexBefore);

      for (final Map.Entry<Integer, Component> entry : before.entrySet())
        if (entry.getKey() != indexFileId)
          assertThat(schema.getFileByIdIfExists(entry.getKey()))
              .as("component for file %d was not touched by this entry and must survive it", entry.getKey())
              .isSameAs(entry.getValue());

      // The rebuilt component has to end up wired into the logical schema exactly as the full load would leave it.
      assertThat(schema.getType("Issue6988Type_0").getAllIndexes(false)).isNotEmpty();
      assertThat(firstBucketIndexOf(schema, "Issue6988Type_0").getComponent())
          .isSameAs(schema.getFileByIdIfExists(indexFileId));
    }

    /**
     * The rebuild gate keys on {@code getMainComponent() instanceof IndexInternal}, so it covers every index kind by
     * construction - but the other tests only ever exercise an {@code LSM_TREE}, and the two index kinds that are NOT
     * an LSM tree are precisely the ones with something to lose. {@code HashIndexBucket} re-reads its metadata in
     * BOTH load hooks, and {@code LSMVectorIndexMutable} overrides {@code onAfterSchemaLoad()} only, loading its
     * vectors there once {@code readConfiguration()} has set its dimensions - which makes it the one component whose
     * usable state is established entirely after publication. This pins that both go through the same
     * build-then-swap the LSM tree does, rather than being refreshed in place or skipped.
     */
    @Test
    void aTouchedHashOrVectorIndexComponentIsRebuiltToo() throws Exception {
      final LocalSchema schema = schema();

      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue6988Hashed").createProperty("id", Type.STRING)
            .createIndex(Schema.INDEX_TYPE.HASH, true);
        database.getSchema().createDocumentType("Issue6988Vectored").createProperty("embedding", Type.ARRAY_OF_FLOATS);
        database.command("sql",
            "CREATE INDEX ON Issue6988Vectored (embedding) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE'}");
      });

      for (final String typeName : List.of("Issue6988Hashed", "Issue6988Vectored")) {
        final int indexFileId = firstBucketIndexOf(schema, typeName).getComponent().getFileId();
        final Component indexBefore = schema.getFileByIdIfExists(indexFileId);
        final Map<Integer, Component> before = componentsByFileId(schema);

        assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(indexFileId)))
            .as("an entry writing pages into a '%s' index is expressible incrementally", typeName)
            .isTrue();

        assertThat(schema.getFileByIdIfExists(indexFileId))
            .as("the touched %s index component must be rebuilt, not mutated in place", typeName)
            .isNotSameAs(indexBefore);

        for (final Map.Entry<Integer, Component> entry : before.entrySet())
          if (entry.getKey() != indexFileId)
            assertThat(schema.getFileByIdIfExists(entry.getKey()))
                .as("component for file %d was not touched by this entry and must survive it", entry.getKey())
                .isSameAs(entry.getValue());

        // ...and it has to come back wired into the logical schema, which for a vector index means it also ran the
        // onAfterSchemaLoad() that loads its vectors.
        assertThat(schema.getType(typeName).getAllIndexes(false)).isNotEmpty();
        assertThat(firstBucketIndexOf(schema, typeName).getComponent())
            .isSameAs(schema.getFileByIdIfExists(indexFileId));
      }
    }

    /**
     * A bucket - or the dictionary - carries no state its load hooks would re-derive ({@code Component} leaves both
     * hooks empty, and {@code TransactionManager.applyChanges} reloads the dictionary itself), so naming one in
     * {@code touchedFileIds} must cost nothing at all.
     */
    @Test
    void aTouchedNonIndexComponentIsLeftAlone() throws Exception {
      final LocalSchema schema = schema();

      final int bucketFileId = schema.getType("Issue6988Type_0").getBuckets(false).getFirst().getFileId();
      final Map<Integer, Component> before = componentsByFileId(schema);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(bucketFileId))).isTrue();

      for (final Map.Entry<Integer, Component> entry : before.entrySet())
        assertThat(schema.getFileByIdIfExists(entry.getKey()))
            .as("component for file %d must not be re-instantiated for a bucket write", entry.getKey())
            .isSameAs(entry.getValue());
    }

    @Test
    void retiredFilesForceTheFullRebuildWithoutTouchingAnything() throws Exception {
      final LocalSchema schema = schema();
      final int someFileId = schema.getType("Issue6988Type_0").getBuckets(false).getFirst().getFileId();
      final Map<Integer, Component> before = componentsByFileId(schema);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(someFileId), Set.of()))
          .as("a retired file leaves stale by-name entries behind, so only the full rebuild can express it")
          .isFalse();

      // A refusal must be a no-op, otherwise the caller's fallback would run over half-applied state.
      assertThat(componentsByFileId(schema)).isEqualTo(before);
    }

    @Test
    void aSchemaThatWasNeverLoadedForcesTheFullRebuild() throws Exception {
      // A LocalSchema with no dictionary is what a database looks like before its first load(): there is no baseline
      // for an incremental refresh to add to, so the only honest answer is to hand the caller back to load().
      final LocalSchema neverLoaded = new LocalSchema((DatabaseInternal) database, database.getDatabasePath(), null);

      assertThat(neverLoaded.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isFalse();
    }

    /**
     * The three component kinds whose load has a side effect on ANOTHER component. Each is planted as a file the
     * {@code FileManager} knows and nothing is registered for - the state the follower is in when such a file arrives
     * - and each must send the caller to the full rebuild rather than being instantiated in isolation.
     */
    @Test
    void anUnregisteredDictionaryCompactedIndexOrBloomFilterForcesTheFullRebuild() throws Exception {
      final LocalSchema schema = schema();

      for (final String extension : List.of("dict", "uctidx", "nuctidx", "bfidx")) {
        final Map<Integer, Component> before = componentsByFileId(schema);
        final int ghostFileId = plantUnregisteredFile(extension);
        try {
          assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of()))
              .as("an unregistered '%s' file cannot be added to a live schema in isolation", extension)
              .isFalse();

          // A refusal must be a no-op, so the caller's fallback runs over consistent state.
          assertThat(componentsByFileId(schema)).isEqualTo(before);
        } finally {
          ((DatabaseInternal) database).getFileManager().dropFile(ghostFileId);
        }
      }
    }

    /**
     * Registers a file with the {@code FileManager} without building a component for it. The name shape is the one
     * every component file on disk carries, {@code <name>.<fileId>.<pageSize>.v<version>.<ext>}, because that is what
     * the file id and the extension are parsed back out of.
     */
    private int plantUnregisteredFile(final String extension) throws Exception {
      final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
      final int fileId = fileManager.newFileId();
      fileManager.getOrCreateFile(fileId,
          database.getDatabasePath() + File.separator + "issue6988ghost." + fileId + ".65536.v0." + extension);
      return fileId;
    }

    private static IndexInternal firstBucketIndexOf(final LocalSchema schema, final String typeName) {
      return schema.getType(typeName).getAllIndexes(false).iterator().next().getIndexesOnBuckets()[0];
    }

    private LocalSchema schema() {
      return ((DatabaseInternal) database).getSchema().getEmbedded();
    }

    /**
     * Snapshots the {@code Component} instance currently registered for every file the database holds. Comparing two
     * such snapshots by identity is what distinguishes an incremental refresh from a full rebuild.
     */
    private Map<Integer, Component> componentsByFileId(final LocalSchema schema) {
      final Map<Integer, Component> result = new HashMap<>();
      for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles()) {
        if (file == null)
          continue;
        final Component component = schema.getFileByIdIfExists(file.getFileId());
        if (component != null)
          result.put(file.getFileId(), component);
      }
      return result;
    }
  }

  /**
   * Regression for #7213: the publication barrier of the schema load.
   * <p>
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
  @Nested
  class SchemaLoadPublicationBarrier extends TestHelper {

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

  /**
   * Regression for #7266: the second pass of the incremental schema load.
   * <p>
   * Regression test for finding 3 of issue #7266.
   * <p>
   * {@link LocalSchema#loadIncremental} decides in two passes, and its second one - the files this entry wrote pages
   * into - narrowed on {@code getMainComponent() instanceof IndexInternal} BEFORE consulting the extension set that
   * names the components which cannot be refreshed in isolation. A bloom filter answers ITSELF from
   * {@code getMainComponent()} and is not an index, so a touched {@code .bfidx} short-circuited out of the loop and
   * never reached the guard: the entry stayed on the incremental path with the filter's in-RAM directory still
   * describing the file as it was before the entry appended to it. The class javadoc and the {@code attachBloomFilters}
   * skip comment both claimed the fallback happened "in both passes", so the guard a reader relies on to know when a
   * rebuild is forced said one thing and did another.
   * <p>
   * The dictionary is the deliberate exception, and the last test pins it: every DDL entry that adds a type or a
   * property name writes dictionary pages, so those file ids arrive in {@code touchedFileIds} on the common path.
   * Sending them to the full rebuild would undo issue #6988 - which is the whole reason this method exists - and the
   * second pass's own comment already argues the dictionary needs nothing here.
   */
  @Nested
  class IncrementalSchemaSecondPass extends TestHelper {

    private static final String TYPE_NAME = "Issue7266Indexed";
    /**
     * Enough keys, at the 1024-byte index page size below, for the compaction to write a multi-page series with a
     * bloom filter over it - which is what these tests need - and no more. Sized down from 10,000 after a review
     * asked whether the class had earned {@code @Tag("slow")}: at this size the whole class runs in about a second,
     * so it belongs in the default lane.
     */
    private static final int    RECORDS   = 2_000;

    @Override
    protected void beginTest() {
      // A small index page size is what makes the compacted output span several pages, i.e. a real series with a real
      // bloom filter over it - the same fixture shape IndexMetadataTest.IndexCursorFollowUps uses. Without it the whole
      // index fits one page and the compactor has nothing to write.
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
        type.createProperty("value", Integer.class);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "value" }).withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(true).withPageSize(1024).create();
      });

      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE_NAME).set("value", i).save();
      });

      compactTheIndex();
    }

    /**
     * The defect as reported: a touched bloom filter is not an {@code IndexInternal}, so the narrowing dropped it
     * before the extension check could refuse the entry.
     */
    @Test
    void aTouchedBloomFilterForcesTheFullRebuild() throws Exception {
      final LocalSchema schema = schema();
      final int filterFileId = fileIdOfRegistered(schema, LSMTreeIndexBloomFilter.class);
      assertThat(filterFileId)
          .as("the fixture must produce a bloom filter component, otherwise this test asserts nothing")
          .isNotNegative();

      final Map<Integer, Component> before = componentsByFileId(schema);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(filterFileId)))
          .as("an entry writing pages into a bloom filter cannot be expressed incrementally")
          .isFalse();

      // A refusal must be a no-op, so the caller's fallback runs over consistent state.
      assertThat(componentsByFileId(schema)).isEqualTo(before);
    }

    /**
     * A touched compacted index refuses today too, but only as a side effect: its {@code getMainComponent()} answers
     * the mutable index that {@code LSMTreeIndexMutable.onAfterLoad} wired into it, and a compacted component the
     * factory builds starts with that field null. Nothing in the guard said so, so this pins the refusal itself
     * rather than the accident that produced it.
     */
    @Test
    void aTouchedCompactedIndexForcesTheFullRebuild() throws Exception {
      final LocalSchema schema = schema();
      final int compactedFileId = fileIdOfRegistered(schema, LSMTreeIndexCompacted.class);
      assertThat(compactedFileId)
          .as("the fixture must produce a compacted index component, otherwise this test asserts nothing")
          .isNotNegative();

      final Map<Integer, Component> before = componentsByFileId(schema);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(compactedFileId)))
          .as("an entry writing pages into a compacted index cannot be expressed incrementally")
          .isFalse();

      assertThat(componentsByFileId(schema)).isEqualTo(before);
    }

    /**
     * The other half of the contract, and the one a wholesale "check the extension first" fix would have broken: a
     * touched dictionary stays on the incremental path. Every DDL entry that adds a name writes dictionary pages, so
     * refusing here would send the common case back to the O(total files) rebuild #6988 removed.
     */
    @Test
    void aTouchedDictionaryStaysIncremental() throws Exception {
      final LocalSchema schema = schema();
      final int dictionaryFileId = fileIdOfRegistered(schema, Dictionary.class);
      assertThat(dictionaryFileId).as("every database has a dictionary").isNotNegative();

      final Map<Integer, Component> before = componentsByFileId(schema);

      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(dictionaryFileId)))
          .as("a DDL entry writing new names into the dictionary is the common case and must stay incremental")
          .isTrue();

      for (final Map.Entry<Integer, Component> entry : before.entrySet())
        assertThat(schema.getFileByIdIfExists(entry.getKey()))
            .as("component for file %d must not be re-instantiated for a dictionary write", entry.getKey())
            .isSameAs(entry.getValue());
    }

    private void compactTheIndex() {
      final LSMTreeIndex index = (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator()
          .next().getIndexesOnBuckets()[0];
      try {
        if (index.scheduleCompaction())
          index.compact();
      } catch (final Exception e) {
        throw new IllegalStateException("cannot compact the fixture index", e);
      }
    }

    /** The file id of the first registered component of the given class, or -1 when the fixture produced none. */
    private int fileIdOfRegistered(final LocalSchema schema, final Class<? extends Component> componentClass) {
      for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles()) {
        if (file == null)
          continue;
        final Component component = schema.getFileByIdIfExists(file.getFileId());
        if (componentClass.isInstance(component))
          return component.getFileId();
      }
      return -1;
    }

    private LocalSchema schema() {
      return ((DatabaseInternal) database).getSchema().getEmbedded();
    }

    private Map<Integer, Component> componentsByFileId(final LocalSchema schema) {
      final Map<Integer, Component> result = new HashMap<>();
      for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles()) {
        if (file == null)
          continue;
        final Component component = schema.getFileByIdIfExists(file.getFileId());
        if (component != null)
          result.put(file.getFileId(), component);
      }
      return result;
    }
  }

  /**
   * Regression for #11301: one property the schema loader could not restore aborted the property loop, so every
   * property, every type index and the inheritance of every type after it vanished from the opened schema, a UNIQUE index stopped
   * being enforced, and the next DDL statement saved the truncated schema over {@code schema.json}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaLoadPropertyRecovery extends TestHelper {

    /**
     * The trigger the issue reported: a schema whose {@code LIST OF} property names a type that is gone. {@code DROP TYPE} reached it
     * until it refused a type a property is declared OF (issue #11305), so it is now the schema of a database written before that, or
     * a file edited by hand. Before the fix the reopen threw out of {@code setOfType} for the LIST branch only (EMBEDDED and LINK
     * already accepted a missing type while loading).
     */
    @Test
    void aListOfADroppedTypeReloadsAndTheTypesAfterItKeepTheirSchema() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Address");
      database.command("sql", "CREATE DOCUMENT TYPE AaaHolder");
      database.command("sql", "CREATE PROPERTY AaaHolder.tags LIST OF Address");
      database.command("sql", "CREATE PROPERTY AaaHolder.items MAP OF Address");
      database.command("sql", "CREATE PROPERTY AaaHolder.one EMBEDDED OF Address");
      database.command("sql", "CREATE PROPERTY AaaHolder.link LINK OF Address");
      createGuardedType();
      assertThatThrownBy(() -> database.command("sql", "DROP TYPE Address")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("AaaHolder.");
      database.close();

      editSchema(root -> root.getJSONObject("types").remove("Address"));

      database = factory.open();

      final DocumentType holder = database.getSchema().getType("AaaHolder");
      assertThat(holder.getProperty("tags").getOfType()).isEqualTo("Address");
      assertThat(holder.getProperty("items").getOfType()).isEqualTo("Address");
      assertThat(holder.getProperty("one").getOfType()).isEqualTo("Address");
      assertThat(holder.getProperty("link").getOfType()).isEqualTo("Address");
      assertGuardedTypeIntact();

      // the next DDL statement saves the schema: it must still carry everything
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
      reopenDatabase();
      assertThat(database.getSchema().getType("AaaHolder").getProperty("tags").getOfType()).isEqualTo("Address");
      assertGuardedTypeIntact();
    }

    /**
     * The loader itself: an attribute a setter refuses on load costs that attribute only. Every other attribute of the property, and
     * every property and index of the other types, is restored, and the refused attribute is written back as it was read, so the
     * next save does not lose what the file declared.
     */
    @Test
    void anAttributeTheLoaderCannotRestoreCostsOnlyThatAttribute() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE AaaHolder");
      database.command("sql", "CREATE PROPERTY AaaHolder.payload STRING (mandatory true, notnull true)");
      database.command("sql", "CREATE PROPERTY AaaHolder.other STRING (readonly true)");
      createGuardedType();
      database.close();

      editSchema(root -> root.getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties").getJSONObject("payload")
          .put("compression", "no-such-codec"));

      database = factory.open();

      final Property payload = database.getSchema().getType("AaaHolder").getProperty("payload");
      assertThat(payload.isMandatory()).isTrue();
      assertThat(payload.isNotNull()).isTrue();
      assertThat(payload.getCompression()).isEqualTo("none");
      assertThat(database.getSchema().getType("AaaHolder").getProperty("other").isReadonly()).isTrue();
      assertGuardedTypeIntact();

      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
      assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties")
          .getJSONObject("payload").getString("compression")).isEqualTo("no-such-codec");

      // setting the attribute again replaces what the file held
      database.command("sql", "ALTER PROPERTY AaaHolder.payload COMPRESSION 'fast'");
      assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties")
          .getJSONObject("payload").getString("compression")).isEqualTo("fast");
      reopenDatabase();
      assertThat(database.getSchema().getType("AaaHolder").getProperty("payload").getCompression()).isEqualTo("fast");
    }

    /**
     * A property the loader cannot create at all (a type this release does not know) is not served, keeps its definition in the
     * schema file, and does not take the rest of the schema with it. DROP PROPERTY removes the definition.
     */
    @Test
    void aPropertyTheLoaderCannotCreateKeepsItsDefinitionAndCostsNothingElse() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE AaaHolder");
      database.command("sql", "CREATE PROPERTY AaaHolder.future STRING");
      database.command("sql", "CREATE PROPERTY AaaHolder.present STRING (mandatory true)");
      createGuardedType();
      database.close();

      editSchema(root -> root.getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties").getJSONObject("future")
          .put("type", "NO_SUCH_TYPE"));

      database = factory.open();

      final DocumentType holder = database.getSchema().getType("AaaHolder");
      assertThat(holder.existsProperty("future")).isFalse();
      assertThat(holder.getProperty("present").isMandatory()).isTrue();
      assertGuardedTypeIntact();

      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
      assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties")
          .getJSONObject("future").getString("type")).isEqualTo("NO_SUCH_TYPE");

      database.command("sql", "DROP PROPERTY AaaHolder.future");
      assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties").has("future")).isFalse();
      reopenDatabase();
      assertThat(database.getSchema().getType("AaaHolder").existsProperty("future")).isFalse();
      assertGuardedTypeIntact();
    }

    /**
     * DROP TYPE takes a kept definition with the type: nothing of it comes back when a type of the same name is created again.
     */
    @Test
    void droppingTheTypeTakesTheKeptDefinitionsWithIt() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE AaaHolder");
      database.command("sql", "CREATE PROPERTY AaaHolder.future STRING");
      database.command("sql", "CREATE PROPERTY AaaHolder.payload STRING");
      database.close();

      editSchema(root -> {
        final JSONObject properties = root.getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties");
        properties.getJSONObject("future").put("type", "NO_SUCH_TYPE");
        properties.getJSONObject("payload").put("compression", "no-such-codec");
      });

      database = factory.open();
      database.command("sql", "DROP TYPE AaaHolder");
      database.command("sql", "CREATE DOCUMENT TYPE AaaHolder");
      assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties").isEmpty()).isTrue();

      reopenDatabase();
      assertThat(database.getSchema().getType("AaaHolder").getPropertyNames()).isEmpty();
    }

    /**
     * Creating a property under the name of one the loader could not create replaces the kept definition.
     */
    @Test
    void creatingThePropertyAgainReplacesTheKeptDefinition() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE AaaHolder");
      database.command("sql", "CREATE PROPERTY AaaHolder.future STRING");
      database.close();

      editSchema(root -> root.getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties").getJSONObject("future")
          .put("type", "NO_SUCH_TYPE"));

      database = factory.open();
      database.command("sql", "CREATE PROPERTY AaaHolder.future INTEGER");
      assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties")
          .getJSONObject("future").getString("type")).isEqualTo("INTEGER");
      reopenDatabase();
      assertThat(database.getSchema().getType("AaaHolder").getProperty("future").getType()).isEqualTo(Type.INTEGER);
    }

    private void createGuardedType() {
      database.command("sql", "CREATE DOCUMENT TYPE Zzz1");
      database.command("sql", "CREATE PROPERTY Zzz1.name STRING (mandatory true, notnull true)");
      database.command("sql", "CREATE INDEX ON Zzz1 (name) UNIQUE");
      database.transaction(() -> database.command("sql", "INSERT INTO Zzz1 SET name = 'keep-me'"));
    }

    private void assertGuardedTypeIntact() {
      final DocumentType zzz = database.getSchema().getType("Zzz1");
      assertThat(zzz.existsProperty("name")).isTrue();
      assertThat(zzz.getProperty("name").isMandatory()).isTrue();
      assertThat(zzz.getProperty("name").isNotNull()).isTrue();
      assertThat(database.getSchema().existsIndex("Zzz1[name]")).isTrue();
      assertThat(database.getSchema().getIndexByName("Zzz1[name]").isUnique()).isTrue();
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO Zzz1 SET name = 'keep-me'")))
          .isInstanceOf(DuplicatedKeyException.class);
      assertThat(database.query("sql", "SELECT count(*) AS n FROM Zzz1").next().<Long>getProperty("n")).isEqualTo(1L);
    }

    private interface SchemaEdit {
      void apply(JSONObject root);
    }

    private void editSchema(final SchemaEdit edit) throws Exception {
      final JSONObject root = readSchema();
      edit.apply(root);
      Files.writeString(schemaFile().toPath(), root.toString());
      // the fallback copy must not stand in for the edited primary
      final File prev = new File(getDatabasePath() + File.separator + "schema.prev.json");
      if (prev.exists())
        Files.writeString(prev.toPath(), root.toString());
    }

    private JSONObject readSchema() throws Exception {
      return new JSONObject(Files.readString(schemaFile().toPath()));
    }

    private File schemaFile() {
      return new File(getDatabasePath() + File.separator + LocalSchema.SCHEMA_FILE_NAME);
    }
  }

  /**
   * Regression for #11568: a refused index definition during an incremental reload leaves its name published.
   * <p>
   * Issue #11568, incremental reload: a refused definition of an index whose file the reload re-instantiates (a touched file id)
   * leaves its name published until the publication barrier, and a reload that aborts after the refusal leaves it published for
   * good. Withdrawing the name while the load was still staging made the previous generation, which readers keep using until the
   * barrier, lose the index, and an aborted load never put it back.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IncrementalReloadRefusal extends TestHelper {
    private static final String VECTOR_INDEX = "Vec[vector]";

    @Override
    protected void beginTest() {
      database.command("sqlscript", """
          CREATE DOCUMENT TYPE Vec BUCKETS 1;
          CREATE PROPERTY Vec.vector ARRAY_OF_FLOATS;
          CREATE INDEX ON Vec (vector) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE', quantization: 'PRODUCT', pqClusters: 16};
          CREATE DOCUMENT TYPE Other BUCKETS 1;""");
      database.transaction(() -> database.newDocument("Vec").set("vector", new float[] { 1, 0, 0, 0 }).save());
      reopenDatabase();
    }

    @Test
    void aTouchedIndexWhoseDefinitionIsRefusedStaysPublishedUntilThePublication() throws IOException {
      final LocalSchema schema = database.getSchema().getEmbedded();
      final TypeIndex typeIndex = (TypeIndex) schema.getIndexByName(VECTOR_INDEX);
      final IndexInternal bucketIndex = typeIndex.getIndexesOnBuckets()[0];
      final int vectorFileId = bucketIndex.getFileId();
      final String valid = readSchemaFile();

      // the reload re-instantiates the index file and refuses its definition, then fails on a section it reads after the indexes
      final JSONObject broken = new JSONObject(valid.replace("\"pqClusters\":16", "\"pqClusters\":512"));
      broken.put("triggers", "not an object");
      writeSchemaFile(broken.toString());
      assertThatThrownBy(() -> schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(vectorFileId)))
          .as("the fixture must actually break this load").isInstanceOf(SchemaException.class);
      assertThat(schema.getIndexByName(VECTOR_INDEX)).as("an aborted load leaves the previous generation's index published")
          .isSameAs(typeIndex);
      assertThat(schema.getIndexByName(bucketIndex.getName())).isSameAs(bucketIndex);

      // the same reload, published: the refused index goes, with its type index
      writeSchemaFile(valid.replace("\"pqClusters\":16", "\"pqClusters\":512"));
      assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(vectorFileId))).isTrue();
      assertThat(schema.existsIndex(VECTOR_INDEX)).isFalse();
      assertThat(schema.existsIndex(bucketIndex.getName())).isFalse();
      assertThat(schema.existsType("Other")).isTrue();
    }

    private String readSchemaFile() throws IOException {
      return Files.readString(new File(getDatabasePath(), "schema.json").toPath(), StandardCharsets.UTF_8);
    }

    private void writeSchemaFile(final String content) throws IOException {
      Files.writeString(new File(getDatabasePath(), "schema.json").toPath(), content, StandardCharsets.UTF_8);
    }
  }

  /**
   * Regression for #11224: a schema load that fell back to {@code schema.prev.json} must say so, so a caller that
   * needs the intended files (snapshot install) can refuse instead of going on with a database that looks fine.
   */
  @Nested
  class LoadedFromPreviousSchema extends TestHelper {
    private static final String TRUNCATED_SCHEMA = "{\"schemaVersion\":42,\"settings\":{\"dateForm";

    @Test
    void cleanLoadDoesNotReportFallback() {
      database.getSchema().createDocumentType("A");
      database.close();
      database = factory.open();
      assertThat(schema().isLoadedFromPreviousSchema()).isFalse();
    }

    @Test
    void corruptPrimaryReportsFallback() throws Exception {
      prepare();
      final File schemaFile = schemaFile();
      Files.writeString(schemaFile.toPath(), TRUNCATED_SCHEMA);
      database = factory.open();
      assertThat(database.getSchema().existsType("A")).isTrue();
      assertThat(schema().isLoadedFromPreviousSchema()).isTrue();
    }

    @Test
    void emptyPrimaryReportsFallback() throws Exception {
      prepare();
      Files.write(schemaFile().toPath(), new byte[0]);
      database = factory.open();
      assertThat(database.getSchema().existsType("A")).isTrue();
      assertThat(schema().isLoadedFromPreviousSchema()).isTrue();
    }

    @Test
    void missingPrimaryReportsFallback() throws Exception {
      prepare();
      assertThat(schemaFile().delete()).isTrue();
      database = factory.open();
      assertThat(database.getSchema().existsType("A")).isTrue();
      assertThat(schema().isLoadedFromPreviousSchema()).isTrue();
    }

    @Test
    void flagClearsOnNextCleanLoad() throws Exception {
      prepare();
      Files.writeString(schemaFile().toPath(), TRUNCATED_SCHEMA);
      database = factory.open();
      assertThat(schema().isLoadedFromPreviousSchema()).isTrue();
      // the open healed schema.json, so the next open reads the primary
      database.close();
      database = factory.open();
      assertThat(schema().isLoadedFromPreviousSchema()).isFalse();
    }

    private void prepare() {
      database.getSchema().createDocumentType("A");
      database.getSchema().createDocumentType("Marker");
      database.close();
    }

    private File schemaFile() {
      return new File(getDatabasePath() + File.separator + LocalSchema.SCHEMA_FILE_NAME);
    }

    private LocalSchema schema() {
      return ((DatabaseInternal) database).getSchema().getEmbedded();
    }
  }
}
