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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.IndexInternal;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue6988IncrementalSchemaLoadTest extends TestHelper {

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
