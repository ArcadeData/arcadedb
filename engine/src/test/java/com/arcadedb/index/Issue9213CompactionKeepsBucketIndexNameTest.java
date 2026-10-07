/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9213. A compaction swaps a new mutable file named {@code <prefix>_<nanoTime>} into an
 * {@link LSMTreeIndex} but keeps the index's logical name, while every re-read of the schema (a restart, the full
 * {@code load()} an HA follower takes for a compaction entry, an incremental refresh) built the bucket index from the
 * file and so named it after the new file. The live instance and any reloaded one then disagreed on the name, which
 * {@code TypeIndex.equals} compares, so {@code DatabaseComparator} reported the type as configured differently.
 */
class Issue9213CompactionKeepsBucketIndexNameTest extends TestHelper {
  private static final String TYPE_NAME  = "Issue9213Item";
  private static final int    PAGE_SIZE  = 8192;
  private static final int    RECORDS    = 700;
  private static final int    GEO_POINTS = 1_000;

  @Test
  void bucketIndexKeepsItsNameAcrossCompactionAndRestart() throws Exception {
    final List<String> keys = createAndFill(false);
    final String nameBefore = bucketIndex().getName();

    compact();
    assertThat(bucketIndex().getMostRecentFileName()).as("the compaction must have swapped in a new mutable file")
        .isNotEqualTo(nameBefore);
    assertThat(bucketIndex().getName()).isEqualTo(nameBefore);

    reopenDatabase();

    assertThat(bucketIndex().getName()).as("a restart must keep the name the live index answered to")
        .isEqualTo(nameBefore);
    assertThat(database.getSchema().existsIndex(nameBefore)).isTrue();
    assertIndexServes(keys);

    // A second compaction after the restart, and a second restart, keep the same name.
    insertMore(keys);
    compact();
    reopenDatabase();
    assertThat(bucketIndex().getName()).isEqualTo(nameBefore);
    assertIndexServes(keys);
  }

  @Test
  void uniqueBucketIndexKeepsItsNameAcrossCompactionAndRestart() throws Exception {
    final List<String> keys = createAndFill(true);
    final String nameBefore = bucketIndex().getName();

    compact();
    reopenDatabase();

    assertThat(bucketIndex().getName()).isEqualTo(nameBefore);
    assertIndexServes(keys);
  }

  /**
   * The path an HA follower takes for a compaction schema entry (the entry retires a file): a full schema
   * {@code load()} on a live database, compared against the instance the leader keeps.
   */
  @Test
  void fullSchemaLoadKeepsTheBucketIndexName() throws Exception {
    final List<String> keys = createAndFill(false);
    final String nameBefore = bucketIndex().getName();
    compact();

    ((LocalSchema) database.getSchema().getEmbedded()).load(ComponentFile.MODE.READ_WRITE, true);

    final LSMTreeIndex reloaded = bucketIndex();
    assertThat(reloaded.getName()).isEqualTo(nameBefore);
    assertThat(database.getSchema().getIndexByName(nameBefore)).isSameAs(reloaded);
    assertThat(database.getSchema().existsIndex(reloaded.getMostRecentFileName()))
        .as("the index must not stay registered under its file name as well").isFalse();
    assertIndexServes(keys);
  }

  @Test
  void incrementalSchemaLoadKeepsTheBucketIndexAttached() throws Exception {
    final List<String> keys = createAndFill(false);
    final String nameBefore = bucketIndex().getName();
    compact();

    // Persist the post-compaction schema (the logical name included) and re-read it incrementally.
    database.getSchema().getEmbedded().saveConfiguration();
    ((LocalSchema) database.getSchema().getEmbedded()).loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());

    assertThat(bucketIndex().getName()).isEqualTo(nameBefore);
    assertThat(database.getSchema().getType(TYPE_NAME).getIndexesByProperties("key")).hasSize(1);
    assertIndexServes(keys);
  }

  /**
   * A full-text index is an {@link LSMTreeIndex} behind a wrapper the load re-creates from the schema entry: the wrapper
   * answers to the inner index's name, so it must come back registered under the logical name too.
   */
  @Test
  void fullTextIndexKeepsItsNameAcrossCompactionAndRestart() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("text", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "text" })
          .withType(Schema.INDEX_TYPE.FULL_TEXT).withPageSize(PAGE_SIZE).create();
    });
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE_NAME).set("text", "word" + i + " other" + (i * 7) + " shared").save();
    });

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("text").getFirst();
    final IndexInternal bucketIndex = typeIndex.getIndexesOnBuckets()[0];
    final String nameBefore = bucketIndex.getName();
    final LSMTreeIndex lsm = (LSMTreeIndex) ((LSMTreeIndexMutable) bucketIndex.getComponent()).getMainIndex();
    database.async().waitCompletion();
    lsm.scheduleCompaction();
    assertThat(lsm.compact()).isTrue();

    reopenDatabase();

    final TypeIndex reloaded = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("text").getFirst();
    assertThat(reloaded.getIndexesOnBuckets()[0].getName()).isEqualTo(nameBefore);
    assertThat(database.getSchema().getIndexByName(nameBefore).getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);
    assertThat(reloaded.get(new Object[] { "word42" }).hasNext()).isTrue();
  }

  /** Same as the full-text case, for the geospatial wrapper. */
  @Test
  void geospatialIndexKeepsItsNameAcrossCompactionAndRestart() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("location", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "location" })
          .withType(Schema.INDEX_TYPE.GEOSPATIAL).withPageSize(PAGE_SIZE).create();
    });
    database.transaction(() -> {
      for (int i = 0; i < GEO_POINTS; i++)
        database.newDocument(TYPE_NAME).set("location", "POINT (" + (i % 170) + "." + i + " " + (i % 80) + ".5)").save();
    });

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("location").getFirst();
    final IndexInternal bucketIndex = typeIndex.getIndexesOnBuckets()[0];
    final String nameBefore = bucketIndex.getName();
    final LSMTreeIndex lsm = (LSMTreeIndex) ((LSMTreeIndexMutable) bucketIndex.getComponent()).getMainIndex();
    database.async().waitCompletion();
    assertThat(lsm.getMutableIndex().getTotalPages()).as("the compaction needs at least 2 mutable pages to run")
        .isGreaterThanOrEqualTo(2);
    lsm.scheduleCompaction();
    assertThat(lsm.compact()).isTrue();
    assertThat(lsm.getMostRecentFileName()).isNotEqualTo(nameBefore);

    reopenDatabase();

    final TypeIndex reloaded = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("location").getFirst();
    assertThat(reloaded.getIndexesOnBuckets()[0].getName()).isEqualTo(nameBefore);
    assertThat(database.getSchema().getIndexByName(nameBefore).getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
  }

  /** An index that never compacted still answers to its file's name, and its schema entry stays exactly as before. */
  @Test
  void aNeverCompactedIndexWritesNoLogicalName() {
    createAndFill(false);
    assertThat(bucketIndex().getName()).isEqualTo(bucketIndex().getMostRecentFileName());
    assertThat(schemaIndexEntry().has("logicalName")).isFalse();
  }

  @Test
  void aCompactedIndexPersistsItsLogicalName() throws Exception {
    createAndFill(false);
    final String nameBefore = bucketIndex().getName();
    compact();
    assertThat(schemaIndexEntry().getString("logicalName")).isEqualTo(nameBefore);
  }

  private JSONObject schemaIndexEntry() {
    final JSONObject indexes = database.getSchema().getEmbedded().toJSON().getJSONObject("types")
        .getJSONObject(TYPE_NAME).getJSONObject("indexes");
    return indexes.getJSONObject(bucketIndex().getMostRecentFileName());
  }

  private List<String> createAndFill(final boolean unique) {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("key", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "key" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(unique).withPageSize(PAGE_SIZE).create();
    });

    final List<String> keys = new ArrayList<>(RECORDS * 2);
    insertMore(keys);
    return keys;
  }

  private void insertMore(final List<String> keys) {
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        final String key = UUID.randomUUID().toString();
        keys.add(key);
        database.newDocument(TYPE_NAME).set("key", key).save();
      }
    });
  }

  private void compact() throws Exception {
    final LSMTreeIndex index = bucketIndex();
    assertThat(index.getMutableIndex().getTotalPages()).as("the compaction needs at least 2 mutable pages to run")
        .isGreaterThanOrEqualTo(2);
    // Kept below the auto-compaction threshold, so nothing else has scheduled it; drained anyway for safety.
    database.async().waitCompletion();
    index.scheduleCompaction();
    assertThat(index.compact()).as("the compaction must actually run").isTrue();
  }

  private void assertIndexServes(final List<String> keys) {
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("key").getFirst();
    assertThat(typeIndex.countEntries()).isEqualTo(keys.size());
    for (int i = 0; i < keys.size(); i += 97)
      assertThat(typeIndex.get(new Object[] { keys.get(i) }).hasNext()).as("key %s", keys.get(i)).isTrue();
  }

  private LSMTreeIndex bucketIndex() {
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("key").getFirst();
    return (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
  }
}
