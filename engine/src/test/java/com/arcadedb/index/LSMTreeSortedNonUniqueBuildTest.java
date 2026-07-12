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
package com.arcadedb.index;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexBuildMode;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LSMTreeSortedNonUniqueBuildTest extends TestHelper {
  @Test
  void buildsExternalRunsAcrossBucketsAndSupportsReopenAndMutation() throws Exception {
    final Path spillParent = Path.of(getDatabasePath()).resolve("sorted-spill");
    final DocumentType type = database.getSchema().buildDocumentType().withName("SortedExternal").withTotalBuckets(3).create();
    type.createProperty("lookupKey", Type.STRING);

    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++) {
        final int permuted = Math.floorMod(i * 1_237, 2_000);
        database.newDocument("SortedExternal").set("lookupKey", String.format("key-%05d-padding", permuted)).save();
      }
    });

    TypeIndex index = database.getSchema().buildTypeIndex("SortedExternal", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(1L << 20)
        .withBuildSpillDirectory(spillParent)
        .withBuildMergeFanIn(4)
        .withUnique(false)
        .withPageSize(1_024)
        .create();

    assertOrdered(index, 2_000);
    assertThat(count(index.get(new Object[] { "key-01234-padding" }))).isEqualTo(1);
    assertNoSpillDirectories(spillParent);

    reopenDatabase();
    index = database.getSchema().getType("SortedExternal").getIndexByProperties("lookupKey");
    assertOrdered(index, 2_000);

    final RID[] added = new RID[1];
    database.transaction(() -> added[0] = database.newDocument("SortedExternal").set("lookupKey", "key-new")
        .save().getIdentity());
    assertThat(count(index.get(new Object[] { "key-new" }))).isEqualTo(1);
    database.transaction(() -> database.lookupByRID(added[0], true).asDocument().delete());
    assertThat(count(index.get(new Object[] { "key-new" }))).isZero();
  }

  @Test
  void preservesListExpansionAndCollapsesRepeatedRidKeys() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("SortedList").withTotalBuckets(2).create();
    type.createProperty("tags", Type.LIST);

    database.transaction(() -> {
      database.newDocument("SortedList").set("tags", Arrays.asList("java", "database", "java")).save();
      database.newDocument("SortedList").set("tags", Arrays.asList("python", "database")).save();
      database.newDocument("SortedList").set("tags", Arrays.asList("java", "systems")).save();
    });

    final TypeIndex index = database.getSchema().buildTypeIndex("SortedList", new String[] { "tags by item" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(1L << 20)
        .withBuildMergeFanIn(2)
        .withUnique(false)
        .create();

    assertThat(count(index.get(new Object[] { "java" }))).isEqualTo(2);
    assertThat(count(index.get(new Object[] { "database" }))).isEqualTo(2);
  }

  @Test
  void buildsOneHighCardinalityKeyAcrossRunsBucketsAndRidChunks() throws Exception {
    final DocumentType type = database.getSchema().buildDocumentType().withName("SortedHighCardinality")
        .withTotalBuckets(3).create();
    type.createProperty("lookupKey", Type.STRING);

    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newDocument("SortedHighCardinality").set("lookupKey", "shared-key").save();
    });

    TypeIndex index = database.getSchema().buildTypeIndex("SortedHighCardinality", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(1L << 20)
        .withBuildMergeFanIn(3)
        .withUnique(false)
        .withPageSize(1_024)
        .create();

    assertThat(count(index.get(new Object[] { "shared-key" }))).isEqualTo(2_000);
    assertThat(count(index.iterator(true))).isEqualTo(2_000);

    reopenDatabase();
    index = database.getSchema().getType("SortedHighCardinality").getIndexByProperties("lookupKey");
    assertThat(count(index.get(new Object[] { "shared-key" }))).isEqualTo(2_000);
    assertThat(count(index.iterator(false))).isEqualTo(2_000);
  }

  @Test
  void preservesCompositeCollationAndNullSkippingAcrossSpillRuns() throws Exception {
    final DocumentType type = database.getSchema().buildDocumentType().withName("SortedComposite")
        .withTotalBuckets(2).create();
    type.createProperty("groupName", Type.STRING);
    type.createProperty("ordinal", Type.INTEGER);

    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++) {
        final boolean nullKey = i % 50 == 0;
        final String group = nullKey ? null : "Group-%02d".formatted(i % 20);
        database.newDocument("SortedComposite")
            .set("groupName", group != null && i % 2 == 0 ? group.toUpperCase() : group)
            .set("ordinal", nullKey ? null : i % 10)
            .save();
      }
    });

    TypeIndex index = database.getSchema().buildTypeIndex("SortedComposite",
            new String[] { "groupName", "ordinal" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(1L << 20)
        .withBuildMergeFanIn(2)
        .withCollations(List.of("CI", "DEFAULT"))
        .withUnique(false)
        .create();

    assertThat(count(index.iterator(true))).isEqualTo(980);
    assertThat(count(index.get(new Object[] { "GROUP-01", 1 }))).isEqualTo(50);
    assertThat(count(index.get(new Object[] { null, null }))).isZero();

    reopenDatabase();
    index = database.getSchema().getType("SortedComposite").getIndexByProperties("groupName", "ordinal");
    assertThat(count(index.iterator(false))).isEqualTo(980);
    assertThat(count(index.get(new Object[] { "group-01", 1 }))).isEqualTo(50);
  }

  @Test
  void indexesPartialNullCompositeKeysWithSkipStrategy() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("SortedPartialNullSkip");
    type.createProperty("groupName", Type.STRING);
    type.createProperty("ordinal", Type.INTEGER);

    database.transaction(() -> {
      database.newDocument("SortedPartialNullSkip").set("groupName", null).set("ordinal", 5).save();
      database.newDocument("SortedPartialNullSkip").set("groupName", "group").set("ordinal", null).save();
      database.newDocument("SortedPartialNullSkip").set("groupName", null).set("ordinal", null).save();
      database.newDocument("SortedPartialNullSkip").set("groupName", "group").set("ordinal", 7).save();
    });

    TypeIndex index = database.getSchema().buildTypeIndex("SortedPartialNullSkip",
            new String[] { "groupName", "ordinal" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.SKIP)
        .withUnique(false)
        .create();

    assertThat(count(index.iterator(true))).isEqualTo(3);
    assertThat(count(index.get(new Object[] { null, 5 }))).isEqualTo(1);
    assertThat(count(index.get(new Object[] { "group", null }))).isEqualTo(1);
    assertThat(count(index.get(new Object[] { null, null }))).isZero();

    reopenDatabase();
    index = database.getSchema().getType("SortedPartialNullSkip").getIndexByProperties("groupName", "ordinal");
    assertThat(count(index.get(new Object[] { null, 5 }))).isEqualTo(1);
    assertThat(count(index.get(new Object[] { "group", null }))).isEqualTo(1);
  }

  @Test
  void rejectsPartialNullCompositeKeysWithErrorStrategy() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("SortedPartialNullError");
    type.createProperty("groupName", Type.STRING);
    type.createProperty("ordinal", Type.INTEGER);
    database.transaction(() -> database.newDocument("SortedPartialNullError")
        .set("groupName", null).set("ordinal", 5).save());

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("SortedPartialNullError",
            new String[] { "groupName", "ordinal" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR)
        .withUnique(false)
        .create())
        .isInstanceOf(IndexException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Indexed key SortedPartialNullError[groupName, ordinal] cannot be NULL ([null, 5])");

    assertThat(type.getIndexByProperties("groupName", "ordinal")).isNull();
    reopenDatabase();
    assertThat(database.getSchema().getType("SortedPartialNullError").getIndexByProperties("groupName", "ordinal")).isNull();
  }

  @Test
  void rejectsUnsupportedUniqueBuildBeforeCreatingFiles() {
    final DocumentType type = database.getSchema().createDocumentType("SortedUnique");
    type.createProperty("lookupKey", Type.STRING);
    database.transaction(() -> database.newDocument("SortedUnique").set("lookupKey", "one").save());

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("SortedUnique", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withUnique(true)
        .create())
        .isInstanceOf(IndexException.class)
        .hasMessageContaining("non-unique");
    assertThat(type.getIndexByProperties("lookupKey")).isNull();
  }

  @Test
  void rejectsUnsupportedIndexTypeAndActiveTransaction() {
    final DocumentType type = database.getSchema().createDocumentType("SortedPreconditions");
    type.createProperty("lookupKey", Type.STRING);

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("SortedPreconditions", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.HASH)
        .withBuildMode(IndexBuildMode.SORTED)
        .withUnique(false)
        .create())
        .isInstanceOf(IndexException.class)
        .hasMessageContaining("only LSM_TREE");

    database.begin();
    try {
      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("SortedPreconditions", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withUnique(false)
          .create())
          .isInstanceOf(IndexException.class)
          .hasMessageContaining("no active transaction");
    } finally {
      database.rollback();
    }
    assertThat(type.getIndexByProperties("lookupKey")).isNull();
  }

  @Test
  void publishesEmptyIndexAndAcceptsLaterWrites() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("SortedEmpty");
    type.createProperty("lookupKey", Type.STRING);

    TypeIndex index = database.getSchema().buildTypeIndex("SortedEmpty", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withUnique(false)
        .create();
    assertThat(count(index.iterator(true))).isZero();

    database.transaction(() -> database.newDocument("SortedEmpty").set("lookupKey", "later").save());
    assertThat(count(index.get(new Object[] { "later" }))).isEqualTo(1);
    reopenDatabase();
    index = database.getSchema().getType("SortedEmpty").getIndexByProperties("lookupKey");
    assertThat(count(index.get(new Object[] { "later" }))).isEqualTo(1);
  }

  @Test
  void cleansSchemaAndSpillFilesAfterLateScanFailure() throws Exception {
    final Path spillParent = Path.of(getDatabasePath()).resolve("failed-spill");
    final DocumentType type = database.getSchema().buildDocumentType().withName("SortedFailure").withTotalBuckets(2).create();
    type.createProperty("lookupKey", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("SortedFailure").set("lookupKey", i).save();
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("SortedFailure", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(1L << 20)
        .withBuildSpillDirectory(spillParent)
        .withBuildMergeFanIn(2)
        .withUnique(false)
        .withCallback((document, total) -> {
          if (total == 500L)
            throw new IllegalStateException("injected late scan failure");
        })
        .create())
        .isInstanceOf(IndexException.class)
        .hasRootCauseMessage("injected late scan failure");

    assertThat(type.getIndexByProperties("lookupKey")).isNull();
    assertNoSpillDirectories(spillParent);
    reopenDatabase();
    assertThat(database.getSchema().getType("SortedFailure").getIndexByProperties("lookupKey")).isNull();
  }

  @Test
  void supportsMultipleCompactedSeriesAndLaterNormalCompaction() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    final DocumentType type = database.getSchema().createDocumentType("SortedMultiSeries");
    type.createProperty("lookupKey", Type.STRING);

    database.transaction(() -> {
      for (int i = 0; i < 20_000; i++) {
        final int permuted = Math.floorMod(i * 12_347, 20_000);
        database.newDocument("SortedMultiSeries").set("lookupKey", paddedKey("key", permuted)).save();
      }
    });

    TypeIndex index = database.getSchema().buildTypeIndex("SortedMultiSeries", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(128L << 20)
        .withUnique(false)
        .withPageSize(1_024)
        .create();

    final LSMTreeIndex bucketIndex = (LSMTreeIndex) index.getIndexesOnBuckets()[0];
    assertThat(bucketIndex.getMutableIndex().getSubIndex().newIterators(true, null, null).size()).isGreaterThan(1);
    assertOrdered(index, 20_000);
    assertDescending(index, 20_000);
    assertThat(count(index.range(true, new Object[] { paddedKey("key", 9_990) }, true,
        new Object[] { paddedKey("key", 10_010) }, true))).isEqualTo(21);

    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newDocument("SortedMultiSeries").set("lookupKey", paddedKey("z-live", i)).save();
    });
    assertThat(((IndexInternal) index).scheduleCompaction()).isTrue();
    assertThat(((IndexInternal) index).compact()).isTrue();
    assertOrdered(index, 22_000);
    assertDescending(index, 22_000);

    reopenDatabase();
    index = database.getSchema().getType("SortedMultiSeries").getIndexByProperties("lookupKey");
    assertOrdered(index, 22_000);
    assertThat(count(index.get(new Object[] { paddedKey("z-live", 1_234) }))).isEqualTo(1);
  }

  private static long count(final IndexCursor cursor) {
    long total = 0L;
    while (cursor.hasNext()) {
      cursor.next();
      total++;
    }
    return total;
  }

  private static void assertOrdered(final TypeIndex index, final int expected) {
    final IndexCursor cursor = index.iterator(true);
    String previous = null;
    int count = 0;
    while (cursor.hasNext()) {
      cursor.next();
      final String current = (String) cursor.getKeys()[0];
      if (previous != null)
        assertThat(current).isGreaterThanOrEqualTo(previous);
      previous = current;
      count++;
    }
    assertThat(count).isEqualTo(expected);
  }

  private static void assertDescending(final TypeIndex index, final int expected) {
    final IndexCursor cursor = index.iterator(false);
    String previous = null;
    int count = 0;
    while (cursor.hasNext()) {
      cursor.next();
      final String current = (String) cursor.getKeys()[0];
      if (previous != null)
        assertThat(current).isLessThanOrEqualTo(previous);
      previous = current;
      count++;
    }
    assertThat(count).isEqualTo(expected);
  }

  private static String paddedKey(final String prefix, final int value) {
    return "%s-%08d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".formatted(prefix, value);
  }

  private static void assertNoSpillDirectories(final Path parent) throws Exception {
    if (!Files.exists(parent))
      return;
    try (var paths = Files.list(parent)) {
      assertThat(paths.filter(path -> path.getFileName().toString().startsWith(".arcadedb-index-sort-")).toList()).isEmpty();
    }
  }
}
