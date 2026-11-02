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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.lsm.LSMTreeIndexBulkLoader;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexBuildMode;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LSMTreeSortedNonUniqueBuildTest extends TestHelper {
  @AfterEach
  void clearEntriesPerRunLimit() {
    LSMTreeIndexBulkLoader.setEntriesPerRunTestLimit(0);
  }

  @Test
  void buildsExternalRunsAcrossBucketsAndSupportsReopenAndMutation() throws Exception {
    // The runs are sized by what their entries retain (issue #10267): the cap makes this small table spill as the budget used to
    LSMTreeIndexBulkLoader.setEntriesPerRunTestLimit(341);
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
    // The runs are sized by what their entries retain (issue #10267): the cap makes this small table spill as the budget used to
    LSMTreeIndexBulkLoader.setEntriesPerRunTestLimit(341);
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
    // The runs are sized by what their entries retain (issue #10267): the cap makes this small table spill as the budget used to
    LSMTreeIndexBulkLoader.setEntriesPerRunTestLimit(341);
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
    // The runs are sized by what their entries retain (issue #10267): the cap makes this small table spill as the budget used to
    LSMTreeIndexBulkLoader.setEntriesPerRunTestLimit(341);
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
    // The runs are sized by what their entries retain (issue #10267): the cap makes this small table spill as the budget used to
    LSMTreeIndexBulkLoader.setEntriesPerRunTestLimit(341);
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

  // Issue #9291: CREATE INDEX can ask for the sorted build with METADATA {"buildMode": "SORTED"}, and the sorted build orders a
  // run by the rank of its distinct keys when keys repeat (low-cardinality column) instead of comparing every pair of entries.
  // Whatever the cardinality, both builds must hold the same entries in the same order.
  private static final int SORTED_BUILD_ROWS = 60_000;

  @Test
  @DisplayName("#9291 sorted build matches the default build on low and high cardinality")
  void sqlBuildModeSortedMatchesDefaultOnLowAndHighCardinality() {
    database.command("sql", "CREATE DOCUMENT TYPE Sorted");
    database.command("sql", "CREATE PROPERTY Sorted.low STRING");
    database.command("sql", "CREATE PROPERTY Sorted.high LONG");
    database.command("sql", "CREATE DOCUMENT TYPE Plain");
    database.command("sql", "CREATE PROPERTY Plain.low STRING");
    database.command("sql", "CREATE PROPERTY Plain.high LONG");
    for (final String type : new String[] { "Sorted", "Plain" }) {
      database.begin();
      for (int i = 0; i < SORTED_BUILD_ROWS; i++) {
        database.newDocument(type).set("low", "k" + (i * 7919 % 37)).set("high", (long) (i * 31 % 40_009)).save();
        if (i % 10_000 == 9_999) {
          database.commit();
          database.begin();
        }
      }
      database.commit();
    }

    database.command("sql", "CREATE INDEX ON Sorted (low) NOTUNIQUE METADATA {\"buildMode\": \"SORTED\"}");
    database.command("sql", "CREATE INDEX ON Sorted (high) NOTUNIQUE METADATA {\"buildMode\": \"sorted\"}");
    database.command("sql", "CREATE INDEX ON Plain (low) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Plain (high) NOTUNIQUE");

    for (final String property : new String[] { "low", "high" }) {
      final TypeIndex sorted = database.getSchema().getType("Sorted").getPolymorphicIndexByProperties(property);
      final TypeIndex plain = database.getSchema().getType("Plain").getPolymorphicIndexByProperties(property);
      assertThat(sorted.countEntries()).isEqualTo(SORTED_BUILD_ROWS);
      assertThat(plain.countEntries()).isEqualTo(SORTED_BUILD_ROWS);
      final List<String> sortedEntries = entries(sorted);
      final List<String> plainEntries = entries(plain);
      // the key order is the index's contract; the order of the rids within one key is not
      assertThat(keysOnly(sortedEntries)).as(property + " key order").isEqualTo(keysOnly(plainEntries));
      assertThat(sortedEntries.stream().sorted().toList()).as(property + " entries").isEqualTo(plainEntries.stream().sorted().toList());
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Sorted WHERE low = 'k5'")) {
      final long expected = countRows("Plain", "low = 'k5'");
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(expected).isPositive();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Sorted WHERE high = 123")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(countRows("Plain", "high = 123"));
    }
  }

  @Test
  @DisplayName("#9291 buildMode default and invalid values")
  void sqlBuildModeDefaultAndInvalidValues() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.p STRING");
    database.command("sql", "CREATE PROPERTY T.q STRING");
    database.transaction(() -> database.newDocument("T").set("p", "a").set("q", "b").save());

    database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE METADATA {\"buildMode\": \"DEFAULT\"}");
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON T (q) NOTUNIQUE METADATA {\"buildMode\": \"FAST\"}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("buildMode");
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON T (q) FULL_TEXT METADATA {\"buildMode\": \"SORTED\"}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("SORTED");
    // an invalid directive fails whether or not the index already exists
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON T (p) NOTUNIQUE METADATA {\"buildMode\": \"FAST\"}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("buildMode");
    // the directive never reaches the index configuration, so a statement that only carries it is not "METADATA on an index that takes none"
    database.command("sql", "CREATE INDEX ON T (q) UNIQUE METADATA {\"buildMode\": \"SORTED\"}");
    assertThat(database.getSchema().getType("T").getPolymorphicIndexByProperties("q").countEntries()).isEqualTo(1);
  }

  @Test
  @DisplayName("#9291 case insensitive collation keys share a group and stay ordered")
  void caseInsensitiveCollationKeysShareAGroupAndStayOrdered() {
    for (final String type : new String[] { "CiSorted", "CiPlain" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".p STRING");
      database.transaction(() -> {
        for (int i = 0; i < 5_000; i++)
          database.newDocument(type).set("p", (i % 2 == 0 ? "Key" : "kEY") + (i % 13)).save();
      });
    }
    database.command("sql", "CREATE INDEX ON CiSorted (p COLLATE ci) NOTUNIQUE METADATA {\"buildMode\": \"SORTED\"}");
    database.command("sql", "CREATE INDEX ON CiPlain (p COLLATE ci) NOTUNIQUE");

    final List<String> sorted = entries(database.getSchema().getType("CiSorted").getPolymorphicIndexByProperties("p"));
    final List<String> plain = entries(database.getSchema().getType("CiPlain").getPolymorphicIndexByProperties("p"));
    assertThat(sorted).hasSize(5_000);
    assertThat(keysOnly(sorted)).isEqualTo(keysOnly(plain));
    assertThat(sorted.stream().sorted().toList()).isEqualTo(plain.stream().sorted().toList());
  }

  @Test
  @DisplayName("#9291 decimal keys of different scale are one key")
  void decimalKeysOfDifferentScaleAreOneKeyAndStayOrderedByRid() {
    for (final String type : new String[] { "DecSorted", "DecPlain" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".d DECIMAL");
      database.transaction(() -> {
        for (int i = 0; i < 3_000; i++)
          database.newDocument(type).set("d", i % 2 == 0 ? new BigDecimal("1.0") : new BigDecimal("1.00")).save();
      });
    }
    database.command("sql", "CREATE INDEX ON DecSorted (d) NOTUNIQUE METADATA {\"buildMode\": \"SORTED\"}");
    database.command("sql", "CREATE INDEX ON DecPlain (d) NOTUNIQUE");
    final List<String> sorted = entries(database.getSchema().getType("DecSorted").getPolymorphicIndexByProperties("d"));
    final List<String> plain = entries(database.getSchema().getType("DecPlain").getPolymorphicIndexByProperties("d"));
    assertThat(sorted).hasSize(3_000);
    assertThat(sorted.stream().sorted().toList()).isEqualTo(plain.stream().sorted().toList());
  }

  @Test
  @DisplayName("#9291 unique sorted build still detects duplicates of repeated keys")
  void uniqueSortedBuildStillDetectsDuplicatesOfRepeatedKeys() {
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.p STRING");
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("U").set("p", "dup" + (i % 10)).save();
    });
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON U (p) UNIQUE METADATA {\"buildMode\": \"SORTED\"}"))
        .isInstanceOf(RuntimeException.class);
    assertThat(database.getSchema().getType("U").getPolymorphicIndexByProperties("p")).isNull();
  }

  private long countRows(final String type, final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE " + where)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private static List<String> keysOnly(final List<String> entries) {
    return entries.stream().map(e -> e.substring(0, e.indexOf('@'))).toList();
  }

  /** every (key, rid-position) of the index in cursor order, with the key as text */
  private List<String> entries(final TypeIndex index) {
    final List<String> out = new ArrayList<>();
    final IndexCursor cursor = index.iterator(true);
    while (cursor.hasNext()) {
      final RID rid = cursor.next().getIdentity();
      out.add(Arrays.toString(cursor.getKeys()) + "@" + rid.getPosition());
    }
    return out;
  }
}
