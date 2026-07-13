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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexBuildMode;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LSMTreeSortedUniqueBuildTest extends TestHelper {
  @Test
  void buildsAcrossBucketsAndSpillRunsThenSupportsPlannerCompactionReopenAndMutation() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    final Path spillParent = Path.of(getDatabasePath()).resolve("unique-success-spill");
    final DocumentType type = database.getSchema().buildDocumentType().withName("SortedUniqueSuccess")
        .withTotalBuckets(3).create();
    type.createProperty("lookupKey", Type.STRING);

    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++) {
        final int permuted = Math.floorMod(i * 1_237, 2_000);
        database.newDocument("SortedUniqueSuccess").set("lookupKey", paddedKey("key", permuted)).save();
      }
    });

    TypeIndex index = database.getSchema().buildTypeIndex("SortedUniqueSuccess", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(1L << 20)
        .withBuildSpillDirectory(spillParent)
        .withBuildMergeFanIn(2)
        .withPageSize(1_024)
        .withUnique(true)
        .create();

    assertThat(index.isUnique()).isTrue();
    assertOrdered(index, 2_000);
    assertThat(count(index.iterator(false))).isEqualTo(2_000);
    assertThat(count(index.get(new Object[] { paddedKey("key", 1_234) }))).isEqualTo(1);
    assertThat(count(index.range(true, new Object[] { paddedKey("key", 990) }, true,
        new Object[] { paddedKey("key", 1_010) }, true))).isEqualTo(21);
    assertPlannerUses(index, "SortedUniqueSuccess", paddedKey("key", 1_234));
    assertNoBuildArtifacts(spillParent);

    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        database.newDocument("SortedUniqueSuccess").set("lookupKey", paddedKey("live", i)).save();
    });
    assertThat(((IndexInternal) index).scheduleCompaction()).isTrue();
    assertThat(((IndexInternal) index).compact()).isTrue();
    assertThat(count(index.iterator(true))).isEqualTo(2_500);

    reopenDatabase();
    index = database.getSchema().getType("SortedUniqueSuccess").getIndexByProperties("lookupKey");
    assertThat(index.isUnique()).isTrue();
    assertThat(count(index.get(new Object[] { paddedKey("live", 250) }))).isEqualTo(1);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("SortedUniqueSuccess")
        .set("lookupKey", paddedKey("key", 1_234)).save()))
        .isInstanceOf(DuplicatedKeyException.class);

    final RID[] reusable = new RID[1];
    database.transaction(() -> reusable[0] = database.newDocument("SortedUniqueSuccess")
        .set("lookupKey", "reusable-key").save().getIdentity());
    database.transaction(() -> database.lookupByRID(reusable[0], true).asDocument().delete());
    database.transaction(() -> database.newDocument("SortedUniqueSuccess").set("lookupKey", "reusable-key").save());
    assertThat(count(index.get(new Object[] { "reusable-key" }))).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "0000-duplicate", "key-00400", "zzzz-duplicate" })
  void rejectsDuplicateAtAnySortedPositionAndPublishesNothing(final String duplicateKey) throws Exception {
    final Path spillParent = Path.of(getDatabasePath()).resolve("position-spill");
    final DocumentType type = database.getSchema().createDocumentType("SortedUniquePosition");
    type.createProperty("lookupKey", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < 800; i++)
        database.newDocument("SortedUniquePosition").set("lookupKey", "key-%05d".formatted(i)).save();
      database.newDocument("SortedUniquePosition").set("lookupKey", duplicateKey).save();
      database.newDocument("SortedUniquePosition").set("lookupKey", duplicateKey).save();
    });

    assertThatThrownBy(() -> buildUnique("SortedUniquePosition", spillParent, 2))
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);
    assertThat(type.getIndexByProperties("lookupKey")).isNull();
    assertThat(database.getSchema().getIndexes()).isEmpty();
    assertNoBuildArtifacts(spillParent);
    assertNoBuildArtifactsInDatabase();

    reopenDatabase();
    assertThat(database.getSchema().getType("SortedUniquePosition").getIndexByProperties("lookupKey")).isNull();
  }

  @Test
  void rejectsCrossBucketDuplicateAfterMultipleMergeGenerationsAndAllowsRetry() throws Exception {
    final Path spillParent = Path.of(getDatabasePath()).resolve("cross-bucket-spill");
    final DocumentType type = database.getSchema().buildDocumentType().withName("SortedUniqueCrossBucket")
        .withTotalBuckets(2).create();
    type.createProperty("lookupKey", Type.STRING);
    final RID[] duplicates = new RID[2];
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newDocument("SortedUniqueCrossBucket").set("lookupKey", "key-%05d".formatted(i)).save();
      duplicates[0] = database.newDocument("SortedUniqueCrossBucket").set("lookupKey", "zzzz-duplicate")
          .save().getIdentity();
      duplicates[1] = database.newDocument("SortedUniqueCrossBucket").set("lookupKey", "zzzz-duplicate")
          .save().getIdentity();
    });
    assertThat(duplicates[0].getBucketId()).isNotEqualTo(duplicates[1].getBucketId());

    assertThatThrownBy(() -> buildUnique("SortedUniqueCrossBucket", spillParent, 2))
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);
    assertThat(type.getIndexByProperties("lookupKey")).isNull();
    assertNoBuildArtifacts(spillParent);
    assertNoBuildArtifactsInDatabase();

    reopenDatabase();
    database.transaction(() -> database.lookupByRID(duplicates[1], true).asDocument().delete());
    final TypeIndex index = buildUnique("SortedUniqueCrossBucket", spillParent, 2);
    assertThat(count(index.iterator(true))).isEqualTo(2_001);
    assertThat(count(index.get(new Object[] { "zzzz-duplicate" }))).isEqualTo(1);
  }

  @Test
  void collapsesRepeatedListEntryForSameRidButRejectsAnotherRid() {
    final DocumentType type = database.getSchema().createDocumentType("SortedUniqueList");
    type.createProperty("tags", Type.LIST);
    final List<String> repeatedTags = new ArrayList<>(Collections.nCopies(400, "alpha"));
    repeatedTags.add("beta");
    database.transaction(() -> {
      database.newDocument("SortedUniqueList").set("tags", repeatedTags).save();
      database.newDocument("SortedUniqueList").set("tags", List.of("gamma")).save();
    });

    final TypeIndex index = database.getSchema().buildTypeIndex("SortedUniqueList", new String[] { "tags by item" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(1L << 20)
        .withBuildMergeFanIn(2)
        .withUnique(true)
        .create();
    assertThat(count(index.get(new Object[] { "alpha" }))).isEqualTo(1);
    assertThat(count(index.iterator(true))).isEqualTo(3);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("SortedUniqueList")
        .set("tags", List.of("alpha")).save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void rejectsCaseInsensitiveCompositeDuplicate() throws Exception {
    final DocumentType type = database.getSchema().buildDocumentType().withName("SortedUniqueComposite")
        .withTotalBuckets(2).create();
    type.createProperty("code", Type.STRING);
    type.createProperty("ordinal", Type.INTEGER);
    database.transaction(() -> {
      database.newDocument("SortedUniqueComposite").set("code", "Alpha").set("ordinal", 7).save();
      database.newDocument("SortedUniqueComposite").set("code", "ALPHA").set("ordinal", 7).save();
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("SortedUniqueComposite",
            new String[] { "code", "ordinal" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withCollations(List.of("CI", "DEFAULT"))
        .withUnique(true)
        .create())
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);
    assertThat(type.getIndexByProperties("code", "ordinal")).isNull();
    assertNoBuildArtifactsInDatabase();
  }

  @Test
  void indexesPartialNullCompositeKeysWithSkipStrategyAndEnforcesFutureUniqueness() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("SortedUniquePartialNullSkip");
    type.createProperty("groupName", Type.STRING);
    type.createProperty("ordinal", Type.INTEGER);

    database.transaction(() -> {
      database.newDocument("SortedUniquePartialNullSkip").set("groupName", null).set("ordinal", 5).save();
      database.newDocument("SortedUniquePartialNullSkip").set("groupName", "group").set("ordinal", null).save();
      database.newDocument("SortedUniquePartialNullSkip").set("groupName", null).set("ordinal", null).save();
      database.newDocument("SortedUniquePartialNullSkip").set("groupName", "group").set("ordinal", 7).save();
    });

    TypeIndex index = database.getSchema().buildTypeIndex("SortedUniquePartialNullSkip",
            new String[] { "groupName", "ordinal" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.SKIP)
        .withUnique(true)
        .create();

    assertThat(count(index.iterator(true))).isEqualTo(3);
    assertThat(count(index.get(new Object[] { null, 5 }))).isEqualTo(1);
    assertThat(count(index.get(new Object[] { "group", null }))).isEqualTo(1);
    assertThat(count(index.get(new Object[] { null, null }))).isZero();

    reopenDatabase();
    index = database.getSchema().getType("SortedUniquePartialNullSkip")
        .getIndexByProperties("groupName", "ordinal");
    assertThat(count(index.get(new Object[] { null, 5 }))).isEqualTo(1);
    assertThat(count(index.get(new Object[] { "group", null }))).isEqualTo(1);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("SortedUniquePartialNullSkip")
        .set("groupName", null).set("ordinal", 5).save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void rejectsInitialDuplicatePartialNullCompositeKeyWithSkipStrategy() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("SortedUniquePartialNullDuplicate");
    type.createProperty("groupName", Type.STRING);
    type.createProperty("ordinal", Type.INTEGER);
    database.transaction(() -> {
      database.newDocument("SortedUniquePartialNullDuplicate").set("groupName", null).set("ordinal", 5).save();
      database.newDocument("SortedUniquePartialNullDuplicate").set("groupName", null).set("ordinal", 5).save();
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("SortedUniquePartialNullDuplicate",
            new String[] { "groupName", "ordinal" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.SKIP)
        .withUnique(true)
        .create())
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);
    assertThat(type.getIndexByProperties("groupName", "ordinal")).isNull();
    assertNoBuildArtifactsInDatabase();

    reopenDatabase();
    assertThat(database.getSchema().getType("SortedUniquePartialNullDuplicate")
        .getIndexByProperties("groupName", "ordinal")).isNull();
  }

  @Test
  void rejectsPartialNullCompositeKeysWithErrorStrategy() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("SortedUniquePartialNullError");
    type.createProperty("groupName", Type.STRING);
    type.createProperty("ordinal", Type.INTEGER);
    database.transaction(() -> database.newDocument("SortedUniquePartialNullError")
        .set("groupName", null).set("ordinal", 5).save());

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("SortedUniquePartialNullError",
            new String[] { "groupName", "ordinal" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR)
        .withUnique(true)
        .create())
        .isInstanceOf(IndexException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage(
            "Indexed key SortedUniquePartialNullError[groupName, ordinal] cannot be NULL ([null, 5])");
    assertThat(type.getIndexByProperties("groupName", "ordinal")).isNull();
    assertNoBuildArtifactsInDatabase();

    reopenDatabase();
    assertThat(database.getSchema().getType("SortedUniquePartialNullError")
        .getIndexByProperties("groupName", "ordinal")).isNull();
  }

  @Test
  void honorsIndexSkipAndErrorNullStrategies() {
    final TypeIndex indexedNulls = createUniqueNullIndex("UniqueNullIndex", LSMTreeIndexAbstract.NULL_STRATEGY.INDEX);
    assertThat(countNullKeys(indexedNulls.iterator(true))).isEqualTo(2);
    assertThat(count(indexedNulls.iterator(true))).isEqualTo(3);

    final TypeIndex skippedNulls = createUniqueNullIndex("UniqueNullSkip", LSMTreeIndexAbstract.NULL_STRATEGY.SKIP);
    assertThat(countNullKeys(skippedNulls.iterator(true))).isZero();
    assertThat(count(skippedNulls.iterator(true))).isEqualTo(1);

    final DocumentType errorType = createNullRecords("UniqueNullError");
    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("UniqueNullError", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR)
        .withUnique(true)
        .create())
        .hasRootCauseInstanceOf(IllegalArgumentException.class);
    assertThat(errorType.getIndexByProperties("lookupKey")).isNull();
  }

  @Test
  void publishesEmptyUniqueIndexAndEnforcesLaterWrites() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("SortedUniqueEmpty");
    type.createProperty("lookupKey", Type.STRING);
    TypeIndex index = buildUnique("SortedUniqueEmpty", null, 8);
    assertThat(index.isUnique()).isTrue();
    assertThat(count(index.iterator(true))).isZero();

    database.transaction(() -> database.newDocument("SortedUniqueEmpty").set("lookupKey", "later").save());
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("SortedUniqueEmpty")
        .set("lookupKey", "later").save()))
        .isInstanceOf(DuplicatedKeyException.class);
    reopenDatabase();
    index = database.getSchema().getType("SortedUniqueEmpty").getIndexByProperties("lookupKey");
    assertThat(index.isUnique()).isTrue();
    assertThat(count(index.get(new Object[] { "later" }))).isEqualTo(1);
  }

  private TypeIndex createUniqueNullIndex(final String typeName, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    createNullRecords(typeName);
    return database.getSchema().buildTypeIndex(typeName, new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withNullStrategy(nullStrategy)
        .withUnique(true)
        .create();
  }

  private DocumentType createNullRecords(final String typeName) {
    final DocumentType type = database.getSchema().createDocumentType(typeName);
    type.createProperty("lookupKey", Type.STRING);
    database.transaction(() -> {
      database.newDocument(typeName).set("lookupKey", (Object) null).save();
      database.newDocument(typeName).set("lookupKey", (Object) null).save();
      database.newDocument(typeName).set("lookupKey", "value").save();
    });
    return type;
  }

  private TypeIndex buildUnique(final String typeName, final Path spillParent, final int fanIn) {
    final var builder = database.getSchema().buildTypeIndex(typeName, new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(1L << 20)
        .withBuildMergeFanIn(fanIn);
    if (spillParent != null)
      builder.withBuildSpillDirectory(spillParent);
    return builder.withUnique(true).create();
  }

  private void assertPlannerUses(final TypeIndex index, final String typeName, final String key) {
    try (var result = database.query("sql", "SELECT FROM " + typeName + " WHERE lookupKey = ?", key)) {
      final String plan = result.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).contains("FETCH FROM INDEX").contains(index.getName());
      assertThat(result.stream()).hasSize(1);
    }
  }

  private static long count(final IndexCursor cursor) {
    long total = 0L;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        total++;
      }
      return total;
    } finally {
      cursor.close();
    }
  }

  private static long countNullKeys(final IndexCursor cursor) {
    long total = 0L;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        if (cursor.getKeys()[0] == null)
          total++;
      }
      return total;
    } finally {
      cursor.close();
    }
  }

  private static void assertOrdered(final TypeIndex index, final int expected) {
    final IndexCursor cursor = index.iterator(true);
    String previous = null;
    int count = 0;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        final String current = (String) cursor.getKeys()[0];
        if (previous != null)
          assertThat(current).isGreaterThan(previous);
        previous = current;
        count++;
      }
    } finally {
      cursor.close();
    }
    assertThat(count).isEqualTo(expected);
  }

  private void assertNoBuildArtifactsInDatabase() throws Exception {
    try (Stream<Path> files = Files.list(Path.of(database.getDatabasePath()))) {
      assertThat(files.map(path -> path.getFileName().toString())
          .filter(name -> name.endsWith(".numtidx") || name.endsWith(".nuctidx")
              || name.endsWith(".temp_numtidx") || name.endsWith(".temp_nuctidx")
              || name.startsWith(".arcadedb-sorted-index-build-")
              || name.startsWith(".arcadedb-index-sort-"))
          .toList()).isEmpty();
    }
  }

  private static void assertNoBuildArtifacts(final Path parent) throws Exception {
    if (parent == null || !Files.exists(parent))
      return;
    try (Stream<Path> paths = Files.list(parent)) {
      assertThat(paths.filter(path -> path.getFileName().toString().startsWith(".arcadedb-index-sort-")).toList()).isEmpty();
    }
  }

  private static String paddedKey(final String prefix, final int value) {
    return "%s-%08d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".formatted(prefix, value);
  }
}
