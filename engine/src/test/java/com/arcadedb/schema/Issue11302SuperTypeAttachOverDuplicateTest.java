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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #11302: {@code ALTER TYPE A SUPERTYPE +S} over a type whose records hold a key the
 * super type's UNIQUE index already holds was accepted, and left the sub-index propagated over A's bucket with NO
 * entry at all (not only the duplicate: every record of the bucket). Index-backed lookups then missed records a scan
 * finds, {@code CHECK DATABASE} reported nothing, and the next open found a sub-index file with no header page and
 * declared the super type's index incomplete.
 * <p>
 * The attach must be refused with the {@link DuplicatedKeyException} the linkage guard of
 * {@code LocalDocumentType.addSuperTypeInternal} is written for, and leave the schema as it found it: A not a subtype,
 * no sub-index of A's bucket under the super type's index, before and after a reopen.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11302SuperTypeAttachOverDuplicateTest extends TestHelper {

  @Test
  void attachOverAKeyTheSuperTypeAlreadyHoldsIsRefusedAndUndone() {
    createSuperTypeHolding("x");
    createSubTypeCandidateHolding("x", "solo");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE A SUPERTYPE +S"))//
        .isInstanceOf(DuplicatedKeyException.class)//
        .hasMessageContaining("x");

    assertRefusedAndUndone();
    reopenDatabase();
    assertRefusedAndUndone();
  }

  @Test
  void attachOverAKeyDuplicatedInsideTheSubTypeIsRefusedAndUndone() {
    // NO RECORD IN S: THE DUPLICATE IS BETWEEN TWO RECORDS OF A, WHICH HAS NO UNIQUE INDEX OF ITS OWN
    createSuperTypeHolding();
    createSubTypeCandidateHolding("x", "x", "solo");

    assertThatThrownBy(() -> database.getSchema().getType("A").addSuperType("S"))//
        .isInstanceOf(DuplicatedKeyException.class)//
        .hasMessageContaining("x");

    assertThat(database.getSchema().getType("A").isSubTypeOf("S")).isFalse();
    assertThat(subIndexNamesOfA()).isEmpty();
    assertThat(database.getSchema().getType("S").getPolymorphicIndexByProperties("k").isValid()).isTrue();

    reopenDatabase();
    assertThat(database.getSchema().getType("A").isSubTypeOf("S")).isFalse();
    assertThat(subIndexNamesOfA()).isEmpty();
  }

  @Test
  void attachInsideACallerTransactionRefusesOnlyThePendingDuplicate() {
    // THE BUILD SHARES THE CALLER'S TRANSACTION WHEN IT HOLDS WRITES IN A'S BUCKETS: THE COMMITTED RECORDS ARE INDEXED IN
    // A TRANSACTION OF THEIR OWN, THE CALLER'S PENDING ONES QUEUE THEIR ENTRIES ON THE CALLER'S TRANSACTION AND ARE CHECKED
    // WHEN IT COMMITS, EXACTLY AS CREATE INDEX IN A TRANSACTION DOES. THE ATTACH IS A SCHEMA CHANGE AND STANDS; THE
    // DUPLICATE IS REFUSED WITH THE RECORD THAT CARRIES IT, AND THE INDEX COVERS EVERY RECORD THAT DID COMMIT
    createSuperTypeHolding("x");
    createSubTypeCandidateHolding("solo");

    assertThatThrownBy(() -> database.transaction(() -> {
      database.newDocument("A").set("k", "x").save();
      database.getSchema().getType("A").addSuperType("S");
    }))//
        .isInstanceOf(DuplicatedKeyException.class)//
        .hasMessageContaining("x");

    assertIndexCoversTheCommittedRecords();
    reopenDatabase();
    assertIndexCoversTheCommittedRecords();
  }

  private void assertIndexCoversTheCommittedRecords() {
    assertThat(database.getSchema().getType("A").isSubTypeOf("S")).isTrue();
    assertThat(subIndexNamesOfA()).hasSize(1);
    final TypeIndex index = database.getSchema().getType("S").getPolymorphicIndexByProperties("k");
    assertThat(index.isValid()).isTrue();
    assertThat(index.isComplete()).isTrue();
    assertThat(index.countEntries()).isEqualTo(2L);
    assertThat(count("SELECT count(*) AS c FROM A")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS c FROM S WHERE k = 'solo'")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS c FROM S WHERE k = 'x'")).isEqualTo(1L);
    assertCheckDatabaseClean();
  }

  @Test
  void attachWithoutADuplicateIndexesEveryRecordOfTheSubType() {
    // CONTROL: THE SAME SHAPE WITHOUT THE DUPLICATE IS ACCEPTED AND EVERY KEY IS FOUND THROUGH THE INDEX
    createSuperTypeHolding("x");
    createSubTypeCandidateHolding("a1", "solo");

    database.command("sql", "ALTER TYPE A SUPERTYPE +S");

    assertAttachedAndIndexed();
    reopenDatabase();
    assertAttachedAndIndexed();
  }

  @Test
  void createUniqueIndexOverADuplicateLeavesNoComponentBehind() {
    final DocumentType t = database.getSchema().createDocumentType("T", 1);
    t.createProperty("k", Type.STRING);
    database.transaction(() -> {
      database.newDocument("T").set("k", "x").save();
      database.newDocument("T").set("k", "x").save();
    });

    assertThatThrownBy(() -> t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k")).hasStackTraceContaining("DuplicatedKeyException");

    assertThat(Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList()).isEmpty();
    reopenDatabase();
    assertThat(Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList()).isEmpty();
  }

  @Test
  void createUniqueIndexOverADuplicateAcrossBucketsLeavesNoComponentBehind() {
    // THE DUPLICATE IS FOUND WHEN THE SECOND BUCKET'S BUILD COMMITS, WHICH IS WHERE THE PROPAGATION LOST ITS BUILD
    final DocumentType t = database.getSchema().createDocumentType("T", 2);
    t.createProperty("k", Type.STRING);
    database.transaction(() -> {
      database.newDocument("T").set("k", "x").save();
      database.newDocument("T").set("k", "x").save();
    });
    assertThat(t.getBuckets(false).stream().filter(b -> b.count() > 0).count()).isEqualTo(2L);

    assertThatThrownBy(() -> t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k")).hasStackTraceContaining("DuplicatedKeyException");

    assertThat(Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList()).isEmpty();
    reopenDatabase();
    assertThat(Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList()).isEmpty();
  }

  @Test
  void attachOfAPopulatedBucketOverADuplicateIsRefusedAndUndone() {
    // THE SIBLING PROPAGATION, addBucket, OVER A BUCKET THAT ALREADY HOLDS RECORDS: ONE A TYPE GAVE BACK. ITS RETRY OF THE
    // DUPLICATE CREATED A SECOND COMPONENT BESIDE THE FIRST ATTEMPT'S, AND THE BUCKET STAYED ATTACHED WITH BOTH
    createSuperTypeHolding("x");
    final DocumentType a = database.getSchema().createDocumentType("A", 1);
    final String bucketName = a.getBuckets(false).getFirst().getName();
    database.transaction(() -> database.newDocument("A").set("k", "x").save());
    a.removeBucket(database.getSchema().getBucketByName(bucketName));

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE S BUCKET +" + bucketName))//
        .isInstanceOf(DuplicatedKeyException.class)//
        .hasMessageContaining("x");

    assertBucketNotAttached(bucketName);
    reopenDatabase();
    assertBucketNotAttached(bucketName);

    // THE BUCKET IS FREE AGAIN: ONCE THE DUPLICATE IS GONE THE SAME ATTACH IS ACCEPTED AND INDEXED
    database.command("sql", "ALTER TYPE A BUCKET +" + bucketName);
    database.transaction(() -> database.command("sql", "UPDATE A SET k = 'y'"));
    database.command("sql", "ALTER TYPE A BUCKET -" + bucketName);
    database.command("sql", "ALTER TYPE S BUCKET +" + bucketName);
    assertThat(indexNames().stream().filter(n -> n.startsWith(bucketName + "_")).toList()).hasSize(1);
    assertThat(count("SELECT count(*) AS c FROM S WHERE k = 'y'")).isEqualTo(1L);
    assertCheckDatabaseClean();
  }

  private void assertBucketNotAttached(final String bucketName) {
    assertThat(database.getSchema().getType("S").getBuckets(false).stream().map(b -> b.getName()).toList()).doesNotContain(bucketName);
    assertThat(indexNames().stream().filter(n -> n.startsWith(bucketName + "_")).toList()).isEmpty();
    final TypeIndex index = database.getSchema().getType("S").getPolymorphicIndexByProperties("k");
    assertThat(index.isValid()).isTrue();
    assertThat(index.countEntries()).isEqualTo(1L);
    assertThat(database.getSchema().getType("S").isNeedsRepartition()).isFalse();
  }

  private List<String> indexNames() {
    return Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList();
  }

  private void assertAttachedAndIndexed() {
    assertThat(database.getSchema().getType("A").isSubTypeOf("S")).isTrue();
    assertThat(subIndexNamesOfA()).hasSize(1);
    final TypeIndex index = database.getSchema().getType("S").getPolymorphicIndexByProperties("k");
    assertThat(index.isValid()).isTrue();
    assertThat(index.isComplete()).isTrue();
    assertThat(index.countEntries()).isEqualTo(3L);
    for (final String key : List.of("x", "a1", "solo"))
      assertThat(count("SELECT count(*) AS c FROM S WHERE k = '" + key + "'")).as("key " + key).isEqualTo(1L);
    assertCheckDatabaseClean();
  }

  private void assertRefusedAndUndone() {
    assertThat(database.getSchema().getType("A").isSubTypeOf("S")).isFalse();
    assertThat(database.getSchema().getType("S").getSubTypes()).isEmpty();
    assertThat(subIndexNamesOfA()).as("no sub-index of A may be left under S[k]").isEmpty();

    final TypeIndex index = database.getSchema().getType("S").getPolymorphicIndexByProperties("k");
    assertThat(index.isValid()).isTrue();
    assertThat(index.isComplete()).isTrue();
    assertThat(index.countEntries()).isEqualTo(1L);

    assertThat(count("SELECT count(*) AS c FROM S")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS c FROM S WHERE k = 'x'")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS c FROM A")).isEqualTo(2L);
    assertCheckDatabaseClean();
  }

  private void createSuperTypeHolding(final String... keys) {
    final DocumentType s = database.getSchema().createDocumentType("S");
    s.createProperty("k", Type.STRING);
    s.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
    database.transaction(() -> {
      for (final String key : keys)
        database.newDocument("S").set("k", key).save();
    });
  }

  private void createSubTypeCandidateHolding(final String... keys) {
    database.getSchema().createDocumentType("A", 1);
    database.transaction(() -> {
      for (final String key : keys)
        database.newDocument("A").set("k", key).save();
    });
  }

  private List<String> subIndexNamesOfA() {
    return Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).filter(n -> n.startsWith("A_")).toList();
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private void assertCheckDatabaseClean() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("totalErrors").longValue()).isZero();
      assertThat(r.<Collection<?>>getProperty("corruptedIndexes")).isNullOrEmpty();
      assertThat(r.<Collection<?>>getProperty("incompleteIndexes")).isNullOrEmpty();
    }
  }
}
