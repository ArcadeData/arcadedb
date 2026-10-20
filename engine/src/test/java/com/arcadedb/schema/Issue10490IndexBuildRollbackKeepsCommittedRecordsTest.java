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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10490: a {@code CREATE INDEX} inside a transaction that had already written to the indexed type
 * joined that transaction, and the scan - which reads the caller's pending pages first and the committed ones underneath -
 * queued one entry per record, the COMMITTED ones included, on the transaction the caller then rolled back. The index
 * component itself is committed on its own, so after the rollback the schema had the index and its file had no entries: a
 * lookup found nothing, and a UNIQUE index accepted a duplicate of a committed key.
 * <p>
 * The committed records are indexed in a transaction of their own now, and only what the caller itself created, updated or
 * deleted is queued on the caller's transaction, so a rollback leaves the index describing exactly the committed state and a
 * commit leaves it describing the caller's.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10490IndexBuildRollbackKeepsCommittedRecordsTest extends TestHelper {

  /** One row per index family the issue lists: how to create it, how to ask it for key 1, and what a scan must find. */
  private record Family(String name, String create, String lookupOne, String lookupNine) {
  }

  private static final List<Family> FAMILIES = List.of(
      new Family("LSM_TREE", "CREATE INDEX ON %s (k) NOTUNIQUE", "SELECT FROM %s WHERE k = 1", "SELECT FROM %s WHERE k = 9"),
      new Family("UNIQUE", "CREATE INDEX ON %s (k) UNIQUE", "SELECT FROM %s WHERE k = 1", "SELECT FROM %s WHERE k = 9"),
      new Family("FULL_TEXT", "CREATE INDEX ON %s (s) FULL_TEXT", "SELECT FROM %s WHERE SEARCH_INDEX('%s[s]', 'w1') = true",
          "SELECT FROM %s WHERE SEARCH_INDEX('%s[s]', 'w9') = true"),
      new Family("LSM_TREE BY ITEM", "CREATE INDEX ON %s (tags BY ITEM) NOTUNIQUE", "SELECT FROM %s WHERE tags CONTAINS 'a1'",
          "SELECT FROM %s WHERE tags CONTAINS 'a9'"),
      new Family("HASH", "CREATE INDEX ON %s (k) UNIQUE_HASH", "SELECT FROM %s WHERE k = 1", "SELECT FROM %s WHERE k = 9"));

  @Test
  void rollbackAfterAWriteToTheIndexedTypeKeepsEveryCommittedRecordIndexed() {
    int n = 0;
    for (final Family family : FAMILIES) {
      final String type = createTypeWithThreeCommittedRecords("Ins" + n++);

      database.begin();
      database.command("sql", "INSERT INTO " + type + " SET k = 9, s = 'w9', tags = ['a9']");
      database.command("sql", family.create().formatted(type));
      database.rollback();

      assertThat(find(family.lookupOne(), type)).as(family.name() + " finds the committed record").isEqualTo(1);
      assertThat(find(family.lookupNine(), type)).as(family.name() + " does not find the rolled back record").isZero();
      assertThat(entries(type)).as(family.name() + " entries after the rollback").isGreaterThanOrEqualTo(3L);
    }
  }

  @Test
  void commitAfterAWriteToTheIndexedTypeIndexesTheCommittedAndTheNewRecord() {
    int n = 0;
    for (final Family family : FAMILIES) {
      final String type = createTypeWithThreeCommittedRecords("Commit" + n++);

      database.begin();
      database.command("sql", "INSERT INTO " + type + " SET k = 9, s = 'w9', tags = ['a9']");
      database.command("sql", family.create().formatted(type));
      database.commit();

      assertThat(find(family.lookupOne(), type)).as(family.name() + " finds the committed record").isEqualTo(1);
      assertThat(find(family.lookupNine(), type)).as(family.name() + " finds the record committed with the index").isEqualTo(1);
    }
  }

  @Test
  void anUpdateOfACommittedRecordIsAppliedOnCommitAndTakenBackOnRollback() {
    int n = 0;
    for (final Family family : FAMILIES) {
      final String rolledBack = createTypeWithThreeCommittedRecords("UpdRb" + n);
      final String committed = createTypeWithThreeCommittedRecords("UpdCm" + n++);

      database.begin();
      database.command("sql", "UPDATE " + rolledBack + " SET k = 9, s = 'w9', tags = ['a9'] WHERE k = 1");
      database.command("sql", family.create().formatted(rolledBack));
      database.rollback();
      assertThat(find(family.lookupOne(), rolledBack)).as(family.name() + " rollback keeps the old key").isEqualTo(1);
      assertThat(find(family.lookupNine(), rolledBack)).as(family.name() + " rollback drops the new key").isZero();

      database.begin();
      database.command("sql", "UPDATE " + committed + " SET k = 9, s = 'w9', tags = ['a9'] WHERE k = 1");
      database.command("sql", family.create().formatted(committed));
      database.commit();
      assertThat(find(family.lookupOne(), committed)).as(family.name() + " commit drops the old key").isZero();
      assertThat(find(family.lookupNine(), committed)).as(family.name() + " commit has the new key").isEqualTo(1);
    }
  }

  @Test
  void aDeleteOfACommittedRecordIsAppliedOnCommitAndTakenBackOnRollback() {
    int n = 0;
    for (final Family family : FAMILIES) {
      final String rolledBack = createTypeWithThreeCommittedRecords("DelRb" + n);
      final String committed = createTypeWithThreeCommittedRecords("DelCm" + n++);

      database.begin();
      database.command("sql", "DELETE FROM " + rolledBack + " WHERE k = 1");
      database.command("sql", family.create().formatted(rolledBack));
      database.rollback();
      assertThat(find(family.lookupOne(), rolledBack)).as(family.name() + " rollback restores the record").isEqualTo(1);

      database.begin();
      database.command("sql", "DELETE FROM " + committed + " WHERE k = 1");
      database.command("sql", family.create().formatted(committed));
      database.commit();
      assertThat(find(family.lookupOne(), committed)).as(family.name() + " commit removes the record").isZero();
    }
  }

  @Test
  void aUniqueIndexRefusesADuplicateOfACommittedKeyAfterTheRollback() {
    final String type = createTypeWithThreeCommittedRecords("Dup");

    database.begin();
    database.command("sql", "INSERT INTO " + type + " SET k = 9, s = 'w9', tags = ['a9']");
    database.command("sql", "CREATE INDEX ON " + type + " (k) UNIQUE");
    database.rollback();

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET k = 1").close()))
        .isInstanceOf(DuplicatedKeyException.class);
    assertThat(scan("SELECT FROM " + type + " WHERE k = 1")).isEqualTo(1);

    reopenDatabase();
    assertThat(find("SELECT FROM %s WHERE k = 1", type)).isEqualTo(1);
  }

  /**
   * A pending duplicate of a committed key is queued on the caller's transaction like any other write, so a UNIQUE index built
   * over it is created and the duplicate is refused where every unique violation is, at the caller's commit. The committed phase
   * of the build has committed on its own by then, and what the refused commit leaves behind is a complete index that goes on
   * refusing duplicates, not one the next write cannot trust.
   */
  @Test
  void aPendingDuplicateOfACommittedKeyIsRefusedAtTheCommitAndLeavesACompleteUniqueIndex() {
    final String type = createTypeWithThreeCommittedRecords("PendingDup");

    database.begin();
    database.command("sql", "INSERT INTO " + type + " SET k = 1, s = 'dup', tags = ['dup']");
    database.command("sql", "CREATE INDEX ON " + type + " (k) UNIQUE");
    assertThatThrownBy(() -> database.commit()).isInstanceOf(DuplicatedKeyException.class);
    if (database.isTransactionActive())
      database.rollback();

    assertThat(scan("SELECT FROM " + type + " WHERE k = 1")).as("only the committed record is left").isEqualTo(1);
    assertThat(entries(type)).as("the index holds every committed record").isEqualTo(3L);
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET k = 1").close()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  /** More writes than one chunk of the reconciliation holds: the chunk boundaries must not lose or repeat a record. */
  @Test
  void aTransactionWritingMoreThanOneChunkIsReconciledInFull() {
    final String type = createTypeWithThreeCommittedRecords("Chunks");
    final int inserted = IndexBuilder.BUILD_BATCH_SIZE + 123;

    database.begin();
    for (int i = 0; i < inserted; i++)
      database.newDocument(type).set("k", 1000 + i).set("s", "w").save();
    database.command("sql", "UPDATE " + type + " SET k = 9 WHERE k = 1");
    database.command("sql", "DELETE FROM " + type + " WHERE k = 2");
    database.command("sql", "CREATE INDEX ON " + type + " (k) NOTUNIQUE");
    database.commit();

    assertThat(find("SELECT FROM %s WHERE k = 1", type)).isZero();
    assertThat(find("SELECT FROM %s WHERE k = 2", type)).isZero();
    assertThat(find("SELECT FROM %s WHERE k = 9", type)).isEqualTo(1);
    assertThat(find("SELECT FROM %s WHERE k = 3", type)).isEqualTo(1);
    assertThat(find("SELECT FROM %s WHERE k = " + (1000 + inserted - 1), type)).isEqualTo(1);
    assertThat(find("SELECT FROM %s WHERE k = 1000", type)).isEqualTo(1);
    assertThat(entries(type)).isEqualTo(2L + inserted);
  }

  @Test
  void aTransactionThatCreatedAndThenUpdatedOrDeletedRecordsLeavesTheIndexConsistent() {
    final String type = createTypeWithThreeCommittedRecords("Mixed");

    database.begin();
    database.command("sql", "INSERT INTO " + type + " SET k = 7, s = 'w7', tags = ['a7']");
    database.command("sql", "INSERT INTO " + type + " SET k = 8, s = 'w8', tags = ['a8']");
    database.command("sql", "UPDATE " + type + " SET k = 70 WHERE k = 7");
    database.command("sql", "DELETE FROM " + type + " WHERE k = 8");
    database.command("sql", "UPDATE " + type + " SET k = 20 WHERE k = 2");
    database.command("sql", "DELETE FROM " + type + " WHERE k = 3");
    database.command("sql", "CREATE INDEX ON " + type + " (k) NOTUNIQUE");
    database.commit();

    assertThat(find("SELECT FROM %s WHERE k = 1", type)).isEqualTo(1);
    assertThat(find("SELECT FROM %s WHERE k = 2", type)).isZero();
    assertThat(find("SELECT FROM %s WHERE k = 20", type)).isEqualTo(1);
    assertThat(find("SELECT FROM %s WHERE k = 3", type)).isZero();
    assertThat(find("SELECT FROM %s WHERE k = 7", type)).isZero();
    assertThat(find("SELECT FROM %s WHERE k = 70", type)).isEqualTo(1);
    assertThat(find("SELECT FROM %s WHERE k = 8", type)).isZero();
    assertThat(entries(type)).isEqualTo(3L);
  }

  @Test
  void aSuperTypeIndexPropagatedToATypeWrittenInTheTransactionKeepsTheCommittedRecordsOnRollback() {
    database.command("sql", "CREATE DOCUMENT TYPE PParent");
    database.command("sql", "CREATE PROPERTY PParent.k INTEGER");
    database.command("sql", "CREATE INDEX ON PParent (k) UNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE PChild");
    database.command("sql", "CREATE PROPERTY PChild.k INTEGER");
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.command("sql", "INSERT INTO PChild SET k = " + i).close();
    });

    database.begin();
    database.command("sql", "INSERT INTO PChild SET k = 9");
    database.getSchema().getType("PChild").addSuperType("PParent");
    database.rollback();

    assertThat(database.getSchema().getIndexByName("PParent[k]").countEntries()).isEqualTo(3L);
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO PChild SET k = 1").close()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void aBucketIndexBuiltAfterAWriteToItsBucketKeepsTheCommittedRecordsOnRollback() {
    final DocumentType type = database.getSchema().createDocumentType("BucketIdx", 1);
    type.createProperty("k", Type.INTEGER);
    final String bucket = type.getBuckets(false).getFirst().getName();
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.newDocument("BucketIdx").set("k", i).save();
    });

    database.begin();
    database.newDocument("BucketIdx").set("k", 9).save();
    final Index index = database.getSchema().buildBucketIndex("BucketIdx", bucket, new String[] { "k" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    database.rollback();

    assertThat(index.countEntries()).isEqualTo(3L);
  }

  private String createTypeWithThreeCommittedRecords(final String name) {
    database.command("sql", "CREATE DOCUMENT TYPE " + name);
    database.command("sql", "CREATE PROPERTY " + name + ".k INTEGER");
    database.command("sql", "CREATE PROPERTY " + name + ".s STRING");
    database.command("sql", "CREATE PROPERTY " + name + ".tags LIST");
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.command("sql", "INSERT INTO " + name + " SET k = " + i + ", s = 'w" + i + "', tags = ['a" + i + "']").close();
    });
    return name;
  }

  /** Rows a lookup answers; the SEARCH_INDEX spelling carries the type twice. */
  private int find(final String sql, final String type) {
    return scan(sql.contains("%s[") ? sql.formatted(type, type) : sql.formatted(type));
  }

  private int scan(final String sql) {
    final List<Object> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows.size();
  }

  private long entries(final String type) {
    long entries = 0;
    for (final Index index : database.getSchema().getType(type).getAllIndexes(true))
      entries = Math.max(entries, index.countEntries());
    return entries;
  }
}
