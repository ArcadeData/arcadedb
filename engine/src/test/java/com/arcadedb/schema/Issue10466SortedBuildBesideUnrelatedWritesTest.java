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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10466: the sorted index build was refused whenever ANY transaction stacked on the thread held ANY write, so
 * {@code BEGIN; INSERT INTO Unrelated ...; CREATE INDEX ON Big (id) UNIQUE; COMMIT} paid the quadratic record-at-a-time
 * build of #10334 again. The sorted build only needs the buckets it scans to be free of pending writes, in every
 * transaction of the stack, since it sets the whole stack aside.
 */
class Issue10466SortedBuildBesideUnrelatedWritesTest extends TestHelper {
  private static final int RECORDS = 2_000;

  private final List<SortedIndexBuildMetrics> sortedBuilds = new ArrayList<>();

  @BeforeEach
  void captureSortedBuilds() {
    TypeIndexBuilder.setSortedBuildMetricsTestHook(sortedBuilds::add);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
    createPopulatedType("Big", 2, RECORDS);
    final DocumentType unrelated = database.getSchema().buildDocumentType().withName("Unrelated").withTotalBuckets(1).create();
    unrelated.createProperty("name", Type.STRING);
    unrelated.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
  }

  @AfterEach
  void releaseHook() {
    TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
  }

  @Test
  void writeToAnotherTypeKeepsTheSortedBuild() {
    database.begin();
    database.newDocument("Unrelated").set("name", "pending").save();

    database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
    assertThat(sortedBuilds).hasSize(1);

    // The caller's transaction is handed back with its own write still in it, and commits it
    assertThat(database.isTransactionActive()).isTrue();
    assertThat(((TransactionContext) ((DatabaseInternal) database).getTransaction()).hasChanges()).isTrue();
    database.newDocument("Big").set("id", "after-index").save();
    database.commit();

    assertThat(database.query("sql", "SELECT FROM Unrelated WHERE name = 'pending'").stream().count()).isEqualTo(1);
    final TypeIndex index = database.getSchema().getType("Big").getIndexByProperties("id");
    assertThat(index.countEntries()).isEqualTo(RECORDS + 1);
    assertThat(count(index.get(new Object[] { "after-index" }))).isEqualTo(1);
    assertThat(count(index.get(new Object[] { id(7) }))).isEqualTo(1);
    // The Unrelated index committed its pending entry along with the record
    assertThat(count(database.getSchema().getType("Unrelated").getIndexByProperties("name").get(new Object[] { "pending" })))
        .isEqualTo(1);

    // The new index is live and refuses a duplicate
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Big").set("id", id(3)).save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void rollingBackTheUnrelatedWriteKeepsTheIndex() {
    database.begin();
    database.newDocument("Unrelated").set("name", "pending").save();
    database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
    assertThat(sortedBuilds).hasSize(1);
    database.rollback();

    // The index is DDL and survives the rollback of the caller's transaction, as the record-at-a-time build's does
    assertThat(database.query("sql", "SELECT FROM Unrelated").stream().count()).isZero();
    final TypeIndex index = database.getSchema().getType("Big").getIndexByProperties("id");
    assertThat(index).isNotNull();
    assertThat(index.countEntries()).isEqualTo(RECORDS);
  }

  @Test
  void writeToAnotherTypeInAnOuterTransactionKeepsTheSortedBuild() {
    database.begin();
    database.newDocument("Unrelated").set("name", "outer").save();
    database.begin();
    database.newDocument("Unrelated").set("name", "inner").save();

    database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
    assertThat(sortedBuilds).hasSize(1);

    database.commit();
    database.commit();
    assertThat(database.query("sql", "SELECT FROM Unrelated").stream().count()).isEqualTo(2);
    assertThat(database.getSchema().getType("Big").getIndexByProperties("id").countEntries()).isEqualTo(RECORDS);
  }

  @Test
  void explicitSortedModeAcceptsAnUnrelatedWrite() {
    database.begin();
    database.newDocument("Unrelated").set("name", "pending").save();

    database.getSchema().buildTypeIndex("Big", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED).withUnique(true).create();
    assertThat(sortedBuilds).hasSize(1);
    database.commit();

    assertThat(database.getSchema().getType("Big").getIndexByProperties("id").countEntries()).isEqualTo(RECORDS);
  }

  @Test
  void insertIntoTheIndexedTypeInAnOuterTransactionStillRefusesTheSortedBuild() {
    // Outer wrote to Big, inner wrote elsewhere: the inner one is NOT the one that matters, every frame is asked
    database.begin();
    database.newDocument("Big").set("id", "outer-pending").save();
    database.begin();
    database.newDocument("Unrelated").set("name", "inner").save();

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Big", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED).withUnique(true).create())
        .hasMessageContaining("Sorted build requires no pending changes");

    database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
    assertThat(sortedBuilds).isEmpty();

    database.commit();
    database.commit();
  }

  @Test
  void updateOfAnIndexedRecordStillRefusesTheSortedBuild() {
    final RID rid = database.query("sql", "SELECT FROM Big LIMIT 1").next().getIdentity().get();

    database.begin();
    database.newDocument("Unrelated").set("name", "pending").save();
    final MutableDocument doc = rid.asDocument().modify();
    doc.set("id", "updated");
    doc.save();

    database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
    assertThat(sortedBuilds).isEmpty();
    database.commit();

    final TypeIndex index = database.getSchema().getType("Big").getIndexByProperties("id");
    assertThat(index.countEntries()).isEqualTo(RECORDS);
    assertThat(count(index.get(new Object[] { "updated" }))).isEqualTo(1);
  }

  @Test
  void deleteOfAnIndexedRecordStillRefusesTheSortedBuild() {
    final RID rid = database.query("sql", "SELECT FROM Big LIMIT 1").next().getIdentity().get();
    final String deletedId = rid.asDocument().getString("id");

    database.begin();
    database.newDocument("Unrelated").set("name", "pending").save();
    rid.asDocument().delete();

    database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
    assertThat(sortedBuilds).isEmpty();
    database.commit();

    final TypeIndex index = database.getSchema().getType("Big").getIndexByProperties("id");
    assertThat(index.countEntries()).isEqualTo(RECORDS - 1);
    assertThat(count(index.get(new Object[] { deletedId }))).isZero();
  }

  @Test
  void insertIntoASubTypeStillRefusesTheSortedBuild() {
    // The build is polymorphic, so a sub-type's bucket is one it scans
    database.getSchema().buildDocumentType().withName("BigChild").withSuperType("Big").withTotalBuckets(1).create();

    database.begin();
    database.newDocument("BigChild").set("id", "child-pending").save();

    database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
    assertThat(sortedBuilds).isEmpty();
    database.commit();

    final TypeIndex index = database.getSchema().getType("Big").getIndexByProperties("id");
    assertThat(count(index.get(new Object[] { "child-pending" }))).isEqualTo(1);
  }

  private void createPopulatedType(final String typeName, final int buckets, final int records) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
    type.createProperty("id", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        database.newDocument(typeName).set("id", id(i)).save();
    });
  }

  private static String id(final int i) {
    return "W" + (1_000_000_000L + ((i * 2_654_435_761L) & 0xFFFFFFFFL));
  }

  private static int count(final IndexCursor cursor) {
    int total = 0;
    while (cursor.hasNext()) {
      cursor.next();
      ++total;
    }
    return total;
  }
}
