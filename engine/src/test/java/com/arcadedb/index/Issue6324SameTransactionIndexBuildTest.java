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

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6324, item 1: a {@code CREATE INDEX} that runs in the SAME transaction as the writes it
 * is supposed to index must index them.
 * <p>
 * The build scans the buckets, and a record written by the transaction the build runs in is not on disk yet - so it is
 * not in the scan. It cannot fall back on staging its own index operation either, because it was saved BEFORE the
 * index existed to stage one for. The result was an index that is readable, reported healthy by {@code CHECK DATABASE}
 * and answers {@code WHERE id = 7} with nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6324SameTransactionIndexBuildTest extends TestHelper {

  /** The reported shape, verbatim: one script, one transaction, an INSERT then a CREATE INDEX over it. */
  @Test
  @Timeout(60)
  void aScriptThatInsertsThenIndexesInOneTransactionIndexesWhatItInserted() {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

    database.transaction(
        () -> database.command("sqlscript", "INSERT INTO V SET id = 7; CREATE INDEX ON V (id) UNIQUE;").close());

    assertThat(database.countType("V", false)).as("the record is there").isEqualTo(1);

    final Index index = database.getSchema().getIndexByName("V[id]");
    assertThat(index.countEntries()).as("and so is its index entry").isEqualTo(1);
    assertThat(index.get(new Object[] { 7 }).hasNext()).as("an index that answers the lookup it exists for").isTrue();
  }

  /** The same through the API rather than through SQL: {@code createTypeIndex} inside an open transaction. */
  @Test
  @Timeout(60)
  void aTypeIndexCreatedInsideTheWritersTransactionIndexesTheUncommittedRecords() {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        v.save();
      }
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
    });

    assertThat(database.countType("V", false)).isEqualTo(10);

    final Index index = database.getSchema().getIndexByName("V[id]");
    assertThat(index.countEntries()).isEqualTo(10);
    for (int i = 0; i < 10; i++)
      assertThat(index.get(new Object[] { i }).hasNext()).as("id " + i + " must be found through the index").isTrue();
  }

  /**
   * The build must still see what the transaction DELETED, not only what it inserted: a record removed in the same
   * transaction has no business getting an index entry from a scan that has not caught up with the removal.
   */
  @Test
  @Timeout(60)
  void aRecordDeletedInTheSameTransactionGetsNoIndexEntry() {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

    database.transaction(() -> {
      for (int i = 0; i < 4; i++) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        v.save();
      }
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE id = 2")) {
        rs.next().getRecord().get().asDocument().getRecord().delete();
      }
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
    });

    assertThat(database.countType("V", false)).isEqualTo(3);

    final Index index = database.getSchema().getIndexByName("V[id]");
    assertThat(index.countEntries()).as("the deleted record must not leave a phantom entry behind").isEqualTo(3);
    assertThat(index.get(new Object[] { 2 }).hasNext()).as("and must not be answerable through the index").isFalse();
  }

  /**
   * And a record UPDATED in the same transaction must be indexed under its new key, not the one still on disk.
   */
  @Test
  @Timeout(60)
  void aRecordUpdatedInTheSameTransactionIsIndexedUnderItsNewKey() {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

    database.transaction(() -> {
      final MutableDocument v = database.newDocument("V");
      v.set("id", 1);
      v.save();
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE id = 1")) {
        final MutableDocument v = rs.next().getRecord().get().asDocument().modify();
        v.set("id", 42);
        v.save();
      }
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
    });

    final Index index = database.getSchema().getIndexByName("V[id]");
    assertThat(index.countEntries()).isEqualTo(1);
    // Asked of the INDEX and not of a SELECT, which would answer the same from a full scan and so would pass
    // whatever the index holds.
    assertThat(index.get(new Object[] { 42 }).hasNext()).as("the new key is the one the index answers on").isTrue();
    assertThat(index.get(new Object[] { 1 }).hasNext()).as("and the old key is not").isFalse();
  }

  /**
   * The other half of the split: the index COMPONENT is committed on its own, whatever the caller's transaction goes
   * on to do.
   * <p>
   * The schema entry naming the index is written by {@code recordFileChanges} as soon as the statement returns and
   * nothing later takes it back, so an index whose first page was left in a transaction that then rolled back would
   * be a schema entry pointing at a file with no pages - and the next write to it fails with "the file is invalid",
   * on a database that has been reopened since and has no idea why.
   */
  @Test
  @Timeout(60)
  void anIndexCreatedInATransactionThatRollsBackIsStillUsable() {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

    database.begin();
    final MutableDocument doomed = database.newDocument("V");
    doomed.set("id", 1);
    doomed.save();
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
    database.rollback();

    assertThat(database.countType("V", false)).as("the record went with the rollback").isZero();

    final Index index = database.getSchema().getIndexByName("V[id]");
    assertThat(index).as("the index the schema still names must still be there").isNotNull();
    assertThat(index.countEntries()).as("and empty, like the type").isZero();

    // The point of the test: the index is WRITEABLE afterwards, not merely present.
    database.transaction(() -> {
      final MutableDocument v = database.newDocument("V");
      v.set("id", 7);
      v.save();
    });
    assertThat(index.get(new Object[] { 7 }).hasNext()).isTrue();
  }

  /**
   * A NESTED transaction cannot see an outer one's uncommitted pages, so an index created in the outer transaction
   * has to be committed to be usable from inside a nested one - which is what the component half of the split is
   * for. The shape is ordinary enough to reach by accident: any test or handler running with an ambient transaction
   * and a {@code begin()}/{@code commit()} pair inside it.
   */
  @Test
  @Timeout(60)
  void anIndexCreatedInAnOuterTransactionIsWriteableFromANestedOne() {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

    database.begin();
    try {
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");

      database.begin();
      final MutableDocument v = database.newDocument("V");
      v.set("id", 3);
      v.save();
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.commit();
    }

    assertThat(database.getSchema().getIndexByName("V[id]").get(new Object[] { 3 }).hasNext()).isTrue();
  }
}
