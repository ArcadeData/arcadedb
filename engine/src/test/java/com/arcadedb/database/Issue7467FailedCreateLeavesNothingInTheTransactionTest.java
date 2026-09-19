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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #7467: a {@code save()} that a unique index refuses must leave the transaction exactly as it found it.
 * <p>
 * {@code LocalDatabase.createRecordNoLock} writes the record into the bucket, assigns its identity, increments
 * the bucket's record delta and puts it in the transaction's record cache, and only THEN runs
 * {@code DocumentIndexer.createDocument}, which is where the unique check lives. So a
 * {@link DuplicatedKeyException} raised there used to leave the record body in the transaction with nothing to
 * find it by, and any caller that tallies the refusal and carries on - the {@code /ws} insert session, the gRPC
 * insert stream, an HTTP batch, a SQL script with its own error handling - committed a record that:
 * <ul>
 * <li>exists in the bucket and is returned by a full scan,</li>
 * <li>is counted by {@code count(*)},</li>
 * <li>is NOT in the unique index that was supposed to forbid it, so no lookup by key finds it,</li>
 * <li>and was acknowledged to the client as not written.</li>
 * </ul>
 * Three views of the system that disagree, and no single one of them reveals the problem - which is why the
 * retraction belongs in the engine rather than in each caller.
 * <p>
 * The inline refusal is the same-transaction twin: {@code TransactionIndexContext.addIndexKeyLock} compares
 * against the keys this transaction has already queued. A duplicate of a COMMITTED record is decided at commit
 * time instead, which fails the whole transaction and was never the defect - the last test here keeps that
 * behaviour pinned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7467FailedCreateLeavesNothingInTheTransactionTest extends TestHelper {
  private static final String TYPE = "Keyed7467";

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("name", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "name");
  }

  /** The defect itself, on the shape the report describes: the refused row must not reach the commit. */
  @Test
  void aRefusedRowIsNotCommittedWithTheRestOfTheTransaction() {
    database.transaction(() -> {
      database.newDocument(TYPE).set("name", "dup").set("row", 1).save();

      assertThatThrownBy(() -> database.newDocument(TYPE).set("name", "dup").set("row", 2).save())
          .isInstanceOf(DuplicatedKeyException.class);

      // The caller carries on with the rest of its batch, exactly as the /ws session does.
      database.newDocument(TYPE).set("name", "other").set("row", 3).save();
    });

    assertThat(database.countType(TYPE, false)).as("the refused row must not be in the bucket").isEqualTo(2);
    assertThat(scanCount()).as("a full scan must agree with count(*)").isEqualTo(2);
    assertThat(rowOfKey("dup")).as("the key must belong to the row that took it").isEqualTo(1);
    assertThat(rowOfKey("other")).isEqualTo(3);
  }

  /**
   * The index is the authority afterwards: the key is taken by exactly one record, and a later transaction is
   * still refused. Before the fix the index held one entry and the bucket held two records, so which of them a
   * later insert collided with depended on what the index happened to hold.
   */
  @Test
  void theUniqueIndexStillHoldsExactlyOneEntryForTheKey() {
    database.transaction(() -> {
      database.newDocument(TYPE).set("name", "dup").set("row", 1).save();
      assertThat(catchThrowable(() -> database.newDocument(TYPE).set("name", "dup").set("row", 2).save()))
          .isInstanceOf(DuplicatedKeyException.class);
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE name = 'dup'")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }

    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument(TYPE).set("name", "dup").set("row", 4).save()))
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  /**
   * The document the engine handed back is re-insertable. Its identity was assigned by the bucket write that has
   * just been taken back, so leaving it set would make the next {@code save()} an UPDATE of a record that is not
   * there - which is the same reasoning {@code TransactionContext.rollback} applies to every new record.
   */
  @Test
  void theRefusedDocumentIsCleanlyReInsertableUnderAFreeKey() {
    final MutableDocument[] refused = new MutableDocument[1];

    database.transaction(() -> {
      database.newDocument(TYPE).set("name", "dup").save();

      refused[0] = database.newDocument(TYPE).set("name", "dup");
      assertThat(catchThrowable(refused[0]::save)).isInstanceOf(DuplicatedKeyException.class);
      assertThat(refused[0].getIdentity()).as("the identity of a create that was taken back").isNull();

      refused[0].set("name", "free");
      refused[0].save();
    });

    assertThat(database.countType(TYPE, false)).isEqualTo(2);
    assertThat(refused[0].getIdentity()).isNotNull();
  }

  /**
   * More than one index on the type: the undo has to take back the entries the indexes BEFORE the refusing one
   * already hold, or the record is gone and they still point at it.
   */
  @Test
  void theEntriesOfTheIndexesBeforeTheRefusingOneAreTakenBackToo() {
    // The NON-unique index is created first so the indexer reaches it first: by the time the unique one refuses
    // the row, this one already holds an entry for a record that is about to be taken back.
    final DocumentType type = database.getSchema().createDocumentType("Two7467");
    type.createProperty("tag", Type.STRING);
    type.createProperty("name", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Two7467", "tag");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Two7467", "name");

    database.transaction(() -> {
      database.newDocument("Two7467").set("name", "dup").set("tag", "shared").save();
      assertThat(catchThrowable(() -> database.newDocument("Two7467").set("name", "dup").set("tag", "shared").save()))
          .isInstanceOf(DuplicatedKeyException.class);
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM Two7467 WHERE tag = 'shared'")) {
      assertThat(rs.stream().count()).as("the non-unique index must not hold an entry for the refused row").isEqualTo(1);
    }
    assertThat(database.countType("Two7467", false)).isEqualTo(1);
  }

  /** A vertex goes down the same path, so the graph types get the same guarantee. */
  @Test
  void aRefusedVertexIsNotCommittedEither() {
    database.getSchema().createVertexType("V7467").createProperty("name", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V7467", "name");

    database.transaction(() -> {
      final MutableVertex first = database.newVertex("V7467").set("name", "dup");
      first.save();
      assertThat(catchThrowable(() -> database.newVertex("V7467").set("name", "dup").save()))
          .isInstanceOf(DuplicatedKeyException.class);
    });

    assertThat(database.countType("V7467", false)).isEqualTo(1);
  }

  /**
   * The counter-case that keeps the undo honest: a duplicate of a record that is already COMMITTED is not
   * decided at {@code save()} at all - the check runs inside the commit lock - so the whole transaction fails
   * and nothing of it lands. Unchanged by this fix, and the reason the /ws session documents {@code per_stream}
   * as reporting such a conflict on the commit frame rather than on the row.
   */
  @Test
  void aDuplicateOfACommittedRecordStillFailsTheWholeTransaction() {
    database.transaction(() -> database.newDocument(TYPE).set("name", "committed").save());

    assertThatThrownBy(() -> database.transaction(() -> {
      database.newDocument(TYPE).set("name", "fresh").save();
      database.newDocument(TYPE).set("name", "committed").save();
    })).isInstanceOf(DuplicatedKeyException.class);

    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  private long scanCount() {
    final long[] total = new long[1];
    database.transaction(() -> database.scanType(TYPE, false, record -> {
      total[0]++;
      return true;
    }));
    return total[0];
  }

  private int rowOfKey(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT row FROM " + TYPE + " WHERE name = ?", name)) {
      return rs.next().<Integer>getProperty("row");
    }
  }
}
