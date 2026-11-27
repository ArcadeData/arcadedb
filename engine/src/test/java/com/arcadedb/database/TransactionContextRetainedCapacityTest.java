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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13479: the transaction context of a thread is reused by every transaction the thread runs on the database, for the life of
 * the database. A bulk statement run inside a caller transaction (the shape of every gRPC and HTTP command run in a session) grew the
 * statement's index journal to one holder per queued index entry, up to {@code arcadedb.txStatementSavepointMaxEntries} (100,000), and
 * the transaction's list of new records to one slot per created record. Both were cleared when the statement or the transaction ended
 * but never shrunk, so each worker thread of a server kept the high-water mark of the largest statement it ever ran: about 13 threads
 * held 1.3 million empty {@code RecordUndoEntry} holders in the reporter's heap. What a context keeps between transactions is now
 * bounded, whatever the size of the statements it ran.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class TransactionContextRetainedCapacityTest extends TestHelper {
  private static final int ROWS = 20_000;

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.command("sql", "CREATE PROPERTY T.k LONG").close();
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE").close();
    database.command("sql", "CREATE INDEX ON T (k) NOTUNIQUE").close();
  }

  private String bulkInsert(final int from, final int count) {
    final StringBuilder sql = new StringBuilder(count * 16).append("INSERT INTO T (id, k) VALUES ");
    for (int i = from; i < from + count; i++) {
      if (i > from)
        sql.append(", ");
      sql.append('(').append(i).append(", ").append(i % 100).append(')');
    }
    return sql.toString();
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private static void assertJournalTrimmed(final TransactionContext tx) {
    assertThat(tx.getIndexChanges().recordUndoHolders()).isLessThanOrEqualTo(TransactionIndexContext.RECORD_UNDO_RETAINED);
    assertThat(tx.getIndexChanges().recordUndoCapacity()).isLessThanOrEqualTo(TransactionIndexContext.RECORD_UNDO_RETAINED);
  }

  @Test
  void aBulkStatementInACallerTransactionLeavesNoJournalHoldersBehindWhenItEnds() {
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    // One statement queueing 2 index entries per row: 40,000 journal entries, under the 100,000 limit, so the journal is never abandoned
    database.command("sql", bulkInsert(0, ROWS)).close();

    // The statement is over, the transaction is not: the holders it no longer needs are gone already
    assertJournalTrimmed(tx);

    database.commit();

    assertJournalTrimmed(tx);
    assertThat(tx.getNewRecordsCapacity()).isLessThanOrEqualTo(TransactionNewRecords.MAX_RETAINED_CAPACITY);
    assertThat(count()).isEqualTo(ROWS);
  }

  @Test
  void theTrimmedJournalStillTakesBackAFailedStatementOfTheSameContext() {
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    database.command("sql", bulkInsert(0, ROWS)).close();
    assertJournalTrimmed(tx);

    // A second bulk statement of the same transaction that fails on its last row (duplicated id 0) is taken back whole: the journal
    // grows again past what was kept, and every entry it queued is undone
    final String failing = bulkInsert(ROWS, ROWS) + ", (0, 0)";
    try {
      database.command("sql", failing).close();
    } catch (final RuntimeException expected) {
      // DuplicatedKeyException
    }
    assertThat(count()).isEqualTo(ROWS);
    database.commit();

    assertThat(count()).isEqualTo(ROWS);
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T WHERE id = ?", ROWS)) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
    assertJournalTrimmed(tx);
  }
}
