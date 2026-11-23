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

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12789: the context of a thread keeps the isolation level of its last transaction, and a read outside any transaction pinned its
 * pages as a {@code REPEATABLE_READ} transaction does, for good. Scans of the thread stayed frozen, and so did the pages the next
 * transaction started from.
 */
class Issue12789RepeatableReadOutlivesTransactionTest extends TestHelper {
  private static final String SCAN       = "SELECT count(*) AS c FROM T WHERE v >= 0";
  // LARGER THAN A PAGE: THE RECORD IS A CHAIN OF CHUNKS
  private static final int    BIG_CHARS  = 200 * 1024;
  private static final String BIG_BEFORE = "a".repeat(BIG_CHARS);
  private static final String BIG_AFTER  = "b".repeat(BIG_CHARS);

  @BeforeEach
  void load() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.v LONG");
    insert("T", 0, 100);
  }

  private void insert(final String type, final int from, final int to) {
    database.transaction(() -> {
      for (int i = from; i < to; i++)
        database.newDocument(type).set("v", (long) i).save();
    });
  }

  /** Runs {@code work} on a thread whose context has never held a transaction of this thread, and fails when it threw. */
  private void onAnotherThread(final Runnable work) throws InterruptedException {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread other = new Thread(() -> {
      try {
        work.run();
      } catch (final Throwable e) {
        failure.set(e);
      }
    });
    other.start();
    other.join();
    assertThat(failure.get()).as("the work of the other thread").isNull();
  }

  /** Commits {@code records} more records of T from another thread. */
  private void insertFromAnotherThread(final int records) throws InterruptedException {
    onAnotherThread(() -> insert("T", 1_000, 1_000 + records));
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long rows(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      long rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      return rows;
    }
  }

  @Test
  void aScanOutsideATransactionSeesWhatOtherThreadsCommitAfterARepeatableReadTransaction() throws InterruptedException {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    database.commit();

    assertThat(count(SCAN)).isEqualTo(100L);
    insertFromAnotherThread(50);
    assertThat(count(SCAN)).isEqualTo(150L);
    assertThat(count("SELECT count(*) AS c FROM T")).isEqualTo(150L);
  }

  @Test
  void theNextReadCommittedTransactionSeesWhatOtherThreadsCommitWhileItRuns() throws InterruptedException {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    database.commit();
    assertThat(count(SCAN)).isEqualTo(100L);

    database.begin();
    try {
      assertThat(count("SELECT count(*) AS c FROM T")).isEqualTo(100L);
      insertFromAnotherThread(200);
      assertThat(count(SCAN)).isEqualTo(300L);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aRepeatableReadTransactionStillReadsItsSnapshot() throws InterruptedException {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(count(SCAN)).isEqualTo(100L);
      insertFromAnotherThread(50);
      assertThat(count(SCAN)).isEqualTo(100L);
    } finally {
      database.rollback();
    }
    assertThat(count(SCAN)).isEqualTo(150L);
  }

  @Test
  void aRepeatableReadTransactionThatReadLeavesNothingPinnedWhenItCommits() throws InterruptedException {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    assertThat(count(SCAN)).isEqualTo(100L);
    database.commit();

    insertFromAnotherThread(50);
    assertThat(count(SCAN)).isEqualTo(150L);
  }

  @Test
  void aRepeatableReadTransactionThatReadLeavesNothingPinnedWhenItRollsBack() throws InterruptedException {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    assertThat(count(SCAN)).isEqualTo(100L);
    database.rollback();

    insertFromAnotherThread(50);
    assertThat(count(SCAN)).isEqualTo(150L);
  }

  @Test
  void anIndexedLookupOutsideATransactionSeesWhatOtherThreadsCommitAfterARepeatableReadTransaction() throws InterruptedException {
    database.command("sql", "CREATE DOCUMENT TYPE I");
    database.command("sql", "CREATE PROPERTY I.v LONG");
    database.command("sql", "CREATE INDEX ON I (v) NOTUNIQUE");
    insert("I", 0, 100);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    database.commit();

    assertThat(rows("SELECT FROM I WHERE v = 5")).isEqualTo(1L);
    assertThat(rows("SELECT FROM I WHERE v = 1000")).isZero();
    onAnotherThread(() -> insert("I", 1_000, 1_001));
    assertThat(rows("SELECT FROM I WHERE v = 1000")).isEqualTo(1L);
    assertThat(rows("SELECT FROM I WHERE v >= 1000")).isEqualTo(1L);
  }

  @Test
  void aMultiPageRecordReadOutsideATransactionIsReadAgainAfterARepeatableReadTransaction() throws InterruptedException {
    database.command("sql", "CREATE DOCUMENT TYPE Big");
    database.command("sql", "CREATE PROPERTY Big.v STRING");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Big").set("v", BIG_BEFORE).save().getIdentity());

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    database.commit();

    assertThat(bigValueOf(rid[0])).isEqualTo('a');
    onAnotherThread(() -> database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("v", BIG_AFTER).save()));
    assertThat(bigValueOf(rid[0])).isEqualTo('b');
  }

  /** The character the whole value of the record is made of, or '?' when it is not the one value the test wrote: a failure stays short. */
  private char bigValueOf(final RID rid) {
    final String value = database.lookupByRID(rid, true).asDocument().getString("v");
    if (value.equals(BIG_BEFORE))
      return 'a';
    return value.equals(BIG_AFTER) ? 'b' : '?';
  }
}
