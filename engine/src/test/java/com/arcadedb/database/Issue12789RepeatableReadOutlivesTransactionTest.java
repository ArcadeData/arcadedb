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
 * Issue #12789: the context of a thread keeps the isolation level of its last transaction, and a read outside any transaction pinned its
 * pages as a {@code REPEATABLE_READ} transaction does, for good. Scans of the thread stayed frozen, and so did the pages the next
 * transaction started from.
 */
class Issue12789RepeatableReadOutlivesTransactionTest extends TestHelper {
  private static final String SCAN = "SELECT count(*) AS c FROM T WHERE v >= 0";

  @BeforeEach
  void load() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.v LONG");
    insert(100);
  }

  private void insert(final int records) {
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        database.newDocument("T").set("v", (long) i).save();
    });
  }

  /** Commits {@code records} more records from another thread, whose context has never held a transaction of this thread. */
  private void insertFromAnotherThread(final int records) throws InterruptedException {
    final Thread other = new Thread(() -> insert(records));
    other.start();
    other.join();
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
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
}
