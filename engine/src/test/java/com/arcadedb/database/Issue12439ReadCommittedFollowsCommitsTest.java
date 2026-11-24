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
 * Issue #12439: a {@code READ_COMMITTED} transaction that has read a record of a type keeps following the commits of other transactions
 * in its {@code count(*)} and in its scan of the type.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12439ReadCommittedFollowsCommitsTest extends TestHelper {

  @BeforeEach
  void seed() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.id INTEGER");
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, v = 10");
      database.command("sql", "INSERT INTO T SET id = 2, v = 20");
    });
  }

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

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long scan() {
    try (final ResultSet rs = database.query("sql", "SELECT id FROM T")) {
      return rs.stream().count();
    }
  }

  private int valueOfFirst() {
    try (final ResultSet rs = database.query("sql", "SELECT v FROM T WHERE id = 1")) {
      return ((Number) rs.next().getProperty("v")).intValue();
    }
  }

  @Test
  void readCommittedFollowsCommitsAfterReadingARecord() throws InterruptedException {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    try {
      assertThat(valueOfFirst()).isEqualTo(10);

      onAnotherThread(() -> database.transaction(() -> database.command("sql", "UPDATE T SET v = 99 WHERE id = 1")));
      assertThat(count()).isEqualTo(2);

      onAnotherThread(() -> database.transaction(() -> database.command("sql", "INSERT INTO T SET id = 3, v = 30")));

      assertThat(count()).as("the count follows the insert another transaction committed").isEqualTo(3);
      assertThat(scan()).as("the scan sees the row another transaction committed").isEqualTo(3);
      assertThat(valueOfFirst()).as("a second read sees the committed update").isEqualTo(99);
    } finally {
      database.commit();
    }
  }

  /** The same thread ran a {@code REPEATABLE_READ} transaction and read outside any transaction before (#12789). */
  @Test
  void readCommittedFollowsCommitsAfterARepeatableReadTransactionOnTheThread() throws InterruptedException {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    assertThat(count()).isEqualTo(2);
    assertThat(scan()).isEqualTo(2);
    database.commit();

    // READS OUTSIDE A TRANSACTION, ON THE CONTEXT THAT KEPT THE REPEATABLE_READ LEVEL
    assertThat(valueOfFirst()).isEqualTo(10);
    assertThat(scan()).isEqualTo(2);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    try {
      assertThat(valueOfFirst()).isEqualTo(10);

      onAnotherThread(() -> database.transaction(() -> database.command("sql", "INSERT INTO T SET id = 3, v = 30")));

      assertThat(count()).isEqualTo(3);
      assertThat(scan()).isEqualTo(3);
    } finally {
      database.commit();
    }
  }
}
