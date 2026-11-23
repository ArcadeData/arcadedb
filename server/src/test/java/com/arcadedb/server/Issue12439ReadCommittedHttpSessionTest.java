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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12439: an HTTP session at {@code READ_COMMITTED} that has read a record of a type must still see the commits of other sessions
 * in its {@code count(*)} and in its scan of the type.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12439ReadCommittedHttpSessionTest extends BaseGraphServerTest {

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  private RemoteDatabase connect() {
    return new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private void seed(final RemoteDatabase db) {
    db.command("sql", "CREATE DOCUMENT TYPE T IF NOT EXISTS");
    db.command("sql", "CREATE PROPERTY T.id IF NOT EXISTS INTEGER");
    db.command("sql", "CREATE INDEX IF NOT EXISTS ON T (id) UNIQUE");
    db.command("sql", "INSERT INTO T SET id = 1, v = 10");
    db.command("sql", "INSERT INTO T SET id = 2, v = 20");
  }

  private static long count(final RemoteDatabase db) {
    try (final ResultSet rs = db.command("sql", "SELECT count(*) AS c FROM T")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private static long scan(final RemoteDatabase db) {
    try (final ResultSet rs = db.command("sql", "SELECT id FROM T")) {
      return rs.stream().count();
    }
  }

  @Test
  void readCommittedSessionSeesCommitsAfterReadingARecord() {
    try (final RemoteDatabase setup = connect()) {
      seed(setup);
    }

    try (final RemoteDatabase t1 = connect(); final RemoteDatabase t2 = connect()) {
      t1.begin();
      t2.begin();

      try (final ResultSet rs = t1.command("sql", "SELECT v FROM T WHERE id = 1")) {
        assertThat(((Number) rs.next().getProperty("v")).intValue()).isEqualTo(10);
      }

      t2.command("sql", "UPDATE T SET v = 99 WHERE id = 1");
      t2.commit();

      assertThat(count(t1)).isEqualTo(2);

      t2.begin();
      t2.command("sql", "INSERT INTO T SET id = 3, v = 30");
      t2.commit();

      assertThat(count(t1)).as("READ_COMMITTED count must follow the insert committed by the other session").isEqualTo(3);
      assertThat(scan(t1)).as("READ_COMMITTED scan must see the row committed by the other session").isEqualTo(3);
      try (final ResultSet rs = t1.command("sql", "SELECT v FROM T WHERE id = 1")) {
        assertThat(((Number) rs.next().getProperty("v")).intValue()).as("READ_COMMITTED re-read sees the update").isEqualTo(99);
      }

      t1.commit();
    }
  }

  /**
   * The control that proves the two sessions above really run inside their own transactions: the same sequence at {@code REPEATABLE_READ}
   * repeats the first count after the other session's insert.
   */
  @Test
  void repeatableReadSessionRepeatsItsCount() {
    try (final RemoteDatabase setup = connect()) {
      seed(setup);
    }

    try (final RemoteDatabase t1 = connect(); final RemoteDatabase t2 = connect()) {
      t1.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      t2.begin();

      try (final ResultSet rs = t1.command("sql", "SELECT v FROM T WHERE id = 1")) {
        assertThat(((Number) rs.next().getProperty("v")).intValue()).isEqualTo(10);
      }
      assertThat(count(t1)).isEqualTo(2);

      t2.command("sql", "INSERT INTO T SET id = 3, v = 30");
      t2.commit();

      assertThat(count(t1)).as("REPEATABLE_READ repeats the count it first read").isEqualTo(2);
      t1.commit();
    }
  }

  @Test
  void readCommittedSessionAfterRepeatableReadSessionsSeesCommits() {
    try (final RemoteDatabase setup = connect()) {
      seed(setup);
    }

    // EARLIER REPEATABLE_READ SESSIONS ON THE SAME SERVER, SO THE WORKER THREADS HAVE CARRIED THAT LEVEL
    for (int i = 0; i < 8; i++) {
      try (final RemoteDatabase rr = connect()) {
        rr.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
        count(rr);
        scan(rr);
        rr.commit();
      }
    }

    try (final RemoteDatabase t1 = connect(); final RemoteDatabase t2 = connect()) {
      t1.begin();
      t2.begin();

      try (final ResultSet rs = t1.command("sql", "SELECT v FROM T WHERE id = 1")) {
        assertThat(rs.hasNext()).isTrue();
      }
      assertThat(count(t1)).isEqualTo(2);

      t2.command("sql", "INSERT INTO T SET id = 3, v = 30");
      t2.commit();

      assertThat(count(t1)).isEqualTo(3);
      assertThat(scan(t1)).isEqualTo(3);
      t1.commit();
    }
  }
}
