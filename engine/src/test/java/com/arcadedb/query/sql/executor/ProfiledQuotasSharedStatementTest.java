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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code resultSetLimit} and {@code readTimeout} quotas of a security group are applied to the SQL statement a user runs.
 * That statement comes out of the database's statement cache, shared by every user and thread that runs the same text, and a
 * SELECT's plan out of a plan cache keyed by that text. The quotas used to be written INTO the shared statement, and the plan
 * was looked up by the text regardless of whose quotas it carried, so:
 * <ul>
 *   <li>a capped user's limit stayed in the statement and truncated the answer of every later user of the same text, root
 *       included, without any error;</li>
 *   <li>a user with a short read timeout made the same text time out for everyone after it;</li>
 *   <li>a capped user running a text an uncapped user had already planned got the cached, uncapped plan, and the cap was not
 *       applied at all.</li>
 * </ul>
 * The query engine wrote the limit into the shared TRAVERSE, UPDATE and DELETE as well, so a capped user's UPDATE or DELETE
 * changed only that many records for root too, and every statement replaced a LIMIT tighter than the cap with the cap.
 * <p>
 * {@code ServerProfilingIT.resultSetLimit} failed on "TIMEOUT clause of 1ms" whenever it ran after
 * {@code ServerProfilingIT.readTimeout}: its user has no read timeout, the previous test's user had one of 1 ms.
 */
class ProfiledQuotasSharedStatementTest {
  private static final String PATH    = "target/databases/ProfiledQuotasSharedStatementTest";
  private static final int    RECORDS = 20;
  private static final int    CAP     = 5;

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    // A SECURITY MANAGER, OR THE DATABASE READS NO QUOTA FROM THE CURRENT USER AT ALL
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE VERTEX TYPE V");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        database.newDocument("Doc").set("id", i).save();
        database.newVertex("V").set("id", i).save();
      }
    });
  }

  @AfterEach
  void tearDown() {
    bindUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aCappedUserDoesNotTruncateTheSelectOfTheNextUser() {
    bindUser(quotas(CAP, -1L));
    assertThat(count("SELECT FROM Doc")).isEqualTo(CAP);

    bindUser(null);
    assertThat(count("SELECT FROM Doc")).as("the cap belongs to the capped user, not to the text it ran").isEqualTo(RECORDS);
  }

  @Test
  void aCachedPlanDoesNotLiftTheCapOfTheNextUser() {
    assertThat(count("SELECT FROM Doc")).isEqualTo(RECORDS);

    bindUser(quotas(CAP, -1L));
    assertThat(count("SELECT FROM Doc")).as("the plan an uncapped user cached must not answer a capped one").isEqualTo(CAP);
  }

  @Test
  void aShortReadTimeoutDoesNotTimeOutTheSelectOfTheNextUser() throws InterruptedException {
    bindUser(quotas(-1L, 1L));
    assertThatThrownBy(() -> readSlowly("SELECT FROM Doc")).isInstanceOf(TimeoutException.class);

    bindUser(null);
    assertThat(readSlowly("SELECT FROM Doc")).as("the read timeout belongs to its user, not to the text it ran")
        .isEqualTo(RECORDS);
  }

  @Test
  void aCappedUserDoesNotTruncateTheMatchOfTheNextUser() {
    bindUser(quotas(CAP, -1L));
    assertThat(count("MATCH {type: V, as: v} RETURN v")).isEqualTo(CAP);

    bindUser(null);
    assertThat(count("MATCH {type: V, as: v} RETURN v")).as("the cap belongs to the capped user, not to the text it ran")
        .isEqualTo(RECORDS);
  }

  @Test
  void aStatementLimitTighterThanTheCapStaysInForce() {
    bindUser(quotas(CAP, -1L));
    assertThat(count("SELECT FROM Doc LIMIT 3")).isEqualTo(3);
  }

  @Test
  void aCappedUserDoesNotTruncateTheTraverseOfTheNextUser() {
    bindUser(quotas(CAP, -1L));
    assertThat(count("TRAVERSE out() FROM V")).isEqualTo(CAP);

    bindUser(null);
    assertThat(count("TRAVERSE out() FROM V")).as("the cap belongs to the capped user, not to the text it ran").isEqualTo(RECORDS);
  }

  @Test
  void aCappedUserDoesNotTruncateTheUpdateOfTheNextUser() {
    bindUser(quotas(CAP, -1L));
    assertThat(affected("UPDATE Doc SET touched = true")).isEqualTo(CAP);

    bindUser(null);
    assertThat(affected("UPDATE Doc SET touched = true")).as("the cap belongs to the capped user, not to the text it ran")
        .isEqualTo(RECORDS);
  }

  @Test
  void aCappedUserDoesNotTruncateTheDeleteOfTheNextUser() {
    bindUser(quotas(CAP, -1L));
    assertThat(affected("DELETE FROM Doc")).isEqualTo(CAP);

    bindUser(null);
    assertThat(affected("DELETE FROM Doc")).as("the cap belongs to the capped user, not to the text it ran")
        .isEqualTo(RECORDS - CAP);
  }

  @Test
  void aStatementLimitTighterThanTheCapStaysInForceOnEveryStatement() {
    bindUser(quotas(CAP, -1L));
    assertThat(count("MATCH {type: V, as: v} RETURN v LIMIT 3")).isEqualTo(3);
    assertThat(count("TRAVERSE out() FROM V LIMIT 3")).isEqualTo(3);
    assertThat(affected("UPDATE Doc SET touched = true LIMIT 3")).isEqualTo(3);
    assertThat(affected("DELETE FROM Doc LIMIT 3")).isEqualTo(3);
  }

  /** The count a DML statement answers with: the records it updated or deleted. */
  private long affected(final String sql) {
    final long[] affected = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql)) {
        affected[0] = rs.next().<Number>getProperty("count").longValue();
      }
    });
    return affected[0];
  }

  private int count(final String sql) {
    int count = 0;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
    }
    return count;
  }

  /** Reads the first row, then holds the read past a 1 ms read timeout before it reads the rest. */
  private int readSlowly(final String sql) throws InterruptedException {
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rs.next()).isNotNull();
      Thread.sleep(20);
      int count = 1;
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
      return count;
    }
  }

  private void bindUser(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user);
  }

  private static SecurityDatabaseUser quotas(final long resultSetLimit, final long readTimeout) {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "profiled";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return resultSetLimit;
      }

      @Override
      public long getReadTimeout() {
        return readTimeout;
      }
    };
  }
}
