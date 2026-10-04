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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9207: UPDATE and DELETE planned their WHERE as a SELECT with the plan cache off, so every execution paid the
 * planning. The read side now comes from the execution plan cache, keyed on the text of the synthetic SELECT.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9207DmlSourcePlanCacheTest extends TestHelper {

  private static final String SOURCE = "SELECT FROM Part WHERE p_partkey = :k";

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Part");
      database.command("sql", "CREATE PROPERTY Part.p_partkey LONG");
      database.command("sql", "CREATE INDEX ON Part (p_partkey) UNIQUE");
      for (long k = 0; k < 20; k++)
        database.command("sql", "INSERT INTO Part SET p_partkey = ?, stock = 100", k);
    });
  }

  @Test
  void updateReusesTheCachedSourcePlanWithTheRightParameters() {
    for (long k = 0; k < 5; k++)
      database.transaction(() -> database.command("sql", "UPDATE Part SET stock = stock - 1 WHERE p_partkey = :k", Map.of("k", 0L)));
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(SOURCE)).isTrue();

    database.transaction(() -> database.command("sql", "UPDATE Part SET stock = stock - 1 WHERE p_partkey = :k", Map.of("k", 7L)));
    assertThat(stock(0L)).isEqualTo(95);
    assertThat(stock(7L)).isEqualTo(99);
    assertThat(stock(8L)).isEqualTo(100);
  }

  @Test
  void deleteReusesTheCachedSourcePlanWithTheRightParameters() {
    for (long k = 0; k < 3; k++) {
      final long key = k;
      database.transaction(() -> database.command("sql", "DELETE FROM Part WHERE p_partkey = :k", Map.of("k", key)));
    }
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(SOURCE)).isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Part")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(17L);
    }
  }

  @Test
  void cachedSourcePlanIsDroppedOnSchemaChange() {
    database.transaction(() -> database.command("sql", "UPDATE Part SET stock = 1 WHERE p_partkey = :k", Map.of("k", 1L)));
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(SOURCE)).isTrue();
    database.command("sql", "DROP INDEX `Part[p_partkey]`");
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(SOURCE)).isFalse();
    database.transaction(() -> database.command("sql", "UPDATE Part SET stock = 2 WHERE p_partkey = :k", Map.of("k", 1L)));
    assertThat(stock(1L)).isEqualTo(2);
  }

  @Test
  void halloweenGuardStillMaterializesOnACacheHit() {
    // the second execution takes the source plan from the cache: rows moved ahead of the index walk must not be revisited
    for (int run = 0; run < 2; run++) {
      database.transaction(() -> database.command("sql", "UPDATE Part SET p_partkey = p_partkey + 100 WHERE p_partkey >= 10"));
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Part")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(20L);
      }
    }
    try (final ResultSet rs = database.query("sql", "SELECT min(p_partkey) AS lo, max(p_partkey) AS hi FROM Part WHERE p_partkey >= 10")) {
      final Result r = rs.next();
      assertThat(r.<Long>getProperty("lo")).isEqualTo(210L);
      assertThat(r.<Long>getProperty("hi")).isEqualTo(219L);
    }
  }

  private int stock(final long key) {
    try (final ResultSet rs = database.query("sql", "SELECT stock FROM Part WHERE p_partkey = ?", key)) {
      return rs.next().<Integer>getProperty("stock");
    }
  }
}
