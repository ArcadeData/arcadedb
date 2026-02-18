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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3419: Cypher timestamp() should return an integer (millis since epoch),
 * not a date in string format.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherTimestampTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-timestamp").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void timestampReturnsLong() {
    final long before = System.currentTimeMillis();
    try (final ResultSet rs = database.command("opencypher", "RETURN timestamp() AS ts")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object ts = row.getProperty("ts");
      assertThat(ts).isInstanceOf(Long.class);
      final long tsValue = (Long) ts;
      final long after = System.currentTimeMillis();
      assertThat(tsValue).isBetween(before, after);
    }
  }

  @Test
  void timestampIsNumeric() {
    try (final ResultSet rs = database.command("opencypher", "RETURN timestamp() > 0 AS positive")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("positive")).isTrue();
    }
  }

  @Test
  void timestampArithmetic() {
    try (final ResultSet rs = database.command("opencypher", "RETURN timestamp() - timestamp() AS diff")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object diff = row.getProperty("diff");
      assertThat(diff).isInstanceOf(Long.class);
      // The difference should be very small (close to 0)
      assertThat(Math.abs((Long) diff)).isLessThan(1000L);
    }
  }
}
