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
package com.arcadedb.function.sql.math;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5906: {@code SUM()}/{@code AVG()} over an INTEGER column silently overflowed once the running total
 * exceeded {@code Integer.MAX_VALUE}, because {@code Type.increment()}'s "upgrade to long" guard recomputed the
 * same overflowing {@code int} addition before widening it instead of widening the operands first.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5906SumAverageIntegerOverflowTest {

  @Test
  void sumOverIntegerColumnDoesNotOverflow() throws Exception {
    TestHelper.executeInNewDatabase("issue-5906-sum", db -> {
      db.command("sql", "CREATE VERTEX TYPE V");
      db.command("sql", "CREATE PROPERTY V.n INTEGER");
      // TRUE SUM = 10,000,000,000 - FITS IN A LONG, NOT AN INTEGER
      for (int i = 0; i < 5; i++)
        db.command("sql", "INSERT INTO V SET n = 2000000000");

      try (final ResultSet rs = db.query("sql", "SELECT sum(n) AS s, avg(n) AS a, count(n) AS c FROM V")) {
        final var row = rs.next();
        assertThat(((Number) row.getProperty("s")).longValue()).isEqualTo(10_000_000_000L);
        assertThat(((Number) row.getProperty("a")).doubleValue()).isEqualTo(2.0E9);
        assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(5L);
      }
    });
  }

  @Test
  void sumOverIntegerColumnHandlesNegativeOverflow() throws Exception {
    TestHelper.executeInNewDatabase("issue-5906-sum-negative", db -> {
      db.command("sql", "CREATE VERTEX TYPE V");
      db.command("sql", "CREATE PROPERTY V.n INTEGER");
      for (int i = 0; i < 3; i++)
        db.command("sql", "INSERT INTO V SET n = -2000000000");

      try (final ResultSet rs = db.query("sql", "SELECT sum(n) AS s FROM V")) {
        assertThat(((Number) rs.next().getProperty("s")).longValue()).isEqualTo(-6_000_000_000L);
      }
    });
  }
}
