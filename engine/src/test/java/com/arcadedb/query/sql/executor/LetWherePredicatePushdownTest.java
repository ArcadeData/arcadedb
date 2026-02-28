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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: WHERE clause referencing LET variables must not be pushed down
 * into the fetch step, since LET variables are computed after fetching.
 * Without the fix, predicate pushdown would evaluate $-variables before LET computes them,
 * causing the WHERE to silently filter out all rows.
 */
class LetWherePredicatePushdownTest extends TestHelper {

  @Test
  void letVariableInWhereShouldNotBePushedDown() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestDoc");
      database.command("sql", "CREATE PROPERTY TestDoc.value INTEGER");
      database.command("sql", "INSERT INTO TestDoc SET value = 5");
      database.command("sql", "INSERT INTO TestDoc SET value = 15");
      database.command("sql", "INSERT INTO TestDoc SET value = 25");
    });

    database.transaction(() -> {
      // LET computes $doubled, WHERE filters on it — should NOT be pushed into fetch
      final ResultSet result = database.query("sql",
          "SELECT value, $doubled AS doubled FROM TestDoc LET $doubled = (value * 2) WHERE $doubled < 20");

      int count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        final int value = row.getProperty("value");
        final int doubled = row.getProperty("doubled");
        assertThat(doubled).isEqualTo(value * 2);
        assertThat(doubled).isLessThan(20);
        count++;
      }
      // value=5 → doubled=10 (<20 ✓), value=15 → doubled=30 (✗), value=25 → doubled=50 (✗)
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void letExpressionInWhereWithOrderBy() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE ScoreDoc");
      database.command("sql", "CREATE PROPERTY ScoreDoc.name STRING");
      database.command("sql", "CREATE PROPERTY ScoreDoc.score FLOAT");
      database.command("sql", "INSERT INTO ScoreDoc SET name = 'A', score = 0.9");
      database.command("sql", "INSERT INTO ScoreDoc SET name = 'B', score = 0.5");
      database.command("sql", "INSERT INTO ScoreDoc SET name = 'C', score = 0.2");
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT name, $dist AS distance FROM ScoreDoc LET $dist = (1 - score) WHERE $dist < 0.6 ORDER BY $dist");

      assertThat(result.hasNext()).isTrue();
      final Result first = result.next();
      assertThat((String) first.getProperty("name")).isEqualTo("A");
      assertThat((float) first.getProperty("distance")).isCloseTo(0.1f, org.assertj.core.data.Offset.offset(0.001f));

      assertThat(result.hasNext()).isTrue();
      final Result second = result.next();
      assertThat((String) second.getProperty("name")).isEqualTo("B");
      assertThat((float) second.getProperty("distance")).isCloseTo(0.5f, org.assertj.core.data.Offset.offset(0.001f));

      assertThat(result.hasNext()).isFalse();
    });
  }
}
