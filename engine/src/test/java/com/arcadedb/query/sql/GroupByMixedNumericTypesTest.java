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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #4516: GROUP BY keys compared via {@code Arrays.equals -> Object.equals}.
 * Numerically-equal values represented with different numeric types (e.g. {@code BigDecimal("1")}
 * vs {@code BigDecimal("1.0")}, or {@code Integer(1)} vs {@code Long(1)}) were split into separate
 * groups, producing silently wrong totals/counts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GroupByMixedNumericTypesTest extends TestHelper {

  @Test
  void groupByDecimalDifferentScale() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE Sale");
      database.command("SQL", "CREATE PROPERTY Sale.amount DECIMAL");

      // Same logical value 1, but stored with different scales -> BigDecimal("1") vs BigDecimal("1.0")
      database.command("SQL", "INSERT INTO Sale SET amount = 1");
      database.command("SQL", "INSERT INTO Sale SET amount = 1.0");
      database.command("SQL", "INSERT INTO Sale SET amount = 1.00");

      final ResultSet rs = database.query("SQL", "SELECT amount, count(*) as cnt FROM Sale GROUP BY amount");

      int groups = 0;
      long total = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        groups++;
        total += ((Number) r.getProperty("cnt")).longValue();
      }

      // All three rows are numerically equal -> a single group with count 3
      assertThat(groups).isEqualTo(1);
      assertThat(total).isEqualTo(3L);
    });
  }

  @Test
  void groupByMixedIntegerAndLong() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE Item");

      // Schemaless property: drive the Java numeric type via bound parameters.
      database.command("SQL", "INSERT INTO Item SET k = :v", Map.of("v", Integer.valueOf(1)));
      database.command("SQL", "INSERT INTO Item SET k = :v", Map.of("v", Long.valueOf(1L)));
      database.command("SQL", "INSERT INTO Item SET k = :v", Map.of("v", Short.valueOf((short) 1)));

      final ResultSet rs = database.query("SQL", "SELECT k, count(*) as cnt FROM Item GROUP BY k");

      int groups = 0;
      long total = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        groups++;
        total += ((Number) r.getProperty("cnt")).longValue();
      }

      assertThat(groups).isEqualTo(1);
      assertThat(total).isEqualTo(3L);
    });
  }

  @Test
  void groupByDistinctNumericValuesStaySeparate() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE Reading");
      database.command("SQL", "CREATE PROPERTY Reading.v DECIMAL");

      database.command("SQL", "INSERT INTO Reading SET v = 1");
      database.command("SQL", "INSERT INTO Reading SET v = 1.0");
      database.command("SQL", "INSERT INTO Reading SET v = 2");
      database.command("SQL", "INSERT INTO Reading SET v = 2.50");
      database.command("SQL", "INSERT INTO Reading SET v = 2.5");

      final ResultSet rs = database.query("SQL", "SELECT v, count(*) as cnt FROM Reading GROUP BY v");

      int groups = 0;
      long total = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        groups++;
        total += ((Number) r.getProperty("cnt")).longValue();
      }

      // Distinct logical values: {1, 2, 2.5}
      assertThat(groups).isEqualTo(3);
      assertThat(total).isEqualTo(5L);
    });
  }
}
