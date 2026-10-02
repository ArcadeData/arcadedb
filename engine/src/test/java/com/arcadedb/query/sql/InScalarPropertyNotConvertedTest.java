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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #8913: an unindexed scalar {@code p IN [?]} must not convert the operands to the property's type,
 * so it agrees with {@code p = ?} and with the same IN through an index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class InScalarPropertyNotConvertedTest extends TestHelper {

  @Test
  void scalarInAgreesWithEqualsAndIndex() {
    final LocalDateTime micros = LocalDateTime.of(2026, 10, 1, 12, 34, 56, 789_123_000);
    final Object[][] cases = { { "STRING", "7", 7.0 }, { "DATE", LocalDate.of(2026, 10, 1), 1790812800000L },
        { "DATETIME_MICROS", micros, Date.from(micros.toInstant(ZoneOffset.UTC)) } };

    for (final Object[] c : cases) {
      final String t = "T_" + c[0];
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      database.command("sql", "CREATE PROPERTY " + t + ".a " + c[0]);
      database.command("sql", "CREATE PROPERTY " + t + ".b " + c[0]);
      database.command("sql", "CREATE INDEX ON " + t + " (a) NOTUNIQUE");
      database.transaction(() -> database.newDocument(t).set("a", c[1], "b", c[1]).save());

      final Object v = c[2];
      final long eq = count("SELECT FROM " + t + " WHERE b = ?", v);
      assertThat(count("SELECT FROM " + t + " WHERE b IN [?]", v)).as(c[0] + " unindexed IN list").isEqualTo(eq);
      assertThat(count("SELECT FROM " + t + " WHERE b IN ?", List.of(v))).as(c[0] + " unindexed IN param").isEqualTo(eq);
      assertThat(count("SELECT FROM " + t + " WHERE b IN ?", new HashSet<>(List.of(v)))).as(c[0] + " unindexed IN set").isEqualTo(eq);
      assertThat(count("SELECT FROM " + t + " WHERE a IN [?]", v)).as(c[0] + " indexed IN").isEqualTo(eq);
      assertThat(eq).isEqualTo(0);
    }
  }

  private long count(final String query, final Object... params) {
    try (final ResultSet rs = database.query("sql", query, params)) {
      return rs.stream().count();
    }
  }
}
