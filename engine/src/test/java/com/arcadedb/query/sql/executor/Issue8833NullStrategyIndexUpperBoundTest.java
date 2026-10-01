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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8833: a range with only an upper bound on a {@code NULL_STRATEGY INDEX} index started at the first key,
 * where the null keys sort, and returned the records with no value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8833NullStrategyIndexUpperBoundTest extends TestHelper {

  @BeforeEach
  void load() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.n LONG");
    database.command("sql", "CREATE INDEX ON T (n) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      for (long i = 0; i < 10; i++)
        database.newVertex("T").set("n", i).save();
      for (int i = 0; i < 3; i++)
        database.newVertex("T").save();
    });
  }

  private long count(final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T WHERE " + where)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private List<Object> values(final String sql) {
    final List<Object> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("n"));
    }
    return got;
  }

  @Test
  void upperBoundOnlyExcludesNullKeys() {
    assertThat(count("n < 2")).isEqualTo(2);
    assertThat(count("n <= 0")).isEqualTo(1);
    assertThat(count("n < 0")).isZero();
    assertThat(count("n >= 0 AND n < 2")).isEqualTo(2);
    assertThat(count("n BETWEEN -1 AND 1")).isEqualTo(2);
    assertThat(count("n IS NULL")).isEqualTo(3);
  }

  @Test
  void upperBoundOnlyRowsAndOrder() {
    assertThat(values("SELECT n FROM T WHERE n < 2")).containsExactlyInAnyOrder(0L, 1L);
    assertThat(values("SELECT n FROM T WHERE n <= 0 ORDER BY n LIMIT 1")).containsExactly(0L);
    assertThat(values("SELECT n FROM T WHERE n < 3 ORDER BY n DESC")).containsExactly(2L, 1L, 0L);
    assertThat(values("SELECT min(n) AS n FROM T WHERE n < 5")).containsExactly(0L);
  }

  @Test
  void compositeIndexUpperBoundOnLeadingColumnExcludesNullKeys() {
    database.command("sql", "CREATE VERTEX TYPE C");
    database.command("sql", "CREATE PROPERTY C.a LONG");
    database.command("sql", "CREATE PROPERTY C.b LONG");
    database.command("sql", "CREATE INDEX ON C (a, b) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      for (long i = 0; i < 10; i++)
        database.newVertex("C").set("a", i, "b", i).save();
      for (long i = 0; i < 3; i++)
        database.newVertex("C").set("b", i).save(); // a is null, b is not
    });
    assertThat(values("SELECT a AS n FROM C WHERE a < 2")).containsExactlyInAnyOrder(0L, 1L);
    assertThat(values("SELECT a AS n FROM C WHERE a < 3 ORDER BY a ASC")).containsExactly(0L, 1L, 2L);
    assertThat(values("SELECT a AS n FROM C WHERE a < 3 ORDER BY a DESC")).containsExactly(2L, 1L, 0L);
  }
}
