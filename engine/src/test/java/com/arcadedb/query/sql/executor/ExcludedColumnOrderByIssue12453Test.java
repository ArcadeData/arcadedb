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
import com.arcadedb.query.sql.parser.GeneratedAlias;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12453: when the ORDER BY key is not a projected alias the planner projects it under a
 * generated alias and re-projects the row after sorting. That second projection must keep {@code !name} an exclusion
 * (not turn it into a null {@code name} column) and must drop the generated ORDER BY alias that {@code *} copies.
 */
class ExcludedColumnOrderByIssue12453Test extends TestHelper {

  @BeforeEach
  void createData() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      for (int i = 2; i >= 0; i--)
        database.command("sql", "INSERT INTO P SET name = ?, secret = ?, seq = ?", "n" + i, "s" + i, i);
    });
  }

  private List<Result> rows(final String query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private void eachRow(final String query, final Consumer<Result> check) {
    final List<Result> rows = rows(query);
    assertThat(rows).hasSize(3);
    for (int i = 0; i < rows.size(); i++) {
      // the ORDER BY is still applied
      assertThat(rows.get(i).<Integer>getProperty("seq")).isEqualTo(i);
      check.accept(rows.get(i));
    }
  }

  private static void noGeneratedColumn(final Result row) {
    assertThat(row.getPropertyNames()).noneMatch(GeneratedAlias::is);
    assertThat(row.toJSON().keySet()).noneMatch(GeneratedAlias::is);
  }

  @Test
  void excludedPropertyStaysExcludedUnderOrderBy() {
    eachRow("SELECT *, !secret FROM P ORDER BY seq", row -> {
      assertThat(row.getPropertyNames()).doesNotContain("secret").contains("name", "seq");
      assertThat(row.hasProperty("secret")).isFalse();
      final JSONObject json = row.toJSON();
      assertThat(json.has("secret")).isFalse();
      assertThat(json.getString("name")).startsWith("n");
      noGeneratedColumn(row);
    });
  }

  @Test
  void excludedRecordAttributeStaysExcludedUnderOrderBy() {
    eachRow("SELECT *, !@rid FROM P ORDER BY seq", row -> {
      assertThat(row.getPropertyNames()).doesNotContain("@rid");
      assertThat(row.hasProperty("@rid")).isFalse();
      assertThat(row.toJSON().has("@rid")).isFalse();
      assertThat(row.getPropertyNames()).contains("secret", "name");
      noGeneratedColumn(row);
    });
  }

  @Test
  void starWithOrderByDropsTheGeneratedAlias() {
    final List<Result> rows = rows("SELECT *, name AS n FROM P ORDER BY seq DESC");
    assertThat(rows).extracting(r -> r.<Integer>getProperty("seq")).containsExactly(2, 1, 0);
    for (final Result row : rows) {
      assertThat(row.<String>getProperty("n")).isEqualTo(row.getProperty("name"));
      noGeneratedColumn(row);
    }
  }

  @Test
  void excludedPropertyUnderOrderByOnAnExpression() {
    final List<Result> rows = rows("SELECT *, !secret FROM P ORDER BY seq * -1");
    assertThat(rows).extracting(r -> r.<Integer>getProperty("seq")).containsExactly(2, 1, 0);
    for (final Result row : rows) {
      assertThat(row.hasProperty("secret")).isFalse();
      noGeneratedColumn(row);
    }
  }

  @Test
  void orderByTheExcludedPropertyStillSorts() {
    // records are inserted as seq 2, 1, 0 with secret s2, s1, s0: sorting on secret must reorder them
    eachRow("SELECT *, !secret FROM P ORDER BY secret", row -> {
      assertThat(row.hasProperty("secret")).isFalse();
      assertThat(row.toJSON().has("secret")).isFalse();
      noGeneratedColumn(row);
    });
  }

  @Test
  void orderByTheExcludedRecordAttributeStillSorts() {
    // records are inserted as seq 2, 1, 0, so their @rid ascends with insertion: descending @rid gives seq 0, 1, 2
    eachRow("SELECT *, !@rid FROM P ORDER BY @rid DESC", row -> {
      assertThat(row.hasProperty("@rid")).isFalse();
      assertThat(row.toJSON().has("@rid")).isFalse();
      noGeneratedColumn(row);
    });
  }

  @Test
  void explicitProjectionWithoutStarIsUnchanged() {
    eachRow("SELECT name, seq FROM P ORDER BY seq", row -> {
      assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("name", "seq");
    });
    final List<Result> rows = rows("SELECT name FROM P ORDER BY seq");
    assertThat(rows).extracting(r -> r.<String>getProperty("name")).containsExactly("n0", "n1", "n2");
    for (final Result row : rows)
      assertThat(row.getPropertyNames()).containsExactly("name");
  }
}
