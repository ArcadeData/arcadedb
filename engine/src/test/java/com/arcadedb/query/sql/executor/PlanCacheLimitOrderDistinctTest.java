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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A statement with a LIMIT, an ORDER BY, a DISTINCT or an expand() is planned once per statement text and its plan is copied for each
 * execution, as a statement with only a WHERE and a projection is. The steps that sort, cut and deduplicate used to refuse the copy,
 * so every execution of such a statement planned it again.
 */
class PlanCacheLimitOrderDistinctTest extends TestHelper {
  private static final int RECORDS = 200;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Item");
    database.getSchema().getType("Item").createProperty("grp", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Item", "grp");
    database.getSchema().getType("Item").createProperty("seq", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Item").set("grp", i % 10).set("seq", i).set("tag", "t" + (i % 3)).save();
    });
  }

  @Test
  void aLimitedStatementIsPlannedOnce() {
    assertCachedAfterRun("SELECT FROM Item WHERE grp = :g LIMIT 3");
  }

  @Test
  void anOrderedStatementIsPlannedOnce() {
    assertCachedAfterRun("SELECT FROM Item WHERE grp = :g ORDER BY seq DESC LIMIT 4");
    assertCachedAfterRun("SELECT FROM Item WHERE grp = :g ORDER BY seq");
  }

  @Test
  void aDistinctStatementIsPlannedOnce() {
    // without an ORDER BY or a LIMIT the DISTINCT may be planned as a GROUP BY, a choice that depends on the size of the type
    assertCachedAfterRun("SELECT DISTINCT tag FROM Item WHERE grp = :g ORDER BY tag");
  }

  @Test
  void aSkipOfZeroIsPlannedOnceAndAnOffsetIsNot() {
    assertCachedAfterRun("SELECT FROM Item WHERE grp = :g SKIP 0 LIMIT 3");
    assertNotCachedAfterRun("SELECT FROM Item WHERE grp = :g SKIP 2 LIMIT 3");
  }

  @Test
  void aLimitReadFromAParameterIsPlannedForEveryExecution() {
    final String sql = "SELECT FROM Item WHERE grp = :g ORDER BY seq LIMIT :n";
    assertNotCachedAfterRun(sql);
    for (final int n : new int[] { 1, 5, 2, 7 })
      assertThat(seqs(sql, Map.of("g", 4, "n", n))).as("LIMIT %d", n).hasSize(n);
  }

  @Test
  void aCachedPlanAnswersEachExecutionWithItsOwnParameters() {
    final String top = "SELECT FROM Item WHERE grp = :g ORDER BY seq DESC LIMIT 4";
    final String asc = "SELECT FROM Item WHERE grp = :g ORDER BY seq";
    final String limited = "SELECT FROM Item WHERE grp = :g LIMIT 3";
    final String distinct = "SELECT DISTINCT tag FROM Item WHERE grp = :g ORDER BY tag";
    // planned by the first execution, copied by the others
    for (int round = 0; round < 3; round++)
      for (final int g : new int[] { 0, 7, 3, 7 }) {
        final List<Integer> own = new ArrayList<>();
        for (int i = 0; i < RECORDS; i++)
          if (i % 10 == g)
            own.add(i);

        assertThat(seqs(top, Map.of("g", g))).as("%s g=%d", top, g).containsExactlyElementsOf(own.reversed().subList(0, 4));
        assertThat(seqs(asc, Map.of("g", g))).as("%s g=%d", asc, g).containsExactlyElementsOf(own);
        assertThat(seqs(limited, Map.of("g", g))).as("%s g=%d", limited, g).hasSize(3).isSubsetOf(own);

        final List<String> tags = new ArrayList<>();
        try (final ResultSet rs = database.query("sql", distinct, Map.of("g", g))) {
          while (rs.hasNext())
            tags.add(rs.next().getProperty("tag"));
        }
        assertThat(tags).doesNotHaveDuplicates().hasSize(own.stream().map(i -> i % 3).distinct().toList().size());
      }
  }

  private void assertCachedAfterRun(final String sql) {
    run(sql);
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(sql)).as("plan cache holds: %s", sql).isTrue();
  }

  private void assertNotCachedAfterRun(final String sql) {
    run(sql);
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(sql)).as("plan cache holds: %s", sql).isFalse();
  }

  private void run(final String sql) {
    // a statement without the parameters it reads cannot run: bind the ones the shapes above use
    seqs(sql, Map.of("g", 4, "n", 2));
  }

  private List<Integer> seqs(final String sql, final Map<String, Object> params) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty("seq"));
      }
    }
    return values;
  }
}
