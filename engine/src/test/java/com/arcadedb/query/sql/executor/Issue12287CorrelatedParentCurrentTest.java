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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12287: a correlated subquery reading {@code $parent.$current} answered nothing (1) as a per-record
 * LET when the outer rows came from an index, because {@code $current} still held the index entry, and (2) as the right
 * side of {@code IN}, because the lifted subquery alias was planned as an index key or filter ahead of the LET computing it.
 * Every statement runs on D (NOTUNIQUE index on k) and on N (same records, no index): both must give the same answer.
 */
class Issue12287CorrelatedParentCurrentTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      for (final String type : List.of("D", "N")) {
        database.getSchema().createDocumentType(type).createProperty("k", Integer.class);
        for (int k = 0; k < 3; k++)
          database.newDocument(type).set("k", k).save();
      }
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "D", "k");
    });
  }

  @Test
  void letSubqueryAfterIndexFetch() {
    check("SELECT k, $x.size() AS s FROM %s LET $x = (SELECT k FROM %s WHERE k >= $parent.$current.k) WHERE k < 2 ORDER BY k",
        List.of("0:3", "1:2"));
    check("SELECT k, $x.size() AS s FROM %s LET $x = (SELECT k FROM %s WHERE k = $parent.$current.k) WHERE k = 1", List.of("1:1"));
  }

  @Test
  void inCorrelatedSubquery() {
    check("SELECT k FROM %s WHERE k IN (SELECT k FROM %s WHERE k = $parent.$current.k) ORDER BY k", List.of("0", "1", "2"));
    check("SELECT k FROM %s WHERE k + 0 < 2 AND k IN (SELECT k FROM %s WHERE k = $parent.$current.k) ORDER BY k", List.of("0", "1"));
    check("SELECT k FROM %s WHERE k < 2 AND k IN (SELECT k FROM %s WHERE k = $parent.$current.k) ORDER BY k", List.of("0", "1"));
    check("SELECT k FROM %s WHERE k = 1 AND k IN (SELECT k FROM %s WHERE k = $parent.$current.k) ORDER BY k", List.of("1"));
  }

  @Test
  void inCorrelatedAndNonCorrelatedSubquery() {
    // one lifted sub-query is computed once (global LET), the other once per record: only the second one is late
    check("SELECT k FROM %s WHERE k IN (SELECT k FROM %s WHERE k >= $parent.$current.k) AND k IN (SELECT k FROM %s WHERE k <= 1) ORDER BY k",
        List.of("0", "1"));
  }

  @Test
  void correlatedSubqueryAsAnOperandOrAMethodTarget() {
    // shapes that already answered: they must keep doing so now that a correlated alias is no longer early calculated
    check("SELECT k FROM %s WHERE k < 2 AND (SELECT k FROM %s WHERE k = $parent.$current.k).size() > 0 ORDER BY k", List.of("0", "1"));
    check("SELECT k FROM %s WHERE k >= (SELECT k FROM %s WHERE k = $parent.$current.k)[0].k ORDER BY k", List.of("0", "1", "2"));
    check("SELECT k FROM %s WHERE k > (SELECT k FROM %s WHERE k = $parent.$current.k - 1)[0].k ORDER BY k", List.of("1", "2"));
  }

  @Test
  void nonCorrelatedInKeepsItsIndexFetch() {
    // only a sub-query that reads the outer row is late: one that does not is computed up front and stays an index key
    assertThat(plan("SELECT k FROM D WHERE k IN (SELECT k FROM D WHERE k < 2)")).contains("FETCH FROM INDEX D[k]");
    assertThat(plan("SELECT k FROM D WHERE k IN (SELECT k FROM D WHERE k = $parent.$current.k)")).doesNotContain("FETCH FROM INDEX");
  }

  @Test
  void aliasIsMatchedAsAWholeWord() {
    final String alias = "_$$$SUBQUERY$$$_1";
    assertThat(SelectExecutionPlanner.mentionsWord("k IN _$$$SUBQUERY$$$_1", alias)).isTrue();
    assertThat(SelectExecutionPlanner.mentionsWord("_$$$SUBQUERY$$$_1.size() > 0", alias)).isTrue();
    assertThat(SelectExecutionPlanner.mentionsWord("k IN _$$$SUBQUERY$$$_10", alias)).isFalse();
    assertThat(SelectExecutionPlanner.mentionsWord("k IN x_$$$SUBQUERY$$$_1", alias)).isFalse();
    assertThat(SelectExecutionPlanner.mentionsWord("k IN _$$$SUBQUERY$$$_10 AND j = _$$$SUBQUERY$$$_1", alias)).isTrue();
    assertThat(SelectExecutionPlanner.mentionsWord("k = 1", alias)).isFalse();
  }

  private String plan(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  private void check(final String template, final List<String> expected) {
    assertThat(rows(template.replace("%s", "D"))).as("indexed").isEqualTo(expected);
    assertThat(rows(template.replace("%s", "N"))).as("no index").isEqualTo(expected);
  }

  private List<String> rows(final String sql) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.hasProperty("s") ? r.getProperty("k") + ":" + r.getProperty("s") : String.valueOf((Object) r.getProperty("k")));
      }
    }
    return out;
  }
}
