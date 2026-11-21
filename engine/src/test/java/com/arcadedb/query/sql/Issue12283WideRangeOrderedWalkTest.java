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
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code WHERE <range on indexed id> ORDER BY <indexed score> DESC LIMIT n} walks score's index in order and filters, when the range
 * holds so many records that reading and sorting it costs more (issue #12283). The planner already weighed the two for an equality
 * (issue #11700); a range had no entry count, so the range was always read and sorted whole.
 */
class Issue12283WideRangeOrderedWalkTest extends TestHelper {
  private static final int RECORDS = 20_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 4");
    database.command("sql", "CREATE PROPERTY T.id INTEGER");
    database.command("sql", "CREATE PROPERTY T.score INTEGER");
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE");
    database.command("sql", "CREATE INDEX ON T (score) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        // distinct scores, none on every 97th record: the walk of score's index does not hold those
        database.newDocument("T").set("id", i).set("score", i % 97 == 0 ? null : (i * 7919) % 1_000_003).save();
    });
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE score = (SELECT max(score) AS m FROM T).m[0]"));
  }

  @Test
  void aWideRangeWalksTheSortKeyIndex() {
    assertWalked("SELECT id, score FROM T WHERE id > 10 ORDER BY score DESC LIMIT 10", Map.of(), i -> i > 10, 10);
    assertWalked("SELECT id, score FROM T WHERE id >= :lo AND id < :hi ORDER BY score DESC LIMIT 10", Map.of("lo", 10, "hi", 19_000),
        i -> i >= 10 && i < 19_000, 10);
    assertWalked("SELECT id, score FROM T WHERE id BETWEEN 5 AND 19990 ORDER BY score DESC LIMIT 20", Map.of(),
        i -> i >= 5 && i <= 19_990, 20);
    assertWalked("SELECT id, score FROM T WHERE id > 10 ORDER BY score DESC SKIP 5 LIMIT 10", Map.of(), i -> i > 10, 15);
  }

  @Test
  void aNarrowRangeIsStillReadAndSorted() {
    assertKept("SELECT id, score FROM T WHERE id < 50 ORDER BY score DESC LIMIT 10", i -> i < 50, 10);
    assertKept("SELECT id, score FROM T WHERE id > 19980 ORDER BY score DESC LIMIT 10", i -> i > 19_980, 10);
  }

  @Test
  void anAscendingSortWhoseNullsComeFirstKeepsTheRange() {
    // ascending, the records with no score come first: a walk would read every one of them before the first score (issue #11701)
    final String sql = "SELECT id, score FROM T WHERE id > 10 ORDER BY score ASC LIMIT 10";
    try (final ResultSet rs = database.query("sql", sql)) {
      final List<Map<String, Object>> rows = rs.stream().map(r -> Map.<String, Object>of("id", r.getProperty("id"),
          "score", String.valueOf(r.<Integer>getProperty("score")))).toList();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("%s keeps the range:%n%s", sql, plan).contains("FETCH FROM INDEX T[id]");
      // the first rows are records of the range with no score, any of them: they tie
      assertThat(rows).hasSize(10).allSatisfy(row -> {
        assertThat((Integer) row.get("id")).isGreaterThan(10);
        assertThat(row.get("score")).isEqualTo("null");
      });
    }
  }

  private void assertWalked(final String sql, final Map<String, Object> params, final IntPredicate range, final int rows) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      final List<Integer> ids = rs.stream().map(r -> r.<Integer>getProperty("id")).toList();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("%s walks score's index (issue #12283):%n%s", sql, plan).contains("FETCH FROM INDEX VALUES DESC T[score]")
          .doesNotContain("FETCH FROM INDEX T[id]");
      final int skip = sql.contains("SKIP 5") ? 5 : 0;
      assertThat(ids).as(sql).containsExactlyElementsOf(expected(range, true).subList(skip, rows));
    }
  }

  private void assertKept(final String sql, final IntPredicate range, final int rows) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final List<Integer> ids = rs.stream().map(r -> r.<Integer>getProperty("id")).toList();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("%s keeps the range:%n%s", sql, plan).contains("FETCH FROM INDEX T[id]");
      final List<Integer> expected = expected(range, true);
      assertThat(ids).as(sql).containsExactlyElementsOf(expected.subList(0, Math.min(rows, expected.size())));
    }
  }

  /** The ids of the records in {@code range}, by score (nulls first ascending, last descending), as ORDER BY score sorts them. */
  private List<Integer> expected(final IntPredicate range, final boolean descending) {
    final List<Document> documents = new ArrayList<>();
    final Iterator<?> it = database.iterateType("T", true);
    while (it.hasNext()) {
      final Document document = ((Record) it.next()).asDocument();
      if (range.test(document.getInteger("id")))
        documents.add(document);
    }
    final Comparator<Integer> byScore = descending ?
        Comparator.nullsLast(Comparator.<Integer>reverseOrder()) :
        Comparator.nullsFirst(Comparator.<Integer>naturalOrder());
    documents.sort(Comparator.comparing(d -> d.getInteger("score"), byScore));
    return documents.stream().map(d -> d.getInteger("id")).toList();
  }
}
