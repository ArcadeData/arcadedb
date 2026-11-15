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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11890: {@code WHERE n.prop IN <list>} was served by an index seek when the list was a literal, a
 * parameter or a variable bound by a WITH, and scanned the whole label when it was a list computed from constants
 * ({@code range(1, 3)}, {@code [1, 2] + [3]}, a list comprehension over a range). Such a list is now evaluated once per execution
 * and sought key by key.
 * <p>
 * Every answer is checked against the same statement on a twin label without indexes, which can only scan: the seek has to
 * answer exactly as the scan does, nulls, duplicates and mixed kinds included.
 */
class CypherInConstantListIndexSeekIssue11890Test extends TestHelper {
  private static final int VERTICES = 200;

  @BeforeEach
  void createGraph() {
    for (final String type : List.of("P", "Q")) {
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".id LONG");
      database.command("sql", "CREATE PROPERTY " + type + ".g LONG");
      database.command("sql", "CREATE PROPERTY " + type + ".d DATE");
    }
    database.command("sql", "CREATE INDEX ON P (id) UNIQUE");
    database.command("sql", "CREATE INDEX ON P (g) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON P (d) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : List.of("P", "Q"))
        for (int i = 0; i < VERTICES; i++)
          database.command("opencypher", "CREATE (:" + type + " {id: $id, g: $g, d: $d})",
              Map.of("id", i, "g", i % 20, "d", LocalDate.of(2024, 1, 1 + i % 3))).close();
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "p.g IN range(1, 3)",
      "p.id IN range(100, 102)",
      "p.id IN [100, 101, 102] + [103]",
      "p.id IN [x IN range(1, 3) | x * 10]",
      "p.id IN [1 + 1, 3]",
      "p.id IN [x IN range(1, 30) WHERE x % 7 = 0]",
      "p.g IN [x IN [1, 2, 3] WHERE any(y IN [2, 3] WHERE y = x)]",
      "p.id IN range(10, 150, 7)",
      "p.id IN reverse(range(5, 9))",
      // Duplicates are one key each: no vertex is returned twice
      "p.id IN range(1, 3) + range(2, 4)",
      // A null equals nothing, and the other items still find theirs
      "p.id IN [1, null] + [2, 2]",
      // Mixed kinds keep the IN semantics: 1.0 equals 1, a text equals no number
      "p.id IN [1.0, 'x'] + [2]",
      // An empty list is never true
      "p.id IN range(5, 1)",
      "p.id IN range(1, 3) AND p.g > 1",
      "p.id IN range(1, 3) OR p.g IN [5]" })
  void aListComputedFromConstantsIsSought(final String predicate) {
    assertThat(plan(predicate)).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
    assertThat(profile("MATCH (p:P) WHERE " + predicate + " RETURN p.id")).doesNotContain("read the label");
    assertAnswersAsTheScan(predicate, Map.of());
  }

  @Test
  void aListComputedFromParametersIsSoughtPerExecution() {
    final String predicate = "p.id IN range($from, $to)";
    assertThat(plan(predicate, Map.of("from", 1, "to", 3))).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
    // The cached plan evaluates the list again for the parameters of each execution
    assertAnswersAsTheScan(predicate, Map.of("from", 1, "to", 3));
    assertAnswersAsTheScan(predicate, Map.of("from", 40, "to", 45));
    assertAnswersAsTheScan(predicate, Map.of("from", 7, "to", 2));
  }

  /** A list that reads the row, or that may answer differently on each evaluation, is left to the scan and the filter. */
  @ParameterizedTest
  @ValueSource(strings = {
      "p.id IN range(p.g, p.g + 1)",
      "p.id IN [toInteger(rand() * 0) + 5]",
      "p.id IN [x IN range(1, 3) | x + p.g]",
      // The iterator shadows the node, but the list it walks reads the node
      "p.id IN [p IN range(p.g, 3) | p]" })
  void aListOnlyTheRowCanEvaluateIsNotSought(final String predicate) {
    assertThat(plan(predicate)).doesNotContain("NodeIndexSeek");
    assertAnswersAsTheScan(predicate, Map.of());
  }

  /**
   * A computed list of temporal values, lists or maps is not sought: the index key conversion of those is not Cypher's equality.
   * Nor is one holding the text of a RID, which Cypher's equality compares with a number as the id it encodes ({@code 1 = '#0:1'},
   * issue #4183) and the index key conversion of a number refuses. The seek reads the label instead, and the predicate decides each
   * vertex.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "p.d IN [date('2024-01-02')] + []",
      "p.d IN [x IN range(1, 2) | date({year: 2024, month: 1, day: x})]",
      "p.id IN [[1], 2] + [3]",
      "p.id IN [x IN ['#0:1', '#0:2'] | x]",
      "p.id IN ['#0:3'] + [4]" })
  void aComputedListOfOtherKindsAnswersAsTheScan(final String predicate) {
    assertAnswersAsTheScan(predicate, Map.of());
  }

  /**
   * The predicate the seek keeps evaluates a list computed from constants once per execution, as the seek does: it used to rebuild
   * the list for every vertex it decided, quadratic in the list for a seek that finds a vertex per item.
   */
  @ParameterizedTest
  @ValueSource(strings = { "P", "Q" })
  void aComputedListIsEvaluatedOncePerExecution(final String label) {
    final List<Object> elements = new ArrayList<>();
    for (long i = 0; i < VERTICES; i++)
      elements.add(i);
    final CountingList ids = new CountingList(elements);
    assertThat(this.ids(label, "p.id IN [x IN $ids | x]", Map.of("ids", ids))).hasSize(VERTICES);
    assertThat(ids.reads).as("elements of a %d-element list read on %s", VERTICES, label).isLessThanOrEqualTo(4L * VERTICES);
  }

  /** A list that counts the elements read from it, by index or through its iterator. */
  private static final class CountingList extends AbstractList<Object> {
    private final List<Object> elements;
    private       long         reads;

    private CountingList(final List<Object> elements) {
      this.elements = elements;
    }

    @Override
    public Object get(final int index) {
      reads++;
      return elements.get(index);
    }

    @Override
    public int size() {
      return elements.size();
    }
  }

  /**
   * More items than the label has vertices: a seek per item would cost more than the scan, and would materialise the range. The
   * range cap, which scales with the heap, is lifted so the range is one no heap could hold as keys, on any runner.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aRangeLargerThanTheLabelIsNotMaterialised() {
    // The overlay value this replaces, null when the database had none: putting it back removes the override
    final Object previous = database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_RANGE_SIZE, -1L);
    try {
      assertThat(ids("P", "p.id IN range(0, 1000000000)", Map.of())).hasSize(VERTICES);
      assertThat(ids("P", "p.id IN range(-1000000000, 1000000000) AND p.g = 3", Map.of())).hasSize(VERTICES / 20);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_RANGE_SIZE, previous);
    }
  }

  /**
   * An aggregation over a seek that reads the label instead counts the rows the scan of the same statement counts, and PROFILE says
   * the label was read.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "p.id IN range(0, 100000)",
      "p.id IN range(-100000, 100000) AND p.g = 3",
      "p.d IN [date('2024-01-02')] + []" })
  void anAggregationOverTheLabelReadInsteadAnswersAsTheScan(final String predicate) {
    for (final String returned : List.of("count(p) AS c", "count(*) AS c", "sum(p.id) AS c", "p.g AS g, count(p) AS c ORDER BY g")) {
      final String query = " WHERE " + predicate + " RETURN " + returned;
      assertThat(rows("MATCH (p:P)" + query)).as(query).isEqualTo(rows("MATCH (p:Q)" + query));
    }
    assertThat(profile("MATCH (p:P) WHERE " + predicate + " RETURN count(p)")).contains("NodeIndexSeek").contains("read the label");
  }

  private String profile(final String query) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private void assertAnswersAsTheScan(final String predicate, final Map<String, Object> params) {
    assertThat(ids("P", predicate, params)).isEqualTo(ids("Q", predicate, params));
  }

  private List<Map<String, Object>> rows(final String query) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next().toMap());
    }
    return rows;
  }

  private List<Object> ids(final String label, final String predicate, final Map<String, Object> params) {
    final List<Object> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:" + label + ") WHERE " + predicate + " RETURN p.id AS id ORDER BY id", params)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private String plan(final String predicate) {
    return plan(predicate, Map.of());
  }

  private String plan(final String predicate, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (p:P) WHERE " + predicate + " RETURN p.id AS id",
        params)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
