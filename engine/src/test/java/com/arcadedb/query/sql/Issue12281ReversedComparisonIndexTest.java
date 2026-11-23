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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A comparison on an indexed property written with the value on the left ({@code 5 = id}, {@code :lo < id}) or under NOT
 * ({@code NOT (id <> 5)}, {@code NOT (id >= 3)}) is answered by the index, as the same comparison written property first is
 * (issue #12281). A range whose value has another type than the property keeps the scan: the two spellings convert in opposite
 * directions and do not select the same records.
 */
class Issue12281ReversedComparisonIndexTest extends TestHelper {
  private static final int RECORDS = 2_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 4");
    database.command("sql", "CREATE PROPERTY T.id INTEGER");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE NULL_STRATEGY SKIP");
    database.command("sql", "CREATE INDEX ON T (s) NOTUNIQUE NULL_STRATEGY SKIP");
    database.command("sql", "CREATE DOCUMENT TYPE S EXTENDS T");
    // the same records with no index: what a scan answers, which the indexed type must answer too
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.id INTEGER");
    database.command("sql", "CREATE PROPERTY U.s STRING");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        database.newDocument(i % 7 == 6 ? "S" : "T").set("id", i).set("s", "s" + (i % 50)).set("v", i).save();
        database.newDocument("U").set("id", i).set("s", "s" + (i % 50)).set("v", i).save();
      }
      // records with no id: a comparison with null is unknown, under NOT too
      for (int i = 0; i < 5; i++) {
        database.newDocument("T").set("s", "no-id").save();
        database.newDocument("U").set("s", "no-id").save();
      }
      // texts a number reads as 9 (or does not): an equality converts the right operand to the type of the left one
      final String[] nines = { "09", "9", " 9", "+9", "9.0", "9x" };
      for (int i = 0; i < nines.length; i++) {
        database.newDocument("T").set("id", RECORDS + 100 + i).set("s", nines[i]).save();
        database.newDocument("U").set("id", RECORDS + 100 + i).set("s", nines[i]).save();
      }
    });
    database.transaction(() -> {
      database.command("sql", "DELETE FROM T WHERE id = 4 OR id = 1500");
      database.command("sql", "DELETE FROM U WHERE id = 4 OR id = 1500");
    });

    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.d DOUBLE");
    database.command("sql", "CREATE PROPERTY D.f FLOAT");
    database.command("sql", "CREATE INDEX ON D (d) NOTUNIQUE NULL_STRATEGY SKIP");
    database.command("sql", "CREATE INDEX ON D (f) NOTUNIQUE NULL_STRATEGY SKIP");
    database.command("sql", "CREATE DOCUMENT TYPE DU");
    database.command("sql", "CREATE PROPERTY DU.d DOUBLE");
    database.command("sql", "CREATE PROPERTY DU.f FLOAT");
    database.transaction(() -> {
      for (final String type : new String[] { "D", "DU" }) {
        for (int i = 0; i < 20; i++)
          database.newDocument(type).set("d", i * 0.5).set("f", i * 0.5f).save();
        // no d, no f: unknown under every comparison
        database.newDocument(type).set("name", "empty").save();
      }
    });

    database.command("sql", "CREATE VERTEX TYPE V BUCKETS 4");
    database.command("sql", "CREATE PROPERTY V.id INTEGER");
    database.command("sql", "CREATE INDEX ON V (id) UNIQUE NULL_STRATEGY SKIP");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newVertex("V").set("id", i).save();
      database.newVertex("V").save();
    });
  }

  @Test
  void aValueOnTheLeftIsAnIndexKey() {
    assertIndexed("SELECT id FROM T WHERE 5 = id", Map.of(), id(i -> i == 5));
    assertIndexed("SELECT id FROM T WHERE :k = id", Map.of("k", 5), id(i -> i == 5));
    assertIndexed("SELECT id FROM T WHERE :lo < id", Map.of("lo", 1990), id(i -> i > 1990));
    assertIndexed("SELECT id FROM T WHERE 3 >= id", Map.of(), id(i -> i <= 3));
    assertIndexed("SELECT id FROM T WHERE 10 > id AND id > 2", Map.of(), id(i -> i > 2 && i < 10));
    assertIndexed("SELECT id FROM T WHERE id > 2 AND 10 > id", Map.of(), id(i -> i > 2 && i < 10));
    assertIndexed("SELECT id FROM T WHERE 's7' = s", Map.of(), d -> "s7".equals(d.getString("s")));
  }

  @Test
  void aComparisonUnderNotIsAnIndexKey() {
    assertIndexed("SELECT id FROM T WHERE NOT (id <> 5)", Map.of(), id(i -> i == 5));
    assertIndexed("SELECT id FROM T WHERE NOT (id != 5)", Map.of(), id(i -> i == 5));
    assertIndexed("SELECT id FROM T WHERE NOT (id >= 3)", Map.of(), id(i -> i < 3));
    assertIndexed("SELECT id FROM T WHERE NOT (id < 1995)", Map.of(), id(i -> i >= 1995));
    assertIndexed("SELECT id FROM T WHERE NOT (3 <= id)", Map.of(), id(i -> i < 3));
  }

  @Test
  void aValueOfAnotherTypeKeepsTheScan() {
    // '10' > id compares text, id < '10' numbers: not the same records, so not the index
    assertScanned("SELECT id FROM T WHERE '10' > id", Map.of());
    assertScanned("SELECT id FROM T WHERE 10 > s", Map.of());
    assertScanned("SELECT id FROM T WHERE NOT (id >= 'abc')", Map.of());
  }

  /**
   * {@code =} converts its right operand to the class of its left one, so it does not compare the same both ways round once the
   * two are not of one class: {@code 9 = s} reads the text "09" as 9 (a match) where the key of {@code s = 9} is the text "9" (no
   * match), and {@code '09' = id} compares the texts "09" and "9" (no match) where {@code id = '09'} reads the text as 9 (a match).
   * The index answers the property-first spelling, so only a value of the property's own kind may be turned round.
   */
  @Test
  void anEqualityWrittenValueFirstIsAnIndexKeyOnlyWhenItComparesAsThePropertyFirstOneDoes() {
    assertSameAsScan("T", "U", "id", "9 = s", Map.of(), false);
    assertSameAsScan("T", "U", "id", ":k = s", Map.of("k", 9), false);
    assertSameAsScan("T", "U", "id", "'09' = id", Map.of(), false);
    assertSameAsScan("T", "U", "id", "'9' = id", Map.of(), false);
    assertSameAsScan("T", "U", "id", ":k = id", Map.of("k", "09"), false);
    assertSameAsScan("T", "U", "id", "NOT ('09' <> id)", Map.of(), false);
    assertSameAsScan("T", "U", "id", "NOT (9 <> s)", Map.of(), false);

    // a value of the property's own kind converts nothing either way round
    assertSameAsScan("T", "U", "id", "9 = id", Map.of(), true);
    assertSameAsScan("T", "U", "id", ":k = id", Map.of("k", 9L), true);
    assertSameAsScan("T", "U", "id", "NOT (9 <> id)", Map.of(), true);
    assertSameAsScan("T", "U", "id", "'s7' = s", Map.of(), true);
    assertSameAsScan("T", "U", "id", ":k = s", Map.of("k", "09"), true);

    // the property first converts the value to its own type, in the index and in the scan alike
    assertSameAsScan("T", "U", "id", "s = 9", Map.of(), true);
    assertSameAsScan("T", "U", "id", "id = '09'", Map.of(), true);
    assertSameAsScan("T", "U", "id", "NOT (id <> '09')", Map.of(), true);
    assertSameAsScan("T", "U", "id", "NOT (s <> 9)", Map.of(), true);
  }

  @Test
  void aDoubleNegationIsTheConditionItself() {
    assertSameAsScan("T", "U", "id", "NOT (NOT (5 = id))", Map.of(), true);
    assertSameAsScan("T", "U", "id", "NOT (NOT (id = 5))", Map.of(), true);
    assertSameAsScan("T", "U", "id", "NOT (NOT (id < 3))", Map.of(), true);
    assertSameAsScan("T", "U", "id", "NOT (NOT (3 > id))", Map.of(), true);
    assertSameAsScan("T", "U", "id", "NOT (NOT (id <> 5))", Map.of(), false);
  }

  @Test
  void aNegationThatIsNoKeyKeepsTheScan() {
    for (final String where : new String[] { "NOT (id = 5)", "NOT (id = 5 OR id = 6)", "NOT (id > 2 AND id < 10)",
        "NOT (id <> 5 OR id < 3)", "NOT (id IN [5, 6])", "NOT (id BETWEEN 3 AND 9)" })
      assertSameAsScan("T", "U", "id", where, Map.of(), false);
  }

  @Test
  void aDecimalPropertyAnswersAsTheScanDoes() {
    for (final String property : new String[] { "d", "f" }) {
      for (final String where : new String[] { "2.5 > P", "NOT (P >= 2.5)", "NOT (2.5 <= P)", "2.5 = P", "NOT (P <> 2.5)", "NOT (2.5 <> P)",
          "P > 1 AND 4 > P", "3 > P", "NOT (P >= 3)", "3 = P" })
        assertSameAsScan("D", "DU", property, where.replace("P", property), Map.of(), true);
      assertSameAsScan("D", "DU", property, "2.5 > " + property + " AND NOT (" + property + " >= 4)", Map.of(), true);
      // a value that is no number is no key of a range, and the answer is the scan's
      assertSameAsScan("D", "DU", property, "'3' > " + property, Map.of(), false);
      assertSameAsScan("D", "DU", property, "NOT (" + property + " >= 'abc')", Map.of(), false);
    }
  }

  @Test
  void aNaNBoundAnswersAsTheScanDoes() {
    for (final String where : new String[] { "d < :v", ":v > d", "NOT (d >= :v)", "NOT (:v <= d)", ":v = d", "NOT (d <> :v)" })
      assertSameAsScan("D", "DU", "d", where, Map.of("v", Double.NaN), null);
  }

  /**
   * The planner reads a value it cannot answer from a record only when it is a literal or a bound parameter: any other expression
   * that needs no record (a call of a function) would be invoked once more than the statement asks for (issue #6179).
   */
  @Test
  void aValueThatIsCalledIsNotEvaluatedWhilePlanning() {
    final AtomicInteger calls = new AtomicInteger();
    final SQLQueryEngine sql = (SQLQueryEngine) database.getQueryEngine("sql");
    sql.getFunctionFactory().register(new SQLFunctionAbstract("counted12281") {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        calls.incrementAndGet();
        return 10;
      }

      @Override
      public String getSyntax() {
        return "counted12281()";
      }
    });
    try {
      for (final String where : new String[] { "counted12281() > id", "NOT (id >= counted12281())", "counted12281() = id",
          "NOT (counted12281() <> id)" }) {
        calls.set(0);
        // the plan alone: no row is pulled, so a call is the planner's
        try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT id FROM T WHERE " + where)) {
          final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
          assertThat(calls.get()).as("%s is called by the planner:%n%s", where, plan).isZero();
        }
      }
    } finally {
      sql.getFunctionFactory().unregister("counted12281");
    }
  }

  @Test
  void aParameterDecidesThePlanForItsOwnBinding() {
    final String sql = "SELECT id FROM T WHERE :v > id";
    assertScanned(sql, Map.of("v", "10"));
    assertIndexed(sql, Map.of("v", 10), id(i -> i < 10));
    assertScanned(sql, Map.of("v", "10"));
  }

  @Test
  void openCypherSeeksOnANegatedInequality() {
    final String cypher = "MATCH (n:V) WHERE NOT (n.id <> $k) RETURN n.id AS id";
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher, Map.of("k", 5))) {
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("issue #12281:%n%s", plan).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
    }
    for (final int k : new int[] { 5, 0, 1999, 2000 })
      try (final ResultSet rs = database.query("opencypher", cypher, Map.of("k", k))) {
        assertThat(rs.stream().map(r -> r.<Integer>getProperty("id")).toList()).as("k = %d", k)
            .containsExactlyElementsOf(k < RECORDS ? List.of(k) : List.of());
      }
  }

  @Test
  void openCypherSeeksOnANegatedInequalityBesideOtherConditions() {
    final String cypher = "MATCH (n:V) WHERE n.id > 2 AND NOT (n.id <> $k) RETURN n.id AS id";
    assertThat(openCypherPlan(cypher, 5)).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
    for (final int k : new int[] { 5, 1, 2, 3, 2000 })
      assertThat(openCypherIds(cypher, k)).as("k = %d", k).containsExactlyElementsOf(k > 2 && k < RECORDS ? List.of(k) : List.of());
  }

  @Test
  void openCypherKeepsTheScanWhereTheNegatedInequalityIsNotTrueOfEveryRow() {
    // an OR holds where one side does, and a NOT over an equality is no key: the label scan answers them
    for (final String where : new String[] { "n.id = 3 OR NOT (n.id <> $k)", "NOT (n.id = $k)", "NOT (n.id <> $k) OR n.id < 0" }) {
      final String cypher = "MATCH (n:V) WHERE " + where + " RETURN n.id AS id";
      assertThat(openCypherPlan(cypher, 5)).as(where).contains("NodeByLabelScan").doesNotContain("NodeIndexSeek");
    }
    assertThat(openCypherIds("MATCH (n:V) WHERE n.id = 3 OR NOT (n.id <> $k) RETURN n.id AS id", 5)).containsExactlyInAnyOrder(3, 5);
    assertThat(openCypherIds("MATCH (n:V) WHERE NOT (NOT (n.id = $k)) RETURN n.id AS id", 5)).containsExactly(5);
    assertThat(openCypherIds("MATCH (n:V) WHERE NOT (n.id = $k) AND n.id < 8 RETURN n.id AS id", 5)).containsExactlyInAnyOrder(0, 1, 2, 3,
        4, 6, 7);
    assertThat(openCypherIds("MATCH (n:V) WHERE NOT (n.id <> $k) OR n.id < 0 RETURN n.id AS id", 5)).containsExactly(5);
  }

  /**
   * A MATCH after an UNWIND is matched by {@code MatchNodeStep}, which seeks per row from the same equality predicates the optimizer
   * does: the negated inequality has to be one of them there too, or the same statement scans the label once per row.
   */
  @Test
  @SuppressWarnings("unchecked")
  void openCypherSeeksPerRowOnANegatedInequalityOfAnUnwoundValue() {
    final Map<String, Object> params = Map.of("ks", List.of(5, 1999, 2000, 7));
    final String equality = "UNWIND $ks AS k MATCH (n:V) WHERE n.id = k RETURN n.id AS id";
    assertThat(profile(equality, params)).as("the per-row lookup reads the index:%n%s", profile(equality, params)).contains("V[id]");
    // the condition, whether the lookup is a seek, the rows it answers
    final Object[][] cases = {
        { "NOT (n.id <> k)", true, List.of(5, 1999, 7) },
        { "NOT (n.id <> k) AND n.id > 2", true, List.of(5, 1999, 7) },
        { "NOT (n.id <> k) AND n.id > 6", true, List.of(1999, 7) },
        // not true of every row: an OR holds where its other side does
        { "NOT (n.id <> k) OR n.id = 3", false, List.of(5, 3, 1999, 3, 3, 7, 3) } };
    for (final Object[] testCase : cases) {
      final String where = (String) testCase[0];
      final String cypher = "UNWIND $ks AS k MATCH (n:V) WHERE " + where + " RETURN n.id AS id";
      assertThat(openCypherIds(cypher, params)).as(where).containsExactlyInAnyOrderElementsOf((List<Integer>) testCase[2]);
      final String profile = profile(cypher, params);
      if ((boolean) testCase[1])
        assertThat(profile).as("%s:%n%s", where, profile).contains("V[id]");
      else
        assertThat(profile).as("%s:%n%s", where, profile).doesNotContain("V[id]");
    }
  }

  @Test
  void aSqlMatchNodeFilterWithTheValueOnTheLeftIsAnIndexKey() {
    for (final String where : new String[] { "id = 5", "5 = id", "NOT (id <> 5)" })
      try (final ResultSet rs = database.query("sql", "MATCH {type: V, as: a, where: (" + where + ")} RETURN a.id AS id")) {
        final List<Integer> ids = rs.stream().map(r -> r.<Integer>getProperty("id")).toList();
        final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
        assertThat(plan).as("%s (issue #12281):%n%s", where, plan).contains("FETCH FROM INDEX V[id]");
        assertThat(ids).as(where).containsExactly(5);
      }
  }

  @Test
  void anUpdateFindsItsRecordThroughTheIndex() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE T SET v = -1 WHERE 5 = id")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
    });
    try (final ResultSet rs = database.query("sql", "SELECT v FROM T WHERE id = 5")) {
      assertThat(rs.next().<Integer>getProperty("v")).isEqualTo(-1);
    }
  }

  @Test
  void uncommittedChangesAreSeen() {
    database.begin();
    try {
      database.newDocument("T").set("id", 4).set("s", "back").save();
      database.command("sql", "UPDATE T SET id = 99999 WHERE id = 3");
      assertIndexed("SELECT id FROM T WHERE NOT (id >= 5)", Map.of(), id(i -> i < 5));
      assertIndexed("SELECT id FROM T WHERE 4 = id", Map.of(), id(i -> i == 4));
    } finally {
      database.rollback();
    }
  }

  private void assertIndexed(final String sql, final Map<String, Object> params, final Predicate<Document> expected) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      final List<Object> ids = rs.stream().map(r -> (Object) r.getProperty("id")).toList();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("%s %s is answered by an index (issue #12281):%n%s", sql, params, plan).contains("FETCH FROM INDEX T[")
          .doesNotContain("FETCH FROM TYPE");
      assertThat(ids).as("%s %s", sql, params).containsExactlyInAnyOrderElementsOf(expected(expected));
    }
  }

  private String openCypherPlan(final String cypher, final int k) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher, Map.of("k", k))) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private List<Integer> openCypherIds(final String cypher, final int k) {
    return openCypherIds(cypher, Map.of("k", k));
  }

  private List<Integer> openCypherIds(final String cypher, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", cypher, params)) {
      return rs.stream().map(r -> r.<Integer>getProperty("id")).toList();
    }
  }

  private String profile(final String cypher, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + cypher, params)) {
      rs.stream().count();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private void assertScanned(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      rs.stream().count();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("%s %s keeps the scan:%n%s", sql, params, plan).contains("FETCH FROM TYPE").doesNotContain("FETCH FROM INDEX");
    }
  }

  /**
   * The rows of {@code type} that {@code where} keeps are those a scan of {@code twin} (the same records, no index) keeps.
   *
   * @param indexed true when the plan must read an index of {@code type}, false when it must scan it, null when it may do either
   */
  private void assertSameAsScan(final String type, final String twin, final String column, final String where,
      final Map<String, Object> params, final Boolean indexed) {
    final List<Object> scanned;
    try (final ResultSet rs = database.query("sql", "SELECT " + column + " FROM " + twin + " WHERE " + where, params)) {
      scanned = rs.stream().map(r -> (Object) r.getProperty(column)).toList();
    }
    try (final ResultSet rs = database.query("sql", "SELECT " + column + " FROM " + type + " WHERE " + where, params)) {
      final List<Object> answered = rs.stream().map(r -> (Object) r.getProperty(column)).toList();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(answered).as("%s %s answers as a scan does:%n%s", where, params, plan).containsExactlyInAnyOrderElementsOf(scanned);
      if (indexed == null)
        return;
      if (indexed)
        assertThat(plan).as("%s %s is answered by an index (issue #12281):%n%s", where, params, plan)
            .contains("FETCH FROM INDEX " + type + "[").doesNotContain("FETCH FROM TYPE");
      else
        assertThat(plan).as("%s %s keeps the scan:%n%s", where, params, plan).contains("FETCH FROM TYPE").doesNotContain("FETCH FROM INDEX");
    }
  }

  private static Predicate<Document> id(final IntPredicate test) {
    return d -> d.getInteger("id") != null && test.test(d.getInteger("id"));
  }

  /** What a scan of T and its subtype keeps with {@code filter}. */
  private List<Object> expected(final Predicate<Document> filter) {
    final List<Object> ids = new ArrayList<>();
    final Iterator<?> it = database.iterateType("T", true);
    while (it.hasNext()) {
      final Document document = ((Record) it.next()).asDocument();
      if (filter.test(document))
        ids.add(document.getInteger("id"));
    }
    return ids;
  }
}
