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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per reported case.
 */
class SqlQueryPlanningTest {
  /**
   * Regression for #6926.
   * <p>
   * Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/6926">#6926</a>.
   * <p>
   * {@code SelectExecutionPlanner.fullySorted()} collected the ORDER BY items with {@code OrderByItem.getAlias()},
   * which is null for a record attribute (such as {@code @rid}) and for a complex expression (such as
   * {@code CASE WHEN}). With a composite index the size guard on the index property list did not stop the null from
   * reaching the {@code equals()} comparison, so the query blew up with an NPE at plan time instead of returning its
   * rows.
   * <p>
   * The same method also accepted an item carrying a modifier ({@code ORDER BY s.right(1)}) as if the index order on
   * {@code s} already satisfied it, which silently returned the rows in the wrong order.
   */
  @Nested
  class OrderByRecordAttributeWithIndex extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE I");
        database.command("sql", "CREATE PROPERTY I.p INTEGER");
        database.command("sql", "CREATE PROPERTY I.q INTEGER");
        database.command("sql", "CREATE INDEX ON I (p, q) NOTUNIQUE");

        // insert the p=1 rows with descending q, so the index order (p, q ASC) is the reverse of the RID order
        for (int q = 5; q >= 1; q--)
          database.command("sql", "INSERT INTO I SET p = 1, q = " + q);

        database.command("sql", "INSERT INTO I SET p = 2, q = 9");

        database.command("sql", "CREATE DOCUMENT TYPE J");
        database.command("sql", "CREATE PROPERTY J.p INTEGER");
        database.command("sql", "CREATE PROPERTY J.s STRING");
        database.command("sql", "CREATE INDEX ON J (p, s) NOTUNIQUE");

        // the index order on s (a5, b4, c3, d2, e1) is the reverse of the order on the last character
        database.command("sql", "INSERT INTO J SET p = 1, s = 'a5'");
        database.command("sql", "INSERT INTO J SET p = 1, s = 'b4'");
        database.command("sql", "INSERT INTO J SET p = 1, s = 'c3'");
        database.command("sql", "INSERT INTO J SET p = 1, s = 'd2'");
        database.command("sql", "INSERT INTO J SET p = 1, s = 'e1'");

        // single-property index: the fields.size() < orderedFields.size() guard already stopped the NPE here, so this
        // type pins that pre-existing rescue in place
        database.command("sql", "CREATE DOCUMENT TYPE K");
        database.command("sql", "CREATE PROPERTY K.p INTEGER");
        database.command("sql", "CREATE INDEX ON K (p) NOTUNIQUE");

        for (int i = 0; i < 5; i++)
          database.command("sql", "INSERT INTO K SET p = 1, i = " + i);
      });
    }

    @Test
    void orderByRidWithIndexedEqualityOnCompositeIndex() {
      final List<RID> rids = ridsOf("SELECT FROM I WHERE p = 1 ORDER BY @rid");

      assertThat(rids).hasSize(5);
      for (int i = 0; i < rids.size() - 1; i++)
        assertThat(rids.get(i).getPosition()).isLessThan(rids.get(i + 1).getPosition());
    }

    @Test
    void orderByRidDescWithIndexedEqualityOnCompositeIndex() {
      final List<RID> rids = ridsOf("SELECT FROM I WHERE p = 1 ORDER BY @rid DESC");

      assertThat(rids).hasSize(5);
      for (int i = 0; i < rids.size() - 1; i++)
        assertThat(rids.get(i).getPosition()).isGreaterThan(rids.get(i + 1).getPosition());
    }

    @Test
    void orderByIndexedPropertyThenRidWithIndexedEqualityOnCompositeIndex() {
      final List<RID> rids = ridsOf("SELECT FROM I WHERE p = 1 ORDER BY p, @rid");

      assertThat(rids).hasSize(5);
      for (int i = 0; i < rids.size() - 1; i++)
        assertThat(rids.get(i).getPosition()).isLessThan(rids.get(i + 1).getPosition());
    }

    @Test
    void orderByCaseExpressionWithIndexedEqualityOnCompositeIndex() {
      final List<Integer> qs = intsOf("q", "SELECT FROM I WHERE p = 1 ORDER BY CASE WHEN q > 3 THEN 0 ELSE 1 END");

      assertThat(qs).hasSize(5);
      assertThat(qs.subList(0, 2)).containsExactlyInAnyOrder(4, 5);
      assertThat(qs.subList(2, 5)).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    void orderByIndexedPropertyWithModifierIsNotSatisfiedByTheIndexOrder() {
      final List<String> values = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM J WHERE p = 1 ORDER BY s.right(1)")) {
        while (rs.hasNext())
          values.add(rs.next().getProperty("s"));
      }

      assertThat(values).containsExactly("e1", "d2", "c3", "b4", "a5");
    }

    @Test
    void orderByIndexedPropertyWithModifierDescIsNotSatisfiedByTheIndexOrder() {
      final List<String> values = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM J WHERE p = 1 ORDER BY s.right(1) DESC")) {
        while (rs.hasNext())
          values.add(rs.next().getProperty("s"));
      }

      assertThat(values).containsExactly("a5", "b4", "c3", "d2", "e1");
    }

    @Test
    void orderByRidWithIndexedEqualityOnSinglePropertyIndex() {
      final List<RID> rids = ridsOf("SELECT FROM K WHERE p = 1 ORDER BY @rid");

      assertThat(rids).hasSize(5);
      for (int i = 0; i < rids.size() - 1; i++)
        assertThat(rids.get(i).getPosition()).isLessThan(rids.get(i + 1).getPosition());

      final String plan = explain("SELECT FROM K WHERE p = 1 ORDER BY @rid");
      assertThat(plan).contains("FETCH FROM INDEX");
      assertThat(plan).contains("ORDER BY");
    }

    @Test
    void orderByIndexedPropertiesIsStillSatisfiedByTheIndexOrder() {
      assertThat(intsOf("q", "SELECT FROM I WHERE p = 1 ORDER BY p, q")).containsExactly(1, 2, 3, 4, 5);
      assertThat(intsOf("q", "SELECT FROM I WHERE p = 1 ORDER BY q")).containsExactly(1, 2, 3, 4, 5);

      // the rows would come out in this order anyway if the ORDER BY step were kept, so assert on the plan too:
      // without it, this test would still pass if fullySorted() had simply been made to always answer false
      assertThat(explain("SELECT FROM I WHERE p = 1 ORDER BY p, q")).doesNotContain("ORDER BY");
      assertThat(explain("SELECT FROM I WHERE p = 1 ORDER BY q")).doesNotContain("ORDER BY");
    }

    @Test
    void indexedEqualityStillUsesTheIndexWhileTheOrderByStepIsKept() {
      final String plan = explain("SELECT FROM I WHERE p = 1 ORDER BY @rid");
      assertThat(plan).contains("FETCH FROM INDEX");
      assertThat(plan).contains("ORDER BY");
    }

    private String explain(final String query) {
      final StringBuilder plan = new StringBuilder();
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
        while (rs.hasNext())
          plan.append(rs.next().toJSON());
      }
      return plan.toString();
    }

    private List<Integer> intsOf(final String property, final String query) {
      final List<Integer> values = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext())
          values.add(rs.next().getProperty(property));
      }
      return values;
    }

    private List<RID> ridsOf(final String query) {
      final List<RID> rids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext())
          rs.next().getIdentity().ifPresent(rids::add);
      }
      return rids;
    }
  }

  /**
   * Regression for #8889.
   * <p>
   * {@code COUNT(DISTINCT expr)} was a syntax error, with or without GROUP BY: only a DISTINCT subquery counted the
   * distinct values (issue #8889).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CountDistinct extends TestHelper {

    private void load() {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO P SET g = 'x', b = 1");
        database.command("sql", "INSERT INTO P SET g = 'x', b = 1");
        database.command("sql", "INSERT INTO P SET g = 'x', b = 2");
        database.command("sql", "INSERT INTO P SET g = 'y', b = 3");
      });
    }

    @Test
    void countDistinctWithoutGroupBy() {
      load();
      try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT b) AS n FROM P")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(3L);
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(distinct b) AS n, count(b) AS total, count(*) AS rows FROM P")) {
        final var row = rs.next();
        assertThat(row.<Long>getProperty("n")).isEqualTo(3L);
        assertThat(row.<Long>getProperty("total")).isEqualTo(4L);
        assertThat(row.<Long>getProperty("rows")).isEqualTo(4L);
      }
    }

    @Test
    void countDistinctWithGroupBy() {
      load();
      final Map<String, Long> counts = new HashMap<>();
      try (final ResultSet rs = database.query("sql", "SELECT g, count(DISTINCT b) AS n FROM P GROUP BY g")) {
        while (rs.hasNext()) {
          final var row = rs.next();
          counts.put(row.getProperty("g"), row.getProperty("n"));
        }
      }
      assertThat(counts).containsOnly(Map.entry("x", 2L), Map.entry("y", 1L));
    }

    @Test
    void countDistinctOfAParenthesizedArgument() {
      load();
      // used to be rejected as "'distinct' is supported only as the whole SELECT projection"
      try (final ResultSet rs = database.query("sql", "SELECT count(distinct(b)) AS n FROM P")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(3L);
      }
    }

    @Test
    void numbersAreOneValueWhateverTheirTypeOrScale() {
      database.command("sql", "CREATE DOCUMENT TYPE N");
      database.transaction(() -> {
        database.newDocument("N").set("v", 1).save();
        database.newDocument("N").set("v", 1.0).save();
        database.newDocument("N").set("v", 1L).save();
        database.newDocument("N").set("v", new BigDecimal("19.9")).save();
        database.newDocument("N").set("v", new BigDecimal("19.90")).save();
        database.newDocument("N").set("v", 19.9d).save();
      });
      try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT v) AS n FROM N")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(2L);
      }
    }

    @Test
    void nullsAreNotCountedAndAnEmptyTypeCountsZero() {
      database.command("sql", "CREATE DOCUMENT TYPE Q");
      try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT v) AS n FROM Q")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(0L);
      }
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Q SET v = 'a'");
        database.command("sql", "INSERT INTO Q SET v = 'a'");
        database.command("sql", "INSERT INTO Q SET v = 'b'");
        database.command("sql", "INSERT INTO Q SET w = 1");
        database.command("sql", "INSERT INTO Q SET v = null");
      });
      try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT v) AS n FROM Q")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(2L);
      }
    }

    @Test
    void sumAndAvgOfDistinctValues() {
      load();
      try (final ResultSet rs = database.query("sql", "SELECT sum(DISTINCT b) AS s, avg(DISTINCT b) AS a FROM P")) {
        final var row = rs.next();
        assertThat(row.<Number>getProperty("s").intValue()).isEqualTo(6);
        assertThat(row.<Number>getProperty("a").doubleValue()).isEqualTo(2.0);
      }
    }

    @Test
    void manyRowsSpreadOverGroups() {
      database.command("sql", "CREATE DOCUMENT TYPE Big");
      database.transaction(() -> {
        for (int i = 0; i < 6_000; i++)
          database.newDocument("Big").set("g", i % 3, "v", i % 100).save();
      });
      try (final ResultSet rs = database.query("sql", "SELECT g, count(DISTINCT v) AS n FROM Big GROUP BY g")) {
        int groups = 0;
        while (rs.hasNext()) {
          final var row = rs.next();
          // g = 0 sees i = 0,3,6... so v covers every residue of 100 reachable by multiples of 3: all 100 of them
          assertThat(row.<Long>getProperty("n")).isEqualTo(100L);
          groups++;
        }
        assertThat(groups).isEqualTo(3);
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT v) AS n FROM Big")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(100L);
      }
    }

    @Test
    void distinctInAScalarFunctionIsRejected() {
      load();
      assertThatThrownBy(() -> database.query("sql", "SELECT abs(DISTINCT b) AS n FROM P").next())
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("DISTINCT is supported only by an aggregate function");
    }

    @Test
    void countDistinctOfAStarIsASyntaxError() {
      load();
      assertThatThrownBy(() -> database.query("sql", "SELECT count(DISTINCT *) AS n FROM P").next())
          .isInstanceOf(CommandSQLParsingException.class);
    }

    @Test
    void countDistinctWithoutATarget() {
      try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT 1) AS n")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(1L);
      }
    }

    @Test
    void distinctInsideAnExpressionFilterAndOrderBy() {
      load();
      try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT b) + 1 AS n, count(DISTINCT b) * 1.0 / count(*) AS r FROM P")) {
        final var row = rs.next();
        assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(4L);
        assertThat(row.<Number>getProperty("r").doubleValue()).isEqualTo(0.75);
      }
      try (final ResultSet rs = database.query("sql",
          "SELECT FROM (SELECT g, count(DISTINCT b) AS n FROM P GROUP BY g) WHERE n > 1")) {
        assertThat(rs.next().<String>getProperty("g")).isEqualTo("x");
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("sql", "SELECT g, count(DISTINCT b) AS n FROM P GROUP BY g ORDER BY n DESC")) {
        assertThat(rs.next().<String>getProperty("g")).isEqualTo("x");
        assertThat(rs.next().<String>getProperty("g")).isEqualTo("y");
      }
    }

    @Test
    void aNullIsOneDistinctValueForAFunctionThatKeepsIt() {
      database.command("sql", "CREATE DOCUMENT TYPE Nl");
      database.transaction(() -> {
        database.newDocument("Nl").set("v", 1).save();
        database.newDocument("Nl").set("w", 1).save();
        database.newDocument("Nl").set("w", 2).save();
      });
      // list() skips a NULL, so the one NULL the DISTINCT set lets through leaves the list exactly as list(v)
      try (final ResultSet rs = database.query("sql", "SELECT list(DISTINCT v) AS l, list(v) AS all FROM Nl")) {
        final var row = rs.next();
        assertThat(row.<List<Object>>getProperty("l")).containsExactly(1);
        assertThat(row.<List<Object>>getProperty("all")).containsExactly(1);
      }
    }

    @Test
    void distinctOnAnAggregateThatHoldsEveryValue() {
      load();
      try (final ResultSet rs = database.query("sql", "SELECT list(DISTINCT b) AS l FROM P")) {
        assertThat(rs.next().<List<Object>>getProperty("l")).hasSize(3);
      }
    }

    @Test
    void collectionsAreOneValueWhateverTheirNumbersAreTypedAs() {
      database.command("sql", "CREATE DOCUMENT TYPE C");
      database.transaction(() -> {
        database.newDocument("C").set("v", new ArrayList<>(List.of(1, 2))).save();
        database.newDocument("C").set("v", new ArrayList<>(List.of(1.0, 2L))).save();
        database.newDocument("C").set("v", new ArrayList<>(List.of(2, 1))).save();
      });
      try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT v) AS n FROM C")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(2L);
      }
    }

    @Test
    void distinctAndPlainCallsDoNotCollide() {
      load();
      final var statement = ((DatabaseInternal) database).getStatementCache().get("SELECT count(DISTINCT b) AS n, count(b) AS m FROM P");
      assertThat(statement.toString()).contains("count(DISTINCT b)").contains("count(b)");
      assertThat(statement.copy().toString()).isEqualTo(statement.toString());
      assertThat(((DatabaseInternal) database).getStatementCache().get("SELECT count(DISTINCT b) FROM P"))
          .isNotEqualTo(((DatabaseInternal) database).getStatementCache().get("SELECT count(b) FROM P"));
    }

    @Test
    void plainCountStillWorks() {
      load();
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM P")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(4L);
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM (SELECT DISTINCT b FROM P)")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(3L);
      }
    }
  }

  /**
   * Regression for #9802.
   * <p>
   * Issue #9802: {@code WHERE @rid = {"@rid": <expr>}} is planned with the statement's own context, so a string or a
   * parameter inside the expression form resolves exactly like the same value written as a literal or inside
   * {@code IN [...]}, instead of failing at plan time.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RidExpressionPlanTime extends TestHelper {
    private RID first;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("D1");
        first = database.newDocument("D1").set("n", 1).save().getIdentity();
        database.newDocument("D1").set("n", 2).save();
      });
    }

    @Test
    void everySpellingOfTheSameRidSelectsTheSameRecord() {
      assertSelectsFirst("SELECT FROM D1 WHERE @rid = " + first);
      assertSelectsFirst("SELECT FROM D1 WHERE @rid = ?", first.toString());
      assertSelectsFirst("SELECT FROM D1 WHERE @rid IN [{\"@rid\": \"" + first + "\"}]");
      assertSelectsFirst("SELECT FROM D1 WHERE @rid = {\"@rid\": \"" + first + "\"}");
      assertSelectsFirst("SELECT FROM D1 WHERE @rid = {\"@rid\": ?}", first.toString());
    }

    @Test
    void aParameterInsideTheExpressionFormIsReResolvedOnEveryExecution() {
      // The plan is cached by statement text: a second execution with another binding must not reuse the first RID
      assertSelectsFirst("SELECT FROM D1 WHERE @rid = {\"@rid\": ?}", first.toString());
      try (final ResultSet rs = database.query("sql", "SELECT FROM D1 WHERE @rid = {\"@rid\": ?}",
          new RID(first.getBucketId(), first.getPosition() + 1).toString())) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((Integer) rs.next().getProperty("n")).isEqualTo(2);
      }
    }

    @Test
    void aPositionBeyondIntRangeResolves() {
      try (final ResultSet rs = database.query("sql",
          "SELECT FROM D1 WHERE @rid = {\"@rid\": \"#" + first.getBucketId() + ":3000000000\"}")) {
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("sql", "SELECT {\"@rid\": \"#1:3000000000\"} AS r")) {
        assertThat(rs.next().<Object>getProperty("r").toString()).isEqualTo("#1:3000000000");
      }
    }

    @Test
    void aRidTargetOnAnUnknownBucketIsNoMatchUnderABucketFilter() {
      final String bucketName = database.getSchema().getBucketById(first.getBucketId()).getName();
      try (final ResultSet rs = database.query("sql",
          "SELECT FROM bucket:" + bucketName + " WHERE @rid = #" + first.getBucketId() + ":" + first.getPosition())) {
        assertThat(rs.hasNext()).isTrue();
      }
      // A RID target in a bucket the schema does not know: no row, instead of a NullPointerException
      try (final ResultSet rs = database.query("sql", "SELECT FROM [#9999:0] WHERE @rid IS NOT NULL")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void deleteAndUpdateResolveTheExpressionForm() {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "UPDATE D1 SET touched = true WHERE @rid = {\"@rid\": ?}",
            first.toString())) {
          assertThat((Long) rs.next().getProperty("count")).isEqualTo(1L);
        }
      });
      database.transaction(() -> assertThat(first.asDocument().getBoolean("touched")).isTrue());

      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "DELETE FROM D1 WHERE @rid = {\"@rid\": \"" + first + "\"}")) {
          assertThat((Long) rs.next().getProperty("count")).isEqualTo(1L);
        }
      });
      database.transaction(() -> assertThat(database.countType("D1", false)).isEqualTo(1L));
    }

    private void assertSelectsFirst(final String sql, final Object... args) {
      try (final ResultSet rs = database.query("sql", sql, args)) {
        assertThat(rs.hasNext()).as(sql).isTrue();
        assertThat(rs.next().getIdentity().get()).as(sql).isEqualTo(first);
        assertThat(rs.hasNext()).as(sql).isFalse();
      }
    }
  }

  /**
   * Regression for #8633.
   * <p>
   * Issue #8633: a LET between two BEGIN/COMMIT blocks of a SQL script made the first block's CREATE EDGE run twice.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SqlScriptLetBetweenTransactions extends TestHelper {
    @Test
    void letBetweenTwoTransactionsDoesNotRepeatTheFirstCreateEdgeOnUnidirectionalType() {
      assertEachBlockRunsOnce(false);
    }

    @Test
    void letBetweenTwoTransactionsDoesNotRepeatTheFirstCreateEdgeOnBidirectionalType() {
      assertEachBlockRunsOnce(true);
    }

    @Test
    void everyBlockOfAScriptRunsExactlyOnce() {
      database.getSchema().createDocumentType("Log");
      database.command("sqlscript", """
          BEGIN;
          INSERT INTO Log SET n = 1;
          COMMIT;
          BEGIN;
          INSERT INTO Log SET n = 2;
          COMMIT;
          LET x = SELECT count(*) AS c FROM Log;
          BEGIN;
          INSERT INTO Log SET n = 3;
          COMMIT;
          """).close();
      assertThat(database.countType("Log", false)).isEqualTo(3);
    }

    @Test
    void aRetryBlockAndAPlainBlockEachRunOnceInBothOrders() {
      database.getSchema().createDocumentType("Log");
      database.command("sqlscript", """
          BEGIN;
          INSERT INTO Log SET n = 1;
          COMMIT RETRY 3;
          BEGIN;
          INSERT INTO Log SET n = 2;
          COMMIT;
          BEGIN;
          INSERT INTO Log SET n = 3;
          COMMIT RETRY 3;
          """).close();
      assertThat(database.countType("Log", false)).isEqualTo(3);
    }

    private void assertEachBlockRunsOnce(final boolean bidirectional) {
      database.getSchema().createVertexType("Question");
      database.getSchema().createVertexType("Tag");
      database.getSchema().buildEdgeType().withName("TAGGED_WITH").withBidirectional(bidirectional).create();
      database.transaction(() -> {
        database.newVertex("Tag").set("name", "s").save();
        database.newVertex("Question").set("qid", 1).save();
        database.newVertex("Question").set("qid", 2).save();
      });

      final ResultSet rs = database.command("sqlscript", """
          BEGIN;
          CREATE EDGE TAGGED_WITH FROM (SELECT FROM Question WHERE qid = 1) TO (SELECT FROM Tag WHERE name = 's');
          COMMIT;
          LET a = SELECT out('TAGGED_WITH').size() AS n FROM Question WHERE qid = 1;
          BEGIN;
          CREATE EDGE TAGGED_WITH FROM (SELECT FROM Question WHERE qid = 2) TO (SELECT FROM Tag WHERE name = 's');
          COMMIT;
          LET c = SELECT @out.qid AS n FROM TAGGED_WITH;
          RETURN $c;
          """);
      final List<Integer> qids = new ArrayList<>();
      while (rs.hasNext())
        qids.add(rs.next().<Integer>getProperty("n"));
      rs.close();

      assertThat(qids).containsExactlyInAnyOrder(1, 2);
      assertThat(database.countType("TAGGED_WITH", false)).isEqualTo(2);
    }
  }
}
