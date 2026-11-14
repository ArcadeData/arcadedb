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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.executor.operators.NodeIndexRangeScan;
import com.arcadedb.query.opencypher.optimizer.RangePredicate;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cypher ORDER BY / LIMIT and min/max answered from an index, including prefix and case-insensitive indexes.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherIndexOrderingTest {
  /**
   * Issue #8422: a Cypher query that filters and orders by the same indexed property, under a LIMIT, sorted the whole
   * qualifying range to return its first rows, while the index range scan it reads from already produces them in that
   * order. The plan now leaves the sort out and lets the LIMIT stop the scan, in both directions, and reads a label with
   * no range predicate in index order too, followed by the vertices the index does not hold (a null key sorts last).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8422CypherIndexOrderLimit extends TestHelper {
    private static final int     VERTICES      = 5_000;
    private static final Pattern PROJECTED_ROWS = Pattern.compile("PROJECT RETURN[^\\n]*?(\\d[\\d,]*) rows?");

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Repro");
      database.command("sql", "CREATE PROPERTY Repro.id STRING");
      database.command("sql", "CREATE PROPERTY Repro.num INTEGER");
      database.command("sql", "CREATE INDEX ON Repro (id) UNIQUE");
      database.command("sql", "CREATE INDEX ON Repro (num) NOTUNIQUE");
      database.command("sql", "CREATE EDGE TYPE Link");
      database.transaction(() -> {
        MutableVertex previous = null;
        // Inserted in reverse order, so physical order and key order are opposite
        for (int i = VERTICES - 1; i >= 0; i--) {
          final MutableVertex v = database.newVertex("Repro").set("id", id(i)).set("num", i % 100).set("seq", i).save();
          if (previous != null)
            v.newEdge("Link", previous).save();
          previous = v;
        }
      });
    }

    @Test
    void orderByTheRangePropertyStopsAtTheLimit() {
      final String query = "MATCH (n:Repro) WHERE n.id > \"\" RETURN n.id ORDER BY n.id LIMIT 10";
      assertThat(column(query, Map.of(), "n.id")).containsExactlyElementsOf(ids(0, 10));

      final String profile = profile(query, Map.of());
      assertThat(profile).contains("NodeIndexRangeScan").contains("index order").doesNotContain("+ ORDER BY ");
      assertThat(projectedRows(profile)).as(profile).isLessThan(VERTICES);
    }

    @Test
    void keysetPaginationWalksTheWholeTypeInOrder() {
      final String query = "MATCH (n:Repro) WHERE n.id > $cursor RETURN n.id AS id ORDER BY id LIMIT 37";
      final List<Object> all = new ArrayList<>();
      String cursor = "";
      while (true) {
        final List<Object> page = column(query, Map.of("cursor", cursor), "id");
        if (page.isEmpty())
          break;
        all.addAll(page);
        cursor = (String) page.getLast();
      }
      assertThat(all).containsExactlyElementsOf(ids(0, VERTICES));
      assertThat(profile(query, Map.of("cursor", ""))).doesNotContain("+ ORDER BY ");
    }

    @Test
    void descendingOrderReadsTheIndexBackwards() {
      assertThat(column("MATCH (n:Repro) WHERE n.id < $cursor RETURN n.id AS id ORDER BY n.id DESC LIMIT 4",
          Map.of("cursor", id(100)), "id")).containsExactly(id(99), id(98), id(97), id(96));
      assertThat(column("MATCH (n:Repro) WHERE n.id >= $from RETURN n.id AS id ORDER BY id DESC LIMIT 3",
          Map.of("from", id(10)), "id")).containsExactly(id(VERTICES - 1), id(VERTICES - 2), id(VERTICES - 3));
      assertThat(column("MATCH (n:Repro) WHERE n.id > $from AND n.id <= $to RETURN n.id AS id ORDER BY id DESC SKIP 2 LIMIT 3",
          Map.of("from", id(10), "to", id(20)), "id")).containsExactly(id(18), id(17), id(16));

      final String profile = profile("MATCH (n:Repro) WHERE n.id < $cursor RETURN n.id AS id ORDER BY n.id DESC LIMIT 4",
          Map.of("cursor", id(100)));
      assertThat(profile).contains("index order, descending").doesNotContain("+ ORDER BY ");
    }

    @Test
    void bothBoundsWithSkip() {
      assertThat(column("MATCH (n:Repro) WHERE n.id >= $from AND n.id < $to RETURN n.id AS id ORDER BY id SKIP 5 LIMIT 5",
          Map.of("from", id(1000), "to", id(2000)), "id")).containsExactlyElementsOf(ids(1005, 1010));
    }

    @Test
    void aResidualFilterIsAppliedWhileTheScanStreams() {
      final String query = "MATCH (n:Repro) WHERE n.id > $cursor AND n.seq % 2 = 1 RETURN n.seq AS seq ORDER BY n.id LIMIT 5";
      assertThat(column(query, Map.of("cursor", id(10)), "seq")).containsExactly(11, 13, 15, 17, 19);
      assertThat(profile(query, Map.of("cursor", id(10)))).doesNotContain("+ ORDER BY ");
    }

    @Test
    void anExpansionKeepsTheAnchorOrder() {
      final String query =
          "MATCH (n:Repro)-[:Link]->(m:Repro) WHERE n.id > $cursor RETURN n.id AS a, m.id AS b ORDER BY n.id LIMIT 3";
      final List<Map<String, Object>> rows = rows(query, Map.of("cursor", id(10)));
      assertThat(rows).extracting(r -> r.get("a")).containsExactly(id(11), id(12), id(13));
      assertThat(rows).extracting(r -> r.get("b")).containsExactly(id(12), id(13), id(14));
      assertThat(profile(query, Map.of("cursor", id(10)))).doesNotContain("+ ORDER BY ");
    }

    @Test
    void distinctKeepsTheIndexOrder() {
      final String query = "MATCH (n:Repro) WHERE n.num >= 90 RETURN DISTINCT n.num AS num ORDER BY num LIMIT 4";
      assertThat(column(query, Map.of(), "num")).containsExactly(90, 91, 92, 93);
      assertThat(profile(query, Map.of())).doesNotContain("+ ORDER BY ");
    }

    @Test
    void noRangePredicateReadsTheIndexAndThenTheNullKeys() {
      database.transaction(() -> {
        database.newVertex("Repro").set("seq", -1).save();
        database.newVertex("Repro").set("seq", -2).save();
      });

      final String query = "MATCH (n:Repro) RETURN n.id AS id ORDER BY n.id LIMIT 10";
      assertThat(column(query, Map.of(), "id")).containsExactlyElementsOf(ids(0, 10));
      final String profile = profile(query, Map.of());
      assertThat(profile).contains("NodeIndexRangeScan").contains("index order").doesNotContain("+ ORDER BY ");
      assertThat(projectedRows(profile)).as(profile).isLessThan(VERTICES);

      // The vertices without a key come last, as a null sorts last in Cypher
      final List<Object> tail = column("MATCH (n:Repro) RETURN n.id AS id ORDER BY n.id SKIP " + (VERTICES - 2) + " LIMIT 10",
          Map.of(), "id");
      final List<Object> expected = new ArrayList<>(ids(VERTICES - 2, VERTICES));
      expected.add(null);
      expected.add(null);
      assertThat(tail).containsExactlyElementsOf(expected);
    }

    @Test
    void aSubTypeIsOrderedWithItsParent() {
      database.command("sql", "CREATE VERTEX TYPE ReproChild EXTENDS Repro");
      database.transaction(() -> {
        database.newVertex("ReproChild").set("id", id(3) + "x").set("seq", -3).save();
        database.newVertex("ReproChild").set("seq", -4).save();
      });
      final List<Object> expected = List.of(id(0), id(1), id(2), id(3), id(3) + "x", id(4));

      assertThat(column("MATCH (n:Repro) WHERE n.id >= $from RETURN n.id AS id ORDER BY n.id LIMIT 6", Map.of("from", id(0)),
          "id")).containsExactlyElementsOf(expected);
      assertThat(column("MATCH (n:Repro) RETURN n.id AS id ORDER BY n.id LIMIT 6", Map.of(), "id"))
          .containsExactlyElementsOf(expected);
      final List<Object> tail = column("MATCH (n:Repro) RETURN n.id AS id ORDER BY n.id SKIP " + (VERTICES + 1) + " LIMIT 5", Map.of(),
          "id");
      assertThat(tail).hasSize(1).containsOnlyNulls();
    }

    @Test
    void shapesTheIndexOrderDoesNotAnswerKeepTheSort() {
      // No LIMIT: the adaptive range scan of #8333 serves the rows, and the sort stays
      assertThat(profile("MATCH (n:Repro) WHERE n.id > \"\" RETURN n.id ORDER BY n.id", Map.of())).contains("+ ORDER BY ");
      // A second sort key the single-property index does not hold
      final String twoKeys = "MATCH (n:Repro) WHERE n.num >= 98 RETURN n.num AS num, n.seq AS seq ORDER BY num, seq DESC LIMIT 3";
      assertThat(profile(twoKeys, Map.of())).contains("+ ORDER BY ");
      final List<Map<String, Object>> rows = rows(twoKeys, Map.of());
      assertThat(rows).extracting(r -> r.get("num")).containsExactly(98, 98, 98);
      assertThat(rows).extracting(r -> r.get("seq")).containsExactly(4998, 4898, 4798);
      // Ordered by a property other than the range one
      assertThat(profile("MATCH (n:Repro) WHERE n.id > \"\" RETURN n.seq ORDER BY n.seq LIMIT 3", Map.of())).contains("+ ORDER BY ");
      // A WITH may hand the rows on in another shape
      assertThat(profile("MATCH (n:Repro) WHERE n.id > \"\" WITH n RETURN n.id ORDER BY n.id LIMIT 3", Map.of()))
          .contains("+ ORDER BY ");
      // A SET may rewrite the key the rows would be ordered by
      database.transaction(() -> assertThat(
          column("MATCH (n:Repro) WHERE n.id < $cursor SET n.id = 'z' + toString(10 - n.seq) RETURN n.id AS id ORDER BY n.id LIMIT 2",
              Map.of("cursor", id(3)), "id")).containsExactly("z10", "z8"));
      // A RETURN alias that shadows the pattern variable is not the vertex any more
      assertThat(profile("MATCH (n:Repro) WHERE n.id > \"\" RETURN n.seq AS n ORDER BY n LIMIT 3", Map.of())).contains("+ ORDER BY ");
    }

    @Test
    void stringsSortByCodePointLikeTheIndex() {
      database.command("sql", "CREATE VERTEX TYPE Text");
      database.command("sql", "CREATE PROPERTY Text.s STRING");
      database.command("sql", "CREATE INDEX ON Text (s) UNIQUE");
      final String bmpPrivateUse = "";
      final String emoji = "😀";
      database.transaction(() -> {
        database.newVertex("Text").set("s", emoji).set("copy", emoji).save();
        database.newVertex("Text").set("s", bmpPrivateUse).set("copy", bmpPrivateUse).save();
        database.newVertex("Text").set("s", "a").set("copy", "a").save();
      });

      // Index order (sort elided) and an explicit sort on a non-indexed copy agree: U+E000 sorts below U+1F600
      assertThat(column("MATCH (t:Text) WHERE t.s > '' RETURN t.s AS s ORDER BY t.s LIMIT 10", Map.of(), "s"))
          .containsExactly("a", bmpPrivateUse, emoji);
      assertThat(column("MATCH (t:Text) RETURN t.copy AS s ORDER BY t.copy", Map.of(), "s"))
          .containsExactly("a", bmpPrivateUse, emoji);
      // The filter compares the way the index range does
      assertThat(column("MATCH (t:Text) WHERE t.copy > $b RETURN t.copy AS s", Map.of("b", bmpPrivateUse), "s"))
          .containsExactly(emoji);
      assertThat(column("MATCH (t:Text) WHERE t.s > $b RETURN t.s AS s", Map.of("b", bmpPrivateUse), "s"))
          .containsExactly(emoji);
      assertThat(column("RETURN '" + emoji + "' > '" + bmpPrivateUse + "' AS gt", Map.of(), "gt")).containsExactly(true);
      assertThat(column("UNWIND ['" + emoji + "', '" + bmpPrivateUse + "'] AS s RETURN max(s) AS m", Map.of(), "m"))
          .containsExactly(emoji);
    }

    private static String id(final int i) {
      return "d-%07d".formatted(i);
    }

    private static List<Object> ids(final int from, final int to) {
      final List<Object> ids = new ArrayList<>();
      for (int i = from; i < to; i++)
        ids.add(id(i));
      return ids;
    }

    private List<Object> column(final String query, final Map<String, Object> params, final String name) {
      final List<Object> values = new ArrayList<>();
      for (final Map<String, Object> row : rows(query, params))
        values.add(row.get(name));
      return values;
    }

    private List<Map<String, Object>> rows(final String query, final Map<String, Object> params) {
      final List<Map<String, Object>> rows = new ArrayList<>();
      try (final ResultSet rs = database.command("opencypher", query, params)) {
        while (rs.hasNext())
          rows.add(new HashMap<>(rs.next().toMap()));
      }
      return rows;
    }

    private String profile(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    /** The rows the RETURN projection saw, from a PROFILE: all of the range when the plan sorts, a batch when it streams. */
    private static long projectedRows(final String profile) {
      final Matcher matcher = PROJECTED_ROWS.matcher(profile);
      assertThat(matcher.find()).as(profile).isTrue();
      return Long.parseLong(matcher.group(1).replace(",", ""));
    }
  }

  /**
   * Issue #8666: {@code STARTS WITH} and {@code min()} / {@code max()} over an indexed property scanned the whole label
   * while the index answers both. {@code STARTS WITH} now bounds an index range scan by the prefix (the predicate stays
   * as a filter, so the answer never depends on the index's collation), and a bare {@code min} / {@code max} reads one end
   * of the index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8666CypherPrefixAndMinMaxIndex extends TestHelper {
    private static final int VERTICES = 3_000;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE D");
      database.command("sql", "CREATE PROPERTY D.x INTEGER");
      database.command("sql", "CREATE PROPERTY D.s STRING");
      database.command("sql", "CREATE PROPERTY D.u STRING");
      database.command("sql", "CREATE INDEX ON D (x) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON D (s) NOTUNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++) {
          // every 50th vertex holds neither key: a null is in neither the index nor the min / max
          if (i % 50 == 0)
            database.newVertex("D").set("u", "v" + i).save();
          else
            database.newVertex("D").set("x", 100 + (i * 7) % 1000).set("s", String.format("%04x", i)).set("u", "v" + i).save();
        }
        database.newVertex("D").set("s", "ab\\c%").set("x", -5).save();
        database.newVertex("D").set("s", "ab\uD7FF").save();
        database.newVertex("D").set("s", "ab\uD7FFz").save();
        database.newVertex("D").set("s", "\uFFFF").save();
        database.newVertex("D").set("s", "\uFFFFq").save();
        database.newVertex("D").set("s", "\uD83D\uDE00").save();
      });
    }

    private long expectedPrefixCount(final String prefix) {
      long count = 0;
      for (int i = 0; i < VERTICES; i++)
        if (i % 50 != 0 && String.format("%04x", i).startsWith(prefix))
          count++;
      for (final String extra : new String[] { "ab\\c%", "ab\uD7FF", "ab\uD7FFz", "\uFFFF", "\uFFFFq", "\uD83D\uDE00" })
        if (extra.startsWith(prefix))
          count++;
      return count;
    }

    @Test
    void startsWithUsesTheIndexRange() {
      final String query = "MATCH (d:D) WHERE d.s STARTS WITH 'a' RETURN count(*) AS n";
      assertThat(scalar(query, Map.of(), "n")).isEqualTo(expectedPrefixCount("a"));
      assertThat(profile(query, Map.of())).contains("NodeIndexRangeScan").doesNotContain("NodeByLabelScan");
    }

    @Test
    void startsWithAParameterUsesTheIndexRange() {
      final String query = "MATCH (d:D) WHERE d.s STARTS WITH $p RETURN count(*) AS n";
      for (final String prefix : new String[] { "0", "0a", "0a1", "ff", "zzz", "ab\\", "ab\\c%", "ab", "ab\uD7FF", "\uFFFF", "\uFFFFq" }) {
        assertThat(scalar(query, Map.of("p", prefix), "n")).as("prefix '%s'", prefix).isEqualTo(expectedPrefixCount(prefix));
      }
      assertThat(profile(query, Map.of("p", "0a"))).contains("NodeIndexRangeScan");
    }

    @Test
    void startsWithOnTheEndOfTheKeySpace() {
      // No key sorts after a run of U+FFFF: the range has no upper end
      final String query = "MATCH (d:D) WHERE d.s STARTS WITH $p RETURN count(*) AS n";
      assertThat(scalar(query, Map.of("p", "\uFFFF"), "n")).isEqualTo(2L);
      assertThat(scalar(query, Map.of("p", "\uFFFFq"), "n")).isEqualTo(1L);
      assertThat(scalar(query, Map.of("p", "ab\uD7FF"), "n")).isEqualTo(2L);
    }

    @Test
    void startsWithAnEmptyOrNullPrefix() {
      assertThat(scalar("MATCH (d:D) WHERE d.s STARTS WITH '' RETURN count(*) AS n", Map.of(), "n"))
          .isEqualTo(expectedPrefixCount(""));
      final Map<String, Object> nullParam = new HashMap<>();
      nullParam.put("p", null);
      assertThat(scalar("MATCH (d:D) WHERE d.s STARTS WITH $p RETURN count(*) AS n", nullParam, "n")).isEqualTo(0L);
    }

    @Test
    void startsWithKeepsTheOtherPredicates() {
      assertThat(scalar("MATCH (d:D) WHERE d.s STARTS WITH '0a' AND d.x > 500 RETURN count(*) AS n", Map.of(), "n"))
          .isEqualTo(countWhere("0a", 500));
      assertThat(scalar("MATCH (d:D) WHERE d.s STARTS WITH '0a' AND d.u ENDS WITH '3' RETURN count(*) AS n", Map.of(), "n"))
          .isEqualTo(countEndingWith("0a", '3'));
    }

    @Test
    void startsWithOnANonStringIndexFallsBackToTheFilter() {
      // A string prefix against an INTEGER key: no row qualifies, and the index must not be asked to compare them
      assertThat(scalar("MATCH (d:D) WHERE d.x STARTS WITH '1' RETURN count(*) AS n", Map.of(), "n")).isEqualTo(0L);
    }

    @Test
    void startsWithOnAnUnindexedPropertyStillWorks() {
      assertThat(scalar("MATCH (d:D) WHERE d.u STARTS WITH 'v12' RETURN count(*) AS n", Map.of(), "n")).isEqualTo(countU("v12"));
    }

    @Test
    void minAndMaxAreReadFromTheIndex() {
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (int i = 0; i < VERTICES; i++)
        if (i % 50 != 0) {
          min = Math.min(min, 100 + (i * 7) % 1000);
          max = Math.max(max, 100 + (i * 7) % 1000);
        }
      min = Math.min(min, -5);

      assertThat(scalar("MATCH (d:D) RETURN min(d.x) AS v", Map.of(), "v")).isEqualTo((int) min);
      assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo((int) max);
      assertThat(scalar("MATCH (d:D) RETURN max(d.x)", Map.of(), "max(d.x)")).isEqualTo((int) max);
      assertThat(profile("MATCH (d:D) RETURN min(d.x) AS v", Map.of())).contains("MIN FROM INDEX");
      assertThat(profile("MATCH (d:D) RETURN max(d.x) AS v", Map.of())).contains("MAX FROM INDEX");
    }

    @Test
    void minAndMaxOfAStringKey() {
      assertThat(scalar("MATCH (d:D) RETURN min(d.s) AS v", Map.of(), "v")).isEqualTo("0001");
      // U+FFFF is above every BMP character in UTF-16 and the 4-byte emoji above it in UTF-8, where the index orders the keys
      assertThat(scalar("MATCH (d:D) RETURN max(d.s) AS v", Map.of(), "v")).isEqualTo("\uD83D\uDE00");
    }

    @Test
    void minAndMaxSkipAndLimit() {
      assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v LIMIT 1", Map.of(), "v")).isNotNull();
      try (final ResultSet rs = database.query("opencypher", "MATCH (d:D) RETURN max(d.x) AS v SKIP 1")) {
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (d:D) RETURN max(d.x) AS v LIMIT 0")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void minAndMaxOfAnEmptyLabelAreNull() {
      database.command("sql", "CREATE VERTEX TYPE Empty");
      database.command("sql", "CREATE PROPERTY Empty.x INTEGER");
      database.command("sql", "CREATE INDEX ON Empty (x) NOTUNIQUE");
      try (final ResultSet rs = database.query("opencypher", "MATCH (e:Empty) RETURN min(e.x) AS lo, max(e.x) AS hi")) {
        assertThat(rs.hasNext()).isTrue();
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (e:Empty) RETURN min(e.x) AS v")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((Object) rs.next().getProperty("v")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
      database.transaction(() -> database.newVertex("Empty").save());
      assertThat(scalar("MATCH (e:Empty) RETURN max(e.x) AS v", Map.of(), "v")).isNull();
    }

    @Test
    void minAndMaxSeeTheOpenTransaction() {
      database.begin();
      try {
        database.newVertex("D").set("x", 99_999).save();
        database.newVertex("D").set("x", -99_999).save();
        assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo(99_999);
        assertThat(scalar("MATCH (d:D) RETURN min(d.x) AS v", Map.of(), "v")).isEqualTo(-99_999);
      } finally {
        database.rollback();
      }
      assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isNotEqualTo(99_999);
    }

    @Test
    void minAndMaxWithAnotherFilterOrAnUnindexedKeyKeepTheScan() {
      assertThat(scalar("MATCH (d:D) WHERE d.x > 200 RETURN min(d.x) AS v", Map.of(), "v")).isEqualTo(201);
      // a range of the same property is answered from the index too, since #8812; anything else in the WHERE keeps the scan
      assertThat(scalar("MATCH (d:D) WHERE d.x > 200 AND d.u <> 'zz' RETURN min(d.x) AS v", Map.of(), "v")).isEqualTo(201);
      assertThat(profile("MATCH (d:D) WHERE d.x > 200 AND d.u <> 'zz' RETURN min(d.x) AS v", Map.of())).doesNotContain("MIN FROM INDEX");
      assertThat(profile("MATCH (d:D) WHERE d.x = 200 RETURN min(d.x) AS v", Map.of())).doesNotContain("MIN FROM INDEX");
      assertThat(profile("MATCH (d:D) RETURN min(d.u) AS v", Map.of())).doesNotContain("MIN FROM INDEX");
      assertThat(scalar("MATCH (d:D) RETURN min(d.u) AS v", Map.of(), "v")).isEqualTo("v0");
      assertThat(profile("MATCH (d:D) RETURN min(d.x) AS lo, count(*) AS n", Map.of())).doesNotContain("MIN FROM INDEX");
      assertThat(profile("MATCH (d:D) RETURN DISTINCT min(d.x) AS v", Map.of())).doesNotContain("MIN FROM INDEX");
    }

    @Test
    void minAndMaxWithNullsIndexedKeepTheScan() {
      database.command("sql", "CREATE VERTEX TYPE N");
      database.command("sql", "CREATE PROPERTY N.x INTEGER");
      database.command("sql", "CREATE INDEX ON N (x) NOTUNIQUE NULL_STRATEGY INDEX");
      database.transaction(() -> {
        database.newVertex("N").set("x", 5).save();
        database.newVertex("N").save();
      });
      assertThat(scalar("MATCH (n:N) RETURN min(n.x) AS v", Map.of(), "v")).isEqualTo(5);
      assertThat(profile("MATCH (n:N) RETURN min(n.x) AS v", Map.of())).doesNotContain("MIN FROM INDEX");
    }

    @Test
    void minAndMaxOverASubTypeHierarchy() {
      database.command("sql", "CREATE VERTEX TYPE DChild EXTENDS D");
      database.transaction(() -> {
        database.newVertex("DChild").set("x", 50_000).save();
        database.newVertex("DChild").set("x", -50_000).save();
      });
      assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo(50_000);
      assertThat(scalar("MATCH (d:D) RETURN min(d.x) AS v", Map.of(), "v")).isEqualTo(-50_000);
      assertThat(scalar("MATCH (d:DChild) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo(50_000);
    }

    @Test
    void startsWithOnACaseInsensitiveIndexMissesNothing() {
      // The keys of a case-insensitive index are lower-cased: 'AZ' bumped to 'A[' and lower-cased is 'a[', which sorts
      // below 'azb', so a range built from it would drop a row the predicate accepts
      database.command("sql", "CREATE VERTEX TYPE Ci");
      database.command("sql", "CREATE PROPERTY Ci.s STRING");
      database.command("sql", "CREATE INDEX ON Ci (s COLLATE ci) NOTUNIQUE");
      database.transaction(() -> {
        for (final String v : new String[] { "AZb", "AZ", "azc", "Az", "AY", "B", "a[" })
          database.newVertex("Ci").set("s", v).save();
      });
      for (final String prefix : new String[] { "AZ", "Az", "az", "A", "a" }) {
        long expected = 0;
        for (final String v : new String[] { "AZb", "AZ", "azc", "Az", "AY", "B", "a[" })
          if (v.startsWith(prefix))
            expected++;
        assertThat(scalar("MATCH (c:Ci) WHERE c.s STARTS WITH $p RETURN count(*) AS n", Map.of("p", prefix), "n"))
            .as("prefix '%s'", prefix).isEqualTo(expected);
      }
      assertThat(scalar("MATCH (c:Ci) WHERE c.s STARTS WITH 'AZ' RETURN count(*) AS n", Map.of(), "n")).isEqualTo(2L);
      assertThat(profile("MATCH (c:Ci) WHERE c.s STARTS WITH 'AZ' RETURN count(*) AS n", Map.of())).doesNotContain("next(");
    }

    @Test
    void startsWithANonStringParameter() {
      for (final Object param : new Object[] { 5, 5.5, true, List.of("0a"), Map.of("a", 1) })
        assertThat(scalar("MATCH (d:D) WHERE d.s STARTS WITH $p RETURN count(*) AS n", Map.of("p", param), "n")).as(String.valueOf(param))
            .isEqualTo(0L);
      assertThat(scalar("MATCH (d:D) WHERE d.x STARTS WITH $p RETURN count(*) AS n", Map.of("p", 5), "n")).isEqualTo(0L);
    }

    @Test
    void minAndMaxSeeADeleteInTheOpenTransaction() {
      final int max = (Integer) scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v");
      database.begin();
      try {
        database.command("sql", "DELETE FROM D WHERE x = " + max);
        final Object afterDelete = scalar("MATCH (d:D) WHERE d.x IS NOT NULL RETURN max(d.x) AS v", Map.of(), "v");
        assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo(afterDelete);
        assertThat(afterDelete).isNotEqualTo(max);
      } finally {
        database.rollback();
      }
      assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo(max);
    }

    private long countWhere(final String prefix, final int above) {
      long count = 0;
      for (int i = 0; i < VERTICES; i++)
        if (i % 50 != 0 && String.format("%04x", i).startsWith(prefix) && 100 + (i * 7) % 1000 > above)
          count++;
      return count;
    }

    private long countEndingWith(final String prefix, final char last) {
      long count = 0;
      for (int i = 0; i < VERTICES; i++)
        if (i % 50 != 0 && String.format("%04x", i).startsWith(prefix) && ("v" + i).endsWith(String.valueOf(last)))
          count++;
      return count;
    }

    private long countU(final String prefix) {
      long count = 0;
      for (int i = 0; i < VERTICES; i++)
        if (("v" + i).startsWith(prefix))
          count++;
      return count;
    }

    private Object scalar(final String query, final Map<String, Object> params, final String column) {
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        assertThat(rs.hasNext()).isTrue();
        return rs.next().getProperty(column);
      }
    }

    private String profile(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }
  }

  /**
   * Issue #8724: {@code ORDER BY v.x LIMIT k} over a whole label read the index in order for one case only (ascending, no
   * WHERE, default null strategy) and scanned and sorted the label in the cases next to it. The index order now answers:
   * both directions when no vertex can lack a key ({@code MANDATORY} and {@code NOTNULL}) or the WHERE is
   * {@code v.x IS NOT NULL}, and both directions of an index with {@code NULL_STRATEGY INDEX}, whose null keys are read
   * from the end of the index openCypher puts them at (last ascending, first descending).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8724CypherIndexOrderWholeLabel extends TestHelper {
    private static final int VERTICES   = 3_000;
    private static final int NULL_EVERY = 50;

    private final List<Integer> mandatoryValues = new ArrayList<>();
    private final List<Integer> nullableValues  = new ArrayList<>();

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE M");
      database.command("sql", "CREATE PROPERTY M.x INTEGER (mandatory true, notnull true)");
      database.command("sql", "CREATE INDEX ON M (x) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE D");
      database.command("sql", "CREATE PROPERTY D.x INTEGER");
      database.command("sql", "CREATE INDEX ON D (x) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE I");
      database.command("sql", "CREATE PROPERTY I.x INTEGER");
      database.command("sql", "CREATE INDEX ON I (x) NOTUNIQUE NULL_STRATEGY INDEX");

      final Random random = new Random(7);
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++) {
          final int value = random.nextInt(1_000_000);
          database.newVertex("M").set("x", value).set("seq", i).save();
          mandatoryValues.add(value);
          final boolean isNull = i % NULL_EVERY == 0;
          nullableValues.add(isNull ? null : value);
          for (final String type : new String[] { "D", "I" }) {
            final var vertex = database.newVertex(type).set("seq", i);
            if (!isNull)
              vertex.set("x", value);
            vertex.save();
          }
        }
      });
    }

    @Test
    void mandatoryNotNullReadsTheIndexInBothDirections() {
      for (final boolean ascending : new boolean[] { true, false }) {
        final String query = "MATCH (m:M) RETURN m.x AS x ORDER BY m.x" + (ascending ? "" : " DESC") + " LIMIT 10";
        assertThat(column(query)).containsExactlyElementsOf(expected(mandatoryValues, ascending, 0, 10));
        final String profile = profile(query);
        assertThat(profile).contains("NodeIndexRangeScan").contains("index order").doesNotContain("+ ORDER BY ");
        assertThat(profile.contains("descending")).isEqualTo(!ascending);
      }
    }

    @Test
    void skipWalksTheWholeIndex() {
      assertThat(column("MATCH (m:M) RETURN m.x AS x ORDER BY m.x DESC SKIP 2990 LIMIT 20"))
          .containsExactlyElementsOf(expected(mandatoryValues, false, 2990, 20));
    }

    @Test
    void isNotNullReadsTheIndexInBothDirections() {
      for (final String type : new String[] { "D", "I" })
        for (final boolean ascending : new boolean[] { true, false }) {
          final String query = "MATCH (v:" + type + ") WHERE v.x IS NOT NULL RETURN v.x AS x ORDER BY v.x" + (ascending ? "" : " DESC")
              + " LIMIT 10";
          assertThat(column(query)).as(query).containsExactlyElementsOf(expected(nonNull(nullableValues), ascending, 0, 10));
          assertThat(profile(query)).as(query).contains("NodeIndexRangeScan").contains("index order").doesNotContain("+ ORDER BY ");
        }
    }

    @Test
    void isNotNullWalksToTheEndOfTheIndex() {
      final List<Integer> nonNull = nonNull(nullableValues);
      for (final String type : new String[] { "D", "I" }) {
        final String query = "MATCH (v:" + type + ") WHERE v.x IS NOT NULL RETURN v.x AS x ORDER BY v.x DESC SKIP " + (nonNull.size() - 5)
            + " LIMIT 50";
        assertThat(column(query)).as(query).containsExactlyElementsOf(expected(nonNull, false, nonNull.size() - 5, 50));
      }
    }

    @Test
    void nullStrategyIndexPlacesTheNullKeysAtTheEndOpenCypherPutsThem() {
      final int nulls = (int) nullableValues.stream().filter(v -> v == null).count();

      // Ascending: the values, then the nulls
      final String ascending = "MATCH (i:I) RETURN i.x AS x ORDER BY i.x LIMIT 10";
      assertThat(column(ascending)).containsExactlyElementsOf(expected(nullableValues, true, 0, 10));
      assertThat(profile(ascending)).contains("NodeIndexRangeScan").contains("index order").doesNotContain("+ ORDER BY ");
      assertThat(column("MATCH (i:I) RETURN i.x AS x ORDER BY i.x SKIP " + (VERTICES - nulls - 5) + " LIMIT 20"))
          .containsExactlyElementsOf(expected(nullableValues, true, VERTICES - nulls - 5, 20));

      // Descending: the nulls, then the values from the top
      final String descending = "MATCH (i:I) RETURN i.x AS x ORDER BY i.x DESC LIMIT 10";
      assertThat(column(descending)).containsExactlyElementsOf(expected(nullableValues, false, 0, 10));
      assertThat(profile(descending)).contains("NodeIndexRangeScan").contains("index order, descending").doesNotContain("+ ORDER BY ");
      assertThat(column("MATCH (i:I) RETURN i.x AS x ORDER BY i.x DESC SKIP " + (nulls - 3) + " LIMIT 20"))
          .containsExactlyElementsOf(expected(nullableValues, false, nulls - 3, 20));

      // The whole index, both directions
      assertThat(column("MATCH (i:I) RETURN i.x AS x ORDER BY i.x LIMIT " + VERTICES))
          .containsExactlyElementsOf(expected(nullableValues, true, 0, VERTICES));
      assertThat(column("MATCH (i:I) RETURN i.x AS x ORDER BY i.x DESC LIMIT " + VERTICES))
          .containsExactlyElementsOf(expected(nullableValues, false, 0, VERTICES));
    }

    @Test
    void aNullableDefaultIndexKeepsTheSortDescending() {
      // The index holds no null keys and openCypher puts the nulls first descending: the answer has to come from a scan
      final String query = "MATCH (d:D) RETURN d.x AS x ORDER BY d.x DESC LIMIT 10";
      assertThat(column(query)).containsExactlyElementsOf(expected(nullableValues, false, 0, 10));
      assertThat(profile(query)).contains("+ ORDER BY ");
      // and ascending still reads the index, then the vertices with no key
      assertThat(column("MATCH (d:D) RETURN d.x AS x ORDER BY d.x LIMIT " + VERTICES))
          .containsExactlyElementsOf(expected(nullableValues, true, 0, VERTICES));
    }

    @Test
    void anIsNotNullOnASecondMatchStillAnswersCorrectly() {
      final List<Integer> nonNull = nonNull(nullableValues);
      assertThat(column("MATCH (a:D) WHERE a.seq = 1 MATCH (v:I) WHERE v.x IS NOT NULL RETURN v.x AS x ORDER BY v.x DESC LIMIT 5"))
          .containsExactlyElementsOf(expected(nonNull, false, 0, 5));
      assertThat(column("MATCH (v:I) WITH v WHERE v.x IS NOT NULL RETURN v.x AS x ORDER BY v.x LIMIT 5"))
          .containsExactlyElementsOf(expected(nonNull, true, 0, 5));
    }

    @Test
    void aSubTypeIsSeekedThroughItsParentsIndex() {
      database.command("sql", "CREATE VERTEX TYPE DChild EXTENDS D");
      database.transaction(() -> database.newVertex("DChild").set("x", -1).set("seq", -1).save());
      assertThat(column("MATCH (d:D) WHERE d.x IS NOT NULL RETURN d.x AS x ORDER BY d.x LIMIT 1")).containsExactly(-1);
      assertThat(column("MATCH (d:D) WHERE d.x < 0 OR d.x = 999999999 RETURN d.x AS x")).containsExactly(-1);
    }

    @Test
    void nullKeysAreSeparatedFromZeroAndNegativeValues() {
      database.command("sql", "CREATE VERTEX TYPE Z");
      database.command("sql", "CREATE PROPERTY Z.x INTEGER");
      database.command("sql", "CREATE INDEX ON Z (x) NOTUNIQUE NULL_STRATEGY INDEX");
      final List<Integer> values = new ArrayList<>();
      for (final Integer value : new Integer[] { 0, null, -5, 7, null, 0, Integer.MIN_VALUE, Integer.MAX_VALUE, null }) {
        values.add(value);
        database.transaction(() -> {
          final var vertex = database.newVertex("Z");
          if (value != null)
            vertex.set("x", value);
          vertex.save();
        });
      }
      assertThat(column("MATCH (z:Z) RETURN z.x AS x ORDER BY z.x LIMIT 20")).containsExactlyElementsOf(expected(values, true, 0, 20));
      assertThat(column("MATCH (z:Z) RETURN z.x AS x ORDER BY z.x DESC LIMIT 20")).containsExactlyElementsOf(expected(values, false, 0, 20));
      assertThat(column("MATCH (z:Z) WHERE z.x IS NOT NULL RETURN z.x AS x ORDER BY z.x DESC LIMIT 20"))
          .containsExactlyElementsOf(expected(nonNull(values), false, 0, 20));
      assertThat(column("MATCH (z:Z) WHERE z.x IS NOT NULL RETURN z.x AS x ORDER BY z.x LIMIT 20"))
          .containsExactlyElementsOf(expected(nonNull(values), true, 0, 20));
    }

    @Test
    void otherPredicatesKeepTheSort() {
      final String query = "MATCH (v:D) WHERE v.x IS NOT NULL AND v.seq >= 10 RETURN v.x AS x ORDER BY v.x DESC LIMIT 5";
      final List<Integer> values = new ArrayList<>();
      for (int i = 10; i < VERTICES; i++)
        if (nullableValues.get(i) != null)
          values.add(nullableValues.get(i));
      assertThat(column(query)).containsExactlyElementsOf(expected(values, false, 0, 5));
    }

    @Test
    void theNullKeysSpanMoreThanOneFetchBatch() {
      database.command("sql", "CREATE VERTEX TYPE N");
      database.command("sql", "CREATE PROPERTY N.x INTEGER");
      database.command("sql", "CREATE INDEX ON N (x) NOTUNIQUE NULL_STRATEGY INDEX");
      final List<Integer> values = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 1_000; i++) {
          final Integer value = i % 3 == 0 ? null : i;
          values.add(value);
          final var vertex = database.newVertex("N");
          if (value != null)
            vertex.set("x", value);
          vertex.save();
        }
      });
      assertThat(column("MATCH (n:N) RETURN n.x AS x ORDER BY n.x DESC LIMIT 1000")).containsExactlyElementsOf(expected(values, false, 0, 1000));
      assertThat(column("MATCH (n:N) RETURN n.x AS x ORDER BY n.x LIMIT 1000")).containsExactlyElementsOf(expected(values, true, 0, 1000));
      assertThat(column("MATCH (n:N) RETURN n.x AS x ORDER BY n.x DESC SKIP 150 LIMIT 300")).containsExactlyElementsOf(expected(values, false, 150, 300));
    }

    private static List<Integer> nonNull(final List<Integer> values) {
      return values.stream().filter(v -> v != null).toList();
    }

    /** The window of the values sorted the way openCypher sorts them: null last ascending, first descending. */
    private static List<Object> expected(final List<Integer> values, final boolean ascending, final int skip, final int limit) {
      final List<Integer> sorted = new ArrayList<>(values);
      final Comparator<Integer> nullsLast = Comparator.nullsLast(Comparator.<Integer>naturalOrder());
      sorted.sort(ascending ? nullsLast : nullsLast.reversed());
      return new ArrayList<>(sorted.subList(Math.min(skip, sorted.size()), Math.min(skip + limit, sorted.size())));
    }

    private List<Object> column(final String query) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.command("opencypher", query, Map.of())) {
        while (rs.hasNext())
          values.add(rs.next().getProperty("x"));
      }
      return values;
    }

    private String profile(final String query) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, Map.of())) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }
  }

  /**
   * Issue #8766 (#8698, #8699, #8700): a {@code COLLATE ci} index stores its string keys lower-cased, so its order and its
   * key ranges are those of the folded keys, not of the values. SQL {@code min()} / {@code max()}, SQL and Cypher
   * {@code ORDER BY ... LIMIT} and Cypher range predicates must answer exactly what the same statement answers on a type
   * with no index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8766CaseInsensitiveIndexOrdering extends TestHelper {
    private static final String[] VALUES = { "AZb", "AZ", "azc", "Az", "AY", "B", "a[", "abc", "Mzz", "m", "Zoo", "apple", "Banana" };

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Ci");
      database.command("sql", "CREATE PROPERTY Ci.s STRING");
      database.command("sql", "CREATE INDEX ON Ci (s COLLATE ci) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE Nx");
      database.command("sql", "CREATE PROPERTY Nx.s STRING");
      database.command("sql", "CREATE VERTEX TYPE Cc");
      database.command("sql", "CREATE PROPERTY Cc.k INTEGER");
      database.command("sql", "CREATE PROPERTY Cc.s STRING");
      database.command("sql", "CREATE INDEX ON Cc (k, s COLLATE ci) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE Cx");
      database.command("sql", "CREATE PROPERTY Cx.k INTEGER");
      database.command("sql", "CREATE PROPERTY Cx.s STRING");
      database.transaction(() -> {
        int k = 0;
        for (final String value : VALUES) {
          database.newVertex("Ci").set("s", value).save();
          database.newVertex("Nx").set("s", value).save();
          database.newVertex("Cc").set("k", k % 2).set("s", value).save();
          database.newVertex("Cx").set("k", k++ % 2).set("s", value).save();
        }
        for (final String value : new String[] { "\u00c9clair", "\u00e9t\u00e9", "zebra" }) {
          database.newVertex("Ci").set("s", value).save();
          database.newVertex("Nx").set("s", value).save();
        }
        database.newVertex("Ci").save();
        database.newVertex("Nx").save();
      });
    }

    private List<Object> column(final String language, final String query) {
      return column(language, query, Map.of());
    }

    private List<Object> column(final String language, final String query, final Map<String, Object> params) {
      final List<Object> result = new ArrayList<>();
      try (final ResultSet rs = database.query(language, query, params)) {
        while (rs.hasNext())
          result.add(rs.next().getProperty("s"));
      }
      return result;
    }

    private void sameAnswer(final String language, final String template, final boolean ordered) {
      final List<Object> expected = column(language, template.replace("%T", "Nx"));
      final List<Object> actual = column(language, template.replace("%T", "Ci"));
      if (ordered)
        assertThat(actual).as(template).containsExactlyElementsOf(expected);
      else
        assertThat(actual).as(template).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void sqlMaxAndMinReturnARecordValue() {
      sameAnswer("sql", "SELECT max(s) AS s FROM %T", true);
      sameAnswer("sql", "SELECT min(s) AS s FROM %T", true);
    }

    @Test
    void sqlOrderByFollowsTheValues() {
      sameAnswer("sql", "SELECT s FROM %T ORDER BY s", true);
      sameAnswer("sql", "SELECT s FROM %T ORDER BY s LIMIT 3", true);
      sameAnswer("sql", "SELECT s FROM %T ORDER BY s DESC LIMIT 3", true);
    }

    @Test
    void cypherOrderByFollowsTheValues() {
      sameAnswer("opencypher", "MATCH (c:%T) RETURN c.s AS s ORDER BY s LIMIT 3", true);
      sameAnswer("opencypher", "MATCH (c:%T) RETURN c.s AS s ORDER BY s", true);
      sameAnswer("opencypher", "MATCH (c:%T) WHERE c.s >= 'B' RETURN c.s AS s ORDER BY s LIMIT 4", true);
    }

    @Test
    void cypherRangePredicatesFollowTheValues() {
      for (final String predicate : new String[] { "c.s < 'a'", "c.s >= 'M'", "c.s > 'M'", "c.s <= 'Zoo'", "c.s >= 'B' AND c.s < 'b'" })
        sameAnswer("opencypher", "MATCH (c:%T) WHERE " + predicate + " RETURN c.s AS s", false);
    }

    @Test
    void cypherOrderByDescendingFollowsTheValues() {
      sameAnswer("opencypher", "MATCH (c:%T) RETURN c.s AS s ORDER BY s DESC LIMIT 3", true);
    }

    @Test
    void compositeIndexWithAFoldedColumnFollowsTheValues() {
      sameAnswer("sql", "SELECT s FROM %T WHERE k = 1 ORDER BY s", true);
      sameAnswer("sql", "SELECT s FROM %T WHERE k = 0 ORDER BY s LIMIT 3", true);
    }

    @Test
    void aQueryStaysCorrectAfterItsIndexIsRecreatedFolded() {
      database.command("sql", "CREATE VERTEX TYPE Late");
      database.command("sql", "CREATE PROPERTY Late.s STRING");
      database.command("sql", "CREATE INDEX ON Late (s) NOTUNIQUE");
      database.transaction(() -> {
        for (final String value : VALUES)
          database.newVertex("Late").set("s", value).save();
      });
      final String query = "MATCH (c:Late) WHERE c.s < 'a' RETURN c.s AS s";
      final List<Object> before = column("opencypher", query);
      database.command("sql", "DROP INDEX `Late[s]`");
      database.command("sql", "CREATE INDEX ON Late (s COLLATE ci) NOTUNIQUE");
      assertThat(column("opencypher", query)).containsExactlyInAnyOrderElementsOf(before);
    }

    @Test
    void parameterizedRangeFollowsTheValues() {
      final List<Object> expected = column("opencypher", "MATCH (c:Nx) WHERE c.s < $p RETURN c.s AS s", Map.of("p", "a"));
      assertThat(column("opencypher", "MATCH (c:Ci) WHERE c.s < $p RETURN c.s AS s", Map.of("p", "a")))
          .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void cypherMinAndMaxReturnARecordValue() {
      sameAnswer("opencypher", "MATCH (c:%T) RETURN max(c.s) AS s", true);
      sameAnswer("opencypher", "MATCH (c:%T) RETURN min(c.s) AS s", true);
    }

    @Test
    void aRetainedRangeScanRefusesAFoldedIndex() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final NodeIndexRangeScan scan = new NodeIndexRangeScan("c", "Ci", "s",
          List.of(new RangePredicate("s", ComparisonExpression.Operator.LESS_THAN, "a", false)), "Ci[s]", 1.0, 1L);
      assertThatThrownBy(() -> scan.execute(context, -1).hasNext()).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("re-plan");
    }
  }

  /**
   * Regression for issue #8812: {@code MATCH (v:V) WHERE v.a > x RETURN min(v.a)} (and {@code max} with {@code <}) read
   * every vertex of the range; the first row of the index range is the answer. Every answer is compared with the same
   * query written with {@code v.a + 0}, which no index serves.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8812CypherRangeMinMaxIndex extends TestHelper {

    @BeforeEach
    void load() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE PROPERTY V.a LONG");
      database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 2_000; i++)
          database.newVertex("V").set("a", (long) i * 2).save();
        // no value: a range must never return it
        database.newVertex("V").set("b", 1).save();
      });
    }

    @Test
    void minOverLowerBoundAndMaxOverUpperBound() {
      assertAnswer("MATCH (v:V) WHERE v.a > 1001 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 1001 RETURN min(v.a + 0) AS c", Map.of(),
          1002L);
      assertAnswer("MATCH (v:V) WHERE v.a >= 1002 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 >= 1002 RETURN min(v.a + 0) AS c", Map.of(),
          1002L);
      assertAnswer("MATCH (v:V) WHERE v.a < 1001 RETURN max(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 < 1001 RETURN max(v.a + 0) AS c", Map.of(),
          1000L);
      assertAnswer("MATCH (v:V) WHERE v.a <= 1000 RETURN max(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 <= 1000 RETURN max(v.a + 0) AS c", Map.of(),
          1000L);
      assertThat(profile("MATCH (v:V) WHERE v.a > 1001 RETURN min(v.a) AS c", Map.of())).contains("MIN FROM INDEX");
      assertThat(profile("MATCH (v:V) WHERE v.a < 1001 RETURN max(v.a) AS c", Map.of())).contains("MAX FROM INDEX");
    }

    @Test
    void bothBoundsAndTheOppositeEnd() {
      assertAnswer("MATCH (v:V) WHERE v.a > 100 AND v.a < 200 RETURN min(v.a) AS c",
          "MATCH (v:V) WHERE v.a + 0 > 100 AND v.a + 0 < 200 RETURN min(v.a + 0) AS c", Map.of(), 102L);
      assertAnswer("MATCH (v:V) WHERE v.a > 100 AND v.a < 200 RETURN max(v.a) AS c",
          "MATCH (v:V) WHERE v.a + 0 > 100 AND v.a + 0 < 200 RETURN max(v.a + 0) AS c", Map.of(), 198L);
      assertAnswer("MATCH (v:V) WHERE v.a < 1001 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 < 1001 RETURN min(v.a + 0) AS c", Map.of(), 0L);
      assertAnswer("MATCH (v:V) WHERE v.a > 1001 RETURN max(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 1001 RETURN max(v.a + 0) AS c", Map.of(),
          3998L);
    }

    @Test
    void comparisonWrittenTheOtherWayRoundAndParameters() {
      assertAnswer("MATCH (v:V) WHERE 1001 < v.a RETURN min(v.a) AS c", "MATCH (v:V) WHERE 1001 < v.a + 0 RETURN min(v.a + 0) AS c", Map.of(),
          1002L);
      for (final long lo : new long[] { 10, 1001, 3000 })
        assertAnswer("MATCH (v:V) WHERE v.a > $lo RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > $lo RETURN min(v.a + 0) AS c",
            Map.of("lo", lo), null);
    }

    @Test
    void boundsOfAnotherTypeAndRepeatedBounds() {
      // fractional bounds, a bound the key cannot be compared with, two lower bounds: the answer is the scan's
      assertAnswer("MATCH (v:V) WHERE v.a > 100.5 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 100.5 RETURN min(v.a + 0) AS c", Map.of(),
          102L);
      assertAnswer("MATCH (v:V) WHERE v.a < 101.5 RETURN max(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 < 101.5 RETURN max(v.a + 0) AS c", Map.of(),
          100L);
      assertAnswer("MATCH (v:V) WHERE v.a > 'x' RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 'x' RETURN min(v.a + 0) AS c", Map.of(), null);
      assertAnswer("MATCH (v:V) WHERE v.a > 500 AND v.a > 100 RETURN min(v.a) AS c",
          "MATCH (v:V) WHERE v.a + 0 > 500 AND v.a + 0 > 100 RETURN min(v.a + 0) AS c", Map.of(), 502L);
      assertAnswer("MATCH (v:V) WHERE v.a < 500 AND v.a < 100 RETURN max(v.a) AS c",
          "MATCH (v:V) WHERE v.a + 0 < 500 AND v.a + 0 < 100 RETURN max(v.a + 0) AS c", Map.of(), 98L);
    }

    @Test
    void emptyRangeReturnsOneNullRow() {
      try (final ResultSet rs = database.query("opencypher", "MATCH (v:V) WHERE v.a > 999999 RETURN min(v.a) AS c")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Object>getProperty("c")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void aNullParameterBoundAnswersNull() {
      final Map<String, Object> nullParam = new HashMap<>();
      nullParam.put("lo", null);
      assertAnswer("MATCH (v:V) WHERE v.a > $lo RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > $lo RETURN min(v.a + 0) AS c", nullParam, null);
      assertThat(scalar("MATCH (v:V) WHERE v.a > $lo RETURN min(v.a) AS c", nullParam)).isNull();
    }

    @Test
    void aDeletedEndOfTheRangeIsSkipped() {
      database.transaction(() -> database.command("sql", "DELETE FROM V WHERE a = 1002"));
      assertAnswer("MATCH (v:V) WHERE v.a > 1001 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 1001 RETURN min(v.a + 0) AS c", Map.of(),
          1004L);
    }

    @Test
    void changesOfTheOpenTransactionAreSeen() {
      database.begin();
      try {
        database.command("sql", "CREATE VERTEX V SET a = 1003");
        database.command("sql", "DELETE FROM V WHERE a = 1002");
        assertAnswer("MATCH (v:V) WHERE v.a > 1001 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 1001 RETURN min(v.a + 0) AS c", Map.of(),
            1003L);
      } finally {
        database.rollback();
      }
      assertAnswer("MATCH (v:V) WHERE v.a > 1001 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 1001 RETURN min(v.a + 0) AS c", Map.of(),
          1002L);
    }

    @Test
    void anythingElseInTheWhereKeepsTheScan() {
      assertThat(profile("MATCH (v:V) WHERE v.a > 100 AND v.a <> 500 RETURN min(v.a) AS c", Map.of())).doesNotContain("MIN FROM INDEX");
      assertThat(profile("MATCH (v:V) WHERE v.a > 100 OR v.a < 5 RETURN min(v.a) AS c", Map.of())).doesNotContain("MIN FROM INDEX");
      assertThat(profile("MATCH (v:V) WHERE v.a = 100 RETURN min(v.a) AS c", Map.of())).doesNotContain("MIN FROM INDEX");
      assertAnswer("MATCH (v:V) WHERE v.a > 100 AND v.a <> 102 RETURN min(v.a) AS c",
          "MATCH (v:V) WHERE v.a + 0 > 100 AND v.a + 0 <> 102 RETURN min(v.a + 0) AS c", Map.of(), 104L);
    }

    private void assertAnswer(final String query, final String check, final Map<String, Object> params, final Object expected) {
      final Object actual = scalar(query, params);
      assertThat(actual).as(query).isEqualTo(scalar(check, params));
      if (expected != null)
        assertThat(actual).as(query).isEqualTo(expected);
    }

    private Object scalar(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        return rs.next().getProperty("c");
      }
    }

    private String profile(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }
  }
}
