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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8422: a Cypher query that filters and orders by the same indexed property, under a LIMIT, sorted the whole
 * qualifying range to return its first rows, while the index range scan it reads from already produces them in that
 * order. The plan now leaves the sort out and lets the LIMIT stop the scan, in both directions, and reads a label with
 * no range predicate in index order too, followed by the vertices the index does not hold (a null key sorts last).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8422CypherIndexOrderLimitTest extends TestHelper {
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
    assertThat(profile).contains("NodeIndexRangeScan").contains("index order").doesNotContain("OrderByStep");
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
    assertThat(profile(query, Map.of("cursor", ""))).doesNotContain("OrderByStep");
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
    assertThat(profile).contains("index order, descending").doesNotContain("OrderByStep");
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
    assertThat(profile(query, Map.of("cursor", id(10)))).doesNotContain("OrderByStep");
  }

  @Test
  void anExpansionKeepsTheAnchorOrder() {
    final String query =
        "MATCH (n:Repro)-[:Link]->(m:Repro) WHERE n.id > $cursor RETURN n.id AS a, m.id AS b ORDER BY n.id LIMIT 3";
    final List<Map<String, Object>> rows = rows(query, Map.of("cursor", id(10)));
    assertThat(rows).extracting(r -> r.get("a")).containsExactly(id(11), id(12), id(13));
    assertThat(rows).extracting(r -> r.get("b")).containsExactly(id(12), id(13), id(14));
    assertThat(profile(query, Map.of("cursor", id(10)))).doesNotContain("OrderByStep");
  }

  @Test
  void distinctKeepsTheIndexOrder() {
    final String query = "MATCH (n:Repro) WHERE n.num >= 90 RETURN DISTINCT n.num AS num ORDER BY num LIMIT 4";
    assertThat(column(query, Map.of(), "num")).containsExactly(90, 91, 92, 93);
    assertThat(profile(query, Map.of())).doesNotContain("OrderByStep");
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
    assertThat(profile).contains("NodeIndexRangeScan").contains("index order").doesNotContain("OrderByStep");
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
    assertThat(profile("MATCH (n:Repro) WHERE n.id > \"\" RETURN n.id ORDER BY n.id", Map.of())).contains("OrderByStep");
    // A second sort key the single-property index does not hold
    final String twoKeys = "MATCH (n:Repro) WHERE n.num >= 98 RETURN n.num AS num, n.seq AS seq ORDER BY num, seq DESC LIMIT 3";
    assertThat(profile(twoKeys, Map.of())).contains("OrderByStep");
    final List<Map<String, Object>> rows = rows(twoKeys, Map.of());
    assertThat(rows).extracting(r -> r.get("num")).containsExactly(98, 98, 98);
    assertThat(rows).extracting(r -> r.get("seq")).containsExactly(4998, 4898, 4798);
    // Ordered by a property other than the range one
    assertThat(profile("MATCH (n:Repro) WHERE n.id > \"\" RETURN n.seq ORDER BY n.seq LIMIT 3", Map.of())).contains("OrderByStep");
    // A WITH may hand the rows on in another shape
    assertThat(profile("MATCH (n:Repro) WHERE n.id > \"\" WITH n RETURN n.id ORDER BY n.id LIMIT 3", Map.of()))
        .contains("OrderByStep");
    // A SET may rewrite the key the rows would be ordered by
    database.transaction(() -> assertThat(
        column("MATCH (n:Repro) WHERE n.id < $cursor SET n.id = 'z' + toString(10 - n.seq) RETURN n.id AS id ORDER BY n.id LIMIT 2",
            Map.of("cursor", id(3)), "id")).containsExactly("z10", "z8"));
    // A RETURN alias that shadows the pattern variable is not the vertex any more
    assertThat(profile("MATCH (n:Repro) WHERE n.id > \"\" RETURN n.seq AS n ORDER BY n LIMIT 3", Map.of())).contains("OrderByStep");
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
