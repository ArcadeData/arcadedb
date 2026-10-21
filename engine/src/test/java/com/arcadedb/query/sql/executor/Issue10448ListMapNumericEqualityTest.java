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
import com.arcadedb.database.RID;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10448: SQL {@code =} on two lists or two maps compared the elements with Java {@code equals()}, so {@code [1L, 2L]} was not
 * equal to {@code [1, 2]} (a list written by the Java API or a 64-bit client against the literal's {@code Integer} elements),
 * where {@code CONTAINSALL}, {@code GROUP BY} and openCypher treat them as one value, as SQL {@code =} already does for two
 * scalars of different numeric classes. Every operator that compares a whole list or map inherits the same rule, and a
 * {@code BY ITEM} index must answer {@code li = [1, 2]} as the scan does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10448ListMapNumericEqualityTest extends TestHelper {

  private static final List<String> ALL = List.of("integer", "long", "sql_content");

  private List<String> keys(final String sql, final Object... params) {
    final List<String> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext())
        keys.add(String.valueOf(rs.next().<Object>getProperty("k")));
    }
    return keys;
  }

  private void createThreeVertices() {
    database.command("sql", "CREATE VERTEX TYPE D").close();
    database.transaction(() -> {
      database.newVertex("D").set("k", "long").set("li", List.of(1L, 2L)).set("mp", Map.of("a", 1L)).save();
      database.newVertex("D").set("k", "integer").set("li", List.of(1, 2)).set("mp", Map.of("a", 1)).save();
      database.command("sql", "CREATE VERTEX D CONTENT {\"k\": \"sql_content\", \"li\": [1, 2], \"mp\": {\"a\": 1}}").close();
    });
  }

  @Test
  void listEqualsLiteralMatchesEveryElementClass() {
    createThreeVertices();
    assertThat(keys("SELECT k FROM D WHERE li = [1, 2] ORDER BY k")).isEqualTo(ALL);
    assertThat(keys("SELECT k FROM D WHERE [1, 2] = li ORDER BY k")).isEqualTo(ALL);
  }

  @Test
  void listEqualsParameterMatchesEveryElementClass() {
    createThreeVertices();
    final Map<String, Object> longs = new HashMap<>();
    longs.put("p", List.of(1L, 2L));
    final Map<String, Object> ints = new HashMap<>();
    ints.put("p", List.of(1, 2));
    for (final Map<String, Object> params : List.of(longs, ints))
      try (final ResultSet rs = database.query("sql", "SELECT k FROM D WHERE li = :p ORDER BY k", params)) {
        final List<String> found = new ArrayList<>();
        while (rs.hasNext())
          found.add(rs.next().getProperty("k"));
        assertThat(found).as("parameter " + params.get("p")).isEqualTo(ALL);
      }
  }

  @Test
  void mapEqualsLiteralMatchesEveryValueClass() {
    createThreeVertices();
    assertThat(keys("SELECT k FROM D WHERE mp = {\"a\": 1} ORDER BY k")).isEqualTo(ALL);
  }

  @Test
  void notEqualsAgreesWithEquals() {
    createThreeVertices();
    assertThat(keys("SELECT k FROM D WHERE li <> [1, 2] ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE mp <> {\"a\": 1} ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE li <> [1, 3] ORDER BY k")).isEqualTo(ALL);
  }

  @Test
  void differentListsAndMapsStayDifferent() {
    createThreeVertices();
    assertThat(keys("SELECT k FROM D WHERE li = [1, 3] ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE li = [2, 1] ORDER BY k")).as("order is part of a list").isEmpty();
    assertThat(keys("SELECT k FROM D WHERE li = [1, 2, 3] ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE li = [1] ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE mp = {\"a\": 2} ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE mp = {\"b\": 1} ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE mp = {\"a\": 1, \"b\": 1} ORDER BY k")).isEmpty();
  }

  @Test
  void mapsWithNullValuesAndNestedCollections() {
    database.command("sql", "CREATE DOCUMENT TYPE M").close();
    database.transaction(() -> {
      database.newDocument("M").set("k", "long").set("mp", Map.of("a", List.of(1L, 2L), "b", Map.of("c", 3L))).save();
      database.newDocument("M").set("k", "integer").set("mp", Map.of("a", List.of(1, 2), "b", Map.of("c", 3))).save();
      final Map<String, Object> withNull = new HashMap<>();
      withNull.put("a", null);
      database.newDocument("M").set("k", "null").set("mp", withNull).save();
    });
    assertThat(keys("SELECT k FROM M WHERE mp = {\"a\": [1, 2], \"b\": {\"c\": 3}} ORDER BY k")).containsExactly("integer", "long");
    assertThat(keys("SELECT k FROM M WHERE mp = {\"a\": null} ORDER BY k")).containsExactly("null");
    assertThat(keys("SELECT k FROM M WHERE mp = {\"a\": [1, 2], \"b\": {\"c\": 4}} ORDER BY k")).isEmpty();
  }

  @Test
  void everyOperatorThatComparesAWholeList() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.transaction(() -> {
      database.newVertex("V").set("k", "long").set("li", List.of(1L, 2L)).set("nl", List.of(List.of(1L, 2L), List.of(3L))).save();
      database.newVertex("V").set("k", "integer").set("li", List.of(1, 2)).set("nl", List.of(List.of(1, 2), List.of(3))).save();
    });
    for (final String condition : List.of("li = [1, 2]", "[1, 2] = li", "li IN [[1, 2]]", "[1, 2] IN nl", "nl CONTAINS [1, 2]",
        "nl CONTAINSALL [[3]]", "nl CONTAINSANY [[3]]", "nl[0] = [1, 2]", "li[0] = 1", "li CONTAINSALL [1, 2]", "li CONTAINS 2"))
      assertThat(keys("SELECT k FROM V WHERE " + condition + " ORDER BY k")).as(condition).containsExactly("integer", "long");
    for (final String condition : List.of("li <> [1, 2]", "NOT (li = [1, 2])", "nl CONTAINS [1, 3]", "nl CONTAINSALL [[4]]",
        "nl CONTAINSANY [[4]]", "li IN [[1, 3]]"))
      assertThat(keys("SELECT k FROM V WHERE " + condition + " ORDER BY k")).as(condition).isEmpty();
  }

  @Test
  void groupByAndContainsAllStillTreatTheListsAsOneValue() {
    createThreeVertices();
    assertThat(keys("SELECT k FROM D WHERE li CONTAINSALL [1, 2] ORDER BY k")).isEqualTo(ALL);
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM D GROUP BY li")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }

  @Test
  void theComparatorHandlesNullsNestingAndSizes() {
    assertThat(BinaryComparator.equals(List.of(1L, 2L), List.of(1, 2))).isTrue();
    assertThat(BinaryComparator.equals(new LinkedList<>(List.of(1L, 2L)), new ArrayList<>(List.of(1, 2)))).as("two implementations").isTrue();
    assertThat(BinaryComparator.equals(List.of(), List.of())).isTrue();
    assertThat(BinaryComparator.equals(List.of(1), List.of(1, 1))).as("size").isFalse();
    assertThat(BinaryComparator.equals(List.of(1, 2), List.of(2, 1))).as("order").isFalse();
    assertThat(BinaryComparator.equals(Arrays.asList(1L, null), Arrays.asList(1, null))).isTrue();
    assertThat(BinaryComparator.equals(Arrays.asList(1L, null), Arrays.asList(1, 2))).isFalse();
    assertThat(BinaryComparator.equals(Arrays.asList(null, 1L), Arrays.asList(1, null))).isFalse();

    assertThat(BinaryComparator.equals(List.of(List.of(1L), Map.of("a", List.of(2L))), List.of(List.of(1), Map.of("a", List.of(2)))))
        .as("nested").isTrue();
    assertThat(BinaryComparator.equals(List.of(List.of(1L), Map.of("a", List.of(2L))), List.of(List.of(1), Map.of("a", List.of(3)))))
        .as("nested, one value apart").isFalse();

    final Map<String, Object> nullValue = new HashMap<>();
    nullValue.put("a", null);
    final Map<String, Object> otherKey = new HashMap<>();
    otherKey.put("b", null);
    assertThat(BinaryComparator.equals(Map.of("a", 1L), Map.of("a", 1))).isTrue();
    assertThat(BinaryComparator.equals(Map.of("a", 1L), Map.of("a", 1, "b", 2))).as("size").isFalse();
    assertThat(BinaryComparator.equals(nullValue, new HashMap<>(nullValue))).isTrue();
    assertThat(BinaryComparator.equals(nullValue, otherKey)).as("a null value is still a key").isFalse();
    assertThat(BinaryComparator.equals(nullValue, Map.of("a", 1))).isFalse();
    assertThat(BinaryComparator.equals(Map.of(1L, "x"), Map.of(1, "x"))).as("keys are matched by the map, as Map.equals does").isFalse();
    assertThat(BinaryComparator.equals(new TreeMap<>(Map.of("a", 1L)), Map.of("a", 1))).as("a sorted map and a hash map").isTrue();
    // a sorted map asked for a key of a class it cannot hold throws: that is a key it does not have, not a failure of the comparison
    assertThat(BinaryComparator.equals(Map.of(1, "x"), new TreeMap<>(Map.of("a", "x")))).as("key class the sorted map cannot hold").isFalse();
    assertThat(BinaryComparator.equals(new TreeMap<>(Map.of("a", "x")), Map.of(1, "x"))).isFalse();
  }

  /** Lists of links compare as they always did: by the records they point to, a loaded record or its RID alike. */
  @Test
  void listsOfLinksAreComparedByTheirRecords() {
    database.command("sql", "CREATE VERTEX TYPE T").close();
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newVertex("T").set("name", "first").save().getIdentity();
      rids[1] = database.newVertex("T").set("name", "second").save().getIdentity();
      database.newVertex("T").set("name", "holder").set("refs", List.of(rids[0], rids[1])).save();
      database.newVertex("T").set("name", "reversed").set("refs", List.of(rids[1], rids[0])).save();
    });

    assertThat(names("SELECT name FROM T WHERE refs = [" + rids[0] + ", " + rids[1] + "]")).containsExactly("holder");
    assertThat(names("SELECT name FROM T WHERE refs = [" + rids[1] + ", " + rids[0] + "]")).containsExactly("reversed");
    assertThat(names("SELECT name FROM T WHERE refs = [" + rids[0] + "]")).isEmpty();
    assertThat(names("SELECT name FROM T WHERE refs CONTAINS " + rids[1] + " ORDER BY name")).containsExactly("holder", "reversed");
    // the loaded records on one side, their RIDs on the other
    assertThat(names("SELECT name FROM T WHERE refs = (SELECT FROM T WHERE name IN ['first', 'second'] ORDER BY name DESC)"))
        .as("a sub-query result is not a list of links in this order, and must not blow up").hasSizeLessThanOrEqualTo(2);
    assertThat(BinaryComparator.equals(List.of(rids[0], rids[1]), List.of(rids[0].asVertex(), rids[1].asVertex()))).isTrue();
    assertThat(BinaryComparator.equals(List.of(rids[0], rids[1]), List.of(rids[1].asVertex(), rids[0].asVertex()))).isFalse();
  }

  /** A list searched for as one element is looked for among the lists and maps of the collection only. */
  @Test
  void containsFindsAListOrMapAmongMixedElements() {
    database.command("sql", "CREATE DOCUMENT TYPE Mixed").close();
    database.transaction(() -> database.newDocument("Mixed").set("name", "mixed")
        .set("items", Arrays.asList("x", 5L, null, List.of(1L, 2L), Map.of("a", 1L))).save());

    assertThat(names("SELECT name FROM Mixed WHERE items CONTAINS [1, 2]")).containsExactly("mixed");
    assertThat(names("SELECT name FROM Mixed WHERE items CONTAINS {\"a\": 1}")).containsExactly("mixed");
    assertThat(names("SELECT name FROM Mixed WHERE items CONTAINS [1, 3]")).isEmpty();
    assertThat(names("SELECT name FROM Mixed WHERE items CONTAINS {\"a\": 2}")).isEmpty();
    assertThat(names("SELECT name FROM Mixed WHERE items CONTAINS 5")).containsExactly("mixed");
  }

  private List<String> names(final String sql) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  /** The hash keys keep the strict comparison: the hash of {@code [1L]} is not the hash of {@code [1]}. */
  @Test
  void theExactComparisonForHashKeysStaysStrict() {
    assertThat(BinaryComparator.equalsExact(List.of(1L, 2L), List.of(1, 2))).isFalse();
    assertThat(BinaryComparator.equalsExact(List.of(1, 2), List.of(1, 2))).isTrue();
    assertThat(BinaryComparator.equalsExact(Map.of("a", 1L), Map.of("a", 1))).isFalse();
    assertThat(BinaryComparator.equalsExact(Map.of("a", 1), Map.of("a", 1))).isTrue();
  }

  /** The values that group together do not depend on the element class either, and are not changed by the comparison of {@code =}. */
  @Test
  void groupByAndDistinctAgreeOnTheClassOfTheElements() {
    createThreeVertices();
    long groups;
    try (final ResultSet rs = database.query("sql", "SELECT li FROM D GROUP BY li")) {
      groups = rs.stream().count();
    }
    long distinct;
    try (final ResultSet rs = database.query("sql", "SELECT DISTINCT li FROM D")) {
      distinct = rs.stream().count();
    }
    assertThat(groups).isEqualTo(1L);
    assertThat(distinct).isEqualTo(groups);
  }

  /**
   * A regular index on a list holds the whole list as its key, element classes included, so it must answer {@code li = [1, 2]}
   * with the rows the scan answers, whichever class the elements were written with.
   */
  @Test
  void aRegularIndexOnAListAnswersWholeListEqualityAsTheScanDoes() {
    for (final String type : List.of("P", "R")) {
      database.command("sql", "CREATE VERTEX TYPE " + type).close();
      database.command("sql", "CREATE PROPERTY " + type + ".li LIST").close();
      if (type.equals("R"))
        database.command("sql", "CREATE INDEX ON R (li) NOTUNIQUE").close();
      database.transaction(() -> {
        database.newVertex(type).set("k", "long").set("li", List.of(1L, 2L)).save();
        database.newVertex(type).set("k", "integer").set("li", List.of(1, 2)).save();
        database.newVertex(type).set("k", "other").set("li", List.of(2, 3)).save();
      });
    }
    for (final String condition : List.of("li = [1, 2]", "li = [2, 3]", "li <> [1, 2]", "li IN [[1, 2]]", "li IN [[1, 2], [2, 3]]")) {
      final List<String> scan = keys("SELECT k FROM P WHERE " + condition + " ORDER BY k");
      final List<String> indexed = keys("SELECT k FROM R WHERE " + condition + " ORDER BY k");
      assertThat(indexed).as(condition).isEqualTo(scan);
    }
    assertThat(keys("SELECT k FROM R WHERE li = [1, 2] ORDER BY k")).containsExactly("integer", "long");
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT k FROM R WHERE li = [1, 2]")) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).as("the index is the one asked").contains("FETCH FROM INDEX");
    }
  }

  /** The scan answers {@code li = [1, 2]} with every vertex; a BY ITEM index (one entry per item) must give the same rows. */
  @Test
  void byItemIndexAnswersWholeListEqualityAsTheScanDoes() {
    for (final String type : List.of("P", "X")) {
      database.command("sql", "CREATE VERTEX TYPE " + type).close();
      database.command("sql", "CREATE PROPERTY " + type + ".li LIST").close();
      if (type.equals("X"))
        database.command("sql", "CREATE INDEX ON X (li BY ITEM) NOTUNIQUE").close();
      database.transaction(() -> {
        database.newVertex(type).set("k", "long").set("li", List.of(1L, 2L)).save();
        database.newVertex(type).set("k", "integer").set("li", List.of(1, 2)).save();
        database.newVertex(type).set("k", "other").set("li", List.of(2, 3)).save();
        database.newVertex(type).set("k", "empty").set("li", List.of()).save();
      });
    }
    for (final String condition : List.of("li = [1, 2]", "li = [2, 3]", "li = []", "li <> [1, 2]", "li CONTAINS 2",
        "li CONTAINSANY [2]", "li CONTAINSALL [1, 2]")) {
      final List<String> scan = keys("SELECT k FROM P WHERE " + condition + " ORDER BY k");
      final List<String> indexed = keys("SELECT k FROM X WHERE " + condition + " ORDER BY k");
      assertThat(indexed).as(condition).isEqualTo(scan);
    }
    assertThat(keys("SELECT k FROM X WHERE li = [1, 2] ORDER BY k")).containsExactly("integer", "long");
    assertThat(keys("SELECT k FROM X WHERE li = [] ORDER BY k")).containsExactly("empty");
  }
}
