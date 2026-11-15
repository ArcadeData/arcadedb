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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11949: a point is the map point() returns, so it was ordered among the maps (by size), before lists and paths. The
 * openCypher order is MAP < NODE < RELATIONSHIP < LIST < PATH < POINT < temporal values < DURATION < STRING < BOOLEAN < NUMBER
 * < NULL, for ORDER BY and for min() / max() alike.
 */
class CypherPointOrderabilityIssue11949Test extends TestHelper {

  private static final String MIXED = "[date('2020-01-01'), point({x: 1, y: 2}), [1], {a: 1}, 'b']";

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {k: 1})-[:R]->(:A {k: 2})"));
  }

  @Test
  void orderByPutsAPointAfterListsAndPathsAndBeforeTemporals() {
    assertThat(kinds("UNWIND " + MIXED + " AS x RETURN valueType(x) AS t ORDER BY x"))
        .containsExactly("MAP", "LIST", "POINT", "DATE", "STRING");
    assertThat(kinds("UNWIND " + MIXED + " AS x RETURN valueType(x) AS t ORDER BY x DESC"))
        .containsExactly("STRING", "DATE", "POINT", "LIST", "MAP");
    assertThat(kinds("MATCH p = (:A)-[:R]->(:A) UNWIND [point({x: 1, y: 2}), p, [1], date('2020-01-01')] AS x "
        + "RETURN valueType(x) AS t ORDER BY x")).containsExactly("LIST", "PATH", "POINT", "DATE");
    // A point and a map with as many entries were ordered as two maps, by their sorted keys: crs < z1 put the point first
    assertThat(kinds("UNWIND [point({x: 1, y: 2}), {z1: 1, z2: 2, z3: 3, z4: 4}] AS x RETURN valueType(x) AS t ORDER BY x"))
        .containsExactly("MAP", "POINT");
  }

  @Test
  void minAndMaxFollowTheSameOrder() {
    assertThat(kinds("UNWIND [point({x: 1, y: 2}), [1]] AS x RETURN valueType(min(x)) AS t")).containsExactly("LIST");
    assertThat(kinds("UNWIND [point({x: 1, y: 2}), [1]] AS x RETURN valueType(max(x)) AS t")).containsExactly("POINT");
    assertThat(kinds("UNWIND [point({x: 1, y: 2}), date('2020-01-01')] AS x RETURN valueType(min(x)) AS t"))
        .containsExactly("POINT");
    assertThat(kinds("UNWIND [point({x: 1, y: 2}), {a: 1}] AS x RETURN valueType(max(x)) AS t")).containsExactly("POINT");
    assertThat(kinds("RETURN valueType(coll.min([point({x: 1, y: 2}), [1]])) AS t")).containsExactly("LIST");
    assertThat(kinds("RETURN valueType(coll.max([point({x: 1, y: 2}), [1], {a: 1}])) AS t")).containsExactly("POINT");
  }

  @Test
  void pointsKeepTheirOrderAmongThemselves() {
    final String points = "[point({x: 2, y: 1}), point({x: 1, y: 5}), point({x: 1, y: 2})]";
    assertThat(coordinates("UNWIND " + points + " AS p RETURN p.x AS x, p.y AS y ORDER BY p"))
        .containsExactly(List.of(1.0, 2.0), List.of(1.0, 5.0), List.of(2.0, 1.0));
    assertThat(coordinates("UNWIND " + points + " AS p RETURN p.x AS x, p.y AS y ORDER BY p DESC"))
        .containsExactly(List.of(2.0, 1.0), List.of(1.0, 5.0), List.of(1.0, 2.0));
    assertThat(coordinates("UNWIND " + points + " AS p WITH min(p) AS p RETURN p.x AS x, p.y AS y"))
        .containsExactly(List.of(1.0, 2.0));
    assertThat(coordinates("UNWIND " + points + " AS p WITH max(p) AS p RETURN p.x AS x, p.y AS y"))
        .containsExactly(List.of(2.0, 1.0));
  }

  private List<String> kinds(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      // valueType() answers e.g. "LIST<INTEGER NOT NULL> NOT NULL": the type name is its first word
      return rs.stream().map(r -> r.<String>getProperty("t").split("[ <]")[0]).toList();
    }
  }

  private List<List<Double>> coordinates(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.stream()
          .map(r -> List.of(r.<Number>getProperty("x").doubleValue(), r.<Number>getProperty("y").doubleValue())).toList();
    }
  }
}
