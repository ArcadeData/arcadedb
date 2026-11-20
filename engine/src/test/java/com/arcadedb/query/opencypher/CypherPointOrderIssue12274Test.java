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
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12274: ORDER BY, min() and max() ordered points as maps (WGS-84 by latitude first, CRS by key count), and two equal
 * points were not {@code <=} each other. Expected values are the ones Neo4j 5 returns.
 */
class CypherPointOrderIssue12274Test extends TestHelper {

  private static final String WGS84 = "[point({longitude:10,latitude:5}), point({longitude:-10,latitude:1}), point({longitude:0,latitude:9})]";

  private Object value(final String statement) {
    try (final ResultSet rs = database.query("opencypher", statement)) {
      return rs.next().getProperty("v");
    }
  }

  @Test
  void orderByWgs84ByLongitudeFirst() {
    assertThat(value("UNWIND " + WGS84 + " AS p WITH p ORDER BY p RETURN collect(p.longitude) AS v"))
        .isEqualTo(List.of(-10.0, 0.0, 10.0));
    assertThat(value("UNWIND [point({longitude:10,latitude:5,height:0}), point({longitude:-10,latitude:1,height:9}), "
        + "point({longitude:0,latitude:9,height:3})] AS p WITH p ORDER BY p RETURN collect(p.longitude) AS v"))
        .isEqualTo(List.of(-10.0, 0.0, 10.0));
  }

  @Test
  void orderByCrs() {
    assertThat(value("UNWIND [point({x:1,y:1}), point({x:1,y:1,z:0}), point({longitude:0,latitude:0}), "
        + "point({longitude:0,latitude:0,height:0})] AS p WITH p ORDER BY p RETURN collect(p.srid) AS v"))
        .isEqualTo(List.of(4326, 4979, 7203, 9157));
  }

  // A point of a crs name that is not one of the four Neo4j defines carries no srid (ArcadeDB keeps it as given): it sorts after the
  // points of a known srid
  @Test
  void aPointOfAnUnknownCrsSortsLast() {
    assertThat(value("UNWIND [point({x:1,y:1,crs:'custom'}), point({x:1,y:1}), point({longitude:0,latitude:0})] AS p "
        + "WITH p ORDER BY p RETURN collect(p.crs) AS v")).isEqualTo(List.of("wgs-84", "cartesian", "custom"));
    assertThat(value("UNWIND [point({x:1,y:1,crs:'custom'}), point({x:1,y:1}), point({longitude:0,latitude:0})] AS p "
        + "WITH p ORDER BY p DESC RETURN collect(p.crs) AS v")).isEqualTo(List.of("custom", "cartesian", "wgs-84"));
  }

  // Two unknown crs names carry no srid to tell them apart: the name does, so the order of equal coordinates does not depend on the
  // order of the input
  @Test
  void pointsOfDifferentUnknownCrsOrderByTheirName() {
    for (final String list : List.of("[point({x:1,y:1,crs:'b'}), point({x:1,y:1,crs:'a'})]",
        "[point({x:1,y:1,crs:'a'}), point({x:1,y:1,crs:'b'})]")) {
      assertThat(value("UNWIND " + list + " AS p WITH p ORDER BY p RETURN collect(p.crs) AS v")).isEqualTo(List.of("a", "b"));
      assertThat(value("UNWIND " + list + " AS p WITH min(p) AS lo, max(p) AS hi RETURN [lo.crs, hi.crs] AS v")).isEqualTo(List.of("a", "b"));
    }
    // The system comes before the coordinates: all the points of one name sort together
    assertThat(value("UNWIND [point({x:5,y:5,crs:'a'}), point({x:1,y:1,crs:'b'}), point({x:9,y:9,crs:'a'})] AS p "
        + "WITH p ORDER BY p RETURN collect([p.crs, p.x]) AS v")).isEqualTo(List.of(List.of("a", 5.0), List.of("a", 9.0), List.of("b", 1.0)));
  }

  // Equal srid, x and y: the points of one srid order by z
  @Test
  void pointsOfOneCrsTieBreakByZ() {
    assertThat(value("UNWIND [point({x:1,y:1,z:5}), point({x:1,y:1,z:2}), point({x:0,y:9,z:7})] AS p "
        + "WITH p ORDER BY p RETURN collect(p.z) AS v")).isEqualTo(List.of(7.0, 2.0, 5.0));
    // A point-shaped map written as a literal may lack the z of its srid: the missing z sorts before a present one
    assertThat(value("UNWIND [{crs:'cartesian', srid:7203, x:1, y:1, z:3}, {crs:'cartesian', srid:7203, x:1, y:1}] AS p "
        + "WITH p ORDER BY p RETURN collect(p.z IS NULL) AS v")).isEqualTo(List.of(true, false));
  }

  // Points read back from a stored property order as the points they were written as
  @Test
  void storedPointsOrderAsPoints() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Place {name:'a', loc: point({longitude:10,latitude:5})})");
      database.command("opencypher", "CREATE (:Place {name:'b', loc: point({longitude:-10,latitude:1})})");
      database.command("opencypher", "CREATE (:Place {name:'c', loc: point({longitude:0,latitude:9})})");
    });
    assertThat(value("MATCH (n:Place) WITH n ORDER BY n.loc RETURN collect(n.name) AS v")).isEqualTo(List.of("b", "c", "a"));
    assertThat(value("MATCH (n:Place) WITH n ORDER BY n.loc DESC RETURN collect(n.name) AS v")).isEqualTo(List.of("a", "c", "b"));
    assertThat(value("MATCH (n:Place) WITH min(n.loc) AS m, max(n.loc) AS M RETURN [m.longitude, M.longitude] AS v"))
        .isEqualTo(List.of(-10.0, 10.0));
  }

  @Test
  void minAndMax() {
    assertThat(value("UNWIND " + WGS84 + " AS p WITH min(p) AS m, max(p) AS M RETURN [m.longitude, M.longitude] AS v"))
        .isEqualTo(List.of(-10.0, 10.0));
    assertThat(value("UNWIND [point({x:1,y:1}), point({longitude:0,latitude:0})] AS p WITH min(p) AS m, max(p) AS M "
        + "RETURN [m.srid, M.srid] AS v")).isEqualTo(List.of(4326, 7203));
  }

  @Test
  void equalPointsAreLessOrEqual() {
    assertThat(value("RETURN point({x:1,y:2}) <= point({x:1,y:2}) AS v")).isEqualTo(true);
    assertThat(value("RETURN point({longitude:1,latitude:2}) >= point({longitude:1,latitude:2}) AS v")).isEqualTo(true);
    assertThat(value("RETURN point({x:1,y:2}) <= point({x:1,y:3}) AS v")).isNull();
    assertThat(value("RETURN point({x:1,y:2}) < point({x:1,y:3}) AS v")).isNull();
  }

  @Test
  void ordinaryMapsAreNotPointsUnderInclusiveComparison() {
    assertThat(value("RETURN {x:1,y:2} <= {x:1,y:2} AS v")).isNull();
    assertThat(value("RETURN {x:1,y:2} >= {x:1,y:2} AS v")).isNull();
  }

  // A point and an ordinary map with the same keys: a map sorts before a point, so the order does not depend on the input order
  @Test
  void mapsSortBeforePointsWhateverTheInputOrder() {
    final String a = "{crs:'wgs-84', srid:4326, x:0, y:0}";
    final String b = "{crs:'cartesian', srid:7203, x:0, y:0}";
    final String m = "{crs:'middle', srid:0, x:'text', y:0}";
    for (final String list : List.of("[" + a + "," + b + "," + m + "]", "[" + b + "," + m + "," + a + "]",
        "[" + m + "," + a + "," + b + "]")) {
      assertThat(value("UNWIND " + list + " AS p WITH min(p) AS lo, max(p) AS hi RETURN [lo.crs, hi.crs] AS v"))
          .isEqualTo(List.of("middle", "cartesian"));
      assertThat(value("UNWIND " + list + " AS p WITH p ORDER BY p RETURN collect(p.crs) AS v"))
          .isEqualTo(List.of("middle", "wgs-84", "cartesian"));
    }
    // Two ordinary maps keep the map order: size first
    assertThat(value("UNWIND [{x:1, y:2, k:3}, {x:9}] AS p WITH p ORDER BY p RETURN collect(p.x) AS v")).isEqualTo(List.of(9L, 1L));
  }

  // A Java parameter map may be sorted by keys that are not strings (a TreeMap of Integer): looking a String key up in it throws a
  // ClassCastException, which the point check must not let out of the map order and the inclusive comparison (#11657 orders such maps)
  @Test
  void aMapWithNonStringKeysIsNotAPoint() {
    final TreeMap<Integer, Integer> integerKeys = new TreeMap<>(Map.of(1, 10));
    try (final ResultSet rs = database.query("opencypher", "UNWIND $maps AS m WITH m ORDER BY m RETURN size(keys(m)) AS v",
        Map.of("maps", List.of(Map.of("a", 1, "b", 2), integerKeys)))) {
      assertThat(rs.stream().map(r -> r.<Number>getProperty("v").intValue()).toList()).containsExactly(1, 2);
    }
    try (final ResultSet rs = database.query("opencypher", "RETURN $a <= $b AS v",
        Map.of("a", integerKeys, "b", new TreeMap<>(integerKeys)))) {
      assertThat(rs.next().<Object>getProperty("v")).isNull();
    }
  }
}
