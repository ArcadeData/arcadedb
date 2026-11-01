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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11113: the crs name of a point was {@code WGS-84}, {@code WGS-84-3D} and {@code cartesian-3D}, where
 * Neo4j (and its Cypher manual) answers {@code wgs-84}, {@code wgs-84-3d} and {@code cartesian-3d}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11113PointCrsNameCaseTest extends TestHelper {

  @Test
  void everyConstructorAnswersNeo4jSpelling() {
    assertThat(crs("point({longitude: 1, latitude: 2, height: 3})")).isEqualTo("wgs-84-3d");
    assertThat(crs("point({longitude: 1.5, latitude: 2.5})")).isEqualTo("wgs-84");
    assertThat(crs("point({x: 1, y: 2, z: 3})")).isEqualTo("cartesian-3d");
    assertThat(crs("point({x: 1, y: 2})")).isEqualTo("cartesian");
    // ArcadeDB's positional form is WGS-84 too
    assertThat(crs("point(1, 2)")).isEqualTo("wgs-84");
  }

  @Test
  void aNameOrSridGivenInAnySpellingAnswersTheCanonicalName() {
    assertThat(crs("point({x: 1, y: 2, crs: 'WGS-84'})")).isEqualTo("wgs-84");
    assertThat(crs("point({x: 1, y: 2, z: 3, crs: 'WGS-84-3D'})")).isEqualTo("wgs-84-3d");
    assertThat(crs("point({x: 1, y: 2, z: 3, crs: 'Cartesian-3D'})")).isEqualTo("cartesian-3d");
    assertThat(crs("point({x: 1, y: 2, srid: 4326})")).isEqualTo("wgs-84");
    assertThat(crs("point({x: 1, y: 2, z: 3, srid: 4979})")).isEqualTo("wgs-84-3d");
    assertThat(crs("point({x: 1, y: 2, z: 3, srid: 9157})")).isEqualTo("cartesian-3d");
  }

  @Test
  void theCommonEndsWithPatternWorks() {
    final Result row = single("RETURN point({longitude: 1, latitude: 2, height: 3}).crs ENDS WITH '-3d' AS geo3d, "
        + "point({x: 1, y: 2, z: 3}).crs ENDS WITH '-3d' AS cart3d, point({x: 1, y: 2}).crs ENDS WITH '-3d' AS cart2d");
    assertThat(row.<Boolean>getProperty("geo3d")).isTrue();
    assertThat(row.<Boolean>getProperty("cart3d")).isTrue();
    assertThat(row.<Boolean>getProperty("cart2d")).isFalse();
  }

  /**
   * Points written before the fix carry the old spelling in the database: the functions reading a crs still recognise them.
   */
  @Test
  void aPointStoredWithTheOldSpellingIsStillGeographic() {
    final Map<String, Object> oldPoint = Map.of("longitude", 12.4964, "latitude", 41.9028, "x", 12.4964, "y", 41.9028, "crs",
        "WGS-84", "srid", 4326);
    final Map<String, Object> newPoint = Map.of("longitude", 9.19, "latitude", 45.4642, "x", 9.19, "y", 45.4642, "crs",
        "wgs-84", "srid", 4326);
    try (final ResultSet rs = database.query("opencypher",
        "RETURN point.distance($a, $b) AS mixed, point.distance(point({longitude: 12.4964, latitude: 41.9028}), "
            + "point({longitude: 9.19, latitude: 45.4642})) AS fresh, "
            + "point.withinBBox($a, point({longitude: 12, latitude: 41}), point({longitude: 13, latitude: 42})) AS inside",
        Map.of("a", oldPoint, "b", newPoint))) {
      final Result row = rs.next();
      final double fresh = row.<Number>getProperty("fresh").doubleValue();
      // geographic distance in metres (Rome-Milan is ~477 km), not a cartesian one in degrees
      assertThat(fresh).isBetween(470_000.0, 490_000.0);
      assertThat(row.<Number>getProperty("mixed").doubleValue()).isEqualTo(fresh);
      assertThat(row.<Boolean>getProperty("inside")).isTrue();
    }
  }

  private String crs(final String pointExpression) {
    return single("RETURN " + pointExpression + ".crs AS crs").getProperty("crs");
  }

  private Result single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next();
    }
  }
}
