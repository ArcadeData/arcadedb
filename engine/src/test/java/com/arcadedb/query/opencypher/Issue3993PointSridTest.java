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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #3993: Cypher {@code point()} must expose the SRID Neo4j reports for every
 * coordinate reference system (7203/9157 cartesian, 4326/4979 WGS-84).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3993PointSridTest extends TestHelper {

  private Map<?, ?> point(final String expression) {
    try (final ResultSet rs = database.command("opencypher", "RETURN " + expression + " AS p")) {
      return (Map<?, ?>) rs.next().getProperty("p");
    }
  }

  @Test
  void cartesian2DDefaultsToSrid7203() {
    final Map<?, ?> p = point("point({x: 2.3, y: 4.5})");
    assertThat(p.get("srid")).isEqualTo(7203);
    assertThat(p.get("crs")).isEqualTo("cartesian");
  }

  @Test
  void cartesian3DDefaultsToSrid9157() {
    final Map<?, ?> p = point("point({x: 2.3, y: 4.5, z: 1.0})");
    assertThat(p.get("srid")).isEqualTo(9157);
    assertThat(p.get("crs")).isEqualTo("cartesian-3D");
  }

  @Test
  void wgs84SridsAreUnchanged() {
    assertThat(point("point({longitude: 1, latitude: 2})").get("srid")).isEqualTo(4326);
    assertThat(point("point({longitude: 1, latitude: 2, height: 3})").get("srid")).isEqualTo(4979);
  }

  @Test
  void sridDerivedFromExplicitCrs() {
    assertThat(point("point({x: 1, y: 2, crs: 'WGS-84'})").get("srid")).isEqualTo(4326);
    assertThat(point("point({x: 1, y: 2, z: 3, crs: 'WGS-84-3D'})").get("srid")).isEqualTo(4979);
    assertThat(point("point({x: 1, y: 2, crs: 'cartesian'})").get("srid")).isEqualTo(7203);
    assertThat(point("point({x: 1, y: 2, z: 3, crs: 'cartesian-3D'})").get("srid")).isEqualTo(9157);
  }

  @Test
  void crsDerivedFromExplicitSrid() {
    final Map<?, ?> p = point("point({x: 1, y: 2, srid: 4326})");
    assertThat(p.get("srid")).isEqualTo(4326);
    assertThat(p.get("crs")).isEqualTo("WGS-84");
    assertThat(point("point({x: 1, y: 2, srid: 7203})").get("crs")).isEqualTo("cartesian");
  }

  @Test
  void sridDerivedFromLowerCaseCrs() {
    assertThat(point("point({x: 1, y: 2, crs: 'wgs-84'})").get("srid")).isEqualTo(4326);
    assertThat(point("point({x: 1, y: 2, z: 3, crs: 'wgs-84-3d'})").get("srid")).isEqualTo(4979);
    assertThat(point("point({x: 1, y: 2, z: 3, crs: 'cartesian-3d'})").get("srid")).isEqualTo(9157);
  }

  @Test
  void mismatchedCrsAndSridIsRejected() {
    assertThatThrownBy(() -> point("point({x: 1, y: 2, crs: 'WGS-84', srid: 7203})")).isInstanceOf(CommandSemanticException.class);
    assertThat(point("point({x: 1, y: 2, crs: 'WGS-84', srid: 4326})").get("srid")).isEqualTo(4326);
  }

  @Test
  void unknownSridFallsBackToCartesianName() {
    final Map<?, ?> p = point("point({x: 1, y: 2, srid: 9999})");
    assertThat(p.get("srid")).isEqualTo(9999);
    assertThat(p.get("crs")).isEqualTo("cartesian");
  }

  @Test
  void unknownCrsNameIsKeptWithoutSrid() {
    final Map<?, ?> p = point("point({x: 1, y: 2, crs: 'foo'})");
    assertThat(p.get("crs")).isEqualTo("foo");
    assertThat(p.containsKey("srid")).isFalse();
  }

  @Test
  void dimensionMismatchIsRejected() {
    assertThatThrownBy(() -> point("point({x: 1, y: 2, z: 3, crs: 'WGS-84'})")).isInstanceOf(CommandSemanticException.class);
    assertThatThrownBy(() -> point("point({x: 1, y: 2, z: 3, srid: 4326})")).isInstanceOf(CommandSemanticException.class);
    assertThatThrownBy(() -> point("point({x: 1, y: 2, crs: 'cartesian-3D'})")).isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void positionalFormHasWgs84Srid() {
    assertThat(point("point(1, 2)").get("srid")).isEqualTo(4326);
  }

  @Test
  void xyPointWithGeographicSridIsGeographicForDistance() {
    try (final ResultSet rs = database.command("opencypher",
        "RETURN point.distance(point({x: 1, y: 2, srid: 4326}), point({longitude: 1, latitude: 2})) AS d")) {
      assertThat(((Number) rs.next().getProperty("d")).doubleValue()).isEqualTo(0.0);
    }
  }

  @Test
  void lowerCaseCrsIsStoredCanonicallyAndIsGeographic() {
    assertThat(point("point({x: 1, y: 2, crs: 'wgs-84'})").get("crs")).isEqualTo("WGS-84");
    try (final ResultSet rs = database.command("opencypher",
        "RETURN point.distance(point({x: 1, y: 2, crs: 'wgs-84'}), point({x: 1, y: 3, crs: 'WGS-84'})) AS d")) {
      // one degree of latitude is about 111 km, only possible when both points are measured geographically
      assertThat(((Number) rs.next().getProperty("d")).doubleValue()).isBetween(110_000.0, 112_000.0);
    }
  }

  @Test
  void fractionalOrNegativeSridIsRejected() {
    assertThatThrownBy(() -> point("point({x: 1, y: 2, srid: 4326.9})")).isInstanceOf(CommandSemanticException.class);
    assertThatThrownBy(() -> point("point({x: 1, y: 2, srid: -1})")).isInstanceOf(CommandSemanticException.class);
  }
}
