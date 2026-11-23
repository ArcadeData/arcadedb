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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12276: point() built a WGS-84 point without longitude, latitude and height from x/y and a geographic srid or crs,
 * built a WGS-84 point from a map holding both coordinate styles, and accepted non-finite coordinates and incomplete maps.
 * Expected values are the ones Neo4j 5 returns.
 */
class CypherPointConstructionIssue12276Test extends TestHelper {

  private Object query(final String statement) {
    try (final ResultSet rs = database.query("opencypher", statement)) {
      return rs.next().getProperty("v");
    }
  }

  private Object value(final String expression) {
    return query("RETURN " + expression + " AS v");
  }

  private void assertRefused(final String expression, final String message) {
    assertThatThrownBy(() -> value(expression)).hasMessageContaining(message);
  }

  @Test
  void geographicSridWithXYHasLongitudeAndLatitude() {
    assertThat(value("point({x: 1, y: 2, srid: 4326}).longitude")).isEqualTo(1.0);
    assertThat(value("point({x: 1, y: 2, srid: 4326}).latitude")).isEqualTo(2.0);
    assertThat(value("point({x: 1, y: 2, srid: 4326}).crs")).isEqualTo("wgs-84");
    assertThat(value("point({x: 1, y: 2, crs: 'WGS-84'}).longitude")).isEqualTo(1.0);
    assertThat(value("point({x: 1, y: 2, crs: 'WGS-84'}).crs")).isEqualTo("wgs-84");
  }

  @Test
  void geographic3DWithXYZHasHeight() {
    assertThat(value("point({x: 1, y: 2, z: 3, crs: 'wgs-84-3d'}).height")).isEqualTo(3.0);
    assertThat(value("point({x: 1, y: 2, z: 3, srid: 4979}).height")).isEqualTo(3.0);
    assertThat(value("point({x: 1, y: 2, z: 3, srid: 4979}).z")).isEqualTo(3.0);
    assertThat(value("point({x: 1, y: 2, z: 3, srid: 4979}).longitude")).isEqualTo(1.0);
    assertThat(value("point({x: 1, y: 2, z: 3, srid: 4979}).crs")).isEqualTo("wgs-84-3d");
  }

  // The two ways to spell the same WGS-84 point build the same point, key for key
  @Test
  void aGeographicPointFromXYEqualsTheOneFromLongitudeLatitude() {
    assertThat(value("point({x: 1, y: 2, srid: 4326})")).isEqualTo(value("point({longitude: 1, latitude: 2})"));
    assertThat(value("point({x: 1, y: 2, crs: 'wgs-84'})")).isEqualTo(value("point({longitude: 1, latitude: 2})"));
    assertThat(value("point({x: 1, y: 2, z: 3, srid: 4979})")).isEqualTo(value("point({longitude: 1, latitude: 2, height: 3})"));
    assertThat(value("point({x: 1, y: 2, z: 3, crs: 'wgs-84-3d'})")).isEqualTo(value("point({longitude: 1, latitude: 2, z: 3})"));
    assertThat(value("point.distance(point({x: 1, y: 2, srid: 4326}), point({longitude: 1, latitude: 2}))")).isEqualTo(0.0);
  }

  // The longitude wraps and the latitude is checked on the x/y spelling too
  @Test
  void geographicXYWrapsLongitudeAndChecksLatitude() {
    assertThat(value("point({x: 181, y: 2, srid: 4326}).longitude")).isEqualTo(-179.0);
    assertThat(value("point({x: 181, y: 2, srid: 4326}).x")).isEqualTo(-179.0);
    assertRefused("point({x: 1, y: 91, srid: 4326})", "invalid latitude");
  }

  @Test
  void geographicSridKeepsTheDimensionCheck() {
    assertRefused("point({x: 1, y: 2, z: 3, srid: 4326})", "do not match the dimension of srid 4326");
    assertRefused("point({x: 1, y: 2, srid: 4979})", "do not match the dimension of srid 4979");
    assertRefused("point({x: 1, y: 2, srid: 4326, crs: 'cartesian'})", "do not match");
  }

  // The coordinates of a geographic point built from x/y are the ones a query reads back from the stored property
  @Test
  void aStoredGeographicPointBuiltFromXYReadsLongitudeLatitudeAndHeight() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Place {name: 'a', loc: point({x: 1, y: 2, srid: 4326})})");
      database.command("opencypher", "CREATE (:Place {name: 'b', loc: point({x: 4, y: 5, z: 6, crs: 'wgs-84-3d'})})");
    });
    assertThat(query("MATCH (n:Place {name: 'a'}) RETURN [n.loc.longitude, n.loc.latitude] AS v")).isEqualTo(List.of(1.0, 2.0));
    assertThat(query("MATCH (n:Place {name: 'b'}) RETURN [n.loc.longitude, n.loc.latitude, n.loc.height] AS v"))
        .isEqualTo(List.of(4.0, 5.0, 6.0));
  }

  @Test
  void xyWinsOverLongitudeLatitude() {
    assertThat(value("point({x: 1, y: 2, latitude: 3, longitude: 4}).crs")).isEqualTo("cartesian");
    assertThat(value("point({x: 1, y: 2, latitude: 3, longitude: 4}).x")).isEqualTo(1.0);
    assertThat(value("point({x: 1, y: 2, latitude: 3, longitude: 4}).y")).isEqualTo(2.0);
    // a geographic crs asked for by name still builds the point from x/y
    assertThat(value("point({x: 1, y: 2, latitude: 3, longitude: 4, crs: 'wgs-84'}).longitude")).isEqualTo(1.0);
    assertThat(value("point({x: 1, y: 2, latitude: 3, longitude: 4, crs: 'wgs-84'}).latitude")).isEqualTo(2.0);
  }

  @Test
  void nonFiniteCoordinatesAreRefused() {
    assertRefused("point({x: 1/0.0, y: 2}).x", "non-finite");
    assertRefused("point({x: 0.0/0.0, y: 2}).x", "non-finite");
    assertRefused("point({x: 1, y: -1/0.0})", "non-finite");
    assertRefused("point({longitude: 1/0.0, latitude: 2})", "non-finite");
    assertRefused("point({longitude: 1, latitude: 0.0/0.0})", "non-finite");
    assertRefused("point({x: 1, y: 2, z: -1/0.0})", "non-finite");
    assertRefused("point({longitude: 1, latitude: 2, height: 0.0/0.0})", "non-finite");
    assertRefused("point(1/0.0, 2)", "non-finite");
    assertRefused("point({x: 1/0.0, y: 2, srid: 4326})", "non-finite");
    assertRefused("point.distance(point({x: 1/0.0, y: 2}), point({x: 0, y: 0}))", "non-finite");
  }

  @Test
  void incompleteMapIsRefused() {
    assertRefused("point({x: 1})", "is not describing a valid point");
    assertRefused("point({y: 1})", "is not describing a valid point");
    assertRefused("point({latitude: 1})", "is not describing a valid point");
    assertRefused("point({longitude: 1})", "is not describing a valid point");
    assertRefused("point({x: 1, longitude: 2, latitude: 3})", "is not describing a valid point");
    // a key that is present with a null is still a null point, as before
    assertThat(value("point({x: null, y: 2})")).isNull();
    assertThat(value("point({longitude: 1, latitude: null})")).isNull();
  }

  // The refusal names the keys, a capped and cleaned list of them: a wide map or a key with a line break must not reach the log
  @Test
  void theRefusalOfAnIncompleteMapNamesAFewKeysOnOneLine() {
    assertThatThrownBy(() -> value("point({x: 1, k1: 1, k2: 2, k3: 3, k4: 4, k5: 5, k6: 6, k7: 7, k8: 8})"))
        .hasMessageContaining("is not describing a valid point").hasMessageContaining("k3").hasMessageContaining("more")
        .hasMessageNotContaining("k8");
    assertThatThrownBy(() -> value("point({x: 1, `a\nb`: 1})")).hasMessageContaining("is not describing a valid point")
        .hasMessageNotContaining("\n");
  }

  // Not part of the issue and kept as it was: a numeric string is still a coordinate (#4305)
  @Test
  void aNumericStringCoordinateIsStillConverted() {
    assertThat(value("point({x: '1', y: 2}).x")).isEqualTo(1.0);
    assertThat(value("point({x: '1', y: 2, srid: 4326}).longitude")).isEqualTo(1.0);
  }
}
