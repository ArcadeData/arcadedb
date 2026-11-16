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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12276: point() built wrong points from x/y with a geographic srid and from maps holding both coordinate styles, and
 * accepted non-finite coordinates, unknown crs/srid and incomplete maps; reading a coordinate a point does not have answered
 * null. Expected values are the ones Neo4j 5 returns.
 */
class CypherPointConstructionIssue12276Test extends TestHelper {

  private Object value(final String expression) {
    try (final ResultSet rs = database.query("opencypher", "RETURN " + expression + " AS v")) {
      return rs.next().getProperty("v");
    }
  }

  private void assertRefused(final String expression, final String message) {
    assertThatThrownBy(() -> value(expression)).hasMessageContaining(message);
  }

  @Test
  void geographicSridWithXYHasLongitudeAndLatitude() {
    assertThat(value("point({x: 1, y: 2, srid: 4326}).longitude")).isEqualTo(1.0);
    assertThat(value("point({x: 1, y: 2, srid: 4326}).latitude")).isEqualTo(2.0);
    assertThat(value("point({x: 1, y: 2, z: 3, crs: 'wgs-84-3d'}).height")).isEqualTo(3.0);
  }

  @Test
  void xyWinsOverLongitudeLatitude() {
    assertThat(value("point({x: 1, y: 2, latitude: 3, longitude: 4}).crs")).isEqualTo("cartesian");
    assertThat(value("point({x: 1, y: 2, latitude: 3, longitude: 4}).x")).isEqualTo(1.0);
  }

  @Test
  void nonFiniteCoordinatesAreRefused() {
    assertRefused("point({x: 1/0.0, y: 2}).x", "non-finite");
    assertRefused("point({x: 0.0/0.0, y: 2}).x", "non-finite");
    assertRefused("point({longitude: 1/0.0, latitude: 2})", "non-finite");
    assertRefused("point({x: 1, y: 2, z: -1/0.0})", "non-finite");
    assertRefused("point.distance(point({x: 1/0.0, y: 2}), point({x: 0, y: 0}))", "non-finite");
  }

  @Test
  void unknownCrsOrSridIsRefused() {
    assertRefused("point({x: 1, y: 2, crs: 'foo'}).crs", "Unknown coordinate reference system: foo");
    assertRefused("point({x: 1, y: 2, srid: 1234}).srid", "Unknown coordinate reference system: code=1234");
  }

  @Test
  void incompleteMapIsRefused() {
    assertRefused("point({x: 1})", "is not describing a valid point");
    assertRefused("point({latitude: 1})", "is not describing a valid point");
    assertThat(value("point({x: null, y: 2})")).isNull();
  }

  @Test
  void missingCoordinateFieldIsRefused() {
    assertRefused("point({x: 1, y: 2}).z", "Field: z is not available on point");
    assertRefused("point({x: 1, y: 2}).longitude", "Field: longitude is not available on point");
    assertRefused("point({longitude: 1, latitude: 2}).height", "Field: height is not available on point");
    assertThatThrownBy(() -> database.query("opencypher", "WITH point({x: 1, y: 2}) AS p RETURN p.z AS v").next())
        .hasMessageContaining("Field: z is not available on point");
    // A field that is not a coordinate, and a map that is not a point, still read as null
    assertThat(value("point({x: 1, y: 2}).foo")).isNull();
    assertThat(value("{x: 1, y: 2}.z")).isNull();
    assertThat(value("point({x: 1, y: 2, z: 3}).z")).isEqualTo(3.0);
  }
}
