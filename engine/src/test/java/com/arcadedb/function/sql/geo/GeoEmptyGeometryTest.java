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
package com.arcadedb.function.sql.geo;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code geo.*} functions answer an EMPTY geometry the way PostGIS and GeoJSON do instead of with an exception or NaN
 * (issue #12277): no GeoJSON position, NULL for a distance or a coordinate, an empty envelope. Also the deprecated
 * {@code rectangle()} reads its two latitudes in either order.
 */
class GeoEmptyGeometryTest extends TestHelper {
  private static final String SQUARE = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";

  @Test
  void geoJsonOfAnEmptyGeometryHasNoPosition() {
    assertThat(value("SELECT geo.asGeoJson('POINT EMPTY') AS v")).isEqualTo("{\"type\":\"Point\",\"coordinates\":[]}");
    assertThat(value("SELECT geo.asGeoJson('LINESTRING EMPTY') AS v")).isEqualTo(
        "{\"type\":\"LineString\",\"coordinates\":[]}");
    assertThat(value("SELECT geo.asGeoJson('POLYGON EMPTY') AS v")).isEqualTo("{\"type\":\"Polygon\",\"coordinates\":[]}");
    assertThat(value("SELECT geo.asGeoJson('MULTIPOINT EMPTY') AS v")).isEqualTo(
        "{\"type\":\"MultiPoint\",\"coordinates\":[]}");
    assertThat(value("SELECT geo.asGeoJson('MULTIPOINT ((1 2), EMPTY)') AS v")).isEqualTo(
        "{\"type\":\"MultiPoint\",\"coordinates\":[[1.0,2.0],[]]}");
    // a parsed empty point, not only its text
    assertThat(value("SELECT geo.asGeoJson(geo.geomFromText('POINT EMPTY')) AS v")).isEqualTo(
        "{\"type\":\"Point\",\"coordinates\":[]}");
  }

  @Test
  void anEmptyGeometryHasNoDistanceAndNoCoordinates() {
    assertThat(value("SELECT geo.distance('POINT EMPTY', 'POINT (1 1)') AS v")).isNull();
    assertThat(value("SELECT geo.distance('POINT (1 1)', 'POINT EMPTY', 'km') AS v")).isNull();
    assertThat(value("SELECT geo.distance('LINESTRING EMPTY', 'POINT (1 1)') AS v")).isNull();
    assertThat(value("SELECT geo.distance(?, 'POLYGON EMPTY') AS v", SQUARE)).isNull();
    assertThat(value("SELECT geo.x('POINT EMPTY') AS v")).isNull();
    assertThat(value("SELECT geo.y('POINT EMPTY') AS v")).isNull();
    assertThat(value("SELECT geo.x(geo.geomFromText('POINT EMPTY')) AS v")).isNull();

    assertThat(value("SELECT geo.dWithin('POINT EMPTY', 'POINT (1 1)', 1000) AS v")).isEqualTo(false);
    assertThat(value("SELECT geo.dWithin('LINESTRING EMPTY', 'POINT (1 1)', 1000) AS v")).isEqualTo(false);
    assertThat(value("SELECT geo.dWithin(?, 'POLYGON EMPTY', 1000) AS v", SQUARE)).isEqualTo(false);

    // non-empty answers are unchanged
    assertThat(((Number) value("SELECT geo.distance('POINT (0 0)', 'POINT (1 0)', 'km') AS v")).doubleValue()).isBetween(111.0,
        111.4);
    assertThat(value("SELECT geo.x('POINT (3 4)') AS v")).isEqualTo(3.0);
    assertThat(value("SELECT geo.y('POINT (3 4)') AS v")).isEqualTo(4.0);
  }

  @Test
  void theEnvelopeOfAnEmptyGeometryIsEmpty() {
    assertThat(value("SELECT geo.envelope('POINT EMPTY') AS v")).isEqualTo("POLYGON EMPTY");
    assertThat(value("SELECT geo.envelope('LINESTRING EMPTY') AS v")).isEqualTo("POLYGON EMPTY");
    assertThat(value("SELECT geo.envelope('POLYGON EMPTY') AS v")).isEqualTo("POLYGON EMPTY");
    assertThat(value("SELECT geo.area(geo.envelope('POINT EMPTY')) AS v")).isEqualTo(0.0);
    assertThat(value("SELECT geo.envelope('POINT (1 2)') AS v")).isEqualTo("POLYGON ((1 2, 1 2, 1 2, 1 2, 1 2))");
  }

  @Test
  void rectangleReadsItsLatitudesInEitherOrder() {
    // (left-x, top-y, right-x, bottom-y), as documented
    assertThat(value("SELECT geo.within('POINT (5 5)', rectangle(0, 10, 10, 0)) AS v")).isEqualTo(true);
    // (minX, minY, maxX, maxY), as the documentation's own example and the implementation read it
    assertThat(value("SELECT geo.within('POINT (15 15)', rectangle(10, 10, 20, 20)) AS v")).isEqualTo(true);
    assertThat(value("SELECT geo.equals(rectangle(0, 10, 10, 0), rectangle(0, 0, 10, 10)) AS v")).isEqualTo(true);
    assertThat(value("SELECT geo.within('POINT (15 5)', rectangle(0, 10, 10, 0)) AS v")).isEqualTo(false);
  }

  private Object value(final String sql, final Object... args) {
    try (final ResultSet result = database.query("sql", sql, args)) {
      return result.next().getProperty("v");
    }
  }
}
