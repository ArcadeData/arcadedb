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
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
  void anEmptyCircleIsAtNoDistance() {
    // a circle is empty when its centre is: the guard in distanceDegrees covers the shapes that wrap another one too
    final Shape emptyCircle = GeoUtils.getSpatialContext().getShapeFactory().circle((Point) GeoUtils.parseGeometry("POINT EMPTY"), 1.0);
    final Shape point = GeoUtils.parseGeometry("POINT (1 1)");

    assertThat(emptyCircle.isEmpty()).isTrue();
    assertThat(GeoUtils.distanceDegrees(emptyCircle, point)).isNaN();
    assertThat(GeoUtils.distanceDegrees(point, emptyCircle)).isNaN();
    assertThat(new SQLFunctionGeoDistance().execute(null, null, null, new Object[] { emptyCircle, point }, null)).isNull();
  }

  @Test
  void anEmptyPointIsNoVertexOfALineOrARing() {
    // an EMPTY point has no position: it is refused as any other point that is not one, instead of written as NaN
    assertThatThrownBy(() -> value("SELECT geo.lineString([geo.geomFromText('POINT EMPTY'), 'POINT (1 1)']) AS v")).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("Invalid point element");
    assertThatThrownBy(() -> value("SELECT geo.lineString(['POINT (1 1)', 'POINT EMPTY']) AS v")).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("Invalid point element");
    assertThatThrownBy(() -> value("SELECT geo.polygon(['POINT (0 0)', 'POINT (10 0)', 'POINT EMPTY']) AS v")).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("Invalid point element");
    assertThatThrownBy(() -> value("SELECT geo.polygon([geo.geomFromText('POINT EMPTY'), 'POINT (10 0)', 'POINT (10 10)']) AS v"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid point element");

    // non-empty answers are unchanged
    assertThat(value("SELECT geo.lineString(['POINT (0 0)', geo.geomFromText('POINT (1 1)')]) AS v")).isEqualTo("LINESTRING (0 0, 1 1)");
    assertThat(value("SELECT geo.polygon([[0, 0], [10, 0], [10, 10]]) AS v")).isEqualTo("POLYGON ((0 0, 10 0, 10 10, 0 0))");
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
