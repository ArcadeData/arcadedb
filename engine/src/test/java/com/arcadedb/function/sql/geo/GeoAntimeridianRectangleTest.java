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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * A rectangle that crosses the antimeridian stays the rectangle, not the 340-degree band between its two meridians
 * (issue #12270): {@code geo.envelope()} of a shape across it is a box every {@code geo.*} function reads back as that box,
 * and the JTS-backed functions ({@code geo.crosses}, {@code geo.equals}, {@code geo.asGeoJson}, ...) see a crossing
 * {@code ENVELOPE} as its two parts either side of the antimeridian.
 */
class GeoAntimeridianRectangleTest extends TestHelper {
  /** 20 by 10 degrees across the antimeridian, split at 180. */
  private static final String M = "MULTIPOLYGON (((170 0, 180 0, 180 10, 170 10, 170 0)), ((-180 0, -170 0, -170 10, -180 10, -180 0)))";
  /** The same box as a Spatial4j rectangle: ENVELOPE(minX, maxX, maxY, minY). */
  private static final String E = "ENVELOPE(170, -170, 10, 0)";

  @Test
  void theEnvelopeOfAShapeAcrossTheAntimeridianReadsBackAsThatBox() {
    final String envelope = (String) value("SELECT geo.envelope(?) AS v", M);
    assertThat(envelope).isEqualTo(
        "MULTIPOLYGON (((170 0, 180 0, 180 10, 170 10, 170 0)), ((-180 0, -170 0, -170 10, -180 10, -180 0)))");

    assertThat(value("SELECT geo.within('POINT (175 5)', geo.envelope(?)) AS v", M)).isEqualTo(true);
    assertThat(value("SELECT geo.within('POINT (-175 5)', geo.envelope(?)) AS v", M)).isEqualTo(true);
    assertThat(value("SELECT geo.within('POINT (0 5)', geo.envelope(?)) AS v", M)).isEqualTo(false);
    assertThat(((Number) value("SELECT geo.area(geo.envelope(?)) AS v", M)).doubleValue()).isCloseTo(
        ((Number) value("SELECT geo.area(?) AS v", M)).doubleValue(), within(1e-9));
    // the envelope of the 170 .. 190 polygon, which is read split at 180, is the same box
    assertThat(value("SELECT geo.envelope('POLYGON ((170 0, 190 0, 190 10, 170 10, 170 0))') AS v")).isEqualTo(envelope);
  }

  @Test
  void anEnvelopeThatDoesNotCrossIsStillOnePolygon() {
    assertThat(value("SELECT geo.envelope('LINESTRING (2 48, 3 49.5)') AS v")).isEqualTo(
        "POLYGON ((2 48, 3 48, 3 49.5, 2 49.5, 2 48))");
    // the 340-degree band between 170 and -170 is not across the antimeridian
    assertThat(value("SELECT geo.envelope('POLYGON ((170 0, -170 0, -170 10, 170 10, 170 0))') AS v")).isEqualTo(
        "POLYGON ((-170 0, 170 0, 170 10, -170 10, -170 0))");
  }

  @Test
  void theJtsPredicatesSeeACrossingEnvelopeAsTheBox() {
    assertThat(value("SELECT geo.intersects('LINESTRING (0 -5, 0 5)', ?) AS v", E)).isEqualTo(false);
    assertThat(value("SELECT geo.crosses('LINESTRING (0 -5, 0 5)', ?) AS v", E)).isEqualTo(false);
    assertThat(value("SELECT geo.intersects('LINESTRING (175 -5, 175 5)', ?) AS v", E)).isEqualTo(true);
    assertThat(value("SELECT geo.crosses('LINESTRING (175 -5, 175 5)', ?) AS v", E)).isEqualTo(true);
    assertThat(value("SELECT geo.equals(?, ?) AS v", E, M)).isEqualTo(true);
    assertThat(value("SELECT geo.overlaps(?, 'POLYGON ((175 5, 179 5, 179 15, 175 15, 175 5))') AS v", E)).isEqualTo(true);
    assertThat(value("SELECT geo.overlaps(?, 'POLYGON ((0 5, 10 5, 10 15, 0 15, 0 5))') AS v", E)).isEqualTo(false);
    assertThat(value("SELECT geo.touches(?, 'POLYGON ((160 0, 170 0, 170 10, 160 10, 160 0))') AS v", E)).isEqualTo(true);
    // the rectangle as a parsed shape, not as text
    assertThat(value("SELECT geo.equals(geo.geomFromText(?), ?) AS v", E, M)).isEqualTo(true);
  }

  @Test
  void theGeoJsonOfACrossingEnvelopeIsSplitAtTheAntimeridian() {
    final JSONObject json = new JSONObject((String) value("SELECT geo.asGeoJson(?) AS v", E));
    assertThat(json.getString("type")).isEqualTo("MultiPolygon");
    assertThat(json.getJSONArray("coordinates").length()).isEqualTo(2);
    assertThat(json.getJSONArray("coordinates").getJSONArray(0).getJSONArray(0).toString()).isEqualTo(
        "[[170.0,0.0],[180.0,0.0],[180.0,10.0],[170.0,10.0],[170.0,0.0]]");
    assertThat(json.getJSONArray("coordinates").getJSONArray(1).getJSONArray(0).toString()).isEqualTo(
        "[[-180.0,0.0],[-170.0,0.0],[-170.0,10.0],[-180.0,10.0],[-180.0,0.0]]");

    // an envelope that does not cross is one polygon, as before
    final JSONObject plain = new JSONObject((String) value("SELECT geo.asGeoJson('ENVELOPE(0, 10, 10, 0)') AS v"));
    assertThat(plain.getString("type")).isEqualTo("Polygon");
  }

  @Test
  void aBufferOfACrossingEnvelopeStaysNearTheAntimeridian() {
    final String buffer = (String) value("SELECT geo.buffer(?, 1) AS v", E);
    assertThat(value("SELECT geo.within('POINT (175 5)', ?) AS v", buffer)).isEqualTo(true);
    assertThat(value("SELECT geo.within('POINT (0 5)', ?) AS v", buffer)).isEqualTo(false);
  }

  private Object value(final String sql, final Object... args) {
    try (final ResultSet result = database.query("sql", sql, args)) {
      return result.next().getProperty("v");
    }
  }
}
