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
import static org.assertj.core.api.Assertions.within;

/**
 * {@code geo.area()} is the area in square degrees on the longitude/latitude plane, as documented, for every kind of shape
 * (issue #12272). It used to be Spatial4j's geodetic estimate: the spherical area of the bounding box scaled by the planar
 * fraction of the box the shape fills.
 */
class GeoAreaSquareDegreesTest extends TestHelper {

  @Test
  void aSquareAtTheEquatorIsWidthTimesHeight() {
    assertThat(area("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")).isCloseTo(100.0, within(1e-9));
    assertThat(area("POLYGON ((0 88, 1 88, 1 89, 0 89, 0 88))")).isCloseTo(1.0, within(1e-9));
  }

  @Test
  void mirroredTrianglesHaveTheirOwnPlanarArea() {
    assertThat(area("POLYGON ((0 0, 10 0, 0 80, 0 0))")).isCloseTo(400.0, within(1e-9));
    assertThat(area("POLYGON ((0 80, 10 80, 0 0, 0 80))")).isCloseTo(400.0, within(1e-9));
    // half of its bounding box, which the old estimate could not tell from any other shape filling half of it
    assertThat(area("POLYGON ((0 0, 10 0, 10 80, 0 0))")).isCloseTo(400.0, within(1e-9));
  }

  @Test
  void aMultiPolygonIsTheSumOfItsParts() {
    final double parts = area("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))") + area("POLYGON ((0 88, 1 88, 1 89, 0 89, 0 88))");
    assertThat(area("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((0 88, 1 88, 1 89, 0 89, 0 88)))")).isCloseTo(parts,
        within(1e-9));
    assertThat(parts).isCloseTo(2.0, within(1e-9));
  }

  @Test
  void aPolygonWithAHoleLeavesTheHoleOut() {
    assertThat(area("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))")).isCloseTo(96.0, within(1e-9));
  }

  @Test
  void rectanglesAcrossTheAntimeridianAndCircles() {
    // ENVELOPE(minX, maxX, maxY, minY): 20 degrees across the antimeridian by 10
    assertThat(area("ENVELOPE (170, -170, 10, 0)")).isCloseTo(200.0, within(1e-9));
    // the same box written past 180, which is read split at the antimeridian
    assertThat(area("POLYGON ((170 0, 190 0, 190 10, 170 10, 170 0))")).isCloseTo(200.0, within(1e-9));
    assertThat(area("BUFFER (POINT (0 0), 2)")).isCloseTo(Math.PI * 4, within(1e-9));
  }

  @Test
  void linesAndPointsHaveNoArea() {
    assertThat(area("POINT (5 5)")).isEqualTo(0.0);
    assertThat(area("LINESTRING (0 0, 10 10)")).isEqualTo(0.0);
  }

  private double area(final String wkt) {
    try (final ResultSet result = database.query("sql", "SELECT geo.area(?) AS area", wkt)) {
      return ((Number) result.next().getProperty("area")).doubleValue();
    }
  }
}
