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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #12972: the haversine formula {@code 2 * atan2(sqrt(h), sqrt(1 - h))} answered NaN for the pairs of points near
 * the antipodes whose {@code h} rounds a few ULPs above 1, in {@code geo.distance}, {@code geo.dWithin} and Cypher
 * {@code point.distance}.
 */
class GeoAntipodalDistanceIssue12972Test {
  // a pair that answered NaN before the fix: lon -113.09, lat 2.6 and its antipode
  private static final String POINT_A = "POINT(-113.09 2.6)";
  private static final String POINT_B = "POINT(66.91 -2.6)";
  // half the circumference of the sphere geo.distance measures on: pi * 6371 km
  private static final double HALF_EARTH_METERS = Math.PI * GeoUtils.EARTH_RADIUS_KM * 1000;

  @Test
  void antipodesOfAWholeGridOfPointsAreHalfTheEarthApart() {
    // counted with primitives and asserted once: an AssertJ assertion per point would allocate millions of objects
    int measured = 0;
    int wrong = 0;
    double firstWrongX = 0;
    double firstWrongY = 0;
    double firstWrongRadians = 0;
    for (int lon = -18_000; lon < 18_000; lon += 7) {
      for (int lat = -9_000; lat <= 9_000; lat += 13) {
        final double x = lon / 100.0;
        final double y = lat / 100.0;
        final double radians = GeoUtils.haversineRadians(x, y, x > 0 ? x - 180 : x + 180, -y);
        // written so that NaN counts as wrong
        if (!(Math.abs(radians - Math.PI) <= 1e-6)) {
          if (wrong == 0) {
            firstWrongX = x;
            firstWrongY = y;
            firstWrongRadians = radians;
          }
          ++wrong;
        }
        ++measured;
      }
    }
    assertThat(measured).isGreaterThan(7_000_000);
    assertThat(wrong).as("antipodes not pi radians apart, the first at %s,%s answered %s", firstWrongX, firstWrongY,
        firstWrongRadians).isZero();
  }

  @Test
  void sqlDistanceOfAntipodalPointsIsHalfTheEarth() throws Exception {
    TestHelper.executeInNewDatabase("GeoAntipodalDistanceIssue12972Test", db -> {
      // geo.dWithin takes its radius in degrees of arc, and the antipodes are 180 of them apart
      try (final ResultSet rs = db.query("sql", "SELECT geo.distance('" + POINT_A + "', '" + POINT_B + "') AS d, "
          + "geo.dWithin('" + POINT_A + "', '" + POINT_B + "', 181) AS near, "
          + "geo.dWithin('" + POINT_A + "', '" + POINT_B + "', 179) AS far")) {
        final Result row = rs.next();
        assertThat(row.<Double>getProperty("d")).isCloseTo(HALF_EARTH_METERS, within(1.0));
        assertThat(row.<Boolean>getProperty("near")).isTrue();
        assertThat(row.<Boolean>getProperty("far")).isFalse();
      }
    });
  }

  @Test
  void cypherDistanceOfAntipodalPointsIsHalfTheEarth() throws Exception {
    TestHelper.executeInNewDatabase("GeoAntipodalDistanceIssue12972Test", db -> {
      try (final ResultSet rs = db.query("opencypher",
          "RETURN point.distance(point({longitude: -113.09, latitude: 2.6}), point({longitude: 66.91, latitude: -2.6})) AS d")) {
        final double distance = rs.next().<Number>getProperty("d").doubleValue();
        // point.distance uses the Neo4j earth radius (6378.14 km), not the 6371 km of geo.distance
        assertThat(distance).isCloseTo(Math.PI * 6_378_140.0, within(1.0));
      }
    });
  }
}
