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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GeoPredicateFunctionsTest {

  private static final String POLYGON_0_10 = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
  private static final String POINT_INSIDE  = "POINT (5 5)";
  private static final String POINT_OUTSIDE = "POINT (15 15)";

  // ─── geo.within ───────────────────────────────────────────────────────────────

  @Nested
  class Within {

    @Test
    void sql_pointInsidePolygon_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.within('POINT (5 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_pointOutsidePolygon_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.within('POINT (15 15)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_pointInside_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoWithin()
          .execute(null, null, null, new Object[] { POINT_INSIDE, POLYGON_0_10 }, null)).isTrue();
    }

    @Test
    void javaExecute_pointOutside_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoWithin()
          .execute(null, null, null, new Object[] { POINT_OUTSIDE, POLYGON_0_10 }, null)).isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.within(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        final Object val = r.next().getProperty("v");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoWithin()
          .execute(null, null, null, new Object[] { POINT_INSIDE, null }, null)).isNull();
    }

    @Test
    void nullFirstArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoWithin()
          .execute(null, null, null, new Object[] { null, POLYGON_0_10 }, null)).isNull();
    }
  }

  // ─── geo.intersects ───────────────────────────────────────────────────────────

  @Nested
  class Intersects {

    @Test
    void sql_overlappingPolygons_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.intersects('POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))', 'POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_disjointPolygons_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.intersects('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_overlapping_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoIntersects()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))", "POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))" }, null))
          .isTrue();
    }

    @Test
    void javaExecute_disjoint_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoIntersects()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", "POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))" }, null))
          .isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.intersects(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        final Object val = r.next().getProperty("v");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoIntersects()
          .execute(null, null, null, new Object[] { POLYGON_0_10, null }, null)).isNull();
    }
  }

  // ─── geo.contains ─────────────────────────────────────────────────────────────

  @Nested
  class Contains {

    @Test
    void sql_polygonContainsPoint_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.contains('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'POINT (5 5)') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_polygonDoesNotContainOutsidePoint_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.contains('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'POINT (15 15)') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_contains_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoContains()
          .execute(null, null, null, new Object[] { POLYGON_0_10, POINT_INSIDE }, null)).isTrue();
    }

    @Test
    void javaExecute_doesNotContain_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoContains()
          .execute(null, null, null, new Object[] { POLYGON_0_10, POINT_OUTSIDE }, null)).isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.contains(null, 'POINT (5 5)') as v");
        final Object val = r.next().getProperty("v");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoContains()
          .execute(null, null, null, new Object[] { POLYGON_0_10, null }, null)).isNull();
    }
  }

  // ─── geo.dWithin ──────────────────────────────────────────────────────────────

  @Nested
  class DWithin {

    @Test
    void sql_nearbyPoints_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.dWithin('POINT (0 0)', 'POINT (1 1)', 2.0) as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_farPoints_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.dWithin('POINT (0 0)', 'POINT (10 10)', 1.0) as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_nearby_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoDWithin()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 1)", 2.0 }, null)).isTrue();
    }

    @Test
    void javaExecute_farAway_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoDWithin()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (10 10)", 1.0 }, null)).isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.dWithin(null, 'POINT (1 1)', 2.0) as v");
        final Object val = r.next().getProperty("v");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullThirdArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoDWithin()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 1)", null }, null)).isNull();
    }

    @Test
    void negativeDistance_execute_returnsFalse() {
      // No real distance is <= a negative threshold
      assertThat((Boolean) new SQLFunctionGeoDWithin()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 0)", -1.0 }, null)).isFalse();
    }
  }

  // ─── geo.disjoint ─────────────────────────────────────────────────────────────

  @Nested
  class Disjoint {

    @Test
    void sql_farApartShapes_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.disjoint('POINT (50 50)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_intersectingShapes_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.disjoint('POINT (5 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_disjoint_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoDisjoint()
          .execute(null, null, null, new Object[] { "POINT (50 50)", POLYGON_0_10 }, null)).isTrue();
    }

    @Test
    void javaExecute_intersecting_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoDisjoint()
          .execute(null, null, null, new Object[] { POINT_INSIDE, POLYGON_0_10 }, null)).isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.disjoint(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        final Object val = r.next().getProperty("v");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoDisjoint()
          .execute(null, null, null, new Object[] { POINT_INSIDE, null }, null)).isNull();
    }
  }

  // ─── geo.equals ───────────────────────────────────────────────────────────────

  @Nested
  class Equals {

    @Test
    void sql_identicalPoints_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.equals('POINT (5 5)', 'POINT (5 5)') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_differentPoints_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.equals('POINT (5 5)', 'POINT (6 6)') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_identicalPoints_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoEquals()
          .execute(null, null, null, new Object[] { "POINT (5 5)", "POINT (5 5)" }, null)).isTrue();
    }

    @Test
    void javaExecute_differentPoints_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoEquals()
          .execute(null, null, null, new Object[] { "POINT (5 5)", "POINT (6 6)" }, null)).isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.equals(null, 'POINT (5 5)') as v");
        final Object val = r.next().getProperty("v");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoEquals()
          .execute(null, null, null, new Object[] { "POINT (5 5)", null }, null)).isNull();
    }
  }

  // ─── geo.crosses ──────────────────────────────────────────────────────────────

  @Nested
  class Crosses {

    @Test
    void sql_lineCrossesPolygon_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.crosses('LINESTRING (-1 5, 11 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void javaExecute_lineCrossesPolygon_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoCrosses()
          .execute(null, null, null,
              new Object[] { "LINESTRING (-1 5, 11 5)", POLYGON_0_10 }, null)).isTrue();
    }

    @Test
    void javaExecute_overlappingPolygons_returnsFalse() {
      // Two polygons of the same dimension cannot "cross" by the JTS DE-9IM definition
      assertThat((Boolean) new SQLFunctionGeoCrosses()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))", "POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))" }, null))
          .isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.crosses(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        final Object val = r.next().getProperty("v");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoCrosses()
          .execute(null, null, null, new Object[] { "LINESTRING (-1 5, 11 5)", null }, null)).isNull();
    }
  }

  // ─── geo.overlaps ─────────────────────────────────────────────────────────────

  @Nested
  class Overlaps {

    @Test
    void sql_partiallyOverlapping_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.overlaps('POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))', 'POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_disjointPolygons_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.overlaps('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_overlapping_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoOverlaps()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))", "POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))" }, null))
          .isTrue();
    }

    @Test
    void javaExecute_disjoint_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoOverlaps()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", "POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))" }, null))
          .isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.overlaps(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        final Object val = r.next().getProperty("v");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoOverlaps()
          .execute(null, null, null, new Object[] { POLYGON_0_10, null }, null)).isNull();
    }
  }

  // ─── geo.touches ──────────────────────────────────────────────────────────────

  @Nested
  class Touches {

    @Test
    void sql_adjacentPolygons_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.touches('POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))', 'POLYGON ((5 0, 10 0, 10 5, 5 5, 5 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_disjointPolygons_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.touches('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_adjacent_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoTouches()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((5 0, 10 0, 10 5, 5 5, 5 0))" }, null))
          .isTrue();
    }

    @Test
    void javaExecute_disjoint_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoTouches()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", "POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))" }, null))
          .isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.touches(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        final Object val = r.next().getProperty("v");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoTouches()
          .execute(null, null, null, new Object[] { POLYGON_0_10, null }, null)).isNull();
    }
  }
}
