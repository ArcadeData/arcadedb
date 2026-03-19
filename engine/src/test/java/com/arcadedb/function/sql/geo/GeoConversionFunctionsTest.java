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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Shape;

import static org.assertj.core.api.Assertions.assertThat;

class GeoConversionFunctionsTest {

  // ─── geo.asText ───────────────────────────────────────────────────────────────

  @Nested
  class AsText {

    @Test
    void sqlStringInput_returnedAsIs() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.asText('POINT (10 20)') as wkt");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().<String>getProperty("wkt")).isEqualTo("POINT (10 20)");
      });
    }

    @Test
    void sqlShapeInput_returnsWkt() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.asText(geo.geomFromText('POINT (10 20)')) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POINT");
        assertThat(wkt).contains("10");
        assertThat(wkt).contains("20");
      });
    }

    @Test
    void javaExecute_stringInput_returnedAsIs() {
      final Object result = new SQLFunctionGeoAsText()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      assertThat(result).isEqualTo("POINT (10 20)");
    }

    @Test
    void javaExecute_shapeInput_returnsWkt() {
      final Shape shape = (Shape) new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      final Object result = new SQLFunctionGeoAsText()
          .execute(null, null, null, new Object[] { shape }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("POINT");
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.asText(null) as wkt");
        assertThat(result.hasNext()).isTrue();
        final Object val = result.next().getProperty("wkt");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoAsText()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }
  }

  // ─── geo.asGeoJson ────────────────────────────────────────────────────────────

  @Nested
  class AsGeoJson {

    @Test
    void sqlPoint_containsPointType() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.asGeoJson('POINT (10 20)') as json");
        assertThat(result.hasNext()).isTrue();
        final String json = result.next().getProperty("json");
        assertThat(json).isNotNull();
        assertThat(json).contains("Point");
        assertThat(json).contains("coordinates");
        assertThat(json).contains("10");
        assertThat(json).contains("20");
      });
    }

    @Test
    void sqlPolygon_containsPolygonType() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.asGeoJson('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as json");
        assertThat(result.hasNext()).isTrue();
        final String json = result.next().getProperty("json");
        assertThat(json).isNotNull();
        assertThat(json).contains("Polygon");
        assertThat(json).contains("coordinates");
      });
    }

    @Test
    void javaExecute_point_containsPointType() {
      final Object result = new SQLFunctionGeoAsGeoJson()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).contains("Point");
      assertThat((String) result).contains("coordinates");
    }

    @Test
    void javaExecute_lineString_containsLineStringType() {
      final Object result = new SQLFunctionGeoAsGeoJson()
          .execute(null, null, null, new Object[] { "LINESTRING (0 0, 10 10, 20 0)" }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).contains("LineString");
      assertThat((String) result).contains("coordinates");
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.asGeoJson(null) as json");
        assertThat(result.hasNext()).isTrue();
        final Object val = result.next().getProperty("json");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoAsGeoJson()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }
  }

  // ─── geo.x ────────────────────────────────────────────────────────────────────

  @Nested
  class X {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.x('POINT (10 20)') as x");
        assertThat(result.hasNext()).isTrue();
        final Double x = result.next().getProperty("x");
        assertThat(x).isNotNull();
        assertThat(x).isEqualTo(10.0);
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Double x = (Double) new SQLFunctionGeoX()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      assertThat(x).isNotNull();
      assertThat(x).isEqualTo(10.0);
    }

    @Test
    void javaExecute_shapeInput() {
      final Shape shape = (Shape) new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      final Double x = (Double) new SQLFunctionGeoX()
          .execute(null, null, null, new Object[] { shape }, null);
      assertThat(x).isNotNull();
      assertThat(x).isEqualTo(10.0);
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.x(null) as x");
        assertThat(result.hasNext()).isTrue();
        final Object val = result.next().getProperty("x");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoX()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void polygonInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.x('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as x");
        assertThat(result.hasNext()).isTrue();
        final Object val = result.next().getProperty("x");
        assertThat(val).isNull();
      });
    }

    @Test
    void polygonInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoX()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))" }, null)).isNull();
    }

    @Test
    void invalidWkt_execute_returnsNullSilently() {
      // geo.x silently swallows parse errors and returns null
      assertThat(new SQLFunctionGeoX()
          .execute(null, null, null, new Object[] { "NOT_WKT_AT_ALL" }, null)).isNull();
    }

    @Test
    void roundTrip_pointXMatches() {
      final String wkt = (String) new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { 42.5, -7.3 }, null);
      final Double x = (Double) new SQLFunctionGeoX()
          .execute(null, null, null, new Object[] { wkt }, null);
      assertThat(x).isNotNull();
      assertThat(x).isCloseTo(42.5, Offset.offset(1e-6));
    }
  }

  // ─── geo.y ────────────────────────────────────────────────────────────────────

  @Nested
  class Y {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.y('POINT (10 20)') as y");
        assertThat(result.hasNext()).isTrue();
        final Double y = result.next().getProperty("y");
        assertThat(y).isNotNull();
        assertThat(y).isEqualTo(20.0);
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Double y = (Double) new SQLFunctionGeoY()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      assertThat(y).isNotNull();
      assertThat(y).isEqualTo(20.0);
    }

    @Test
    void javaExecute_shapeInput() {
      final Shape shape = (Shape) new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      final Double y = (Double) new SQLFunctionGeoY()
          .execute(null, null, null, new Object[] { shape }, null);
      assertThat(y).isNotNull();
      assertThat(y).isEqualTo(20.0);
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.y(null) as y");
        assertThat(result.hasNext()).isTrue();
        final Object val = result.next().getProperty("y");
        assertThat(val).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoY()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void polygonInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.y('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as y");
        assertThat(result.hasNext()).isTrue();
        final Object val = result.next().getProperty("y");
        assertThat(val).isNull();
      });
    }

    @Test
    void polygonInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoY()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))" }, null)).isNull();
    }

    @Test
    void invalidWkt_execute_returnsNullSilently() {
      assertThat(new SQLFunctionGeoY()
          .execute(null, null, null, new Object[] { "NOT_WKT_AT_ALL" }, null)).isNull();
    }

    @Test
    void roundTrip_pointYMatches() {
      final String wkt = (String) new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { 42.5, -7.3 }, null);
      final Double y = (Double) new SQLFunctionGeoY()
          .execute(null, null, null, new Object[] { wkt }, null);
      assertThat(y).isNotNull();
      assertThat(y).isCloseTo(-7.3, Offset.offset(1e-6));
    }
  }
}
