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
import org.locationtech.spatial4j.shape.Shape;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeoConstructionFunctionsTest {

  // ─── geo.geomFromText ─────────────────────────────────────────────────────────

  @Nested
  class GeomFromText {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.geomFromText('POINT (10 20)') as geom");
        assertThat(result.hasNext()).isTrue();
        final Object geom = result.next().getProperty("geom");
        assertThat(geom).isNotNull();
        assertThat(geom).isInstanceOf(Shape.class);
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Object result = new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      assertThat(result).isNotNull();
      assertThat(result).isInstanceOf(Shape.class);
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.geomFromText(null) as geom");
        assertThat(result.hasNext()).isTrue();
        final Object geom = result.next().getProperty("geom");
        assertThat(geom).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void noArgs_execute_returnsNull() {
      assertThat(new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[0], null)).isNull();
    }

    @Test
    void invalidWkt_execute_throwsException() {
      assertThatThrownBy(() -> new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "NOT VALID WKT ###" }, null))
          .isInstanceOf(Exception.class);
    }

    @Test
    void emptyString_execute_returnsNull() {
      // GeoUtils.parseGeometry returns null for empty strings rather than throwing
      assertThat(new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "" }, null)).isNull();
    }
  }

  // ─── geo.point ────────────────────────────────────────────────────────────────

  @Nested
  class Point {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.point(10, 20) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POINT");
        assertThat(wkt).contains("10");
        assertThat(wkt).contains("20");
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Object result = new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { 10.0, 20.0 }, null);
      assertThat(result).isNotNull();
      assertThat(result).isInstanceOf(String.class);
      assertThat((String) result).startsWith("POINT");
      assertThat((String) result).contains("10");
      assertThat((String) result).contains("20");
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.point(null, 20) as wkt");
        assertThat(result.hasNext()).isTrue();
        final Object wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNull();
      });
    }

    @Test
    void nullFirstArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { null, 20.0 }, null)).isNull();
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { 10.0, null }, null)).isNull();
    }

    @Test
    void tooFewArgs_execute_returnsNull() {
      assertThat(new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { 10.0 }, null)).isNull();
    }

    @Test
    void noArgs_execute_returnsNull() {
      assertThat(new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[0], null)).isNull();
    }
  }

  // ─── geo.lineString ───────────────────────────────────────────────────────────

  @Nested
  class LineString {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.lineString([[0,0],[10,10],[20,0]]) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("LINESTRING");
        assertThat(wkt).contains("0 0");
        assertThat(wkt).contains("10 10");
        assertThat(wkt).contains("20 0");
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final List<List<Object>> coords = List.of(
          List.of(0.0, 0.0),
          List.of(10.0, 10.0),
          List.of(20.0, 0.0));
      final Object result = new SQLFunctionGeoLineString()
          .execute(null, null, null, new Object[] { coords }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("LINESTRING");
      assertThat((String) result).contains("0 0");
      assertThat((String) result).contains("10 10");
      assertThat((String) result).contains("20 0");
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.lineString(null) as wkt");
        assertThat(result.hasNext()).isTrue();
        final Object wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoLineString()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void emptyList_execute_returnsNull() {
      assertThat(new SQLFunctionGeoLineString()
          .execute(null, null, null, new Object[] { List.of() }, null)).isNull();
    }

    @Test
    void invalidListElement_execute_throwsException() {
      final List<Object> badCoords = List.of("not_a_coordinate");
      assertThatThrownBy(() -> new SQLFunctionGeoLineString()
          .execute(null, null, null, new Object[] { badCoords }, null))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void singlePoint_execute_returnsLineString() {
      // Degenerate but valid from the function's perspective — just formats the coord
      final List<List<Object>> single = List.of(List.of(0.0, 0.0));
      final Object result = new SQLFunctionGeoLineString()
          .execute(null, null, null, new Object[] { single }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("LINESTRING");
    }
  }

  // ─── geo.polygon ──────────────────────────────────────────────────────────────

  @Nested
  class Polygon {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.polygon([[0,0],[10,0],[10,10],[0,10],[0,0]]) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POLYGON");
        assertThat(wkt).contains("0 0");
        assertThat(wkt).contains("10 0");
        assertThat(wkt).contains("10 10");
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final List<List<Object>> ring = List.of(
          List.of(0.0, 0.0), List.of(10.0, 0.0),
          List.of(10.0, 10.0), List.of(0.0, 10.0),
          List.of(0.0, 0.0));
      final Object result = new SQLFunctionGeoPolygon()
          .execute(null, null, null, new Object[] { ring }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("POLYGON");
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.polygon(null) as wkt");
        assertThat(result.hasNext()).isTrue();
        final Object wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoPolygon()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void openRing_sql_autoClose() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.polygon([[0,0],[10,0],[10,10],[0,10]]) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POLYGON");
        final String inner = wkt.substring(wkt.indexOf("((") + 2, wkt.lastIndexOf("))"));
        final String[] coords = inner.split(",");
        assertThat(coords[0].trim()).isEqualTo(coords[coords.length - 1].trim());
      });
    }

    @Test
    void openRing_execute_autoClose() {
      // Ring without closing coord — function should auto-close it
      final List<List<Object>> openRing = List.of(
          List.of(0.0, 0.0), List.of(10.0, 0.0),
          List.of(10.0, 10.0), List.of(0.0, 10.0));
      final String wkt = (String) new SQLFunctionGeoPolygon()
          .execute(null, null, null, new Object[] { openRing }, null);
      assertThat(wkt).isNotNull();
      assertThat(wkt).startsWith("POLYGON");
      final String inner = wkt.substring(wkt.indexOf("((") + 2, wkt.lastIndexOf("))"));
      final String[] coords = inner.split(",");
      assertThat(coords[0].trim()).isEqualTo(coords[coords.length - 1].trim());
    }

    @Test
    void invalidListElement_execute_throwsException() {
      final List<Object> badRing = List.of("not_a_coordinate");
      assertThatThrownBy(() -> new SQLFunctionGeoPolygon()
          .execute(null, null, null, new Object[] { badRing }, null))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
