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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeoMeasurementFunctionsTest {

  // ─── geo.buffer ───────────────────────────────────────────────────────────────

  @Nested
  class Buffer {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.buffer('POINT (10 20)', 1.0) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POLYGON");
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Object result = new SQLFunctionGeoBuffer()
          .execute(null, null, null, new Object[] { "POINT (10 20)", 1.0 }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("POLYGON");
    }

    @Test
    void nullGeometry_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.buffer(null, 1.0) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNull();
      });
    }

    @Test
    void nullGeometry_execute_returnsNull() {
      assertThat(new SQLFunctionGeoBuffer()
          .execute(null, null, null, new Object[] { null, 1.0 }, null)).isNull();
    }

    @Test
    void nullDistance_execute_returnsNull() {
      assertThat(new SQLFunctionGeoBuffer()
          .execute(null, null, null, new Object[] { "POINT (10 20)", null }, null)).isNull();
    }

    @Test
    void invalidGeometry_execute_throwsException() {
      assertThatThrownBy(() -> new SQLFunctionGeoBuffer()
          .execute(null, null, null, new Object[] { "NOT VALID WKT", 1.0 }, null))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  // ─── geo.distance ─────────────────────────────────────────────────────────────

  @Nested
  class Distance {

    @Test
    void sqlHappyPath_meters() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.distance('POINT (0 0)', 'POINT (1 0)') as dist");
        assertThat(result.hasNext()).isTrue();
        final Double dist = result.next().getProperty("dist");
        assertThat(dist).isNotNull();
        assertThat(dist).isGreaterThan(0.0);
      });
    }

    @Test
    void sqlHappyPath_km_lessThanMeters() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet rMeters = db.query("sql", "select geo.distance('POINT (0 0)', 'POINT (1 0)') as dist");
        final Double distM = rMeters.next().getProperty("dist");
        final ResultSet rKm = db.query("sql", "select geo.distance('POINT (0 0)', 'POINT (1 0)', 'km') as dist");
        final Double distKm = rKm.next().getProperty("dist");

        assertThat(distKm).isGreaterThan(0.0);
        assertThat(distKm).isLessThan(distM);
      });
    }

    @Test
    void javaExecuteHappyPath_meters() {
      final Double dist = (Double) new SQLFunctionGeoDistance()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 0)" }, null);
      assertThat(dist).isNotNull();
      assertThat(dist).isGreaterThan(0.0);
    }

    @Test
    void javaExecuteHappyPath_miles() {
      final Double distMi = (Double) new SQLFunctionGeoDistance()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 0)", "mi" }, null);
      assertThat(distMi).isNotNull();
      assertThat(distMi).isGreaterThan(0.0);
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.distance(null, 'POINT (1 0)') as dist");
        assertThat(result.hasNext()).isTrue();
        final Double dist = result.next().getProperty("dist");
        assertThat(dist).isNull();
      });
    }

    @Test
    void nullFirstArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoDistance()
          .execute(null, null, null, new Object[] { null, "POINT (1 0)" }, null)).isNull();
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoDistance()
          .execute(null, null, null, new Object[] { "POINT (0 0)", null }, null)).isNull();
    }

    @Test
    void invalidUnit_execute_throwsIllegalArgument() {
      assertThatThrownBy(() -> new SQLFunctionGeoDistance()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 0)", "lightyear" }, null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("lightyear");
    }

    @Test
    void allSupportedUnits_execute_returnPositive() {
      for (final String unit : new String[] { "m", "km", "mi", "nmi" }) {
        final Double d = (Double) new SQLFunctionGeoDistance()
            .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 0)", unit }, null);
        assertThat(d).as("unit=%s", unit).isGreaterThan(0.0);
      }
    }
  }

  // ─── geo.area ─────────────────────────────────────────────────────────────────

  @Nested
  class Area {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.area('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as area");
        assertThat(result.hasNext()).isTrue();
        final Double area = result.next().getProperty("area");
        assertThat(area).isNotNull();
        assertThat(area).isGreaterThan(0.0);
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Double area = (Double) new SQLFunctionGeoArea()
          .execute(null, null, null, new Object[] { "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))" }, null);
      assertThat(area).isNotNull();
      assertThat(area).isGreaterThan(0.0);
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.area(null) as area");
        assertThat(result.hasNext()).isTrue();
        final Double area = result.next().getProperty("area");
        assertThat(area).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoArea()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void pointGeometry_execute_returnsZero() {
      // A point has no area
      final Double area = (Double) new SQLFunctionGeoArea()
          .execute(null, null, null, new Object[] { "POINT (5 5)" }, null);
      assertThat(area).isNotNull();
      assertThat(area).isEqualTo(0.0);
    }
  }

  // ─── geo.envelope ─────────────────────────────────────────────────────────────

  @Nested
  class Envelope {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.envelope('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POLYGON");
        assertThat(wkt).contains("0 0");
        assertThat(wkt).contains("10 10");
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Object result = new SQLFunctionGeoEnvelope()
          .execute(null, null, null, new Object[] { "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))" }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("POLYGON");
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.envelope(null) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoEnvelope()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void invalidWkt_execute_throwsException() {
      assertThatThrownBy(() -> new SQLFunctionGeoEnvelope()
          .execute(null, null, null, new Object[] { "NOT VALID WKT" }, null))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pointEnvelope_execute_returnsResult() {
      // Envelope of a point is degenerate but should not crash
      final Object result = new SQLFunctionGeoEnvelope()
          .execute(null, null, null, new Object[] { "POINT (5 5)" }, null);
      assertThat(result).isNotNull();
    }
  }
}
