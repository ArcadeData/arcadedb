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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Shape;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLGeoFunctionsTest {

  @Test
  void geoManualIndexPoints() throws Exception {
    final int TOTAL = 1_000;

    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {

      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType("Restaurant");
        type.createProperty("coords", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

        long begin = System.currentTimeMillis();

        for (int i = 0; i < TOTAL; i++) {
          final MutableDocument doc = db.newDocument("Restaurant");
          doc.set("lat", 10 + (0.01D * i));
          doc.set("long", 10 + (0.01D * i));
          doc.set("coords", GeohashUtils.encodeLatLon(doc.getDouble("lat"), doc.getDouble("long"))); // INDEXED
          doc.save();
        }

        final String[] area = new String[] { GeohashUtils.encodeLatLon(10.5, 10.5), GeohashUtils.encodeLatLon(10.55, 10.55) };

        begin = System.currentTimeMillis();
        ResultSet result = db.query("sql", "select from Restaurant where coords >= ? and coords <= ?", area[0], area[1]);

        assertThat(result.hasNext()).isTrue();
        int returned = 0;
        while (result.hasNext()) {
          final Document record = result.next().toElement();
          assertThat(record.getDouble("lat")).isGreaterThanOrEqualTo(10.5);
          assertThat(record.getDouble("long")).isLessThanOrEqualTo(10.55);
          ++returned;
        }

        assertThat(returned).isEqualTo(6);
      });
    });
  }

  @Test
  void geoManualIndexBoundingBoxes() throws Exception {
    final int TOTAL = 1_000;

    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {

      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType("Restaurant");
        type.createProperty("bboxTL", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
        type.createProperty("bboxBR", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

        long begin = System.currentTimeMillis();

        for (int i = 0; i < TOTAL; i++) {
          final MutableDocument doc = db.newDocument("Restaurant");
          doc.set("x1", 10D + (0.0001D * i));
          doc.set("y1", 10D + (0.0001D * i));
          doc.set("x2", 10D + (0.001D * i));
          doc.set("y2", 10D + (0.001D * i));
          doc.set("bboxTL", GeohashUtils.encodeLatLon(doc.getDouble("x1"), doc.getDouble("y1"))); // INDEXED
          doc.set("bboxBR", GeohashUtils.encodeLatLon(doc.getDouble("x2"), doc.getDouble("y2"))); // INDEXED
          doc.save();
        }

        for (Index idx : type.getAllIndexes(false)) {
          assertThat(idx.countEntries()).isEqualTo(TOTAL);
        }

        final String[] area = new String[] {//
            GeohashUtils.encodeLatLon(10.0001D, 10.0001D),//
            GeohashUtils.encodeLatLon(10.020D, 10.020D) };

        begin = System.currentTimeMillis();
        ResultSet result = db.query("sql", "select from Restaurant where bboxTL >= ? and bboxBR <= ?", area[0], area[1]);

        assertThat(result.hasNext()).isTrue();
        int returned = 0;
        while (result.hasNext()) {
          final Document record = result.next().toElement();
          assertThat(record.getDouble("x1")).isGreaterThanOrEqualTo(10.0001D).withFailMessage("x1: " + record.getDouble("x1"));
          assertThat(record.getDouble("y1")).isGreaterThanOrEqualTo(10.0001D).withFailMessage("y1: " + record.getDouble("y1"));
          assertThat(record.getDouble("x2")).isLessThanOrEqualTo(10.020D).withFailMessage("x2: " + record.getDouble("x2"));
          assertThat(record.getDouble("y2")).isLessThanOrEqualTo(10.020D).withFailMessage("y2: " + record.getDouble("y2"));
          ++returned;
        }

        assertThat(returned).isEqualTo(20);
      });
    });
  }

  // ─── ST_* standard function tests ────────────────────────────────────────────

  @Test
  void stGeomFromText() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Valid WKT point
      ResultSet result = db.query("sql", "select ST_GeomFromText('POINT (10 20)') as geom");
      assertThat(result.hasNext()).isTrue();
      final Object geom = result.next().getProperty("geom");
      assertThat(geom).isNotNull();
      assertThat(geom).isInstanceOf(Shape.class);
    });
  }

  @Test
  void stGeomFromTextNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_GeomFromText(null) as geom");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("geom");
      assertThat(val).isNull();
    });
  }

  @Test
  void stPoint() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Point(10, 20) as wkt");
      assertThat(result.hasNext()).isTrue();
      final String wkt = result.next().getProperty("wkt");
      assertThat(wkt).isNotNull();
      assertThat(wkt).startsWith("POINT");
      assertThat(wkt).contains("10");
      assertThat(wkt).contains("20");
    });
  }

  @Test
  void stPointNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Point(null, 20) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stLineString() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_LineString([[0,0],[10,10],[20,0]]) as wkt");
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
  void stLineStringNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_LineString(null) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stPolygon() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Closed ring
      ResultSet result = db.query("sql", "select ST_Polygon([[0,0],[10,0],[10,10],[0,10],[0,0]]) as wkt");
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
  void stPolygonAutoClose() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Open ring — should be auto-closed
      ResultSet result = db.query("sql", "select ST_Polygon([[0,0],[10,0],[10,10],[0,10]]) as wkt");
      assertThat(result.hasNext()).isTrue();
      final String wkt = result.next().getProperty("wkt");
      assertThat(wkt).isNotNull();
      assertThat(wkt).startsWith("POLYGON");
      // Ring is closed: last coord equals first
      final String inner = wkt.substring(wkt.indexOf("((") + 2, wkt.lastIndexOf("))"));
      final String[] coords = inner.split(",");
      assertThat(coords[0].trim()).isEqualTo(coords[coords.length - 1].trim());
    });
  }

  @Test
  void stPolygonNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Polygon(null) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stBuffer() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Buffer('POINT (10 20)', 1.0) as wkt");
      assertThat(result.hasNext()).isTrue();
      final String wkt = result.next().getProperty("wkt");
      assertThat(wkt).isNotNull();
      assertThat(wkt).startsWith("POLYGON");
    });
  }

  @Test
  void stBufferNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Buffer(null, 1.0) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stEnvelope() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
          "select ST_Envelope('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as wkt");
      assertThat(result.hasNext()).isTrue();
      final String wkt = result.next().getProperty("wkt");
      assertThat(wkt).isNotNull();
      assertThat(wkt).startsWith("POLYGON");
      assertThat(wkt).contains("0 0");
      assertThat(wkt).contains("10 10");
    });
  }

  @Test
  void stEnvelopeNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Envelope(null) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stDistance() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Distance between two points in meters (default unit)
      ResultSet result = db.query("sql", "select ST_Distance('POINT (0 0)', 'POINT (1 0)') as dist");
      assertThat(result.hasNext()).isTrue();
      final Double dist = result.next().getProperty("dist");
      assertThat(dist).isNotNull();
      assertThat(dist).isGreaterThan(0.0);

      // Distance in km
      result = db.query("sql", "select ST_Distance('POINT (0 0)', 'POINT (1 0)', 'km') as dist");
      assertThat(result.hasNext()).isTrue();
      final Double distKm = result.next().getProperty("dist");
      assertThat(distKm).isNotNull();
      assertThat(distKm).isGreaterThan(0.0);
      // km < m for the same distance
      assertThat(distKm).isLessThan(dist);
    });
  }

  @Test
  void stDistanceNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Distance(null, 'POINT (1 0)') as dist");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("dist");
      assertThat(val).isNull();
    });
  }

  @Test
  void stArea() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
          "select ST_Area('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as area");
      assertThat(result.hasNext()).isTrue();
      final Double area = result.next().getProperty("area");
      assertThat(area).isNotNull();
      assertThat(area).isGreaterThan(0.0);
    });
  }

  @Test
  void stAreaNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Area(null) as area");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("area");
      assertThat(val).isNull();
    });
  }

  @Test
  void stAsText() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // String input → returned as-is
      ResultSet result = db.query("sql", "select ST_AsText('POINT (10 20)') as wkt");
      assertThat(result.hasNext()).isTrue();
      final String wkt = result.next().getProperty("wkt");
      assertThat(wkt).isEqualTo("POINT (10 20)");
    });
  }

  @Test
  void stAsTextFromShape() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Shape → WKT
      ResultSet result = db.query("sql", "select ST_AsText(ST_GeomFromText('POINT (10 20)')) as wkt");
      assertThat(result.hasNext()).isTrue();
      final String wkt = result.next().getProperty("wkt");
      assertThat(wkt).isNotNull();
      assertThat(wkt).startsWith("POINT");
      assertThat(wkt).contains("10");
      assertThat(wkt).contains("20");
    });
  }

  @Test
  void stAsTextNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_AsText(null) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stAsGeoJson() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_AsGeoJson('POINT (10 20)') as json");
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
  void stAsGeoJsonPolygon() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
          "select ST_AsGeoJson('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as json");
      assertThat(result.hasNext()).isTrue();
      final String json = result.next().getProperty("json");
      assertThat(json).isNotNull();
      assertThat(json).contains("Polygon");
      assertThat(json).contains("coordinates");
    });
  }

  @Test
  void stAsGeoJsonNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_AsGeoJson(null) as json");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("json");
      assertThat(val).isNull();
    });
  }

  @Test
  void stX() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_X('POINT (10 20)') as x");
      assertThat(result.hasNext()).isTrue();
      final Double x = result.next().getProperty("x");
      assertThat(x).isNotNull();
      assertThat(x).isEqualTo(10.0);
    });
  }

  @Test
  void stXFromShape() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_X(ST_GeomFromText('POINT (10 20)')) as x");
      assertThat(result.hasNext()).isTrue();
      final Double x = result.next().getProperty("x");
      assertThat(x).isNotNull();
      assertThat(x).isEqualTo(10.0);
    });
  }

  @Test
  void stXNonPoint() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // ST_X on a polygon should return null
      ResultSet result = db.query("sql",
          "select ST_X('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as x");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("x");
      assertThat(val).isNull();
    });
  }

  @Test
  void stXNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_X(null) as x");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("x");
      assertThat(val).isNull();
    });
  }

  @Test
  void stY() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Y('POINT (10 20)') as y");
      assertThat(result.hasNext()).isTrue();
      final Double y = result.next().getProperty("y");
      assertThat(y).isNotNull();
      assertThat(y).isEqualTo(20.0);
    });
  }

  @Test
  void stYFromShape() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Y(ST_GeomFromText('POINT (10 20)')) as y");
      assertThat(result.hasNext()).isTrue();
      final Double y = result.next().getProperty("y");
      assertThat(y).isNotNull();
      assertThat(y).isEqualTo(20.0);
    });
  }

  @Test
  void stYNonPoint() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
          "select ST_Y('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as y");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("y");
      assertThat(val).isNull();
    });
  }

  @Test
  void stYNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select ST_Y(null) as y");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("y");
      assertThat(val).isNull();
    });
  }

  @Test
  void stPointRoundTrip() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // ST_Point → ST_X / ST_Y round-trip
      // Note: small floating-point precision loss is expected when going through WKT parsing
      ResultSet result = db.query("sql", "select ST_X(ST_GeomFromText(ST_Point(42.5, -7.3))) as x");
      assertThat(result.hasNext()).isTrue();
      final Double x = result.next().getProperty("x");
      assertThat(x).isNotNull();
      assertThat(x).isCloseTo(42.5, org.assertj.core.data.Offset.offset(1e-6));

      result = db.query("sql", "select ST_Y(ST_GeomFromText(ST_Point(42.5, -7.3))) as y");
      assertThat(result.hasNext()).isTrue();
      final Double y = result.next().getProperty("y");
      assertThat(y).isNotNull();
      assertThat(y).isCloseTo(-7.3, org.assertj.core.data.Offset.offset(1e-6));
    });
  }
}
