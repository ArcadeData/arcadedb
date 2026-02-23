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

  // ─── geo.* standard function tests ────────────────────────────────────────────

  @Test
  void stGeomFromText() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Valid WKT point
      ResultSet result = db.query("sql", "select geo.geomFromText('POINT (10 20)') as geom");
      assertThat(result.hasNext()).isTrue();
      final Object geom = result.next().getProperty("geom");
      assertThat(geom).isNotNull();
      assertThat(geom).isInstanceOf(Shape.class);
    });
  }

  @Test
  void stGeomFromTextNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.geomFromText(null) as geom");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("geom");
      assertThat(val).isNull();
    });
  }

  @Test
  void stPoint() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.point(10, 20) as wkt");
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
      ResultSet result = db.query("sql", "select geo.point(null, 20) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stLineString() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.lineString([[0,0],[10,10],[20,0]]) as wkt");
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
      ResultSet result = db.query("sql", "select geo.lineString(null) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stPolygon() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Closed ring
      ResultSet result = db.query("sql", "select geo.polygon([[0,0],[10,0],[10,10],[0,10],[0,0]]) as wkt");
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
      ResultSet result = db.query("sql", "select geo.polygon([[0,0],[10,0],[10,10],[0,10]]) as wkt");
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
      ResultSet result = db.query("sql", "select geo.polygon(null) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stBuffer() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.buffer('POINT (10 20)', 1.0) as wkt");
      assertThat(result.hasNext()).isTrue();
      final String wkt = result.next().getProperty("wkt");
      assertThat(wkt).isNotNull();
      assertThat(wkt).startsWith("POLYGON");
    });
  }

  @Test
  void stBufferNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.buffer(null, 1.0) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stEnvelope() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
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
  void stEnvelopeNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.envelope(null) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stDistance() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Distance between two points in meters (default unit)
      ResultSet result = db.query("sql", "select geo.distance('POINT (0 0)', 'POINT (1 0)') as dist");
      assertThat(result.hasNext()).isTrue();
      final Double dist = result.next().getProperty("dist");
      assertThat(dist).isNotNull();
      assertThat(dist).isGreaterThan(0.0);

      // Distance in km
      result = db.query("sql", "select geo.distance('POINT (0 0)', 'POINT (1 0)', 'km') as dist");
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
      ResultSet result = db.query("sql", "select geo.distance(null, 'POINT (1 0)') as dist");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("dist");
      assertThat(val).isNull();
    });
  }

  @Test
  void stArea() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
          "select geo.area('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as area");
      assertThat(result.hasNext()).isTrue();
      final Double area = result.next().getProperty("area");
      assertThat(area).isNotNull();
      assertThat(area).isGreaterThan(0.0);
    });
  }

  @Test
  void stAreaNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.area(null) as area");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("area");
      assertThat(val).isNull();
    });
  }

  @Test
  void stAsText() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // String input → returned as-is
      ResultSet result = db.query("sql", "select geo.asText('POINT (10 20)') as wkt");
      assertThat(result.hasNext()).isTrue();
      final String wkt = result.next().getProperty("wkt");
      assertThat(wkt).isEqualTo("POINT (10 20)");
    });
  }

  @Test
  void stAsTextFromShape() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Shape → WKT
      ResultSet result = db.query("sql", "select geo.asText(geo.geomFromText('POINT (10 20)')) as wkt");
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
      ResultSet result = db.query("sql", "select geo.asText(null) as wkt");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("wkt");
      assertThat(val).isNull();
    });
  }

  @Test
  void stAsGeoJson() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.asGeoJson('POINT (10 20)') as json");
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
          "select geo.asGeoJson('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as json");
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
      ResultSet result = db.query("sql", "select geo.asGeoJson(null) as json");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("json");
      assertThat(val).isNull();
    });
  }

  @Test
  void stX() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.x('POINT (10 20)') as x");
      assertThat(result.hasNext()).isTrue();
      final Double x = result.next().getProperty("x");
      assertThat(x).isNotNull();
      assertThat(x).isEqualTo(10.0);
    });
  }

  @Test
  void stXFromShape() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.x(geo.geomFromText('POINT (10 20)')) as x");
      assertThat(result.hasNext()).isTrue();
      final Double x = result.next().getProperty("x");
      assertThat(x).isNotNull();
      assertThat(x).isEqualTo(10.0);
    });
  }

  @Test
  void stXNonPoint() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // geo.x on a polygon should return null
      ResultSet result = db.query("sql",
          "select geo.x('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as x");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("x");
      assertThat(val).isNull();
    });
  }

  @Test
  void stXNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.x(null) as x");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("x");
      assertThat(val).isNull();
    });
  }

  @Test
  void stY() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.y('POINT (10 20)') as y");
      assertThat(result.hasNext()).isTrue();
      final Double y = result.next().getProperty("y");
      assertThat(y).isNotNull();
      assertThat(y).isEqualTo(20.0);
    });
  }

  @Test
  void stYFromShape() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.y(geo.geomFromText('POINT (10 20)')) as y");
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
          "select geo.y('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as y");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("y");
      assertThat(val).isNull();
    });
  }

  @Test
  void stYNull() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select geo.y(null) as y");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("y");
      assertThat(val).isNull();
    });
  }

  @Test
  void stPointRoundTrip() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // geo.point → geo.x / geo.y round-trip
      // Note: small floating-point precision loss is expected when going through WKT parsing
      ResultSet result = db.query("sql", "select geo.x(geo.geomFromText(geo.point(42.5, -7.3))) as x");
      assertThat(result.hasNext()).isTrue();
      final Double x = result.next().getProperty("x");
      assertThat(x).isNotNull();
      assertThat(x).isCloseTo(42.5, org.assertj.core.data.Offset.offset(1e-6));

      result = db.query("sql", "select geo.y(geo.geomFromText(geo.point(42.5, -7.3))) as y");
      assertThat(result.hasNext()).isTrue();
      final Double y = result.next().getProperty("y");
      assertThat(y).isNotNull();
      assertThat(y).isCloseTo(-7.3, org.assertj.core.data.Offset.offset(1e-6));
    });
  }

  // ─── geo.within ────────────────────────────────────────────────────────────────

  @Test
  void stWithinPointInsidePolygon() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.within('POINT (5 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isTrue();
    });
  }

  @Test
  void stWithinPointOutsidePolygon() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.within('POINT (15 15)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isFalse();
    });
  }

  @Test
  void stWithinNullArg() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.within(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("v");
      assertThat(val).isNull();
    });
  }

  // ─── geo.intersects ────────────────────────────────────────────────────────────

  @Test
  void stIntersectsOverlappingPolygons() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.intersects('POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))', 'POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isTrue();
    });
  }

  @Test
  void stIntersectsDisjointPolygons() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.intersects('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isFalse();
    });
  }

  @Test
  void stIntersectsNullArg() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.intersects(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("v");
      assertThat(val).isNull();
    });
  }

  // ─── geo.contains ──────────────────────────────────────────────────────────────

  @Test
  void stContainsPolygonContainsPoint() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.contains('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'POINT (5 5)') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isTrue();
    });
  }

  @Test
  void stContainsPolygonDoesNotContainOutsidePoint() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.contains('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'POINT (15 15)') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isFalse();
    });
  }

  @Test
  void stContainsNullArg() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.contains(null, 'POINT (5 5)') as v");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("v");
      assertThat(val).isNull();
    });
  }

  // ─── geo.dWithin ───────────────────────────────────────────────────────────────

  @Test
  void stDWithinNearbyPoints() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Two points at about 1.414 degrees apart; distance threshold = 2.0 → true
      final ResultSet result = db.query("sql",
          "select geo.dWithin('POINT (0 0)', 'POINT (1 1)', 2.0) as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isTrue();
    });
  }

  @Test
  void stDWithinFarPoints() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Two points far apart; distance threshold = 1.0 → false
      final ResultSet result = db.query("sql",
          "select geo.dWithin('POINT (0 0)', 'POINT (10 10)', 1.0) as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isFalse();
    });
  }

  @Test
  void stDWithinNullArg() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.dWithin(null, 'POINT (1 1)', 2.0) as v");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("v");
      assertThat(val).isNull();
    });
  }

  // ─── geo.disjoint ──────────────────────────────────────────────────────────────

  @Test
  void stDisjointFarApartShapes() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.disjoint('POINT (50 50)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isTrue();
    });
  }

  @Test
  void stDisjointIntersectingShapes() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.disjoint('POINT (5 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isFalse();
    });
  }

  @Test
  void stDisjointNullArg() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.disjoint(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("v");
      assertThat(val).isNull();
    });
  }

  // ─── geo.equals ────────────────────────────────────────────────────────────────

  @Test
  void stEqualsIdenticalPoints() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.equals('POINT (5 5)', 'POINT (5 5)') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isTrue();
    });
  }

  @Test
  void stEqualsDifferentPoints() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.equals('POINT (5 5)', 'POINT (6 6)') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isFalse();
    });
  }

  @Test
  void stEqualsNullArg() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.equals(null, 'POINT (5 5)') as v");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("v");
      assertThat(val).isNull();
    });
  }

  // ─── geo.crosses ───────────────────────────────────────────────────────────────

  @Test
  void stCrossesLineCrossesPolygon() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // A line crossing a polygon boundary
      final ResultSet result = db.query("sql",
          "select geo.crosses('LINESTRING (-1 5, 11 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isTrue();
    });
  }

  @Test
  void stCrossesNullArg() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.crosses(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("v");
      assertThat(val).isNull();
    });
  }

  // ─── geo.overlaps ──────────────────────────────────────────────────────────────

  @Test
  void stOverlapsPartiallyOverlappingPolygons() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.overlaps('POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))', 'POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isTrue();
    });
  }

  @Test
  void stOverlapsDisjointPolygons() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.overlaps('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isFalse();
    });
  }

  @Test
  void stOverlapsNullArg() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.overlaps(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("v");
      assertThat(val).isNull();
    });
  }

  // ─── geo.touches ───────────────────────────────────────────────────────────────

  @Test
  void stTouchesAdjacentPolygons() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      // Two polygons sharing exactly one edge
      final ResultSet result = db.query("sql",
          "select geo.touches('POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))', 'POLYGON ((5 0, 10 0, 10 5, 5 5, 5 0))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isTrue();
    });
  }

  @Test
  void stTouchesDisjointPolygons() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.touches('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))') as v");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("v")).isFalse();
    });
  }

  @Test
  void stTouchesNullArg() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      final ResultSet result = db.query("sql",
          "select geo.touches(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
      assertThat(result.hasNext()).isTrue();
      final Object val = result.next().getProperty("v");
      assertThat(val).isNull();
    });
  }
}
