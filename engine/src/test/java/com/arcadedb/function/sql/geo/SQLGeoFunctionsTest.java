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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

import java.io.File;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLGeoFunctionsTest {

  @Test
  void point() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select point(11,11) as point");
      assertThat(result.hasNext()).isTrue();
      Point point = result.next().getProperty("point");
      assertThat(point).isNotNull();
    });
  }

  @Test
  void rectangle() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select rectangle(10,10,20,20) as shape");
      assertThat(result.hasNext()).isTrue();
      Rectangle rectangle = result.next().getProperty("shape");
      assertThat(rectangle).isNotNull();
    });
  }

  @Test
  void circle() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select circle(10,10,10) as circle");
      assertThat(result.hasNext()).isTrue();
      Circle circle = result.next().getProperty("circle");
      assertThat(circle).isNotNull();
    });
  }

  @Test
  void polygon() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
          "select polygon( [ point(10,10), point(20,10), point(20,20), point(10,20), point(10,10) ] ) as polygon");
      assertThat(result.hasNext()).isTrue();
      Shape polygon = result.next().getProperty("polygon");
      assertThat(polygon).isNotNull();

      result = db.query("sql", "select polygon( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ) as polygon");
      assertThat(result.hasNext()).isTrue();
      polygon = result.next().getProperty("polygon");
      assertThat(polygon).isNotNull();
    });
  }

  @Test
  void pointIsWithinRectangle() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select point(11,11).isWithin( rectangle(10,10,20,20) ) as isWithin");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("isWithin")).isTrue();

      result = db.query("sql", "select point(11,21).isWithin( rectangle(10,10,20,20) ) as isWithin");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("isWithin")).isFalse();
    });
  }

  @Test
  void pointIsWithinCircle() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select point(11,11).isWithin( circle(10,10,10) ) as isWithin");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("isWithin")).isTrue();

      result = db.query("sql", "select point(10,21).isWithin( circle(10,10,10) ) as isWithin");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("isWithin")).isFalse();
    });
  }

  @Test
  void pointIntersectWithRectangle() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select rectangle(9,9,11,11).intersectsWith( rectangle(10,10,20,20) ) as intersectsWith");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("intersectsWith")).isTrue();

      result = db.query("sql", "select rectangle(9,9,9.9,9.9).intersectsWith( rectangle(10,10,20,20) ) as intersectsWith");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("intersectsWith")).isFalse();
    });
  }

  @Test
  void pointIntersectWithPolygons() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
          "select polygon( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ).intersectsWith( rectangle(10,10,20,20) ) as intersectsWith");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("intersectsWith")).isTrue();

      result = db.query("sql",
          "select polygon( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ).intersectsWith( rectangle(21,21,22,22) ) as intersectsWith");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("intersectsWith")).isFalse();
    });
  }

  @Test
  void lineStringsIntersect() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
          "select linestring( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ).intersectsWith( rectangle(10,10,20,20) ) as intersectsWith");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("intersectsWith")).isTrue();

      result = db.query("sql",
          "select linestring( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ).intersectsWith( rectangle(21,21,22,22) ) as intersectsWith");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("intersectsWith")).isFalse();
    });
  }

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

        //System.out.println("Elapsed insert: " + (System.currentTimeMillis() - begin));

        final String[] area = new String[] { GeohashUtils.encodeLatLon(10.5, 10.5), GeohashUtils.encodeLatLon(10.55, 10.55) };

        begin = System.currentTimeMillis();
        ResultSet result = db.query("sql", "select from Restaurant where coords >= ? and coords <= ?", area[0], area[1]);

        //System.out.println("Elapsed query: " + (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();

        assertThat(result.hasNext()).isTrue();
        int returned = 0;
        while (result.hasNext()) {
          final Document record = result.next().toElement();
          assertThat(record.getDouble("lat")).isGreaterThanOrEqualTo(10.5);
          assertThat(record.getDouble("long")).isLessThanOrEqualTo(10.55);
//          System.out.println(record.toJSON());

          ++returned;
        }

        //System.out.println("Elapsed browsing: " + (System.currentTimeMillis() - begin));

        assertThat(returned).isEqualTo(6);
      });
    });
  }

  /**
   * Test for issue #1843: Exact reproduction of the reported issue.
   * Original query: insert into point set geom=(select point(30,30) as point);
   */
  @Test
  void insertPointIntoDocumentExactIssue() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      db.transaction(() -> {
        // Exact steps from issue #1843
        db.command("sql", "create document type point");
        db.command("sql", "insert into point set geom=(select point(30,30) as point)");

        // Verify data was stored
        final ResultSet result = db.query("sql", "select from point");
        assertThat(result.hasNext()).isTrue();

        final Document doc = result.next().toElement();
        final Object geom = doc.get("geom");
        assertThat(geom).isNotNull();

        // The geom field should contain the stored point (as WKT or nested result)
        // When using subquery, the result is a map containing the point
        if (geom instanceof Map) {
          final Map<?, ?> map = (Map<?, ?>) geom;
          assertThat(map.get("point")).isNotNull();
        }
      });
    });
  }

  /**
   * Test for issue #1843: Cannot serialize value of type class org.locationtech.spatial4j.shape.jts.JtsPoint
   * This test verifies that Point objects can be serialized and persisted across database restarts.
   */
  @Test
  void insertPointIntoDocument() throws Exception {
    // Use a unique database name to avoid conflicts with parallel tests
    final String dbName = "GeoPointPersistence_" + System.nanoTime();
    final String dbPath = "./target/databases/" + dbName;

    try {
      // Clean up first
      FileUtils.deleteRecursively(new File(dbPath));

      // Create database and insert data
      try (Database db = new DatabaseFactory(dbPath).create()) {
        db.transaction(() -> {
          // Create the document type as described in the issue
          db.command("sql", "create document type GeoPoint");

          // This should work: insert a point using the point() function
          db.command("sql", "insert into GeoPoint set geom = point(30, 30)");
        });
      }

      // Reopen database to verify data was persisted correctly
      try (Database db = new DatabaseFactory(dbPath).open()) {
        db.transaction(() -> {
          // Verify we can retrieve the stored value
          final ResultSet result = db.query("sql", "select from GeoPoint");
          assertThat(result.hasNext()).isTrue();

          final Document doc = result.next().toElement();
          assertThat(doc.get("geom")).isNotNull();

          // The value should be stored as WKT string
          final String storedValue = doc.getString("geom");
          assertThat(storedValue).contains("Pt");
          assertThat(storedValue).contains("30");
        });
      }
    } finally {
      FileUtils.deleteRecursively(new File(dbPath));
    }
  }

  /**
   * Test for issue #1843: Test various shape types serialization
   * Note: Point, LineString, Polygon are standard WKT types and stored as WKT.
   *       Circle and Rectangle are not standard WKT types and are stored as their toString() representation.
   */
  @Test
  void insertVariousShapesIntoDocument() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      db.transaction(() -> {
        db.command("sql", "create document type GeoShapes");

        // Insert circle (not standard WKT - stored as toString)
        db.command("sql", "insert into GeoShapes set name = 'circle', geom = circle(10, 10, 5)");

        // Insert rectangle (not standard WKT - stored as toString)
        db.command("sql", "insert into GeoShapes set name = 'rectangle', geom = rectangle(0, 0, 10, 10)");

        // Insert polygon (standard WKT)
        db.command("sql",
            "insert into GeoShapes set name = 'polygon', geom = polygon([[0,0], [10,0], [10,10], [0,10], [0,0]])");

        // Insert linestring (standard WKT)
        db.command("sql", "insert into GeoShapes set name = 'linestring', geom = linestring([[0,0], [10,10], [20,0]])");

        // Verify all shapes were stored
        final ResultSet result = db.query("sql", "select from GeoShapes order by name");

        // Circle - not standard WKT, stored as toString() which contains "Circle"
        assertThat(result.hasNext()).isTrue();
        Document doc = result.next().toElement();
        assertThat(doc.getString("name")).isEqualTo("circle");
        assertThat(doc.getString("geom")).contains("Circle");  // toString() format

        // Linestring - standard WKT format
        assertThat(result.hasNext()).isTrue();
        doc = result.next().toElement();
        assertThat(doc.getString("name")).isEqualTo("linestring");
        assertThat(doc.getString("geom")).contains("LINESTRING");

        // Polygon - standard WKT format
        assertThat(result.hasNext()).isTrue();
        doc = result.next().toElement();
        assertThat(doc.getString("name")).isEqualTo("polygon");
        assertThat(doc.getString("geom")).contains("POLYGON");

        // Rectangle - not standard WKT, stored as toString() which contains "Rect"
        assertThat(result.hasNext()).isTrue();
        doc = result.next().toElement();
        assertThat(doc.getString("name")).isEqualTo("rectangle");
        assertThat(doc.getString("geom")).contains("Rect");  // toString() format
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

        //System.out.println("Elapsed insert: " + (System.currentTimeMillis() - begin));

        final String[] area = new String[] {//
            GeohashUtils.encodeLatLon(10.0001D, 10.0001D),//
            GeohashUtils.encodeLatLon(10.020D, 10.020D) };

        begin = System.currentTimeMillis();
        //ResultSet result = db.query("sql", "select from Restaurant where bboxBR <= ?",area[1]);
        ResultSet result = db.query("sql", "select from Restaurant where bboxTL >= ? and bboxBR <= ?", area[0], area[1]);

        //System.out.println("Elapsed query: " + (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();

        assertThat(result.hasNext()).isTrue();
        int returned = 0;
        while (result.hasNext()) {
          final Document record = result.next().toElement();
          assertThat(record.getDouble("x1")).isGreaterThanOrEqualTo(10.0001D).withFailMessage("x1: " + record.getDouble("x1"));
          assertThat(record.getDouble("y1")).isGreaterThanOrEqualTo(10.0001D).withFailMessage("y1: " + record.getDouble("y1"));
          assertThat(record.getDouble("x2")).isLessThanOrEqualTo(10.020D).withFailMessage("x2: " + record.getDouble("x2"));
          assertThat(record.getDouble("y2")).isLessThanOrEqualTo(10.020D).withFailMessage("y2: " + record.getDouble("y2"));
          //System.out.println(record.toJSON());

          ++returned;
        }

        //System.out.println("Elapsed browsing: " + (System.currentTimeMillis() - begin));

        assertThat(returned).isEqualTo(20);
      });
    });
  }
}
