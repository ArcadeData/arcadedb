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
package com.arcadedb.query.sql.functions.geo;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLGeoFunctionsTest {

  @Test
  public void testPoint() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select point(11,11) as point");
      Assertions.assertTrue(result.hasNext());
      Point point = result.next().getProperty("point");
      Assertions.assertNotNull(point);
    });
  }

  @Test
  public void testRectangle() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select rectangle(10,10,20,20) as shape");
      Assertions.assertTrue(result.hasNext());
      Rectangle rectangle = result.next().getProperty("shape");
      Assertions.assertNotNull(rectangle);
    });
  }

  @Test
  public void testCircle() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select circle(10,10,10) as circle");
      Assertions.assertTrue(result.hasNext());
      Circle circle = result.next().getProperty("circle");
      Assertions.assertNotNull(circle);
    });
  }

  @Test
  public void testPolygon() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select polygon( [ point(10,10), point(20,10), point(20,20), point(10,20), point(10,10) ] ) as polygon");
      Assertions.assertTrue(result.hasNext());
      Shape polygon = result.next().getProperty("polygon");
      Assertions.assertNotNull(polygon);

      result = db.query("sql", "select polygon( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ) as polygon");
      Assertions.assertTrue(result.hasNext());
      polygon = result.next().getProperty("polygon");
      Assertions.assertNotNull(polygon);
    });
  }

  @Test
  public void testPointIsWithinRectangle() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select point(11,11).isWithin( rectangle(10,10,20,20) ) as isWithin");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertTrue((Boolean) result.next().getProperty("isWithin"));

      result = db.query("sql", "select point(11,21).isWithin( rectangle(10,10,20,20) ) as isWithin");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("isWithin"));
    });
  }

  @Test
  public void testPointIsWithinCircle() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select point(11,11).isWithin( circle(10,10,10) ) as isWithin");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertTrue((Boolean) result.next().getProperty("isWithin"));

      result = db.query("sql", "select point(10,21).isWithin( circle(10,10,10) ) as isWithin");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("isWithin"));
    });
  }

  @Test
  public void testPointIntersectWithRectangle() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql", "select rectangle(9,9,11,11).intersectsWith( rectangle(10,10,20,20) ) as intersectsWith");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertTrue((Boolean) result.next().getProperty("intersectsWith"));

      result = db.query("sql", "select rectangle(9,9,9.9,9.9).intersectsWith( rectangle(10,10,20,20) ) as intersectsWith");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("intersectsWith"));
    });
  }

  @Test
  public void testPointIntersectWithPolygons() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
          "select polygon( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ).intersectsWith( rectangle(10,10,20,20) ) as intersectsWith");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertTrue((Boolean) result.next().getProperty("intersectsWith"));

      result = db.query("sql", "select polygon( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ).intersectsWith( rectangle(21,21,22,22) ) as intersectsWith");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("intersectsWith"));
    });
  }

  @Test
  public void testLineStringsIntersect() throws Exception {
    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
      ResultSet result = db.query("sql",
          "select linestring( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ).intersectsWith( rectangle(10,10,20,20) ) as intersectsWith");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertTrue((Boolean) result.next().getProperty("intersectsWith"));

      result = db.query("sql",
          "select linestring( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ).intersectsWith( rectangle(21,21,22,22) ) as intersectsWith");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("intersectsWith"));
    });
  }

  @Test
  public void testGeoManualIndexPoints() throws Exception {
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

        Assertions.assertTrue(result.hasNext());
        int returned = 0;
        while (result.hasNext()) {
          final Document record = result.next().toElement();
          Assertions.assertTrue(record.getDouble("lat") >= 10.5);
          Assertions.assertTrue(record.getDouble("long") <= 10.55);
//          System.out.println(record.toJSON());

          ++returned;
        }

        //System.out.println("Elapsed browsing: " + (System.currentTimeMillis() - begin));

        Assertions.assertEquals(6, returned);
      });
    });
  }

  @Test
  public void testGeoManualIndexBoundingBoxes() throws Exception {
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
          Assertions.assertEquals(TOTAL, idx.countEntries());
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

        Assertions.assertTrue(result.hasNext());
        int returned = 0;
        while (result.hasNext()) {
          final Document record = result.next().toElement();
          Assertions.assertTrue(record.getDouble("x1") >= 10.0001D, "x1: " + record.getDouble("x1"));
          Assertions.assertTrue(record.getDouble("y1") >= 10.0001D, "y1: " + record.getDouble("y1"));
          Assertions.assertTrue(record.getDouble("x2") <= 10.020D, "x2: " + record.getDouble("x2"));
          Assertions.assertTrue(record.getDouble("y2") <= 10.020D, "y2: " + record.getDouble("y2"));
          //System.out.println(record.toJSON());

          ++returned;
        }

        //System.out.println("Elapsed browsing: " + (System.currentTimeMillis() - begin));

        Assertions.assertEquals(20, returned);
      });
    });
  }
}
