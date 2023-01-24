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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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

      result = db.query("sql", "select linestring( [ [10,10], [20,10], [20,20], [10,20], [10,10] ] ).intersectsWith( rectangle(21,21,22,22) ) as intersectsWith");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("intersectsWith"));
    });
  }
}
