/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.within;

/**
 * Comprehensive tests for OpenCypher Spatial functions based on Neo4j Cypher documentation.
 * Tests cover all spatial functions: point(), point.distance(), point.withinBBox()
 */
class OpenCypherSpatialFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-cypher-spatial-functions");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== point() Tests - WGS 84 2D ====================

  @Test
  void pointWGS84_2D_WithLongitudeLatitude() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({longitude: 56.7, latitude: 12.78}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
    // Verify it's a point with the correct coordinates
    assertThat(point.toString()).contains("56.7");
    assertThat(point.toString()).contains("12.78");
  }

  @Test
  void pointWGS84_2D_WithXY() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({x: 56.7, y: 12.78, crs: 'WGS-84'}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
  }

  @Test
  void pointWGS84_2D_WithSRID() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({longitude: 56.7, latitude: 12.78, srid: 4326}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
  }

  // ==================== point() Tests - WGS 84 3D ====================

  @Test
  void pointWGS84_3D_WithLongitudeLatitudeHeight() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({longitude: 56.7, latitude: 12.78, height: 100.0}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
    assertThat(point.toString()).contains("56.7");
    assertThat(point.toString()).contains("12.78");
    assertThat(point.toString()).contains("100");
  }

  @Test
  void pointWGS84_3D_WithXYZ() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({x: 56.7, y: 12.78, z: 100.0, crs: 'WGS-84-3D'}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
  }

  @Test
  void pointWGS84_3D_WithSRID() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({longitude: 56.7, latitude: 12.78, height: 100.0, srid: 4979}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
  }

  // ==================== point() Tests - Cartesian 2D ====================

  @Test
  void pointCartesian2D_Basic() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({x: 3.0, y: 4.0}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
    assertThat(point.toString()).contains("3");
    assertThat(point.toString()).contains("4");
  }

  @Test
  void pointCartesian2D_WithCRS() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({x: 3.0, y: 4.0, crs: 'cartesian'}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
  }

  @Test
  void pointCartesian2D_WithSRID() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({x: 3.0, y: 4.0, srid: 7203}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
  }

  // ==================== point() Tests - Cartesian 3D ====================

  @Test
  void pointCartesian3D_Basic() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({x: 3.0, y: 4.0, z: 5.0}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
    assertThat(point.toString()).contains("3");
    assertThat(point.toString()).contains("4");
    assertThat(point.toString()).contains("5");
  }

  @Test
  void pointCartesian3D_WithCRS() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({x: 3.0, y: 4.0, z: 5.0, crs: 'cartesian-3D'}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
  }

  @Test
  void pointCartesian3D_WithSRID() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({x: 3.0, y: 4.0, z: 5.0, srid: 9157}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
  }

  // ==================== point() Tests - Null Handling ====================

  @Test
  void pointNullHandling() {
    final ResultSet result = database.command("opencypher",
        "RETURN point(null) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("point") == null).isTrue();
  }

  @Test
  void pointWithNullCoordinate() {
    final ResultSet result = database.command("opencypher",
        "RETURN point({x: null, y: 4.0}) AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("point") == null).isTrue();
  }

  // ==================== point.distance() Tests ====================

  @Test
  void pointDistanceCartesian2D() {
    // Distance between (0,0) and (3,4) should be 5 (Pythagorean theorem)
    final ResultSet result = database.command("opencypher",
        "RETURN point.distance(point({x: 0.0, y: 0.0}), point({x: 3.0, y: 4.0})) AS distance");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double distance = (Double) result.next().getProperty("distance");
    assertThat(distance).isCloseTo(5.0, within(0.001));
  }

  @Test
  void pointDistanceCartesian3D() {
    // Distance between (0,0,0) and (1,1,1) should be sqrt(3)
    final ResultSet result = database.command("opencypher",
        "RETURN point.distance(point({x: 0.0, y: 0.0, z: 0.0}), point({x: 1.0, y: 1.0, z: 1.0})) AS distance");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double distance = (Double) result.next().getProperty("distance");
    assertThat(distance).isCloseTo(Math.sqrt(3), within(0.001));
  }

  @Test
  void pointDistanceWGS84() {
    // Geodesic distance between two geographic points
    final ResultSet result = database.command("opencypher",
        "RETURN point.distance(" +
            "point({longitude: 12.564590, latitude: 55.672874}), " +
            "point({longitude: 12.994341, latitude: 55.611784})) AS distance");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double distance = (Double) result.next().getProperty("distance");
    // Distance should be approximately 35 km (in meters)
    assertThat(distance).isGreaterThan(30000.0);
    assertThat(distance).isLessThan(40000.0);
  }

  @Test
  void pointDistanceSamePoint() {
    final ResultSet result = database.command("opencypher",
        "RETURN point.distance(point({x: 1.0, y: 2.0}), point({x: 1.0, y: 2.0})) AS distance");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double distance = (Double) result.next().getProperty("distance");
    assertThat(distance).isCloseTo(0.0, within(0.001));
  }

  @Test
  void pointDistanceNullHandling() {
    ResultSet result = database.command("opencypher",
        "RETURN point.distance(null, point({x: 1.0, y: 2.0})) AS distance");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("distance") == null).isTrue();

    result = database.command("opencypher",
        "RETURN point.distance(point({x: 1.0, y: 2.0}), null) AS distance");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("distance") == null).isTrue();
  }

  // ==================== point.withinBBox() Tests ====================

  @Test
  void pointWithinBBoxInside() {
    final ResultSet result = database.command("opencypher",
        "RETURN point.withinBBox(" +
            "point({x: 5.0, y: 5.0}), " +
            "point({x: 0.0, y: 0.0}), " +
            "point({x: 10.0, y: 10.0})) AS inside");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Boolean inside = (Boolean) result.next().getProperty("inside");
    assertThat(inside).isTrue();
  }

  @Test
  void pointWithinBBoxOutside() {
    final ResultSet result = database.command("opencypher",
        "RETURN point.withinBBox(" +
            "point({x: 15.0, y: 15.0}), " +
            "point({x: 0.0, y: 0.0}), " +
            "point({x: 10.0, y: 10.0})) AS inside");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Boolean inside = (Boolean) result.next().getProperty("inside");
    assertThat(inside).isFalse();
  }

  @Test
  void pointWithinBBoxOnEdge() {
    final ResultSet result = database.command("opencypher",
        "RETURN point.withinBBox(" +
            "point({x: 0.0, y: 5.0}), " +
            "point({x: 0.0, y: 0.0}), " +
            "point({x: 10.0, y: 10.0})) AS inside");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Boolean inside = (Boolean) result.next().getProperty("inside");
    assertThat(inside).isTrue();
  }

  @Test
  void pointWithinBBoxWGS84() {
    // Test with geographic coordinates
    final ResultSet result = database.command("opencypher",
        "RETURN point.withinBBox(" +
            "point({longitude: 12.8, latitude: 55.6}), " +
            "point({longitude: 12.5, latitude: 55.5}), " +
            "point({longitude: 13.0, latitude: 55.7})) AS inside");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Boolean inside = (Boolean) result.next().getProperty("inside");
    assertThat(inside).isTrue();
  }

  @Test
  void pointWithinBBoxNullHandling() {
    ResultSet result = database.command("opencypher",
        "RETURN point.withinBBox(null, point({x: 0.0, y: 0.0}), point({x: 10.0, y: 10.0})) AS inside");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("inside") == null).isTrue();

    result = database.command("opencypher",
        "RETURN point.withinBBox(point({x: 5.0, y: 5.0}), null, point({x: 10.0, y: 10.0})) AS inside");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("inside") == null).isTrue();

    result = database.command("opencypher",
        "RETURN point.withinBBox(point({x: 5.0, y: 5.0}), point({x: 0.0, y: 0.0}), null) AS inside");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("inside") == null).isTrue();
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void pointFunctionsCombined() {
    // Create two points and verify they're at a specific distance
    final ResultSet result = database.command("opencypher",
        "WITH point({x: 0.0, y: 0.0}) AS p1, point({x: 3.0, y: 4.0}) AS p2 " +
            "RETURN point.distance(p1, p2) AS dist, " +
            "point.withinBBox(p2, point({x: 0.0, y: 0.0}), point({x: 10.0, y: 10.0})) AS inBox");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Result row = result.next();
    assertThat((Double) row.getProperty("dist")).isCloseTo(5.0, within(0.001));
    assertThat((Boolean) row.getProperty("inBox")).isTrue();
  }

  @Test
  void pointStoredInNode() {
    database.getSchema().createVertexType("Location");
    database.command("opencypher",
        "CREATE (loc:Location {name: 'Copenhagen', position: point({longitude: 12.564590, latitude: 55.672874})})");

    final ResultSet result = database.command("opencypher",
        "MATCH (loc:Location {name: 'Copenhagen'}) RETURN loc.position AS point");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Object point = result.next().getProperty("point");
    assertThat(point).isNotNull();
  }

  @Test
  void pointDistanceBetweenNodes() {
    database.getSchema().createVertexType("Location");
    database.command("opencypher",
        "CREATE (a:Location {name: 'A', pos: point({x: 0.0, y: 0.0})}), " +
            "(b:Location {name: 'B', pos: point({x: 3.0, y: 4.0})})");

    final ResultSet result = database.command("opencypher",
        "MATCH (a:Location {name: 'A'}), (b:Location {name: 'B'}) " +
            "RETURN point.distance(a.pos, b.pos) AS distance");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double distance = (Double) result.next().getProperty("distance");
    assertThat(distance).isCloseTo(5.0, within(0.001));
  }
}
