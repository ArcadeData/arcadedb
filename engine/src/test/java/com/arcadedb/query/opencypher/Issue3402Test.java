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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3402: java.lang.NumberFormatException on spatial functions usage (distance).
 * When using point() function to create spatial data and then querying with distance() function,
 * the system throws NumberFormatException because points are serialized as WKT strings but
 * distance() expects them to be Point objects or individual coordinates.
 */
class Issue3402Test {
  private Database database;
  private static final String DB_PATH = "./target/databases/test-issue3402";

  @BeforeEach
  void setUp() {
    // Delete any existing database
    new File(DB_PATH).mkdirs();
    database = new DatabaseFactory(DB_PATH).create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Device");
    database.getSchema().createVertexType("Ping");
    database.getSchema().createEdgeType("OWNS");
    database.getSchema().createEdgeType("GENERATED");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testSimplePointCreationAndRetrieval() {
    // Create a simple ping with a point location
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (ping:Ping {location: point(48.8566, 2.3522), time: datetime('2024-01-01T12:00:00')})");
    });

    // Retrieve the ping and check that location is stored correctly
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Ping) RETURN p.location as loc")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final Object location = result.getProperty("loc");
      assertThat(location).isNotNull();
    }
  }

  @Test
  void testDistanceWithTwoPoints() {
    // Create two pings with point locations
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (ping1:Ping {id: 1, location: point(48.8566, 2.3522), time: datetime('2024-01-01T12:00:00')})");
      database.command("opencypher",
          "CREATE (ping2:Ping {id: 2, location: point(48.8606, 2.3376), time: datetime('2024-01-01T12:05:00')})");
    });

    // This should NOT throw NumberFormatException
    // Query with distance function - this is where the bug occurs
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (ping1:Ping {id: 1}), (ping2:Ping {id: 2}) " +
        "RETURN distance(ping1.location, ping2.location) as dist")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final Object dist = result.getProperty("dist");
      assertThat(dist).isNotNull();
      assertThat(dist).isInstanceOf(Number.class);

      // Distance should be approximately 1200 meters between these Paris coordinates
      // Neo4j returns meters by default, so we expect meters here
      final double distanceMeters = ((Number) dist).doubleValue();
      assertThat(distanceMeters).isGreaterThan(500);
      assertThat(distanceMeters).isLessThan(5000);
    }
  }

  @Test
  void testDistanceInWhereClause() {
    // Create test data similar to the original issue
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (p1:Person {name: 'Alice'})-[:OWNS]->(d1:Device {imei: 'IMEI_001'})-[:GENERATED]->" +
          "(ping1:Ping {location: point(48.8566, 2.3522), time: datetime('2024-01-01T12:00:00')})");
      database.command("opencypher",
          "CREATE (p2:Person {name: 'Bob'})-[:OWNS]->(d2:Device {imei: 'IMEI_002'})-[:GENERATED]->" +
          "(ping2:Ping {location: point(48.8606, 2.3376), time: datetime('2024-01-01T12:05:00')})");
    });

    // Query with distance in WHERE clause - reproduces the original issue
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (person1:Person)-[:OWNS]->(device1:Device)-[:GENERATED]->(ping1:Ping), " +
        "      (person2:Person)-[:OWNS]->(device2:Device)-[:GENERATED]->(ping2:Ping) " +
        "WHERE id(device1) < id(device2) " +
        "  AND distance(ping1.location, ping2.location) < 5000 " +
        "RETURN person1.name, person2.name, distance(ping1.location, ping2.location) as dist")) {

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((Object) result.getProperty("person1.name")).isNotNull();
      assertThat((Object) result.getProperty("person2.name")).isNotNull();
      final Object dist = result.getProperty("dist");
      assertThat(dist).isInstanceOf(Number.class);
      // Distance in meters (Neo4j default)
      assertThat(((Number) dist).doubleValue()).isLessThan(5000.0);
    }
  }

  @Test
  void testDistanceWithUnits() {
    // Create two pings with known distance
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (ping1:Ping {id: 1, location: point(48.8566, 2.3522), time: datetime('2024-01-01T12:00:00')})");
      database.command("opencypher",
          "CREATE (ping2:Ping {id: 2, location: point(48.8606, 2.3376), time: datetime('2024-01-01T12:05:00')})");
    });

    // Test default (meters for Cypher style)
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (ping1:Ping {id: 1}), (ping2:Ping {id: 2}) " +
        "RETURN distance(ping1.location, ping2.location) as dist")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final double distMeters = ((Number) result.getProperty("dist")).doubleValue();
      assertThat(distMeters).isGreaterThan(500).isLessThan(5000);
    }

    // Test explicit kilometers
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (ping1:Ping {id: 1}), (ping2:Ping {id: 2}) " +
        "RETURN distance(ping1.location, ping2.location, 'km') as dist")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final double distKm = ((Number) result.getProperty("dist")).doubleValue();
      assertThat(distKm).isGreaterThan(0.5).isLessThan(5);
    }

    // Test explicit meters
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (ping1:Ping {id: 1}), (ping2:Ping {id: 2}) " +
        "RETURN distance(ping1.location, ping2.location, 'm') as dist")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final double distMeters = ((Number) result.getProperty("dist")).doubleValue();
      assertThat(distMeters).isGreaterThan(500).isLessThan(5000);
    }
  }

  @Test
  void testComplexQueryFromIssue() {
    // Create a subset of data from the original issue (smaller for testing)
    database.transaction(() -> {
      // Create 3 persons with devices and pings
      for (int i = 1; i <= 3; i++) {
        final double lat = 48.80 + (i * 0.01);
        final double lon = 2.25 + (i * 0.01);
        database.command("opencypher",
            "CREATE (p:Person {name: 'Suspect_" + i + "'})-[:OWNS]->" +
            "(d:Device {imei: 'IMEI_" + i + "', num: '06" + i + "0000000'})-[:GENERATED]->" +
            "(ping:Ping {location: point(" + lat + ", " + lon + "), " +
            "time: datetime('2024-01-01T12:0" + i + ":00')})");
      }
    });

    // Run the problematic query from the issue
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (person1:Person)-[:OWNS]->(device1:Device)-[:GENERATED]->(ping1:Ping), " +
        "      (person2:Person)-[:OWNS]->(device2:Device)-[:GENERATED]->(ping2:Ping) " +
        "WHERE id(device1) < id(device2) " +
        "  AND ping1.time > datetime('2024-01-01T00:00:00') " +
        "  AND distance(ping1.location, ping2.location) < 1500 " +
        "RETURN person1.name, device1.imei, ping1.time, " +
        "       person2.name, device2.imei, ping2.time, " +
        "       distance(ping1.location, ping2.location) as dist_m " +
        "LIMIT 100")) {

      // Should execute without throwing NumberFormatException
      long count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        count++;
        // Verify distance is a valid number
        final Object dist = result.getProperty("dist_m");
        assertThat(dist).isInstanceOf(Number.class);
      }

      // Should find at least some matching pairs
      assertThat(count).isGreaterThan(0);
    }
  }

  @Test
  void testExactScenarioFromGitHubIssue() {
    // This test reproduces the exact scenario from GitHub issue #3402
    // Create data matching the issue's structure
    database.transaction(() -> {
      // Create a few persons with devices and pings similar to the issue
      database.command("opencypher",
          "CREATE (p:Person {name: 'Suspect_1'})-[:OWNS]->(d:Device {imei: 'IMEI_001', num: '0612345678'})");
      database.command("opencypher",
          "CREATE (p:Person {name: 'Suspect_2'})-[:OWNS]->(d:Device {imei: 'IMEI_002', num: '0687654321'})");

      // Create pings with spatial points
      database.command("opencypher",
          "MATCH (d:Device {imei: 'IMEI_001'}) " +
          "CREATE (ping:Ping {location: point(48.8566, 2.3522), time: datetime('2024-01-01T12:00:00')}), " +
          "(d)-[:GENERATED]->(ping)");
      database.command("opencypher",
          "MATCH (d:Device {imei: 'IMEI_002'}) " +
          "CREATE (ping:Ping {location: point(48.8606, 2.3376), time: datetime('2024-01-01T12:05:00')}), " +
          "(d)-[:GENERATED]->(ping)");
    });

    // This is the exact query structure from the issue (adapted for smaller dataset)
    // Should NOT throw "For input string: \"POINT (48.885347 2.322881)\""
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (person1:Person)-[:OWNS]->(device1:Device)-[:GENERATED]->(ping1:Ping), " +
        "      (person2:Person)-[:OWNS]->(device2:Device)-[:GENERATED]->(ping2:Ping) " +
        "WHERE id(device1) < id(device2) " +
        "  AND ping1.time > datetime('2024-01-01T00:00:00') " +
        "  AND distance(ping1.location, ping2.location) < 1500 " +
        "RETURN person1.name, device1.imei, ping1.time, " +
        "       person2.name, device2.imei, ping2.time, " +
        "       distance(ping1.location, ping2.location) as dist_m " +
        "LIMIT 100")) {

      // Should successfully execute without NumberFormatException
      assertThat(rs.hasNext()).isTrue();
      final var result = rs.next();

      // Verify all expected fields are present
      assertThat((Object) result.getProperty("person1.name")).isNotNull();
      assertThat((Object) result.getProperty("person2.name")).isNotNull();
      assertThat((Object) result.getProperty("device1.imei")).isNotNull();
      assertThat((Object) result.getProperty("device2.imei")).isNotNull();

      // Verify distance is returned in meters (Neo4j default)
      final Object dist = result.getProperty("dist_m");
      assertThat(dist).isInstanceOf(Number.class);
      final double distMeters = ((Number) dist).doubleValue();
      assertThat(distMeters).isGreaterThan(0).isLessThan(1500);
    }
  }
}
