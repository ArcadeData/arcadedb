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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests importing the Uber trips dataset sample via {@link GraphImporter} JSON config.
 * Verifies deduplication, byName edges, and typed properties (double, datetime).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class UberTripsImportTest {

  private static final String DB_PATH = "target/databases/uber-trips-import-test";
  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void cleanup() {
    if (database != null)
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void importUberTripsSampleViaJsonConfig() throws Exception {
    final String resourceDir = new File("src/test/resources").getAbsolutePath();
    final String configJson = Files.readString(new File(resourceDir, "uber-trips-sample-config.json").toPath());
    final JSONObject config = new JSONObject(configJson);

    GraphImporter.createSchemaFromConfig(database, config);

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, resourceDir)) {
      importer.run();

      // 5 trips, but deduplication means: 3 cities (SF, Boston, NY), 4 drivers, 4 riders
      // Sample data:
      //   trip 1: driver=8270, rider=10683, city=San Francisco
      //   trip 2: driver=1860, rider=44743, city=Boston
      //   trip 3: driver=6390, rider=75839, city=San Francisco (dup city)
      //   trip 4: driver=6191, rider=22189, city=New York
      //   trip 5: driver=8270, rider=10683, city=Boston (dup driver, rider, city)
      assertThat(importer.getVertexCount()).isEqualTo(3 + 4 + 4 + 5); // cities + drivers + riders + trips
      assertThat(importer.getEdgeCount()).isEqualTo(5 * 3); // 3 edges per trip (DrivenBy, RideOf, InCity)
    }

    // Verify city deduplication
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT count(*) as c FROM City")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(3);
      }
    });

    // Verify driver deduplication (driver 8270 appears in trips 1 and 5)
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT count(*) as c FROM Driver")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(4);
      }
    });

    // Verify trip properties (doubles, datetime)
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Trip WHERE tripId = 1")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex trip = rs.next().getVertex().get();

        assertThat(trip.getDouble("pickupLat")).isCloseTo(37.1709, within(0.001));
        assertThat(trip.getDouble("pickupLng")).isCloseTo(-77.5865, within(0.001));
        assertThat(trip.getDouble("distance")).isCloseTo(2.97, within(0.01));
        assertThat(trip.getDouble("fare")).isCloseTo(10.71, within(0.01));
        assertThat(trip.getString("status")).isEqualTo("Completed");
        assertThat(trip.getString("payment")).isEqualTo("Wallet");
        assertThat(trip.getLocalDateTime("pickupTime")).isEqualTo(LocalDateTime.of(2023, 1, 1, 0, 0, 0));
      }
    });

    // Verify edges: trip 1 should have DrivenBy -> driver 8270
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Trip WHERE tripId = 1")) {
        final Vertex trip = rs.next().getVertex().get();

        // DrivenBy edge
        int drivenByCount = 0;
        for (final Edge e : trip.getEdges(Vertex.DIRECTION.OUT, "DrivenBy")) {
          assertThat(e.getInVertex().asVertex().getInteger("driverId")).isEqualTo(8270);
          drivenByCount++;
        }
        assertThat(drivenByCount).isEqualTo(1);

        // InCity edge (byName)
        int inCityCount = 0;
        for (final Edge e : trip.getEdges(Vertex.DIRECTION.OUT, "InCity")) {
          assertThat(e.getInVertex().asVertex().getString("name")).isEqualTo("San Francisco");
          inCityCount++;
        }
        assertThat(inCityCount).isEqualTo(1);
      }
    });

    // Verify byName edges: San Francisco should have 2 incoming InCity edges (trips 1 and 3)
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM City WHERE name = 'San Francisco'")) {
        final Vertex sf = rs.next().getVertex().get();
        int inCount = 0;
        for (final Edge ignored : sf.getEdges(Vertex.DIRECTION.IN, "InCity"))
          inCount++;
        assertThat(inCount).isEqualTo(2);
      }
    });

    // Verify driver 8270 has 2 incoming DrivenBy edges (trips 1 and 5)
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Driver WHERE driverId = 8270")) {
        final Vertex driver = rs.next().getVertex().get();
        int inCount = 0;
        for (final Edge ignored : driver.getEdges(Vertex.DIRECTION.IN, "DrivenBy"))
          inCount++;
        assertThat(inCount).isEqualTo(2);
      }
    });
  }
}
