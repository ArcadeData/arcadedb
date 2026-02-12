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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * Benchmark for bulk CREATE operations in OpenCypher.
 * <p>
 * This benchmark measures the performance of bulk inserts using UNWIND + CREATE pattern,
 * which is a common use case for data loading and generation scenarios.
 * <p>
 * The test reproduces a realistic scenario:
 * - Create 500 Person vertices
 * - Each person owns 1-3 Device vertices (distributed: 90% have 1, 7% have 2, 3% have 3)
 * - Each device generates ~500 Ping vertices with point geometry and timestamp properties
 * - Total: ~500 persons + ~567 devices + ~283,500 pings = ~284,567 vertices + ~284,067 edges
 * <p>
 * Performance baseline (before optimization):
 * - Expected time: ~8-10 seconds for full dataset
 * - Bottleneck: per-record transaction overhead in CreateStep
 */
@Tag("benchmark")
class BulkCreateBenchmark {
  private static final String DB_PATH = "./target/databases/test-bulk-create-benchmark";

  private Database database;

  private static final int PERSONS_MEDIUM = 500;  // Default test size

  private static final int PINGS_MIN = 100;
  private static final int PINGS_MAX = 100;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.OPENCYPHER_BULK_CREATE_BATCH_SIZE.setValue(50_000);

    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();

    // Create schema
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Device");
      database.getSchema().createVertexType("Ping");
      database.getSchema().createEdgeType("OWNS");
      database.getSchema().createEdgeType("GENERATED");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Benchmark with medium dataset matching the original query (500 persons, ~283K pings).
   */
  @Test
  void benchmarkMediumDataset() {
    runBenchmark("Medium Dataset (500 persons)", PERSONS_MEDIUM, 500, 500);
  }

  /**
   * Runs the bulk CREATE benchmark with specified parameters.
   */
  private void runBenchmark(final String testName, final int nbPersons, final int minPings, final int maxPings) {
    System.out.println("\n╔═══════════════════════════════════════════════════════════════════╗");
    System.out.println("║ " + String.format("%-65s", "Bulk CREATE Benchmark: " + testName) + " ║");
    System.out.println("╚═══════════════════════════════════════════════════════════════════╝");
    System.out.println();

    final String query = buildBulkCreateQuery(nbPersons, minPings, maxPings);

    // Single execution with timing
    System.out.println("Executing bulk CREATE query...");
    System.out.println("Parameters:");
    System.out.println("  - Persons: " + nbPersons);
    System.out.println("  - Pings per device: " + minPings + "-" + maxPings);
    System.out.println("  - Expected total vertices: ~" + estimateVertices(nbPersons, minPings));
    System.out.println("  - Expected total edges: ~" + estimateEdges(nbPersons, minPings));
    System.out.println();

    final long startTime = System.nanoTime();
    final ResultSet result = database.command("opencypher", query);

    // Consume all results
    int rowCount = 0;
    while (result.hasNext()) {
      result.next();
      rowCount++;
    }
    result.close();

    final long elapsedTime = System.nanoTime() - startTime;
    final double elapsedMs = elapsedTime / 1_000_000.0;
    final double elapsedSec = elapsedMs / 1_000.0;

    System.out.println(result);

    // Count created entities
    final long personCount =
        database.query("opencypher", "MATCH (p:Person) RETURN count(p) AS cnt").next().getProperty("cnt");
    final long deviceCount =
        database.query("opencypher", "MATCH (d:Device) RETURN count(d) AS cnt").next().getProperty("cnt");
    final long pingCount = database.query("opencypher", "MATCH (p:Ping) RETURN count(p) AS cnt").next().getProperty(
        "cnt");
    final long ownsCount =
        database.query("opencypher", "MATCH ()-[r:OWNS]->() RETURN count(r) AS cnt").next().getProperty("cnt");
    final long generatedCount =
        database.query("opencypher", "MATCH ()-[r:GENERATED]->() RETURN count(r) AS cnt").next().getProperty("cnt");

    final long totalVertices = personCount + deviceCount + pingCount;
    final long totalEdges = ownsCount + generatedCount;

    // Calculate throughput
    final double verticesPerSec = totalVertices / elapsedSec;
    final double edgesPerSec = totalEdges / elapsedSec;
    final double totalPerSec = (totalVertices + totalEdges) / elapsedSec;

    // Display results
    System.out.println("╔═══════════════════════════════════════════════════════════════════╗");
    System.out.println("║                        BENCHMARK RESULTS                          ║");
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
    System.out.println(String.format("║ Execution Time:        %,10.2f ms (%,.2f sec)              ║", elapsedMs,
        elapsedSec));
    System.out.println(String.format("║ Query returned:        %,10d rows                          ║", rowCount));
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
    System.out.println("║                      ENTITIES CREATED                             ║");
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
    System.out.println(String.format("║ Person vertices:       %,10d                               ║", personCount));
    System.out.println(String.format("║ Device vertices:       %,10d                               ║", deviceCount));
    System.out.println(String.format("║ Ping vertices:         %,10d                               ║", pingCount));
    System.out.println(String.format("║ OWNS edges:            %,10d                               ║", ownsCount));
    System.out.println(String.format("║ GENERATED edges:       %,10d                               ║", generatedCount));
    System.out.println("╟───────────────────────────────────────────────────────────────────╢");
    System.out.println(String.format("║ Total vertices:        %,10d                               ║", totalVertices));
    System.out.println(String.format("║ Total edges:           %,10d                               ║", totalEdges));
    System.out.println(String.format("║ Total entities:        %,10d                               ║",
        totalVertices + totalEdges));
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
    System.out.println("║                         THROUGHPUT                                ║");
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
    System.out.println(String.format("║ Vertices/sec:          %,10.0f                               ║",
        verticesPerSec));
    System.out.println(String.format("║ Edges/sec:             %,10.0f                               ║", edgesPerSec));
    System.out.println(String.format("║ Total entities/sec:    %,10.0f                               ║", totalPerSec));
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
    System.out.println("║                    PERFORMANCE METRICS                            ║");
    System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
    System.out.println(String.format("║ μs per vertex:         %,10.2f                               ║",
        elapsedMs * 1000 / totalVertices));
    System.out.println(String.format("║ μs per edge:           %,10.2f                               ║",
        elapsedMs * 1000 / totalEdges));
    System.out.println(String.format("║ μs per Ping creation:  %,10.2f (vertex + edge)               ║",
        elapsedMs * 1000 / pingCount));
    System.out.println("╚═══════════════════════════════════════════════════════════════════╝");
  }

  /**
   * Builds the bulk CREATE query matching the original user query.
   */
  private String buildBulkCreateQuery(final int nbPersons, final int minPings, final int maxPings) {
    return """
        PROFILE WITH
            %d AS nb_persons,
            %d AS min_pings,
            %d AS max_pings,
            datetime('2024-01-01T00:00:00') AS start_date,
            7 * 24 * 60 * 60 AS time_window_sec,
            48.80 AS lat_min, 48.90 AS lat_max,
            2.25 AS lon_min, 2.40 AS lon_max

        UNWIND range(1, nb_persons) AS i
        WITH i, min_pings, max_pings, start_date, time_window_sec, lat_min, lat_max, lon_min, lon_max,
             rand() AS r_dev_distro

        WITH i, min_pings, max_pings, start_date, time_window_sec, lat_min, lat_max, lon_min, lon_max,
             CASE
                WHEN r_dev_distro < 0.90 THEN 1
                WHEN r_dev_distro < 0.97 THEN 2
                ELSE 3
             END AS num_devices

        CREATE (p:Person {
            name: "Suspect_" + tostring(rand()),
            dataset_batch: start_date
        })

        WITH p, num_devices, min_pings, max_pings, start_date, time_window_sec, lat_min, lat_max, lon_min, lon_max
        UNWIND range(1, num_devices) AS d_idx
        CREATE (d:Device {
            imei: "IMEI_" + split(p.name, '_')[1] + "_" + d_idx,
            num:  "06" + toInteger(rand() * 89999999 + 10000000)
        })
        CREATE (p)-[:OWNS]->(d)

        WITH d, min_pings, max_pings, start_date, time_window_sec, lat_min, lat_max, lon_min, lon_max,
             toInteger(rand() * (max_pings - min_pings) + min_pings) AS nb_pings_target

        UNWIND range(1, nb_pings_target) AS p_idx

        WITH d, start_date, time_window_sec,
               lat_min, lat_max, lon_min, lon_max,
               rand() AS r_lat, rand() AS r_lon, rand() AS r_time

        WITH d, start_date, time_window_sec,
               (r_lat * (lat_max - lat_min) + lat_min) AS x,
               (r_lon * (lon_max - lon_min) + lon_min) AS y,
               r_time

        CREATE (ping:Ping {
            location: point(x, y),
            time: start_date + duration({seconds: toInteger(r_time * time_window_sec)})
        })

        CREATE (d)-[:GENERATED]->(ping)
        """.formatted(nbPersons, minPings, maxPings);
  }

  private int estimateVertices(final int nbPersons, final int avgPings) {
    // Assuming average 1.13 devices per person (90% * 1 + 7% * 2 + 3% * 3)
    final int avgDevices = (int) (nbPersons * 1.13);
    final int pings = avgDevices * avgPings;
    return nbPersons + avgDevices + pings;
  }

  private int estimateEdges(final int nbPersons, final int avgPings) {
    final int avgDevices = (int) (nbPersons * 1.13);
    final int pings = avgDevices * avgPings;
    return avgDevices + pings; // OWNS + GENERATED
  }
}
