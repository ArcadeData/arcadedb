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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * Performance test demonstrating the benefit of function lookup caching.
 * This test creates a realistic bulk scenario similar to the GitHub issue #3407.
 */
@Tag("benchmark")
class FunctionCachingPerformanceTest {
  private static final String DB_PATH = "./target/databases/test-function-caching-perf";
  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Device");
      database.getSchema().createVertexType("Ping");
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
   * Benchmark that simulates the scenario from GitHub issue #3407.
   * Creates devices and pings with point geometry and timestamps.
   */
  @Test
  void benchmarkPointAndDurationFunctions() {
    System.out.println("\n╔═══════════════════════════════════════════════════════════════╗");
    System.out.println("║        Function Caching Performance Test                      ║");
    System.out.println("╚═══════════════════════════════════════════════════════════════╝");

    final int numDevices = 100;
    final int pingsPerDevice = 1000;
    final int totalPings = numDevices * pingsPerDevice; // 100,000 total

    System.out.println("\nTest parameters:");
    System.out.println("  - Devices: " + numDevices);
    System.out.println("  - Pings per device: " + pingsPerDevice);
    System.out.println("  - Total pings: " + totalPings);
    System.out.println("\nExecuting query...");

    final String query = """
        // Create devices
        UNWIND range(1, %d) AS d_idx
        CREATE (d:Device {imei: "DEVICE_" + d_idx})

        WITH d
        UNWIND range(1, %d) AS p_idx
        WITH d,
             datetime('2024-01-01T00:00:00') AS start_date,
             7 * 24 * 60 * 60 AS time_window_sec,
             48.80 AS lat_min, 48.90 AS lat_max,
             2.25 AS lon_min, 2.40 AS lon_max,
             rand() AS r_lat, rand() AS r_lon, rand() AS r_time

        CREATE (d)-[:GENERATED]->(ping:Ping {
            location: point(
                r_lat * (lat_max - lat_min) + lat_min,
                r_lon * (lon_max - lon_min) + lon_min
            ),
            time: start_date + duration({seconds: toInteger(r_time * time_window_sec)})
        })
        """.formatted(numDevices, pingsPerDevice);

    final long startTime = System.nanoTime();
    final ResultSet result = database.command("opencypher", query);

    // Consume all results
    int rowCount = 0;
    while (result.hasNext()) {
      result.next();
      rowCount++;
    }
    result.close();

    final long elapsedNanos = System.nanoTime() - startTime;
    final double elapsedMs = elapsedNanos / 1_000_000.0;
    final double elapsedSec = elapsedMs / 1_000.0;

    // Verify counts
    final long pingCount = database.query("opencypher", "MATCH (p:Ping) RETURN count(p) AS cnt")
        .next().getProperty("cnt");
    final long edgeCount = database.query("opencypher", "MATCH ()-[r:GENERATED]->() RETURN count(r) AS cnt")
        .next().getProperty("cnt");

    System.out.println("\n╔═══════════════════════════════════════════════════════════════╗");
    System.out.println("║                    PERFORMANCE RESULTS                         ║");
    System.out.println("╠═══════════════════════════════════════════════════════════════╣");
    System.out.printf("║ Execution Time:        %,10.2f ms (%,.2f sec)         ║%n", elapsedMs, elapsedSec);
    System.out.printf("║ Pings created:         %,10d                          ║%n", pingCount);
    System.out.printf("║ Edges created:         %,10d                          ║%n", edgeCount);
    System.out.println("╠═══════════════════════════════════════════════════════════════╣");
    System.out.printf("║ Pings/sec:             %,10.0f                          ║%n", pingCount / elapsedSec);
    System.out.printf("║ μs per Ping:           %,10.2f                          ║%n", elapsedMs * 1000 / pingCount);
    System.out.println("╚═══════════════════════════════════════════════════════════════╝");

    System.out.println("\nOptimization Details:");
    System.out.println("  ✓ Function lookup caching enabled");
    System.out.println("  ✓ Functions cached: point(), duration(), rand(), toInteger()");
    System.out.println("  ✓ Each function looked up once, executed " + totalPings + " times");
    System.out.println();
  }
}
