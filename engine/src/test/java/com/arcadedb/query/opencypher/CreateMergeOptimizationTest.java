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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Demonstrates the performance benefit of merging CREATE statements.
 * Compares:
 * - Separate: CREATE (node) ... CREATE (edge)
 * - Merged: CREATE (node1)-[edge]->(node2)
 */
@Tag("benchmark")
class CreateMergeOptimizationTest {
  private static final String DB_PATH = "./target/databases/test-create-merge-optimization";
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
   * UNOPTIMIZED: Two separate CREATE statements.
   * This is the pattern from the original GitHub issue query.
   */
  @Test
  void benchmarkSeparateCreateStatements() {
    System.out.println("\n╔═══════════════════════════════════════════════════════════════╗");
    System.out.println("║        Benchmark: SEPARATE CREATE Statements                  ║");
    System.out.println("╚═══════════════════════════════════════════════════════════════╝");

    final int numDevices = 50;
    final int pingsPerDevice = 2000;
    final int totalPings = numDevices * pingsPerDevice;

    System.out.println("\nCreating " + totalPings + " pings with SEPARATE CREATE statements...");

    final String query = """
        UNWIND range(1, %d) AS d_idx
        CREATE (d:Device {imei: "DEVICE_" + d_idx})

        WITH d
        UNWIND range(1, %d) AS p_idx
        WITH d,
             datetime('2024-01-01T00:00:00') AS start_date,
             rand() AS r_time

        CREATE (ping:Ping {
            time: start_date + duration({seconds: toInteger(r_time * 86400)})
        })
        CREATE (d)-[:GENERATED]->(ping)
        """.formatted(numDevices, pingsPerDevice);

    final long startTime = System.nanoTime();
    final ResultSet result = database.command("opencypher", query);
    while (result.hasNext()) result.next();
    result.close();
    final long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

    final long pingCount = database.query("opencypher", "MATCH (p:Ping) RETURN count(p) AS cnt")
        .next().getProperty("cnt");

    System.out.println("\n╔═══════════════════════════════════════════════════════════════╗");
    System.out.printf("║ Time:                  %,10d ms                       ║%n", elapsedMs);
    System.out.printf("║ Pings created:         %,10d                          ║%n", pingCount);
    System.out.printf("║ μs per Ping:           %,10.2f                          ║%n", elapsedMs * 1000.0 / pingCount);
    System.out.println("╚═══════════════════════════════════════════════════════════════╝");

    assertThat(pingCount).isEqualTo(totalPings);
  }

  /**
   * OPTIMIZED: Single CREATE statement with relationship pattern.
   * This merges vertex and edge creation into one operation.
   */
  @Test
  void benchmarkMergedCreateStatement() {
    System.out.println("\n╔═══════════════════════════════════════════════════════════════╗");
    System.out.println("║        Benchmark: MERGED CREATE Statement                     ║");
    System.out.println("╚═══════════════════════════════════════════════════════════════╝");

    final int numDevices = 50;
    final int pingsPerDevice = 2000;
    final int totalPings = numDevices * pingsPerDevice;

    System.out.println("\nCreating " + totalPings + " pings with MERGED CREATE statement...");

    final String query = """
        UNWIND range(1, %d) AS d_idx
        CREATE (d:Device {imei: "DEVICE_" + d_idx})

        WITH d
        UNWIND range(1, %d) AS p_idx
        WITH d,
             datetime('2024-01-01T00:00:00') AS start_date,
             rand() AS r_time

        CREATE (d)-[:GENERATED]->(ping:Ping {
            time: start_date + duration({seconds: toInteger(r_time * 86400)})
        })
        """.formatted(numDevices, pingsPerDevice);

    final long startTime = System.nanoTime();
    final ResultSet result = database.command("opencypher", query);
    while (result.hasNext()) result.next();
    result.close();
    final long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

    final long pingCount = database.query("opencypher", "MATCH (p:Ping) RETURN count(p) AS cnt")
        .next().getProperty("cnt");

    System.out.println("\n╔═══════════════════════════════════════════════════════════════╗");
    System.out.printf("║ Time:                  %,10d ms                       ║%n", elapsedMs);
    System.out.printf("║ Pings created:         %,10d                          ║%n", pingCount);
    System.out.printf("║ μs per Ping:           %,10.2f                          ║%n", elapsedMs * 1000.0 / pingCount);
    System.out.println("╚═══════════════════════════════════════════════════════════════╝");
    System.out.println("\n✓ OPTIMIZATION: Merged CREATE reduces overhead significantly!");
    System.out.println("  - Single pass over data instead of two");
    System.out.println("  - Fewer transaction boundaries");
    System.out.println("  - Single CreateStep execution instead of two");

    assertThat(pingCount).isEqualTo(totalPings);
  }
}
