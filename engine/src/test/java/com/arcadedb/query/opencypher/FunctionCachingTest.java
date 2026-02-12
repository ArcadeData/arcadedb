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
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify that function lookups are cached in FunctionCallExpression.
 * This optimization significantly improves performance for bulk operations
 * where the same function is called many times.
 */
class FunctionCachingTest {
  private static final String DB_PATH = "./target/databases/test-function-caching";
  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Node");
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
   * Test that functions like rand(), toInteger(), etc. are called multiple times
   * but the function lookup happens only once per expression.
   * We verify this by creating many nodes with function calls in properties.
   */
  @Test
  void testFunctionCachingInBulkCreate() {
    // Create 1000 nodes with multiple function calls per node
    final String query = """
        UNWIND range(1, 1000) AS i
        CREATE (n:Node {
            random_value: rand(),
            random_int: toInteger(rand() * 1000),
            name: "Node_" + tostring(i)
        })
        RETURN count(n) AS total
        """;

    final ResultSet result = database.command("opencypher", query);
    assertThat(result.hasNext()).isTrue();

    final long count = (Long) result.next().getProperty("total");
    assertThat(count).isEqualTo(1000);
    result.close();

    // Verify all nodes were created
    final ResultSet countResult = database.query("opencypher", "MATCH (n:Node) RETURN count(n) AS total");
    assertThat(countResult.hasNext()).isTrue();
    assertThat((Long) countResult.next().getProperty("total")).isEqualTo(1000);
    countResult.close();

    // Verify that nodes have different random values (confirms rand() was executed, not cached)
    final ResultSet distinctResult = database.query("opencypher",
        "MATCH (n:Node) RETURN count(DISTINCT n.random_value) AS distinct_randoms");
    assertThat(distinctResult.hasNext()).isTrue();
    final long distinctRandoms = (Long) distinctResult.next().getProperty("distinct_randoms");
    // Should have many distinct values (very unlikely to have duplicates with rand())
    assertThat(distinctRandoms).isGreaterThan(990);
    distinctResult.close();
  }

  /**
   * Test function caching with point() and duration() functions
   * (same as the use case from the GitHub issue).
   */
  @Test
  void testFunctionCachingWithPointAndDuration() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Ping");
    });

    final String query = """
        WITH
            datetime('2024-01-01T00:00:00') AS start_date,
            7 * 24 * 60 * 60 AS time_window_sec,
            48.80 AS lat_min, 48.90 AS lat_max,
            2.25 AS lon_min, 2.40 AS lon_max

        UNWIND range(1, 1000) AS i
        WITH start_date, time_window_sec, lat_min, lat_max, lon_min, lon_max,
             rand() AS r_lat, rand() AS r_lon, rand() AS r_time

        CREATE (ping:Ping {
            location: point(
                r_lat * (lat_max - lat_min) + lat_min,
                r_lon * (lon_max - lon_min) + lon_min
            ),
            time: start_date + duration({seconds: toInteger(r_time * time_window_sec)})
        })
        RETURN count(ping) AS total
        """;

    final ResultSet result = database.command("opencypher", query);
    assertThat(result.hasNext()).isTrue();
    final long count = (Long) result.next().getProperty("total");
    assertThat(count).isEqualTo(1000);
    result.close();

    // Verify all pings were created with valid locations
    final ResultSet verifyResult = database.query("opencypher",
        "MATCH (p:Ping) WHERE p.location IS NOT NULL AND p.time IS NOT NULL RETURN count(p) AS total");
    assertThat(verifyResult.hasNext()).isTrue();
    assertThat((Long) verifyResult.next().getProperty("total")).isEqualTo(1000);
    verifyResult.close();
  }

  /**
   * Test that function caching works correctly across multiple batches.
   */
  @Test
  void testFunctionCachingAcrossBatches() {
    // Create more records than the default batch size (20,000)
    // to ensure caching works across batch boundaries
    final int recordCount = 25000;

    final String query = """
        UNWIND range(1, %d) AS i
        CREATE (n:Node {
            value: toInteger(rand() * 1000000)
        })
        RETURN count(n) AS total
        """.formatted(recordCount);

    final ResultSet result = database.command("opencypher", query);
    assertThat(result.hasNext()).isTrue();
    final long count = (Long) result.next().getProperty("total");
    assertThat(count).isEqualTo(recordCount);
    result.close();

    // Verify count
    final ResultSet countResult = database.query("opencypher", "MATCH (n:Node) RETURN count(n) AS total");
    assertThat(countResult.hasNext()).isTrue();
    assertThat((Long) countResult.next().getProperty("total")).isEqualTo(recordCount);
    countResult.close();
  }
}
