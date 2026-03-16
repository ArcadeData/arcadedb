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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.test.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.*;

import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance test for single-edge creation via Bolt protocol.
 * Reproduces issue #3613: edge creation via MATCH...MATCH...CREATE is significantly
 * slower than expected compared to lookup queries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BoltEdgeCreationPerformanceIT extends BaseGraphServerTest {

  private static final int PERSON_COUNT = 500;
  private static final int EDGE_COUNT = 500;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Driver getDriver() {
    return GraphDatabase.driver(
        "bolt://localhost:7687",
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder()
            .withoutEncryption()
            .build()
    );
  }

  @Test
  void edgeCreationPerformanceViaBolt() {
    final Database db = getServerDatabase(0, getDatabaseName());

    // Setup schema and data using direct API for speed
    db.transaction(() -> {
      if (!db.getSchema().existsType("PerfPerson")) {
        final var type = db.getSchema().createVertexType("PerfPerson");
        type.createProperty("id", Type.INTEGER);
        db.getSchema().createEdgeType("PERF_KNOWS");
      }
    });
    db.transaction(() -> {
      db.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "PerfPerson", "id");
    });

    // Create Person vertices in batch
    db.transaction(() -> {
      for (int i = 0; i < PERSON_COUNT; i++) {
        final MutableVertex v = db.newVertex("PerfPerson");
        v.set("id", i);
        v.set("name", "Person_" + i);
        v.save();
      }
    });

    // ---- Measure Bolt edge creation performance ----
    try (final Driver driver = getDriver()) {
      // Warm up: run a few lookups
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        for (int i = 0; i < 10; i++) {
          session.run("MATCH (p:PerfPerson) WHERE p.id = $id RETURN p.id",
              Map.of("id", i)).consume();
        }
      }

      // Measure lookup latency (baseline)
      final long[] lookupTimes = new long[EDGE_COUNT];
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        for (int i = 0; i < EDGE_COUNT; i++) {
          final long start = System.nanoTime();
          session.run("MATCH (p:PerfPerson) WHERE p.id = $id RETURN p.id",
              Map.of("id", i % PERSON_COUNT)).consume();
          lookupTimes[i] = System.nanoTime() - start;
        }
      }

      // Warm up edge creation
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        for (int i = 0; i < 5; i++) {
          try (final Transaction tx = session.beginTransaction()) {
            tx.run("MATCH (a:PerfPerson) WHERE a.id = $src " +
                    "MATCH (b:PerfPerson) WHERE b.id = $dst " +
                    "CREATE (a)-[:PERF_KNOWS {weight: $w}]->(b)",
                Map.of("src", i, "dst", i + 1, "w", 0.5)).consume();
            tx.commit();
          }
        }
      }

      // Measure single-edge creation latency via explicit Bolt transactions
      final long[] edgeTimes = new long[EDGE_COUNT];
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        for (int i = 0; i < EDGE_COUNT; i++) {
          final int src = i % PERSON_COUNT;
          final int dst = (i + 1) % PERSON_COUNT;
          final long start = System.nanoTime();
          try (final Transaction tx = session.beginTransaction()) {
            tx.run("MATCH (a:PerfPerson) WHERE a.id = $src " +
                    "MATCH (b:PerfPerson) WHERE b.id = $dst " +
                    "CREATE (a)-[:PERF_KNOWS {weight: $w}]->(b)",
                Map.of("src", src, "dst", dst, "w", 0.5)).consume();
            tx.commit();
          }
          edgeTimes[i] = System.nanoTime() - start;
        }
      }

      // Measure edge creation latency via auto-commit (implicit transactions)
      final long[] autoCommitTimes = new long[EDGE_COUNT];
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        for (int i = 0; i < EDGE_COUNT; i++) {
          final int src = i % PERSON_COUNT;
          final int dst = (i + 1) % PERSON_COUNT;
          final long start = System.nanoTime();
          session.run("MATCH (a:PerfPerson) WHERE a.id = $src " +
                  "MATCH (b:PerfPerson) WHERE b.id = $dst " +
                  "CREATE (a)-[:PERF_KNOWS {weight: $w}]->(b)",
              Map.of("src", src, "dst", dst, "w", 0.5)).consume();
          autoCommitTimes[i] = System.nanoTime() - start;
        }
      }

      // Measure direct Cypher edge creation (no Bolt, direct API)
      final long[] directCypherTimes = new long[EDGE_COUNT];
      for (int i = 0; i < EDGE_COUNT; i++) {
        final int src = i % PERSON_COUNT;
        final int dst = (i + 1) % PERSON_COUNT;
        final long start = System.nanoTime();
        db.command("opencypher",
            "MATCH (a:PerfPerson) WHERE a.id = $src " +
                "MATCH (b:PerfPerson) WHERE b.id = $dst " +
                "CREATE (a)-[:PERF_KNOWS {weight: $w}]->(b)",
            Map.of("src", src, "dst", dst, "w", 0.5));
        directCypherTimes[i] = System.nanoTime() - start;
      }

      // Measure direct API edge creation (no Cypher, no Bolt)
      final long[] directApiTimes = new long[EDGE_COUNT];
      for (int i = 0; i < EDGE_COUNT; i++) {
        final int src = i % PERSON_COUNT;
        final int dst = (i + 1) % PERSON_COUNT;
        final long start = System.nanoTime();
        db.transaction(() -> {
          final var srcVertex = db.query("sql",
              "SELECT FROM PerfPerson WHERE id = ?", src).nextIfAvailable().getRecord().get().asVertex();
          final var dstVertex = db.query("sql",
              "SELECT FROM PerfPerson WHERE id = ?", dst).nextIfAvailable().getRecord().get().asVertex();
          srcVertex.newEdge("PERF_KNOWS", dstVertex, "weight", 0.5);
        });
        directApiTimes[i] = System.nanoTime() - start;
      }

      // Calculate statistics
      Arrays.sort(lookupTimes);
      Arrays.sort(edgeTimes);
      Arrays.sort(autoCommitTimes);
      Arrays.sort(directCypherTimes);
      Arrays.sort(directApiTimes);

      final double lookupMedianMs = lookupTimes[EDGE_COUNT / 2] / 1_000_000.0;
      final double edgeMedianMs = edgeTimes[EDGE_COUNT / 2] / 1_000_000.0;
      final double autoCommitMedianMs = autoCommitTimes[EDGE_COUNT / 2] / 1_000_000.0;
      final double directCypherMedianMs = directCypherTimes[EDGE_COUNT / 2] / 1_000_000.0;
      final double directApiMedianMs = directApiTimes[EDGE_COUNT / 2] / 1_000_000.0;

      final double lookupMinMs = lookupTimes[0] / 1_000_000.0;
      final double edgeMinMs = edgeTimes[0] / 1_000_000.0;
      final double autoCommitMinMs = autoCommitTimes[0] / 1_000_000.0;
      final double directCypherMinMs = directCypherTimes[0] / 1_000_000.0;
      final double directApiMinMs = directApiTimes[0] / 1_000_000.0;

      final double lookupP95Ms = lookupTimes[(int) (EDGE_COUNT * 0.95)] / 1_000_000.0;
      final double edgeP95Ms = edgeTimes[(int) (EDGE_COUNT * 0.95)] / 1_000_000.0;
      final double autoCommitP95Ms = autoCommitTimes[(int) (EDGE_COUNT * 0.95)] / 1_000_000.0;
      final double directCypherP95Ms = directCypherTimes[(int) (EDGE_COUNT * 0.95)] / 1_000_000.0;
      final double directApiP95Ms = directApiTimes[(int) (EDGE_COUNT * 0.95)] / 1_000_000.0;

      // Report results
      final String report = String.format(
          "\n=== Edge Creation Performance (Issue #3613) ===" +
              "\n%-30s %10s %10s %10s" +
              "\n%-30s %10.2f %10.2f %10.2f" +
              "\n%-30s %10.2f %10.2f %10.2f" +
              "\n%-30s %10.2f %10.2f %10.2f" +
              "\n%-30s %10.2f %10.2f %10.2f" +
              "\n%-30s %10.2f %10.2f %10.2f" +
              "\n\nEdge/Lookup ratio (Bolt):     %.1fx" +
              "\nEdge/Lookup ratio (AutoCommit):%.1fx" +
              "\nEdge/Lookup ratio (Cypher):    %.1fx" +
              "\nEdge/Lookup ratio (DirectAPI): %.1fx",
          "Operation", "Median(ms)", "Min(ms)", "P95(ms)",
          "Bolt Lookup", lookupMedianMs, lookupMinMs, lookupP95Ms,
          "Bolt Edge (explicit tx)", edgeMedianMs, edgeMinMs, edgeP95Ms,
          "Bolt Edge (auto-commit)", autoCommitMedianMs, autoCommitMinMs, autoCommitP95Ms,
          "Direct Cypher Edge", directCypherMedianMs, directCypherMinMs, directCypherP95Ms,
          "Direct API Edge", directApiMedianMs, directApiMinMs, directApiP95Ms,
          edgeMedianMs / lookupMedianMs,
          autoCommitMedianMs / lookupMedianMs,
          directCypherMedianMs / lookupMedianMs,
          directApiMedianMs / lookupMedianMs
      );

      // Log results (visible in test output)
      LogManager.instance().log(this, Level.INFO, report);

      // The edge creation should not be more than 15x the lookup latency
      // (Neo4j shows ~1.3x ratio, we aim for <10x as a reasonable target)
      final double ratio = edgeMedianMs / lookupMedianMs;
      assertThat(ratio)
          .as("Edge creation via Bolt should not be more than 15x slower than lookup. " +
              "Actual ratio: %.1fx. Lookup median: %.2fms, Edge median: %.2fms", ratio, lookupMedianMs, edgeMedianMs)
          .isLessThan(15.0);
    }
  }
}
