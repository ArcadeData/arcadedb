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
package com.arcadedb;

import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;

/**
 * Profiles the commit path for single-edge creation to identify bottlenecks.
 * Related to issue #3613: edge creation performance via Bolt protocol.
 *
 * Breaks down the total time into:
 * 1. Vertex lookup (index scan)
 * 2. Edge creation (in-memory mutation)
 * 3. Commit (WAL write, page flush, lock/unlock)
 * 4. Full Cypher query (parsing + execution + commit)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EdgeCreationCommitProfileTest extends TestHelper {

  private static final int VERTEX_COUNT = 500;
  private static final int ITERATIONS   = 500;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("ProfPerson")) {
        final var type = database.getSchema().createVertexType("ProfPerson");
        type.createProperty("id", Type.INTEGER);
        database.getSchema().createEdgeType("PROF_KNOWS");
      }
    });
    database.transaction(() -> {
      database.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "ProfPerson", "id");
    });
    database.transaction(() -> {
      for (int i = 0; i < VERTEX_COUNT; i++) {
        final MutableVertex v = database.newVertex("ProfPerson");
        v.set("id", i);
        v.set("name", "Person_" + i);
        v.save();
      }
    });
  }

  @Test
  void profileEdgeCreationCommitPath() {
    // ---- Warm up ----
    for (int i = 0; i < 20; i++) {
      final int src = i % VERTEX_COUNT;
      final int dst = (i + 1) % VERTEX_COUNT;
      database.transaction(() -> {
        final Vertex srcV = lookupVertex(src);
        final Vertex dstV = lookupVertex(dst);
        srcV.asVertex().newEdge("PROF_KNOWS", dstV, "weight", 0.5);
      });
    }

    // ---- 1. Profile: Vertex lookup only (read-only, no commit) ----
    final long[] lookupTimes = new long[ITERATIONS];
    for (int i = 0; i < ITERATIONS; i++) {
      final int src = i % VERTEX_COUNT;
      final int dst = (i + 1) % VERTEX_COUNT;
      database.begin();
      final long start = System.nanoTime();
      lookupVertex(src);
      lookupVertex(dst);
      lookupTimes[i] = System.nanoTime() - start;
      database.rollback();
    }

    // ---- 2. Profile: Edge creation without commit (in-memory mutation only) ----
    final long[] mutationTimes = new long[ITERATIONS];
    for (int i = 0; i < ITERATIONS; i++) {
      final int src = i % VERTEX_COUNT;
      final int dst = (i + 1) % VERTEX_COUNT;
      database.begin();
      final Vertex srcV = lookupVertex(src);
      final Vertex dstV = lookupVertex(dst);
      final long start = System.nanoTime();
      srcV.asVertex().newEdge("PROF_KNOWS", dstV, "weight", 0.5);
      mutationTimes[i] = System.nanoTime() - start;
      database.rollback();
    }

    // ---- 3. Profile: Commit only (lookup + mutation already done, time the commit) ----
    final long[] commitTimes = new long[ITERATIONS];
    for (int i = 0; i < ITERATIONS; i++) {
      final int src = i % VERTEX_COUNT;
      final int dst = (i + 1) % VERTEX_COUNT;
      database.begin();
      final Vertex srcV = lookupVertex(src);
      final Vertex dstV = lookupVertex(dst);
      srcV.asVertex().newEdge("PROF_KNOWS", dstV, "weight", 0.5);
      final long start = System.nanoTime();
      database.commit();
      commitTimes[i] = System.nanoTime() - start;
    }

    // ---- 4. Profile: Full transaction (begin + lookup + mutation + commit) ----
    final long[] fullTxTimes = new long[ITERATIONS];
    for (int i = 0; i < ITERATIONS; i++) {
      final int src = i % VERTEX_COUNT;
      final int dst = (i + 1) % VERTEX_COUNT;
      final long start = System.nanoTime();
      database.transaction(() -> {
        final Vertex srcV = lookupVertex(src);
        final Vertex dstV = lookupVertex(dst);
        srcV.asVertex().newEdge("PROF_KNOWS", dstV, "weight", 0.5);
      });
      fullTxTimes[i] = System.nanoTime() - start;
    }

    // ---- 5. Profile: Full Cypher MATCH...CREATE (parsing cached, execution + commit) ----
    final long[] cypherTimes = new long[ITERATIONS];
    for (int i = 0; i < ITERATIONS; i++) {
      final int src = i % VERTEX_COUNT;
      final int dst = (i + 1) % VERTEX_COUNT;
      final long start = System.nanoTime();
      database.command("opencypher",
          "MATCH (a:ProfPerson) WHERE a.id = $src " +
              "MATCH (b:ProfPerson) WHERE b.id = $dst " +
              "CREATE (a)-[:PROF_KNOWS {weight: $w}]->(b)",
          Map.of("src", src, "dst", dst, "w", 0.5));
      cypherTimes[i] = System.nanoTime() - start;
    }

    // ---- 6. Profile: SQL equivalent (for comparison) ----
    final long[] sqlTimes = new long[ITERATIONS];
    for (int i = 0; i < ITERATIONS; i++) {
      final int src = i % VERTEX_COUNT;
      final int dst = (i + 1) % VERTEX_COUNT;
      final long start = System.nanoTime();
      database.transaction(() -> {
        database.command("sql",
            "CREATE EDGE PROF_KNOWS FROM (SELECT FROM ProfPerson WHERE id = ?) TO (SELECT FROM ProfPerson WHERE id = ?) SET weight = ?",
            src, dst, 0.5);
      });
      sqlTimes[i] = System.nanoTime() - start;
    }

    // ---- Calculate and report ----
    Arrays.sort(lookupTimes);
    Arrays.sort(mutationTimes);
    Arrays.sort(commitTimes);
    Arrays.sort(fullTxTimes);
    Arrays.sort(cypherTimes);
    Arrays.sort(sqlTimes);

    final String report = String.format(
        "\n=== Edge Creation Commit Path Profile (Issue #3613) ===" +
            "\n%-35s %10s %10s %10s %10s" +
            "\n%-35s %10.3f %10.3f %10.3f %10.3f" +
            "\n%-35s %10.3f %10.3f %10.3f %10.3f" +
            "\n%-35s %10.3f %10.3f %10.3f %10.3f" +
            "\n%-35s %10.3f %10.3f %10.3f %10.3f" +
            "\n%-35s %10.3f %10.3f %10.3f %10.3f" +
            "\n%-35s %10.3f %10.3f %10.3f %10.3f" +
            "\n\nBreakdown of full tx median:" +
            "\n  Lookup:   %10.3f ms (%.1f%%)" +
            "\n  Mutation: %10.3f ms (%.1f%%)" +
            "\n  Commit:   %10.3f ms (%.1f%%)" +
            "\n  Overhead: %10.3f ms (%.1f%%)" +
            "\n\nCypher (2 MATCH) vs API ratio: %.2fx" +
            "\n  SQL vs API ratio:            %.2fx",
        "Operation", "Median(ms)", "Min(ms)", "P95(ms)", "P99(ms)",
        "1. Vertex lookup (2x index)", medianMs(lookupTimes), minMs(lookupTimes), p95Ms(lookupTimes), p99Ms(lookupTimes),
        "2. Edge mutation (no commit)", medianMs(mutationTimes), minMs(mutationTimes), p95Ms(mutationTimes), p99Ms(mutationTimes),
        "3. Commit only", medianMs(commitTimes), minMs(commitTimes), p95Ms(commitTimes), p99Ms(commitTimes),
        "4. Full tx (begin+all+commit)", medianMs(fullTxTimes), minMs(fullTxTimes), p95Ms(fullTxTimes), p99Ms(fullTxTimes),
        "5. Cypher 2xMATCH..CREATE", medianMs(cypherTimes), minMs(cypherTimes), p95Ms(cypherTimes), p99Ms(cypherTimes),
        "6. SQL CREATE EDGE", medianMs(sqlTimes), minMs(sqlTimes), p95Ms(sqlTimes), p99Ms(sqlTimes),
        medianMs(lookupTimes), pct(medianMs(lookupTimes), medianMs(fullTxTimes)),
        medianMs(mutationTimes), pct(medianMs(mutationTimes), medianMs(fullTxTimes)),
        medianMs(commitTimes), pct(medianMs(commitTimes), medianMs(fullTxTimes)),
        medianMs(fullTxTimes) - medianMs(lookupTimes) - medianMs(mutationTimes) - medianMs(commitTimes),
        pct(medianMs(fullTxTimes) - medianMs(lookupTimes) - medianMs(mutationTimes) - medianMs(commitTimes), medianMs(fullTxTimes)),
        medianMs(cypherTimes) / medianMs(fullTxTimes),
        medianMs(sqlTimes) / medianMs(fullTxTimes)
    );

    LogManager.instance().log(this, Level.INFO, report);
  }

  private Vertex lookupVertex(final int id) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM ProfPerson WHERE id = ?", id)) {
      return rs.nextIfAvailable().getRecord().get().asVertex();
    }
  }

  private static double medianMs(final long[] sorted) {
    return sorted[sorted.length / 2] / 1_000_000.0;
  }

  private static double minMs(final long[] sorted) {
    return sorted[0] / 1_000_000.0;
  }

  private static double p95Ms(final long[] sorted) {
    return sorted[(int) (sorted.length * 0.95)] / 1_000_000.0;
  }

  private static double p99Ms(final long[] sorted) {
    return sorted[(int) (sorted.length * 0.99)] / 1_000_000.0;
  }

  /**
   * Profiles UNWIND batch edge creation to verify index usage.
   * Related to #3613 comment: UNWIND batch should use index, not full scan.
   */
  @Test
  void profileUnwindBatchEdgeCreation() {
    // Test with increasing batch sizes to verify linear (not quadratic) scaling
    final int[] batchSizes = {10, 50, 100};

    final StringBuilder report = new StringBuilder();
    report.append("\n=== UNWIND Batch Edge Creation Profile (Issue #3613) ===");
    report.append(String.format("\n%-20s %12s %12s", "Batch Size", "Total(ms)", "Per-Edge(ms)"));

    for (final int batchSize : batchSizes) {
      final java.util.List<Map<String, Object>> batch = new java.util.ArrayList<>();
      for (int i = 0; i < batchSize; i++)
        batch.add(Map.of("src_id", i % VERTEX_COUNT, "dst_id", (i + 1) % VERTEX_COUNT,
            "weight", 0.5, "since", "2024"));

      // Warm up
      database.command("opencypher",
          "UNWIND $batch AS e MATCH (a:ProfPerson) WHERE a.id = e.src_id " +
              "MATCH (b:ProfPerson) WHERE b.id = e.dst_id " +
              "CREATE (a)-[:PROF_KNOWS {weight: e.weight}]->(b)",
          Map.of("batch", batch));

      // Measure
      final long start = System.nanoTime();
      database.command("opencypher",
          "UNWIND $batch AS e MATCH (a:ProfPerson) WHERE a.id = e.src_id " +
              "MATCH (b:ProfPerson) WHERE b.id = e.dst_id " +
              "CREATE (a)-[:PROF_KNOWS {weight: e.weight}]->(b)",
          Map.of("batch", batch));
      final double totalMs = (System.nanoTime() - start) / 1_000_000.0;
      final double perEdgeMs = totalMs / batchSize;

      report.append(String.format("\n%-20d %12.2f %12.3f", batchSize, totalMs, perEdgeMs));
    }

    LogManager.instance().log(this, Level.INFO, report.toString());
  }

  private static double pct(final double part, final double total) {
    return total > 0 ? (part / total) * 100.0 : 0;
  }
}
