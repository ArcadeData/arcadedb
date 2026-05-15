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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reopened follow-up for issue #4226: when the user has many chunks already
 * attached to the same parent vertex, the per-MERGE elapsed time observed in
 * production keeps growing batch after batch.
 * <p>
 * The first fix (commit {@code 749ed9f}) made the MERGE walker enter at the
 * bound anchor, so the cost stopped scaling with the {@em global} number of
 * edges of the relevant type.  It still scales with the {@em anchor's degree},
 * because the walker iterates every incident edge of the anchor and verifies
 * the unbound endpoint's label and properties after the fact.
 * <p>
 * When the user has an LSM index on the unbound endpoint's properties (the
 * documented workaround in the original issue), the optimal path is an index
 * seek on the unbound endpoint and an edge presence check from the candidate
 * to the bound anchor.  That makes MERGE cost independent of the anchor's
 * degree, matching Neo4j's plan ({@code NodeIndexSeek} + {@code Filter}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue4226MergeBoundAnchorPerfTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-4226-merge-bound-anchor-perf").create();
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createEdgeType("in");
    database.getSchema().getType("CHUNK").createProperty("name", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "CHUNK", "name");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Models the exact production pattern from the reopened report: a single
   * parent vertex accumulates chunks via consecutive {@code MERGE} batches.
   * The per-batch elapsed time must stay roughly constant after the first
   * batch (cold cache), not grow linearly with the running CHUNK count under
   * the parent.
   * <p>
   * With the anchor-walk-only implementation, the {@code k}-th batch performs
   * {@code k * batchSize} edge-list iterations on average, so the total cost is
   * quadratic in the parent's degree.  With the index-seek path applied to the
   * MERGE candidate, each iteration is bounded by the index probe.
   */
  @Test
  void mergePerBatchTimeStaysFlatAsParentDegreeGrows() {
    database.transaction(() -> database.command("opencypher", "CREATE (:DOCUMENT {name:'parent'})"));

    final int batches = 10;
    final int perBatch = 200;
    final long[] elapsedMs = new long[batches];

    for (int b = 0; b < batches; b++) {
      final int offset = b * perBatch;
      final long start = System.nanoTime();
      database.transaction(() -> {
        for (int i = 0; i < perBatch; i++) {
          final String name = "chunk_" + (offset + i);
          database.command("opencypher",
              "MATCH (p:DOCUMENT {name:'parent'}) "
                  + "MERGE (n:CHUNK {name:'" + name + "'})-[:in]->(p)");
        }
      });
      elapsedMs[b] = (System.nanoTime() - start) / 1_000_000L;
    }

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parent'}) RETURN count(n) AS cnt");
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo((long) batches * perBatch);

    // The first batch can be slow due to JIT warm-up and cold caches; from the
    // second batch onward, each subsequent batch should not be drastically
    // slower than the previous one.  A buggy O(N) walk yields elapsedMs[k] ~ k,
    // so the last batch would be ~10x the first.  We expect a ratio bounded by
    // a small constant.
    // Without the index-seek optimization the per-batch time grows linearly
    // with the parent's degree: 10 batches against a parent with no other
    // children took roughly [103, 80, 64, 56, 79, 90, 88, 119, 143, 146] ms on
    // dev hardware - last vs warm ratio ~1.8x.  Scaling the same shape up to
    // 20 batches of 500 produced [179, ..., 1899] ms, ratio ~10x.
    // <p>
    // With the index-seek optimization the cost is bounded by the index probe
    // and the candidate's edge degree, so the ratio stays close to 1.  We allow
    // 2x to absorb GC and OS scheduling jitter on shared CI hardware.
    final long warm = Math.max(elapsedMs[1], 30L); // ignore first batch (JIT warmup)
    final long last = elapsedMs[batches - 1];
    assertThat(last)
        .as("last-batch / warm-batch ratio must not grow linearly with parent degree (perBatchMs: " + java.util.Arrays.toString(elapsedMs) + ")")
        .isLessThan(warm * 2);
  }

  /**
   * Mirror of the previous test for the inverse direction
   * ({@code MERGE (p)-[:in]->(n:CHUNK ...)} with {@code p} bound): the bound
   * anchor sits at index 0 of the pattern. Without the index seek the forward
   * walker iterated every outgoing edge of {@code p}; with it, the per-MERGE
   * cost is independent of the anchor's outgoing degree.
   */
  @Test
  void mergePerBatchTimeStaysFlatWhenAnchorAtStart() {
    database.transaction(() -> database.command("opencypher", "CREATE (:DOCUMENT {name:'parent'})"));

    final int batches = 10;
    final int perBatch = 200;
    final long[] elapsedMs = new long[batches];

    for (int b = 0; b < batches; b++) {
      final int offset = b * perBatch;
      final long start = System.nanoTime();
      database.transaction(() -> {
        for (int i = 0; i < perBatch; i++) {
          final String name = "chunk_out_" + (offset + i);
          database.command("opencypher",
              "MATCH (p:DOCUMENT {name:'parent'}) "
                  + "MERGE (p)-[:in]->(n:CHUNK {name:'" + name + "'})");
        }
      });
      elapsedMs[b] = (System.nanoTime() - start) / 1_000_000L;
    }

    final ResultSet rs = database.query("opencypher",
        "MATCH (:DOCUMENT {name:'parent'})-[:in]->(n:CHUNK) RETURN count(n) AS cnt");
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo((long) batches * perBatch);

    final long warm = Math.max(elapsedMs[1], 30L);
    final long last = elapsedMs[batches - 1];
    assertThat(last)
        .as("last-batch / warm-batch ratio must not grow linearly with parent's outgoing degree (perBatchMs: " + java.util.Arrays.toString(elapsedMs) + ")")
        .isLessThan(warm * 2);
  }

  /**
   * Reproduces the exact query shape the user reported in
   * <a href="https://github.com/ArcadeData/arcadedb/issues/4226">issue #4226</a>'s
   * reopened comments: {@code UNWIND $batch ... MATCH parent BY ID ...
   * MERGE (n:CHUNK {...})-[:in]->(parent)}.
   * <p>
   * Each batch grows the parent's incoming-edge count, so under the previous
   * implementation the total time scaled with {@code O(batches * batchSize *
   * parent.degree)}.  The optimization restores linear-with-rows ingestion.
   */
  @Test
  void unwindMatchMergeBatchScalesLinearlyWithRowsNotParentDegree() {
    final String[] parentRid = new String[1];
    database.transaction(() ->
        parentRid[0] = database.command("opencypher", "CREATE (p:DOCUMENT {name:'p'}) RETURN p AS p")
            .next().<com.arcadedb.graph.Vertex>getProperty("p").getIdentity().toString());

    final int batches = 8;
    final int perBatch = 250;
    final long[] elapsedMs = new long[batches];

    for (int b = 0; b < batches; b++) {
      final List<Map<String, Object>> batch = new ArrayList<>(perBatch);
      for (int i = 0; i < perBatch; i++) {
        final Map<String, Object> entry = new HashMap<>();
        entry.put("_parent_rid", parentRid[0]);
        entry.put("subtype", "CHUNK");
        entry.put("name", "chunk_" + (b * perBatch + i));
        entry.put("text", "body for chunk " + (b * perBatch + i));
        entry.put("index", b * perBatch + i);
        batch.add(entry);
      }

      final Map<String, Object> params = new HashMap<>();
      params.put("batch", batch);

      final long start = System.nanoTime();
      database.transaction(() -> {
        try (final ResultSet ignored = database.command("opencypher",
            "UNWIND $batch AS BatchEntry "
                + "MATCH (parent) WHERE ID(parent) = BatchEntry._parent_rid "
                + "MERGE (n:CHUNK {subtype: BatchEntry.subtype, name: BatchEntry.name, text: BatchEntry.text, index: BatchEntry.index})-[:`in`]->(parent) "
                + "RETURN ID(n) AS id", params)) {
          while (ignored.hasNext()) ignored.next();
        }
      });
      elapsedMs[b] = (System.nanoTime() - start) / 1_000_000L;
    }

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'p'}) RETURN count(n) AS cnt");
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo((long) batches * perBatch);

    final long warm = Math.max(elapsedMs[1], 30L);
    final long last = elapsedMs[batches - 1];
    assertThat(last)
        .as("UNWIND+MATCH+MERGE batch time must not grow linearly with parent degree (perBatchMs: " + java.util.Arrays.toString(elapsedMs) + ")")
        .isLessThan(warm * 2);
  }
}
