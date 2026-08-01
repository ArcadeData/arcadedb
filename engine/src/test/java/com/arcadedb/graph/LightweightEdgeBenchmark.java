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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Regular vs lightweight edges across the operations a graph application actually performs, each with and without a
 * UNIQUE declaration. Prints a table for the documentation.
 * <p>
 * Also reports on-disk bytes split by file role (vertex buckets, the {@code _out_edges} / {@code _in_edges} chunk
 * files, and the edge type's own bucket), which is what separates the two savings a lightweight edge delivers: the
 * edge <b>record</b> it does not write, and the edge-list <b>entry</b> it still does.
 * <p>
 * Not a correctness test - it asserts nothing. Tagged so the regular build skips it: {@code performance} is the tag
 * asked for, {@code benchmark} is the one this repository already excludes from CI.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("performance")
@Tag("benchmark")
public class LightweightEdgeBenchmark {
  private static final String DB_PATH = "target/databases/lightedge-benchmark";
  private static final int    WARMUPS = 1;
  private static final long   SEED    = 20260801L;
  /** Second-hop steps to visit, so the two scenarios do comparable work. */
  private static final int    TWO_HOP_BUDGET = 2_000_000;
  /** Edges to delete per measurement. */
  private static final int    DELETE_BUDGET  = 50_000;

  /** Same edge count, opposite shapes: a wide flat graph vs a handful of super-nodes. */
  private record Scenario(String name, int vertices, int degree) {
    int edges() {
      return vertices * degree;
    }
  }

  private static final Scenario SPARSE    = new Scenario("degree 2 (sparse)", 50_000, 2);
  private static final Scenario SOCIAL    = new Scenario("degree 10 (typical)", 20_000, 10);
  private static final Scenario SUPERNODE = new Scenario("degree 100 (hub)", 500, 100);

  private Scenario scenario = SOCIAL;

  /** One row of the published table. */
  private record Row(String shape, boolean unique, long bulkInsertMs, long singleInsertMs, long oneHopMs,
                     long twoHopMs, long countEdgesMs, long deleteMs, long vertexBytes, long edgeListBytes,
                     long edgeRecordBytes) {
    long totalBytes() {
      return vertexBytes + edgeListBytes + edgeRecordBytes;
    }
  }

  @Test
  void compareRegularAndLightweightEdges() {
    for (final Scenario s : new Scenario[] { SOCIAL, SUPERNODE }) {
      scenario = s;

      for (int i = 0; i < WARMUPS; i++) {
        run(false, false, true);
        run(true, false, true);
      }

      final List<Row> rows = new ArrayList<>();
      for (final boolean lightweight : new boolean[] { false, true })
        for (final boolean unique : new boolean[] { false, true }) {
          final Row row = run(lightweight, unique, false);
          // Printed as it lands: a slow combination should not hide the ones already measured.
          System.out.printf(Locale.ROOT, "[%s] %s unique=%s bulk=%,d ms single=%,d ms delete=%,d ms%n",
              s.name(), row.shape(), unique, row.bulkInsertMs(), row.singleInsertMs(), row.deleteMs());
          rows.add(row);
        }

      printTable(rows);
    }
  }

  /**
   * Sweeps the edge-list first-chunk size for lightweight edges. Each further chunk doubles the previous one, so the
   * total a vertex allocates is the sum of a geometric series: halving the first chunk does NOT halve the space, it
   * just takes more chunks (each with its own record header) to reach the same capacity.
   */
  @Test
  void sweepEdgeListChunkSizeForLightweightEdges() {
    final int saved = GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.getValueAsInteger();
    try {
      for (final Scenario sc : new Scenario[] { SPARSE, SOCIAL, SUPERNODE })
       for (final boolean lightweight : new boolean[] { true, false }) {
        scenario = sc;

        final StringBuilder out = new StringBuilder();
        out.append(String.format(Locale.ROOT, "%n=== First-chunk sweep, %s, %s: %,d vertices, %,d edges ===%n%n",
            lightweight ? "LIGHTWEIGHT" : "REGULAR", sc.name(), sc.vertices(), sc.edges()));
        out.append("| First chunk | Bulk insert | 1-hop | 2-hop | Delete | Edge-list files | Total | Bytes/edge |\n");
        out.append("|---|---|---|---|---|---|---|---|\n");

        for (final int chunk : new int[] { 32, 64, 128, 256, 512, 1024 }) {
          GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.setValue(chunk);
          run(lightweight, false, true);                // warm up at this size
          final Row r = run(lightweight, false, false);
          out.append(String.format(Locale.ROOT, "| %d B%s | %,d ms | %,d ms | %,d ms | %,d ms | %s | %s | %.1f |%n",
              chunk, chunk == 64 ? " (default)" : "", r.bulkInsertMs(), r.oneHopMs(), r.twoHopMs(), r.deleteMs(),
              human(r.edgeListBytes()), human(r.totalBytes()), (double) r.totalBytes() / scenario.edges()));
        }
        System.out.println(out);
      }
    } finally {
      GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.setValue(saved);
    }
  }

  private Row run(final boolean lightweight, final boolean unique, final boolean warmup) {
    FileUtils.deleteRecursively(new File(DB_PATH));

    long bulkInsert, singleInsert, oneHop, twoHop, countEdges, delete;
    final long vertexBytes, edgeListBytes, edgeRecordBytes;

    try (final Database db = new DatabaseFactory(DB_PATH).create()) {
      db.transaction(() -> {
        db.getSchema().buildVertexType().withName("Node").create();
        db.getSchema().buildEdgeType().withName("Link").withLightweight(lightweight).withUnique(unique).create();
      });

      // ---- vertices (outside every measurement: identical in both shapes)
      final int singleInserts = Math.min(2_000, scenario.vertices());
      // Sinks are targets for the single-insert phase only. Without them that phase would re-use random targets and
      // could repeat a pair the bulk phase already created, which a UNIQUE type rejects - so the measurement would
      // become the exception path instead of the insert path.
      final RID[] nodes = new RID[scenario.vertices()];
      final RID[] sinks = new RID[singleInserts];
      db.transaction(() -> {
        for (int i = 0; i < scenario.vertices(); i++)
          nodes[i] = db.newVertex("Node").set("id", i).save().getIdentity();
        for (int i = 0; i < sinks.length; i++)
          sinks[i] = db.newVertex("Node").set("id", -(i + 1)).save().getIdentity();
      });

      // ---- bulk insert: the dominant cost of loading a graph
      final Random random = new Random(SEED);
      final int[] targets = new int[scenario.edges()];
      for (int i = 0; i < scenario.edges(); i++)
        targets[i] = random.nextInt(scenario.vertices());
      // A UNIQUE type rejects a repeated (out, in), so give every source distinct destinations. Without this the
      // unique rows would measure the exception path rather than the insert path.
      dedupeTargets(targets);

      long start = System.nanoTime();
      db.begin();
      for (int i = 0; i < scenario.edges(); i++) {
        final int source = i / scenario.degree();
        db.lookupByRID(nodes[source], true).asVertex().modify().newEdge("Link", nodes[targets[i]]);
        if (i % 20_000 == 0) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
      bulkInsert = millis(start);

      // ---- single-edge insert, each in its own transaction: the OLTP shape
      start = System.nanoTime();
      for (int i = 0; i < singleInserts; i++) {
        final int source = i;
        db.transaction(() -> db.lookupByRID(nodes[source], true).asVertex().modify().newEdge("Link", sinks[source]));
      }
      singleInsert = millis(start);

      // ---- 1-hop expansion over the whole graph
      start = System.nanoTime();
      final long[] seen = new long[1];
      db.transaction(() -> {
        for (int i = 0; i < scenario.vertices(); i++)
          for (final Vertex neighbour : db.lookupByRID(nodes[i], true).asVertex().getVertices(Vertex.DIRECTION.OUT))
            ++seen[0];
      });
      oneHop = millis(start);

      // ---- 2-hop expansion from a sample: the pattern lightweight edges are meant for.
      // Sized by an edge budget, not a vertex count: a 2-hop from a super-node visits degree^2 neighbours, so a
      // fixed sample would make the high-degree scenario hundreds of times more work than the low-degree one and
      // measure the sample size rather than the edge shape.
      final int twoHopSources = Math.max(1,
          Math.min(Math.min(2_000, scenario.vertices()), TWO_HOP_BUDGET / (scenario.degree() * scenario.degree())));
      start = System.nanoTime();
      db.transaction(() -> {
        for (int i = 0; i < twoHopSources; i++)
          for (final Vertex first : db.lookupByRID(nodes[i], true).asVertex().getVertices(Vertex.DIRECTION.OUT))
            for (final Vertex second : first.getVertices(Vertex.DIRECTION.OUT))
              ++seen[0];
      });
      twoHop = millis(start);

      // ---- degree count, which never loads an edge record in either shape
      start = System.nanoTime();
      db.transaction(() -> {
        for (int i = 0; i < scenario.vertices(); i++)
          seen[0] += db.lookupByRID(nodes[i], true).asVertex().countEdges(Vertex.DIRECTION.OUT);
      });
      countEdges = millis(start);

      // ---- delete every edge of a sample of vertices
      start = System.nanoTime();
      // Same reasoning: bound the number of edges deleted, not the number of vertices.
      final int deleteSample = Math.max(1,
          Math.min(Math.min(500, scenario.vertices()), DELETE_BUDGET / scenario.degree()));
      for (int i = 0; i < deleteSample; i++) {
        final int source = i;
        db.transaction(() -> {
          final List<Edge> edges = new ArrayList<>();
          for (final Edge edge : db.lookupByRID(nodes[source], true).asVertex().getEdges(Vertex.DIRECTION.OUT))
            edges.add(edge);
          for (final Edge edge : edges)
            edge.delete();
        });
      }
      delete = millis(start);

      if (seen[0] < 0)
        throw new IllegalStateException("unreachable, keeps the traversals from being optimised away");
    }

    vertexBytes = sizeOf(name -> name.startsWith("Node_") && !name.contains("_edges"));
    edgeListBytes = sizeOf(name -> name.contains("_out_edges") || name.contains("_in_edges"));
    edgeRecordBytes = sizeOf(name -> name.startsWith("Link_"));

    FileUtils.deleteRecursively(new File(DB_PATH));

    return warmup ?
        null :
        new Row(lightweight ? "lightweight" : "regular", unique, bulkInsert, singleInsert, oneHop, twoHop, countEdges,
            delete, vertexBytes, edgeListBytes, edgeRecordBytes);
  }

  /** Makes every source's destinations distinct, so a UNIQUE type has nothing to reject. */
  private void dedupeTargets(final int[] targets) {
    for (int source = 0; source * scenario.degree() < targets.length; source++) {
      final int from = source * scenario.degree();
      for (int i = from; i < from + scenario.degree() && i < targets.length; i++)
        for (int j = from; j < i; j++)
          if (targets[j] == targets[i]) {
            targets[i] = (targets[i] + 1) % scenario.vertices();
            j = from - 1; // restart the scan for the new value
          }
    }
  }

  private long sizeOf(final java.util.function.Predicate<String> matches) {
    final File dir = new File(DB_PATH);
    final File[] files = dir.listFiles();
    if (files == null)
      return 0;
    long total = 0;
    for (final File file : files)
      if (matches.test(file.getName()))
        total += file.length();
    return total;
  }

  private static long millis(final long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000;
  }

  private void printTable(final List<Row> rows) {
    final StringBuilder out = new StringBuilder();
    out.append(String.format(Locale.ROOT,
        "%n=== %s: %,d vertices, %,d edges (2-hop and delete are sized by edge budget) ===%n%n",
        scenario.name(), scenario.vertices(), scenario.edges()));

    out.append("| Shape | UNIQUE | Bulk insert | Single insert (2k tx) | 1-hop (all) | 2-hop (2k) | countEdges | Delete (500 v) |\n");
    out.append("|---|---|---|---|---|---|---|---|\n");
    for (final Row r : rows)
      out.append(String.format(Locale.ROOT, "| %s | %s | %,d ms | %,d ms | %,d ms | %,d ms | %,d ms | %,d ms |%n",
          r.shape(), r.unique() ? "yes" : "no", r.bulkInsertMs(), r.singleInsertMs(), r.oneHopMs(), r.twoHopMs(),
          r.countEdgesMs(), r.deleteMs()));

    out.append("\n| Shape | UNIQUE | Vertex files | Edge-list files | Edge-record files | Total | Bytes/edge |\n");
    out.append("|---|---|---|---|---|---|---|\n");
    for (final Row r : rows)
      out.append(String.format(Locale.ROOT, "| %s | %s | %s | %s | %s | %s | %.1f |%n",
          r.shape(), r.unique() ? "yes" : "no", human(r.vertexBytes()), human(r.edgeListBytes()),
          human(r.edgeRecordBytes()), human(r.totalBytes()), (double) r.totalBytes() / scenario.edges()));

    // Answers the "should the edge list be denser for lightweight edges?" question directly: how much of a
    // lightweight edge's remaining footprint is the edge-list entry.
    for (final Row r : rows)
      if (r.shape().equals("lightweight") && !r.unique())
        out.append(String.format(Locale.ROOT,
            "%nLightweight footprint that is edge-list entries: %.1f%% of total (%s of %s)%n",
            100.0 * r.edgeListBytes() / r.totalBytes(), human(r.edgeListBytes()), human(r.totalBytes())));

    System.out.println(out);
  }

  private static String human(final long bytes) {
    if (bytes < 1024)
      return bytes + " B";
    if (bytes < 1024 * 1024)
      return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
    return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024.0));
  }
}
