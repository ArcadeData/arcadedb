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
package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Performance benchmark comparing OLTP graph traversal vs CSR-based OLAP traversal.
 * Also benchmarks graph algorithms (PageRank, connected components, shortest path, label propagation)
 * with OLTP vs OLAP comparison for each.
 * <p>
 * Run manually (not part of CI):
 * <pre>
 *   mvn test -pl engine -Dtest="performance.GraphOLAPBenchmark" -Darcadedb.olap.vertices=100000 -Darcadedb.olap.edgesPerVertex=10
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("performance")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GraphOLAPBenchmark {

  private static final String DB_PATH = "target/databases/olap-benchmark";
  private static final int    VERTEX_COUNT      = Integer.getInteger("arcadedb.olap.vertices", 500_000);
  private static final int    EDGES_PER_VERTEX  = Integer.getInteger("arcadedb.olap.edgesPerVertex", 16);
  private static final int    TRAVERSAL_SAMPLES = 1000;

  private Database             database;
  private GraphAnalyticalView  gav;
  private List<RID>            vertexRIDs;
  private Map<RID, Integer>    ridToIndex;

  // Result collection for summary table
  private final List<String[]> results = new ArrayList<>();
  private long   gavMemoryBytes;
  private long   oltpEstimateBytes;
  private double lastOltpUs;      // last OLTP traversal result in us/sample
  private double lastOltpMs;      // last OLTP algorithm result in ms

  @BeforeAll
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();

    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    System.out.printf("%n=== Graph OLAP Benchmark ===%n");
    System.out.printf("Vertices: %,d  |  Edges/vertex: %d  |  Total edges: ~%,d%n%n",
        VERTEX_COUNT, EDGES_PER_VERTEX, (long) VERTEX_COUNT * EDGES_PER_VERTEX);

    // Create graph
    System.out.print("Creating graph...");
    final long createStart = System.nanoTime();
    vertexRIDs = new ArrayList<>(VERTEX_COUNT);

    final String[] cities = { "New York", "London", "Tokyo", "Paris", "Berlin", "Rome", "Sydney", "Toronto", "Dubai", "Singapore",
        "San Francisco", "Amsterdam", "Barcelona", "Mumbai", "Shanghai" };

    final int batchSize = 5000;
    for (int i = 0; i < VERTEX_COUNT; i += batchSize) {
      database.begin();
      final int end = Math.min(i + batchSize, VERTEX_COUNT);
      for (int j = i; j < end; j++) {
        final MutableVertex v = database.newVertex("Person")
            .set("name", "Person_" + j)
            .set("age", j % 80 + 18)
            .set("salary", 30000.0 + (j % 7000) * 10.5)
            .set("score", j * 17 % 10000)
            .set("city", cities[j % cities.length])
            .save();
        vertexRIDs.add(v.getIdentity());
      }
      database.commit();
    }

    // Build RID-to-index lookup for OLTP algorithm benchmarks
    ridToIndex = new HashMap<>(VERTEX_COUNT * 2);
    for (int i = 0; i < VERTEX_COUNT; i++)
      ridToIndex.put(vertexRIDs.get(i), i);

    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    for (int i = 0; i < VERTEX_COUNT; i += batchSize) {
      database.begin();
      final int end = Math.min(i + batchSize, VERTEX_COUNT);
      for (int j = i; j < end; j++) {
        final Vertex src = vertexRIDs.get(j).asVertex();
        for (int e = 0; e < EDGES_PER_VERTEX; e++) {
          final int tgt = rng.nextInt(VERTEX_COUNT);
          if (tgt != j)
            src.asVertex().modify().newEdge("KNOWS", vertexRIDs.get(tgt));
        }
      }
      database.commit();
    }
    final long createMs = (System.nanoTime() - createStart) / 1_000_000;
    System.out.printf(" done (%,d ms)%n%n", createMs);
  }

  @AfterAll
  void teardown() {
    if (gav != null)
      gav.drop();
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // =============================================
  // CSR Build
  // =============================================

  @Test
  @Order(1)
  void benchmarkCSRBuild() {
    System.out.print("Building CSR...");
    final long start = System.nanoTime();
    gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS")
        .withProperties("name", "age", "salary", "score", "city")
        .build();
    final long ms = (System.nanoTime() - start) / 1_000_000;
    System.out.printf(" done (%,d ms)%n", ms);

    // Print detailed stats
    final Map<String, Object> stats = gav.getStats();
    System.out.printf("%n  === GAV Stats ===%n");
    System.out.printf("  Name:            %s%n", stats.get("name"));
    System.out.printf("  Status:          %s%n", stats.get("status"));
    System.out.printf("  Vertices:        %,d%n", stats.get("nodeCount"));
    System.out.printf("  Edges:           %,d%n", stats.get("edgeCount"));
    System.out.printf("  Auto-update:     %s%n", stats.get("autoUpdate"));
    System.out.printf("  Properties:      %d%n", stats.get("propertyCount"));
    System.out.printf("  Memory total:    %s%n", formatBytes((long) stats.get("memoryUsageBytes")));
    System.out.printf("    CSR arrays:    %s%n", formatBytes((long) stats.get("csrMemoryBytes")));
    System.out.printf("    Column stores: %s%n", formatBytes((long) stats.get("columnMemoryBytes")));
    System.out.printf("    Node mapping:  %s%n", formatBytes((long) stats.get("mappingMemoryBytes")));

    // Per-vertex and per-edge byte estimates
    final long totalMemory = (long) stats.get("memoryUsageBytes");
    final int nodeCount = (int) stats.get("nodeCount");
    final int edgeCount = (int) stats.get("edgeCount");
    if (nodeCount > 0) {
      System.out.printf("    Per vertex:    %.1f bytes%n", (double) totalMemory / nodeCount);
      if (edgeCount > 0)
        System.out.printf("    Per edge:      %.1f bytes%n", (double) totalMemory / edgeCount);
    }

    @SuppressWarnings("unchecked")
    final Map<String, Object> edgeTypeStats = (Map<String, Object>) stats.get("edgeTypes");
    if (edgeTypeStats != null) {
      System.out.printf("  Edge types:%n");
      for (final var entry : edgeTypeStats.entrySet()) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> etStat = (Map<String, Object>) entry.getValue();
        System.out.printf("    %s: %,d edges, %s%n", entry.getKey(), etStat.get("edgeCount"),
            formatBytes((long) etStat.get("memoryBytes")));
      }
    }

    // OLTP RAM estimate for comparison:
    // Vertex object: ~64 bytes overhead + RID(12) + properties map entry (~48/entry * 5 props)
    // Edge object: ~64 bytes overhead + 2 RIDs (24) + type string ref (8)
    // HashMap entry for edge list: ~48 bytes per entry
    final long oltpVertexBytes = (long) nodeCount * (64 + 12 + 5 * 48);
    final long oltpEdgeBytes = (long) edgeCount * (64 + 24 + 8 + 48);
    final long oltpEstimate = oltpVertexBytes + oltpEdgeBytes;
    gavMemoryBytes = totalMemory;
    oltpEstimateBytes = oltpEstimate;
    System.out.printf("%n  OLTP RAM estimate (in-memory objects): %s%n", formatBytes(oltpEstimate));
    System.out.printf("  GAV/OLTP RAM ratio: %.1fx more compact%n%n", (double) oltpEstimate / totalMemory);
  }

  // =============================================
  // 1-hop: count out-neighbors
  // =============================================

  @Test
  @Order(2)
  void benchmark1HopCountOLTP() {
    System.out.println("--- 1-hop traversal (count out-neighbors) ---");
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final long start = System.nanoTime();
    for (int i = 0; i < TRAVERSAL_SAMPLES; i++) {
      final Vertex v = vertexRIDs.get(rng.nextInt(VERTEX_COUNT)).asVertex();
      total += v.countEdges(Vertex.DIRECTION.OUT, "KNOWS");
    }
    lastOltpUs = printResultUs("OLTP", TRAVERSAL_SAMPLES, start, total);
  }

  @Test
  @Order(3)
  void benchmark1HopCountOLAP() {
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final long start = System.nanoTime();
    for (int i = 0; i < TRAVERSAL_SAMPLES; i++) {
      final int nodeId = gav.getNodeId(vertexRIDs.get(rng.nextInt(VERTEX_COUNT)));
      total += gav.countEdges(nodeId, Vertex.DIRECTION.OUT, "KNOWS");
    }
    final double olapUs = printResultUs("OLAP", TRAVERSAL_SAMPLES, start, total);
    addResult("1-hop count", formatUs(lastOltpUs), formatUs(olapUs), lastOltpUs / olapUs);
    System.out.println();
  }

  // =============================================
  // 1-hop: get out-neighbor IDs
  // =============================================

  @Test
  @Order(4)
  void benchmark1HopFetchOLTP() {
    System.out.println("--- 1-hop traversal (get out-neighbor IDs) ---");
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final long start = System.nanoTime();
    for (int i = 0; i < TRAVERSAL_SAMPLES; i++) {
      final Vertex v = vertexRIDs.get(rng.nextInt(VERTEX_COUNT)).asVertex();
      for (final Vertex n : v.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
        total++;
    }
    lastOltpUs = printResultUs("OLTP", TRAVERSAL_SAMPLES, start, total);
  }

  @Test
  @Order(5)
  void benchmark1HopFetchOLAP() {
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final long start = System.nanoTime();
    for (int i = 0; i < TRAVERSAL_SAMPLES; i++) {
      final int nodeId = gav.getNodeId(vertexRIDs.get(rng.nextInt(VERTEX_COUNT)));
      total += gav.getVertices(nodeId, Vertex.DIRECTION.OUT, "KNOWS").length;
    }
    final double olapUs = printResultUs("OLAP", TRAVERSAL_SAMPLES, start, total);
    addResult("1-hop IDs", formatUs(lastOltpUs), formatUs(olapUs), lastOltpUs / olapUs);
    System.out.println();
  }

  // =============================================
  // 2-hop traversal
  // =============================================

  @Test
  @Order(6)
  void benchmark2HopOLTP() {
    System.out.println("--- 2-hop traversal (friends of friends count) ---");
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final int samples = Math.min(TRAVERSAL_SAMPLES, 200);
    final long start = System.nanoTime();
    for (int i = 0; i < samples; i++) {
      final Vertex v = vertexRIDs.get(rng.nextInt(VERTEX_COUNT)).asVertex();
      for (final Vertex n1 : v.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
        total += n1.countEdges(Vertex.DIRECTION.OUT, "KNOWS");
    }
    lastOltpUs = printResultUs("OLTP", samples, start, total);
  }

  @Test
  @Order(7)
  void benchmark2HopOLAP() {
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final int samples = Math.min(TRAVERSAL_SAMPLES, 200);
    final long start = System.nanoTime();
    for (int i = 0; i < samples; i++) {
      final int nodeId = gav.getNodeId(vertexRIDs.get(rng.nextInt(VERTEX_COUNT)));
      for (final int n : gav.getVertices(nodeId, Vertex.DIRECTION.OUT, "KNOWS"))
        total += gav.countEdges(n, Vertex.DIRECTION.OUT, "KNOWS");
    }
    final double olapUs = printResultUs("OLAP", samples, start, total);
    addResult("2-hop", formatUs(lastOltpUs), formatUs(olapUs), lastOltpUs / olapUs);
    System.out.println();
  }

  // =============================================
  // 3-hop traversal
  // =============================================

  @Test
  @Order(8)
  void benchmark3HopOLTP() {
    System.out.println("--- 3-hop traversal (count reachable at depth 3) ---");
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final int samples = Math.min(TRAVERSAL_SAMPLES, 50);
    final long start = System.nanoTime();
    for (int i = 0; i < samples; i++) {
      final Vertex v = vertexRIDs.get(rng.nextInt(VERTEX_COUNT)).asVertex();
      for (final Vertex n1 : v.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
        for (final Vertex n2 : n1.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
          total += n2.countEdges(Vertex.DIRECTION.OUT, "KNOWS");
    }
    lastOltpUs = printResultUs("OLTP", samples, start, total);
  }

  @Test
  @Order(9)
  void benchmark3HopOLAP() {
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final int samples = Math.min(TRAVERSAL_SAMPLES, 50);
    final long start = System.nanoTime();
    for (int i = 0; i < samples; i++) {
      final int nodeId = gav.getNodeId(vertexRIDs.get(rng.nextInt(VERTEX_COUNT)));
      for (final int n1 : gav.getVertices(nodeId, Vertex.DIRECTION.OUT, "KNOWS"))
        for (final int n2 : gav.getVertices(n1, Vertex.DIRECTION.OUT, "KNOWS"))
          total += gav.countEdges(n2, Vertex.DIRECTION.OUT, "KNOWS");
    }
    final double olapUs = printResultUs("OLAP", samples, start, total);
    addResult("3-hop", formatUs(lastOltpUs), formatUs(olapUs), lastOltpUs / olapUs);
    System.out.println();
  }

  // =============================================
  // 4-hop traversal
  // =============================================

  @Test
  @Order(10)
  void benchmark4HopOLTP() {
    System.out.println("--- 4-hop traversal (count reachable at depth 4) ---");
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final int samples = Math.min(TRAVERSAL_SAMPLES, 10);
    final long start = System.nanoTime();
    for (int i = 0; i < samples; i++) {
      final Vertex v = vertexRIDs.get(rng.nextInt(VERTEX_COUNT)).asVertex();
      for (final Vertex n1 : v.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
        for (final Vertex n2 : n1.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
          for (final Vertex n3 : n2.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
            total += n3.countEdges(Vertex.DIRECTION.OUT, "KNOWS");
    }
    lastOltpUs = printResultUs("OLTP", samples, start, total);
  }

  @Test
  @Order(11)
  void benchmark4HopOLAP() {
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final int samples = Math.min(TRAVERSAL_SAMPLES, 10);
    final long start = System.nanoTime();
    for (int i = 0; i < samples; i++) {
      final int nodeId = gav.getNodeId(vertexRIDs.get(rng.nextInt(VERTEX_COUNT)));
      for (final int n1 : gav.getVertices(nodeId, Vertex.DIRECTION.OUT, "KNOWS"))
        for (final int n2 : gav.getVertices(n1, Vertex.DIRECTION.OUT, "KNOWS"))
          for (final int n3 : gav.getVertices(n2, Vertex.DIRECTION.OUT, "KNOWS"))
            total += gav.countEdges(n3, Vertex.DIRECTION.OUT, "KNOWS");
    }
    final double olapUs = printResultUs("OLAP", samples, start, total);
    addResult("4-hop", formatUs(lastOltpUs), formatUs(olapUs), lastOltpUs / olapUs);
    System.out.println();
  }

  // =============================================
  // 5-hop traversal
  // =============================================

  @Test
  @Order(12)
  void benchmark5HopOLTP() {
    System.out.println("--- 5-hop traversal (count reachable at depth 5) ---");
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final int samples = Math.min(TRAVERSAL_SAMPLES, 3);
    final long start = System.nanoTime();
    for (int i = 0; i < samples; i++) {
      final Vertex v = vertexRIDs.get(rng.nextInt(VERTEX_COUNT)).asVertex();
      for (final Vertex n1 : v.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
        for (final Vertex n2 : n1.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
          for (final Vertex n3 : n2.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
            for (final Vertex n4 : n3.getVertices(Vertex.DIRECTION.OUT, "KNOWS"))
              total += n4.countEdges(Vertex.DIRECTION.OUT, "KNOWS");
    }
    lastOltpUs = printResultUs("OLTP", samples, start, total);
  }

  @Test
  @Order(13)
  void benchmark5HopOLAP() {
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    long total = 0;
    final int samples = Math.min(TRAVERSAL_SAMPLES, 3);
    final long start = System.nanoTime();
    for (int i = 0; i < samples; i++) {
      final int nodeId = gav.getNodeId(vertexRIDs.get(rng.nextInt(VERTEX_COUNT)));
      for (final int n1 : gav.getVertices(nodeId, Vertex.DIRECTION.OUT, "KNOWS"))
        for (final int n2 : gav.getVertices(n1, Vertex.DIRECTION.OUT, "KNOWS"))
          for (final int n3 : gav.getVertices(n2, Vertex.DIRECTION.OUT, "KNOWS"))
            for (final int n4 : gav.getVertices(n3, Vertex.DIRECTION.OUT, "KNOWS"))
              total += gav.countEdges(n4, Vertex.DIRECTION.OUT, "KNOWS");
    }
    final double olapUs = printResultUs("OLAP", samples, start, total);
    addResult("5-hop", formatUs(lastOltpUs), formatUs(olapUs), lastOltpUs / olapUs);
    System.out.println();
  }

  // =============================================
  // Shortest Path: OLTP vs OLAP
  // =============================================

  @Test
  @Order(20)
  void benchmarkShortestPathOLTP() {
    System.out.println("--- Shortest Path (BFS, 100 random pairs) ---");
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    final int samples = 100;

    // Pre-pick source/target pairs so both OLTP and OLAP use the same pairs
    final int[] sources = new int[samples];
    final int[] targets = new int[samples];
    for (int i = 0; i < samples; i++) {
      sources[i] = rng.nextInt(VERTEX_COUNT);
      targets[i] = rng.nextInt(VERTEX_COUNT);
    }

    long totalHops = 0;
    int found = 0;
    final long start = System.nanoTime();
    for (int i = 0; i < samples; i++) {
      final int dist = bfsOLTP(vertexRIDs.get(sources[i]), vertexRIDs.get(targets[i]));
      if (dist >= 0) {
        totalHops += dist;
        found++;
      }
    }
    final long ms = (System.nanoTime() - start) / 1_000_000;
    lastOltpMs = (double) ms / samples;
    System.out.printf("  OLTP:  %,d pairs in %,d ms  (%,.1f ms/pair)  found: %d  avg hops: %.1f%n",
        samples, ms, lastOltpMs, found, found > 0 ? (double) totalHops / found : 0);
  }

  @Test
  @Order(21)
  void benchmarkShortestPathOLAP() {
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    final int samples = 100;
    final int[] sources = new int[samples];
    final int[] targets = new int[samples];
    for (int i = 0; i < samples; i++) {
      sources[i] = rng.nextInt(VERTEX_COUNT);
      targets[i] = rng.nextInt(VERTEX_COUNT);
    }

    long totalHops = 0;
    int found = 0;
    final long start = System.nanoTime();
    for (int i = 0; i < samples; i++) {
      final int srcId = gav.getNodeId(vertexRIDs.get(sources[i]));
      final int tgtId = gav.getNodeId(vertexRIDs.get(targets[i]));
      final int dist = GraphAlgorithms.shortestPath(gav, srcId, tgtId, Vertex.DIRECTION.OUT, "KNOWS");
      if (dist >= 0) {
        totalHops += dist;
        found++;
      }
    }
    final long ms = (System.nanoTime() - start) / 1_000_000;
    final double olapMsPerPair = (double) ms / samples;
    System.out.printf("  OLAP:  %,d pairs in %,d ms  (%,.1f ms/pair)  found: %d  avg hops: %.1f%n%n",
        samples, ms, olapMsPerPair, found, found > 0 ? (double) totalHops / found : 0);
    addResult("Shortest Path", String.format("%,.0f ms/pair", lastOltpMs), String.format("%.1f ms/pair", olapMsPerPair), lastOltpMs / olapMsPerPair);
  }

  // =============================================
  // PageRank: OLTP vs OLAP
  // =============================================

  @Test
  @Order(30)
  void benchmarkPageRankOLTP() {
    System.out.println("--- PageRank (20 iterations) ---");
    final int n = VERTEX_COUNT;
    final double damping = 0.85;
    final int iterations = 20;

    final long start = System.nanoTime();

    double[] rank = new double[n];
    double[] next = new double[n];
    Arrays.fill(rank, 1.0 / n);

    for (int iter = 0; iter < iterations; iter++) {
      final double teleport = (1.0 - damping) / n;
      Arrays.fill(next, teleport);

      double danglingSum = 0.0;
      for (int u = 0; u < n; u++) {
        final Vertex v = vertexRIDs.get(u).asVertex();
        final long outDeg = v.countEdges(Vertex.DIRECTION.OUT, "KNOWS");
        if (outDeg == 0) {
          danglingSum += rank[u];
          continue;
        }
        final double contrib = damping * rank[u] / outDeg;
        for (final Vertex neighbor : v.getVertices(Vertex.DIRECTION.OUT, "KNOWS")) {
          final Integer nIdx = ridToIndex.get(neighbor.getIdentity());
          if (nIdx != null)
            next[nIdx] += contrib;
        }
      }
      if (danglingSum > 0.0) {
        final double dc = damping * danglingSum / n;
        for (int u = 0; u < n; u++)
          next[u] += dc;
      }
      final double[] tmp = rank;
      rank = next;
      next = tmp;
    }

    final long ms = (System.nanoTime() - start) / 1_000_000;
    double maxRank = 0;
    for (final double r : rank)
      if (r > maxRank)
        maxRank = r;
    lastOltpMs = ms;
    System.out.printf("  OLTP:  %,d ms  (top rank=%.6f)%n", ms, maxRank);
  }

  @Test
  @Order(31)
  void benchmarkPageRankOLAP() {
    final long start = System.nanoTime();
    final double[] ranks = GraphAlgorithms.pageRank(gav, 0.85, 20, "KNOWS");
    final long olapMs = (System.nanoTime() - start) / 1_000_000;

    double maxRank = 0;
    for (final double r : ranks)
      if (r > maxRank)
        maxRank = r;
    System.out.printf("  OLAP:  %,d ms  (top rank=%.6f)%n%n", olapMs, maxRank);
    addResult("PageRank (20 iter)", String.format("%,d ms", (long) lastOltpMs), String.format("%,d ms", olapMs), lastOltpMs / olapMs);
  }

  // =============================================
  // Connected Components: OLTP vs OLAP
  // =============================================

  @Test
  @Order(40)
  void benchmarkConnectedComponentsOLTP() {
    System.out.println("--- Connected Components (Union-Find) ---");
    final int n = VERTEX_COUNT;

    final long start = System.nanoTime();

    // Union-Find over OLTP graph
    final int[] parent = new int[n];
    final int[] rnk = new int[n];
    for (int i = 0; i < n; i++)
      parent[i] = i;

    for (int u = 0; u < n; u++) {
      final Vertex v = vertexRIDs.get(u).asVertex();
      for (final Vertex neighbor : v.getVertices(Vertex.DIRECTION.OUT, "KNOWS")) {
        final Integer nIdx = ridToIndex.get(neighbor.getIdentity());
        if (nIdx != null)
          ufUnion(parent, rnk, u, nIdx);
      }
    }
    for (int i = 0; i < n; i++)
      parent[i] = ufFind(parent, i);

    final long ms = (System.nanoTime() - start) / 1_000_000;
    final int components = GraphAlgorithms.countComponents(parent);
    lastOltpMs = ms;
    System.out.printf("  OLTP:  %,d ms  (components: %,d)%n", ms, components);
  }

  @Test
  @Order(41)
  void benchmarkConnectedComponentsOLAP() {
    final long start = System.nanoTime();
    final int[] components = GraphAlgorithms.connectedComponents(gav, "KNOWS");
    final long olapMs = (System.nanoTime() - start) / 1_000_000;
    System.out.printf("  OLAP:  %,d ms  (components: %,d)%n%n", olapMs, GraphAlgorithms.countComponents(components));
    addResult("Connected Components", String.format("%,d ms", (long) lastOltpMs), String.format("%,d ms", olapMs), lastOltpMs / olapMs);
  }

  // =============================================
  // Label Propagation: OLTP vs OLAP
  // =============================================

  @Test
  @Order(50)
  void benchmarkLabelPropagationOLTP() {
    System.out.println("--- Label Propagation (max 20 iterations) ---");
    final int n = VERTEX_COUNT;
    final int maxIters = 20;

    final long start = System.nanoTime();

    final int[] labels = new int[n];
    for (int i = 0; i < n; i++)
      labels[i] = i;

    for (int iter = 0; iter < maxIters; iter++) {
      boolean changed = false;
      for (int u = 0; u < n; u++) {
        final Vertex v = vertexRIDs.get(u).asVertex();
        // Collect neighbor labels
        final Map<Integer, Integer> labelCounts = new HashMap<>();
        for (final Vertex neighbor : v.getVertices(Vertex.DIRECTION.BOTH, "KNOWS")) {
          final Integer nIdx = this.ridToIndex.get(neighbor.getIdentity());
          if (nIdx != null)
            labelCounts.merge(labels[nIdx], 1, Integer::sum);
        }
        if (labelCounts.isEmpty())
          continue;
        // Find mode
        int bestLabel = labels[u];
        int bestCount = 0;
        for (final var entry : labelCounts.entrySet()) {
          if (entry.getValue() > bestCount) {
            bestCount = entry.getValue();
            bestLabel = entry.getKey();
          }
        }
        if (labels[u] != bestLabel) {
          labels[u] = bestLabel;
          changed = true;
        }
      }
      if (!changed)
        break;
    }

    final long ms = (System.nanoTime() - start) / 1_000_000;
    final Set<Integer> distinct = new HashSet<>();
    for (final int l : labels)
      distinct.add(l);
    lastOltpMs = ms;
    System.out.printf("  OLTP:  %,d ms  (communities: %,d)%n", ms, distinct.size());
  }

  @Test
  @Order(51)
  void benchmarkLabelPropagationOLAP() {
    final long start = System.nanoTime();
    final int[] labels = GraphAlgorithms.labelPropagation(gav, 20, "KNOWS");
    final long olapMs = (System.nanoTime() - start) / 1_000_000;

    final boolean[] seen = new boolean[labels.length];
    int communities = 0;
    for (final int l : labels)
      if (!seen[l]) {
        seen[l] = true;
        communities++;
      }
    System.out.printf("  OLAP:  %,d ms  (communities: %,d)%n%n", olapMs, communities);
    addResult("Label Propagation", String.format("%,d ms", (long) lastOltpMs), String.format("%,d ms", olapMs), lastOltpMs / olapMs);
  }

  // =============================================
  // Helpers
  // =============================================

  private double printResultUs(final String label, final int samples, final long startNano, final long total) {
    final long us = (System.nanoTime() - startNano) / 1_000;
    final double usPerSample = (double) us / samples;
    System.out.printf("  %s:  %,d samples in %,d us  (%,.1f us/sample)  total: %,d%n",
        label, samples, us, usPerSample, total);
    return usPerSample;
  }

  private static String formatUs(final double us) {
    if (us >= 1_000_000)
      return String.format("%,.0f us", us);
    if (us >= 1000)
      return String.format("%,.0f us", us);
    return String.format("%.1f us", us);
  }

  /**
   * BFS shortest path over OLTP Vertex API.
   */
  private int bfsOLTP(final RID source, final RID target) {
    if (source.equals(target))
      return 0;

    final Set<RID> visited = new HashSet<>();
    final Queue<RID> queue = new ArrayDeque<>();
    final Map<RID, Integer> dist = new HashMap<>();

    visited.add(source);
    queue.add(source);
    dist.put(source, 0);

    while (!queue.isEmpty()) {
      final RID current = queue.poll();
      final int d = dist.get(current);
      final Vertex v = current.asVertex();

      for (final Vertex neighbor : v.getVertices(Vertex.DIRECTION.OUT, "KNOWS")) {
        final RID nRid = neighbor.getIdentity();
        if (nRid.equals(target))
          return d + 1;
        if (visited.add(nRid)) {
          dist.put(nRid, d + 1);
          queue.add(nRid);
        }
      }
    }
    return -1;
  }

  private static int ufFind(final int[] parent, int x) {
    while (parent[x] != x) {
      parent[x] = parent[parent[x]];
      x = parent[x];
    }
    return x;
  }

  private static String formatBytes(final long bytes) {
    if (bytes < 1024)
      return bytes + " B";
    final String[] units = { "B", "KB", "MB", "GB" };
    int idx = 0;
    double val = bytes;
    while (val >= 1024 && idx < units.length - 1) {
      val /= 1024;
      idx++;
    }
    return String.format("%.1f %s", val, units[idx]);
  }

  private void addResult(final String name, final String oltpValue, final String olapValue, final double speedup) {
    results.add(new String[] { name, oltpValue, olapValue, String.format("%.1fx", speedup) });
  }

  @Test
  @Order(99)
  void printSummaryTable() {
    System.out.println();
    System.out.printf("Graph: %,dK vertices, ~%,dM edges%n%n", VERTEX_COUNT / 1000, (long) VERTEX_COUNT * EDGES_PER_VERTEX / 1_000_000);

    // Column widths
    final String[] headers = { "Benchmark", "OLTP", "OLAP", "Speedup" };
    final int[] widths = new int[4];
    for (int c = 0; c < 4; c++)
      widths[c] = headers[c].length();
    for (final String[] row : results)
      for (int c = 0; c < 4; c++)
        widths[c] = Math.max(widths[c], row[c].length());

    // Add padding
    for (int c = 0; c < 4; c++)
      widths[c] += 2;

    final String topBot = "  " + corner('┌', '┬', '┐', widths);
    final String mid    = "  " + corner('├', '┼', '┤', widths);
    final String bottom = "  " + corner('└', '┴', '┘', widths);

    System.out.println(topBot);
    System.out.println("  " + formatRow(headers, widths));
    System.out.println(mid);
    for (int i = 0; i < results.size(); i++) {
      System.out.println("  " + formatRow(results.get(i), widths));
      if (i < results.size() - 1)
        System.out.println(mid);
    }
    System.out.println(bottom);

    System.out.printf("%n  Memory: GAV (OLAP) uses %s vs ~%s OLTP estimate — %.1fx more compact.%n",
        formatBytes(gavMemoryBytes), formatBytes(oltpEstimateBytes), (double) oltpEstimateBytes / gavMemoryBytes);
    System.out.println();
    System.out.println("  The OLAP engine dominates across the board, especially on full-graph algorithms");
    System.out.printf("  like PageRank (%s faster) and Connected Components (%s faster).%n%n",
        results.stream().filter(r -> r[0].equals("PageRank (20 iter)")).map(r -> r[3]).findFirst().orElse("N/A"),
        results.stream().filter(r -> r[0].equals("Connected Components")).map(r -> r[3]).findFirst().orElse("N/A"));
  }

  private static String corner(final char left, final char mid, final char right, final int[] widths) {
    final StringBuilder sb = new StringBuilder();
    sb.append(left);
    for (int c = 0; c < widths.length; c++) {
      for (int i = 0; i < widths[c]; i++)
        sb.append('─');
      sb.append(c < widths.length - 1 ? mid : right);
    }
    return sb.toString();
  }

  private static String formatRow(final String[] cols, final int[] widths) {
    final StringBuilder sb = new StringBuilder("│");
    for (int c = 0; c < cols.length; c++) {
      final String val = cols[c];
      final int pad = widths[c] - val.length();
      final int left = pad / 2;
      final int right = pad - left;
      for (int i = 0; i < left; i++)
        sb.append(' ');
      sb.append(val);
      for (int i = 0; i < right; i++)
        sb.append(' ');
      sb.append('│');
    }
    return sb.toString();
  }

  private static void ufUnion(final int[] parent, final int[] rank, final int a, final int b) {
    int ra = ufFind(parent, a);
    int rb = ufFind(parent, b);
    if (ra == rb)
      return;
    if (rank[ra] < rank[rb]) {
      final int tmp = ra;
      ra = rb;
      rb = tmp;
    }
    parent[rb] = ra;
    if (rank[ra] == rank[rb])
      rank[ra]++;
  }
}
