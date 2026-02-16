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
package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.Test;

import java.util.*;

/**
 * Benchmark comparing DuanSSSP vs Dijkstra on synthetic graphs.
 * This benchmark generates various graph topologies and measures performance.
 *
 * Based on the paper "Breaking the Sorting Barrier for Directed Single-Source Shortest Paths"
 * Reference: https://arxiv.org/abs/2504.17033
 *
 * Expected result: Dijkstra should outperform DuanSSSP on practical-sized graphs
 * due to DuanSSSP's large constant factors, despite better asymptotic complexity.
 */
class DuanSSSPBenchmark {

  private static final Random RANDOM = new Random(42); // Fixed seed for reproducibility

  /**
   * Generate a random sparse graph (Erdos-Renyi model)
   */
  private GraphData generateRandomGraph(final Database db, final int numVertices, final double edgeProbability) {
    final MutableVertex[] vertices = new MutableVertex[numVertices];

    for (int i = 0; i < numVertices; i++) {
      vertices[i] = db.newVertex("BenchNode");
      vertices[i].set("id", i).save();
    }

    int edgeCount = 0;
    for (int i = 0; i < numVertices; i++) {
      for (int j = 0; j < numVertices; j++) {
        if (i != j && RANDOM.nextDouble() < edgeProbability) {
          final MutableEdge edge = vertices[i].newEdge("BenchEdge", vertices[j]);
          edge.set("weight", 1.0 + RANDOM.nextDouble() * 10.0);
          edge.save();
          edgeCount++;
        }
      }
    }

    return new GraphData(vertices, edgeCount);
  }

  /**
   * Generate a grid graph (like a road network)
   */
  private GraphData generateGridGraph(final Database db, final int gridSize) {
    final int numVertices = gridSize * gridSize;
    final MutableVertex[][] grid = new MutableVertex[gridSize][gridSize];

    // Create vertices
    for (int i = 0; i < gridSize; i++) {
      for (int j = 0; j < gridSize; j++) {
        grid[i][j] = db.newVertex("BenchNode");
        grid[i][j].set("x", i).set("y", j).save();
      }
    }

    // Create edges (4-connected grid)
    int edgeCount = 0;
    for (int i = 0; i < gridSize; i++) {
      for (int j = 0; j < gridSize; j++) {
        // Right edge
        if (j < gridSize - 1) {
          final MutableEdge edge = grid[i][j].newEdge("BenchEdge", grid[i][j + 1]);
          edge.set("weight", 1.0 + RANDOM.nextDouble() * 5.0);
          edge.save();
          edgeCount++;
        }
        // Down edge
        if (i < gridSize - 1) {
          final MutableEdge edge = grid[i][j].newEdge("BenchEdge", grid[i + 1][j]);
          edge.set("weight", 1.0 + RANDOM.nextDouble() * 5.0);
          edge.save();
          edgeCount++;
        }
      }
    }

    final MutableVertex[] vertices = new MutableVertex[numVertices];
    int idx = 0;
    for (int i = 0; i < gridSize; i++) {
      for (int j = 0; j < gridSize; j++) {
        vertices[idx++] = grid[i][j];
      }
    }

    return new GraphData(vertices, edgeCount);
  }

  /**
   * Generate a scale-free graph (Barabasi-Albert model - simplified)
   */
  private GraphData generateScaleFreeGraph(final Database db, final int numVertices, final int edgesPerVertex) {
    final MutableVertex[] vertices = new MutableVertex[numVertices];

    // Create initial complete graph
    final int initialSize = Math.min(edgesPerVertex + 1, numVertices);
    for (int i = 0; i < initialSize; i++) {
      vertices[i] = db.newVertex("BenchNode");
      vertices[i].set("id", i).save();
    }

    int edgeCount = 0;
    for (int i = 0; i < initialSize; i++) {
      for (int j = i + 1; j < initialSize; j++) {
        final MutableEdge edge = vertices[i].newEdge("BenchEdge", vertices[j]);
        edge.set("weight", 1.0 + RANDOM.nextDouble() * 10.0);
        edge.save();
        edgeCount++;
      }
    }

    // Add remaining vertices with preferential attachment (simplified)
    for (int i = initialSize; i < numVertices; i++) {
      vertices[i] = db.newVertex("BenchNode");
      vertices[i].set("id", i).save();

      // Connect to random existing vertices (simplified preferential attachment)
      final Set<Integer> connected = new HashSet<>();
      while (connected.size() < Math.min(edgesPerVertex, i)) {
        final int target = RANDOM.nextInt(i);
        if (connected.add(target)) {
          final MutableEdge edge = vertices[i].newEdge("BenchEdge", vertices[target]);
          edge.set("weight", 1.0 + RANDOM.nextDouble() * 10.0);
          edge.save();
          edgeCount++;
        }
      }
    }

    return new GraphData(vertices, edgeCount);
  }

  /**
   * Run a single benchmark iteration
   */
  private BenchmarkResult runBenchmark(final Database db, final GraphData graphData, final String graphType) {
    final MutableVertex[] vertices = graphData.vertices;
    final int numPairs = Math.min(10, vertices.length / 2); // Test 10 random pairs

    long dijkstraTotalTime = 0;
    long duanSSSPTotalTime = 0;
    int successfulRuns = 0;

    final SQLFunctionDijkstra dijkstra = new SQLFunctionDijkstra();
    final SQLFunctionDuanSSSP duanSSSP = new SQLFunctionDuanSSSP();
    final BasicCommandContext ctx = new BasicCommandContext();

    for (int i = 0; i < numPairs; i++) {
      final int sourceIdx = RANDOM.nextInt(vertices.length);
      final int destIdx = RANDOM.nextInt(vertices.length);

      if (sourceIdx == destIdx)
        continue;

      final Vertex source = vertices[sourceIdx];
      final Vertex dest = vertices[destIdx];

      // Benchmark Dijkstra
      final long dijkstraStart = System.nanoTime();
      final List<RID> dijkstraPath = dijkstra.execute(null, null, null,
          new Object[] { source, dest, "weight" }, ctx);
      final long dijkstraEnd = System.nanoTime();

      // Benchmark DuanSSSP
      final long duanStart = System.nanoTime();
      final List<RID> duanPath = duanSSSP.execute(null, null, null,
          new Object[] { source, dest, "weight" }, ctx);
      final long duanEnd = System.nanoTime();

      // Only count if both found a path (or both didn't find a path)
      if ((dijkstraPath == null && duanPath.isEmpty()) ||
          (dijkstraPath != null && !duanPath.isEmpty())) {
        dijkstraTotalTime += (dijkstraEnd - dijkstraStart);
        duanSSSPTotalTime += (duanEnd - duanStart);
        successfulRuns++;

        // Verify path lengths match (optional correctness check)
        if (dijkstraPath != null && !duanPath.isEmpty()) {
          if (dijkstraPath.size() != duanPath.size()) {
            System.err.println("Warning: Path length mismatch! Dijkstra: " + dijkstraPath.size() +
                ", DuanSSSP: " + duanPath.size());
          }
        }
      }
    }

    if (successfulRuns == 0)
      return new BenchmarkResult(graphType, vertices.length, graphData.edgeCount, 0, 0, 0);

    final double avgDijkstra = dijkstraTotalTime / (double) successfulRuns / 1_000_000.0; // Convert to ms
    final double avgDuanSSSP = duanSSSPTotalTime / (double) successfulRuns / 1_000_000.0;
    final double speedupRatio = avgDuanSSSP / avgDijkstra;

    return new BenchmarkResult(graphType, vertices.length, graphData.edgeCount,
        avgDijkstra, avgDuanSSSP, speedupRatio);
  }

  @Test
  void benchmarkSmallRandomGraph() throws Exception {
    TestHelper.executeInNewDatabase("DuanSSSPBenchmark_smallRandom", (db) -> {
      db.transaction(() -> {
        db.getSchema().createVertexType("BenchNode");
        db.getSchema().createEdgeType("BenchEdge");

        System.out.println("\n=== Small Random Graph Benchmark ===");
        final GraphData graph = generateRandomGraph(db, 50, 0.1); // 50 vertices, ~10% edge probability
        final BenchmarkResult result = runBenchmark(db, graph, "Random");
        System.out.println(result);
      });
    });
  }

  @Test
  void benchmarkMediumRandomGraph() throws Exception {
    TestHelper.executeInNewDatabase("DuanSSSPBenchmark_mediumRandom", (db) -> {
      db.transaction(() -> {
        db.getSchema().createVertexType("BenchNode");
        db.getSchema().createEdgeType("BenchEdge");

        System.out.println("\n=== Medium Random Graph Benchmark ===");
        final GraphData graph = generateRandomGraph(db, 200, 0.05); // 200 vertices, ~5% edge probability
        final BenchmarkResult result = runBenchmark(db, graph, "Random");
        System.out.println(result);
      });
    });
  }

  @Test
  void benchmarkGridGraph() throws Exception {
    TestHelper.executeInNewDatabase("DuanSSSPBenchmark_grid", (db) -> {
      db.transaction(() -> {
        db.getSchema().createVertexType("BenchNode");
        db.getSchema().createEdgeType("BenchEdge");

        System.out.println("\n=== Grid Graph Benchmark (20x20) ===");
        final GraphData graph = generateGridGraph(db, 20); // 400 vertices, ~800 edges
        final BenchmarkResult result = runBenchmark(db, graph, "Grid");
        System.out.println(result);
      });
    });
  }

  @Test
  void benchmarkScaleFreeGraph() throws Exception {
    TestHelper.executeInNewDatabase("DuanSSSPBenchmark_scaleFree", (db) -> {
      db.transaction(() -> {
        db.getSchema().createVertexType("BenchNode");
        db.getSchema().createEdgeType("BenchEdge");

        System.out.println("\n=== Scale-Free Graph Benchmark ===");
        final GraphData graph = generateScaleFreeGraph(db, 150, 3); // 150 vertices, ~3 edges per vertex
        final BenchmarkResult result = runBenchmark(db, graph, "Scale-Free");
        System.out.println(result);
      });
    });
  }

  @Test
  void benchmarkComparison() throws Exception {
    TestHelper.executeInNewDatabase("DuanSSSPBenchmark_comparison", (db) -> {
      db.transaction(() -> {
        db.getSchema().createVertexType("BenchNode");
        db.getSchema().createEdgeType("BenchEdge");

        System.out.println("\n========================================");
        System.out.println("  DuanSSSP vs Dijkstra Benchmark Suite");
        System.out.println("========================================\n");

        final List<BenchmarkResult> results = new ArrayList<>();

        // Small graphs
        System.out.println("1. Small Random Graph (50 vertices)");
        results.add(runBenchmark(db, generateRandomGraph(db, 50, 0.1), "Random-Small"));

        System.out.println("2. Small Grid Graph (10x10)");
        results.add(runBenchmark(db, generateGridGraph(db, 10), "Grid-Small"));

        System.out.println("3. Small Scale-Free (50 vertices)");
        results.add(runBenchmark(db, generateScaleFreeGraph(db, 50, 3), "ScaleFree-Small"));

        // Medium graphs
        System.out.println("4. Medium Random Graph (100 vertices)");
        results.add(runBenchmark(db, generateRandomGraph(db, 100, 0.08), "Random-Medium"));

        System.out.println("5. Medium Grid Graph (15x15)");
        results.add(runBenchmark(db, generateGridGraph(db, 15), "Grid-Medium"));

        // Print summary
        System.out.println("\n========================================");
        System.out.println("              SUMMARY");
        System.out.println("========================================");
        System.out.printf("%-20s | %8s | %8s | %12s | %12s | %10s\n",
            "Graph Type", "Vertices", "Edges", "Dijkstra(ms)", "DuanSSSP(ms)", "Ratio");
        System.out.println("--------------------------------------------------------------------------------------------");

        for (final BenchmarkResult result : results)
          System.out.println(result.toTableRow());

        System.out.println("\nNote: Ratio > 1.0 means DuanSSSP is slower than Dijkstra");
        System.out.println("Expected: DuanSSSP slower on practical graphs due to constant factors");
        System.out.println("Theoretical: DuanSSSP has better asymptotic complexity O(m log^(2/3) n)");
      });
    });
  }

  private static class GraphData {
    final MutableVertex[] vertices;
    final int             edgeCount;

    GraphData(final MutableVertex[] vertices, final int edgeCount) {
      this.vertices = vertices;
      this.edgeCount = edgeCount;
    }
  }

  private static class BenchmarkResult {
    final String graphType;
    final int    numVertices;
    final int    numEdges;
    final double dijkstraAvgMs;
    final double duanSSSPAvgMs;
    final double speedupRatio;

    BenchmarkResult(final String graphType, final int numVertices, final int numEdges,
        final double dijkstraAvgMs, final double duanSSSPAvgMs, final double speedupRatio) {
      this.graphType = graphType;
      this.numVertices = numVertices;
      this.numEdges = numEdges;
      this.dijkstraAvgMs = dijkstraAvgMs;
      this.duanSSSPAvgMs = duanSSSPAvgMs;
      this.speedupRatio = speedupRatio;
    }

    @Override
    public String toString() {
      return String.format("%s Graph: %d vertices, %d edges\n" +
              "  Dijkstra:    %.3f ms (avg)\n" +
              "  DuanSSSP:    %.3f ms (avg)\n" +
              "  Ratio:       %.2fx (DuanSSSP / Dijkstra)\n",
          graphType, numVertices, numEdges, dijkstraAvgMs, duanSSSPAvgMs, speedupRatio);
    }

    String toTableRow() {
      return String.format("%-20s | %8d | %8d | %12.3f | %12.3f | %10.2fx",
          graphType, numVertices, numEdges, dijkstraAvgMs, duanSSSPAvgMs, speedupRatio);
    }
  }
}
