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
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Realistic benchmark using MSMARCO v2.1 embeddings (Cohere embed-english-v3, 1024-dim).
 * <p>
 * Before running, download the dataset:
 * <pre>
 *   cd /path/to/humemai/arcadedb-embedded-python/bindings/python/examples
 *   python download_data.py msmarco-1m
 * </pre>
 * <p>
 * Or set the data path via system property:
 * <pre>
 *   -Dmsmarco.dataDir=/path/to/msmarco-1m
 * </pre>
 * <p>
 * The .f32 shard format: flat float32 arrays, row-major, N vectors × D dimensions × 4 bytes.
 * A .meta.json sidecar describes dimensions, count, and shard file list.
 * A .gt.jsonl file has 1000 queries with top-50 exact neighbors for recall computation.
 *
 * Usage:
 *   mvn test -pl engine -Dtest=MSMARCOBenchmark -DfailIfNoTests=false \
 *     -Dmsmarco.dataDir=/path/to/msmarco-1m \
 *     -Dmsmarco.maxVectors=100000
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class MSMARCOBenchmark {

  // Configurable paths and limits
  // Default: ~/datasets/msmarco-1m. Override with -Dmsmarco.dataDir=/path/to/msmarco-1m
  // Download: python3 engine/src/test/resources/msmarco/download_msmarco_1m.py ~/datasets/msmarco-1m
  private static final String DATA_DIR = System.getProperty("msmarco.dataDir",
      System.getProperty("user.home") + "/datasets/msmarco-1m");
  private static final String DB_PATH = "target/test-databases/MSMARCOBenchmark";
  private static final int MAX_VECTORS = Integer.getInteger("msmarco.maxVectors", 0); // 0 = all
  private static final int BATCH_SIZE = Integer.getInteger("msmarco.batchSize", 10_000);
  private static final int K = Integer.getInteger("msmarco.k", 10);
  private static final int EF_SEARCH = Integer.getInteger("msmarco.efSearch", 100);
  private static final int NUM_QUERIES = Integer.getInteger("msmarco.numQueries", 100);
  private static final String QUANTIZATION = System.getProperty("msmarco.quantization", "NONE");

  @Test
  void runMSMARCOBenchmark() throws Exception {
    final Path dataDir = Paths.get(DATA_DIR);
    if (!Files.isDirectory(dataDir)) {
      System.err.println("=== MSMARCO dataset not found at: " + dataDir);
      System.err.println("=== Download it first (see class javadoc for instructions)");
      System.err.println("=== Skipping benchmark.");
      return;
    }

    // Read metadata
    final Path metaFile = findMetaFile(dataDir);
    if (metaFile == null) {
      System.err.println("=== No .meta.json found in " + dataDir);
      return;
    }
    final JSONObject meta = new JSONObject(Files.readString(metaFile));
    final int dimensions = meta.getInt("dim");
    final int totalCount = meta.getInt("count");
    final int vectorLimit = MAX_VECTORS > 0 ? Math.min(MAX_VECTORS, totalCount) : totalCount;

    System.out.println("=== MSMARCO VECTOR BENCHMARK ===");
    System.out.printf("Dataset: %s%n", dataDir);
    System.out.printf("Dimensions: %d | Total vectors: %,d | Loading: %,d | K: %d | efSearch: %d%n",
        dimensions, totalCount, vectorLimit, K, EF_SEARCH);
    System.out.printf("Quantization: %s | Batch: %,d%n", QUANTIZATION, BATCH_SIZE);
    System.out.println();

    // Collect shard files
    final List<Path> shardFiles = collectShardFiles(dataDir, meta);
    if (shardFiles.isEmpty()) {
      System.err.println("=== No .f32 shard files found");
      return;
    }
    System.out.printf("Found %d shard files%n", shardFiles.size());

    // Phase 1: Ingest
    FileUtils.deleteRecursively(new File(DB_PATH));
    GlobalConfiguration.PROFILE.setValue("high-performance");

    final Runtime rt = Runtime.getRuntime();
    rt.gc();
    final long memBefore = rt.totalMemory() - rt.freeMemory();

    System.out.println();
    System.out.println("--- Phase 1: Ingestion ---");
    final long ingestStart = System.nanoTime();
    int totalInserted = 0;

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        // Create schema
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Vector");
          type.createProperty("vectorId", Type.INTEGER);
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

          final String quantStr = QUANTIZATION.equals("NONE") ? "" :
              String.format(", \"quantization\": \"%s\"", QUANTIZATION);
          db.command("sql", String.format(
              "CREATE INDEX ON Vector (embedding) LSM_VECTOR METADATA { \"dimensions\": %d, \"similarity\": \"COSINE\"%s }",
              dimensions, quantStr));
        });

        // Read and insert from shards
        int globalId = 0;
        db.begin();

        for (final Path shardFile : shardFiles) {
          if (globalId >= vectorLimit)
            break;

          final float[][] vectors = readF32Shard(shardFile, dimensions);
          for (final float[] vector : vectors) {
            if (globalId >= vectorLimit)
              break;

            db.newDocument("Vector").set("vectorId", globalId).set("embedding", vector).save();
            globalId++;
            totalInserted++;

            if (totalInserted % BATCH_SIZE == 0) {
              db.commit();
              final double elapsed = (System.nanoTime() - ingestStart) / 1e9;
              final double rate = totalInserted / elapsed;
              System.out.printf("\r  Inserted %,d / %,d vectors (%.0f v/s, %.1fs elapsed)",
                  totalInserted, vectorLimit, rate, elapsed);
              db.begin();
            }
          }
        }

        if (db.isTransactionActive())
          db.commit();
      }

      final long ingestEnd = System.nanoTime();
      final double ingestSec = (ingestEnd - ingestStart) / 1e9;
      final double vectorsPerSec = totalInserted / ingestSec;
      System.out.printf("%n  Done: %,d vectors in %.2fs (%,.0f vectors/sec)%n", totalInserted, ingestSec, vectorsPerSec);

      // Phase 2: Reopen + Graph Build
      System.out.println();
      System.out.println("--- Phase 2: Graph Build ---");
      final long reopenStart = System.nanoTime();

      try (final Database db = factory.open()) {
        final double reopenSec = (System.nanoTime() - reopenStart) / 1e9;
        System.out.printf("  Reopen: %.2fs%n", reopenSec);

        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Vector")
            .getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets()[0];

        // First query triggers graph build
        final float[] firstQuery = readFirstVector(shardFiles.getFirst(), dimensions);
        final long buildStart = System.nanoTime();
        index.findNeighborsFromVector(firstQuery, K, EF_SEARCH);
        final double buildSec = (System.nanoTime() - buildStart) / 1e9;

        rt.gc();
        final long memAfter = rt.totalMemory() - rt.freeMemory();
        System.out.printf("  Graph build (first query): %.2fs%n", buildSec);
        System.out.printf("  Memory delta: %d MB%n", (memAfter - memBefore) / (1024 * 1024));

        // Phase 3: Search
        System.out.println();
        System.out.println("--- Phase 3: Search ---");

        // Load ground truth if available
        final GroundTruth gt = loadGroundTruth(dataDir, dimensions);
        final float[][] queryVectors;
        final int[][] gtNeighborIds;

        if (gt != null) {
          final int nQueries = Math.min(NUM_QUERIES, gt.queries.length);
          queryVectors = Arrays.copyOf(gt.queries, nQueries);
          gtNeighborIds = Arrays.copyOf(gt.neighborIds, nQueries);
          System.out.printf("  Using %d ground-truth queries (top-%d neighbors)%n", nQueries, gt.neighborIds[0].length);
        } else {
          // No ground truth — use first N vectors from the dataset as queries
          queryVectors = readNVectors(shardFiles, dimensions, NUM_QUERIES);
          gtNeighborIds = null;
          System.out.printf("  No ground truth found — using %d dataset vectors as queries (recall not available)%n", queryVectors.length);
        }

        // Warmup
        for (int i = 0; i < Math.min(20, queryVectors.length); i++)
          index.findNeighborsFromVector(queryVectors[i], K, EF_SEARCH);

        // Timed search
        final int nQueries = queryVectors.length;
        final long[] latencies = new long[nQueries];
        final List<List<Pair<RID, Float>>> allResults = new ArrayList<>(nQueries);

        for (int q = 0; q < nQueries; q++) {
          final long qStart = System.nanoTime();
          final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVectors[q], K, EF_SEARCH);
          latencies[q] = System.nanoTime() - qStart;
          allResults.add(results);
        }

        Arrays.sort(latencies);
        final double meanUs = Arrays.stream(latencies).average().orElse(0) / 1e3;
        final double p50Us = latencies[nQueries / 2] / 1e3;
        final double p95Us = latencies[(int) (nQueries * 0.95)] / 1e3;
        final double p99Us = latencies[(int) (nQueries * 0.99)] / 1e3;

        System.out.printf("  Latency: mean=%.0fus  p50=%.0fus  p95=%.0fus  p99=%.0fus%n",
            meanUs, p50Us, p95Us, p99Us);

        // Compute recall if ground truth available
        if (gtNeighborIds != null) {
          // Build vectorId → RID mapping
          final Map<Integer, RID> vectorIdToRid = new HashMap<>(totalInserted * 4 / 3 + 1);
          db.iterateType("Vector", false).forEachRemaining(rec -> {
            final int vid = rec.asDocument().getInteger("vectorId");
            vectorIdToRid.put(vid, rec.getIdentity());
          });

          double totalRecall = 0;
          int validQueries = 0;

          for (int q = 0; q < nQueries; q++) {
            final List<Pair<RID, Float>> results = allResults.get(q);
            if (results.isEmpty())
              continue;

            final Set<RID> resultRids = new HashSet<>(results.size() * 2);
            for (final Pair<RID, Float> p : results)
              resultRids.add(p.getFirst());

            int hits = 0;
            final int gtK = Math.min(K, gtNeighborIds[q].length);
            for (int i = 0; i < gtK; i++) {
              final int gtId = gtNeighborIds[q][i];
              final RID gtRid = vectorIdToRid.get(gtId);
              if (gtRid != null && resultRids.contains(gtRid))
                hits++;
            }
            totalRecall += (double) hits / K;
            validQueries++;
          }

          final double avgRecall = validQueries > 0 ? totalRecall / validQueries : 0;
          System.out.printf("  Recall@%d: %.3f (%d queries)%n", K, avgRecall, validQueries);
        }

        // Stats
        final Map<String, Long> stats = index.getStats();
        System.out.printf("  Stats: fetchGraph=%d fetchDoc=%d fetchQuantized=%d%n",
            stats.getOrDefault("vectorFetchFromGraph", 0L),
            stats.getOrDefault("vectorFetchFromDocuments", 0L),
            stats.getOrDefault("vectorFetchFromQuantized", 0L));

        // Summary
        System.out.println();
        System.out.println("=== SUMMARY ===");
        System.out.printf("  Vectors: %,d (%d-dim, %s quantization)%n", totalInserted, dimensions, QUANTIZATION);
        System.out.printf("  Ingestion: %.2fs (%,.0f vectors/sec)%n", ingestSec, vectorsPerSec);
        System.out.printf("  Graph build: %.2fs%n", buildSec);
        System.out.printf("  Search: mean=%.0fus  p50=%.0fus  p95=%.0fus%n", meanUs, p50Us, p95Us);
        System.out.println("================");
      }
    }
  }

  // ==================== File I/O ====================

  /**
   * Read a .f32 shard file: flat float32 array, row-major, N × D × 4 bytes.
   */
  private float[][] readF32Shard(final Path shardFile, final int dimensions) throws IOException {
    final long fileSize = Files.size(shardFile);
    final int vectorBytes = dimensions * Float.BYTES;
    final int numVectors = (int) (fileSize / vectorBytes);

    final float[][] vectors = new float[numVectors][dimensions];

    try (final FileChannel channel = FileChannel.open(shardFile)) {
      final ByteBuffer buf = ByteBuffer.allocate(vectorBytes).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < numVectors; i++) {
        buf.clear();
        channel.read(buf);
        buf.flip();
        for (int d = 0; d < dimensions; d++)
          vectors[i][d] = buf.getFloat();
      }
    }
    return vectors;
  }

  /**
   * Read the first vector from the first shard (used as initial query to trigger graph build).
   */
  private float[] readFirstVector(final Path shardFile, final int dimensions) throws IOException {
    final int vectorBytes = dimensions * Float.BYTES;
    try (final FileChannel channel = FileChannel.open(shardFile)) {
      final ByteBuffer buf = ByteBuffer.allocate(vectorBytes).order(ByteOrder.LITTLE_ENDIAN);
      channel.read(buf);
      buf.flip();
      final float[] vector = new float[dimensions];
      for (int d = 0; d < dimensions; d++)
        vector[d] = buf.getFloat();
      return vector;
    }
  }

  /**
   * Read N vectors from the beginning of the shard files.
   */
  private float[][] readNVectors(final List<Path> shardFiles, final int dimensions, final int n) throws IOException {
    final List<float[]> vectors = new ArrayList<>(n);
    for (final Path shard : shardFiles) {
      if (vectors.size() >= n)
        break;
      final float[][] shardVectors = readF32Shard(shard, dimensions);
      for (final float[] v : shardVectors) {
        vectors.add(v);
        if (vectors.size() >= n)
          break;
      }
    }
    return vectors.toArray(new float[0][]);
  }

  /**
   * Find the .meta.json file in the data directory.
   */
  private Path findMetaFile(final Path dataDir) {
    try {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.meta.json")) {
        for (final Path p : stream)
          return p;
      }
      // Check subdirectories (the format may nest under a corpus name like "all")
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir)) {
        for (final Path sub : stream) {
          if (Files.isDirectory(sub)) {
            try (DirectoryStream<Path> inner = Files.newDirectoryStream(sub, "*.meta.json")) {
              for (final Path p : inner)
                return p;
            }
          }
        }
      }
    } catch (final IOException e) {
      // ignore
    }
    return null;
  }

  /**
   * Collect .f32 shard files in order.
   */
  private List<Path> collectShardFiles(final Path dataDir, final JSONObject meta) {
    final List<Path> shards = new ArrayList<>();
    final Path metaDir = findMetaFile(dataDir) != null ? findMetaFile(dataDir).getParent() : dataDir;

    if (meta.has("shards")) {
      final JSONArray shardArray = meta.getJSONArray("shards");
      for (int i = 0; i < shardArray.length(); i++) {
        final JSONObject shardObj = shardArray.getJSONObject(i);
        final String filename = shardObj.getString("file");
        final Path shardPath = metaDir.resolve(filename);
        if (Files.exists(shardPath))
          shards.add(shardPath);
      }
    }

    if (shards.isEmpty()) {
      // Fallback: find all .f32 files sorted by name
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(metaDir, "*.f32")) {
        for (final Path p : stream)
          shards.add(p);
      } catch (final IOException e) {
        // ignore
      }
      shards.sort(Comparator.comparing(Path::getFileName));
    }

    return shards;
  }

  /**
   * Load ground truth from .gt.jsonl file.
   * Format: one JSON object per line with "query" (float[]) and "neighbors" (int[]).
   */
  private GroundTruth loadGroundTruth(final Path dataDir, final int dimensions) {
    // Search for .gt.jsonl in dataDir and subdirectories
    Path gtFile = null;
    try {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.gt.jsonl")) {
        for (final Path p : stream) {
          gtFile = p;
          break;
        }
      }
      if (gtFile == null) {
        // Check subdirectories
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir)) {
          for (final Path sub : stream) {
            if (Files.isDirectory(sub)) {
              try (DirectoryStream<Path> inner = Files.newDirectoryStream(sub, "*.gt.jsonl")) {
                for (final Path p : inner) {
                  gtFile = p;
                  break;
                }
              }
            }
            if (gtFile != null)
              break;
          }
        }
      }
    } catch (final IOException e) {
      return null;
    }

    if (gtFile == null)
      return null;

    try {
      final List<String> lines = Files.readAllLines(gtFile);
      final List<float[]> queries = new ArrayList<>(lines.size());
      final List<int[]> neighbors = new ArrayList<>(lines.size());

      for (final String line : lines) {
        if (line.isBlank())
          continue;
        final JSONObject obj = new JSONObject(line);

        // Parse query vector
        final JSONArray qArr = obj.getJSONArray("query");
        final float[] query = new float[qArr.length()];
        for (int i = 0; i < qArr.length(); i++)
          query[i] = (float) qArr.getDouble(i);
        queries.add(query);

        // Parse neighbor IDs (global sequential IDs matching our vectorId)
        final JSONArray nArr = obj.getJSONArray("neighbors");
        final int[] nIds = new int[nArr.length()];
        for (int i = 0; i < nArr.length(); i++)
          nIds[i] = nArr.getInt(i);
        neighbors.add(nIds);
      }

      System.out.printf("  Loaded ground truth: %d queries, top-%d neighbors from %s%n",
          queries.size(), neighbors.isEmpty() ? 0 : neighbors.getFirst().length, gtFile.getFileName());

      return new GroundTruth(
          queries.toArray(new float[0][]),
          neighbors.toArray(new int[0][])
      );
    } catch (final Exception e) {
      System.err.println("  Warning: could not parse ground truth: " + e.getMessage());
      return null;
    }
  }

  private record GroundTruth(float[][] queries, int[][] neighborIds) {
  }
}
