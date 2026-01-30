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

import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.disk.SimpleMappedReader;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;

/**
 * Utility class for storing and loading JVector Product Quantization (PQ) data.
 * PQ data consists of:
 * - Codebooks: cluster centroids for each subspace
 * - Encoded vectors: compressed byte codes for each vector
 * <p>
 * This enables zero-disk-I/O approximate search by keeping compressed vectors in memory.
 * The PQ file is stored alongside the graph file and vector index using JVector's native
 * serialization format.
 * <p>
 * Unlike graph files which use ArcadeDB's page system, PQ files use direct file I/O because:
 * 1. PQ data is loaded entirely into RAM for zero-disk-I/O search
 * 2. JVector's native serialization is optimized for PQ data
 * 3. There's no need for random access patterns during search
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexPQFile {
  public static final String FILE_EXT = "vecpq";

  private final Path pqFilePath;

  // Cached PQ structures loaded into memory for zero-disk-I/O search
  private volatile ProductQuantization productQuantization;
  private volatile PQVectors           pqVectors;

  /**
   * Create a PQ file handler for the given index file path.
   *
   * @param indexFilePath Base path of the vector index (without extension)
   */
  public LSMVectorIndexPQFile(final String indexFilePath) {
    this.pqFilePath = Path.of(indexFilePath + "." + FILE_EXT);
  }

  /**
   * Returns the cached PQVectors for zero-disk-I/O search.
   * Returns null if PQ data hasn't been loaded yet.
   */
  public PQVectors getPQVectors() {
    return pqVectors;
  }

  /**
   * Returns the cached ProductQuantization codebook.
   * Returns null if PQ data hasn't been loaded yet.
   */
  public ProductQuantization getProductQuantization() {
    return productQuantization;
  }

  /**
   * Checks if PQ data is loaded and ready for approximate search.
   */
  public boolean isPQReady() {
    return pqVectors != null && productQuantization != null;
  }

  /**
   * Checks if a PQ file exists on disk.
   */
  public boolean exists() {
    return Files.exists(pqFilePath);
  }

  /**
   * Write PQ data to the file using JVector's serialization.
   *
   * @param pq      The ProductQuantization codebook to persist
   * @param vectors The PQVectors containing encoded vectors
   */
  public void writePQ(final ProductQuantization pq, final PQVectors vectors) {
    try {
      LogManager.instance().log(this, Level.INFO,
          "Writing PQ data: %d vectors, %d subspaces, %d clusters",
          vectors.count(), pq.getSubspaceCount(), pq.getClusterCount());

      // Create parent directories if needed
      final Path parent = pqFilePath.getParent();
      if (parent != null && !Files.exists(parent)) {
        Files.createDirectories(parent);
      }

      // Write using JVector's native serialization with IndexWriter
      try (SimpleFileIndexWriter writer = new SimpleFileIndexWriter(pqFilePath)) {
        // Write ProductQuantization codebook first
        pq.write(writer, OnDiskGraphIndex.CURRENT_VERSION);
        // Write encoded vectors
        vectors.write(writer, OnDiskGraphIndex.CURRENT_VERSION);
      }

      // Update cached references
      this.productQuantization = pq;
      this.pqVectors = vectors;

      LogManager.instance().log(this, Level.INFO,
          "PQ data written successfully: %.2f MB",
          Files.size(pqFilePath) / (1024.0 * 1024.0));

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error writing PQ data to %s: %s", pqFilePath, e.getMessage());
      throw new IndexException("Failed to write PQ data", e);
    }
  }

  /**
   * Load PQ data from the file into memory.
   * This is called during index initialization to enable zero-disk-I/O search.
   *
   * @return true if PQ data was loaded successfully, false if file doesn't exist
   */
  public boolean loadPQ() {
    try {
      if (!Files.exists(pqFilePath)) {
        LogManager.instance().log(this, Level.FINE,
            "PQ file not found, will be created on next graph build: %s", pqFilePath);
        return false;
      }

      final long fileSize = Files.size(pqFilePath);
      LogManager.instance().log(this, Level.INFO,
          "Loading PQ data from disk: %.2f MB", fileSize / (1024.0 * 1024.0));

      final long startTime = System.currentTimeMillis();

      // Use JVector's SimpleMappedReader for memory-mapped file access
      try (SimpleMappedReader.Supplier supplier = new SimpleMappedReader.Supplier(pqFilePath)) {
        final var reader = supplier.get();
        // Read ProductQuantization codebook first
        this.productQuantization = ProductQuantization.load(reader);
        // Read encoded vectors (at current position after PQ load)
        this.pqVectors = PQVectors.load(reader, reader.getPosition());
      }

      final long elapsed = System.currentTimeMillis() - startTime;
      LogManager.instance().log(this, Level.INFO,
          "PQ data loaded: %d vectors, %d subspaces in %d ms",
          pqVectors.count(), productQuantization.getSubspaceCount(), elapsed);

      return true;

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error loading PQ data from %s: %s", pqFilePath, e.getMessage());
      // Clear cached data on failure
      this.productQuantization = null;
      this.pqVectors = null;
      return false;
    }
  }

  /**
   * Clear cached PQ data from memory.
   * Called when the index is being rebuilt or closed.
   */
  public void clearPQCache() {
    this.productQuantization = null;
    this.pqVectors = null;
  }

  /**
   * Delete the PQ file from disk.
   * Called when the index is being dropped or rebuilt.
   */
  public void deletePQFile() {
    try {
      if (Files.exists(pqFilePath)) {
        Files.delete(pqFilePath);
        LogManager.instance().log(this, Level.INFO, "Deleted PQ file: %s", pqFilePath);
      }
      clearPQCache();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error deleting PQ file: %s", e.getMessage());
    }
  }

  /**
   * Get the path to the PQ data file.
   */
  public Path getFilePath() {
    return pqFilePath;
  }
}
