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
import com.arcadedb.utility.FileUtils;
import io.github.jbellis.jvector.disk.SimpleMappedReader;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;

import java.io.IOException;
import java.nio.file.DirectoryStream;
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
 * <p>
 * The file is read on a later open, so it is never rewritten in place (issue #9796): {@link #writePQ} streams the new
 * data into a temporary sibling, forces it to the device and renames it over the live file. Whatever interrupts the
 * stream, the previous complete file is still there afterwards - there is no window in which the file is neither the
 * old data nor the new.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexPQFile {
  public static final String FILE_EXT = "vecpq";

  private static final String TEMPORARY_SUFFIX = ".tmp";

  private final Path pqFilePath;

  // Cached PQ structures loaded into memory for zero-disk-I/O search
  private volatile ProductQuantization productQuantization;
  private volatile PQVectors           pqVectors;

  // Set when loadPQ() found the file on disk and could not read it, cleared by anything that replaces or removes it.
  // Sticky on purpose: even a transient read error costs one extra rebuild at the next open, which is the safe side,
  // against trusting a file that may be a fragment.
  private volatile boolean loadFailed;

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
   * Checks if a PQ file exists on disk. This says nothing about whether it can be read: a decision that depends on
   * there being PQ data to use asks {@link #hasUsableData()}.
   */
  public boolean exists() {
    return Files.exists(pqFilePath);
  }

  /**
   * Answers "is there PQ data to use?" the way {@link #isPQReady()} does, for the open-time decisions that cannot wait
   * for the data to be loaded: true when the PQ is loaded, or when a file is on disk that no load has found
   * unreadable. A file that exists and failed to load counts as no data - {@link #exists()} alone reports it as
   * present, which is the answer that sent an open down the wrong route (issue #9796).
   * <p>
   * A point-in-time answer: it reads a flag and then the file system, so it is not atomic against a concurrent
   * {@link #writePQ} or {@link #deletePQFile()}. The open-time decision that asks it runs before any writer of this
   * index does.
   */
  public boolean hasUsableData() {
    return isPQReady() || (!loadFailed && exists());
  }

  /**
   * Write PQ data to the file using JVector's serialization.
   * <p>
   * The data goes to a temporary sibling that replaces the live file only once it is complete and forced to the
   * device, so a failure at any point (an interrupt, a full disk, an I/O error, a crash) leaves the previous file
   * exactly as it was and never a fragment. The cached references are published only after the rename.
   * <p>
   * Writers of one file are serialized by the caller ({@code LSMVectorIndex} holds its graph build lock around both
   * of them): a second writer would race on the rename, and {@link #deleteLeftoverTemporaries()} would remove the
   * first one's in-flight temporary.
   *
   * @param pq      The ProductQuantization codebook to persist
   * @param vectors The PQVectors containing encoded vectors
   */
  public void writePQ(final ProductQuantization pq, final PQVectors vectors) {
    Path temporary = null;
    try {
      LogManager.instance().log(this, Level.INFO,
          "Writing PQ data: %d vectors, %d subspaces, %d clusters",
          vectors.count(), pq.getSubspaceCount(), pq.getClusterCount());

      // Create parent directories if needed
      final Path parent = pqFilePath.getParent();
      if (parent != null && !Files.exists(parent)) {
        Files.createDirectories(parent);
      }

      // A process killed between the write and the rename leaves a temporary nothing else would ever remove
      deleteLeftoverTemporaries();

      // Per-write suffix rather than a fixed name, like the graph manifest's and the ordinal map's temporaries
      temporary = pqFilePath.resolveSibling(pqFilePath.getFileName() + "." + Long.toHexString(System.nanoTime()) + TEMPORARY_SUFFIX);

      // Write using JVector's native serialization with IndexWriter
      try (SimpleFileIndexWriter writer = new SimpleFileIndexWriter(temporary)) {
        // Write ProductQuantization codebook first
        pq.write(writer, OnDiskGraphIndex.CURRENT_VERSION);
        // Write encoded vectors
        vectors.write(writer, OnDiskGraphIndex.CURRENT_VERSION);
        // The rename is atomic, the content behind the new name is not durable until it is forced
        writer.force();
      }

      // Rename over the live file and force the directory, so the new name survives a crash: without it the previous
      // name can come back, and with it a previous file that no longer matches the graph persisted before this one.
      // The writer re-asserts an interrupt it absorbed when it closes, and the directory force is an interruptible
      // channel operation that an interrupted thread fails: in exactly the cancel scenario this write is built for,
      // the directory would never be forced. Cleared around the publish and restored after, so the caller is still told.
      final boolean interrupted = Thread.interrupted();
      try {
        FileUtils.publishAtomically(temporary, pqFilePath, true);
      } finally {
        // "interrupted" is the interrupt that was pending before the publish, cleared above so the directory force could
        // run; Thread.interrupted() catches one that landed during the publish itself. Either means the caller is told.
        if (interrupted || Thread.interrupted())
          Thread.currentThread().interrupt();
      }
      temporary = null;

      // Update cached references
      this.loadFailed = false;
      this.productQuantization = pq;
      this.pqVectors = vectors;

      LogManager.instance().log(this, Level.INFO,
          "PQ data written successfully: %.2f MB",
          Files.size(pqFilePath) / (1024.0 * 1024.0));

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error writing PQ data to %s: %s", pqFilePath, e.getMessage());
      throw new IndexException("Failed to write PQ data", e);
    } finally {
      // Still set only when the write did not reach the rename, and an Error (an OutOfMemoryError while streaming a
      // large file) gets here too: the temporary is never left behind, and the live file was not touched
      if (temporary != null) {
        try {
          Files.deleteIfExists(temporary);
        } catch (final IOException ignored) {
          // NOTHING ELSE TO DO
        }
      }
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

      this.loadFailed = false;

      final long elapsed = System.currentTimeMillis() - startTime;
      LogManager.instance().log(this, Level.INFO,
          "PQ data loaded: %d vectors, %d subspaces in %d ms",
          pqVectors.count(), productQuantization.getSubspaceCount(), elapsed);

      return true;

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error loading PQ data from %s: %s. The file is treated as missing: the next graph load rebuilds the graph and the PQ together",
          pqFilePath, e.getMessage());
      // Clear cached data on failure
      this.productQuantization = null;
      this.pqVectors = null;
      this.loadFailed = true;
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
   *
   * @return {@code true} when the file is gone afterwards (deleted, or never there), {@code false} when it could not be
   * deleted. A caller that drops a stale file has to know: one left behind is paired with the next graph on the
   * following open (issue #9796)
   */
  public boolean deletePQFile() {
    try {
      if (Files.exists(pqFilePath)) {
        Files.delete(pqFilePath);
        LogManager.instance().log(this, Level.INFO, "Deleted PQ file: %s", pqFilePath);
      }
      deleteLeftoverTemporaries();
      this.loadFailed = false;
      clearPQCache();
      return true;
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error deleting PQ file: %s", e.getMessage());
      return false;
    }
  }

  private void deleteLeftoverTemporaries() {
    final Path parent = pqFilePath.getParent();
    if (parent == null || !Files.isDirectory(parent))
      return;

    // A filter, not a glob built from the file name: the name is the index's and may carry glob metacharacters, which
    // would throw or match another file's temporary, with the failure swallowed below
    final String prefix = pqFilePath.getFileName() + ".";
    final DirectoryStream.Filter<Path> temporaries = entry -> {
      final String name = entry.getFileName().toString();
      return name.length() > prefix.length() + TEMPORARY_SUFFIX.length() && name.startsWith(prefix) && name.endsWith(
          TEMPORARY_SUFFIX);
    };
    try (final DirectoryStream<Path> leftovers = Files.newDirectoryStream(parent, temporaries)) {
      for (final Path leftover : leftovers)
        Files.deleteIfExists(leftover);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not remove leftover PQ temporaries next to '%s': %s", pqFilePath,
          e.getMessage());
    }
  }

  /**
   * Get the path to the PQ data file.
   */
  public Path getFilePath() {
    return pqFilePath;
  }
}
