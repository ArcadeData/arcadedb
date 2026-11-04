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
import io.github.jbellis.jvector.disk.RandomAccessReader;
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

  // The identity trailer: this magic, then the fingerprint (LSMVectorIndexGraphManifest) of the ordinal -> (vector id, RID)
  // map of the graph the codes were built for, 16 bytes at the very end of the file. JVector's loaders stop at the end of
  // the codes and ignore what follows, so a file with a trailer loads exactly as one without, and a file written before the
  // trailer existed simply has none: it is read as "identity unknown" and the reopen takes the walk, never a fast path.
  static final long GRAPH_IDENTITY_MAGIC = 0x5051475241504831L; // "PQGRAPH1"
  static final int  GRAPH_IDENTITY_BYTES = 16;

  // The codebook trailer (issue #10847): this magic, the number of vectors the index held when the codebook was trained, and
  // the mean relative reconstruction error of the codebook on its training vectors, 24 bytes right BEFORE the identity
  // trailer (or at the very end of a file with none). The identity stays the last 16 bytes, which is where every build
  // looks for it, so a build that predates this trailer reads a file carrying one exactly as before. A file without one has
  // unknown statistics, which the next rebuild answers by training a new codebook.
  static final long CODEBOOK_STATS_MAGIC = 0x5051434F44454231L; // "PQCODEB1"
  static final int  CODEBOOK_STATS_BYTES = 24;

  private final Path pqFilePath;

  /**
   * What a codebook was trained on, which decides whether a rebuild may keep it (issue #10847).
   *
   * @param trainedAtCount the number of vectors the index held when the codebook was trained
   * @param baselineError  the mean relative reconstruction error ({@code |v - decode(encode(v))|^2 / |v|^2}) of the codebook
   *                       on a sample of the vectors it was trained on
   */
  public record CodebookStats(long trainedAtCount, double baselineError) {
  }

  /**
   * The codebook, the codes and the identity of the graph they were built for, as one immutable value. They are published
   * and cleared together by a single volatile write, so a reader never sees the codes of one write with the codebook or
   * the identity of another, whatever lock the writer and the reader hold. {@code graphFingerprint} is {@code null} when
   * the file carries none (written by a build older than the trailer, or while the graph's manifest could not vouch for an
   * ordinal map of the same size). {@code stats} is {@code null} when the file carries no codebook trailer.
   */
  private record Loaded(ProductQuantization productQuantization, PQVectors pqVectors, Long graphFingerprint,
                        CodebookStats stats) {
  }

  // Cached PQ structures loaded into memory for zero-disk-I/O search
  private volatile Loaded loaded;

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
    final Loaded current = loaded;
    return current != null ? current.pqVectors : null;
  }

  /**
   * Returns the cached ProductQuantization codebook.
   * Returns null if PQ data hasn't been loaded yet.
   */
  public ProductQuantization getProductQuantization() {
    final Loaded current = loaded;
    return current != null ? current.productQuantization : null;
  }

  /**
   * The identity of the graph the given codes were built for: the fingerprint recorded with them, or {@code null} when
   * {@code codes} is not the instance this handler holds or carries no recorded identity. The caller compares it with the
   * fingerprint of the graph it is about to pair them with. The codes and the identity are one published value, so they
   * always belong to the same write.
   */
  Long graphFingerprintOf(final PQVectors codes) {
    final Loaded current = loaded;
    return codes != null && current != null && codes == current.pqVectors ? current.graphFingerprint : null;
  }

  /** The statistics of the codebook that encoded {@code codes}, or {@code null}: the twin of {@link #graphFingerprintOf}. */
  CodebookStats codebookStatsOf(final PQVectors codes) {
    final Loaded current = loaded;
    return codes != null && current != null && codes == current.pqVectors ? current.stats : null;
  }

  /**
   * Checks if PQ data is loaded and ready for approximate search.
   */
  public boolean isPQReady() {
    return loaded != null;
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
    writePQ(pq, vectors, null);
  }

  /**
   * As {@link #writePQ(ProductQuantization, PQVectors)}, recording in the file which graph the codes were built for.
   *
   * @param graphFingerprint the fingerprint of the ordinal map of that graph, or {@code null} to record none
   */
  public void writePQ(final ProductQuantization pq, final PQVectors vectors, final Long graphFingerprint) {
    writePQ(pq, vectors, graphFingerprint, null);
  }

  /**
   * As {@link #writePQ(ProductQuantization, PQVectors, Long)}, recording also what the codebook was trained on (issue #10847).
   *
   * @param stats the statistics of the codebook, or {@code null} to record none
   */
  public void writePQ(final ProductQuantization pq, final PQVectors vectors, final Long graphFingerprint, final CodebookStats stats) {
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
        // Before the identity, which stays the last 16 bytes of the file
        if (stats != null) {
          writer.writeLong(CODEBOOK_STATS_MAGIC);
          writer.writeLong(stats.trainedAtCount());
          writer.writeLong(Double.doubleToLongBits(stats.baselineError()));
        }
        if (graphFingerprint != null) {
          writer.writeLong(GRAPH_IDENTITY_MAGIC);
          writer.writeLong(graphFingerprint);
        }
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
      this.loaded = new Loaded(pq, vectors, graphFingerprint, stats);

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
      final Loaded read;

      // Use JVector's SimpleMappedReader for memory-mapped file access
      try (SimpleMappedReader.Supplier supplier = new SimpleMappedReader.Supplier(pqFilePath)) {
        final var reader = supplier.get();
        // Read ProductQuantization codebook first
        final ProductQuantization loadedPq = ProductQuantization.load(reader);
        // Read encoded vectors (at current position after PQ load)
        final PQVectors loadedCodes = PQVectors.load(reader, reader.getPosition());
        // The identity is read from this same mapped snapshot, right where the codes ended: a second open of the path could
        // meet a file a concurrent write has since renamed over this one, and pair these codes with its identity
        final long codesEnd = reader.getPosition();
        final Long fingerprint = readGraphFingerprint(reader);
        read = new Loaded(loadedPq, loadedCodes, fingerprint, readCodebookStats(reader, codesEnd, fingerprint != null));
      }
      this.loaded = read;

      this.loadFailed = false;

      final long elapsed = System.currentTimeMillis() - startTime;
      LogManager.instance().log(this, Level.INFO,
          "PQ data loaded: %d vectors, %d subspaces in %d ms",
          read.pqVectors.count(), read.productQuantization.getSubspaceCount(), elapsed);

      return true;

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error loading PQ data from %s: %s. The file is treated as missing: the next graph load rebuilds the graph and the PQ together",
          pqFilePath, e.getMessage());
      // Clear cached data on failure
      this.loaded = null;
      this.loadFailed = true;
      return false;
    }
  }

  /**
   * Clear cached PQ data from memory.
   * Called when the index is being rebuilt or closed.
   */
  public void clearPQCache() {
    this.loaded = null;
  }

  /**
   * @param reader positioned where the codes ended
   *
   * @return the graph fingerprint in the identity trailer that follows them, or {@code null} when there is none (a file
   * written before the trailer existed, or without an identity)
   */
  private static Long readGraphFingerprint(final RandomAccessReader reader) throws IOException {
    final long length = reader.length();
    if (length - reader.getPosition() < GRAPH_IDENTITY_BYTES)
      return null;
    reader.seek(length - GRAPH_IDENTITY_BYTES);
    return reader.readLong() == GRAPH_IDENTITY_MAGIC ? reader.readLong() : null;
  }

  /**
   * The codebook trailer, read where {@link #writePQ(ProductQuantization, PQVectors, Long, CodebookStats)} puts it: right
   * before the identity trailer when the file has one, at the very end otherwise. {@code null} when there is none, or when
   * what is there is not a plausible statistic: an implausible one would keep a codebook the next rebuild could not judge.
   */
  private static CodebookStats readCodebookStats(final RandomAccessReader reader, final long codesEnd, final boolean hasIdentity)
      throws IOException {
    final long end = reader.length() - (hasIdentity ? GRAPH_IDENTITY_BYTES : 0);
    if (end - codesEnd < CODEBOOK_STATS_BYTES)
      return null;
    reader.seek(end - CODEBOOK_STATS_BYTES);
    if (reader.readLong() != CODEBOOK_STATS_MAGIC)
      return null;
    final long trainedAtCount = reader.readLong();
    final double baselineError = Double.longBitsToDouble(reader.readLong());
    return trainedAtCount > 0 && Double.isFinite(baselineError) && baselineError >= 0 ?
        new CodebookStats(trainedAtCount, baselineError) :
        null;
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
