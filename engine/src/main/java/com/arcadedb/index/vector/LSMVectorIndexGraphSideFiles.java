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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.NodeLocalFiles;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * The plain side files that vouch for an LSM vector index's persisted graph - the {@code .vecgraphfp} manifest, the
 * {@code .vecordmap} ordinal map and, for a PRODUCT-quantized index, the {@code .vecpq} product quantization - observed
 * at one moment, so that a point-in-time copy of the {@code .vecgraph} pages can carry them and be reused rather than
 * rebuilt (issue #9917, the follow-up to #9888).
 * <p>
 * <b>Why an observation is needed at all.</b> The side files are not page components, so the page snapshot window
 * cannot capture them at its t0, and a copy read at an arbitrary later moment can describe a different generation of
 * the graph than the pages captured at t0: a graph persist rewrites the pages in chunked commits and only then the
 * manifest. Paired the wrong way round, the manifest would certify pages it was not written for.
 * <p>
 * <b>The protocol.</b> {@link #capture} records each side file's identity (file key, size, times) BEFORE the window
 * opens; {@link #openIfUnchanged} opens the files AFTER it opened and accepts them only when every identity still
 * matches. That proves the pages at t0 are the ones the manifest certifies, because of how the writers order their
 * work (see {@link LSMVectorIndexGraphFile#writeGraph} and {@link LSMVectorIndexGraphManifest}):
 * <ul>
 * <li>A persist DELETES the manifest before it writes the first page ({@link LSMVectorIndexGraphManifest#invalidate()})
 * and writes a new one only after its final commit, through a temporary file moved into place - a new file, with a new
 * identity. A manifest that is the same file before t0 and after it was therefore written by a persist that had
 * committed before the capture, and no persist began between the capture and the reopen. The window's t0 image holds
 * exactly that persist's pages.</li>
 * <li>The ordinal map is written immediately before the manifest and dropped with it, so it can only change together
 * with it; its identity is checked anyway, absent-and-still-absent included.</li>
 * <li>The PQ is written AFTER the manifest certifies the graph, and a persist deletes the previous one before it writes
 * the first page ({@code LSMVectorIndex.dropPersistedProductQuantizationBeforeGraphWrite}). So a PQ that is the same
 * file before t0 and after it was written for the graph the unchanged manifest certifies: the persist that wrote that
 * manifest had deleted any older PQ before it started, and the PQ is no longer there while the new one is still owed.
 * A PRODUCT-quantized index whose PQ is missing is refused - its restored copy would rebuild its graph anyway. No
 * modification times are compared: the manifest and the PQ are written milliseconds apart, which a coarse file system
 * clock cannot order.</li>
 * <li>Every writer replaces these files by renaming a new one over them and never rewrites one in place, so a file
 * opened while its identity still matched the capture keeps the captured bytes however long the copy takes, even if a
 * persist replaces or deletes it meanwhile.</li>
 * </ul>
 * The proof rests on {@code writeGraph} being the only writer of the graph pages, which is the case today: it is the one
 * user of {@code ContiguousPageWriter} on a graph file.
 * <p>
 * Anything that does not satisfy the protocol - a manifest missing at the capture (a persist in flight, or none ever
 * completed), a file replaced in between, an index whose graph component is not its current one - yields no side
 * files, and the caller falls back to the #9888 behaviour of an empty graph that the restored database rebuilds.
 */
public final class LSMVectorIndexGraphSideFiles {
  private final Path         manifest;
  private final Path         ordinalMap;
  private final Path         productQuantization;
  private final FileIdentity manifestIdentity;
  private final FileIdentity ordinalMapIdentity;
  private final FileIdentity productQuantizationIdentity;

  /**
   * What a file was when it was observed. Two observations of the same path are equal only when no writer replaced
   * the file between them: every writer of a side file moves a NEW file into place, which changes the file key on any
   * file system that has one, and the modification and creation times on the ones that do not.
   * <p>
   * <b>The proof is weaker where there is no file key</b> (Windows: {@code fileKey()} is {@code null}). There a
   * replacement is told apart only by its size and its two timestamps, so one of the same size whose temporary file was
   * created and written within the file system's timestamp granularity of the original would pass for it. NTFS records
   * both times at 100 ns, and a persist writes every page of a graph between deleting the manifest and writing the
   * next one, so this is a theoretical gap rather than a race a real persist can win - but it is not the strict proof
   * the file key gives. Comparing the manifest's bytes would not close it: a rebuild over the same vectors writes the
   * same manifest.
   */
  record FileIdentity(Object fileKey, long size, long lastModifiedNanos, long createdNanos) {
    /** @return the identity of the file at {@code path}, or {@code null} when there is none */
    static FileIdentity of(final Path path) throws IOException {
      try {
        final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
        return new FileIdentity(attributes.fileKey(), attributes.size(), attributes.lastModifiedTime().to(TimeUnit.NANOSECONDS),
            attributes.creationTime().to(TimeUnit.NANOSECONDS));
      } catch (final NoSuchFileException e) {
        return null;
      }
    }
  }

  /**
   * One side file, opened and verified, to be archived under {@code fileName}. The stream is owned by the
   * {@link Opened} it came from.
   */
  public record Entry(String fileName, long lastModifiedMillis, InputStream input) {
  }

  /**
   * The side files of one graph, opened while still the ones {@link #capture} observed. Closing it closes every stream
   * that has not been consumed; closing a stream twice is harmless.
   */
  public static final class Opened implements AutoCloseable {
    private final List<Entry> entries;

    private Opened(final List<Entry> entries) {
      this.entries = Collections.unmodifiableList(entries);
    }

    /** The ordinal map and the PQ when there are, then the manifest. */
    public List<Entry> entries() {
      return entries;
    }

    @Override
    public void close() {
      closeAll(entries);
    }
  }

  private LSMVectorIndexGraphSideFiles(final Path manifest, final FileIdentity manifestIdentity, final Path ordinalMap,
      final FileIdentity ordinalMapIdentity, final Path productQuantization, final FileIdentity productQuantizationIdentity) {
    this.manifest = manifest;
    this.manifestIdentity = manifestIdentity;
    this.ordinalMap = ordinalMap;
    this.ordinalMapIdentity = ordinalMapIdentity;
    this.productQuantization = productQuantization;
    this.productQuantizationIdentity = productQuantizationIdentity;
  }

  /**
   * Observes the side files of every LSM vector graph registered with {@code database} right now. Call it BEFORE the
   * point in time being copied is fixed (the page snapshot window's t0), so that {@link #openIfUnchanged} can prove
   * nothing touched them across it.
   *
   * @return the graphs whose side files can vouch for their pages, keyed by the file id of their {@code .vecgraph}
   * component. A graph left out is one to copy empty.
   */
  public static Map<Integer, LSMVectorIndexGraphSideFiles> capture(final DatabaseInternal database) {
    final Map<Integer, LSMVectorIndexGraphSideFiles> captured = new HashMap<>();
    for (final ComponentFile file : new ArrayList<>(database.getFileManager().getFiles())) {
      if (file == null || !NodeLocalFiles.isVectorGraphComponentFileName(file.getFileName()))
        continue;
      final Component component = database.getSchema().getFileByIdIfExists(file.getFileId());
      if (component instanceof LSMVectorIndexGraphFile graphFile) {
        final LSMVectorIndexGraphSideFiles sideFiles = capture(graphFile);
        if (sideFiles != null)
          captured.put(file.getFileId(), sideFiles);
      }
    }
    return captured;
  }

  /**
   * Observes the side files of one graph.
   *
   * @return {@code null} when they cannot vouch for the pages: the graph is not its index's current one, there is no
   * manifest, or the index is PRODUCT-quantized and has no PQ on disk
   */
  static LSMVectorIndexGraphSideFiles capture(final LSMVectorIndexGraphFile graphFile) {
    // The PQ is found through the index, which names it after its own component rather than after the graph. A graph
    // that is not the index's current one (the pre-compaction file a COMPACT INDEX keeps until its replacement is
    // certified) has no PQ of its own, so it is not vouched for at all
    if (!(graphFile.getMainComponent() instanceof LSMVectorIndex index) || index.getGraphFile() != graphFile)
      return null;

    final LSMVectorIndexGraphManifest graphManifest = graphFile.getManifest();
    final Path manifestPath = graphManifest.getFilePath();
    final Path ordinalMapPath = graphManifest.getOrdinalMapFilePath();
    final Path pqPath = index.getProductQuantizationFilePath();
    try {
      final FileIdentity manifestIdentity = FileIdentity.of(manifestPath);
      if (manifestIdentity == null)
        // A persist in flight, or none ever completed: there is nothing to vouch for the pages
        return null;

      final FileIdentity pqIdentity = pqPath != null ? FileIdentity.of(pqPath) : null;
      if (pqPath != null && (pqIdentity == null || index.isPersistedPQKnownStale()))
        // Not written yet for the graph this manifest certifies, dropped after a failed write, or an earlier graph's
        // that could not be deleted
        return null;

      return new LSMVectorIndexGraphSideFiles(manifestPath, manifestIdentity, ordinalMapPath, FileIdentity.of(ordinalMapPath),
          pqPath, pqIdentity);
    } catch (final IOException e) {
      LogManager.instance().log(LSMVectorIndexGraphSideFiles.class, Level.FINE,
          "Cannot observe the side files of vector graph '%s', it will be copied without them: %s", graphFile.getName(),
          e.getMessage());
      return null;
    }
  }

  /**
   * Opens the side files and checks, once they are open, that each one is still the file {@link #capture} observed.
   * Call it AFTER the point in time being copied is fixed.
   *
   * @return the opened files, or {@code null} when any of them changed or could not be opened - the graph pages must
   * then be copied empty. A side file that cannot be read costs the graph a rebuild after a restore, never the backup
   */
  public Opened openIfUnchanged() {
    final List<Entry> entries = new ArrayList<>(3);
    boolean verified = false;
    try {
      // OPEN FIRST, CHECK SECOND. A file opened and then found to be the captured one is the captured one: the path
      // can only name it again after naming something else if a writer re-linked an unlinked file, and none does. The
      // reverse order would leave a gap between the check and the open in which a persist can replace the file
      if (!open(ordinalMap, ordinalMapIdentity, entries) || !open(productQuantization, productQuantizationIdentity, entries)
          || !open(manifest, manifestIdentity, entries))
        return null;

      if (!Objects.equals(FileIdentity.of(manifest), manifestIdentity) || !Objects.equals(FileIdentity.of(ordinalMap),
          ordinalMapIdentity) || (productQuantization != null && !Objects.equals(FileIdentity.of(productQuantization),
          productQuantizationIdentity)))
        return null;

      verified = true;
      return new Opened(entries);
    } catch (final IOException e) {
      LogManager.instance().log(LSMVectorIndexGraphSideFiles.class, Level.FINE,
          "Cannot open the side files next to '%s', the graph will be copied without them: %s", manifest, e.getMessage());
      return null;
    } finally {
      if (!verified)
        closeAll(entries);
    }
  }

  /**
   * Opens {@code path} into {@code entries} when it was there at the capture.
   *
   * @return {@code false} when the file is gone - it was there at the capture, so something deleted it since
   */
  private static boolean open(final Path path, final FileIdentity captured, final List<Entry> entries) throws IOException {
    if (path == null || captured == null)
      // Nothing to open: absent at the capture, and the identity check that follows says whether it still is
      return true;
    try {
      // Files.newInputStream rather than FileInputStream: on Windows it opens with FILE_SHARE_DELETE by default, so a persist can
      // still rename over the file while it is being copied, as it can on POSIX
      entries.add(new Entry(path.getFileName().toString(), TimeUnit.NANOSECONDS.toMillis(captured.lastModifiedNanos()),
          Files.newInputStream(path)));
      return true;
    } catch (final NoSuchFileException e) {
      return false;
    }
  }

  private static void closeAll(final List<Entry> entries) {
    for (final Entry entry : entries)
      try {
        entry.input().close();
      } catch (final IOException ignored) {
        // A STREAM THAT CANNOT EVEN BE CLOSED HAS NOTHING LEFT TO GIVE
      }
  }
}
