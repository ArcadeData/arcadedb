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
package com.arcadedb.index.columnar;

import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

/**
 * On-disk layout of the columnar projections of one database, and the durability helpers every publish uses (spec 5.5).
 * <pre>
 * &lt;databasePath&gt;/columnar/           ROOT_DIR, no dot in the name: FileManager registers only dotted files of the database directory
 *   REBUILD                         rebuild-reason marker, holds a reason name (for example "snapshot_install")
 *   b&lt;bucketFileId&gt;/                one directory per projected bucket, keyed by file id only
 *     manifest.json                 the published state, replaced atomically
 *     NOWAL                         marker of a no-WAL write not covered by a flush
 *     seg-*.acs, seg-*.dead.*, dict-*.dic, owner maps
 * </pre>
 * Publish order: segment, dead set, dictionary and owner map files are written and fsynced first; then the bucket directory is
 * fsynced, because POSIX does not order a file creation before a later rename without it; then the manifest is written through
 * {@link #writeAtomically}; only then are unreferenced files deleted ({@link #deleteUnreferenced}).
 */
public final class ColumnarStore {
  public static final String ROOT_DIR       = "columnar";
  public static final String REBUILD_MARKER = "REBUILD";
  public static final String NOWAL_MARKER   = "NOWAL";
  /** Suffix of the temporary file {@link #writeAtomically} writes next to its target. */
  static final        String TMP_SUFFIX     = ".tmp";

  private static final String SNAPSHOT_INSTALL = "snapshot_install";

  private ColumnarStore() {
  }

  /** {@code <databasePath>/columnar/b<bucketFileId>}: a bucket rename keeps its file id, so it keeps its projection. */
  public static Path bucketDir(final Path databasePath, final int bucketFileId) {
    return databasePath.resolve(ROOT_DIR).resolve("b" + bucketFileId);
  }

  /** Creates the bucket directory (and the root) when missing, each new directory entry fsynced into its parent. */
  public static Path createBucketDir(final Path databasePath, final int bucketFileId) throws IOException {
    final Path dir = bucketDir(databasePath, bucketFileId);
    FileUtils.createDirectoriesDurably(dir);
    return dir;
  }

  public static void fsyncFile(final Path file) throws IOException {
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  /**
   * fsyncs the directory's entries through {@link FileUtils#forceDirectory}: skipped on Windows, which has no directory fsync, and
   * tolerated (logged once per JVM) on any file store that refuses the open or fails the force. Never throws today; the
   * {@code IOException} is kept for a stricter policy.
   */
  public static void fsyncDirectory(final Path dir) throws IOException {
    FileUtils.forceDirectory(dir);
  }

  /**
   * Replaces {@code target} with {@code content} so that a reader, after a crash at any point, finds either the previous complete
   * file or the new one: {@code <target>.tmp} written and forced, atomic rename, parent directory fsynced. One writer per target.
   */
  public static void writeAtomically(final Path target, final byte[] content) throws IOException {
    writeAtomically(target, content, null, null);
  }

  /** {@link #writeAtomically(Path, byte[])} reaching the given kill points (null for none) after the tmp write and after the rename. */
  static void writeAtomically(final Path target, final byte[] content, final String tmpWrittenPoint, final String renamedPoint)
      throws IOException {
    writeTmp(target, content, tmpWrittenPoint);
    renameTmp(target, renamedPoint);
  }

  /**
   * First half of {@link #writeAtomically}: writes and forces {@code <target>.tmp}; the target is untouched, so a failure here leaves the
   * previous file in place.
   */
  static void writeTmp(final Path target, final byte[] content, final String tmpWrittenPoint) throws IOException {
    writeTmpFile(target.resolveSibling(target.getFileName() + TMP_SUFFIX), content, tmpWrittenPoint);
  }

  /** {@link #writeTmp} into the given tmp file, for a writer whose tmp name is its own (see {@link ColumnarManifest#writeTmp}). */
  static void writeTmpFile(final Path tmp, final byte[] content, final String tmpWrittenPoint) throws IOException {
    try (final FileChannel channel = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE)) {
      FileUtils.writeFully(channel, ByteBuffer.wrap(content), 0);
      channel.force(true);
    }
    if (tmpWrittenPoint != null)
      ColumnarTestHooks.reach(tmpWrittenPoint);
  }

  /** Second half of {@link #writeAtomically}: the atomic rename of the tmp file {@link #writeTmp} wrote, then the directory fsync. */
  static void renameTmp(final Path target, final String renamedPoint) throws IOException {
    renameTmpFile(target.resolveSibling(target.getFileName() + TMP_SUFFIX), target, renamedPoint);
  }

  /** {@link #renameTmp} of the given tmp file, the one {@link #writeTmpFile} wrote. */
  static void renameTmpFile(final Path tmp, final Path target, final String renamedPoint) throws IOException {
    FileUtils.publishAtomically(tmp, target, false);
    if (renamedPoint != null)
      ColumnarTestHooks.reach(renamedPoint);
    fsyncDirectory(target.getParent());
  }

  /**
   * Called before a snapshot replaces the database files: the projection of the old files must not survive. Writes
   * {@code columnar/REBUILD = "snapshot_install"} first, then deletes everything else under {@code columnar/}, so a crash in between
   * still leaves the marker.
   *
   * @throws IOException when a projection file cannot be deleted
   */
  public static void prepareForSnapshotInstall(final Path databasePath) throws IOException {
    final Path root = databasePath.resolve(ROOT_DIR);
    writeRebuildReason(databasePath, SNAPSHOT_INSTALL);
    try (final DirectoryStream<Path> entries = Files.newDirectoryStream(root)) {
      for (final Path entry : entries) {
        if (entry.getFileName().toString().equals(REBUILD_MARKER))
          continue;
        if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS))
          FileUtils.deleteRecursively(entry.toFile());
        else
          Files.deleteIfExists(entry);
        if (Files.exists(entry, LinkOption.NOFOLLOW_LINKS))
          throw new IOException("Cannot delete the columnar projection entry " + entry + " before a snapshot install");
      }
    }
    fsyncDirectory(root);
  }

  /** The reason in {@code columnar/REBUILD}, null when there is no marker. */
  public static String readRebuildReason(final Path databasePath) {
    try {
      return Files.readString(databasePath.resolve(ROOT_DIR).resolve(REBUILD_MARKER), StandardCharsets.UTF_8).trim();
    } catch (final NoSuchFileException e) {
      return null;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Writes {@code columnar/REBUILD} with the reason: every projection of the database rebuilds at the next open until it is cleared. */
  static void writeRebuildReason(final Path databasePath, final String reason) throws IOException {
    final Path root = databasePath.resolve(ROOT_DIR);
    FileUtils.createDirectoriesDurably(root);
    writeAtomically(root.resolve(REBUILD_MARKER), reason.getBytes(StandardCharsets.UTF_8));
  }

  public static void clearRebuildReason(final Path databasePath) throws IOException {
    final Path root = databasePath.resolve(ROOT_DIR);
    if (Files.deleteIfExists(root.resolve(REBUILD_MARKER)))
      fsyncDirectory(root);
  }

  /** Whether a database-relative name is the projection root or under it, so snapshot shipping and backups can skip it. */
  public static boolean isProjectionEntry(final String relativeName) {
    if (!relativeName.startsWith(ROOT_DIR))
      return false;
    if (relativeName.length() == ROOT_DIR.length())
      return true;
    final char next = relativeName.charAt(ROOT_DIR.length());
    return next == '/' || next == '\\';
  }

  /**
   * Deletes every regular file of the bucket directory that is neither referenced by the current manifest nor held by a live
   * snapshot. The manifest, its tmp file and the {@code NOWAL} marker are never touched, nor are directories. Run at a writable open
   * it removes the partial files a crash left behind, whose names (segment ids, dead versions, dictionary generations derived from
   * the manifest) the next publish reuses.
   *
   * <p>
   * Precondition: no publish is writing files into the directory concurrently, so the caller holds the projection's
   * {@code publishLock}, or includes the names of the files an in-flight flush, merge, rebuild or compaction is writing in
   * {@code heldByReaders}; otherwise a file written for the next manifest is deleted before that manifest names it.
   *
   * @return the names deleted, so the caller can call {@code SegmentFile.deleted()} for each deleted segment; a file that cannot be
   * deleted is logged, left in place and not returned (the next call retries it)
   */
  static List<String> deleteUnreferenced(final Path bucketDir, final Set<String> referenced, final Set<String> heldByReaders) {
    final List<String> deleted = new ArrayList<>();
    try (final DirectoryStream<Path> entries = Files.newDirectoryStream(bucketDir)) {
      for (final Path entry : entries) {
        final String name = entry.getFileName().toString();
        if (isNeverDeleted(name) || referenced.contains(name) || heldByReaders.contains(name) || !Files.isRegularFile(entry,
            LinkOption.NOFOLLOW_LINKS))
          continue;
        try {
          Files.delete(entry);
          deleted.add(name);
        } catch (final NoSuchFileException e) {
          // deleted meanwhile by another release: that caller reports it
        } catch (final IOException e) {
          LogManager.instance().log(ColumnarStore.class, Level.WARNING, "Cannot delete unreferenced columnar file '%s'", e, entry);
        }
      }
    } catch (final NoSuchFileException e) {
      return deleted;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return deleted;
  }

  private static boolean isNeverDeleted(final String name) {
    return name.equals(ColumnarManifest.FILE_NAME) || name.equals(NOWAL_MARKER) || name.endsWith(TMP_SUFFIX);
  }
}
