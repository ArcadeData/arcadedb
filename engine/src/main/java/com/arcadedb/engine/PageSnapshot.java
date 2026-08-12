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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.zip.CRC32;

/**
 * A point-in-time, read-only view of every page file of a database, served by a page-level copy-on-write shadow
 * (issue #6075, phase 2b of the backup roadmap in {@code docs/optimize-backup.md}).
 * <p>
 * <b>What it replaces.</b> Before this, a reader that needed the data files to stand still - the full backup, the HA
 * snapshot ship, the HA database verify - froze them with {@code PageManager.suspendFlushAndExecute}. That parks the
 * flush thread for the WHOLE operation: dirty pages pile up in RAM bounded by
 * {@link com.arcadedb.GlobalConfiguration#FLUSH_SUSPEND_MAX_DEFERRED_RAM}, past which committing threads are
 * throttled, and LSM/vector index compaction is postponed for the entire window. This class removes all of that: the
 * only stall is one bounded flush-queue drain when the window opens.
 * <p>
 * <b>The mechanism.</b> While a window is open, the first write to any page that existed at t0 first copies the
 * page's current on-disk image into the shadow ({@link PageShadow}), inside the per-page I/O slot the write already
 * holds. A reader therefore resolves each page as "shadow if present, data file otherwise", and gets exactly the t0
 * image. Because the pre-image is captured under the same slot as the write, the image and its version header are
 * read together and can never disagree - the property phase 3 (incremental backup) is built on.
 * <p>
 * <b>Reading is bulk, not page-at-a-time</b> (challenge C1). The obvious reader takes the per-page I/O slot for every
 * page, which turns one sequential {@code transferTo} into a lock acquisition per page. It is not needed: a write
 * during the window ALWAYS shadows the page before touching the file, so a run of pages can be read in one bulk
 * {@code read} with no lock at all and the shadow re-probed afterwards - any page a writer touched underneath the
 * read is now in the shadow, and its t0 image is taken from there instead. Pages already shadowed before the read are
 * skipped by the pre-probe. So a torn read is always detected, and the common case (no concurrent write to that run)
 * costs one probe per page against a primitive map.
 * <p>
 * <b>Overlapping windows</b> (challenge C3) are supported directly rather than serialized: each window owns its own
 * shadow, and a write consults every open window, capturing the same freshly read pre-image into each one that does
 * not have the page yet. That is correct because a page's content at capture time IS its content at t0 for every
 * window that has not shadowed it - if a window had been opened after an intervening write, that write would have
 * populated the older window and left this one to capture the newer image. Only the t0 barrier itself is serialized
 * per database, and only for its duration.
 * <p>
 * <b>Files dropped while a window is open</b> (challenge C2) are not deleted: {@code FileManager.dropFile} hands them
 * to the open windows, which keep them open and delete them on close. Index compaction therefore runs freely during
 * a backup instead of being postponed, which was one of the costs of the suspension.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PageSnapshot implements AutoCloseable {
  /**
   * Extension of the shadow spill file. Deliberately NOT in {@code LocalDatabase.SUPPORTED_FILE_EXT}: the shadow is
   * pure scratch, recovery never reads it, and it must never be mistaken for a data file (challenge C8).
   */
  public static final String SHADOW_FILE_EXT = "pshadow";

  public enum STATUS {
    /** Serving the t0 image. */
    ACTIVE,
    /** The shadow breached {@code arcadedb.pageSnapshotMaxSize}: the t0 image is no longer recoverable. */
    OVERFLOWED,
    /** A pre-image capture failed with an I/O error: the t0 image is no longer recoverable. */
    FAILED,
    CLOSED
  }

  /** One page file as it stood at t0: the page count is what makes appended pages need no shadow (challenge C7). */
  public record SnapshotFile(int fileId, PaginatedComponentFile file, int pageSize, int pageCount, String fileName) {
    public long size() {
      return (long) pageSize * pageCount;
    }

    /**
     * Modification timestamp for the archive entry header. Read lazily, on the consumer's thread, rather than
     * captured at t0: it is a {@code stat()} per file and the t0 barrier holds the page-manager lock and the
     * transaction manager's apply lock exclusively, so on a database with many buckets and indexes stat-ing all of
     * them there would stretch the one stall this design is supposed to keep bounded. Nothing depends on the value
     * being the t0 one, and a file dropped since then is still on disk (its deletion is deferred to the close).
     */
    public long lastModified() {
      return file.getOSFile().lastModified();
    }
  }

  /** Pages read in one bulk call. 16 x 64 KB = 1 MB, the same unit the parallel backup compressor works on. */
  private static final int READ_RUN_PAGES = 16;

  private final DatabaseInternal database;
  private final PageManager      pageManager;
  private final long             lastTxId;
  private final SnapshotFile[]   filesById;
  private final List<SnapshotFile> files;
  private final PageShadow       shadow;
  private final long             openedOn = System.currentTimeMillis();

  /**
   * Capture operations currently touching the shadow. {@link #close()} unpublishes the window first and then waits
   * for this to reach zero, so the shadow is never released from under a writer that had already read the published
   * array - the same refcount shape {@code PageManagerFlushThread.setSuspended} uses for the suspension (#5068).
   */
  private final AtomicInteger inFlightCaptures = new AtomicInteger();
  /** Files dropped while this window was open: kept alive for the reader, physically deleted on close (C2). */
  private final ConcurrentLinkedQueue<ComponentFile> retiredFiles = new ConcurrentLinkedQueue<>();

  private volatile STATUS status = STATUS.ACTIVE;
  private volatile String invalidReason;
  private volatile boolean released = false;

  PageSnapshot(final DatabaseInternal database, final PageManager pageManager, final long lastTxId,
      final List<SnapshotFile> files, final PageShadow shadow) {
    this.database = database;
    this.pageManager = pageManager;
    this.lastTxId = lastTxId;
    this.files = Collections.unmodifiableList(files);
    this.shadow = shadow;

    int maxFileId = -1;
    for (final SnapshotFile f : files)
      maxFileId = Math.max(maxFileId, f.fileId());
    this.filesById = new SnapshotFile[maxFileId + 1];
    for (final SnapshotFile f : files)
      this.filesById[f.fileId()] = f;
  }

  // --------------------------------------------------------------------------------------------- PUBLIC READER API

  /** The page files as they stood at t0, in file-id order. Files created after t0 are absent by construction. */
  public List<SnapshotFile> getFiles() {
    return files;
  }

  public SnapshotFile getFile(final int fileId) {
    return fileId >= 0 && fileId < filesById.length ? filesById[fileId] : null;
  }

  /**
   * The last committed transaction id at t0. Because the window opens on a fully drained flush queue, every
   * transaction up to this id is materialised in the pages this snapshot serves - which is what closes the recency
   * gap the suspend-and-freeze path had (section 1.3 of the design document): there, only the in-flight flush batch
   * was awaited, so a restored backup could be a few hundred transactions behind.
   */
  public long getLastTxId() {
    return lastTxId;
  }

  public Database getDatabase() {
    return database;
  }

  public STATUS getStatus() {
    return status;
  }

  /** Number of pages whose pre-image the window has had to capture so far. */
  public int getShadowedPages() {
    return shadow.getPageCount();
  }

  /** RAM plus spill bytes the shadow currently holds: the figure {@code arcadedb.pageSnapshotMaxSize} caps. */
  public long getShadowSizeInBytes() {
    return shadow.getSizeInBytes();
  }

  public long getShadowSpilledBytes() {
    return shadow.getSpilledBytes();
  }

  public long getOpenedOn() {
    return openedOn;
  }

  /**
   * Throws when the window can no longer serve the t0 image, so a consumer never silently produces a torn artifact.
   * Consumers are expected to catch {@link PageSnapshotException} and fall back to the suspend-and-freeze path.
   */
  public void checkValid() {
    final STATUS current = status;
    if (current != STATUS.ACTIVE)
      throw new PageSnapshotException(
          "Snapshot of database '" + database.getName() + "' is " + current + (invalidReason != null ?
              ": " + invalidReason :
              ""));
  }

  /**
   * Streams the whole t0 content of one file, exactly {@code pageCount * pageSize} bytes. The stream reads runs of
   * pages in bulk (see the class comment): sequential I/O and OS readahead are preserved, and no per-page lock is
   * taken.
   */
  public InputStream newInputStream(final int fileId) {
    final SnapshotFile snapshotFile = getFile(fileId);
    if (snapshotFile == null)
      throw new PageSnapshotException(
          "File with id " + fileId + " is not part of the snapshot of database '" + database.getName() + "'");
    return new SnapshotFileInputStream(snapshotFile);
  }

  /**
   * CRC32 of the whole t0 content of a file. Byte-for-byte the same value
   * {@link PaginatedComponentFile#calculateChecksum()} computes on a frozen file, so an HA verify can compare a
   * snapshot-based checksum against a peer's regardless of which path that peer used.
   */
  public long calculateChecksum(final int fileId) throws IOException {
    final CRC32 crc = new CRC32();
    final byte[] buffer = new byte[64 * 1024];
    try (final InputStream in = newInputStream(fileId)) {
      for (int read = in.read(buffer); read > 0; read = in.read(buffer))
        crc.update(buffer, 0, read);
    }
    return crc.getValue();
  }

  @Override
  public void close() {
    if (released)
      return;

    // ORDER MATTERS: UNPUBLISH FIRST SO NO NEW CAPTURE CAN FIND THIS WINDOW, THEN DRAIN THE ONES ALREADY INSIDE
    pageManager.unregisterSnapshot(this);
    released = true;
    // A CAPTURE IN FLIGHT IS ONE PAGE READ PLUS ONE COPY, SO SPINNING IS THE RIGHT WAIT - BUT IT YIELDS AFTER A WHILE
    // SO A CAPTURE STUCK ON A SLOW DISK DOES NOT BURN A CORE
    for (int spins = 0; inFlightCaptures.get() > 0; spins++) {
      if (spins < 1_000)
        Thread.onSpinWait();
      else
        Thread.yield();
    }

    status = STATUS.CLOSED;
    shadow.close();

    // CHALLENGE C2: THE FILES DROPPED WHILE THE WINDOW WAS OPEN WERE KEPT ALIVE FOR THE READER. RELEASE THIS
    // WINDOW'S CLAIM; THE LAST WINDOW STILL HOLDING ONE PERFORMS THE PHYSICAL DELETE
    for (ComponentFile retired = retiredFiles.poll(); retired != null; retired = retiredFiles.poll())
      pageManager.releaseDeferredFileDrop(retired);
  }

  // ---------------------------------------------------------------------------------------------- ENGINE INTERNALS

  /**
   * A page carries whichever {@code Database} instance created it, which on a server is not necessarily the same
   * object the consumer opened the window with (wrappers, proxies). {@code LocalDatabase.equals} keys on the
   * database path, which is the identity that matters here - and the same comparison the flush thread already uses
   * to match batches to their database.
   */
  boolean isFor(final Object candidateDatabase) {
    return database.equals(candidateDatabase);
  }

  /**
   * Called from inside {@code PageManager.concurrentPageAccess}'s write slot, BEFORE the page reaches the file.
   *
   * @return {@code true} when this window still needs the page's pre-image.
   */
  boolean needsPreImage(final int fileId, final int pageNumber) {
    if (status != STATUS.ACTIVE)
      return false;
    final SnapshotFile snapshotFile = getFile(fileId);
    // BEYOND THE t0 PAGE COUNT MEANS THE PAGE DID NOT EXIST AT t0: THE READER IGNORES IT, SO IT NEEDS NO SHADOW (C7)
    if (snapshotFile == null || pageNumber >= snapshotFile.pageCount())
      return false;
    return !shadow.contains(PageShadow.key(fileId, pageNumber));
  }

  int getPageSize(final int fileId) {
    final SnapshotFile snapshotFile = getFile(fileId);
    return snapshotFile != null ? snapshotFile.pageSize() : -1;
  }

  /**
   * Stores a pre-image captured by the write path. Never propagates a failure to the writer: a snapshot must never
   * be able to break the live database, so a breach of the cap or an I/O error invalidates THIS window and lets the
   * write proceed. Every later read fails loudly with {@link PageSnapshotException}.
   */
  void storePreImage(final int fileId, final int pageNumber, final byte[] content, final int length) {
    inFlightCaptures.incrementAndGet();
    try {
      if (released || status != STATUS.ACTIVE)
        return;

      if (!shadow.store(PageShadow.key(fileId, pageNumber), content, length))
        invalidate(STATUS.OVERFLOWED,
            "the copy-on-write shadow reached the " + GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.getKey() + " cap of "
                + database.getConfiguration().getValueAsLong(GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE) + " MB");
    } catch (final IOException e) {
      invalidate(STATUS.FAILED, "error capturing the pre-image of page " + fileId + "/" + pageNumber + ": " + e);
    } finally {
      inFlightCaptures.decrementAndGet();
    }
  }

  /**
   * Takes over a file dropped while this window is open, so the reader keeps seeing it (challenge C2). Called by
   * {@code PageManager.deferFileDrop} while it holds the snapshot registry lock, which {@link #close()} also takes
   * (through {@code unregisterSnapshot}) BEFORE draining this queue - so a file handed over here is never missed.
   */
  void retainDroppedFile(final ComponentFile file) {
    retiredFiles.add(file);
  }

  /** A pre-image could not be read from the data file: this window can no longer serve the t0 image. */
  void invalidateOnCaptureError(final int fileId, final int pageNumber, final IOException error) {
    invalidate(STATUS.FAILED, "error reading the pre-image of page " + fileId + "/" + pageNumber + ": " + error);
  }

  private synchronized void invalidate(final STATUS newStatus, final String reason) {
    if (status == STATUS.ACTIVE) {
      status = newStatus;
      invalidReason = reason;
      LogManager.instance().log(this, Level.WARNING,
          "Snapshot of database '%s' is no longer usable (%s): %s. Readers will fail and can fall back to suspending the page flush",
          null, database.getName(), newStatus, reason);
    }
  }

  // ---------------------------------------------------------------------------------------------- READER

  /**
   * Reads the t0 content of one file as a stream. Pages are pulled a run at a time: the shadow is probed for every
   * page of the run, the pages that are not in it are read from the data file in ONE call, then the shadow is
   * re-probed for those same pages. A page that appears in the second probe was written underneath the bulk read, so
   * its (possibly torn) bytes are replaced by the pre-image the writer captured - which is the t0 image by
   * construction. See the class comment for why this is exactly as safe as taking the per-page I/O slot.
   */
  private final class SnapshotFileInputStream extends InputStream {
    private final SnapshotFile snapshotFile;
    private final byte[]       buffer;
    private final ByteBuffer   byteBuffer;

    private int nextPage    = 0;
    private int bufferLimit = 0;
    private int bufferPos   = 0;

    private SnapshotFileInputStream(final SnapshotFile snapshotFile) {
      this.snapshotFile = snapshotFile;
      this.buffer = new byte[READ_RUN_PAGES * snapshotFile.pageSize()];
      this.byteBuffer = ByteBuffer.wrap(buffer);
    }

    @Override
    public int read() throws IOException {
      if (bufferPos == bufferLimit && !fill())
        return -1;
      return buffer[bufferPos++] & 0xFF;
    }

    @Override
    public int read(final byte[] dst, final int offset, final int length) throws IOException {
      if (length == 0)
        return 0;
      if (bufferPos == bufferLimit && !fill())
        return -1;
      final int copied = Math.min(length, bufferLimit - bufferPos);
      System.arraycopy(buffer, bufferPos, dst, offset, copied);
      bufferPos += copied;
      return copied;
    }

    @Override
    public int available() {
      return bufferLimit - bufferPos;
    }

    private boolean fill() throws IOException {
      checkValid();

      if (nextPage >= snapshotFile.pageCount())
        return false;

      final int pageSize = snapshotFile.pageSize();
      final int runPages = Math.min(READ_RUN_PAGES, snapshotFile.pageCount() - nextPage);
      final int fileId = snapshotFile.fileId();

      // 1. PRE-PROBE: PAGES ALREADY SHADOWED ARE SERVED FROM THE SHADOW, AND THE BULK READ ONLY HAS TO SPAN THE
      //    OUTERMOST PAGES THAT ARE NOT
      int firstUnshadowed = -1;
      int lastUnshadowed = -1;
      for (int i = 0; i < runPages; i++)
        if (!shadow.read(PageShadow.key(fileId, nextPage + i), buffer, i * pageSize, pageSize)) {
          if (firstUnshadowed < 0)
            firstUnshadowed = i;
          lastUnshadowed = i;
        }

      // 2. ONE BULK READ COVERING EVERY PAGE OF THE RUN NOT ALREADY RESOLVED. THE RANGE IS CONTIGUOUS EVEN WHEN IT
      //    SPANS A PAGE THAT WAS ALREADY SHADOWED: RE-READING THAT PAGE IS CHEAPER THAN SPLITTING THE I/O, AND ITS
      //    LIVE BYTES ARE OVERWRITTEN FROM THE SHADOW AGAIN IN STEP 3 - WHICH IS WHY STEP 3 MUST RE-PROBE EVERY
      //    PAGE OF THE RUN, NOT ONLY THE ONES THAT WERE UNSHADOWED AT STEP 1
      if (firstUnshadowed >= 0) {
        final int readPages = lastUnshadowed - firstUnshadowed + 1;
        byteBuffer.clear();
        byteBuffer.limit((lastUnshadowed + 1) * pageSize);
        byteBuffer.position(firstUnshadowed * pageSize);
        try {
          snapshotFile.file().readPages(nextPage + firstUnshadowed, readPages, byteBuffer);
        } catch (final IllegalArgumentException e) {
          // readPages REPORTS A CLOSED FILE THIS WAY. THE WINDOW KEEPS ITS FILES ALIVE (DROPS ARE DEFERRED), SO THIS
          // MEANS THE DATABASE ITSELF WENT AWAY UNDERNEATH THE READER - A SNAPSHOT-INTEGRITY FAILURE, NOT A DISK
          // ERROR, SO IT IS REPORTED AS ONE AND THE CONSUMER CAN FALL BACK INSTEAD OF FAILING OUTRIGHT
          invalidate(STATUS.FAILED, "file '" + snapshotFile.fileName() + "' is no longer readable: " + e.getMessage());
          checkValid();
        }
        byteBuffer.clear();
      }

      // 3. RE-PROBE EVERY PAGE OF THE RUN, UNCONDITIONALLY. TWO DISTINCT CASES NEED IT, AND GATING ON
      //    !fromShadow[i] SILENTLY BROKE THE FIRST ONE:
      //      a) A PAGE ALREADY SHADOWED AT STEP 1 THAT SITS INSIDE THE BULK-READ RANGE (ITS NEIGHBOURS WERE NOT
      //         SHADOWED, SO THE RANGE SPANS IT). STEP 2 OVERWROTE ITS PRE-IMAGE WITH CURRENT, POST-t0 BYTES.
      //         THIS IS THE COMMON CASE, NOT AN EXOTIC ONE: PAGES ARE SHADOWED IN WRITE ORDER, WHICH HAS NOTHING
      //         TO DO WITH READ_RUN_PAGES BOUNDARIES.
      //      b) A PAGE WRITTEN UNDERNEATH THE BULK READ, WHOSE BYTES MAY THEREFORE BE TORN.
      //    A PAGE THAT IS IN NEITHER CASE IS NOT IN THE SHADOW AT ALL, SO THE PROBE IS A NO-OP ON IT
      for (int i = 0; i < runPages; i++)
        shadow.read(PageShadow.key(fileId, nextPage + i), buffer, i * pageSize, pageSize);

      checkValid();

      nextPage += runPages;
      bufferPos = 0;
      bufferLimit = runPages * pageSize;
      return true;
    }
  }
}
