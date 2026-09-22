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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockContext;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class WALFile extends LockContext {
  public enum FlushType {
    NO, YES_NOMETADATA, YES_FULL
  }

  // TXID (long) + TIMESTAMP (long) + PAGES (int) + SEGMENT_SIZE (int)
  private static final int TX_HEADER_SIZE = Binary.LONG_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;
  // SEGMENT_SIZE (int) + MAGIC_NUMBER (long)
  private static final int TX_FOOTER_SIZE = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

  // FILE_ID (int) + PAGE_NUMBER (int) + DELTA_FROM (int) + DELTA_TO (int) + CURR_PAGE_VERSION (int)+ CURR_PAGE_SIZE (int)
  private static final int PAGE_HEADER_SIZE =
      Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE
          + Binary.INT_SERIALIZED_SIZE;

  public static final long MAGIC_NUMBER = 9371515385058702L;

  // #7768: not final any more - a thread interrupt closes the channel underneath this instance and
  // reopenChannel() replaces the trio. volatile because getSize() and the recovery readers run outside
  // the monitor that acquire()/close() hold.
  private volatile RandomAccessFile file;
  private final    String           filePath;
  private volatile FileChannel      channel;
  private volatile FileLock         lock;
  private volatile boolean          active            = true;
  private volatile boolean          open;
  private final    AtomicInteger    pagesToFlush      = new AtomicInteger();
  private          long             statsPagesWritten = 0;
  private          long             statsBytesWritten = 0;

  public static class WALTransaction {
    public long      txId;
    public long      timestamp;
    public WALPage[] pages;
    public long      startPositionInLog;
    public long      endPositionInLog;
    // When true, the version-gap check in applyChanges() is bypassed (used for compaction page replication)
    public boolean   forceApply = false;
  }

  public static class WALPage {
    public int    fileId;
    public int    pageNumber;
    public int    changesFrom;
    public int    changesTo;
    public Binary currentContent;
    public int    currentPageVersion;
    public int    currentPageSize;

    @Override
    public String toString() {
      return "WALPage(fileId=" + fileId + " pageNumber=" + pageNumber + ")";
    }
  }

  public WALFile(final String filePath) throws FileNotFoundException {
    this.filePath = filePath;
    this.file = new RandomAccessFile(filePath, "rw");
    this.channel = file.getChannel();
    this.open = true;
    this.lock = acquireLock();
  }

  /**
   * Exclusive advisory lock on this file for the whole life of this instance (issue #7479). Without it, a
   * second process/instance's directory-wide WAL cleanup on close() had no way to tell this file apart from
   * a genuine orphan an earlier unclean shutdown left behind, and deleted it out from under this one - the
   * reported corruption. Best-effort: a lock that cannot be acquired (a same-named file collision, or a
   * filesystem whose advisory locking does not reach across process/container boundaries, e.g. some Docker
   * Desktop bind mounts) leaves this file exactly as unprotected as it always was, never worse.
   * <p>
   * The guarantee this gives {@code TransactionManager}'s orphan sweep is scoped to a SEPARATE OS process
   * holding the file open, which is the reported scenario and the only one {@code LocalDatabase}'s own
   * per-database lock does not already rule out (two {@code LocalDatabase} instances on the same path
   * within one JVM/process are refused at open time). It is not a guarantee against a second, same-process
   * {@code WALFile} on this exact path: {@code FileChannel}'s own javadoc warns that closing any channel
   * releases every lock the JVM holds on the underlying file, "regardless of whether the locks were
   * acquired via that channel or via another channel open on the same file" - the POSIX advisory-lock
   * model this wraps is scoped to a (process, inode) pair, not a file descriptor, so it cannot do better.
   */
  private FileLock acquireLock() {
    try {
      return channel.tryLock();
    } catch (final IOException | OverlappingFileLockException e) {
      LogManager.instance()
          .log(this, Level.WARNING, "Unable to lock WAL file '%s'; another process may be able to remove it", e, filePath);
      return null;
    }
  }

  /**
   * Whether THIS instance's own {@link #acquireLock()} actually got the lock (issue #7479) - i.e. nobody
   * else had the file open at that moment - not whether the file is locked in general. Lets a caller that
   * opened this file only to probe whether anyone else has it open - {@code TransactionManager}'s orphan
   * sweep and construction-site guards - tell that apart from "someone was already there" without having
   * to reach into the lock field itself.
   */
  boolean acquiredLock() {
    return lock != null;
  }

  /**
   * Reopens the channel after a {@link ClosedChannelException} closed it by accident (issue #7768). This is
   * the WAL-side twin of {@code PaginatedComponentFile.reopenChannelUnderWriteLock()} (issue #4930): a thread
   * interrupted anywhere inside a commit makes NIO close the channel from under this instance -
   * {@code AbstractInterruptibleChannel.begin()} fires the interruptor even when the flag is ALREADY set, so
   * no timing race is needed - and before this fix nothing ever repaired the descriptor. The
   * {@code WALFile} stayed in {@code TransactionManager}'s active pool with {@code open == true} and a dead
   * channel, so every later transaction that hashed to that slot stalled for the whole WAL write timeout and
   * then failed. The bytes already in the log are untouched by this: only the descriptor died.
   * <p>
   * Only an ACCIDENTAL close is repaired, for the same two reasons #4930 lists. A file closed on purpose
   * ({@link #close()}/{@link #drop()} set {@code open = false}) must stay closed, and a path that no longer
   * exists on disk must not be re-created - {@code RandomAccessFile(path, "rw")} would otherwise resurrect a
   * WAL file a concurrent instance's cleanup deleted, which is exactly the file {@code checkWALFiles()} has
   * to fence the database over (issue #7479).
   * <p>
   * Synchronized, and double-checked inside, so concurrent callers reopen the channel once instead of leaking
   * a descriptor each. {@link #close()} and {@link #acquire(Callable)} hold the same monitor, so a reopen can
   * never interleave with a deliberate close.
   */
  private synchronized void reopenChannel() throws IOException {
    if (!open)
      throw new ClosedChannelException();

    if (channel != null && channel.isOpen())
      // ANOTHER THREAD ALREADY REOPENED IT
      return;

    if (!new File(filePath).exists())
      throw new FileNotFoundException(
          "WAL file '" + filePath + "' no longer exists on disk, refusing to re-create it after a ClosedChannelException");

    if (file != null)
      try {
        file.close();
      } catch (final IOException e) {
        // Not fatal: the descriptor is already dead and this only releases what is left of it. Logged
        // rather than dropped so a handle that genuinely refuses to close is still traceable.
        LogManager.instance().log(this, Level.FINE, "Error on closing the dead handle of WAL file '%s'", e, filePath);
      }

    this.file = new RandomAccessFile(filePath, "rw");
    this.channel = file.getChannel();
    // Closing a channel releases every lock this JVM holds on the underlying file, so the #7479 advisory
    // lock died with it and has to be taken again. Best-effort exactly as at construction time.
    this.lock = acquireLock();
  }

  /**
   * Re-runs an I/O operation after {@link #reopenChannel()} has replaced a channel a thread interrupt closed
   * (issue #7768), clearing and then restoring the interrupt flag around it. The clear is mandatory:
   * {@code ClosedByInterruptException} leaves the flag set, and the very next interruptible operation on the
   * fresh channel would close that one too. The restore is mandatory as well, so the cancellation stays
   * observable to the caller that asked for it.
   * <p>
   * {@code cause} is the close that triggered the recovery. It is logged with the diagnostic, and attached
   * as a suppressed exception if the retry fails too, so a refused reopen never hides what closed the
   * channel in the first place.
   */
  private <T> T retryAfterReopen(final ClosedChannelException cause, final String operation, final ChannelOperation<T> io)
      throws IOException {
    LogManager.instance().log(this, Level.SEVERE,
        "WAL file '%s' was closed on %s (interrupted thread?). Reopen it and retry...", cause, filePath, operation);

    final boolean wasInterrupted = Thread.interrupted();
    try {
      reopenChannel();
      return io.run();
    } catch (final IOException retryFailed) {
      // Keep the close that started all this attached to whatever the recovery ran into - a refused reopen
      // reports why it refused, and losing the original leaves no trace of what closed the channel.
      retryFailed.addSuppressed(cause);
      throw retryFailed;
    } finally {
      if (wasInterrupted)
        Thread.currentThread().interrupt();
    }
  }

  @FunctionalInterface
  private interface ChannelOperation<T> {
    T run() throws IOException;
  }

  /**
   * Single funnel for every positional read in this class, so a channel an interrupt closed is reopened and
   * the read retried on all of them (issue #7768) rather than only on the commit path.
   */
  private int readChunk(final ByteBuffer buffer, final long pos) throws IOException {
    // Recorded before the attempt for the same reason append() records its start offset: an interrupted
    // read may already have transferred bytes into the buffer before throwing, and the retry reads from
    // the SAME file offset - so without rewinding the buffer to where this attempt began, those bytes
    // would be written twice and the caller's readPos/position bookkeeping would drift apart.
    final int positionBeforeAttempt = buffer.position();
    try {
      return channel.read(buffer, pos);
    } catch (final ClosedChannelException e) {
      return retryAfterReopen(e, "read", () -> {
        buffer.position(positionBeforeAttempt);
        return channel.read(buffer, pos);
      });
    }
  }

  /**
   * {@code fsync} of this WAL file, with the same reopen-and-retry recovery as every other channel operation
   * here (issue #7768).
   */
  void force(final boolean metaData) throws IOException {
    try {
      channel.force(metaData);
    } catch (final ClosedChannelException e) {
      retryAfterReopen(e, "force", () -> {
        channel.force(metaData);
        return null;
      });
    }
  }

  public synchronized void close() throws IOException {
    this.open = false;
    if (lock != null)
      try {
        lock.release();
      } catch (final IOException e) {
        // IGNORE IT: the channel close right below releases it regardless
      }

    if (channel != null)
      channel.close();

    if (file != null)
      file.close();
  }

  public boolean isOpen() {
    return open;
  }

  public synchronized void drop() throws IOException {
    close();
    FileUtils.deleteFile(new File(filePath));
  }

  /** What {@link #deleteIfNotHeldByAnotherInstance(File)} did with the file it was handed. */
  public enum SweepOutcome {
    /** The file is gone: it was deleted, or it was already gone when the sweep reached it. */
    DELETED,
    /** Somebody else has the file open, so it was left alone. */
    SKIPPED_LOCKED,
    /** The file should have been deleted and could not be. */
    ERROR
  }

  /**
   * Deletes {@code walFile} only if an exclusive lock on it can be acquired first (issue #7479). A file no live
   * instance tracks is either a genuine orphan from an earlier unclean shutdown of the same database - nothing
   * holds it open, the lock succeeds instantly, and it is deleted - or a WAL file another live instance still has
   * open (every {@link WALFile} has held an exclusive lock on itself for its whole life since that same issue), in
   * which case the lock fails and the file is left untouched instead of being deleted out from under that
   * instance.
   * <p>
   * The probe is a {@link WALFile} itself, not a raw handle kept open across the delete: on Windows a process
   * cannot delete a file through which it still holds an open, non-share-delete handle - even its own - so the
   * handle used to prove nobody else has the file open must be closed (releasing the lock as a side effect) BEFORE
   * {@code delete()} is attempted, exactly as {@link #drop()} already does for the ordinary case.
   * <p>
   * Accepted trade-off (raised in review): closing the probe before deleting reopens a THIRD instance's window to
   * acquire the lock and start using the file in between - the opposite choice from holding the lock through the
   * delete, which an earlier revision did, until that was found to make the delete itself fail silently on
   * Windows (see above). Between "closeable by Windows, briefly racy against a third instance" and "safe against a
   * third instance, broken on Windows", this keeps the former: the scenario this whole method exists for is
   * already "more than one instance should not share this directory", so a THIRD one racing into the exact same
   * window is a corner of that corner.
   * <p>
   * Lives HERE rather than on {@code TransactionManager}, where it was written, because a database directory has
   * more than one {@code *.wal} sweep over it and the second one had no protection at all: HA's
   * {@code SnapshotInstaller.cleanupWalFiles} deleted every {@code *.wal} in the directory by name after a
   * snapshot swap, which on a shared directory reproduces the exact corruption #7479 reported. One implementation,
   * next to the lock it depends on, so a third sweep cannot be written without it (issue #7505).
   *
   * @param walFile the {@code *.wal} file to remove
   *
   * @return what happened to it; never {@code null}
   */
  public static SweepOutcome deleteIfNotHeldByAnotherInstance(final File walFile) {
    if (!walFile.exists())
      // Already gone - its owner's own clean shutdown, or a concurrent sweep, beat us to it. Opening it
      // below would otherwise recreate it as an empty file just to delete it again.
      return SweepOutcome.DELETED;

    try {
      final boolean nobodyElseHasItOpen;
      final WALFile probe = new WALFile(walFile.getPath());
      try {
        nobodyElseHasItOpen = probe.acquiredLock();
      } finally {
        probe.close();
      }

      if (!nobodyElseHasItOpen) {
        // Someone else already has it open - either a live peer instance still using it, or (rarer) another
        // closing instance's own sweep racing this one over the same ownerless orphan. Either way it is not
        // this sweep's to remove: the file survives, and whoever does hold it will remove it if it turns
        // out to be an orphan after all.
        LogManager.instance().log(WALFile.class, Level.WARNING,
            "Skipped removing WAL file '%s': it is still open, either by a live database instance or a competing cleanup",
            null, walFile);
        return SweepOutcome.SKIPPED_LOCKED;
      }

      if (!walFile.delete()) {
        LogManager.instance().log(WALFile.class, Level.WARNING, "Error on removing WAL file '%s'", null, walFile);
        return SweepOutcome.ERROR;
      }
      return SweepOutcome.DELETED;
    } catch (final IOException e) {
      LogManager.instance().log(WALFile.class, Level.WARNING, "Error on removing WAL file '%s'", e, walFile);
      return SweepOutcome.ERROR;
    }
  }

  public WALTransaction getFirstTransaction() throws WALException {
    return getTransaction(0);
  }

  public int getPendingPagesToFlush() {
    return pagesToFlush.get();
  }

  /**
   * If the WAL is still active, execute the callback. This avoids closing a file where a thread is still writing to it.
   *
   * @return true if acquired, otherwise false
   */
  public synchronized boolean acquire(final Callable<Object> callable) {
    if (!active || !open)
      return false;

    try {
      callable.call();
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new WALException("Error on writing to WAL file " + filePath, e);
    }

    return true;
  }

  public synchronized void setActive(final boolean active) {
    this.active = active;
  }

  /**
   * Whether this file is still the one its pool slot writes to, as opposed to having been rotated out and
   * left waiting for its pending pages to flush. Used by the WAL-pool diagnostic in
   * {@code TransactionManager.writeTransactionToWAL} (#7768). Package-private, like {@link #acquiredLock()}
   * above it: nothing outside {@code com.arcadedb.engine} has any use for it.
   */
  boolean isActive() {
    return active;
  }

  public WALTransaction getTransaction(long pos) {
    final WALTransaction tx = new WALTransaction();

    tx.startPositionInLog = pos;

    try {
      if (pos + TX_HEADER_SIZE + TX_FOOTER_SIZE > getSize())
        // TRUNCATED FILE
        return null;

      tx.txId = readLong(pos);
      pos += Binary.LONG_SERIALIZED_SIZE;

      tx.timestamp = readLong(pos);
      pos += Binary.LONG_SERIALIZED_SIZE;

      final int pages = readInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final int segmentSize = readInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      if (segmentSize < 0)
        // CORRUPT HEADER: a negative size makes the truncation bound below pass trivially, and would
        // let a record no writer could have produced parse cleanly. INVALID.
        return null;

      if (pos + segmentSize + Binary.LONG_SERIALIZED_SIZE > getSize())
        // TRUNCATED FILE
        return null;

      // #6932: `pages` comes straight off disk and sizes this array before a byte of the segment is read.
      // The OutOfMemoryError it can raise is an Error, so neither the catch(Exception) below nor
      // TransactionManager.checkIntegrity (try/finally, no catch) can contain it: it unwinds out of
      // LocalDatabase.open before the WAL is dropped or renamed '.corrupt', and every later open reads
      // the same bytes. The bound is exact, not a heuristic: writeTransactionToBuffer charges every
      // segment PAGE_HEADER_SIZE + deltaSize and rejects deltaSize < 1. Same guard as #4420 (Raft entry
      // parser) and #4958 (per-page deltaSize).
      if (pages < 0 || (long) pages * (PAGE_HEADER_SIZE + 1) > segmentSize)
        // INVALID
        return null;

      tx.pages = new WALPage[pages];

      for (int i = 0; i < pages; ++i) {
        if (pos > getSize())
          // INVALID
          return null;

        tx.pages[i] = new WALPage();

        tx.pages[i].fileId = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].pageNumber = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].changesFrom = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].changesTo = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        final int deltaSize = tx.pages[i].changesTo - tx.pages[i].changesFrom + 1;

        tx.pages[i].currentPageVersion = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].currentPageSize = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        // Reject obviously corrupted page headers so ByteBuffer.allocate cannot blow up with a
        // negative size and a garbage delta cannot be applied to disk. #4958: the delta must also
        // fit in the remaining file - the outer segment-size check bounds the SEGMENT by file size,
        // but a corrupt per-page header can still declare a huge deltaSize (up to Integer.MAX_VALUE)
        // and trigger a transient ~2GB allocation, or an OutOfMemoryError that escapes the
        // catch(Exception) recovery guard below, before the read would fail.
        if (deltaSize <= 0 || tx.pages[i].changesFrom < 0 || pos + deltaSize > getSize())
          return null;

        final ByteBuffer buffer = ByteBuffer.allocate(deltaSize);

        tx.pages[i].currentContent = new Binary(buffer);

        long readPos = pos;
        while (buffer.hasRemaining()) {
          final int n = readChunk(buffer, readPos);
          if (n == -1)
            return null; // truncated WAL: EOF before delta is complete
          readPos += n;
        }

        pos += deltaSize;
      }

      final long mn = readLong(pos + Binary.INT_SERIALIZED_SIZE);
      if (mn != MAGIC_NUMBER)
        // INVALID
        return null;

      tx.endPositionInLog = pos + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

      return tx;
    } catch (final EOFException e) {
      // TRUNCATED FILE: same benign outcome as the explicit size checks above.
      return null;
    } catch (final IOException e) {
      throw new WALException("Error reading WAL file " + filePath, e);
    } catch (final Exception e) {
      return null;
    }
  }

  /**
   * Scans forward from {@code fromPos} for the start of the next valid transaction record. Used during
   * recovery to tell a benign torn record at the physical end of the file (the last write interrupted by
   * a crash) apart from a corrupt record in the middle of the WAL that is followed by intact transactions.
   * In the latter case stopping silently would drop committed data (issue #4508).
   * <p>
   * Every transaction ends with an 8-byte {@link #MAGIC_NUMBER} footer, so this scans for that pattern and,
   * for each occurrence, derives the candidate transaction start and validates it with {@link #getTransaction(long)}.
   * The validation rejects a {@link #MAGIC_NUMBER} that appears by coincidence inside a page payload because the
   * derived transaction must parse cleanly and its own footer must land exactly on the matched magic number.
   *
   * @param fromPos position where transaction parsing failed
   *
   * @return start position of the next valid transaction, or -1 if none is found before EOF
   */
  public long findNextValidTransactionPosition(final long fromPos) {
    try {
      final long size = getSize();
      if (fromPos < 0 || fromPos + TX_HEADER_SIZE + TX_FOOTER_SIZE > size)
        return -1;

      final int CHUNK_SIZE = 1 << 16; // 64K
      final ByteBuffer chunk = ByteBuffer.allocate(CHUNK_SIZE);

      // Begin one byte past the failed record so its broken content is skipped.
      long scanPos = fromPos + 1;
      while (scanPos + Binary.LONG_SERIALIZED_SIZE <= size) {
        chunk.clear();
        final int toRead = (int) Math.min(CHUNK_SIZE, size - scanPos);
        chunk.limit(toRead);
        int read = 0;
        while (read < toRead) {
          final int n = readChunk(chunk, scanPos + read);
          if (n == -1)
            break;
          read += n;
        }
        if (read < Binary.LONG_SERIALIZED_SIZE)
          break;

        for (int i = 0; i + Binary.LONG_SERIALIZED_SIZE <= read; i++) {
          if (chunk.getLong(i) != MAGIC_NUMBER)
            continue;

          final long magicPos = scanPos + i;
          // TX_FOOTER = SEGMENT_SIZE(int) + MAGIC_NUMBER(long): the matched magic is preceded by the segment size.
          final long footerSegmentSizePos = magicPos - Binary.INT_SERIALIZED_SIZE;
          if (footerSegmentSizePos < fromPos)
            continue;

          final int segmentSize = readInt(footerSegmentSizePos);
          if (segmentSize < 0)
            continue;

          final long txStart = footerSegmentSizePos - (long) segmentSize - TX_HEADER_SIZE;
          if (txStart < fromPos)
            continue;

          final WALTransaction tx = getTransaction(txStart);
          if (tx != null && tx.endPositionInLog == magicPos + Binary.LONG_SERIALIZED_SIZE)
            return txStart;
        }

        // Overlap by 7 bytes so a magic number straddling a chunk boundary is not missed.
        scanPos += read - (Binary.LONG_SERIALIZED_SIZE - 1);
      }
    } catch (final IOException e) {
      // Treat an I/O error during the scan as "no valid transaction found": recovery stops at fromPos.
    }
    return -1;
  }

  /**
   * Serializes the changes of a transaction into the binary format shared by the WAL file and by the Raft entry that
   * ships the transaction to the replicas.
   * <p>
   * A page contributes ONE SEGMENT PER DISJOINT MODIFIED INTERVAL, not one segment covering the hull of everything it
   * touched (issue #5470). Page layouts write at both ends - an LSM index page appends its key/value content
   * downwards from the tail while inserting the pointer into the sorted array that grows up from the header - so the
   * hull of a page that received a handful of keys is the whole page. Charging the WAL a full 256KB index page (or a
   * 64KB bucket page) per touched page made the size of a transaction a function of how many pages it touched
   * rather than of how much data it wrote: on a replicated database a bulk load of small records produced tens of MB
   * per Raft entry and hit the maximum entry size no matter how small the batch was made.
   * <p>
   * The reader is unchanged: segments are applied in order, and several segments of the same page simply carry the
   * same target version - {@code TransactionManager.applyChanges} groups them and applies them to one page image.
   */
  public static Binary writeTransactionToBuffer(final List<MutablePage> pages, final long txId) {
    // COMPUTE TOTAL TXLOG SEGMENT SIZE
    int segmentSize = 0;
    int segments = 0;
    for (final MutablePage newPage : pages) {
      final int[] ranges = newPage.getModifiedRanges();
      final int rangeCount = newPage.getModifiedRangeCount();
      segments += rangeCount;

      for (int r = 0; r < rangeCount; ++r) {
        final int deltaSize = ranges[r * 2 + 1] - ranges[r * 2] + 1;

        final long totalSizeCheck = 0L + TX_HEADER_SIZE + TX_FOOTER_SIZE + segmentSize + PAGE_HEADER_SIZE + deltaSize; // USE A LONG TO CHECK THE BOUNDARIES
        if (totalSizeCheck > Integer.MAX_VALUE)
          throw new TransactionException("Transaction buffer bigger than " + FileUtils.getSizeAsString(Integer.MAX_VALUE)
              + ". Split the big transaction in smaller transactions. This transaction will be roll backed");

        segmentSize += PAGE_HEADER_SIZE + deltaSize;
      }
    }

    final Binary bufferChanges = new Binary(TX_HEADER_SIZE + TX_FOOTER_SIZE + segmentSize);
    bufferChanges.setAutoResizable(false);

    // WRITE TX HEADER (TXID, TIMESTAMP, PAGE SEGMENTS, SEGMENT-SIZE)
    bufferChanges.putLong(txId);
    bufferChanges.putLong(System.currentTimeMillis());
    bufferChanges.putInt(segments);
    bufferChanges.putInt(segmentSize);

    assert bufferChanges.position() == TX_HEADER_SIZE;

    // WRITE ALL PAGES SEGMENTS
    for (final MutablePage newPage : pages) {
      final int[] ranges = newPage.getModifiedRanges();
      final int rangeCount = newPage.getModifiedRangeCount();

      for (int r = 0; r < rangeCount; ++r) {
        final int rangeFrom = ranges[r * 2];
        final int rangeTo = ranges[r * 2 + 1];

        assert rangeFrom > -1 && rangeTo < newPage.getPhysicalSize();

        final int deltaSize = rangeTo - rangeFrom + 1;
        if (deltaSize < 1)
          throw new TransactionException(
              "Invalid modified range for page " + newPage.getPageId() + " v" + newPage.version + ": deltaRange=[" + rangeFrom
                  + "," + rangeTo + "] deltaSize=" + deltaSize + " pageSize=" + newPage.getPhysicalSize());

        LogManager.instance()
            .log(WALFile.class, Level.FINE, "Writing page %s v%d range %d-%d into buffer (txId=%d threadId=%d)", null,
                newPage.getPageId(), newPage.version + 1, rangeFrom, rangeTo, txId, Thread.currentThread().threadId());

        bufferChanges.putInt(newPage.getPageId().getFileId());
        bufferChanges.putInt(newPage.getPageId().getPageNumber());
        bufferChanges.putInt(rangeFrom);
        bufferChanges.putInt(rangeTo);
        bufferChanges.putInt(newPage.version + 1);
        bufferChanges.putInt(newPage.getContentSize());

        bufferChanges.size(bufferChanges.position() + deltaSize);

        final ByteBuffer newPageBuffer = newPage.getContent();
        newPageBuffer.position(rangeFrom);
        newPageBuffer.get(bufferChanges.getContent(), bufferChanges.position(), deltaSize);

        bufferChanges.position(bufferChanges.position() + deltaSize);
      }
    }

    // WRITE TX FOOTER (MAGIC NUMBER)
    bufferChanges.putInt(segmentSize);
    bufferChanges.putLong(MAGIC_NUMBER);

    return bufferChanges;
  }

  public void writeTransactionToFile(final DatabaseInternal database, final List<MutablePage> pages, final FlushType sync, final WALFile file, final long txId,
      final Binary buffer) throws IOException {

    LogManager.instance()
        .log(this, Level.FINE, "Appending WAL for txId=%d (size=%d file=%s threadId=%d)", null, txId, buffer.size(), filePath, Thread.currentThread().threadId());

    file.append(buffer.getByteBuffer());

    // WRITE ALL PAGES SEGMENTS
    for (final MutablePage newPage : pages) {
      // SET THE WAL FILE TO NOTIFY LATER WHEN THE PAGE HAS BEEN FLUSHED
      newPage.setWALFile(file);

      pagesToFlush.incrementAndGet();
      statsPagesWritten++;
    }

    statsBytesWritten += buffer.size();

    // This instance's own channel, exactly as the channel.force() calls these replaced: the `file`
    // parameter is this same object at the only call site (TransactionManager.tryWriteTransactionToWALFile
    // passes it twice), but the fsync has always been of `this` and this fix does not change that.
    if (sync == FlushType.YES_NOMETADATA)
      force(false);
    else if (sync == FlushType.YES_FULL)
      force(true);

    database.executeCallbacks(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE);
  }

  public void notifyPageFlushed() {
    pagesToFlush.decrementAndGet();
  }

  public long getSize() throws IOException {
    try {
      return channel.size();
    } catch (final ClosedChannelException e) {
      return retryAfterReopen(e, "getSize", () -> channel.size());
    }
  }

  public String getFilePath() {
    return filePath;
  }

  @Override
  public String toString() {
    return filePath;
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> map = new HashMap<>();
    map.put("pagesWritten", statsPagesWritten);
    map.put("bytesWritten", statsBytesWritten);
    return map;
  }

  public static FlushType getWALFlushType(final int txFlushType) {
    switch (txFlushType) {
    case 0:
      return FlushType.NO;
    case 1:
      return FlushType.YES_NOMETADATA;
    case 2:
      return FlushType.YES_FULL;
    default:
      throw new ConfigurationException("Invalid TX_WAL_FLUSH setting " + txFlushType);
    }
  }

  private long readLong(final long pos) throws IOException {
    // #4958: per-call buffer (the shared instance buffers were unsynchronized) filled with a full-read
    // loop: a single channel.read may return early and the old code then decoded stale garbage.
    final ByteBuffer buffer = ByteBuffer.allocate(Binary.LONG_SERIALIZED_SIZE);
    readFully(buffer, pos);
    return buffer.getLong(0);
  }

  private int readInt(final long pos) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(Binary.INT_SERIALIZED_SIZE);
    readFully(buffer, pos);
    return buffer.getInt(0);
  }

  private void readFully(final ByteBuffer buffer, final long pos) throws IOException {
    long readPos = pos;
    while (buffer.hasRemaining()) {
      final int n = readChunk(buffer, readPos);
      if (n == -1)
        throw new EOFException("EOF reading " + buffer.capacity() + " bytes at position " + pos + " of WAL file " + filePath);
      readPos += n;
    }
  }

  protected void append(final ByteBuffer buffer) throws IOException {
    // Recorded BEFORE the first write attempt so the #7768 retry below can restart from the same offset.
    // -1 means channel.size() itself is what the interrupt killed, so nothing was written and the reopened
    // channel's own size is the right place to start.
    long startPos = -1;
    try {
      startPos = channel.size();
      appendAt(buffer, startPos);
    } catch (final ClosedChannelException e) {
      // #7768: a thread interrupt closed the channel mid-append. Reopen it and write the record again from
      // the offset the first attempt started at - NOT from the reopened channel's size(), which already
      // counts however many bytes the interrupted write managed to put down. Restarting past them would
      // leave that torn prefix in the log ahead of the complete record, which is exactly the corruption the
      // #4508 gap detector reports. Rewriting over them is safe: these are positional writes into a region
      // no committed record has ever claimed.
      final long retryFrom = startPos;
      retryAfterReopen(e, "append", () -> {
        appendAt(buffer, retryFrom >= 0 ? retryFrom : channel.size());
        return null;
      });
    }
  }

  /**
   * Writes {@code buffer} in full at {@code startPos}.
   * <p>
   * #4958: loop until the buffer is fully written. A single channel.write may write only part of the record,
   * leaving a torn entry that the #4508 gap detector would then flag as corruption. Single-writer assumption
   * (same as the pre-loop code that wrote at channel.size() once): appends to a WALFile are externally
   * serialized by acquire(); two concurrent appenders would both seed writePos from the same channel.size()
   * and interleave. The local writePos only tolerates PARTIAL writes, not writers.
   */
  private void appendAt(final ByteBuffer buffer, final long startPos) throws IOException {
    buffer.rewind();
    long writePos = startPos;
    while (buffer.hasRemaining())
      writePos += channel.write(buffer, writePos);
  }
}
