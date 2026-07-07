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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexCompacted;
import com.arcadedb.index.vector.LSMVectorIndexMutable;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.LockManager;

import java.io.*;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.stream.Stream;

public class TransactionManager {
  private static final long MAX_LOG_FILE_SIZE = 64 * 1024 * 1024;
  private static final int  WRITE_WAL_TIMEOUT = 30_000;
  /**
   * On-disk record of the highest assigned transaction id, written on close and read on open. Lets
   * {@link #getLastTransactionId()} return a meaningful value even after a clean shutdown wiped the
   * WAL. Used by the HA bootstrap path (issue #4147) so a peer staged from a clean backup can
   * report its database recency without having to scan WAL files.
   */
  static final  String LAST_TX_ID_FILE_NAME = "last-tx-id.bin";

  private final DatabaseInternal             database;
  private       WALFile[]                    activeWALFilePool;
  private final List<WALFile>                inactiveWALFilePool = Collections.synchronizedList(new ArrayList<>());
  private final String                       logContext;
  private final Timer                        task;
  private       CountDownLatch               taskExecuting       = new CountDownLatch(0);
  private final AtomicLong                   transactionIds      = new AtomicLong();
  private final AtomicLong                   logFileCounter      = new AtomicLong();
  private final LockManager<Integer, Object> fileIdsLockManager  = new LockManager<>();
  private final AtomicLong                   statsPagesWritten   = new AtomicLong();
  private final AtomicLong                   statsBytesWritten   = new AtomicLong();

  public TransactionManager(final DatabaseInternal database) {
    this.database = database;

    this.logContext = LogManager.instance().getContext();

    // Restore the last assigned transaction id from disk (written on the previous close). Lets
    // freshly-opened databases that have no pending WAL still report a meaningful recency value to
    // the HA bootstrap protocol (issue #4147). Falls through to 0 when the file is missing
    // (legitimate: empty database, or pre-#4147 backup).
    final long persistedLastTxId = readPersistedLastTransactionId();
    if (persistedLastTxId >= 0)
      transactionIds.set(persistedLastTxId + 1);

    if (database.getMode() == ComponentFile.MODE.READ_WRITE) {
      createWALFilePool();

      task = new Timer("ArcadeDB TransactionManager " + database.getName());
      task.schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            if (!database.isOpen()) {
              // DB CLOSED, CANCEL THE TASK
              cancel();
              return;
            }

            if (activeWALFilePool != null) {
              taskExecuting = new CountDownLatch(1);
              try {
                if (logContext != null)
                  LogManager.instance().setContext(logContext);

                checkWALFiles();
                // Runtime WAL rotation: fsync the data files before a rotated WAL is dropped, so a power
                // loss cannot lose pages that were only write()'n to the OS cache (issue #4509).
                cleanWALFiles(true, false, true);
              } finally {
                taskExecuting.countDown();
              }
            }
          } catch (Throwable e) {
            LogManager.instance().log(this, Level.SEVERE, "Error on transaction manager task", e);
          } finally {
            if (logContext != null)
              LogManager.instance().setContext(null);
          }
        }
      }, 1000, 1000);
    } else
      task = null;
  }

  public void close(final boolean drop) {
    close(drop, false);
  }

  /**
   * @param preserveWalFiles when true (a close after {@code waitAllPagesOfDatabaseAreFlushed} gave up, #4928)
   *                         the WAL files are closed but NOT deleted: they still protect pages that never
   *                         reached the disk, and together with the preserved lock file they make the next
   *                         open run recovery and replay them. A normal clean close deletes them as before.
   *                         Independently of this flag, a non-drop close also preserves the WAL when any WAL
   *                         file still has UNACKED pages: a page whose flush failed and was contained (#4928)
   *                         leaves the flush pipeline without ever calling notifyPageFlushed, so the pending
   *                         ack - not the pipeline emptiness the caller's wait observes - is the ground truth
   *                         that the WAL still holds the only durable copy of something. Such a page is never
   *                         re-flushed in-process; deleting its WAL on a later clean close would silently
   *                         lose the committed change.
   *
   * @return whether the WAL files were preserved for recovery (the caller then also keeps the lock file, so
   *     the next open replays them).
   */
  public boolean close(final boolean drop, final boolean preserveWalFiles) {
    if (task != null)
      task.cancel();

    try {
      taskExecuting.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      // IGNORE IT
    }

    fileIdsLockManager.close();

    // Persist (or clean up) the lastTxId marker before WAL files are deleted on close, so the
    // recency signal survives a clean shutdown for the HA bootstrap path (issue #4147).
    if (drop) {
      final File f = lastTxIdFile();
      if (f != null && f.exists())
        f.delete();
    } else {
      writePersistedLastTransactionId();
    }

    if (activeWALFilePool != null) {
      // MOVE ALL WAL FILES AS INACTIVE
      for (int i = 0; i < activeWALFilePool.length; ++i) {
        final WALFile file = activeWALFilePool[i];
        if (file != null) {
          activeWALFilePool[i] = null;
          inactiveWALFilePool.add(file);
          file.setActive(false);
        }
      }
    }

    // Ground-truth durability check BEFORE the files are closed below: any unacked page means some WAL
    // entry is the only durable copy of a committed change (e.g. a contained flush failure, #4928).
    boolean pendingAcks = false;
    if (!drop)
      for (final WALFile file : List.copyOf(inactiveWALFilePool))
        if (file.getPendingPagesToFlush() > 0) {
          pendingAcks = true;
          break;
        }
    final boolean preserve = preserveWalFiles || (!drop && pendingAcks);

    for (int retry = 0; retry < 20 && !cleanWALFiles(drop, false); ++retry) {
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    if (!cleanWALFiles(drop, drop)) {
      LogManager.instance()
          .log(this, Level.WARNING, "Error on removing all transaction files. Remained: %s", null, inactiveWALFilePool);
      return preserve;
    } else if (preserve) {
      // #4928: pages never reached the disk (the bounded wait gave up, or a contained flush failure left
      // unacked WAL entries). The WAL content on disk is the only durable copy of those pages: deleting it
      // here (as a clean close does) would silently lose the most recent transactions. The files were closed
      // above; leave them (and the caller leaves the lock file) so the next open runs recovery and replays.
      LogManager.instance().log(this, Level.SEVERE,
          "Preserving the WAL files of database '%s': not all pages reached the disk (%s), the next open will recover them",
          null, database.getName(), preserveWalFiles ? "flush wait gave up" : "unacked WAL pages");
    } else {
      // DELETE ALL THE WAL FILES AT OS-LEVEL
      final File dir = new File(database.getDatabasePath());
      File[] walFiles = dir.listFiles((dir1, name) -> name.endsWith(".wal"));
      if (walFiles != null) {
        Stream.of(walFiles).forEach(File::delete);
        walFiles = dir.listFiles((dir1, name) -> name.endsWith(".wal"));
      }

      if (walFiles != null && walFiles.length > 0)
        LogManager.instance()
            .log(this, Level.WARNING, "Error on removing all transaction files. Remained: %s", null, walFiles.length);
    }
    return preserve;
  }

  public Binary createTransactionBuffer(final long txId, final List<MutablePage> pages) {
    return WALFile.writeTransactionToBuffer(pages, txId);
  }

  public void writeTransactionToWAL(final List<MutablePage> pages, final WALFile.FlushType sync, final long txId,
      final Binary bufferChanges) {
    final long begin = System.currentTimeMillis();

    while (true) {
      final WALFile file = activeWALFilePool[(int) (Thread.currentThread().threadId() % activeWALFilePool.length)];

      if (file != null && file.acquire(() -> {
        file.writeTransactionToFile(database, pages, sync, file, txId, bufferChanges);
        return null;
      }))
        break;

      if (System.currentTimeMillis() - begin > WRITE_WAL_TIMEOUT)
        throw new TransactionException("Timeout on writing transaction to WAL");

      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        // Fail loud instead of returning as if the WAL write succeeded (#4938). Breaking out here let
        // commit2ndPhase publish the transaction's pages with NO WAL record: unrecoverable on crash, and later
        // transactions on the same pages would raise WALVersionGap during recovery. The interrupt flag is
        // restored above so the cancellation is still observable to the caller.
        throw new TransactionException("Interrupted while writing transaction " + txId + " to the WAL", e);
      }
    }
  }

  public void notifyPageFlushed(final MutablePage page) {
    // takeWALFile (not get) so the ack is released exactly once even when this races the file-dropped
    // flush branch or the dropped-file batch purge on the same page (#4928).
    final WALFile walFile = page.takeWALFile();
    if (walFile != null)
      walFile.notifyPageFlushed();
  }

  public void checkIntegrity() {
    LogManager.instance().log(this, Level.WARNING, "Started recovery of database '%s'", null, database);

    try {
      // OPEN EXISTENT WAL FILES
      final File dir = new File(database.getDatabasePath());
      final File[] walFiles = dir.listFiles((dir1, name) -> name.endsWith(".wal"));

      if (walFiles == null || walFiles.length == 0) {
        LogManager.instance().log(this, Level.WARNING, "Recovery not possible because no WAL files were found");
        return;
      }

      if (activeWALFilePool != null && activeWALFilePool.length > 0) {
        for (final WALFile file : activeWALFilePool) {
          try {
            file.close();
          } catch (final IOException e) {
            // IGNORE IT
          }
        }
      }

      activeWALFilePool = new WALFile[walFiles.length];
      for (int i = 0; i < walFiles.length; ++i) {
        try {
          activeWALFilePool[i] = new WALFile(database.getDatabasePath() + File.separator + walFiles[i].getName());
        } catch (final FileNotFoundException e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on WAL file management for file '%s'", e,
              database.getDatabasePath() + walFiles[i].getName());
        }
      }

      if (activeWALFilePool.length > 0) {
        long lastTxId = -1;
        boolean walGapDetected = false;

        final WALFile.WALTransaction[] walPositions = new WALFile.WALTransaction[activeWALFilePool.length];
        for (int i = 0; i < activeWALFilePool.length; ++i) {
          final WALFile file = activeWALFilePool[i];
          walPositions[i] = file.getFirstTransaction();
          // A torn first record followed by intact transactions is corruption, not a clean empty/EOF file:
          // stopping silently would drop committed data (issue #4508). Abort recovery and preserve the WAL.
          if (walPositions[i] == null && file.findNextValidTransactionPosition(0) >= 0) {
            LogManager.instance().log(this, Level.SEVERE,
                "Recovery aborted for database '%s': corrupt transaction record at the start of WAL file '%s' followed by valid transactions. WAL files have been preserved for manual inspection. No further transactions will be replayed.",
                null, database, file);
            walGapDetected = true;
          }
        }

        while (!walGapDetected) {
          int lowerTx = -1;
          long lowerTxId = -1;

          for (int i = 0; i < walPositions.length; ++i) {
            final WALFile.WALTransaction walTx = walPositions[i];
            if (walTx != null) {
              if (lowerTxId == -1 || walTx.txId < lowerTxId) {
                lowerTxId = walTx.txId;
                lowerTx = i;
              }
            }
          }

          if (lowerTxId == -1)
            // FINISHED
            break;

          try {
            applyChanges(walPositions[lowerTx], Collections.emptyMap(), false);
            // Only advance lastTxId after a successful apply, otherwise a failed transaction
            // would wrongly increment the next-tx counter past data that was never written.
            lastTxId = lowerTxId;
          } catch (WALVersionGapException e) {
            LogManager.instance().log(this, Level.SEVERE,
                "Recovery aborted for database '%s': version gap in WAL (txId=%d). WAL files have been preserved for manual inspection. No further transactions will be replayed.",
                null, database, lowerTxId);
            walGapDetected = true;
            break;
          }

          final WALFile walFile = activeWALFilePool[lowerTx];
          final long nextPos = walPositions[lowerTx].endPositionInLog;
          final WALFile.WALTransaction nextTx = walFile.getTransaction(nextPos);
          if (nextTx == null && walFile.findNextValidTransactionPosition(nextPos) >= 0) {
            // A record is corrupt but intact transactions follow it: stopping here would silently drop
            // committed data. Abort recovery and preserve the WAL files for manual inspection (issue #4508).
            LogManager.instance().log(this, Level.SEVERE,
                "Recovery aborted for database '%s': corrupt transaction record in WAL file '%s' at offset %d followed by valid transactions. WAL files have been preserved for manual inspection. No further transactions will be replayed.",
                null, database, walFile, nextPos);
            walGapDetected = true;
            break;
          }
          walPositions[lowerTx] = nextTx;
        }

        // Only update the next-tx counter if recovery actually applied a transaction. When
        // lastTxId is still -1 the counter must keep the persistedLastTxId value loaded by the
        // constructor; overwriting it with 0 would lose recency and risk transaction-id reuse.
        if (lastTxId != -1)
          transactionIds.set(lastTxId + 1);

        if (!walGapDetected) {
          // REMOVE ALL WAL FILES
          for (final WALFile file : activeWALFilePool) {
            if (file == null)
              continue;
            try {
              file.drop();
              LogManager.instance().log(this, Level.FINE, "Dropped WAL file '%s'", null, file);
            } catch (final IOException e) {
              LogManager.instance().log(this, Level.SEVERE, "Error on dropping WAL file '%s'", e, file);
            }
          }
        } else {
          // Close WAL files without deleting: preserve for manual inspection after gap detection.
          // #4958: renamed to '<name>.corrupt' so the next open can neither adopt a preserved file as an
          // active WAL (appending new transactions after the corrupt content) nor re-scan and re-abort on
          // it at every recovery. Recovery is aborted for ALL the files (no further transaction will ever
          // be replayed from them), so all of them are moved aside.
          for (final WALFile file : activeWALFilePool) {
            if (file == null)
              continue;
            try {
              file.close();
            } catch (final IOException e) {
              LogManager.instance().log(this, Level.WARNING, "Error on closing WAL file '%s'", e, file);
            }
            final File walFile = new File(file.getFilePath());
            if (walFile.exists())
              try {
                // Files.move (vs File.renameTo, which just returns false) fails with a cause and performs an
                // atomic rename where the filesystem supports it. Best-effort semantics preserved: log and continue.
                // REPLACE_EXISTING (#5063): a prior ABORTED recovery can leave a same-named
                // .corrupt file behind; without it the move throws FileAlreadyExistsException and the original
                // .wal stays in place, re-scanned and re-aborted on every restart - the exact loop this breaks.
                // The stale .corrupt it replaces is evidence from the SAME file's earlier failed attempt.
                Files.move(walFile.toPath(), walFile.toPath().resolveSibling(walFile.getName() + ".corrupt"),
                    StandardCopyOption.REPLACE_EXISTING);
              } catch (final IOException e) {
                LogManager.instance().log(this, Level.WARNING,
                    "Error on renaming preserved WAL file '%s' to '%s.corrupt'", e, walFile, walFile.getName());
              }
          }
          LogManager.instance().log(this, Level.SEVERE,
              "WAL files for database '%s' have been preserved in '%s' with the '.corrupt' extension for manual inspection.",
              null, database, database.getDatabasePath());
        }
        createWALFilePool();
        database.getPageManager().removeAllReadPagesOfDatabase(database);
      }
    } finally {
      LogManager.instance().log(this, Level.WARNING, "Recovery of database '%s' completed", null, database);
    }
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> map = new HashMap<>();
    map.put("logFiles", logFileCounter.get());

    // #4958: compute a SNAPSHOT of the totals. The previous code folded each active file's cumulative
    // counters into the long-lived accumulators on every call, so the numbers inflated with polling
    // frequency and were double-counted again when the file was retired in cleanWALFiles(). The
    // accumulators are only mutated at file retirement; here the active files' current counters are
    // added on the fly without persisting them. Also guards the read-only case (no active pool).
    long pagesWritten = statsPagesWritten.get();
    long bytesWritten = statsBytesWritten.get();

    final WALFile[] pool = activeWALFilePool;
    if (pool != null)
      for (final WALFile file : pool)
        if (file != null) {
          final Map<String, Object> stats = file.getStats();
          pagesWritten += (Long) stats.get("pagesWritten");
          bytesWritten += (Long) stats.get("bytesWritten");
        }

    map.put("pagesWritten", pagesWritten);
    map.put("bytesWritten", bytesWritten);
    return map;
  }

  public boolean applyChanges(final WALFile.WALTransaction tx, final Map<Integer, Integer> bucketRecordDelta,
      final boolean ignoreErrors) {
    boolean changed = false;
    boolean involveDictionary = false;

    final int dictionaryId =
        database.getSchema().getDictionary() != null ? database.getSchema().getDictionary().file.getFileId() : -1;

    LogManager.instance().log(this, Level.FINE, "- applying changes from txId=%d", null, tx.txId);

    for (final WALFile.WALPage txPage : tx.pages) {
      final PaginatedComponentFile file;

      final PageId pageId = new PageId(database, txPage.fileId, txPage.pageNumber);

      if (!database.getFileManager().existsFile(txPage.fileId)) {
        // Referencing a missing file is expected in two safe cases:
        // 1. LSM compaction migrated the file to a new ID - data is already in the new file.
        // 2. A schema change (DROP TYPE / DROP INDEX) deleted the file intentionally.
        // In both cases we skip. For case 1 the migration map is populated so we can log at FINE.
        // For case 2 (or any unexpected missing file) we log at WARNING so operators notice.
        final Integer migratedTo = database.getSchema().getEmbedded().getMigratedFileId(txPage.fileId);
        if (migratedTo != null)
          LogManager.instance().log(this, Level.FINE,
              "Skipping page for compaction-migrated file %d (now %d) during transaction apply", null, txPage.fileId, migratedTo);
        else
          LogManager.instance().log(this, Level.INFO,
              "Skipping page for missing file %d during transaction apply, file was deleted or migration map is incomplete", null,
              txPage.fileId);
        continue;
      }

      try {
        file = (PaginatedComponentFile) database.getFileManager().getFile(txPage.fileId);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on applying tx changes for page %s", e, txPage);
        throw e;
      }

      try {
        final ImmutablePage page = database.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true);

        LogManager.instance()
            .log(this, Level.FINE, "-- checking page %s versionInLog=%d versionInDB=%d", null, pageId, txPage.currentPageVersion,
                page.getVersion());

        if (txPage.currentPageVersion < page.getVersion()) {
          // Always skip OLDER pages instead of aborting the entire transaction. During Raft state machine
          // replay or crash recovery, a partial apply may have written some pages but not others. Skipping
          // only the superseded pages (instead of throwing ConcurrentModificationException and aborting the
          // loop) ensures the remaining un-applied pages in the same transaction are still processed. This
          // prevents version-gap cascades where skipping a multi-page transaction leaves some pages behind,
          // causing WALVersionGapException on all subsequent transactions that touch those pages.
          //
          // #4926: STRICTLY older only - the equal-version case falls through and RE-APPLIES the delta. A
          // 64KB page flush is many disk sectors and the version header lives in sector 0: a torn write at
          // power loss can persist the new version header while the delta sectors still hold the previous
          // content. The old `<=` skip declared such a page "already applied" and left it silently corrupt
          // (new version, old bytes) despite an intact WAL. Re-applying at equal version is idempotent (the
          // WAL delta carries absolute bytes for exactly this version) and repairs the torn state; skipping
          // a STRICTLY older entry remains required, because its delta would overwrite newer content.
          //
          // KNOWN LIMITATION (multi-version accumulation): async flush coalesces several committed versions
          // into a single physical page write, so a page can jump v1(on-disk) -> v2 -> v3 -> v4 with only the
          // v4 flush hitting the platter. If THAT flush tears, only v4's delta region is repaired here; the
          // regions changed by v2/v3 (skipped as strictly-older) can stay torn. The version header alone
          // cannot tell a torn page from an intact one, so detecting which pages need the older deltas
          // re-applied needs a per-page checksum (the longer-term fix noted in #4926, tracked in #5054); an
          // unconditional
          // in-order replay of every entry <= disk version would repair it but changes the recovery
          // semantics that also govern the version-gap/forceApply paths, out of scope here. This fix still
          // strictly improves on `<=` (which repaired nothing) and fully covers the single-flush case.
          LogManager.instance().log(this, Level.FINE,
              "Skipping superseded page %s (log v.%d < db v.%d)", null,
              pageId, txPage.currentPageVersion, page.getVersion());
          continue;
        }

        if (txPage.currentPageVersion > page.getVersion() + 1) {
          if (!tx.forceApply) {
            LogManager.instance().log(this, Level.WARNING,
                "Cannot apply changes to the database because modified page %s version in WAL (%d) does not match with existent version (%d) fileId=%d",
                null, pageId, txPage.currentPageVersion, page.getVersion(), txPage.fileId);
            if (ignoreErrors)
              continue;
            throw new WALVersionGapException(
                "Cannot apply changes to the database because modified page " + pageId + " version in WAL ("
                    + txPage.currentPageVersion + ") does not match with existent version (" + page.getVersion() + ") fileId="
                    + txPage.fileId);
          }
          // forceApply (compaction page replication) bypasses the version gap, but ONLY when the WAL
          // entry rewrites the WHOLE content region. The follower's page is at version N while this
          // entry carries N+K with K>1: it never saw the intermediate transactions, so the bytes
          // outside [changesFrom, changesTo] do not correspond to the leader's baseline. Applying a
          // partial delta would leave those bytes stale yet force the version forward to N+K, so every
          // later MVCC check would pass over a silently corrupted page that then spreads across the
          // cluster (issue #4510). A full-page payload (what serializeFilePagesAsWal ships) covers the
          // entire content region, so it is safe regardless of the gap; a partial one is rejected here
          // so the follower recovers via a full snapshot instead.
          final int deltaSize = txPage.changesTo - txPage.changesFrom + 1;
          final boolean fullPage =
              txPage.changesFrom == BasePage.PAGE_HEADER_SIZE && deltaSize == file.getPageSize() - BasePage.PAGE_HEADER_SIZE;
          if (!fullPage) {
            LogManager.instance().log(this, Level.SEVERE,
                "Refusing forceApply of partial delta for page %s over a stale baseline (WAL v.%d, db v.%d) fileId=%d: bytes outside the delta range would stay stale while the version is forced forward. A full-page snapshot is required.",
                null, pageId, txPage.currentPageVersion, page.getVersion(), txPage.fileId);
            if (ignoreErrors)
              continue;
            throw new WALVersionGapException(
                "Refusing forceApply of partial delta for page " + pageId + " over a stale baseline (WAL v."
                    + txPage.currentPageVersion + ", db v." + page.getVersion()
                    + "): a full page is required to safely bypass the version gap, fileId=" + txPage.fileId);
          }
        }

        LogManager.instance().log(this, Level.FINE, "Updating page %s versionInLog=%d versionInDB=%d (txId=%d)", null, pageId,
            txPage.currentPageVersion, page.getVersion(), tx.txId);

        // IF VERSION IS THE SAME OR MAJOR, OVERWRITE THE PAGE
        final MutablePage modifiedPage = page.modify();
        txPage.currentContent.rewind();
        modifiedPage.writeByteArray(txPage.changesFrom - BasePage.PAGE_HEADER_SIZE, txPage.currentContent.getContent());
        modifiedPage.version = txPage.currentPageVersion;
        modifiedPage.setContentSize(txPage.currentPageSize);
        modifiedPage.updateMetadata();

        // Write under the per-page I/O lock so concurrent readers never observe partially-written bytes during
        // replicated/recovery replay (forceApply). Evict from the read cache AFTER the write so subsequent reads
        // reload the new content from disk (see PageManager.writePageWithLock).
        database.getPageManager().writePageWithLock(file, modifiedPage);

        database.getPageManager().removePageFromCache(modifiedPage.pageId);

        final PaginatedComponent component = (PaginatedComponent) database.getSchema().getFileByIdIfExists(txPage.fileId);
        if (component != null) {
          component.updatePageCount(modifiedPage.pageId.getPageNumber() + 1);

          // For LSMVectorIndex, incrementally update VectorLocationIndex during replication
          // to keep in-memory metadata synchronized with replicated pages.
          if (component instanceof LSMVectorIndexMutable) {
            final LSMVectorIndexMutable vectorMutable =
                (LSMVectorIndexMutable) component;
            final LSMVectorIndex mainIndex = vectorMutable.getMainIndex();
            if (mainIndex != null) {
              mainIndex.applyReplicatedPageUpdate(modifiedPage);
            } else {
              LogManager.instance().log(this, Level.WARNING,
                  "LSMVectorIndexMutable has null mainIndex for fileId=%d", null, txPage.fileId);
            }
          } else if (component instanceof LSMVectorIndexCompacted) {
            final LSMVectorIndex mainIndex =
                (LSMVectorIndex) ((LSMVectorIndexCompacted) component).getMainComponent();
            if (mainIndex != null) {
              mainIndex.applyReplicatedPageUpdate(modifiedPage);
            }
          }
        }

        if (file.getFileId() == dictionaryId)
          involveDictionary = true;

        changed = true;
        LogManager.instance().log(this, Level.FINE, "  - updating page %s v%d", null, pageId, modifiedPage.version);

      } catch (final ClosedByInterruptException e) {
        // NORMAL EXCEPTION IN CASE THE CONNECTION/THREAD IS CLOSED (=INTERRUPTED)
        Thread.currentThread().interrupt();
        throw new WALException("Cannot apply changes to page " + pageId, e);
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on applying changes to page %s", e, pageId);
        throw new WALException("Cannot apply changes to page " + pageId, e);
      }
    }

    if (changed) {
      for (final Map.Entry<Integer, Integer> entry : bucketRecordDelta.entrySet()) {
        final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(entry.getKey());
        if (bucket.getCachedRecordCount() > -1)
          // UPDATE THE CACHE COUNTER ONLY IF ALREADY COMPUTED
          bucket.setCachedRecordCount(bucket.getCachedRecordCount() + entry.getValue());
      }
    }

    if (involveDictionary) {
      try {
        database.getSchema().getDictionary().reload();
      } catch (final IOException e) {
        throw new SchemaException("Unable to update dictionary after transaction commit", e);
      }
    }

    return changed;
  }

  public void kill() {
    if (task != null) {
      task.cancel();
      task.purge();
    }

    fileIdsLockManager.close();

    try {
      taskExecuting.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      // IGNORE IT
    }

    if (activeWALFilePool != null) {
      for (int i = 0; i < activeWALFilePool.length; ++i) {
        final WALFile file = activeWALFilePool[i];
        if (file != null) {
          activeWALFilePool[i] = null;
          inactiveWALFilePool.add(file);
          file.setActive(false);
        }
      }
    }

    // WAIT FOR ALL THE PAGE TO BE FLUSHED
    for (int retry = 0; retry < 20 && !cleanWALFiles(false, true); ++retry) {
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    if (!cleanWALFiles(false, true))
      LogManager.instance()
          .log(this, Level.WARNING, "Error on removing all transaction files during kill. Remained: %s", null, inactiveWALFilePool);
  }

  public long getNextTransactionId() {
    return transactionIds.getAndIncrement();
  }

  /**
   * Returns the highest transaction id that has been assigned (and therefore persisted to the WAL when committed).
   * After WAL recovery on database open this reflects the last persisted transaction; while the database is
   * actively writing it tracks the next-to-assign counter minus one. Used by the HA bootstrap path
   * (see {@code arcadedb.ha.bootstrapFromLocalDatabase}, issue #4147) as the recency / freshness signal
   * when peers compare their local database state at first cluster formation.
   */
  public long getLastTransactionId() {
    return transactionIds.get() - 1;
  }

  /**
   * Read the last transaction id persisted by a previous {@link #close(boolean)}.
   * Returns -1 if the file is missing (empty database) or unreadable.
   */
  private long readPersistedLastTransactionId() {
    final File f = lastTxIdFile();
    if (f == null || !f.isFile())
      return -1L;
    try (final DataInputStream in = new DataInputStream(new FileInputStream(f))) {
      return in.readLong();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not read %s, treating database as having no recency info: %s", null, LAST_TX_ID_FILE_NAME, e.getMessage());
      return -1L;
    }
  }

  /**
   * Persist the current {@code lastTxId} so it survives a clean shutdown (the WAL is purged on
   * close and would otherwise erase the recency signal we need at startup).
   */
  private void writePersistedLastTransactionId() {
    final File f = lastTxIdFile();
    if (f == null)
      return;
    final long value = getLastTransactionId();
    if (value < 0)
      return;
    final File tmp = new File(f.getParentFile(), f.getName() + ".tmp");
    try (final DataOutputStream out = new DataOutputStream(new FileOutputStream(tmp))) {
      out.writeLong(value);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not write %s; HA bootstrap will fall back to leader-shipped snapshot for this database. Cause: %s",
          null, LAST_TX_ID_FILE_NAME, e.getMessage());
      tmp.delete();
      return;
    }
    // Atomic rename so a crash mid-write never leaves a half-truncated file.
    if (!tmp.renameTo(f)) {
      // On some filesystems renameTo fails when the target exists. Best-effort fallback.
      f.delete();
      tmp.renameTo(f);
    }
  }

  private File lastTxIdFile() {
    final String path = database.getDatabasePath();
    return path == null ? null : new File(path, LAST_TX_ID_FILE_NAME);
  }

  /**
   * Returns the locked files only. In case the current thread already locked a resource, no error is thrown but the lock is not returned. In this way only
   * the new acquired locks are released.
   */
  public List<Integer> tryLockFiles(final Collection<Integer> fileIds, final long timeout, final Object requester) {
    // ORDER THE FILES TO AVOID DEADLOCK
    final List<Integer> orderedFilesIds = new ArrayList<>(fileIds);
    Collections.sort(orderedFilesIds);

    final List<Integer> lockedFiles = new ArrayList<>(orderedFilesIds.size());

    Integer attemptFileId;
    for (final Integer fileId : orderedFilesIds) {
      attemptFileId = fileId;

      final LockManager.LOCK_STATUS lock = tryLockFile(fileId, timeout, requester);

      if (lock == LockManager.LOCK_STATUS.YES)
        lockedFiles.add(fileId);
      else if (lock == LockManager.LOCK_STATUS.NO) {
        // ERROR: UNLOCK LOCKED FILES
        unlockFilesInOrder(lockedFiles, requester);

        if (attemptFileId != null)
          throw new LockTimeoutException(
              "Timeout on locking file " + attemptFileId + " (" + database.getFileManager().getFile(attemptFileId).getFileName()
                  + ") during commit (fileIds=" + orderedFilesIds + ", timeout=" + timeout + "ms");

        throw new LockTimeoutException("Timeout on locking files during commit (fileIds=" + orderedFilesIds + ")");
      }
    }

    // OK: ALL LOCKED
    LogManager.instance()
        .log(this, Level.FINE, "Locked files %s (threadId=%d)", null, orderedFilesIds, Thread.currentThread().threadId());
    // RETURN ONLY THE LOCKED FILES
    return lockedFiles;
  }

  /**
   * Primitive-array overload to avoid Integer boxing on the per-commit hot path. The
   * passed-in {@code fileIds} array is sorted in place to acquire locks in deadlock-safe order.
   */
  public List<Integer> tryLockFiles(final int[] fileIds, final long timeout, final Object requester) {
    // ORDER THE FILES TO AVOID DEADLOCK
    Arrays.sort(fileIds);

    final List<Integer> lockedFiles = new ArrayList<>(fileIds.length);

    int attemptFileId = -1;
    for (final int fileId : fileIds) {
      attemptFileId = fileId;

      final LockManager.LOCK_STATUS lock = tryLockFile(fileId, timeout, requester);

      if (lock == LockManager.LOCK_STATUS.YES)
        lockedFiles.add(fileId);
      else if (lock == LockManager.LOCK_STATUS.NO) {
        // ERROR: UNLOCK LOCKED FILES
        unlockFilesInOrder(lockedFiles, requester);

        throw new LockTimeoutException(
            "Timeout on locking file " + attemptFileId + " (" + database.getFileManager().getFile(attemptFileId).getFileName()
                + ") during commit (fileIds=" + Arrays.toString(fileIds) + ", timeout=" + timeout + "ms");
      }
    }

    // OK: ALL LOCKED
    if (LogManager.instance().isDebugEnabled())
      LogManager.instance().log(this, Level.FINE, "Locked files %s (threadId=%d)", null, Arrays.toString(fileIds),
          Thread.currentThread().threadId());
    return lockedFiles;
  }

  public void unlockFilesInOrder(final List<Integer> lockedFileIds, final Object requester) {
    if (lockedFileIds != null && !lockedFileIds.isEmpty()) {
      for (final Integer fileId : lockedFileIds)
        unlockFile(fileId, requester);

      LogManager.instance()
          .log(this, Level.FINE, "Unlocked files %s (threadId=%d)", null, lockedFileIds, Thread.currentThread().threadId());
    }
  }

  public LockManager.LOCK_STATUS tryLockFile(final Integer fileId, final long timeout, final Object requester) {
    return fileIdsLockManager.tryLock(fileId, requester, timeout);
  }

  public void unlockFile(final Integer fileId, final Object requester) {
    fileIdsLockManager.unlock(fileId, requester);
  }

  private void createWALFilePool() {
    // #4958: seed the counter PAST any existing txlog_<n>.wal. WALFile opens its path in "rw" mode
    // without truncation, so restarting the counter from 0 while preserved WAL files are still on disk
    // (corrupt-gap detection or a #4928 crash-equivalent close) silently reused them as active WALs:
    // new transactions were appended after the old content and the old records replayed again.
    final File[] existingWALFiles = new File(database.getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal"));
    if (existingWALFiles != null)
      for (final File existing : existingWALFiles) {
        final String name = existing.getName();
        if (name.startsWith("txlog_"))
          try {
            final long counter = Long.parseLong(name.substring("txlog_".length(), name.length() - ".wal".length()));
            if (counter >= logFileCounter.get())
              logFileCounter.set(counter + 1);
          } catch (final NumberFormatException e) {
            // NOT A POOL FILE, IGNORE IT
          }
      }

    activeWALFilePool = new WALFile[database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FILES)];
    for (int i = 0; i < activeWALFilePool.length; ++i) {
      final long counter = logFileCounter.getAndIncrement();
      try {
        activeWALFilePool[i] = database.getWALFileFactory().newInstance(database.getDatabasePath() + "/txlog_" + counter + ".wal");
      } catch (final FileNotFoundException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on WAL file management for file '%s'", e,
            database.getDatabasePath() + "/txlog_" + counter + ".wal");
      }
    }
  }

  private void checkWALFiles() {
    if (activeWALFilePool != null)
      for (int i = 0; i < activeWALFilePool.length; ++i) {
        final WALFile file = activeWALFilePool[i];
        try {
          if (file != null && file.isOpen() && file.getSize() > MAX_LOG_FILE_SIZE) {
            LogManager.instance().log(this, Level.FINE,
                "WAL file '%s' reached maximum size (%d), set it as inactive, waiting for the drop (page2flush=%d)", null, file,
                MAX_LOG_FILE_SIZE, file.getPendingPagesToFlush());
            activeWALFilePool[i] = database.getWALFileFactory()
                .newInstance(database.getDatabasePath() + "/txlog_" + logFileCounter.getAndIncrement() + ".wal");

            // SET THE FILE AS INACTIVE READY TO BE DISPOSED
            file.setActive(false);
            inactiveWALFilePool.add(file);
          }
        } catch (final ClosedChannelException e) {
          try {
            file.close();
          } catch (IOException ex) {
            // IGNORE IT
          }
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on WAL file management for file '%s'", e, file);
        }
      }
  }

  private boolean cleanWALFiles(final boolean dropFiles, final boolean force) {
    return cleanWALFiles(dropFiles, force, false);
  }

  /**
   * Removes inactive WAL files, either dropping them ({@code dropFiles}) or just closing them.
   *
   * @param dropFiles       when true the WAL files are physically deleted, otherwise only closed
   * @param force           when true a file is removed even if it still has pages pending to flush
   * @param syncDataOnDrop  when true the data files touched by the WAL are fsync'd before the WAL is dropped.
   *                        {@code WALFile.notifyPageFlushed()} decrements the pending-pages counter right after a
   *                        {@code write()} that only reaches the OS page cache, so {@code pendingPagesToFlush == 0}
   *                        does not mean the data is durable. Dropping the WAL at that point would, on a power loss
   *                        before the OS writes the dirty pages back, lose committed transactions and silently
   *                        corrupt later transactions on the same pages (issue #4509). Used by the runtime WAL
   *                        rotation path; the clean-close path already fsyncs via {@code FileManager.syncFiles()}
   *                        before it gets here.
   */
  private boolean cleanWALFiles(final boolean dropFiles, final boolean force, final boolean syncDataOnDrop) {
    boolean dataSynced = false;
    for (final Iterator<WALFile> it = inactiveWALFilePool.iterator(); it.hasNext(); ) {
      final WALFile file = it.next();

      if (force || !dropFiles || file.getPendingPagesToFlush() == 0) {
        // ALL PAGES FLUSHED, REMOVE THE FILE
        try {
          final Map<String, Object> fileStats = file.getStats();
          statsPagesWritten.addAndGet((Long) fileStats.get("pagesWritten"));
          statsBytesWritten.addAndGet((Long) fileStats.get("bytesWritten"));

          if (dropFiles) {
            // Make the data pages durable before the WAL that protects them is deleted. fsync once, lazily,
            // right before the first WAL file is actually dropped in this pass (issue #4509).
            if (syncDataOnDrop && !dataSynced) {
              if (!database.getFileManager().syncFiles()) {
                // #4934: the fsync failed - the data this WAL protects may never reach the disk. Dropping
                // the WAL now would make it unrecoverable; abort this pass, the rotation retries later.
                return false;
              }
              dataSynced = true;
            }
            file.drop();
          } else
            file.close();

        } catch (final IOException e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on %s WAL file '%s'", e, dropFiles ? "dropping" : "closing", file);
        }
        it.remove();
      }
    }

    return inactiveWALFilePool.isEmpty();
  }

  @Override
  public String toString() {
    return "TransactionManager for database '" + database.getName() + "':" + fileIdsLockManager;
  }
}
