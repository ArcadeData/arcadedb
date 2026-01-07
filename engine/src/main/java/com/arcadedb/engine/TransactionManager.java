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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexCompacted;
import com.arcadedb.index.vector.LSMVectorIndexMutable;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.LockManager;

import java.io.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;
import java.util.stream.*;

public class TransactionManager {
  private static final long MAX_LOG_FILE_SIZE = 64 * 1024 * 1024;
  private static final int  WRITE_WAL_TIMEOUT = 30_000;

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
                cleanWALFiles(true, false);
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
    if (task != null)
      task.cancel();

    try {
      taskExecuting.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      // IGNORE IT
    }

    fileIdsLockManager.close();

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

    for (int retry = 0; retry < 20 && !cleanWALFiles(drop, false); ++retry) {
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    if (!cleanWALFiles(drop, false))
      LogManager.instance()
          .log(this, Level.WARNING, "Error on removing all transaction files. Remained: %s", null, inactiveWALFilePool);
    else {
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
        break;
      }
    }
  }

  public void notifyPageFlushed(final MutablePage page) {
    final WALFile walFile = page.getWALFile();
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
        final WALFile.WALTransaction[] walPositions = new WALFile.WALTransaction[activeWALFilePool.length];
        for (int i = 0; i < activeWALFilePool.length; ++i) {
          final WALFile file = activeWALFilePool[i];
          walPositions[i] = file.getFirstTransaction();
        }

        long lastTxId = -1;

        while (true) {
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

          lastTxId = lowerTxId;

          applyChanges(walPositions[lowerTx], Collections.emptyMap(), true);

          walPositions[lowerTx] = activeWALFilePool[lowerTx].getTransaction(walPositions[lowerTx].endPositionInLog);
        }

        // CONTINUE FROM LAST TXID
        transactionIds.set(lastTxId + 1);

        // REMOVE ALL WAL FILES
        for (final WALFile file : activeWALFilePool) {
          try {
            file.drop();
            LogManager.instance().log(this, Level.FINE, "Dropped WAL file '%s'", null, file);
          } catch (final IOException e) {
            LogManager.instance().log(this, Level.SEVERE, "Error on dropping WAL file '%s'", e, file);
          }
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

    for (final WALFile file : activeWALFilePool) {
      if (file != null) {
        final Map<String, Object> stats = file.getStats();
        statsPagesWritten.addAndGet((Long) stats.get("pagesWritten"));
        statsBytesWritten.addAndGet((Long) stats.get("bytesWritten"));
      }
    }

    map.put("pagesWritten", statsPagesWritten.get());
    map.put("bytesWritten", statsBytesWritten.get());
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
        LogManager.instance()
            .log(this, Level.WARNING, "Error on restoring transaction: received operation on deleted file %d", null, txPage.fileId);
        if (ignoreErrors)
          continue;
        throw new ConcurrentModificationException(
            "Concurrent modification on page " + pageId + ". The file with id " + pageId.getFileId()
                + " does not exist anymore. Please retry the operation");
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

        if (txPage.currentPageVersion <= page.getVersion()) {
          if (ignoreErrors)
            // SKIP IT
            continue;
          throw new ConcurrentModificationException(
              "Concurrent modification on page " + pageId + " in file '" + database.getFileManager().getFile(pageId.getFileId())
                  .getFileName() + "' (current v." + txPage.currentPageVersion + " <= database v." + page.getVersion()
                  + "). Please retry the operation (threadId=" + Thread.currentThread().threadId() + ")");
        }

        if (txPage.currentPageVersion > page.getVersion() + 1) {
          LogManager.instance().log(this, Level.WARNING,
              "Cannot apply changes to the database because modified page %s version in WAL (" + txPage.currentPageVersion
                  + ") does not match with existent version (" + page.getVersion() + ") fileId=" + txPage.fileId, null, pageId);
          if (ignoreErrors)
            continue;
          throw new ConcurrentModificationException(
              "Cannot apply changes to the database because modified page " + pageId + " version in WAL ("
                  + txPage.currentPageVersion + ") does not match with existent version (" + page.getVersion() + ") fileId="
                  + txPage.fileId);
        }
//          throw new WALException("Cannot apply changes to the database because modified page version in WAL (" + txPage.currentPageVersion
//              + ") does not match with existent version (" + page.getVersion() + ") fileId=" + txPage.fileId);

        LogManager.instance().log(this, Level.FINE, "Updating page %s versionInLog=%d versionInDB=%d (txId=%d)", null, pageId,
            txPage.currentPageVersion, page.getVersion(), tx.txId);

        // IF VERSION IS THE SAME OR MAJOR, OVERWRITE THE PAGE
        final MutablePage modifiedPage = page.modify();
        txPage.currentContent.rewind();
        modifiedPage.writeByteArray(txPage.changesFrom - BasePage.PAGE_HEADER_SIZE, txPage.currentContent.getContent());
        modifiedPage.version = txPage.currentPageVersion;
        modifiedPage.setContentSize(txPage.currentPageSize);
        modifiedPage.updateMetadata();
        file.write(modifiedPage);

        database.getPageManager().removePageFromCache(modifiedPage.pageId);

        final PaginatedComponent component = (PaginatedComponent) database.getSchema().getFileById(txPage.fileId);
        if (component != null) {
          component.updatePageCount(modifiedPage.pageId.getPageNumber() + 1);

          // Phase 5: For LSMVectorIndex, incrementally update VectorLocationIndex during replication
          // This keeps in-memory metadata synchronized with replicated pages
          // Note: LSMVectorIndexMutable is what gets registered with Schema (via index.getComponent())
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

    for (Map.Entry<Integer, Integer> entry : bucketRecordDelta.entrySet()) {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(entry.getKey());
      if (bucket.getCachedRecordCount() > -1)
        // UPDATE THE CACHE COUNTER ONLY IF ALREADY COMPUTED
        bucket.setCachedRecordCount(bucket.getCachedRecordCount() + entry.getValue());
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
          throw new TimeoutException(
              "Timeout on locking file " + attemptFileId + " (" + database.getFileManager().getFile(attemptFileId).getFileName()
                  + ") during commit (fileIds=" + orderedFilesIds + ", timeout=" + timeout + "ms");

        throw new TimeoutException("Timeout on locking files during commit (fileIds=" + orderedFilesIds + ")");
      }
    }

    // OK: ALL LOCKED
    LogManager.instance()
        .log(this, Level.FINE, "Locked files %s (threadId=%d)", null, orderedFilesIds, Thread.currentThread().threadId());
    // RETURN ONLY THE LOCKED FILES
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
    for (final Iterator<WALFile> it = inactiveWALFilePool.iterator(); it.hasNext(); ) {
      final WALFile file = it.next();

      if (force || !dropFiles || file.getPendingPagesToFlush() == 0) {
        // ALL PAGES FLUSHED, REMOVE THE FILE
        try {
          final Map<String, Object> fileStats = file.getStats();
          statsPagesWritten.addAndGet((Long) fileStats.get("pagesWritten"));
          statsBytesWritten.addAndGet((Long) fileStats.get("bytesWritten"));

          if (dropFiles)
            file.drop();
          else
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
