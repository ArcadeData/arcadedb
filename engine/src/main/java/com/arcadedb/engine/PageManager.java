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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockContext;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Manages pages from disk to RAM. Each page can have different size.
 */
public class PageManager extends LockContext {
  public static final PageManager INSTANCE = new PageManager();

  private final    ConcurrentMap<PageId, CachedPage> readCache;
  // MANAGE CONCURRENT ACCESS TO THE PAGES. THE VALUE IS TRUE FOR WRITE OPERATION AND FALSE FOR READ
  private final    ConcurrentMap<PageId, Boolean>    pendingFlushPages                     = new ConcurrentHashMap<>();
  private final    long                              maxRAM;
  private final    AtomicLong                        totalReadCacheRAM                     = new AtomicLong();
  private final    AtomicLong                        totalPagesRead                        = new AtomicLong();
  private final    AtomicLong                        totalPagesReadSize                    = new AtomicLong();
  private final    AtomicLong                        totalPagesWritten                     = new AtomicLong();
  private final    AtomicLong                        totalPagesWrittenSize                 = new AtomicLong();
  private final    AtomicLong                        cacheHits                             = new AtomicLong();
  private final    AtomicLong                        cacheMiss                             = new AtomicLong();
  private final    AtomicLong                        totalConcurrentModificationExceptions = new AtomicLong();
  private final    AtomicLong                        evictionRuns                          = new AtomicLong();
  private final    AtomicLong                        pagesEvicted                          = new AtomicLong();
  private volatile long                              lastCheckForRAM                       = 0;
  private final    PageManagerFlushThread            flushThread;
  private final    int                               freePageRAM;

  public interface ConcurrentPageAccessCallback {
    void access() throws IOException;
  }

  public static class PPageManagerStats {
    public long maxRAM;
    public long readCacheRAM;
    public long pagesRead;
    public long pagesReadSize;
    public long pagesWritten;
    public long pagesWrittenSize;
    public int  pageFlushQueueLength;
    public long cacheHits;
    public long cacheMiss;
    public long concurrentModificationExceptions;
    public long evictionRuns;
    public long pagesEvicted;
    public int  readCachePages;
  }

  private PageManager() {
    final ContextConfiguration configuration = new ContextConfiguration();

    this.freePageRAM = configuration.getValueAsInteger(GlobalConfiguration.FREE_PAGE_RAM);
    this.readCache = new ConcurrentHashMap<>(configuration.getValueAsInteger(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE));

    maxRAM = configuration.getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM) * 1024 * 1024;
    if (maxRAM < 0)
      throw new ConfigurationException(GlobalConfiguration.MAX_PAGE_RAM.getKey() + " configuration is invalid (" + maxRAM + " MB)");

    flushThread = new PageManagerFlushThread(this, configuration);
    flushThread.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
  }

  public void close() {
    if (flushThread != null) {
      try {
        flushThread.closeAndJoin();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void removeAllPagesOfDatabase(final Database database) {
    for (final Iterator<CachedPage> it = readCache.values().iterator(); it.hasNext(); ) {
      final CachedPage p = it.next();
      final PageId pageId = p.getPageId();
      if (pageId.getDatabase().equals(database)) {
        totalReadCacheRAM.addAndGet(-1L * p.getPhysicalSize());
        it.remove();
      }
    }
  }

  public void flushAllPagesOfDatabase(final Database database) {
    if (flushThread != null)
      flushThread.flushAllPagesOfDatabase(database);
    removeAllPagesOfDatabase(database);
  }

  public void suspendFlushAndExecute(final Database database, final CallableNoReturn callback)
      throws IOException, InterruptedException {
    if (flushThread.setSuspended(database, true)) {
      flushThread.flushPagesFromQueueToDisk(database, 0L);
      try {
        CodeUtils.executeIgnoringExceptions(callback, "Error during suspend flush", true);
      } finally {
        flushThread.setSuspended(database, false);
      }
    }
  }

  public boolean isPageFlushingSuspended(final Database database) {
    return flushThread.isSuspended(database);
  }

  /**
   * Test only API.
   */
  public void simulateKillOfDatabase(final Database database) {
    removeAllPagesOfDatabase(database);
    if (flushThread != null)
      flushThread.removeAllPagesOfDatabase(database);
  }

  public void deleteFile(final Database database, final int fileId) {
    for (final Iterator<CachedPage> it = readCache.values().iterator(); it.hasNext(); ) {
      final CachedPage p = it.next();
      final PageId pageId = p.getPageId();
      if (pageId.getDatabase().equals(database) && pageId.getFileId() == fileId) {
        totalReadCacheRAM.addAndGet(-1L * p.getPhysicalSize());
        it.remove();
      }
    }
  }

  private int getMostRecentVersionOfPage(final PageId pageId, final int pageSize) throws IOException {
    CachedPage page = readCache.get(pageId);
    if (page == null)
      page = loadPage(pageId, pageSize, false, false);

    if (page != null)
      return page.getVersion();

    // NOT EXISTS, RETURN 0
    return 0;
  }

  public ImmutablePage getImmutablePage(final PageId pageId, final int pageSize, final boolean isNew,
      final boolean createIfNotExists) throws IOException {
    final CachedPage page = getCachedPage(pageId, pageSize, isNew, createIfNotExists);
    if (page != null)
      // RETURN ALWAYS A VIEW OF THE PAGE. THIS PREVENTS CONCURRENCY ON THE BUFFER POSITION
      return page.useAsImmutable();
    return null;
  }

  public MutablePage getMutablePage(final PageId pageId, final int pageSize, final boolean isNew, final boolean createIfNotExists)
      throws IOException {
    final CachedPage page = getCachedPage(pageId, pageSize, isNew, createIfNotExists);
    if (page != null)
      // RETURN ALWAYS A VIEW OF THE PAGE. THIS PREVENT CONCURRENCY ON THE BUFFER POSITION
      return page.useAsMutable();
    return null;
  }

  public void checkPageVersion(final MutablePage page, final boolean isNew) throws IOException {
    final PageId pageId = page.getPageId();

    final FileManager fileManager = ((DatabaseInternal) pageId.getDatabase()).getFileManager();

    if (!fileManager.existsFile(pageId.getFileId()))
      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + ". The file with id " + pageId.getFileId()
              + " does not exist anymore. Please retry the operation (threadId=" + Thread.currentThread().getId() + ")");

    final int mostRecentPageVersion = getMostRecentVersionOfPage(pageId, page.getPhysicalSize());

    if (mostRecentPageVersion != page.getVersion()) {
      totalConcurrentModificationExceptions.incrementAndGet();

      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + " in file '" + fileManager.getFile(pageId.getFileId()).getFileName()
              + "' (current v." + page.getVersion() + " <> database v." + mostRecentPageVersion
              + "). Please retry the operation (threadId=" + Thread.currentThread().getId() + ")");
    }
  }

  public void updatePages(final Map<PageId, MutablePage> newPages, final Map<PageId, MutablePage> modifiedPages,
      final boolean asyncFlush) throws IOException, InterruptedException {
    lock();
    try {
      final List<MutablePage> pagesToWrite = new ArrayList<>((newPages != null ? newPages.size() : 0) + modifiedPages.size());

      if (newPages != null)
        for (final MutablePage p : newPages.values())
          pagesToWrite.add(updatePageVersion(p, true));

      for (final MutablePage p : modifiedPages.values())
        pagesToWrite.add(updatePageVersion(p, false));

      writePages(pagesToWrite, asyncFlush);

    } finally {
      unlock();
    }
  }

  public MutablePage updatePageVersion(final MutablePage page, final boolean isNew) throws IOException, InterruptedException {
    final PageId pageId = page.getPageId();

    final int mostRecentPageVersion = getMostRecentVersionOfPage(pageId, page.getPhysicalSize());
    if (mostRecentPageVersion != page.getVersion()) {
      totalConcurrentModificationExceptions.incrementAndGet();

      final FileManager fileManager = ((DatabaseInternal) pageId.getDatabase()).getFileManager();
      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + " in file '" + fileManager.getFile(pageId.getFileId()).getFileName()
              + "' (current v." + page.getVersion() + " <> database v." + mostRecentPageVersion
              + "). Please retry the operation (threadId=" + Thread.currentThread().getId() + ")");
    }

    page.incrementVersion();
    page.updateMetadata();

    LogManager.instance()
        .log(this, Level.FINE, "Updated page %s (size=%d records=%d threadId=%d)", null, page, page.getPhysicalSize(),
            page.readShort(0), Thread.currentThread().getId());

    return page;
  }

  public void overwritePage(final MutablePage page) throws IOException {
    readCache.remove(page.pageId);

    flushPage(page);

    LogManager.instance().log(this, Level.FINE, "Overwritten page %s (size=%d threadId=%d)", null, page, page.getPhysicalSize(),
        Thread.currentThread().getId());
  }

  public PPageManagerStats getStats() {
    final PPageManagerStats stats = new PPageManagerStats();
    stats.maxRAM = maxRAM;
    stats.readCacheRAM = totalReadCacheRAM.get();
    stats.readCachePages = readCache.size();
    stats.pagesRead = totalPagesRead.get();
    stats.pagesReadSize = totalPagesReadSize.get();
    stats.pagesWritten = totalPagesWritten.get();
    stats.pagesWrittenSize = totalPagesWrittenSize.get();
    stats.pageFlushQueueLength = flushThread.queue.size();
    stats.cacheHits = cacheHits.get();
    stats.cacheMiss = cacheMiss.get();
    stats.concurrentModificationExceptions = totalConcurrentModificationExceptions.get();
    stats.evictionRuns = evictionRuns.get();
    stats.pagesEvicted = pagesEvicted.get();
    return stats;
  }

  public void removePageFromCache(final PageId pageId) {
    final CachedPage page = readCache.remove(pageId);
    if (page != null)
      totalReadCacheRAM.addAndGet(-1L * page.getPhysicalSize());
  }

  public void writePages(final List<MutablePage> updatedPages, final boolean asyncFlush) throws IOException, InterruptedException {
    if (asyncFlush) {
      for (final MutablePage page : updatedPages)
        // SAVE A COPY OF THE PAGE IN CACHE BECAUSE IT WILL BE FLUSHED ASYNCHRONOUSLY
        putPageInReadCache(new CachedPage(page, true));

      // ASYNCHRONOUS FLUSH: ONLY IF NOT ALREADY IN THE QUEUE, ENQUEUE THE PAGE TO BE FLUSHED BY A SEPARATE THREAD
      flushThread.scheduleFlushOfPages(updatedPages);
    } else {
      // SYNCHRONOUS FLUSH
      for (final MutablePage page : updatedPages) {
        flushPage(page);
        // ADD THE PAGE IN TO READ CACHE. FROM THIS POINT THE PAGE IS NEVER MODIFIED, SO IT CAN BE CACHED
        putPageInReadCache(new CachedPage(page, false));
      }
    }
  }

  protected void flushPage(final MutablePage page) throws IOException {
    final DatabaseInternal database = (DatabaseInternal) page.getPageId().getDatabase();

    if (!database.isOpen()) {
      LogManager.instance().log(this, Level.SEVERE, "Cannot flush page %s because the database is closed", page);
      return;
    }

    final FileManager fileManager = database.getFileManager();
    if (fileManager.existsFile(page.pageId.getFileId())) {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getFile(page.pageId.getFileId());
      if (!file.isOpen())
        throw new DatabaseMetadataException("Cannot flush pages on disk because file '" + file.getFileName() + "' is closed");

      LogManager.instance()
          .log(this, Level.FINE, "Flushing page %s to disk (threadId=%d)...", null, page, Thread.currentThread().getId());

      // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES
      concurrentPageAccess(page.pageId, true, () -> {
        final int written = file.write(page);
        totalPagesWrittenSize.addAndGet(written);
      });

      totalPagesWritten.incrementAndGet();

      database.getTransactionManager().notifyPageFlushed(page);

    } else
      LogManager.instance()
          .log(this, Level.FINE, "Cannot flush page %s because the file has been dropped (threadId=%d)...", null, page,
              Thread.currentThread().getId());
  }

  private CachedPage loadPage(final PageId pageId, final int size, final boolean createIfNotExists, final boolean cache)
      throws IOException {
    final DatabaseInternal database = (DatabaseInternal) pageId.getDatabase();

    // ASSURE THE PAGE IS NOT IN THE FLUSHING QUEUE
    CachedPage page = flushThread.getCachedPageFromMutablePageInQueue(pageId);
    if (page == null) {
      final PaginatedComponentFile file = (PaginatedComponentFile) database.getFileManager().getFile(pageId.getFileId());

      final boolean isNewPage = pageId.getPageNumber() >= file.getTotalPages();
      if (!createIfNotExists && isNewPage)
        // AVOID CREATING AN EMPTY PAGE JUST TO CHECK THE VERSION
        return null;

      checkForPageDisposal();

      page = new CachedPage(this, pageId, size);

      if (!isNewPage) {
        // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES
        final CachedPage finalPage = page;
        concurrentPageAccess(pageId, false, () -> file.read(finalPage));
      }

      page.loadMetadata();

      LogManager.instance().log(this, Level.FINE, "Loaded page %s (threadId=%d)", null, page, Thread.currentThread().getId());
    }

    totalPagesRead.incrementAndGet();
    totalPagesReadSize.addAndGet(page.getPhysicalSize());

    if (cache)
      putPageInReadCache(page);

    return page;
  }

  private void concurrentPageAccess(final PageId pageId, final Boolean writeAccess, final ConcurrentPageAccessCallback callback)
      throws IOException {
    // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES
    while (!Thread.currentThread().isInterrupted()) {
      if (pendingFlushPages.putIfAbsent(pageId, writeAccess) == null)
        try {
          callback.access();
          break;
        } finally {
          pendingFlushPages.remove(pageId);
        }

      // WAIT AND RETRY
      Thread.yield();
    }
  }

  private void checkForPageDisposal() {
    final long now = System.currentTimeMillis();
    if (now - lastCheckForRAM < 100)
      return;

    final long totalRAM = totalReadCacheRAM.get();
    if (totalRAM < maxRAM)
      return;

    final long ramToFree = totalRAM * freePageRAM / 100;

    evictOldestPages(ramToFree, totalRAM);
  }

  private synchronized void evictOldestPages(final long ramToFree, final long totalRAM) {
    evictionRuns.incrementAndGet();

    LogManager.instance()
        .log(this, Level.FINE, "Reached max RAM for page cache. Freeing pages from cache (target=%d current=%d max=%d threadId=%d)",
            null, ramToFree, totalRAM, maxRAM, Thread.currentThread().getId());

    // GET THE <DISPOSE_PAGES_PER_CYCLE> OLDEST PAGES
    // ORDER PAGES BY LAST ACCESS + SIZE
    final TreeSet<CachedPage> pagesOrderedByAge = new TreeSet<>((o1, o2) -> {
      final int lastAccessed = Long.compare(o1.getLastAccessed(), o2.getLastAccessed());
      if (lastAccessed != 0)
        return lastAccessed;

      // SAME TIMESTAMP, CHECK THE PAGE SIZE: LARGER PAGE SHOULD BE REMOVED FIRST THAN OTHERS
      final int pageSize = -Long.compare(o1.getPhysicalSize(), o2.getPhysicalSize());
      if (pageSize != 0)
        return pageSize;

      return o1.getPageId().compareTo(o2.getPageId());
    });

    pagesOrderedByAge.addAll(readCache.values());

    // REMOVE OLDEST PAGES FROM RAM
    long freedRAM = 0;
    for (final CachedPage page : pagesOrderedByAge) {
      final CachedPage removedPage = readCache.remove(page.getPageId());
      if (removedPage != null) {
        freedRAM += page.getPhysicalSize();
        totalReadCacheRAM.addAndGet(-1L * page.getPhysicalSize());
        pagesEvicted.incrementAndGet();

        if (freedRAM > ramToFree)
          break;
      }
    }

    final long newTotalRAM = totalReadCacheRAM.get();

    LogManager.instance()
        .log(this, Level.FINE, "Freed %s RAM (current=%s max=%s threadId=%d)", null, FileUtils.getSizeAsString(freedRAM),
            FileUtils.getSizeAsString(newTotalRAM), FileUtils.getSizeAsString(maxRAM), Thread.currentThread().getId());

    if (newTotalRAM > maxRAM)
      LogManager.instance().log(this, Level.WARNING, "Cannot free pages in RAM (current=%s > max=%s threadId=%d)", null,
          FileUtils.getSizeAsString(newTotalRAM), FileUtils.getSizeAsString(maxRAM), Thread.currentThread().getId());

    lastCheckForRAM = System.currentTimeMillis();
  }

  private void putPageInReadCache(final CachedPage page) {
    if (readCache.put(page.getPageId(), page) == null)
      totalReadCacheRAM.addAndGet(page.getPhysicalSize());

    checkForPageDisposal();
  }

  private CachedPage getCachedPage(final PageId pageId, final int pageSize, final boolean isNew, final boolean createIfNotExists)
      throws IOException {
    checkForPageDisposal();

    CachedPage page = readCache.get(pageId);
    if (page == null) {
      page = loadPage(pageId, pageSize, createIfNotExists, true);
      if (page == null) {
        if (isNew)
          return null;
      } else
        return page;

      cacheMiss.incrementAndGet();

    } else {
      cacheHits.incrementAndGet();
      page.updateLastAccesses();
    }

    if (page == null)
      throw new IllegalArgumentException(
          "Page id '" + pageId + "' does not exist (threadId=" + Thread.currentThread().getId() + ")");

    return page;
  }
}
