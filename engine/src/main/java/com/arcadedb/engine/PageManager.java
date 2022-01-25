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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockContext;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Manages pages from disk to RAM. Each page can have different size.
 */
public class PageManager extends LockContext {
  private final FileManager                          fileManager;
  private final ConcurrentMap<PageId, ImmutablePage> readCache;
  private final ConcurrentMap<PageId, MutablePage>   writeCache;
  private final TransactionManager                   txManager;
  private       boolean                              flushOnlyAtClose;
  // MANAGE CONCURRENT ACCESS TO THE PAGES. THE VALUE IS TRUE FOR WRITE OPERATION AND FALSE FOR READ
  private final ConcurrentMap<PageId, Boolean>       pendingFlushPages                     = new ConcurrentHashMap<>();
  private final long                                 maxRAM;
  private final AtomicLong                           totalReadCacheRAM                     = new AtomicLong();
  private final AtomicLong                           totalWriteCacheRAM                    = new AtomicLong();
  private final AtomicLong                           totalPagesRead                        = new AtomicLong();
  private final AtomicLong                           totalPagesReadSize                    = new AtomicLong();
  private final AtomicLong                           totalPagesWritten                     = new AtomicLong();
  private final AtomicLong                           totalPagesWrittenSize                 = new AtomicLong();
  private final AtomicLong                           cacheHits                             = new AtomicLong();
  private final AtomicLong                           cacheMiss                             = new AtomicLong();
  private final AtomicLong                           totalConcurrentModificationExceptions = new AtomicLong();
  private final AtomicLong                           evictionRuns                          = new AtomicLong();
  private final AtomicLong                           pagesEvicted                          = new AtomicLong();
  private       long                                 lastCheckForRAM                       = 0;
  private final PageManagerFlushThread               flushThread;
  private final int                                  freePageRAM;
  private final AtomicBoolean                        flushPagesToDisk                      = new AtomicBoolean(true);

  public interface ConcurrentPageAccessCallback {
    void access() throws IOException;
  }

  public static class PPageManagerStats {
    public long maxRAM;
    public long readCacheRAM;
    public long writeCacheRAM;
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
    public int  writeCachePages;
  }

  public PageManager(final FileManager fileManager, final TransactionManager txManager, final ContextConfiguration configuration) {
    this.fileManager = fileManager;
    this.txManager = txManager;

    this.freePageRAM = configuration.getValueAsInteger(GlobalConfiguration.FREE_PAGE_RAM);
    this.readCache = new ConcurrentHashMap<>(configuration.getValueAsInteger(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE));
    this.writeCache = new ConcurrentHashMap<>(configuration.getValueAsInteger(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE));

    this.flushOnlyAtClose = configuration.getValueAsBoolean(GlobalConfiguration.FLUSH_ONLY_AT_CLOSE);

    maxRAM = configuration.getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM) * 1024 * 1024;
    if (maxRAM < 0)
      throw new ConfigurationException(GlobalConfiguration.MAX_PAGE_RAM.getKey() + " configuration is invalid (" + maxRAM + " MB)");

    flushThread = new PageManagerFlushThread(this, configuration);
    flushThread.start();
  }

  public void close() {
    if (flushThread != null) {
      try {
        flushThread.close();
        flushThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // FLUSH REMAINING PAGES
    final boolean flushOnlyAtCloseOld = flushOnlyAtClose;
    flushOnlyAtClose = true;
    for (MutablePage p : writeCache.values()) {
      try {
        flushPage(p);
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on flushing page %s at closing (threadId=%d)", e, p, Thread.currentThread().getId());
      }
    }
    writeCache.clear();
    readCache.clear();
    totalReadCacheRAM.set(0);
    totalWriteCacheRAM.set(0);

    flushOnlyAtClose = flushOnlyAtCloseOld;
  }

  public void suspendPageFlushing(final boolean value) {
    flushThread.setSuspended(value);
  }

  public boolean isPageFlushingSuspended() {
    return flushThread.isSuspended();
  }

  /**
   * Test only API.
   */
  public void kill() {
    if (flushThread != null) {
      try {
        flushThread.close();
        flushThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    writeCache.clear();
    readCache.clear();
    totalReadCacheRAM.set(0);
    totalWriteCacheRAM.set(0);
  }

  public void clear() {
    readCache.clear();
    totalReadCacheRAM.set(0);
  }

  public void deleteFile(final int fileId) {
    // REMOVE WRITE CACHE 1ST TO REDUCE THE CHANCE TO FLUSH PAGES FOR THE FILE TO DROP
    for (Iterator<MutablePage> it = writeCache.values().iterator(); it.hasNext(); ) {
      final MutablePage p = it.next();
      if (p.getPageId().getFileId() == fileId) {
        totalWriteCacheRAM.addAndGet(-1L * p.getPhysicalSize());
        it.remove();
      }
    }

    for (Iterator<ImmutablePage> it = readCache.values().iterator(); it.hasNext(); ) {
      final ImmutablePage p = it.next();
      if (p.getPageId().getFileId() == fileId) {
        totalReadCacheRAM.addAndGet(-1L * p.getPhysicalSize());
        it.remove();
      }
    }
  }

  public BasePage getPage(final PageId pageId, final int pageSize, final boolean isNew, final boolean createIfNotExists) throws IOException {
    checkForPageDisposal();

    BasePage page = writeCache.get(pageId);
    if (page != null)
      cacheHits.incrementAndGet();
    else {
      page = readCache.get(pageId);
      if (page == null) {
        page = loadPage(pageId, pageSize, createIfNotExists);
        if (isNew)
          return page;

        cacheMiss.incrementAndGet();

      } else {
        cacheHits.incrementAndGet();
        page.updateLastAccesses();
      }

      if (page == null)
        throw new IllegalArgumentException("Page id '" + pageId + "' does not exists (threadId=" + Thread.currentThread().getId() + ")");
    }

    return page;
  }

  public void checkPageVersion(final MutablePage page, final boolean isNew) throws IOException {
    final PageId pageId = page.getPageId();

    if (!fileManager.existsFile(pageId.getFileId()))
      throw new ConcurrentModificationException("Concurrent modification on page " + pageId + " file with id " + pageId.getFileId()
          + " does not exists anymore. Please retry the operation (threadId=" + Thread.currentThread().getId() + ")");

    final BasePage mostRecentPage = getPage(pageId, page.getPhysicalSize(), isNew, false);

    if (mostRecentPage != null && mostRecentPage.getVersion() != page.getVersion()) {
      totalConcurrentModificationExceptions.incrementAndGet();

      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + " in file '" + fileManager.getFile(pageId.getFileId()).getFileName() + "' (current v."
              + page.getVersion() + " <> database v." + mostRecentPage.getVersion() + "). Please retry the operation (threadId=" + Thread.currentThread()
              .getId() + ")");
    }
  }

  public void updatePages(final Map<PageId, MutablePage> newPages, final Map<PageId, MutablePage> modifiedPages, final boolean asyncFlush)
      throws IOException, InterruptedException {
    lock();
    try {
      final List<MutablePage> pagesToFlush = new ArrayList<>((newPages != null ? newPages.size() : 0) + modifiedPages.size());

      if (newPages != null)
        for (MutablePage p : newPages.values())
          pagesToFlush.add(updatePage(p, true));

      for (MutablePage p : modifiedPages.values())
        pagesToFlush.add(updatePage(p, false));

      flushPages(pagesToFlush, asyncFlush);

    } finally {
      unlock();
    }
  }

  public MutablePage updatePage(final MutablePage page, final boolean isNew) throws IOException, InterruptedException {
    final PageId pageId = page.getPageId();

    final BasePage mostRecentPage = getPage(pageId, page.getPhysicalSize(), isNew, true);
    if (mostRecentPage != null) {
      if (mostRecentPage.getVersion() != page.getVersion()) {
        totalConcurrentModificationExceptions.incrementAndGet();
        throw new ConcurrentModificationException(
            "Concurrent modification on page " + pageId + " in file '" + fileManager.getFile(pageId.getFileId()).getFileName() + "' (current v."
                + page.getVersion() + " <> database v." + mostRecentPage.getVersion() + "). Please retry the operation (threadId=" + Thread.currentThread()
                .getId() + ")");
      }

      page.incrementVersion();
      page.updateMetadata();

      LogManager.instance().log(this, Level.FINE, "Updated page %s (size=%d threadId=%d)", null, page, page.getPhysicalSize(), Thread.currentThread().getId());
    }
    return page;
  }

  public void overwritePage(final MutablePage page) throws IOException {
    readCache.remove(page.pageId);

    // ADD THE PAGE IN TO WRITE CACHE. FROM THIS POINT THE PAGE IS NEVER MODIFIED DIRECTLY, SO IT CAN BE SHARED
    if (writeCache.put(page.pageId, page) == null)
      totalWriteCacheRAM.addAndGet(page.getPhysicalSize());

    flushPage(page);

    LogManager.instance()
        .log(this, Level.FINE, "Overwritten page %s (size=%d threadId=%d)", null, page, page.getPhysicalSize(), Thread.currentThread().getId());
  }

  public PPageManagerStats getStats() {
    final PPageManagerStats stats = new PPageManagerStats();
    stats.maxRAM = maxRAM;
    stats.readCacheRAM = totalReadCacheRAM.get();
    stats.writeCacheRAM = totalWriteCacheRAM.get();
    stats.readCachePages = readCache.size();
    stats.writeCachePages = writeCache.size();
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
    final ImmutablePage page = readCache.remove(pageId);
    if (page != null)
      totalReadCacheRAM.addAndGet(-1L * page.getPhysicalSize());

    final MutablePage page2 = writeCache.remove(pageId);
    if (page2 != null)
      totalWriteCacheRAM.addAndGet(-1L * page2.getPhysicalSize());
  }

//  public void preloadFile(final int fileId) {
//    LogManager.instance().log(this, Level.FINE, "Pre-loading file %d (threadId=%d)...", null, fileId, Thread.currentThread().getId());
//
//    try {
//      final PaginatedFile file = fileManager.getFile(fileId);
//      final int pageSize = file.getPageSize();
//      final int pages = (int) (file.getSize() / pageSize);
//
//      for (int pageNumber = 0; pageNumber < pages; ++pageNumber)
//        loadPage(new PageId(fileId, pageNumber), pageSize, false);
//
//    } catch (IOException e) {
//      throw new DatabaseMetadataException("Cannot load file in RAM", e);
//    }
//  }

  public void flushPages(final List<MutablePage> updatedPages, final boolean asyncFlush) throws IOException, InterruptedException {
    for (MutablePage page : updatedPages) {
      // ADD THE PAGE IN TO WRITE CACHE. FROM THIS POINT THE PAGE IS NEVER MODIFIED DIRECTLY, SO IT CAN BE SHARED
      if (writeCache.put(page.pageId, page) == null)
        totalWriteCacheRAM.addAndGet(page.getPhysicalSize());
    }

    if (asyncFlush) {
      // ASYNCHRONOUS FLUSH
      if (!flushOnlyAtClose)
        // ONLY IF NOT ALREADY IN THE QUEUE, ENQUEUE THE PAGE TO BE FLUSHED BY A SEPARATE THREAD
        flushThread.scheduleFlushOfPages(updatedPages);
    } else {
      // SYNCHRONOUS FLUSH
      for (MutablePage page : updatedPages)
        flushPage(page);
    }
  }

  protected void flushPage(final MutablePage page) throws IOException {
    try {
      if (fileManager.existsFile(page.pageId.getFileId())) {
        final PaginatedFile file = fileManager.getFile(page.pageId.getFileId());
        if (!file.isOpen())
          throw new DatabaseMetadataException("Cannot flush pages on disk because file '" + file.getFileName() + "' is closed");

        LogManager.instance().log(this, Level.FINE, "Flushing page %s to disk (threadId=%d)...", null, page, Thread.currentThread().getId());

        if (!flushOnlyAtClose) {
          putPageInCache(page.createImmutableView());

          // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES
          concurrentPageAccess(page.pageId, true, () -> {
            int written = file.write(page);
            totalPagesWrittenSize.addAndGet(written);
          });

          totalPagesWritten.incrementAndGet();
        }
      } else
        LogManager.instance()
            .log(this, Level.FINE, "Cannot flush page %s because the file has been dropped (threadId=%d)...", null, page, Thread.currentThread().getId());

    } finally {
      // DELETE ONLY CURRENT VERSION OF THE PAGE (THIS PREVENT TO REMOVE NEWER PAGES)
      if (writeCache.remove(page.pageId, page))
        totalWriteCacheRAM.addAndGet(-1L * page.getPhysicalSize());

      txManager.notifyPageFlushed(page);
    }
  }

  private ImmutablePage loadPage(final PageId pageId, final int size, boolean createIfNotExists) throws IOException {
    final PaginatedFile file = fileManager.getFile(pageId.getFileId());

    if (!createIfNotExists && pageId.getPageNumber() >= file.getTotalPages())
      // AVOID CREATING AN EMPTY PAGE JUST TO CHECK THE VERSION
      return null;

    checkForPageDisposal();

    final ImmutablePage page = new ImmutablePage(this, pageId, size);

    // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES
    concurrentPageAccess(pageId, false, () -> file.read(page));

    page.loadMetadata();

    LogManager.instance().log(this, Level.FINE, "Loaded page %s (threadId=%d)", null, page, Thread.currentThread().getId());

    totalPagesRead.incrementAndGet();
    totalPagesReadSize.addAndGet(page.getPhysicalSize());

    putPageInCache(page);

    return page;
  }

  private void concurrentPageAccess(final PageId pageId, final Boolean writeAccess, final ConcurrentPageAccessCallback callback) throws IOException {
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

  private synchronized void checkForPageDisposal() {
    final long now = System.currentTimeMillis();
    if (now - lastCheckForRAM < 100)
      return;

    final long totalRAM = totalReadCacheRAM.get();

    if (totalRAM < maxRAM)
      return;

    final long ramToFree = totalRAM * freePageRAM / 100;

    evictionRuns.incrementAndGet();

    LogManager.instance()
        .log(this, Level.FINE, "Reached max RAM for page cache. Freeing pages from cache (target=%d current=%d max=%d threadId=%d)", null, ramToFree, totalRAM,
            maxRAM, Thread.currentThread().getId());

    // GET THE <DISPOSE_PAGES_PER_CYCLE> OLDEST PAGES
    // ORDER PAGES BY LAST ACCESS + SIZE
    long oldestPagesRAM = 0;
    final TreeSet<BasePage> oldestPages = new TreeSet<>((o1, o2) -> {
      final int lastAccessed = Long.compare(o1.getLastAccessed(), o2.getLastAccessed());
      if (lastAccessed != 0)
        return lastAccessed;

      final int pageSize = Long.compare(o1.getPhysicalSize(), o2.getPhysicalSize());
      if (pageSize != 0)
        return pageSize;

      return o1.getPageId().compareTo(o2.getPageId());
    });

    for (ImmutablePage page : readCache.values()) {
      if (oldestPagesRAM < ramToFree) {
        // FILL FIRST PAGES
        oldestPages.add(page);
        oldestPagesRAM += page.getPhysicalSize();
      } else {
        if (page.getLastAccessed() < oldestPages.last().getLastAccessed()) {
          oldestPages.add(page);
          oldestPagesRAM += page.getPhysicalSize();

          // REMOVE THE LESS OLD
          final Iterator<BasePage> it = oldestPages.iterator();
          final BasePage pageToRemove = it.next();
          oldestPagesRAM -= pageToRemove.getPhysicalSize();
          it.remove();
        }
      }
    }

    // REMOVE OLDEST PAGES FROM RAM
    long freedRAM = 0;
    for (BasePage page : oldestPages) {
      if (page instanceof ImmutablePage) {
        final ImmutablePage removedPage = readCache.remove(page.pageId);
        if (removedPage != null) {
          freedRAM += page.getPhysicalSize();
          totalReadCacheRAM.addAndGet(-1L * page.getPhysicalSize());
          pagesEvicted.incrementAndGet();

          if (freedRAM > ramToFree)
            break;
        }
      }
    }

    final long newTotalRAM = totalReadCacheRAM.get();

    LogManager.instance().log(this, Level.FINE, "Freed %s RAM (current=%s max=%s threadId=%d)", null, FileUtils.getSizeAsString(freedRAM),
        FileUtils.getSizeAsString(newTotalRAM), FileUtils.getSizeAsString(maxRAM), Thread.currentThread().getId());

    if (newTotalRAM > maxRAM)
      LogManager.instance().log(this, Level.WARNING, "Cannot free pages in RAM (current=%s > max=%s threadId=%d)", null, FileUtils.getSizeAsString(newTotalRAM),
          FileUtils.getSizeAsString(maxRAM), Thread.currentThread().getId());

    lastCheckForRAM = System.currentTimeMillis();
  }

  private void putPageInCache(final ImmutablePage page) {
    if (readCache.put(page.pageId, page) == null)
      totalReadCacheRAM.addAndGet(page.getPhysicalSize());

    checkForPageDisposal();
  }
}
