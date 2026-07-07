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
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockContext;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.function.BiFunction;

/**
 * Manages pages from disk to RAM. Each page can have different size.
 */
public class PageManager extends LockContext {
  public static final PageManager INSTANCE = new PageManager();

  // Package-private (instead of private) so the white-box regression test for #4925/#4933 can assert on
  // the cache content and RAM accounting directly, without reflection.
  volatile ConcurrentMap<PageId, CachedPage> readCache;
  // MANAGE CONCURRENT ACCESS TO THE PAGES. THE VALUE IS TRUE FOR WRITE OPERATION AND FALSE FOR READ
  private final    ConcurrentMap<PageId, Boolean>    pendingFlushPages                     = new ConcurrentHashMap<>();
  private volatile long                               maxRAM;
  final            AtomicLong                        totalReadCacheRAM                     = new AtomicLong();
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
  // LIFECYCLE INVARIANT (#5070): flushThread and readCache are written only under LIFECYCLE_LOCK
  // during the 0->1 startup / 1->0 shutdown transitions, and read lock-free on the hot paths. That is safe
  // ONLY because a reader implies refcount > 0 (its database holds a reference), so no transition can run
  // concurrently. volatile: the barrier cost is negligible next to the
  // ConcurrentHashMap barriers already on these paths, and it removes the reliance on the external
  // database-publication happens-before for cross-thread visibility of the startup() writes.
  private volatile PageManagerFlushThread             flushThread;
  private volatile int                                freePageRAM;

  @ExcludeFromJacocoGeneratedReport
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
  }

  /**
   * #4927: the JVM-wide PageManager is REFCOUNTED. Every DatabaseFactory open/create acquires one reference
   * (starting the flush machinery on 0 -> 1) and every database close releases it (tearing down on 1 -> 0),
   * all under one global lock. The previous lifecycle keyed on ACTIVE_INSTANCES.isEmpty() was a
   * check-then-act racing across factory instances (open/create synchronize on the factory, the map is
   * static, and registration happens only AFTER database.open() completes): closing the last instance of
   * one database could null the flush thread under a database whose open was mid-flight (NPE on the first
   * cache miss, or scheduled pages never flushed), and two opens could both see the map empty and start two
   * flush threads, leaking one with queued pages.
   */
  private static final Object LIFECYCLE_LOCK = new Object();
  private       int    lifecycleRefCount = 0; // guarded by LIFECYCLE_LOCK

  /** Acquires one lifecycle reference; the flush machinery starts on the 0 -> 1 transition. Pair with {@link #release()}. */
  public void acquire() {
    synchronized (LIFECYCLE_LOCK) {
      if (++lifecycleRefCount == 1)
        try {
          startup();
        } catch (final RuntimeException | Error e) {
          // #5070: startup() can throw (e.g. ConfigurationException on a negative MAX_PAGE_RAM). The
          // increment must not survive it, or the counter is left at 1 with no flush thread and the NEXT
          // acquire sees 1 -> 2 and never starts one - a wedged manager the caller's catch cannot repair
          // (its release() would just decrement back to the same broken state at 1).
          lifecycleRefCount = 0;
          throw e;
        }
    }
  }

  /** Releases one lifecycle reference; the flush machinery is torn down on the 1 -> 0 transition. Over-releases are clamped. */
  public void release() {
    synchronized (LIFECYCLE_LOCK) {
      if (lifecycleRefCount == 0)
        // Over-release (e.g. a test calling close() directly followed by a paired release): clamp instead of
        // going negative and tearing down a manager a later acquire believes it owns.
        return;
      if (--lifecycleRefCount == 0)
        shutdown();
    }
  }

  /**
   * Declares that the configuration changed (used by the GlobalConfiguration PROFILE setter). Always a
   * no-op at runtime: with no database open the next {@link #acquire()} reads the fresh settings anyway
   * (and starting a flush thread with zero databases - the old behavior - leaked it until the next
   * lifecycle transition); with databases OPEN a live shutdown+startup swap would race the hot paths, which
   * read {@code flushThread}/{@code readCache} without the lifecycle lock (#5070) - so a live
   * profile change is refused loudly instead. Set the PROFILE before opening databases.
   */
  public void configure() {
    synchronized (LIFECYCLE_LOCK) {
      if (lifecycleRefCount > 0)
        LogManager.instance().log(this, Level.WARNING,
            "Configuration profile PARTIALLY applied: %d database(s) are open, so the page manager keeps its current sizing while the profile's other settings (async/HTTP threads, queue sizes) did change. Set the profile before opening databases for a consistent configuration",
            null, lifecycleRefCount);
    }
  }

  /**
   * Force teardown regardless of refcount (test/emergency API): the counter resets so the next acquire
   * starts fresh. ONLY safe when no database is open: a live database's eventual release() would decrement
   * the NEW manager started by a later acquire and tear it down under that newcomer (#5070).
   */
  public void close() {
    synchronized (LIFECYCLE_LOCK) {
      lifecycleRefCount = 0;
      shutdown();
    }
  }

  private void startup() {
    final ContextConfiguration configuration = new ContextConfiguration();
    this.freePageRAM = configuration.getValueAsInteger(GlobalConfiguration.FREE_PAGE_RAM);
    this.readCache = new ConcurrentHashMap<>(configuration.getValueAsInteger(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE));

    this.maxRAM = configuration.getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM) * 1024 * 1024;
    if (this.maxRAM < 0)
      throw new ConfigurationException(
          GlobalConfiguration.MAX_PAGE_RAM.getKey() + " configuration is invalid (" + (maxRAM / (1024 * 1024)) + " MB)");

    flushThread = new PageManagerFlushThread(this, configuration);
    flushThread.start();
  }

  /**
   * INVARIANT (#5070): runs under LIFECYCLE_LOCK and joins the flush thread - the flush thread must
   * therefore NEVER call acquire()/release()/configure()/close(), or this join becomes a deadlock (the
   * flush thread blocking on the lock this thread holds while waiting for it to exit). Verified true today.
   */
  private void shutdown() {
    if (flushThread != null) {
      try {
        flushThread.closeAndJoin();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        // Null out regardless of interrupt (#5070): a stale dead reference with refcount 0 was
        // harmless (the next startup() overwrites it) but inconsistent.
        flushThread = null;
      }
    }

    if (readCache != null)
      // close() is a reachable test/emergency API and may run before any startup() (#5070).
      readCache.clear();
    totalReadCacheRAM.set(0L);
  }

  public void removeAllReadPagesOfDatabase(final Database database) {
    for (final CachedPage p : readCache.values()) {
      final PageId pageId = p.getPageId();
      if (pageId.getDatabase().equals(database))
        // #4933: drive the accounting from the value ACTUALLY removed. Subtracting before it.remove()
        // double-subtracted a page that a concurrent evictOldestPages/removePageFromCache removed first,
        // driving totalReadCacheRAM negative and permanently disabling eviction (unbounded cache growth).
        removePageFromCache(pageId);
    }
  }

  /** @return true when everything reached the disk; false when the bounded wait gave up (see #4928). */
  public boolean waitAllPagesOfDatabaseAreFlushed(final Database database) {
    if (flushThread != null)
      return flushThread.waitAllPagesOfDatabaseAreFlushed(database);
    return true;
  }

  public void removeModifiedPagesOfDatabase(final Database database) {
    if (flushThread != null)
      flushThread.removeAllPagesOfDatabase(database);
  }

  public void suspendFlushAndExecute(final Database database, final CallableNoReturn callback)
      throws IOException, InterruptedException {
    // #5068: the suspension is REFCOUNTED, so every caller (backup, verify, HA snapshot serving, nested
    // scopes per #4958) owns its whole window even when the windows overlap on the same database: flushing
    // is resumed (and the deferred batches flushed) only when the LAST suspender exits. The wait for the
    // in-flight batch runs INSIDE the try so an interrupt during the wait still releases this caller's
    // reference; it is cheap for non-first suspenders (the flush thread is already parked deferring).
    flushThread.setSuspended(database, true);
    try {
      flushThread.waitForCurrentFlushToComplete(database);
      CodeUtils.executeIgnoringExceptions(callback, "Error during suspend flush", true);
    } finally {
      flushThread.setSuspended(database, false);
    }
  }

  public boolean isPageFlushingSuspended(final Database database) {
    return flushThread.isSuspended(database);
  }

  /**
   * Test only API.
   */
  public void simulateKillOfDatabase(final Database database) {
    removeAllReadPagesOfDatabase(database);
    if (flushThread != null)
      flushThread.removeAllPagesOfDatabase(database);
  }

  public void deleteFile(final Database database, final int fileId) {
    // Drain the async flush thread for this fileId first, otherwise its parked pages leak RAM and could be flushed to a dropped file.
    if (flushThread != null)
      flushThread.removeAllPagesOfFile(database, fileId);

    for (final CachedPage p : readCache.values()) {
      final PageId pageId = p.getPageId();
      if (pageId.getDatabase().equals(database) && pageId.getFileId() == fileId)
        // #4933: accounting driven by the value actually removed (see removeAllReadPagesOfDatabase).
        removePageFromCache(pageId);
    }
  }

  PageManagerFlushThread getFlushThread() {
    return flushThread;
  }

  private int getMostRecentVersionOfPage(final PageId pageId, final int pageSize) throws IOException {
    CachedPage page = readCache.get(pageId);
    if (page == null)
      page = loadPage(pageId, pageSize, false, true);

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
              + " does not exist anymore. Please retry the operation (threadId=" + Thread.currentThread().threadId() + ")");

    final int mostRecentPageVersion = getMostRecentVersionOfPage(pageId, page.getPhysicalSize());

    if (mostRecentPageVersion != page.getVersion()) {
      totalConcurrentModificationExceptions.incrementAndGet();

      if (page.getVersion() == 0 && mostRecentPageVersion > 1) {
        final ComponentFile file = fileManager.getFile(page.pageId.getFileId());

        // @TODO: TEMPORARY PATCH TO OVERCOME THE ISSUE OF PAGES NOT UPDATED IN THE FILE MANAGER
        final Component component = page.pageId.getDatabase().getSchema().getFileById(page.pageId.getFileId());
        if (component instanceof LocalBucket b) {
          final int realPages = (int) (file.getSize() / b.pageSize);
          try {
            if (realPages > b.pageCount.get()) {
              LogManager.instance().log(this, Level.SEVERE,
                  "New page %s cannot be written because already present in file '%s' with version %d. Updating page count (threadId=%d)",
                  page, file.getFileName(), mostRecentPageVersion, Thread.currentThread().threadId());

              b.updatePageCount(realPages);
            }
          } catch (ConcurrentModificationException e) {
            // IGNORE IT
          }
        }
      }

      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + " in file '" + fileManager.getFile(pageId.getFileId()).getFileName()
              + "' (current v." + page.getVersion() + " <> database v." + mostRecentPageVersion
              + "). Please retry the operation (threadId=" + Thread.currentThread().threadId() + ")");
    }
  }

  /**
   * Atomic convenience form of {@link #validateAndBumpVersions} + {@link #publishPages}. NOTE: since #4936
   * the engine commit path no longer calls this - commit2ndPhase invokes the two halves separately with the
   * WAL append in between, so validate+publish are NOT atomic under one lock there. Kept as public API for
   * embedded users and backward compatibility; new engine code should call the halves explicitly and state
   * what serializes the gap.
   */
  public void updatePages(final Map<PageId, MutablePage> newPages, final Map<PageId, MutablePage> modifiedPages,
      final boolean asyncFlush) throws IOException, InterruptedException {
    publishPages(validateAndBumpVersions(newPages, modifiedPages), newPages, asyncFlush);
  }

  /**
   * First half of {@link #updatePages}: validates every page against the most recent committed version and
   * bumps the (transaction-private) page versions, WITHOUT publishing anything. #4936: the commit calls this
   * BEFORE appending the transaction to the WAL, so the append is the point of no return - a WAL record can
   * only ever exist for a transaction that already passed validation. Any {@link ConcurrentModificationException}
   * fires before anything durable exists, so crash recovery can never partially replay an aborted transaction.
   * The bump only mutates the transaction's own MutablePage instances: if the WAL append still fails
   * (e.g. interrupt), the reset() discards them and nothing observable happened.
   */
  public List<MutablePage> validateAndBumpVersions(final Map<PageId, MutablePage> newPages,
      final Map<PageId, MutablePage> modifiedPages) throws IOException, InterruptedException {
    lock();
    try {
      final List<MutablePage> pagesToWrite = new ArrayList<>((newPages != null ? newPages.size() : 0) + modifiedPages.size());

      if (newPages != null)
        for (final MutablePage p : newPages.values())
          pagesToWrite.add(updatePageVersion(p, true));

      for (final MutablePage p : modifiedPages.values())
        pagesToWrite.add(updatePageVersion(p, false));

      return pagesToWrite;
    } finally {
      unlock();
    }
  }

  /**
   * Second half of {@link #updatePages}: publishes the validated pages (read cache + flush scheduling).
   * Runs AFTER the WAL append (#4936): from the caller's perspective the transaction is committed once the
   * WAL is durable, and a failure here leaves the WAL to replay the pages on recovery. Releasing the global
   * PageManager lock between the two halves is safe only because the caller serializes committers per page
   * by other means. Two regimes exist: on a leader (commit1stPhase(true)) the per-file commit locks are held
   * until reset(), so no other transaction can validate, bump or publish these pages in the gap; on an HA
   * follower (commit1stPhase(false)) lockedFiles is EMPTY and it is the single-threaded Raft apply in
   * RaftReplicatedDatabase that provides the serialization - do not rely on file locks being held there.
   */
  public void publishPages(final List<MutablePage> pagesToWrite, final Map<PageId, MutablePage> newPages,
      final boolean asyncFlush) throws IOException, InterruptedException {
    lock();
    try {
      // Write pages (and put in readCache) BEFORE updating pageCount, otherwise concurrent
      // transactions can observe pageCount > readCache state and treat the new page as a
      // pre-existing empty page (sparse-file semantics), allowing two records' chunk chains
      // to land on the same physical slot.
      writePages(pagesToWrite, asyncFlush);

      if (newPages != null)
        for (final MutablePage p : newPages.values()) {
          final PageId pid = p.getPageId();
          final PaginatedComponent component = (PaginatedComponent) ((DatabaseInternal) pid.getDatabase()).getSchema()
                  .getFileByIdIfExists(pid.getFileId());
          if (component != null)
            component.updatePageCount(pid.getPageNumber() + 1);
        }

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
      if (page.getVersion() == 0 && mostRecentPageVersion > 1) {
        LogManager.instance().log(this, Level.SEVERE,
            "Page %s is new and has version 0, but the file '%s' has been modified. Please retry the operation (threadId=%d)",
            null, page, fileManager.getFile(pageId.getFileId()).getFileName(), Thread.currentThread().threadId());
      }

      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + " in file '" + fileManager.getFile(pageId.getFileId()).getFileName()
              + "' (current v." + page.getVersion() + " <> database v." + mostRecentPageVersion
              + "). Please retry the operation (threadId=" + Thread.currentThread().threadId() + ")");
    }

    page.incrementVersion();
    page.updateMetadata();

    LogManager.instance()
        .log(this, Level.FINE, "Updated page %s (size=%d records=%d threadId=%d)", null, page, page.getPhysicalSize(),
            page.readShort(0), Thread.currentThread().threadId());

    return page;
  }

  public void overwritePage(final MutablePage page) throws IOException {
    readCache.remove(page.pageId);

    flushPage(page);

    LogManager.instance().log(this, Level.FINE, "Overwritten page %s (size=%d threadId=%d)", null, page, page.getPhysicalSize(),
        Thread.currentThread().threadId());
  }

  /**
   * Writes a page to disk holding the per-page I/O lock so concurrent readers never observe partially-written bytes. This is used by
   * {@link TransactionManager#applyChanges} during replicated/recovery replay, which writes pages directly outside the normal flush
   * path. Unlike {@link #flushPage}, it does not touch the read cache: the caller is responsible for evicting the page afterwards (via
   * {@link #removePageFromCache(PageId)}) so subsequent reads reload the new content. Evicting after the write (rather than before)
   * avoids a window where a concurrent reader could reload the stale on-disk version into the cache while the write is in flight.
   */
  public void writePageWithLock(final PaginatedComponentFile file, final MutablePage page) throws IOException {
    // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES (same interlock used by flushPage and loadPage)
    concurrentPageAccess(page.pageId, true, () -> {
      final int written = file.write(page);
      totalPagesWrittenSize.addAndGet(written);
    });
    totalPagesWritten.incrementAndGet();
  }

  public PPageManagerStats getStats() {
    final PPageManagerStats stats = new PPageManagerStats();
    stats.maxRAM = maxRAM;
    stats.readCacheRAM = totalReadCacheRAM.get();
    // readCache and flushThread are populated by startup() (the 0 -> 1 acquire transition), which is called on first DB
    // open. When no database has been opened yet (e.g. a profiler snapshot taken at server
    // startup) they're still null - report empty cache/queue rather than NPE.
    stats.readCachePages = readCache != null ? readCache.size() : 0;
    stats.pagesRead = totalPagesRead.get();
    stats.pagesReadSize = totalPagesReadSize.get();
    stats.pagesWritten = totalPagesWritten.get();
    stats.pagesWrittenSize = totalPagesWrittenSize.get();
    stats.pageFlushQueueLength = flushThread != null ? flushThread.queue.size() : 0;
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
        putPageInReadCache(new CachedPage(page, true));
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
    final int fileId = page.pageId.getFileId();

    if (fileManager.existsFile(fileId)) {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getFile(fileId);
      if (!file.isOpen())
        throw new DatabaseMetadataException("Cannot flush pages on disk because file '" + file.getFileName() + "' is closed");

      LogManager.instance()
          .log(this, Level.FINE, "Flushing page %s to disk (threadId=%d)...", null, page, Thread.currentThread().threadId());

      // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES
      concurrentPageAccess(page.pageId, true, () -> {
        final int written = file.write(page);
        totalPagesWrittenSize.addAndGet(written);
      });

      try {
        final PaginatedComponent component = (PaginatedComponent) database.getSchema().getFileByIdIfExists(fileId);
        if (component != null)
          component.updatePageCount(page.pageId.getPageNumber() + 1);

        totalPagesWritten.incrementAndGet();

        database.getTransactionManager().notifyPageFlushed(page);
      } catch (final DatabaseIsClosedException e) {
        // The database was closed concurrently after the isOpen() check above.
        // The page data has already been written to disk, so we can safely skip
        // the metadata updates.
      }

    } else {
      LogManager.instance()
          .log(this, Level.FINE, "Cannot flush page %s because the file has been dropped (threadId=%d)...", null, page,
              Thread.currentThread().threadId());
      // The page will never be flushed and its content is irrelevant (the file is gone): release its WAL
      // ack, or the stale pending count would make every later clean close preserve the WAL for nothing
      // (the close-time ack gate, #4928). takeWALFile makes the release exactly-once against the racing
      // dropped-file batch purge.
      final WALFile walFile = page.takeWALFile();
      if (walFile != null)
        walFile.notifyPageFlushed();
    }
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

      LogManager.instance().log(this, Level.FINE, "Loaded page %s (threadId=%d)", null, page, Thread.currentThread().threadId());
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
    while (true) {
      // Fail loud on interrupt instead of silently skipping the I/O (#4924). The interrupt is only checked
      // BEFORE acquiring the per-page slot, so an in-flight read/write is never torn. Returning here without
      // running the callback would let a caller cache a zero-filled page (loadPage) or ack a flush that never
      // reached disk via notifyPageFlushed (flushPage) - both silently lose committed data. The interrupt flag
      // is left set so upper layers still observe the cancellation.
      if (Thread.currentThread().isInterrupted())
        throw new InterruptedIOException("Interrupted while acquiring I/O lock for page " + pageId);

      if (pendingFlushPages.putIfAbsent(pageId, writeAccess) == null)
        try {
          callback.access();
          return;
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
            null, ramToFree, totalRAM, maxRAM, Thread.currentThread().threadId());

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
        // Account the entry ACTUALLY removed, not the TreeSet snapshot: the version-monotonic put can swap
        // a same-PageId entry for a different instance between the snapshot and this remove (#4925/#4933).
        freedRAM += removedPage.getPhysicalSize();
        totalReadCacheRAM.addAndGet(-1L * removedPage.getPhysicalSize());
        pagesEvicted.incrementAndGet();

        if (freedRAM > ramToFree)
          break;
      }
    }

    final long newTotalRAM = totalReadCacheRAM.get();

    LogManager.instance()
        .log(this, Level.FINE, "Freed %s RAM (current=%s max=%s threadId=%d)", null, FileUtils.getSizeAsString(freedRAM),
            FileUtils.getSizeAsString(newTotalRAM), FileUtils.getSizeAsString(maxRAM), Thread.currentThread().threadId());

    if (newTotalRAM > maxRAM)
      LogManager.instance().log(this, Level.WARNING, "Cannot free pages in RAM (current=%s > max=%s threadId=%d)", null,
          FileUtils.getSizeAsString(newTotalRAM), FileUtils.getSizeAsString(maxRAM), Thread.currentThread().threadId());

    lastCheckForRAM = System.currentTimeMillis();
  }

  // Reused per-thread slot for the RAM delta decided inside VERSION_MONOTONIC_MERGE: keeps the put hot
  // path allocation-free (a capturing lambda + holder array per call would rely on escape analysis that is
  // not guaranteed under a megamorphic merge call site).
  private static final ThreadLocal<long[]> PUT_RAM_DELTA = ThreadLocal.withInitial(() -> new long[1]);

  // #4925: version-monotonic merge. Keeps whichever version is newer; an EQUAL version replaces on purpose
  // (identical content, freshest instance, zero RAM delta). Static and non-capturing: merge() hands the new
  // page in as the second argument, so this function allocates nothing per call.
  private static final BiFunction<CachedPage, CachedPage, CachedPage> VERSION_MONOTONIC_MERGE = (prev, cur) -> {
    if (cur.getVersion() >= prev.getVersion()) {
      PUT_RAM_DELTA.get()[0] = cur.getPhysicalSize() - prev.getPhysicalSize();
      return cur;
    }
    // STALE WRITE ATTEMPT: KEEP THE NEWER CACHED VERSION, NO ACCOUNTING CHANGE
    PUT_RAM_DELTA.get()[0] = 0;
    return prev;
  };

  void putPageInReadCache(final CachedPage page) {
    // #4925: version-monotonic put. A reader that started a disk read of version N before a committer
    // cached version N+1 must not overwrite the newer committed page with its stale image: the poisoned
    // cache would serve vN to every subsequent reader AND to the commit-time version probe, letting a later
    // transaction pass its MVCC check and silently overwrite the lost committed update. The RAM delta is
    // decided inside the same atomic merge so the accounting always matches the actual cache content.
    final long[] ramDelta = PUT_RAM_DELTA.get();
    // Default covers the absent-key case: merge() inserts the page WITHOUT invoking the remapping function.
    ramDelta[0] = page.getPhysicalSize();
    readCache.merge(page.getPageId(), page, VERSION_MONOTONIC_MERGE);
    if (ramDelta[0] != 0)
      totalReadCacheRAM.addAndGet(ramDelta[0]);

    checkForPageDisposal();
  }

  private CachedPage getCachedPage(final PageId pageId, final int pageSize, final boolean isNew, final boolean createIfNotExists)
      throws IOException {
    checkForPageDisposal();

    CachedPage page = readCache.get(pageId);
    if (page == null) {
      // #4958: count the miss BEFORE returning the freshly loaded page. The counter used to be bumped
      // only on the page-not-found fall-through below, so cacheMiss stayed at ~0 forever and the
      // hit/miss ratio in the stats was meaningless.
      cacheMiss.incrementAndGet();

      page = loadPage(pageId, pageSize, createIfNotExists, true);
      if (page == null) {
        if (isNew)
          return null;
      } else
        return page;

    } else {
      cacheHits.incrementAndGet();
      page.updateLastAccesses();
    }

    if (page == null)
      throw new IllegalArgumentException(
          "Page id '" + pageId + "' does not exist (threadId=" + Thread.currentThread().threadId() + ")");

    return page;
  }
}
