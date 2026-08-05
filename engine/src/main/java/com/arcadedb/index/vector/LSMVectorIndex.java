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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexFactoryHandler;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.BucketLSMVectorIndexBuilder;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.LockManager;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RidHashSet;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.quantization.MutablePQVectors;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.io.File;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * Vector index implementation using JVector library with page-based transactional storage.
 * This implementation stores vector data on disk using ArcadeDB's page system for transactional support.
 * Unlike HNSW which uses graph vertices/edges, this stores vectors directly in pages and maintains
 * the graph structure separately for better performance and transactional integrity.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndex implements Index, IndexInternal {
  public static final  String            FILE_EXT        = "lsmvecidx";
  public static final  int               CURRENT_VERSION = 0;
  public static final  int               DEF_PAGE_SIZE   = 262_144;
  private static final VectorTypeSupport vts             = VectorizationProvider.getInstance().getVectorTypeSupport();

  // JVM-wide semaphore limiting the number of concurrent async graph rebuilds across all indexes
  // and databases.  Multiple concurrent rebuilds are extremely memory-intensive and can cause OOM
  // kills (issue #3868).  The permit count is read once at class-load time from the configuration.
  private static final Semaphore REBUILD_SEMAPHORE = new Semaphore(
      GlobalConfiguration.VECTOR_INDEX_MAX_CONCURRENT_REBUILDS.getValueAsInteger());

  // Page header layout constants
  public static final int OFFSET_FREE_CONTENT = 0;  // 4 bytes
  public static final int OFFSET_NUM_ENTRIES  = 4;   // 4 bytes
  public static final int OFFSET_MUTABLE      = 8;       // 1 byte
  public static final int HEADER_BASE_SIZE    = 9;     // offsetFreeContent(4) + numberOfEntries(4) + mutable(1)

  // Floor for the automatically sized search cache (issue #5412). At 768 dimensions this is ~25MB, small
  // enough to never matter and large enough that a small index is fully resident from the first query.
  private static final int MIN_SEARCH_CACHE_SIZE = 8_192;

  // DOT_PRODUCT is the fast path for cosine on already-normalized data, and JVector documents unit length as its
  // precondition. How many ordinals a rebuild samples to notice that the precondition is not met, and how far a
  // magnitude may drift from 1 before it counts as violated (float accumulation over a few thousand dimensions
  // moves the last digits, so an exact comparison would flag correctly normalized data).
  private static final int    UNIT_VECTOR_SAMPLE_SIZE  = 1_000;
  private static final double UNIT_MAGNITUDE_TOLERANCE = 0.01;
  // Squared bounds, so the sample costs one multiply-add per component and no square root at all.
  private static final double MIN_UNIT_MAGNITUDE_SQUARED = (1.0 - UNIT_MAGNITUDE_TOLERANCE) * (1.0 - UNIT_MAGNITUDE_TOLERANCE);
  private static final double MAX_UNIT_MAGNITUDE_SQUARED = (1.0 + UNIT_MAGNITUDE_TOLERANCE) * (1.0 + UNIT_MAGNITUDE_TOLERANCE);

  // Not final: a compaction swaps in a new data file, and this index is named after its component - see
  // getMostRecentFileName(). Every node names the index after the file it holds, so the leader has to
  // follow its own rename or its schema stops matching the followers that rebuilt it from that file.
  private volatile String              indexName;
  protected     LSMVectorIndexMutable  mutable;
  private final ReentrantReadWriteLock lock;
  LSMVectorIndexMetadata metadata; // Package-private for Phase 2 access from ArcadePageVectorValues and
  // LSMVectorIndexGraphFile

  // Graph lifecycle management (Phase 2: Disk-based graph storage)
  enum GraphState {
    LOADING,    // No graph available yet (initial state during startup)
    IMMUTABLE,  // OnDiskGraphIndex - lazy-loaded from disk, optimized for searches
    MUTABLE     // OnHeapGraphIndex - in memory, accepting incremental updates
  }

  // Index build state (for crash recovery and WAL bypass safety)
  public enum BUILD_STATE {
    BUILDING,  // Index is being created/rebuilt with WAL disabled
    READY,     // Index is complete and ready for use
    INVALID    // Build was interrupted by crash, needs manual REBUILD INDEX
  }

  private volatile GraphState                    graphState;
  private volatile ImmutableGraphIndex           graphIndex;        // Current graph (OnHeap or OnDisk)
  private volatile int[]                         ordinalToVectorId; // Maps graph ordinals to vector IDs
  // Lightweight pointer index. Volatile and swapped as a whole (never cleared and refilled in place) so the
  // readers that take no lock - countEntries() and getStats() - always see a complete location set instead of a
  // rebuild in progress (issue #5568). Everything else reaches it through lock.readLock().
  private volatile VectorLocationIndex           vectorIndex;
  private final    AtomicInteger                 nextId;
  private final    AtomicReference<INDEX_STATUS> status;
  // Set once the ignored location-cache limit has been reported, so a rebuild does not repeat the warning.
  // compareAndSet, not a plain flag: two threads racing the first call would otherwise both log it.
  private final    AtomicBoolean                 locationCacheCapReported = new AtomicBoolean();
  // Same idea for the DOT_PRODUCT unit-length warning: an ingest crosses the rebuild threshold every time the
  // pending set grows by a fraction of the graph, so a million-row load rebuilds a hundred-odd times and would
  // otherwise repeat the same warning on each of them. Set only when the warning is actually emitted, so an index
  // that starts out normalized and later is not still gets told once.
  private final    AtomicBoolean                 nonUnitVectorsReported   = new AtomicBoolean();

  // Graph file for persistent storage of graph topology
  // Allows lazy-loading graph from disk and avoiding expensive rebuilds
  private       LSMVectorIndexGraphFile graphFile;
  private final AtomicInteger           mutationsSinceSerialize;

  // Product Quantization (PQ) for zero-disk-I/O approximate search
  // PQ file stores codebooks and encoded vectors; pqVectors is cached in memory
  private          LSMVectorIndexPQFile pqFile;
  private volatile PQVectors            pqVectors;
  private volatile ProductQuantization  productQuantization;

  // Serializes graph builds for this index (issue #5391). Held only by builders, never by readers or writers.
  private final ReentrantLock graphBuildLock = new ReentrantLock();

  // Index-scoped cache of materialized vectors, shared by every search on this index (issue #5412).
  // Beam search resolves one vector per distance evaluation; before this cache lived at index scope, each
  // query built its own 1024-entry map and discarded it on completion, so every query paid a full record
  // read (or quantized page read) per hop even when repeating the very same query.
  private volatile VectorCache searchVectorCache;
  private final    Object      searchVectorCacheLock = new Object();

  // Index-scoped pool of JVector graph searchers, reused across queries (issue #5413). Allocating a searcher per
  // query made its scratch state (the growable candidate heap above all) the largest single source of garbage on
  // a dense-search workload, and the young-GC frequency that produced was what set the query tail latency.
  private volatile GraphSearcherPool searcherPool;
  private final    Object            searcherPoolLock = new Object();

  // Async graph rebuild support (issue #3679): when mutations reach the configured threshold,
  // the graph is rebuilt in a background daemon thread and hot-swapped when ready.
  // This prevents vectorNeighbors queries from blocking for minutes on large indexes.
  private volatile Thread  asyncRebuildThread     = null;
  private volatile boolean asyncRebuildInProgress = false;
  // Incremented each time a graph build snapshots its start mutation counter. Lets callers (and tests) observe
  // that a build has passed the point after which further mutations are preserved rather than folded into the
  // build's own snapshot (issue #3683).
  private volatile long    rebuildSnapshotGeneration = 0;

  // Dedicated ForkJoinPool for graph building, so we can shut it down on close() to cancel
  // long-running build operations that would otherwise block server shutdown.
  private volatile ForkJoinPool graphBuildPool;

  // Live incremental graph builder: inserts vectors one at a time via addGraphNode()
  // instead of rebuilding the entire graph. The builder stays alive across put() calls.
  // Search uses builder.getGraph() which is immediately searchable after each insert.
  private volatile GraphIndexBuilder liveBuilder;
  private volatile GrowableVectorValues liveVectorValues;

  // Delta vectors inserted since last graph build, cached in RAM for brute-force scan during search.
  // Writers (put/remove/rebuild) hold write lock; readers (search) take a volatile snapshot.
  private static final class DeltaVectorEntry {
    final int             vectorId;
    final RID             rid;
    // Stored already converted to the JVector representation: the delta buffer is scanned in full on every
    // search, so converting once at insert time (instead of once per entry per query) removes a per-query
    // allocation of the entire delta buffer - the dominant source of GC pressure at scale (issue #5391).
    final VectorFloat<?> vector;

    DeltaVectorEntry(final int vectorId, final RID rid, final VectorFloat<?> vector) {
      this.vectorId = vectorId;
      this.rid = rid;
      this.vector = vector;
    }
  }

  private volatile List<DeltaVectorEntry> deltaVectors = new ArrayList<>();

  // Inactivity rebuild timer (issue #3737): when mutations exist but haven't reached the threshold,
  // a timer triggers an async rebuild after a period of inactivity.
  private volatile TimerTask inactivityRebuildTask;
  private volatile Timer     inactivityTimer;

  // Compaction support
  private final    AtomicInteger           currentMutablePages;
  private final    int                     minPagesToScheduleACompaction;
  private          LSMVectorIndexCompacted compactedSubIndex;
  private volatile boolean                 valid      = true;
  private volatile BUILD_STATE             buildState = BUILD_STATE.READY;

  // Page tracking for inserts (avoids getTotalPages() issue with transaction-local pages)
  // Protected by write lock, reset to -1 after transaction commits or graph rebuilds
  private int currentInsertPageNum = -1;

  // Metrics tracking - package-private to allow access from ArcadePageVectorValues
  final LSMVectorIndexMetrics metrics = new LSMVectorIndexMetrics();

  public interface GraphBuildCallback {
    /**
     * Called periodically during graph index construction.
     *
     * @param phase          Current phase: "validating", "building", or "persisting"
     * @param processedNodes Number of unique nodes processed so far
     * @param totalNodes     Total number of nodes to process
     * @param vectorAccesses Total number of vector accesses (getVector calls)
     */
    void onGraphBuildProgress(String phase, int processedNodes, int totalNodes, long vectorAccesses);
  }

  private static final class GraphBuildDiagnosticsSnapshot {
    private final long   heapUsedBytes;
    private final long   heapMaxBytes;
    private final long   offHeapBytes;
    private final long   vectorIndexBytes;
    private final long   graphFileBytes;
    private final long   pqFileBytes;
    private final long   compactedFileBytes;
    private final String fileBreakdown;

    private GraphBuildDiagnosticsSnapshot(final long heapUsedBytes, final long heapMaxBytes, final long offHeapBytes,
        final long vectorIndexBytes, final long graphFileBytes,
        final long pqFileBytes, final long compactedFileBytes,
        final String fileBreakdown) {
      this.heapUsedBytes = heapUsedBytes;
      this.heapMaxBytes = heapMaxBytes;
      this.offHeapBytes = offHeapBytes;
      this.vectorIndexBytes = vectorIndexBytes;
      this.graphFileBytes = graphFileBytes;
      this.pqFileBytes = pqFileBytes;
      this.compactedFileBytes = compactedFileBytes;
      this.fileBreakdown = fileBreakdown;
    }

    private double heapUsedMb() {
      return heapUsedBytes / (1024.0 * 1024.0);
    }

    private double heapMaxMb() {
      return heapMaxBytes / (1024.0 * 1024.0);
    }

    private double offHeapMb() {
      return offHeapBytes / (1024.0 * 1024.0);
    }

    private double totalFilesMb() {
      final long total = vectorIndexBytes + graphFileBytes + pqFileBytes + compactedFileBytes;
      return total / (1024.0 * 1024.0);
    }
  }

  private boolean isGraphBuildDiagnosticsEnabled() {
    return getDatabase().getConfiguration().getValueAsBoolean(GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_DIAGNOSTICS);
  }

  private GraphBuildDiagnosticsSnapshot captureGraphBuildDiagnostics() {
    final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    final MemoryUsage heap = memoryBean != null ? memoryBean.getHeapMemoryUsage() : null;
    final long heapUsed = heap != null ? heap.getUsed() : 0L;
    final long heapMax = heap != null ? (heap.getMax() > 0 ? heap.getMax() : heap.getCommitted()) : 0L;

    long directBytes = 0L;
    long mappedBytes = 0L;
    for (final BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      if (pool == null)
        continue;
      final String name = pool.getName();
      if ("direct".equalsIgnoreCase(name)) {
        directBytes = pool.getMemoryUsed();
      } else if ("mapped".equalsIgnoreCase(name)) {
        mappedBytes = pool.getMemoryUsed();
      }
    }
    final long offHeap = directBytes + mappedBytes;

    final long vectorIndexBytes = safeFileSize(
        mutable != null && mutable.getComponentFile() != null ? Path.of(mutable.getComponentFile().getFilePath()) :
            null);
    final long graphFileBytes = safeFileSize(
        graphFile != null && graphFile.getComponentFile() != null ?
            Path.of(graphFile.getComponentFile().getFilePath()) : null);
    final long pqFileBytes = safeFileSize(pqFile != null ? pqFile.getFilePath() : null);
    final long compactedFileBytes = safeFileSize(
        compactedSubIndex != null && compactedSubIndex.getComponentFile() != null ?
            Path.of(compactedSubIndex.getComponentFile().getFilePath()) :
            null);

    final String breakdown = String.format("idx=%.1f, graph=%.1f, pq=%.1f, compacted=%.1f",
        vectorIndexBytes / (1024.0 * 1024.0),
        graphFileBytes / (1024.0 * 1024.0), pqFileBytes / (1024.0 * 1024.0), compactedFileBytes / (1024.0 * 1024.0));

    return new GraphBuildDiagnosticsSnapshot(heapUsed, heapMax, offHeap, vectorIndexBytes, graphFileBytes, pqFileBytes,
        compactedFileBytes, breakdown);
  }

  private static long safeFileSize(final Path path) {
    if (path == null)
      return 0L;
    try {
      return Files.size(path);
    } catch (final IOException e) {
      return 0L;
    }
  }

  /**
   * Rewrite the index data file keeping only the live entries, and swap it in: this is what a compaction of an
   * LSM_VECTOR index is. Entries are copied verbatim (the quantized payload is never re-quantized) and keep their
   * vector id, so no id is reused and the graph ordinals rebuilt right after stay valid.
   * <p>
   * The whole rewrite runs under the index write lock. A compaction is an explicit maintenance operation, its cost
   * is one sequential pass over the live vectors, and blocking writers for it is the price of not having to
   * reconcile entries that arrive while the file is being replaced.
   * <p>
   * Both old files - the previous data file and the legacy compacted component, if the index still has one - are
   * dropped afterwards, leaving the index with a single data file. That is what actually returns the space to the
   * filesystem: leaving the old file behind was why compaction used to make the index bigger, not smaller.
   *
   * @param liveEntries the live set, re-pointed in place at the new file
   *
   * @return true when the file was actually rewritten and the location index re-published with it, false when the
   * rewrite was skipped
   */
  private boolean rewriteDataFileWithLiveEntries(final Collection<VectorEntryForGraphBuild> liveEntries) {
    if (liveEntries.isEmpty()) {
      LogManager.instance().log(this, Level.INFO, "Skipping compaction of vector index '%s': no live vectors", indexName);
      return false;
    }

    for (final VectorEntryForGraphBuild entry : liveEntries)
      if (entry.absoluteFileOffset < 0 || entry.entryLength <= 0) {
        // A recovery fallback (document scan / in-memory recovery) contributed entries that are not on any page, so
        // they cannot be copied byte for byte. Rebuilding the graph is still correct, and COMPACT INDEX reports the
        // index as not compacted, but an operator who asked for space back is not getting it: say so at WARNING.
        LogManager.instance().log(this, Level.WARNING,
            "Skipping compaction of vector index '%s': the live set contains vectors recovered outside the index "
                + "pages, so no space was reclaimed. Retry once a rebuild has persisted them",
            indexName);
        return false;
      }

    final DatabaseInternal database = getDatabase();
    if (database.isTransactionActive())
      throw new IllegalStateException("Cannot compact vector index '" + indexName + "' inside a transaction");

    final int oldFileId = getFileId();
    final LockManager.LOCK_STATUS locked = database.getTransactionManager().tryLockFile(oldFileId, 0,
        Thread.currentThread());
    if (locked == LockManager.LOCK_STATUS.NO)
      throw new IllegalStateException("Cannot compact vector index '" + indexName + "': cannot lock file " + oldFileId);

    int newFileId = -1;
    LSMVectorIndexMutable previousMutable = null;
    LSMVectorIndexCompacted previousCompacted = null;

    try {
      lock.writeLock().lock();
      try {
        final int last_ = getComponentName().lastIndexOf('_');
        final String newName = getComponentName().substring(0, last_) + "_" + System.nanoTime();

        final LSMVectorIndexMutable newMutable = new LSMVectorIndexMutable(database, newName,
            database.getDatabasePath() + File.separator + newName, mutable.getDatabase().getMode(), getPageSize(),
            PaginatedComponent.TEMP_EXT + LSMVectorIndexMutable.FILE_EXT);
        newMutable.setMainIndex(this);
        database.getSchema().getEmbedded().registerFile(newMutable);
        newFileId = newMutable.getFileId();

        if (database.getTransactionManager().tryLockFile(newFileId, 0, Thread.currentThread()) == LockManager.LOCK_STATUS.NO)
          throw new IllegalStateException(
              "Cannot compact vector index '" + indexName + "': cannot lock the new file " + newFileId);

        // Copy the live entries, in vector id order, into freshly built pages of the new file.
        final List<VectorEntryForGraphBuild> sorted = new ArrayList<>(liveEntries);
        sorted.sort((a, b) -> Integer.compare(a.vectorId, b.vectorId));

        final List<MutablePage> newPages = new ArrayList<>();
        final int pageSize = getPageSize();
        final int pageSizeContent = pageSize - BasePage.PAGE_HEADER_SIZE - HEADER_BASE_SIZE;
        MutablePage page = null;
        int pageNum = -1;
        int freeContent = 0;
        int entriesInPage = 0;
        final byte[] buffer = new byte[maxEntryLength(sorted)];

        for (final VectorEntryForGraphBuild entry : sorted) {
          // Every entry came off a page of this same size, so it fits in a fresh one. Check it anyway: if a future
          // change ever let the pages shrink under an existing index, the loop below would create a new page and
          // then write straight past its end, and a compaction that silently corrupts is the worst possible one.
          if (entry.entryLength > pageSizeContent)
            throw new IndexException(
                "Cannot compact vector index '" + indexName + "': entry of vector " + entry.vectorId + " is "
                    + entry.entryLength + " bytes and does not fit in a " + pageSizeContent + " byte page");

          if (page == null || page.getMaxContentSize() - freeContent < entry.entryLength) {
            if (page != null) {
              // Seal the page just filled: only the last page of the file stays open for new writes.
              page.writeByte(OFFSET_MUTABLE, (byte) 0);
              newPages.add(page);
            }
            page = new MutablePage(new PageId(database, newFileId, ++pageNum), pageSize);
            page.writeInt(OFFSET_FREE_CONTENT, HEADER_BASE_SIZE);
            page.writeInt(OFFSET_NUM_ENTRIES, 0);
            page.writeByte(OFFSET_MUTABLE, (byte) 1);
            freeContent = HEADER_BASE_SIZE;
            entriesInPage = 0;
          }

          readRawEntry(entry, buffer);
          page.writeByteArray(freeContent, buffer, 0, entry.entryLength);

          // Re-point the entry at its new home before anything downstream reads it back.
          entry.absoluteFileOffset = (long) pageNum * pageSize + BasePage.PAGE_HEADER_SIZE + freeContent;
          entry.isCompacted = false;

          freeContent += entry.entryLength;
          entriesInPage++;
          page.writeInt(OFFSET_FREE_CONTENT, freeContent);
          page.writeInt(OFFSET_NUM_ENTRIES, entriesInPage);
        }
        newPages.add(page); // the last page stays mutable: new vectors append to it

        final List<MutablePage> versionedPages = new ArrayList<>(newPages.size());
        for (final MutablePage p : newPages) {
          // A page built outside a transaction carries no content size, and updatePageVersion persists exactly that
          // in the page header: without this the pages reload as empty and the whole index reads as lost.
          p.setContentSize(p.getMaxContentSize());
          versionedPages.add(database.getPageManager().updatePageVersion(p, true));
        }
        database.getPageManager().writePages(versionedPages, false);

        newMutable.updatePageCount(newPages.size());
        newMutable.removeTempSuffix();

        // SWAP: from here on every reader sees the compacted file
        previousMutable = mutable;
        previousCompacted = compactedSubIndex;
        mutable = newMutable;
        indexName = newMutable.getName();
        compactedSubIndex = null;
        currentInsertPageNum = newPages.size() - 1;
        currentMutablePages.set(1);

        // Re-point the location index at the new file in the SAME critical section that swapped it (issue #5568).
        // The entries above already carry their new offsets; leaving the swap to the caller would open a window in
        // which the index answers offsets into a file that no longer exists - a search landing there reads another
        // entry's bytes, or the dropped compacted component through a null reference.
        publishLocationIndex(liveEntries);

        ((LocalSchema) database.getSchema()).setMigratedFileId(oldFileId, newFileId);
        database.getSchema().getEmbedded().saveConfiguration();

        LogManager.instance().log(this, Level.INFO,
            "Compacted vector index '%s': %d live vectors in %d pages (was file %d, now file %d)",
            indexName, sorted.size(), newPages.size(), oldFileId, newFileId);

      } catch (final IOException | InterruptedException e) {
        if (e instanceof InterruptedException)
          Thread.currentThread().interrupt();
        throw new IndexException("Error compacting vector index '" + indexName + "'", e);
      } finally {
        lock.writeLock().unlock();
      }

      // Drop the replaced files OUTSIDE the write lock, as the LSM-tree compaction does: deleteFile also purges
      // their pages from the page cache, and a reader that captured the old component must be able to finish.
      dropReplacedComponent(previousMutable);
      dropReplacedComponent(previousCompacted);

      return true;
    } finally {
      if (newFileId > -1)
        database.getTransactionManager().unlockFile(newFileId, Thread.currentThread());
      if (locked == LockManager.LOCK_STATUS.YES)
        database.getTransactionManager().unlockFile(oldFileId, Thread.currentThread());
    }
  }

  private static int maxEntryLength(final List<VectorEntryForGraphBuild> entries) {
    int max = 0;
    for (final VectorEntryForGraphBuild e : entries)
      if (e.entryLength > max)
        max = e.entryLength;
    return max;
  }

  /** Read the raw bytes of an entry from the file it currently lives in, into the first {@code entryLength} bytes. */
  private void readRawEntry(final VectorEntryForGraphBuild entry, final byte[] buffer) {
    final int pageSize = getPageSize();
    final int pageNum = (int) (entry.absoluteFileOffset / pageSize);
    final int contentOffset = (int) (entry.absoluteFileOffset % pageSize) - BasePage.PAGE_HEADER_SIZE;
    final int fileId = entry.isCompacted ? compactedSubIndex.getFileId() : getFileId();

    try {
      final BasePage page = getDatabase().getPageManager()
          .getImmutablePage(new PageId(getDatabase(), fileId, pageNum), pageSize, false, false);
      page.readByteArray(contentOffset, buffer, 0, entry.entryLength);
    } catch (final IOException e) {
      throw new IndexException(
          "Error reading vector " + entry.vectorId + " of index '" + indexName + "' while compacting", e);
    }
  }

  /** Delete a data file that a compaction replaced, with its pages and its schema registration. */
  private void dropReplacedComponent(final PaginatedComponent component) {
    if (component == null)
      return;
    final DatabaseInternal database = getDatabase();
    try {
      if (database.isOpen()) {
        database.getPageManager().deleteFile(database, component.getFileId());
        database.getFileManager().dropFile(component.getFileId());
        database.getSchema().getEmbedded().removeFile(component.getFileId());
      } else {
        final File file = component.getOSFile();
        if (file != null && file.exists() && !file.delete())
          LogManager.instance().log(this, Level.WARNING, "Error deleting replaced index file '%s'", file.getPath());
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error dropping the file replaced by the compaction of '%s': %s",
          indexName, e.getMessage());
    }
  }

  /**
   * Replace the location index with one holding exactly {@code liveEntries}, published by a single reference
   * assignment to the volatile field (issue #5568).
   * <p>
   * The replacement is populated on a DETACHED instance on purpose. Refilling the live index in place - clear()
   * followed by one addOrUpdate per entry - is atomic only for whoever holds the index lock. The searches do
   * ({@code lock.readLock()} covers their {@code vectorIndex.size() == 0} short-circuit and the RID filters), but
   * {@link #countEntries()} and {@link #getStats()} take no lock at all, and they observed the map mid-refill and
   * reported an arbitrary fraction of its real content - a different wrong number on each run after a compaction,
   * because the rebuild a compaction triggers was still refilling the map while the caller counted it.
   * <p>
   * A swap fixes that without putting a lock on a read path, and it is what makes the invariant structural rather
   * than a rule every future reader has to remember. It also covers a hazard the read lock cannot: a compaction
   * swaps the data file in one write-locked section and used to re-publish the locations in a later one, so a
   * search could take the read lock in between and resolve old offsets against the new file. That is why the
   * compaction publishes from inside the section that swaps the file.
   * <p>
   * Callers must hold the write lock: it is what keeps a concurrent insert or remove from mutating the instance
   * being replaced (its mutation would be dropped by the swap), not what protects the readers.
   * <p>
   * The CPU cost is the same as the in-place refill it replaces, but peak heap is not: the instance being replaced
   * stays reachable for its in-flight readers while the new one is built, so a rebuild transiently holds two
   * location sets (~90 bytes per live entry each) instead of one. That is a bounded, promptly released spike and
   * the price of the atomicity.
   * <p>
   * The population loop runs under the write lock even though only the final store needs it. Hoisting it out would
   * shorten the locked window, but it would also let an insert commit into the instance about to be discarded, so
   * the loss window for a concurrent write would grow by the whole population. Holding the lock keeps exactly the
   * exclusion the in-place refill had, and the loop is an in-memory map fill - tens of milliseconds on a 200K
   * index, not the O(index size) document reads issue #5391 moved out of this lock.
   *
   * @param liveEntries the live set the index must hold, already pointing at the file that is current now
   */
  private void publishLocationIndex(final Collection<VectorEntryForGraphBuild> liveEntries) {
    // The location index never evicts, so it will hold every live entry and is sized for exactly that: the hint is
    // the live count itself, NOT the classic size*4/3 load-factor pre-adjustment. ConcurrentHashMap's constructor
    // argument is an element-count estimate that it divides by the load factor itself (`initialCapacity +
    // (initialCapacity >>> 1) + 1`, rounded up to a power of two), unlike HashMap's, which is a bucket count.
    // Pre-adjusting on top of that provisioned ~1.33x more table than the rebuild can ever use.
    final VectorLocationIndex rebuilt = new VectorLocationIndex(Math.max(16, liveEntries.size()));
    for (final VectorEntryForGraphBuild entry : liveEntries)
      rebuilt.addOrUpdate(entry.vectorId, entry.isCompacted, entry.absoluteFileOffset, entry.rid, false);
    // The rebuilt index only knows the ids that are still live, so its high-water mark can be lower than the one
    // already handed out. Carry the sequence over, or the next insert would reuse an id a tombstone still refers to.
    rebuilt.setNextId(Math.max(rebuilt.getNextId(), nextId.get()));
    vectorIndex = rebuilt;
  }

  /**
   * Merge one page entry into the live set being rebuilt, applying LSM merge-on-read: the highest vector id for a
   * RID wins, and on a tie the entry read later wins (pages are parsed in write order). A winning tombstone removes
   * the RID from the live set instead of adding it.
   */
  private static void mergeEntryIntoLiveSet(final Map<RID, VectorEntryForGraphBuild> liveSet,
      final LSMVectorIndexPageParser.VectorEntry entry, final boolean isCompacted) {
    final VectorEntryForGraphBuild existing = liveSet.get(entry.rid);
    if (existing != null && entry.vectorId < existing.vectorId)
      return; // superseded by a newer vector for the same RID

    if (entry.deleted)
      liveSet.remove(entry.rid);
    else
      liveSet.put(entry.rid,
          new VectorEntryForGraphBuild(entry.vectorId, entry.rid, isCompacted, entry.absoluteFileOffset,
              entry.entryLength));
  }

  /**
   * Helper class for collecting vector entries during graph build.
   * Used to avoid race conditions with concurrent VectorLocationIndex modifications.
   */
  private static class VectorEntryForGraphBuild {
    final int vectorId;
    final RID rid;
    // Not final: a compaction rewrites the data file underneath the live set and re-points these at the new file.
    boolean   isCompacted;
    long      absoluteFileOffset;
    /** Size of the entry on its page, or -1 when it was recovered from a document instead of read from a page. */
    final int entryLength;

    VectorEntryForGraphBuild(final int vectorId, final RID rid, final boolean isCompacted,
        final long absoluteFileOffset) {
      this(vectorId, rid, isCompacted, absoluteFileOffset, -1);
    }

    VectorEntryForGraphBuild(final int vectorId, final RID rid, final boolean isCompacted,
        final long absoluteFileOffset, final int entryLength) {
      this.vectorId = vectorId;
      this.rid = rid;
      this.isCompacted = isCompacted;
      this.absoluteFileOffset = absoluteFileOffset;
      this.entryLength = entryLength;
    }
  }

  public static class LSMVectorIndexFactoryHandler implements IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder<? extends Index> builder) {
      final BucketLSMVectorIndexBuilder vectorBuilder = (BucketLSMVectorIndexBuilder) builder;
      final LSMVectorIndexMetadata vectorMetadata = vectorBuilder.getVectorMetadata();

      // "dimensions" is the one vector setting with no usable default: every put() and every graph
      // build compares the candidate vector length against metadata.dimensions, so a zero (or
      // negative) value yields an index that silently accepts writes and never indexes a single
      // vector. Refusing it here covers all creation entry points at once - SQL METADATA, the
      // schema builders and the importers - instead of only the SQL one (issue #5607).
      if (vectorMetadata.dimensions < 1)
        throw new IndexException(
            "LSM_VECTOR index '" + builder.getIndexName() + "' requires a positive 'dimensions' setting (got "
                + vectorMetadata.dimensions
                + "): it must match the number of components of the indexed vectors, e.g. METADATA {\"dimensions\": 384}");

      // Reject the (encoding=INT8, quantization=INT8) combination: wire/storage is already int8,
      // and JVector's internal INT8 scalar quantization re-runs the same lossy reduction on the
      // float vectors we just dequantized at ingest. The user's intent is almost certainly one or
      // the other, not both. If a future use case justifies this combination, lift the guard with
      // a deliberate justification - silent double-processing is the failure mode we are blocking.
      if (vectorMetadata.encoding == VectorEncoding.INT8 && vectorMetadata.quantizationType == VectorQuantizationType.INT8)
        throw new IndexException(
            """
            Combining encoding=INT8 with quantization=INT8 is redundant: the property is already byte-quantized \
            at the wire level, so JVector's internal INT8 scalar quantization would re-quantize the \
            dequantized floats. Pick one (encoding=INT8 for payload/storage savings, OR quantization=INT8 \
            for index-internal compression) but not both.""");

      // Property-type / encoding consistency: the document property type is what callers actually
      // pass into put(); the encoding is what the index expects. A mismatch (e.g. ARRAY_OF_FLOATS
      // property declared with encoding=INT8) yields a value that toFloatArray cannot dequantize
      // and the put() path either silently treats stored floats as if they were the int8 input
      // (encoding=INT8 + ARRAY_OF_FLOATS), or surfaces a confusing rejection at every query
      // (encoding=FLOAT32 + BINARY). Checking up front turns those silent or late failures into
      // a single clear builder-time error.
      final DocumentType propertyOwner = builder.getDatabase().getSchema().getType(vectorBuilder.getTypeName());
      final String propertyName = vectorBuilder.getPropertyNames()[0];
      final Property property = propertyOwner.getPolymorphicPropertyIfExists(propertyName);
      if (property != null) {
        final Type propertyType = property.getType();
        if (vectorMetadata.encoding == VectorEncoding.INT8 && propertyType != Type.BINARY)
          throw new IndexException(
              "Vector index encoding=INT8 requires property '" + propertyName + "' to be declared as BINARY (one byte per dim), "
                  + "but it is declared as " + propertyType + ". Either change the property type to BINARY or set encoding=FLOAT32.");
        if (vectorMetadata.encoding == VectorEncoding.FLOAT32 && propertyType == Type.BINARY)
          throw new IndexException(
              "Vector index encoding=FLOAT32 (default) does not support a BINARY property '" + propertyName
                  + "'. Either declare the property as ARRAY_OF_FLOATS or set encoding=INT8 to ingest pre-quantized bytes.");
      }

      return new LSMVectorIndex(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(),
          ComponentFile.MODE.READ_WRITE, builder.getPageSize(),
          vectorMetadata.copy(vectorBuilder.getTypeName(), vectorBuilder.getPropertyNames(), -1));
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public Component createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      // Check if this is a compacted index file (created during compaction)
      if (filePath.endsWith(LSMVectorIndexCompacted.FILE_EXT))
        return new LSMVectorIndexCompacted(null, database, name, filePath, id, mode, pageSize, version);

      // Otherwise, load as main mutable index
      return new LSMVectorIndex(database, name, filePath, id, mode, pageSize, version).mutable;
    }
  }

  /**
   * Constructor for creating a new index. Every construction-time setting arrives on a single
   * {@link LSMVectorIndexMetadata}, so the metadata is fully populated before the instance escapes;
   * previously the factory passed 17 positional args and then post-mutated {@code metadata.encoding}
   * after the constructor returned (issue #4134), and later a value object whose own field list could
   * (and did) fall behind the metadata's (issue #5639).
   * <p>
   * The caller must pass an instance it does not keep - typically {@link LSMVectorIndexMetadata#copy} of
   * the builder's - because {@code buildState} and the corpus-dependent fields are per-index state.
   */
  public LSMVectorIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final LSMVectorIndexMetadata metadata) {
    try {
      this.indexName = name;

      this.metadata = metadata;

      this.lock = new ReentrantReadWriteLock();
      warnIfLocationCacheSizeConfigured(database);
      this.vectorIndex = new VectorLocationIndex();
      this.ordinalToVectorId = new int[0];
      this.nextId = new AtomicInteger(0);
      this.status = new AtomicReference<>(INDEX_STATUS.AVAILABLE);

      // Initialize graph lifecycle management
      this.graphState = GraphState.LOADING;
      this.mutationsSinceSerialize = new AtomicInteger(0);

      // Initialize compaction fields
      this.currentMutablePages = new AtomicInteger(0); // No page0 - start with 0 pages
      this.minPagesToScheduleACompaction = database.getConfiguration()
          .getValueAsInteger(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
      this.compactedSubIndex = null;

      // Create the component that handles page storage
      this.mutable = new LSMVectorIndexMutable(database, indexName, filePath, mode, pageSize);
      this.mutable.setMainIndex(this);

      // Create graph file component (same timing as mutable - outside transaction)
      final String graphFileName = indexName + "_vecgraph";
      final String graphFilePath = filePath + "_vecgraph";
      this.graphFile = new LSMVectorIndexGraphFile(database, graphFileName, graphFilePath, mode, pageSize);
      this.graphFile.setMainIndex(this);
      database.getSchema().getEmbedded().registerFile(this.graphFile);

      // Create PQ file handler for Product Quantization (zero-disk-I/O search)
      // Note: PQ file uses direct I/O (not ArcadeDB pages) since it's loaded entirely into memory
      this.pqFile = createPQFileWithFallback(mutable.getFilePath());

      LogManager.instance()
          .log(this, Level.FINE, "Created LSMVectorIndex: indexName=%s, vectorFileId=%d, graphFileId=%d", indexName,
              mutable.getFileId(), graphFile.getFileId());

      initializeGraphIndex();
    } catch (final IOException e) {
      throw new IndexException("Error on creating index '" + name + "'", e);
    }
  }

  /**
   * Constructor for loading an existing index
   */
  protected LSMVectorIndex(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    this.indexName = name;

    this.metadata = new LSMVectorIndexMetadata(null, new String[0], -1);
    this.lock = new ReentrantReadWriteLock();
    warnIfLocationCacheSizeConfigured(database);
    this.vectorIndex = new VectorLocationIndex();
    this.ordinalToVectorId = new int[0];
    this.nextId = new AtomicInteger(0);
    this.status = new AtomicReference<>(INDEX_STATUS.AVAILABLE);

    // Initialize graph lifecycle management
    this.graphState = GraphState.LOADING;
    this.mutationsSinceSerialize = new AtomicInteger(0);

    // Create the component that handles page storage
    this.mutable = new LSMVectorIndexMutable(database, name, filePath, id, mode, pageSize, version);
    this.mutable.setMainIndex(this);

    // Discover and load graph file if it exists on disk.
    // If no graph file exists yet, graphFile stays null and will be created lazily
    // by getOrCreateGraphFile() when buildGraphFromScratch() first needs to persist.
    this.graphFile = discoverAndLoadGraphFile();
    if (this.graphFile != null)
      this.graphFile.setMainIndex(this);

    // Create PQ file handler (for zero-disk-I/O search)
    // PQ data will be loaded after schema loads metadata (see loadVectorsAfterSchemaLoad)
    this.pqFile = createPQFileWithFallback(mutable.getFilePath());

    // Initialize compaction fields
    this.currentMutablePages = new AtomicInteger(mutable.getTotalPages());
    this.minPagesToScheduleACompaction = database.getConfiguration()
        .getValueAsInteger(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);

    // Discover and load compacted sub-index file if it exists (critical for replicas after compaction)
    LogManager.instance().log(this, Level.FINE, "Attempting to discover compacted sub-index for index: %s", null, name);
    this.compactedSubIndex = discoverAndLoadCompactedSubIndex();
    if (this.compactedSubIndex != null) {
      LogManager.instance()
          .log(this, Level.WARNING, "Successfully loaded compacted sub-index: %s (fileId=%d)",
              this.compactedSubIndex.getName(),
              this.compactedSubIndex.getFileId());
    } else {
      LogManager.instance().log(this, Level.FINE, "No compacted sub-index found for index: %s", null, name);
    }

    // DON'T load vectors here - metadata.dimensions is still -1 at this point!
    // Vector loading is deferred until after schema loads metadata via onAfterSchemaLoad() hook.
    // See loadVectorsAfterSchemaLoad() method which is called by LSMVectorIndexMutable.onAfterSchemaLoad()
  }

  private LSMVectorIndexPQFile createPQFileWithFallback(final String primaryBasePath) {
    // Use the component file path as canonical. If legacy PQ exists at a shorter base name, migrate it once.
    final LSMVectorIndexPQFile pq = new LSMVectorIndexPQFile(primaryBasePath);

    // Derive a legacy base path by stripping the first extension (e.g., drop .4.262144.v0.lsmvecidx)
    String legacyBasePath = null;
    final int dot = primaryBasePath.indexOf('.');
    if (dot > 0) {
      legacyBasePath = primaryBasePath.substring(0, dot);
    }

    if (!pq.exists() && legacyBasePath != null) {
      final LSMVectorIndexPQFile legacyPQ = new LSMVectorIndexPQFile(legacyBasePath);
      if (legacyPQ.exists()) {
        try {
          final var targetParent = pq.getFilePath().getParent();
          if (targetParent != null && !Files.exists(targetParent)) {
            Files.createDirectories(targetParent);
          }
          Files.move(legacyPQ.getFilePath(), pq.getFilePath(), StandardCopyOption.REPLACE_EXISTING);
          LogManager.instance().log(this, Level.INFO,
              "Migrated PQ file from legacy path %s to canonical %s", legacyPQ.getFilePath(), pq.getFilePath());
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "Failed to migrate PQ file from legacy path %s to canonical %s: %s", legacyPQ.getFilePath(),
              pq.getFilePath(),
              e.getMessage());
        }
      }
    }

    return pq;
  }

  /**
   * Load vectors from pages after schema has loaded metadata.
   * Called by LSMVectorIndexMutable.onAfterSchemaLoad() after dimensions are set from schema.json.
   */
  public void loadVectorsAfterSchemaLoad() {
    LogManager.instance()
        .log(this, Level.FINE, "loadVectorsAfterSchemaLoad called for index %s: dimensions=%d, mutablePages=%d, hasGraphFile=%s",
            null, indexName, metadata.dimensions, mutable.getTotalPages(), graphFile != null);

    // CRASH RECOVERY: Check if index was BUILDING when database crashed/shutdown
    try {
      final BUILD_STATE loadedState = BUILD_STATE.valueOf(this.metadata.buildState);
      if (loadedState == BUILD_STATE.BUILDING) {
        // Index was being built when database crashed/shutdown
        LogManager.instance().log(this, Level.WARNING,
            "Vector index '%s' was BUILDING during last shutdown. Marking as INVALID. Run 'REBUILD INDEX %s' to recover.",
            indexName, indexName);

        this.buildState = BUILD_STATE.INVALID;
        this.metadata.buildState = BUILD_STATE.INVALID.name();

        // Persist INVALID state immediately
        try {
          getDatabase().getSchema().getEmbedded().saveConfiguration();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to persist INVALID state for index '%s' after crash detection", e, indexName);
        }
      } else {
        this.buildState = loadedState;
      }
    } catch (final Exception e) {
      // Old index without buildState field - assume READY
      this.buildState = BUILD_STATE.READY;
      this.metadata.buildState = BUILD_STATE.READY.name();
    }

    // Only load vectors if we have valid metadata (dimensions > 0) and pages exist
    if (metadata.dimensions > 0 && mutable.getTotalPages() > 0) {
      try {
        LogManager.instance()
            .log(this, Level.FINE, "Loading vectors for index %s after schema load (dimensions=%d, pages=%d, fileId=%d)",
                null, indexName, metadata.dimensions, mutable.getTotalPages(), mutable.getFileId());

        loadVectorsFromPages();

        // Graph will be lazy-loaded on first search via ensureGraphAvailable()
        // Don't build it here - causes deadlock during database load when PageManager isn't fully ready
        LogManager.instance().log(this, Level.FINE,
            "Successfully loaded %d vector locations for index %s (graph will be lazy-loaded on first search)",
            null, vectorIndex.size(), indexName);

        // Load PQ data if PRODUCT quantization is enabled
        if (metadata.quantizationType == VectorQuantizationType.PRODUCT && pqFile != null) {
          if (pqFile.loadPQ()) {
            this.pqVectors = pqFile.getPQVectors();
            this.productQuantization = pqFile.getProductQuantization();
            LogManager.instance().log(this, Level.INFO,
                "PQ data loaded for index %s: %d vectors ready for zero-disk-I/O search",
                indexName, pqVectors != null ? pqVectors.count() : 0);
          }
        }
      } catch (final Exception e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Could not load vectors from pages for index %s: %s", indexName, e.getMessage());
        this.graphState = GraphState.LOADING;
      }
    } else {
      LogManager.instance()
          .log(this, Level.FINE, "Skipping vector load for index %s (dimensions=%d, pages=%d)", null, indexName,
              metadata.dimensions, mutable.getTotalPages());
    }
  }

  /**
   * Discovers and loads the compacted sub-index file if it exists.
   * This is critical for replicas after compaction, where VectorLocationIndex entries
   * may reference fileId of the compacted file, but the compacted component isn't loaded.
   *
   * @return The loaded compacted sub-index, or null if none found
   */
  private LSMVectorIndexCompacted discoverAndLoadCompactedSubIndex() {
    try {
      final DatabaseInternal database = getDatabase();
      final String componentName = mutable.getName();

      // Extract the index name prefix (everything up to the last '_')
      final int lastUnderscore = componentName.lastIndexOf('_');
      if (lastUnderscore == -1) {
        // No underscore in name - no compacted file expected
        return null;
      }

      final String namePrefix = componentName.substring(0, lastUnderscore);

      // First, check if compacted file is already loaded in schema (all files)
      // This handles the case where the file was already loaded by LocalSchema.load()
      for (int i = 0; i < 1000; i++) {  // Check up to 1000 file IDs
        try {
          final Component comp = database.getSchema().getFileByIdIfExists(i);
          if (comp instanceof LSMVectorIndexCompacted) {
            final String compName = comp.getName();
            if (compName.startsWith(namePrefix + "_") && !compName.equals(componentName)) {
              LogManager.instance()
                  .log(this, Level.SEVERE, "Found existing compacted sub-index in schema: %s (fileId=%d)", compName,
                      comp.getFileId());
              return (LSMVectorIndexCompacted) comp;
            }
          }
        } catch (final Exception e) {
          // File ID doesn't exist, continue
        }
      }

      // If not found in schema, look for ComponentFile in FileManager
      // FileManager tracks all files on disk with their fileIds
      ComponentFile compactedComponentFile = null;
      long highestTimestamp = -1;

      LogManager.instance().log(this, Level.FINE, "Searching FileManager for compacted files with prefix: %s", null, namePrefix);

      for (final ComponentFile file : database.getFileManager().getFiles()) {
        // FileManager.getFiles() is a sparse list: dropped slots are null. Skip them
        // (same defensive pattern used in discoverAndLoadGraphFile() below).
        if (file == null)
          continue;

        final String fileName = file.getComponentName();
        final String fileExt = file.getFileExtension();

        // Check if this is a compacted sub-index file matching our pattern
        if (LSMVectorIndexCompacted.FILE_EXT.equals(fileExt) && fileName.startsWith(namePrefix + "_") && !fileName.equals(
            componentName)) {

          // Extract timestamp from filename to find most recent
          final int lastUnder = fileName.lastIndexOf('_');
          if (lastUnder != -1) {
            try {
              final long timestamp = Long.parseLong(fileName.substring(lastUnder + 1));
              if (timestamp > highestTimestamp) {
                highestTimestamp = timestamp;
                compactedComponentFile = file;
              }
            } catch (final NumberFormatException e) {
              // Not a valid timestamp, skip
            }
          }
        }
      }

      if (compactedComponentFile == null) {
        // No compacted file found
        return null;
      }

      // Load the compacted sub-index using the ComponentFile's metadata (includes fileId)
      final String compactedName = compactedComponentFile.getComponentName();
      final int compactedFileId = compactedComponentFile.getFileId();
      final String compactedPath = compactedComponentFile.getFilePath();
      final int pageSize = compactedComponentFile instanceof PaginatedComponentFile ?
          ((PaginatedComponentFile) compactedComponentFile).getPageSize() :
          mutable.getPageSize();
      final int version = compactedComponentFile.getVersion();

      // Create the compacted index component from the ComponentFile
      final LSMVectorIndexCompacted compactedIndex = new LSMVectorIndexCompacted(this, database, compactedName,
          compactedPath,
          compactedFileId, database.getMode(), pageSize, version);

      // NOTE: Do NOT register with schema here - the file is already registered by LocalSchema.load()
      // when it scans the database directory. Registering twice causes "File with id already exists" error.

      LogManager.instance()
          .log(this, Level.WARNING, "Discovered and loaded compacted sub-index: %s (fileId=%d, pages=%d)",
              compactedName,
              compactedIndex.getFileId(), compactedIndex.getTotalPages());

      return compactedIndex;

    } catch (final Exception e) {
      // A failure here orphans an existing compacted file on disk for the lifetime of the process,
      // silently degrading kNN performance. Log at SEVERE with the stack trace so it is not missed.
      LogManager.instance().log(this, Level.SEVERE, "Error discovering compacted sub-index for %s", e, indexName);
      return null;
    }
  }

  /**
   * Discovers and loads the graph file if it exists.
   * Called during index loading to reconnect with persisted graph topology.
   *
   * @return The loaded graph file, or null if none found
   */
  private LSMVectorIndexGraphFile discoverAndLoadGraphFile() {
    try {
      final DatabaseInternal database = getDatabase();
      final String expectedGraphFileName = mutable.getName() + "_" + LSMVectorIndexGraphFile.FILE_EXT;

      LogManager.instance()
          .log(this, Level.FINE, "Discovering graph file for index %s, looking for: %s", indexName,
              expectedGraphFileName);

      // Look for ComponentFile in FileManager
      for (final ComponentFile file : database.getFileManager().getFiles()) {
        if (file != null && LSMVectorIndexGraphFile.FILE_EXT.equals(file.getFileExtension()) && file.getComponentName()
            .equals(expectedGraphFileName)) {

          final int pageSize = file instanceof PaginatedComponentFile ?
              ((PaginatedComponentFile) file).getPageSize() :
              mutable.getPageSize();

          final LSMVectorIndexGraphFile graphFile = new LSMVectorIndexGraphFile(database, file.getComponentName(),
              file.getFilePath(), file.getFileId(), database.getMode(), pageSize, file.getVersion());

          database.getSchema().getEmbedded().registerFile(graphFile);

          LogManager.instance().log(this, Level.INFO, "Discovered and loaded graph file: %s (fileId=%d)",
              graphFile.getName(),
              graphFile.getFileId());

          return graphFile;
        }
      }

      LogManager.instance()
          .log(this, Level.FINE, "No graph file found in FileManager for index %s. Graph will be built on first search.",
              indexName);
      return null;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error discovering graph file for %s: %s", indexName,
          e.getMessage());
      return null;
    }
  }

  /**
   * Returns the existing graphFile or lazily creates one on disk when needed (e.g. first graph build
   * on an index that was loaded without a persisted .vecgraph file). This avoids creating empty files
   * eagerly in the loading constructor.
   */
  private LSMVectorIndexGraphFile getOrCreateGraphFile() {
    if (graphFile != null)
      return graphFile;

    try {
      final DatabaseInternal db = getDatabase();
      final String graphFileName = indexName + "_" + LSMVectorIndexGraphFile.FILE_EXT;
      final String graphFilePath = mutable.getFilePath() + "_" + LSMVectorIndexGraphFile.FILE_EXT;
      this.graphFile = new LSMVectorIndexGraphFile(db, graphFileName, graphFilePath,
          db.getMode(), mutable.getPageSize());
      this.graphFile.setMainIndex(this);
      db.getSchema().getEmbedded().registerFile(this.graphFile);
      LogManager.instance().log(this, Level.INFO, "Created graph file on demand for index: %s", indexName);
      return graphFile;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Could not create graph file for index %s: %s",
          indexName, e.getMessage());
      return null;
    }
  }

  /**
   * Initialize graph index - called during index creation/loading.
   * For new indexes, builds graph immediately. For loaded indexes, graph is lazy-loaded on first search.
   */
  private void initializeGraphIndex() {
    // For newly created indexes (during constructor), build graph immediately
    // For loaded indexes, graph will be lazy-loaded on first search via ensureGraphAvailable()
    if (vectorIndex.size() > 0 && graphState == GraphState.LOADING) {
      // Check if we can lazy-load from persisted graph
      if (graphFile != null && graphFile.hasPersistedGraph()) {
        LogManager.instance().log(this, Level.INFO, "Graph will be lazy-loaded from disk for index: %s", indexName);
        return;
      }

      // No persisted graph - build now for new indexes
      // NOTE: buildGraphFromScratch() manages locking internally
      // Don't hold lock here - JVector uses parallel threads during graph build
      buildGraphFromScratch();
    }
  }

  /**
   * Ensure graph is available for searching. Lazy-loads from disk if needed.
   * This is the entry point for all search operations.
   */
  private void ensureGraphAvailable() {
    if (graphState != GraphState.LOADING)
      return; // Graph already available or being built

    lock.writeLock().lock();
    try {
      // Double-check after acquiring lock
      if (graphState != GraphState.LOADING)
        return; // Another thread already started building

      // Try to load persisted graph from disk
      // IMPORTANT: If PRODUCT quantization is enabled but PQ file doesn't exist, we need to rebuild
      // the graph from scratch so that ordinalToVectorId is consistent between graph and PQ.
      // Loading graph from disk and then building PQ separately causes ordinal mismatch.
      final boolean needsGraphRebuildForPQ = metadata.quantizationType == VectorQuantizationType.PRODUCT &&
          pqFile != null && !pqFile.exists();
      if (needsGraphRebuildForPQ) {
        LogManager.instance().log(this, Level.INFO,
            """
                PRODUCT quantization enabled but PQ file missing - rebuilding graph from scratch for ordinal \
                consistency: %s""",
            indexName);
      }

      // CRITICAL FIX FOR #3135: Check if vectorIndex contains deleted entries
      // If vectors were updated/deleted, the persisted graph's ordinal mappings are stale.
      // The graph was built with ordinals based on the old vector set, but after filtering
      // deleted vectors, the new ordinalToVectorId array will have different indices.
      // This causes NPE when JVector tries to access vectors using stale ordinals.
      // Solution: Rebuild graph from scratch if any deleted entries exist.
      final boolean hasDeletedVectors = vectorIndex.getDeletedCount() > 0;
      if (hasDeletedVectors) {
        LogManager.instance().log(this, Level.INFO,
            """
                Deleted vectors detected in index %s - rebuilding graph from scratch to ensure ordinal consistency \
                (fixes issue #3135: stale ordinal mappings after vector updates)""",
            indexName);
      }

      if (graphFile != null && graphFile.hasPersistedGraph() && !needsGraphRebuildForPQ && !hasDeletedVectors) {
        try {
          final var loadedGraph = graphFile.loadGraph();

          // Rebuild ordinalToVectorId from vectorIndex
          // IMPORTANT: Must match the validation logic used during graph building
          final String vectorProp =
              metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ?
                  metadata.propertyNames.getFirst() : "vector";

          final int[] rebuiltOrdinalToVectorId = vectorIndex.getAllVectorIds().filter(id -> {
            final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(id);
            if (loc == null || loc.deleted) {
              return false;
            }

            // Re-validate vectors to match graph building logic
            // NOTE: PRODUCT quantization does NOT store vectors in pages - must read from documents like NONE
            final boolean hasInlineQuantization = metadata.quantizationType == VectorQuantizationType.INT8 ||
                metadata.quantizationType == VectorQuantizationType.BINARY;
            if (hasInlineQuantization) {
              // With INT8/BINARY quantization: verify we can read the quantized vector from pages
              try {
                final float[] vector = readVectorFromOffset(loc.absoluteFileOffset, loc.isCompacted);
                return vector != null && vector.length == metadata.dimensions && !VectorUtils.isZeroVector(vector);
              } catch (final Exception e) {
                return false;
              }
            } else {
              // Without quantization: validate by reading from document.
              try {
                final Record record = getDatabase().lookupByRID(loc.rid, false);
                if (record == null)
                  return false;
                final Document doc = (Document) record;
                final Object vectorObj = doc.get(vectorProp);
                if (vectorObj == null)
                  return false;
                final float[] vector = VectorUtils.toFloatArray(vectorObj, metadata.encoding);
                return vector.length == metadata.dimensions && !VectorUtils.isZeroVector(vector);
              } catch (final Exception e) {
                return false;
              }
            }
          }).sorted().toArray();

          final int graphSize = loadedGraph != null ? loadedGraph.size() : 0;
          LogManager.instance().log(this, Level.INFO,
              "Loaded graph from disk for index: %s, graphSize=%d, ordinalToVectorIdLength=%d, vectorIndexSize=%d",
              indexName, graphSize, rebuiltOrdinalToVectorId.length, vectorIndex.size());

          // CRITICAL FIX FOR #3722: Check if persisted graph is stale (has fewer vectors than currently active).
          // This happens when vectors were added after the graph was last built and persisted.
          // On database restart, deltaVectors (volatile) are lost and graphState is set to IMMUTABLE,
          // so rebuildGraphBeforeSearch() never triggers. Search can only find nodes in the stale graph.
          if (graphSize < rebuiltOrdinalToVectorId.length) {
            LogManager.instance().log(this, Level.INFO,
                """
                Persisted graph is stale for index %s: graph has %d nodes but %d active vectors exist - \
                rebuilding from scratch (fixes issue #3722: missing vectors after database restart)""",
                indexName, graphSize, rebuiltOrdinalToVectorId.length);
            // Don't use the stale graph — fall through to buildGraphFromScratch() below
          } else {
            // Graph is up to date — use it
            this.graphIndex = loadedGraph;
            this.graphState = GraphState.IMMUTABLE;
            this.ordinalToVectorId = rebuiltOrdinalToVectorId;

            // Build PQ if PRODUCT quantization is enabled but PQ file doesn't exist
            // This handles the case where graph was built before PRODUCT quantization was added
            if (metadata.quantizationType == VectorQuantizationType.PRODUCT && pqFile != null && !pqFile.isPQReady()) {
              LogManager.instance().log(this, Level.INFO,
                  "PQ not available after graph load, building PQ for index: %s", indexName);
              try {
                // Create vector values from the loaded vectorIndex for PQ building
                final Map<Integer, VectorLocationIndex.VectorLocation> vectorLocationSnapshot = new HashMap<>();
                for (int vectorId : ordinalToVectorId) {
                  final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
                  if (loc != null && !loc.deleted) {
                    vectorLocationSnapshot.put(vectorId, loc);
                  }
                }
                final RandomAccessVectorValues vectors = new ArcadePageVectorValues(getDatabase(), metadata.dimensions,
                    vectorProp,
                    vectorLocationSnapshot, ordinalToVectorId, this,
                    computeGraphBuildCacheCapacity(ordinalToVectorId.length, false));
                buildAndPersistPQ(vectors);
              } catch (final Exception e) {
                LogManager.instance().log(this, Level.WARNING,
                    "Failed to build PQ after graph load for index %s: %s", indexName, e.getMessage());
              }
            }

            return;
          }
        } catch (final Exception e) {
          LogManager.instance()
              .log(this, Level.WARNING, "Failed to load graph for %s, will rebuild: %s", indexName, e.getMessage());
        }
      }

    } finally {
      lock.writeLock().unlock();
    }

    // No persisted graph or load failed - build from scratch
    buildGraphFromScratch();
  }

  /**
   * Build (or rebuild) the vector graph immediately instead of waiting for the next search-triggered lazy build.
   * Useful after bulk inserts/updates when callers want the graph to be ready right away.
   */
  public void buildVectorGraphNow() {
    buildVectorGraphNow(null);
  }

  /**
   * Build (or rebuild) the vector graph immediately with an optional progress callback.
   * This forces a full rebuild even if the mutation threshold has not been reached yet.
   *
   * @param graphCallback optional progress callback invoked during graph construction/persistence
   */
  public void buildVectorGraphNow(final GraphBuildCallback graphCallback) {
    checkIsValid();

    // Prevent concurrent graph rebuilds and signal callers to retry if the index is busy.
    if (!status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE))
      throw new NeedRetryException("Vector index '" + indexName + "' is not available for rebuild");

    try {
      // Force rebuild from on-disk pages, bypassing mutation thresholds and lazy-load behavior.
      graphState = GraphState.LOADING;
      mutationsSinceSerialize.set(0);
      buildGraphFromScratchWithRetry(graphCallback, false);
    } finally {
      status.set(INDEX_STATUS.AVAILABLE);
    }
  }

  /**
   * Returns the dedicated ForkJoinPool for graph building, creating it if needed.
   * Using a per-index pool allows us to cancel long-running builds on shutdown
   * by calling shutdownNow() on this pool.
   */
  /**
   * Ordinals that no path from the entry node reaches. Beam search only ever follows edges forward from the entry
   * node, so such a node can never be returned no matter how wide the beam - the graph build occasionally leaves one
   * with a full out-degree and no in-edges (issue #5615). Callers keep those vectors searchable through the delta
   * scan instead.
   * <p>
   * The walk unions the edges of every level, while a search descends the levels before beam-searching level 0.
   * Union-reachability is therefore a superset of what a search can reach: this never invents an orphan, but a node
   * reachable only through a higher-level in-edge is not reported. That suits the observed defect, which is a node
   * with no in-edges at any level, and keeps the check conservative - a false orphan would cost a duplicate delta
   * entry on every search.
   *
   * @return the unreachable ordinals, empty when every node is reachable
   */
  int[] findUnreachableOrdinals(final ImmutableGraphIndex graph) {
    try (final ImmutableGraphIndex.View view = graph.getView()) {
      final int upper = graph.getIdUpperBound();
      if (upper <= 0)
        return EMPTY_ORDINALS;

      final boolean[] reached = new boolean[upper];
      final int[] queue = new int[upper];
      int head = 0, tail = 0;

      final ImmutableGraphIndex.NodeAtLevel entryNode = view.entryNode();
      if (entryNode == null) {
        LogManager.instance().log(this, Level.WARNING,
            "Graph for index %s has no entry node, skipping the connectivity check", indexName);
        return EMPTY_ORDINALS;
      }
      final int entry = entryNode.node;
      if (entry < 0 || entry >= upper)
        return EMPTY_ORDINALS;
      reached[entry] = true;
      queue[tail++] = entry;

      final int maxLevel = graph.getMaxLevel();
      while (head < tail) {
        final int node = queue[head++];
        for (int level = 0; level <= maxLevel; level++) {
          final NodesIterator neighbors;
          try {
            // A node absent from a level yields an empty iterator rather than an exception
            // (OnHeapGraphIndex.getNeighborsIterator returns EMPTY_NODE_ITERATOR for both an out-of-range level
            // and an unmapped node), so this walks the levels without paying for control flow. The catch only
            // guards View implementations that choose to throw instead.
            neighbors = view.getNeighborsIterator(level, node);
          } catch (final Exception e) {
            continue;
          }
          // Skip one node rather than letting an NPE unwind to the outer catch, which would abandon the walk and
          // silently treat every remaining node as reachable.
          if (neighbors == null)
            continue;
          while (neighbors.hasNext()) {
            final int next = neighbors.nextInt();
            if (next >= 0 && next < upper && !reached[next]) {
              reached[next] = true;
              queue[tail++] = next;
            }
          }
        }
      }

      int unreachable = 0;
      for (int node = 0; node < upper; node++)
        if (!reached[node] && graph.containsNode(node))
          unreachable++;

      if (unreachable == 0)
        return EMPTY_ORDINALS;

      final int[] result = new int[unreachable];
      int i = 0;
      for (int node = 0; node < upper; node++)
        if (!reached[node] && graph.containsNode(node))
          result[i++] = node;
      return result;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not verify graph connectivity for index %s: %s", indexName, e.getMessage());
      return EMPTY_ORDINALS;
    }
  }

  private synchronized ForkJoinPool getOrCreateGraphBuildPool() {
    ForkJoinPool pool = graphBuildPool;
    if (pool == null || pool.isShutdown()) {
      final int cores = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
      pool = new ForkJoinPool(cores);
      graphBuildPool = pool;
    }
    return pool;
  }

  /**
   * Build graph from scratch by reading all active vectors and constructing the graph index.
   * After building, persists the graph to disk and transitions to IMMUTABLE state.
   */
  private void buildGraphFromScratch() {
    buildGraphFromScratch(null, false);
  }

  /**
   * Build graph from scratch with optional progress callback.
   *
   * @param graphCallback   Optional callback for graph build progress
   * @param compactDataFile When true the index data file is also rewritten with only the live entries, which is how
   *                        {@code COMPACT INDEX} reclaims the superseded ones. Automatic rebuilds pass false: they
   *                        run unattended and must not swap files under the writers.
   */
  private void buildGraphFromScratch(final GraphBuildCallback graphCallback, final boolean compactDataFile) {
    // buildGraphFromScratchWithRetry() reads pages directly and rebuilds vectorIndex
    // No need to reload here - just call the retry logic directly
    buildGraphFromScratchWithRetry(graphCallback, compactDataFile);
  }

  /**
   * Internal implementation of buildGraphFromScratch.
   * Always reads directly from pages to avoid race conditions with concurrent VectorLocationIndex modifications.
   *
   * @param graphCallback Optional callback for graph build progress
   */
  private void buildGraphFromScratchWithRetry(final GraphBuildCallback graphCallback, final boolean compactDataFile) {
    // Serialize graph builds for this index. The index write lock used to do this implicitly by covering the
    // whole preparation phase; now that the O(index size) validation runs unlocked (issue #5391), two builds
    // could interleave their vectorIndex re-sync and their ordinal-map publication and leave a searcher with an
    // ordinal map pointing at locations another build had just cleared. Builders wait on this mutex; searches
    // and inserts never touch it, so it does not reintroduce the stall.
    graphBuildLock.lock();
    try {
      buildGraphFromScratchExclusively(graphCallback, compactDataFile);
    } finally {
      graphBuildLock.unlock();
    }
  }

  private void buildGraphFromScratchExclusively(final GraphBuildCallback graphCallback, final boolean compactDataFile) {
    // Reset live builder — full rebuild creates a new graph with different ordinal mapping
    if (liveBuilder != null) {
      try {
        liveBuilder.close();
      } catch (final Exception ignored) {
      }
      liveBuilder = null;
      liveVectorValues = null;
    }

    // Snapshot the next vector ID so we know which delta entries were included in this build
    final int deltaSnapshotId = nextId.get();
    // Snapshot mutation counter so we only subtract mutations present at build start (not concurrent ones)
    final int mutationsAtBuildStart = mutationsSinceSerialize.get();
    // Publish that the snapshot has been taken: mutations recorded after this point survive the build.
    rebuildSnapshotGeneration++;

    // Always have a progress reporter: if caller didn't provide one, log throttled progress every ~5s
    final GraphBuildCallback effectiveGraphCallback;
    if (graphCallback != null) {
      effectiveGraphCallback = graphCallback;
    } else {
      final long[] lastLogTimeMs = { System.currentTimeMillis() };
      final int[] lastLoggedProcessed = { -1 };
      effectiveGraphCallback = (phase, processedNodes, totalNodes, vectorAccesses) -> {
        if (totalNodes <= 0)
          return;

        final long now = System.currentTimeMillis();
        final boolean progressed = processedNodes != lastLoggedProcessed[0];
        final boolean timeElapsed = now - lastLogTimeMs[0] >= 5000;
        final boolean reachedEnd = processedNodes >= totalNodes && lastLoggedProcessed[0] != totalNodes;
        final boolean shouldLog = progressed && (timeElapsed || reachedEnd);

        if (shouldLog) {
          if (isGraphBuildDiagnosticsEnabled()) {
            final GraphBuildDiagnosticsSnapshot diagnostics = captureGraphBuildDiagnostics();
            LogManager.instance().log(this, Level.INFO,
                "Graph build %s: %d/%d (vector accesses=%d, heap=%.1f/%.1fMB, offheap=%.1fMB, files=%.1fMB [%s])",
                phase, processedNodes, totalNodes, vectorAccesses,
                diagnostics.heapUsedMb(), diagnostics.heapMaxMb(), diagnostics.offHeapMb(),
                diagnostics.totalFilesMb(), diagnostics.fileBreakdown);
          } else {
            LogManager.instance().log(this, Level.INFO,
                "Graph build %s: %d/%d (vector accesses=%d)", phase, processedNodes, totalNodes, vectorAccesses);
          }
          lastLogTimeMs[0] = now;
          lastLoggedProcessed[0] = processedNodes;
        }
      };
    }
    // CRITICAL FIX: Collect vectors DIRECTLY from pages instead of from vectorIndex.
    // This avoids race conditions where concurrent replication adds entries to vectorIndex
    // that don't yet exist on disk pages. We iterate pages and read what's actually persisted.
    final Map<RID, VectorEntryForGraphBuild> ridToLatestVector = new HashMap<>(Math.max(16, vectorIndex.size() * 4 / 3));
    final int[] totalEntriesRead = { 0 };
    final int[] filteredDeletedVectors = { 0 };

    final DatabaseInternal database = getDatabase();

    // Read from compacted sub-index if it exists, then from the mutable one: pages are parsed in write order, so
    // merging them with "the entry of highest id wins, ties go to the later one" reproduces the LSM read semantics.
    // A tombstone carries the SAME id as the entry it kills, which is why ties must go to the later entry: dropping
    // tombstones here (as this loop used to do) left every deleted RID in the live set, so the next rebuild put the
    // deleted vectors back into the graph and into the location index.
    if (compactedSubIndex != null) {
      LSMVectorIndexPageParser.parsePages(database, compactedSubIndex.getFileId(), compactedSubIndex.getTotalPages(),
          getPageSize(), true, entry -> {
            totalEntriesRead[0]++;
            if (entry.deleted)
              filteredDeletedVectors[0]++;
            mergeEntryIntoLiveSet(ridToLatestVector, entry, true);
          });
    }

    // Read from mutable index (its entries are newer than the compacted ones)
    LSMVectorIndexPageParser.parsePages(database, getFileId(), getTotalPages(), getPageSize(), false, entry -> {
      totalEntriesRead[0]++;
      if (entry.deleted)
        filteredDeletedVectors[0]++;
      mergeEntryIntoLiveSet(ridToLatestVector, entry, false);
    });

    // Build ordinal mapping from deduplicated vectors read directly from pages
    final int[] activeVectorIds = ridToLatestVector.values().stream().mapToInt(v -> v.vectorId).sorted().toArray();

    // Log statistics
    if (filteredDeletedVectors[0] > 0)
      LogManager.instance().log(this, Level.INFO,
          "Graph build from pages: %d total entries, %d deleted, %d active for graph",
          totalEntriesRead[0], filteredDeletedVectors[0], activeVectorIds.length);

    // SECONDARY DEFENSE (issue #3722): Cross-check page-parsed vectors against actual document count.
    // If pages have corrupted entries (e.g., old-format tombstones), the parser may miss many vectors.
    // In that case, fall back to scanning documents directly to rebuild the vector list.
    boolean documentScanPerformed = false;
    if (metadata.associatedBucketId != -1) {
      try {
        final Bucket bucket = database.getSchema().getBucketById(metadata.associatedBucketId);
        final long docCount = database.countBucket(bucket.getName());
        if (ridToLatestVector.size() < docCount * 8 / 10) {
          LogManager.instance().log(this, Level.WARNING,
              """
              Page-parsed vectors (%d) significantly less than document count (%d) for index %s. \
              Falling back to document scan to recover missing vectors.""",
              ridToLatestVector.size(), docCount, indexName);

          // Scan all documents in the associated bucket to find vectors missing from the page-parsed set
          final String vectorProp =
              metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ? metadata.propertyNames.getFirst() :
                  "vector";
          database.scanBucket(bucket.getName(), record -> {
            final Document doc = (Document) record;
            final RID rid = doc.getIdentity();
            if (!ridToLatestVector.containsKey(rid)) {
              // Document exists but was not found in pages - add it with a synthetic vector ID.
              final Object vectorObj = doc.get(vectorProp);
              if (vectorObj != null) {
                try {
                  final float[] vector = VectorUtils.toFloatArray(vectorObj, metadata.encoding);
                  if (vector.length == metadata.dimensions && !VectorUtils.isZeroVector(vector)) {
                    final int syntheticId = nextId.getAndIncrement();
                    ridToLatestVector.put(rid, new VectorEntryForGraphBuild(syntheticId, rid, false, -1));
                  }
                } catch (final IllegalArgumentException ignored) {
                  // unsupported vector type for this index encoding - skip
                }
              }
            }
            return true;
          });

          documentScanPerformed = true;
          LogManager.instance().log(this, Level.INFO,
              "After document scan fallback: %d active vectors for graph build (was %d from pages only)",
              ridToLatestVector.size(), activeVectorIds.length);
        }
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Document count cross-check failed for index %s: %s", indexName, e.getMessage());
      }
    }

    // TERTIARY DEFENSE (issue #3722): Cross-check against in-memory vectorIndex.
    // The vectorIndex contains entries from pages loaded at startup PLUS entries added via put().
    // If page parsing missed vectors (corruption, stale page count, etc.) but they exist in
    // the in-memory vectorIndex, recover them. This catches cases where the document scan
    // fallback could not trigger (e.g., typeName is null, countType failed, or the type has
    // multiple buckets making the count comparison unreliable).
    if (!documentScanPerformed) {
      final int inMemorySize = vectorIndex.size();
      if (inMemorySize > ridToLatestVector.size()) {
        final int pageParsedCount = ridToLatestVector.size();
        // Recover entries from the in-memory vectorIndex that are missing from pages
        vectorIndex.getAllVectorIds().forEach(vectorId -> {
          final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
          if (loc != null && !loc.deleted && !ridToLatestVector.containsKey(loc.rid))
            ridToLatestVector.put(loc.rid,
                new VectorEntryForGraphBuild(vectorId, loc.rid, loc.isCompacted, loc.absoluteFileOffset));
        });
        if (ridToLatestVector.size() > pageParsedCount)
          LogManager.instance().log(this, Level.WARNING,
              "Recovered %d vectors from in-memory index (page-parsed: %d, in-memory: %d) for index %s",
              ridToLatestVector.size() - pageParsedCount, pageParsedCount, inMemorySize, indexName);
      }
    }

    // COMPACTION (issue #5516 follow-up): the live set just computed IS the compacted content of this index, so
    // when the caller asked for a compaction the data file is rewritten here, from the very same set the graph is
    // about to be built on. Doing it in the rebuild instead of in a separate compactor is what keeps the two from
    // disagreeing on what "live" means. Entries are re-pointed at the new file below, so everything downstream
    // (validation, preload, vectorIndex re-sync, graph build) transparently uses the compacted file.
    // A completed rewrite already re-published the location index against the new file, inside the critical section
    // that swapped it - re-publishing an identical copy below would only reopen the window it closed (issue #5568).
    final boolean locationIndexAlreadyPublished = compactDataFile && rewriteDataFileWithLiveEntries(
        ridToLatestVector.values());

    // Rebuild ordinal mapping (may have changed after document scan fallback)
    final int[] finalActiveVectorIdsFromPages = ridToLatestVector.values().stream()
        .mapToInt(v -> v.vectorId).sorted().toArray();

    // Acquire the write lock only to re-sync vectorIndex with what the pages actually hold. The
    // validation/preload phase that follows is O(active vectors) - one document read plus one vector copy each -
    // and used to run under this same lock, freezing every insert and every search on the index for the whole
    // rebuild. On a 200K-vector index that is minutes of blocked commits, surfacing as commit lock timeouts and
    // multi-minute query stalls (issue #5391). It only touches thread-confined structures plus the internally
    // synchronized vectorIndex, so it now runs unlocked.
    lock.writeLock().lock();
    final int[] vectorIds;
    try {
      // CRITICAL: If we couldn't read any live entry from pages, DON'T replace vectorIndex - use what's already
      // in memory! Publishing an empty location index here would drop entries that only exist in RAM.
      if (!ridToLatestVector.isEmpty()) {
        // Update vectorIndex to match what we found on pages (sync it with disk state).
        if (!locationIndexAlreadyPublished)
          publishLocationIndex(ridToLatestVector.values());
        vectorIds = finalActiveVectorIdsFromPages; // Use vector IDs from pages (may include doc-scan fallback)
      } else {
        // Build vector IDs from existing vectorIndex
        vectorIds = vectorIndex.getAllVectorIds().filter(id -> {
          final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(id);
          return loc != null && !loc.deleted;
        }).sorted().toArray();

        // Pages holding no live vector is the ordinary state of a brand new index, and of one whose records have
        // all been deleted: there the tombstones cancel every entry out. Neither says the database is going away,
        // and neither is worth a level operators alert on. Only a disagreement between the two sources is: memory
        // still knows about live vectors the pages did not yield, which is what an interrupted rebuild or an
        // unreadable data file looks like. Report what was observed, never a cause that was not checked.
        LogManager.instance().log(this, vectorIds.length > 0 ? Level.WARNING : Level.FINE,
            "No live vector read from the pages of index %s (%d entries parsed): building the graph from the in-memory index instead (%d live of %d entries)",
            indexName, totalEntriesRead[0], vectorIds.length, vectorIndex.size());
      }
    } finally {
      lock.writeLock().unlock();
    }

    final RandomAccessVectorValues vectors;
    final int[] finalActiveVectorIds;
    {
      // Create a SNAPSHOT of vectorIndex for JVector to use safely
      final String vectorProp =
          metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ? metadata.propertyNames.get(0) :
              "vector";

      // CRITICAL FIX: Validate vectors before building graph to filter out deleted documents
      // When a document is deleted, getVector() returns null which breaks JVector index building
      final int expectedSize = vectorIds.length;
      final Map<Integer, VectorLocationIndex.VectorLocation> vectorLocationSnapshot = new HashMap<>(expectedSize * 4 / 3 + 1);

      // Issue #3144: for inline-quantized indexes (INT8/BINARY) the graph builder reads vectors
      // straight from index pages on any thread (getImmutablePage needs no DatabaseContext), so we
      // only warm the bounded build cache instead of holding a full second on-heap copy of the whole
      // vector set. Document-based indexes (NONE/PRODUCT) still need a full preload because JVector's
      // worker threads cannot lookupByRID without a transaction context bound to the thread.
      final boolean inlineQuantization = metadata.quantizationType == VectorQuantizationType.INT8
          || metadata.quantizationType == VectorQuantizationType.BINARY;
      final int graphBuildCacheSize = computeGraphBuildCacheCapacity(expectedSize, inlineQuantization);
      // Never preload more than the cache can hold: the surplus used to be read, boxed and then dropped.
      final int preloadBudget = Math.min(expectedSize, graphBuildCacheSize);
      final Map<Integer, VectorFloat<?>> preloadedVectors = new HashMap<>(preloadBudget * 4 / 3 + 1);
      final List<Integer> validVectorIds = new ArrayList<>(expectedSize);
      int skippedDeletedDocs = 0;

      // Index the delta buffer by vector id so the document-based validation below can reuse the vectors it
      // already holds in RAM (issue #5391). Only references are stored, so this costs a few tens of bytes per
      // pending vector and saves a record read plus a full vector copy for each of them.
      final Map<Integer, VectorFloat<?>> deltaSnapshotById;
      if (inlineQuantization)
        deltaSnapshotById = Collections.emptyMap();
      else {
        final List<DeltaVectorEntry> deltaSnapshot = deltaVectors;
        deltaSnapshotById = new HashMap<>(deltaSnapshot.size() * 4 / 3 + 1);
        for (final DeltaVectorEntry e : deltaSnapshot)
          deltaSnapshotById.put(e.vectorId, e.vector);
      }

      // Progress tracking for validation phase
      final int totalVectorsToValidate = vectorIds.length;
      int validatedCount = 0;
      final long VALIDATION_PROGRESS_INTERVAL = 1000;

      int validationAttempts = 0;
      int validationSuccesses = 0;
      int validationNullVectors = 0;
      int validationWrongDimensions = 0;
      int validationAllZeros = 0;

      for (int vectorId : vectorIds) {
        final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
        if (loc != null && !loc.deleted) {
          validationAttempts++;

          // CRITICAL FIX: When INT8/BINARY quantization is enabled, vectors are stored in index pages, not documents
          // Skip expensive document validation and trust the page data we already read
          // NOTE: PRODUCT quantization does NOT store vectors in pages - it uses a separate PQ file built AFTER
          // graph construction
          // So for PRODUCT, we must read from documents just like NONE
          if (inlineQuantization) {
            // With INT8/BINARY quantization: vectors are in index pages, document validation not needed
            // Just validate that we can read the quantized vector
            try {
              final float[] vector = readVectorFromOffset(loc.absoluteFileOffset, loc.isCompacted);
              if (vector != null && vector.length == metadata.dimensions) {
                if (!VectorUtils.isZeroVector(vector)) {
                  vectorLocationSnapshot.put(vectorId, loc);
                  validVectorIds.add(vectorId);
                  // Only warm the cache up to the budget; the rest are re-read from index pages
                  // lazily during the build (issue #3144).
                  if (preloadedVectors.size() < preloadBudget)
                    preloadedVectors.put(vectorId, vts.createFloatVector(vector));
                  validationSuccesses++;
                } else {
                  validationAllZeros++;
                  skippedDeletedDocs++;
                }
              } else {
                // Could not read quantized vector - skip
                if (vector == null) {
                  validationNullVectors++;
                } else {
                  validationWrongDimensions++;
                }
                skippedDeletedDocs++;
              }
            } catch (final Exception e) {
              // Error reading quantized vector - skip
              skippedDeletedDocs++;
            }
          } else {
            // Without quantization the vector only lives in the document, so every vector costs one record
            // read plus one on-heap copy. The delta buffer already holds the converted vector for everything
            // ingested since the last rebuild - which under sustained ingestion is most of what this rebuild is
            // absorbing - so reuse those instances instead of re-reading and re-copying them (issue #5391).
            // Delta entries are keyed by the same monotonic vector id the pages carry, and an update removes
            // the old entry and adds a new id, so a hit here can never be a stale version of the vector.
            final VectorFloat<?> fromDelta = deltaSnapshotById.get(vectorId);
            if (fromDelta != null) {
              if (fromDelta.length() == metadata.dimensions && !VectorUtils.isZeroVector(fromDelta)) {
                vectorLocationSnapshot.put(vectorId, loc);
                validVectorIds.add(vectorId);
                if (preloadedVectors.size() < preloadBudget)
                  preloadedVectors.put(vectorId, fromDelta);
              }
            } else {
              // Without quantization: validate by reading from document.
              try {
                final Record record = database.lookupByRID(loc.rid, false);

                final Document doc = (Document) record;
                final Object vectorObj = doc.get(vectorProp);

                if (vectorObj != null) {
                  final float[] vector = VectorUtils.toFloatArray(vectorObj, metadata.encoding);

                  if (vector.length == metadata.dimensions && !VectorUtils.isZeroVector(vector)) {
                    vectorLocationSnapshot.put(vectorId, loc);
                    validVectorIds.add(vectorId);
                    if (preloadedVectors.size() < preloadBudget)
                      preloadedVectors.put(vectorId, vts.createFloatVector(vector));
                  }
                }

              } catch (final RecordNotFoundException e) {
                // Document was deleted - skip this vector
                skippedDeletedDocs++;
              } catch (final Exception e) {
                // Other errors - skip this vector
                skippedDeletedDocs++;
              }
            }
          }
        }

        // Report validation progress
        validatedCount++;
        if (effectiveGraphCallback != null && validatedCount % VALIDATION_PROGRESS_INTERVAL == 0) {
          effectiveGraphCallback.onGraphBuildProgress("validating", validatedCount, totalVectorsToValidate, 0);
        }
      }

      // Final validation progress report
      if (effectiveGraphCallback != null && validatedCount > 0) {
        effectiveGraphCallback.onGraphBuildProgress("validating", validatedCount, totalVectorsToValidate, 0);
      }

      if (skippedDeletedDocs > 0) {
        LogManager.instance()
            .log(this, Level.INFO, "Filtered out %d vectors with deleted/invalid documents during graph build",
                skippedDeletedDocs);
      }

      // Use validated vector IDs instead of unfiltered ones
      // IMPORTANT: Must be sorted to match the ordinal order used when loading from disk
      final int[] filteredVectorIds = validVectorIds.stream().sorted().mapToInt(Integer::intValue).toArray();
      finalActiveVectorIds = filteredVectorIds;

      if (filteredVectorIds.length == 0) {
        // Re-acquire the write lock only to publish index state (issue #5391).
        lock.writeLock().lock();
        try {
          this.ordinalToVectorId = filteredVectorIds;
          this.graphIndex = null;
          mutationsSinceSerialize.addAndGet(-mutationsAtBuildStart);
          this.graphState = deltaVectors.isEmpty() ? GraphState.IMMUTABLE : GraphState.MUTABLE;
        } finally {
          lock.writeLock().unlock();
        }
        LogManager.instance().log(this, Level.INFO, "No vectors to index, graph is null for index: " + indexName);
        return;
      }

      LogManager.instance().log(this, Level.INFO, "Building graph with %d vectors using property '%s' (cache enabled: size=%d)",
          filteredVectorIds.length, vectorProp, graphBuildCacheSize);

      // Create lazy-loading vector values that reads vectors from documents or index pages (if quantized)
      final ArcadePageVectorValues pageVectors = new ArcadePageVectorValues(database, metadata.dimensions, vectorProp,
          vectorLocationSnapshot,  // Use immutable snapshot
          finalActiveVectorIds, this,  // Pass LSM index reference for quantization support
          graphBuildCacheSize  // Pass configurable cache size
      );

      // Pre-populate cache with vectors validated during the validation phase above.
      // This ensures JVector's parallel ForkJoinPool threads can access vectors
      // from cache without needing a DatabaseContext for lookupByRID.
      for (final Map.Entry<Integer, VectorFloat<?>> entry : preloadedVectors.entrySet())
        pageVectors.putInCache(entry.getKey(), entry.getValue());
      preloadedVectors.clear(); // Free memory

      vectors = pageVectors;

      reportNonUnitVectorsOnDotProduct(pageVectors, finalActiveVectorIds.length);

      // Publish the ordinal mapping and the build state under the write lock so searches never observe an
      // ordinal map that disagrees with the vectors snapshot they captured (issue #4581).
      lock.writeLock().lock();
      try {
        this.ordinalToVectorId = filteredVectorIds;
        this.graphState = GraphState.MUTABLE;
      } finally {
        lock.writeLock().unlock();
      }
    }

    try {
      // Build the graph index using JVector 4.0 API (WITHOUT holding our lock - JVector uses parallel threads)
      LogManager.instance()
          .log(this, Level.INFO,
              "Building JVector graph index with " + vectors.size() + " vectors for index: " + indexName);

      // Create BuildScoreProvider for index construction.
      // When PRODUCT quantization is enabled, use PQ-accelerated build: compute PQ first, then build graph
      // using PQ scores (zero disk I/O during graph construction, ~10-50x faster).
      final BuildScoreProvider scoreProvider;
      PQVectors earlyPqVectors = null;
      ProductQuantization earlyPq = null;

      if (metadata.quantizationType == VectorQuantizationType.PRODUCT && isPQTrainable(vectors.size())) {
        LogManager.instance().log(this, Level.INFO,
            "PQ-accelerated graph build: computing PQ codebooks before graph construction for index: %s", indexName);
        final long pqStart = System.currentTimeMillis();

        // Compute PQ subspaces
        final int pqSubspaces = resolvePQSubspaces();

        // Train PQ codebooks from pre-loaded vectors (all in cache, no disk I/O)
        earlyPq = ProductQuantization.compute(vectors, pqSubspaces, metadata.pqClusters, metadata.pqCenterGlobally);

        // Encode all vectors. An ordinal whose vector cannot be read yields the placeholder, not null, and encoding
        // that would put a PQ code carrying the placeholder's own direction into the table - a code the approximate
        // search then scores as if it meant something. setZero records "no information here" instead, and unlike
        // skipping the ordinal it keeps the code array dense (issue #5558).
        final MutablePQVectors mutablePqVectors = new MutablePQVectors(earlyPq);
        final ArcadePageVectorValues earlyPageValues =
            vectors instanceof final ArcadePageVectorValues p ? p : null;
        for (int i = 0; i < vectors.size(); i++) {
          final VectorFloat<?> vector = vectors.getVector(i);
          if (vector == null || (earlyPageValues != null && earlyPageValues.isDeletedSentinel(vector)))
            mutablePqVectors.setZero(i);
          else
            mutablePqVectors.encodeAndSet(i, vector);
        }
        earlyPqVectors = mutablePqVectors;

        LogManager.instance().log(this, Level.INFO,
            "PQ computed in %d ms (%d vectors encoded, %d subspaces). Using PQ scores for graph build.",
            System.currentTimeMillis() - pqStart, mutablePqVectors.count(), pqSubspaces);

        scoreProvider = BuildScoreProvider.pqBuildScoreProvider(metadata.similarityFunction, earlyPqVectors);
      } else {
        if (metadata.quantizationType == VectorQuantizationType.PRODUCT)
          LogManager.instance().log(this, Level.INFO,
              "Index %s holds %d vectors, fewer than the %d configured PQ clusters: building the graph with exact scores and "
                  + "without PQ. A later rebuild will enable PQ once the index is large enough (issue #5417)",
              indexName, vectors.size(), metadata.pqClusters);

        scoreProvider = BuildScoreProvider.randomAccessScoreProvider(vectors, metadata.similarityFunction);
      }

      // Build the graph index (parallel operation - no lock held)
      // Use a dedicated pool so we can cancel building on shutdown via shutdownNow()
      final ForkJoinPool buildPool = getOrCreateGraphBuildPool();
      final ImmutableGraphIndex builtGraph;
      try (final GraphIndexBuilder builder = new GraphIndexBuilder(
          scoreProvider,
          metadata.dimensions,
          metadata.maxConnections,  // M parameter (graph degree)
          metadata.beamWidth,       // efConstruction (construction search depth)
          metadata.neighborOverflowFactor,    // neighbor overflow factor (default: 1.2)
          metadata.alphaDiversityRelaxation,  // alpha diversity relaxation (default: 1.2)
          metadata.addHierarchy,
          true,            // enable concurrent updates
          buildPool,       // simdExecutor - dedicated pool for cancellation support
          buildPool)) {    // parallelExecutor

        // Start progress monitoring thread if callback provided
        final Thread progressMonitor;
        final AtomicBoolean buildComplete = new AtomicBoolean(false);
        if (effectiveGraphCallback != null) {
          final int totalNodes = vectors.size();
          progressMonitor = new Thread(() -> {
            try {
              while (!buildComplete.get()) {
                // Poll JVector's internal state
                final int nodesAdded = builder.getGraph().getIdUpperBound();
                final int insertsInProgress = builder.insertsInProgress();

                // Report progress
                effectiveGraphCallback.onGraphBuildProgress("building", nodesAdded, totalNodes,
                    nodesAdded + insertsInProgress);

                // Sleep briefly before next poll
                Thread.sleep(100); // Poll every 100ms
              }
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (final Exception e) {
              LogManager.instance().log(this, Level.WARNING,
                  "Error in graph build progress monitor: " + e.getMessage());
            }
          }, "JVector-Progress-Monitor-" + indexName);
          progressMonitor.setDaemon(true);
          progressMonitor.start();
        } else {
          progressMonitor = null;
        }

        try {
          builtGraph = builder.build(vectors);
        } finally {
          // Stop progress monitoring. Interrupt as well as flagging: the monitor sleeps 100ms between polls,
          // so flag-only shutdown made every rebuild pay up to an extra 100ms of pure wait (issue #5391).
          buildComplete.set(true);
          if (progressMonitor != null) {
            progressMonitor.interrupt();
            try {
              progressMonitor.join(1000); // Wait up to 1 second for clean shutdown
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        }

        LogManager.instance().log(this, Level.INFO, "JVector graph index built successfully");
      } catch (final AssertionError e) {
        LogManager.instance().log(this, Level.SEVERE, "JVector assertion failed during graph build (dimensions=%d, vectors=%d): %s",
            metadata.dimensions, vectors.size(), e.getMessage());
        throw e;
      }

      // The build occasionally leaves a node with a full out-degree and no in-edges, which no beam search can reach
      // at any efSearch (issue #5615). Collect those vectors here, before the write lock, so neither the O(V+E)
      // walk nor the vector reads stall concurrent searches; they are re-queued into the delta buffer below and
      // stay searchable through the delta scan until the next rebuild wires them into the graph.
      //
      // Only the from-scratch build needs this. Vectors ingested through the live builder are already in the
      // delta buffer from the moment they are inserted, so an orphan there is served by the delta scan until a
      // rebuild absorbs it - at which point this check runs over it.
      //
      // Re-queueing deliberately does not bump mutationsSinceSerialize: a re-queued vector is not a mutation, and
      // counting it would let an index whose last build orphaned a node rebuild itself forever. Two consequences,
      // both accepted:
      //  - the decrement just above may take the counter to zero and cancel the inactivity rebuild timer, so an
      //    otherwise idle index has no self-scheduled path out of scanning these entries until the next real
      //    mutation. That is the price of not spinning on rebuilds;
      //  - deltaVectors is in-memory, so a restart drops the re-queue. The persisted graph still physically holds
      //    the orphan and reports the same node count as the ordinal map, so the staleness check on load sees an
      //    up-to-date graph and the vector is unsearchable again until some mutation triggers a rebuild. Closing
      //    that would mean persisting the orphan set; it is left open because any rebuild re-detects it.
      final List<DeltaVectorEntry> unreachableEntries = new ArrayList<>();
      for (final int ordinal : findUnreachableOrdinals(builtGraph)) {
        if (ordinal >= finalActiveVectorIds.length)
          continue;
        final int vectorId = finalActiveVectorIds[ordinal];
        // Anything at or past the snapshot is already carried over by the trim below; re-adding it here would
        // score the same vector twice in the delta scan.
        if (vectorId >= deltaSnapshotId)
          continue;
        final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
        if (loc == null || loc.deleted)
          continue;
        // getVector() resolves the location through the build snapshot and never returns null: an unreadable
        // ordinal comes back as the sentinel, which would pair a real RID with a meaningless distance in the
        // delta scan. The location check above cannot stand in for this - it reads the live index, not the
        // snapshot, and says nothing about the six document-read failures that also yield the sentinel.
        final VectorFloat<?> vector = vectors.getVector(ordinal);
        if (vector == null || (vectors instanceof final ArcadePageVectorValues pageValues
            && pageValues.isDeletedSentinel(vector)))
          continue;
        unreachableEntries.add(new DeltaVectorEntry(vectorId, loc.rid, vector));
      }
      if (!unreachableEntries.isEmpty())
        LogManager.instance().log(this, Level.WARNING,
            "Graph build left %d of %d vectors unreachable for index %s: serving them from the delta scan until the "
                + "next rebuild", unreachableEntries.size(), finalActiveVectorIds.length, indexName);

      // Reacquire write lock to update graph state
      lock.writeLock().lock();
      try {
        this.graphIndex = builtGraph;
        // Track graph rebuild metric
        metrics.incrementGraphRebuildCount();
        // Reset page tracking since we rebuilt from persisted pages
        currentInsertPageNum = -1;

        // Keep only delta entries inserted DURING the rebuild (vectorId >= snapshot)
        final List<DeltaVectorEntry> remaining = new ArrayList<>();
        for (final DeltaVectorEntry e : deltaVectors)
          if (e.vectorId >= deltaSnapshotId)
            remaining.add(e);

        remaining.addAll(unreachableEntries);

        this.deltaVectors = remaining;

        // Subtract only mutations present at build start, preserving concurrent ones
        mutationsSinceSerialize.addAndGet(-mutationsAtBuildStart);

        // Cancel inactivity timer if all mutations were flushed (issue #3737)
        if (mutationsSinceSerialize.get() <= 0)
          cancelInactivityRebuildTimer();

        // Only transition to IMMUTABLE if no new mutations arrived during rebuild
        this.graphState = remaining.isEmpty() ? GraphState.IMMUTABLE : GraphState.MUTABLE;
      } finally {
        lock.writeLock().unlock();
      }

      // Persist graph to disk IMMEDIATELY in its own transaction
      // This ensures the graph is available on next database open (fast restart)
      final LSMVectorIndexGraphFile gf = getOrCreateGraphFile();
      if (gf != null) {
        final int totalNodes = graphIndex.getIdUpperBound();
        LogManager.instance().log(this, Level.FINE, "Writing vector graph to disk for index: %s (nodes=%d)",
            indexName, totalNodes);

        // Report persistence phase start
        if (effectiveGraphCallback != null)
          effectiveGraphCallback.onGraphBuildProgress("persisting", 0, totalNodes, 0);

        // Start a dedicated transaction for graph persistence with chunked commits
        long chunkSizeMB = getTxChunkSize();

        final boolean startedTransaction = !database.isTransactionActive();
        if (startedTransaction) {
          database.begin();
          database.getTransaction().setUseWAL(false);
        } else {
          database.getTransaction().setUseWAL(false);
        }

        final ChunkCommitCallback chunkCallback = bytesWritten -> {
          LogManager.instance().log(this, Level.INFO,
              "Graph persistence chunk complete: %.1fMB written", bytesWritten / (1024.0 * 1024.0));

          // Commit current transaction
          database.commit();

          // Start new transaction and disable WAL
          database.begin();
          database.getTransaction().setUseWAL(false);
        };

        try {
          gf.writeGraph(graphIndex, vectors, chunkSizeMB, chunkCallback, earlyPq, earlyPqVectors);

          // Report persistence completion
          if (effectiveGraphCallback != null) {
            effectiveGraphCallback.onGraphBuildProgress("persisting", totalNodes, totalNodes, 0);
          }

          // Commit the transaction to persist graph pages
          if (startedTransaction) {
            database.commit();
            LogManager.instance().log(this, Level.FINE, "Vector graph persisted and committed for index: %s",
                indexName);
          } else {
            database.commit();
            LogManager.instance()
                .log(this, Level.FINE, "Vector graph persisted (transaction managed by caller) for index: %s",
                    indexName);
          }

          // When storeVectorsInGraph is enabled, reload the graph as OnDiskGraphIndex so the
          // current session benefits from inline vector storage immediately. This is safe because
          // the transaction has been committed above, so all graph pages are flushed and visible.
          if (metadata.storeVectorsInGraph) {
            try {
              final OnDiskGraphIndex diskGraph = gf.loadGraph();
              if (diskGraph != null) {
                lock.writeLock().lock();
                try {
                  this.graphIndex = diskGraph;
                  if (deltaVectors.isEmpty())
                    this.graphState = GraphState.IMMUTABLE;
                } finally {
                  lock.writeLock().unlock();
                }
                LogManager.instance().log(this, Level.INFO,
                    "Graph reloaded as OnDiskGraphIndex with inline vectors for index: %s", indexName);
              }
            } catch (final Exception e) {
              LogManager.instance().log(this, Level.WARNING,
                  "Could not reload graph as OnDiskGraphIndex (will use in-memory graph): %s: %s",
                  e.getClass().getSimpleName(), e.getMessage());
            }
          }

          // Persist Product Quantization if PRODUCT quantization is enabled
          if (metadata.quantizationType == VectorQuantizationType.PRODUCT && pqFile != null) {
            if (earlyPq != null && earlyPqVectors != null) {
              // PQ was already computed for accelerated graph build — just persist it
              LogManager.instance().log(this, Level.INFO,
                  "Persisting PQ computed during accelerated graph build for index: %s", indexName);
              pqFile.writePQ(earlyPq, earlyPqVectors);
              this.productQuantization = earlyPq;
              this.pqVectors = earlyPqVectors;
            } else {
              buildAndPersistPQ(vectors);
            }
          }
        } catch (final Exception e) {
          // Rollback on error
          if (startedTransaction) {
            try {
              database.rollback();
            } catch (final Exception rollbackEx) {
              // Ignore rollback errors
            }
          }
          LogManager.instance().log(this, Level.SEVERE,
              "PERSIST: Failed to persist graph for %s (nodes=%d, storeVectorsInGraph=%b, txStatus=%s): %s - %s",
              indexName,
              totalNodes,
              metadata.storeVectorsInGraph,
              database.getTransaction().getStatus(),
              e.getClass().getSimpleName(),
              e.getMessage(),
              e);
          if (LogManager.instance().isDebugEnabled())
            e.printStackTrace();
          // Don't throw - allow the index to continue working, just won't have persisted graph
        }
      } else {
        LogManager.instance().log(this, Level.SEVERE, "PERSIST: graphFile is NULL, cannot persist graph for index: %s", indexName);
      }

      LogManager.instance().log(this, Level.INFO, "Built graph for index: " + indexName);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error building graph from scratch", e);
      throw new IndexException("Error building graph from scratch", e);
    }
  }

  /**
   * Warns once when a {@code DOT_PRODUCT} index is being built over vectors that are not unit length.
   * <p>
   * {@code VectorSimilarityFunction.DOT_PRODUCT} scores {@code (1 + dot(a, b)) / 2} and documents unit length as its
   * precondition: it exists as the cheap path for cosine on data that is already normalized. Honour it and the raw dot
   * product stays in {@code [-1, 1]}, so the score stays in {@code [0, 1]} and zero is a genuine floor, which is what
   * {@link #UNREADABLE_NODE_SCORE} relies on to keep an unreadable node from outranking a real one. Break it and the
   * dot product is unbounded below, real vectors can score under zero, and a tombstone at exactly zero wastes beam
   * budget. It cannot enter a result - {@code LiveVectorBitsFilter} decides membership, not the score - so the cost is
   * recall and work, not a wrong answer. Silently worse rankings than COSINE would have given is the opposite of the
   * reason to pick this metric, hence the warning.
   * <p>
   * The rebuild has already read and validated every vector, so sampling costs no I/O beyond what the cache already
   * holds, and it is bounded at {@link #UNIT_VECTOR_SAMPLE_SIZE} ordinals regardless of index size. Unreadable
   * ordinals are skipped: the placeholder {@code ArcadePageVectorValues} hands back is all ones, whose magnitude is
   * {@code sqrt(dimensions)}, and counting it would report a normalization problem the data does not have.
   *
   * @param vectors     the vector view the graph is about to be built on
   * @param totalActive number of active ordinals in that view
   */
  private void reportNonUnitVectorsOnDotProduct(final ArcadePageVectorValues vectors, final int totalActive) {
    if (metadata.similarityFunction != VectorSimilarityFunction.DOT_PRODUCT || nonUnitVectorsReported.get())
      return;

    final int sampleSize = Math.min(totalActive, UNIT_VECTOR_SAMPLE_SIZE);
    int sampled = 0;
    int nonUnit = 0;

    for (int ordinal = 0; ordinal < sampleSize; ordinal++) {
      final VectorFloat<?> vector = vectors.getVector(ordinal);
      if (vector == null || vectors.isDeletedSentinel(vector))
        continue;

      double magnitudeSquared = 0.0;
      for (int i = 0; i < vector.length(); i++) {
        final float component = vector.get(i);
        magnitudeSquared += (double) component * component;
      }

      ++sampled;
      if (magnitudeSquared < MIN_UNIT_MAGNITUDE_SQUARED || magnitudeSquared > MAX_UNIT_MAGNITUDE_SQUARED)
        ++nonUnit;
    }

    if (nonUnit == 0 || !nonUnitVectorsReported.compareAndSet(false, true))
      return;

    LogManager.instance().log(this, Level.WARNING,
        "Vector index '%s' uses DOT_PRODUCT but %d of the %d sampled vectors (%d%%) are not unit length. DOT_PRODUCT is "
            + "defined only for normalized vectors, so search quality is degraded: normalize the vectors on ingest or "
            + "recreate the index with COSINE. This is reported once per index",
        indexName, nonUnit, sampled, nonUnit * 100 / sampled);
  }

  private long getTxChunkSize() {
    long chunkSizeMB = getDatabase().getConfiguration().getValueAsLong(GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB);
    if (chunkSizeMB <= 0) {
      final long configuredChunkSize = chunkSizeMB;
      chunkSizeMB = 50;
      LogManager.instance()
          .log(this, Level.WARNING, "arcadedb.index.buildChunkSizeMB was %dMB during graph persistence; forcing fallback to 50MB",
              configuredChunkSize);
    }
    return chunkSizeMB;
  }

  /**
   * Product Quantization can only be trained when the training set holds at least as many vectors as clusters per subspace:
   * JVector's k-means++ rejects a cluster count larger than the number of points. A freshly created index, a test fixture or
   * the first seconds of an ingest therefore cannot train a codebook at all (issue #5417).
   * <p>
   * Instead of clamping K down to the vector count, the index simply runs without PQ while it is that small. Clamping would
   * persist a degenerate codebook (one centroid per vector), lose the FusedPQ inline optimization (which requires exactly 256
   * clusters) and pin the index to that codebook until the next rebuild, all to compress a data set that is trivially small.
   * Running without PQ costs nothing at this size: graph construction uses exact scores and searches fall back to an exact
   * scan. The next rebuild from scratch upgrades the index to full PQ as soon as enough vectors are present.
   *
   * @param vectorCount number of vectors available for training
   *
   * @return true when the training set is large enough to compute the configured codebook
   */
  private boolean isPQTrainable(final int vectorCount) {
    return vectorCount >= metadata.pqClusters;
  }

  /**
   * Forget any Product Quantization data currently associated with this index, both in memory and on disk.
   */
  private void discardProductQuantization() {
    this.productQuantization = null;
    this.pqVectors = null;
    if (pqFile != null)
      pqFile.deletePQFile();
  }

  /**
   * Resolve the number of PQ subspaces (M), auto-calculating it when not configured and always making sure it divides the
   * vector dimensions evenly, as required by {@link ProductQuantization}.
   *
   * @return the number of subspaces to use
   */
  private int resolvePQSubspaces() {
    int pqSubspaces = metadata.pqSubspaces;
    if (pqSubspaces <= 0) {
      // Auto-calculate: dimensions/4, capped at 512 subspaces
      pqSubspaces = Math.min(metadata.dimensions / 4, 512);
      // Ensure at least 1 subspace and dimensions are divisible
      pqSubspaces = Math.max(1, pqSubspaces);
      // Ensure dimensions are divisible by subspaces
      while (metadata.dimensions % pqSubspaces != 0 && pqSubspaces > 1)
        pqSubspaces--;
    }

    if (metadata.dimensions % pqSubspaces != 0) {
      LogManager.instance().log(this, Level.WARNING,
          "PQ subspaces (%d) does not divide dimensions (%d) evenly for index %s. Adjusting...",
          pqSubspaces, metadata.dimensions, indexName);

      // Find the largest divisor <= pqSubspaces
      for (int m = pqSubspaces; m >= 1; m--) {
        if (metadata.dimensions % m == 0) {
          pqSubspaces = m;
          break;
        }
      }
    }

    return pqSubspaces;
  }

  /**
   * Build and persist Product Quantization (PQ) data for zero-disk-I/O search.
   * <p>
   * PQ compresses vectors by dividing them into M subspaces and quantizing each
   * subspace to K clusters. This enables approximate search using only in-memory
   * compressed vectors, achieving microsecond-level latency.
   *
   * @param vectors The vector values to encode with PQ
   */
  private void buildAndPersistPQ(final RandomAccessVectorValues vectors) {
    try {
      final int vectorCount = vectors.size();
      if (vectorCount == 0) {
        LogManager.instance().log(this, Level.WARNING, "No vectors to build PQ for index: %s", indexName);
        return;
      }

      if (!isPQTrainable(vectorCount)) {
        // Drop any codebook left over from a previous, larger build: its ordinals no longer match the graph that was
        // just rebuilt, so keeping it would make approximate search score the wrong vectors (issue #5417).
        discardProductQuantization();
        LogManager.instance().log(this, Level.INFO,
            "Skipping PQ build for index %s: %d vectors are fewer than the %d configured PQ clusters. Searches use exact "
                + "scoring until the index grows and is rebuilt (issue #5417)",
            indexName, vectorCount, metadata.pqClusters);
        return;
      }

      LogManager.instance().log(this, Level.INFO,
          "Building Product Quantization for index %s: %d vectors, %d dimensions",
          indexName, vectorCount, metadata.dimensions);

      final long startTime = System.currentTimeMillis();

      // Compute PQ subspaces (M) - auto-calculate if not specified
      final int pqSubspaces = resolvePQSubspaces();

      final int pqClusters = metadata.pqClusters;
      final boolean centerGlobally = metadata.pqCenterGlobally;

      LogManager.instance().log(this, Level.INFO,
          "PQ configuration: M=%d subspaces, K=%d clusters, globalCentering=%b",
          pqSubspaces, pqClusters, centerGlobally);

      // Limit training set size (JVector recommends max 128K vectors for training)
      final int trainingLimit = Math.min(vectorCount, metadata.pqTrainingLimit);

      // Build ProductQuantization codebook
      final ProductQuantization pq = ProductQuantization.compute(
          vectors,           // Training vectors
          pqSubspaces,       // M - number of subspaces
          pqClusters,        // K - clusters per subspace
          centerGlobally     // Global centering
      );

      LogManager.instance().log(this, Level.INFO,
          "PQ codebook computed in %d ms (trained on %d vectors)",
          System.currentTimeMillis() - startTime, trainingLimit);

      // Encode all vectors with PQ
      final long encodeStart = System.currentTimeMillis();
      final MutablePQVectors encodedVectors = new MutablePQVectors(pq);
      // Same as the early-PQ loop above: the placeholder is not null, and a PQ code built from it would carry its
      // direction into approximate scoring. Record "no information here" instead (issue #5558).
      final ArcadePageVectorValues encodePageValues = vectors instanceof final ArcadePageVectorValues p ? p : null;
      for (int i = 0; i < vectorCount; i++) {
        final VectorFloat<?> vector = vectors.getVector(i);
        if (vector == null || (encodePageValues != null && encodePageValues.isDeletedSentinel(vector)))
          encodedVectors.setZero(i);
        else
          encodedVectors.encodeAndSet(i, vector);
      }

      LogManager.instance().log(this, Level.INFO,
          "PQ encoding completed in %d ms (%d vectors encoded)",
          System.currentTimeMillis() - encodeStart, encodedVectors.count());

      // Persist PQ data to file
      pqFile.writePQ(pq, encodedVectors);

      // Cache in memory for immediate zero-disk-I/O search
      this.productQuantization = pq;
      this.pqVectors = encodedVectors;

      final long totalTime = System.currentTimeMillis() - startTime;
      LogManager.instance().log(this, Level.INFO,
          "Product Quantization built and persisted for index %s: %d vectors, %d subspaces, total time %d ms",
          indexName, vectorCount, pqSubspaces, totalTime);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error building PQ for index %s: %s", indexName, e.getMessage());
      // Don't throw - PQ is optional, index can still work with exact search
    }
  }

  /**
   * Initialize the live builder for incremental inserts.
   * Uses lazy-loading GrowableVectorValues — existing vectors are loaded from disk
   * on-demand during beam search, not pre-loaded. This makes initialization O(1)
   * instead of O(n) where n is the number of existing vectors.
   * <p>
   * Caller MUST hold the write lock.
   */
  private void ensureLiveBuilder() {
    if (liveBuilder != null)
      return;

    try {
      final String vectorProp = metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ?
          metadata.propertyNames.getFirst() : "vector";

      // Create GrowableVectorValues with lazy disk fallback — vectors are loaded from
      // ArcadeDB pages/documents on first access and cached in the ConcurrentHashMap.
      // This avoids the O(n) pre-loading that was the bottleneck at 1M+ scale.
      // Bound the on-heap cache (issue #3144). The vectors are already persisted to pages before
      // being added here, and GrowableVectorValues re-reads evicted ordinals lazily from disk, so
      // capping the cache removes a second full copy of the whole vector set during bulk ingest
      // without affecting correctness. Reuse the graph-build cache knob for a single tunable.
      final int liveCacheSize = getGraphBuildCacheSize();
      liveVectorValues = new GrowableVectorValues(
          metadata.dimensions,
          Math.max(1024, Math.min(vectorIndex.size(), liveCacheSize <= 0 ? vectorIndex.size() : liveCacheSize)),
          vectorIndex,
          this,
          getDatabase(),
          vectorProp,
          liveCacheSize
      );

      // Set the count to match existing vectors so size() reports correctly
      final int maxId = vectorIndex.getMaxVectorId();
      if (maxId >= 0) {
        // Touch the max ID to set the count correctly (GrowableVectorValues tracks max ordinal)
        liveVectorValues.addVector(maxId, liveVectorValues.getVector(maxId));
      }

      final BuildScoreProvider scoreProvider = BuildScoreProvider.randomAccessScoreProvider(liveVectorValues,
          metadata.similarityFunction);

      final ForkJoinPool buildPool = getOrCreateGraphBuildPool();
      liveBuilder = new GraphIndexBuilder(
          scoreProvider,
          metadata.dimensions,
          metadata.maxConnections,
          metadata.beamWidth,
          metadata.neighborOverflowFactor,
          metadata.alphaDiversityRelaxation,
          metadata.addHierarchy,
          true, // concurrent
          buildPool, // simdExecutor - dedicated pool for cancellation support
          buildPool  // parallelExecutor
      );

      LogManager.instance().log(this, Level.INFO,
          "Live builder initialized (lazy-loading) for incremental inserts on index: %s (vectorIndex size=%d)",
          indexName, vectorIndex.size());

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not initialize live builder for index %s: %s. Falling back to batch rebuild.",
          indexName, e.getMessage());
      liveBuilder = null;
      liveVectorValues = null;
    }
  }

  /**
   * Rebuild the graph if mutation threshold reached (Phase 5+: Periodic Rebuilds).
   * Rebuilds every N mutations to amortize cost over many operations.
   * Assumes write lock is already held by caller.
   */
  private void rebuildGraphIfNeeded() {
    if (graphState != GraphState.MUTABLE)
      return;

    if (mutationsSinceSerialize.get() < getEffectiveMutationsBeforeRebuild())
      return; // Not enough mutations yet

    LogManager.instance().log(this, Level.INFO,
        "Rebuilding graph after " + mutationsSinceSerialize.get() + " mutations (threshold: " + getEffectiveMutationsBeforeRebuild()
            + ", index: " + indexName + ")");

    try {
      // Rebuild graph from current vectorIndex state
      buildGraphFromScratch();
      // buildGraphFromScratch() resets state and counter
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error rebuilding graph after mutations", e);
      // Don't throw - allow operations to continue, will retry on next threshold
    }
  }

  /**
   * Minimum graph size to use async rebuild. Below this, synchronous rebuild is fast enough.
   * Above this threshold, a full rebuild can take seconds to minutes, so async is preferred.
   */
  private static final int ASYNC_REBUILD_MIN_GRAPH_SIZE = 1000;
  private static final int[] EMPTY_ORDINALS             = new int[0];

  /**
   * Check if the graph needs rebuilding before a search, and trigger the appropriate rebuild strategy.
   * <ul>
   *   <li>If graph was never built (graphIndex == null): synchronous build (no existing graph to search).</li>
   *   <li>If threshold reached and graph is small (&lt; 1000 vectors): synchronous rebuild (fast enough).</li>
   *   <li>If threshold reached and graph is large (&ge; 1000 vectors): async rebuild + search the current graph
   *       (valid but excludes the newest vectors not yet incorporated into the graph topology).</li>
   *   <li>Below threshold: no rebuild — search uses current graph.</li>
   * </ul>
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/3679">Issue #3679</a>
   */
  private void rebuildGraphBeforeSearch() {
    if (graphState != GraphState.MUTABLE || mutationsSinceSerialize.get() <= 0)
      return;

    final int mutations = mutationsSinceSerialize.get();
    final int threshold = getEffectiveMutationsBeforeRebuild();
    final boolean isSmallGraph = graphIndex == null || graphIndex.size() < ASYNC_REBUILD_MIN_GRAPH_SIZE;

    if (isSmallGraph) {
      // Small graph or first-ever build: synchronous rebuild (fast enough to not block noticeably).
      // buildGraphFromScratch() manages its own locking internally - do not wrap in an external
      // write lock, as that would prevent the internal lock release during graph build (issue #3722).
      if (graphState == GraphState.MUTABLE && mutationsSinceSerialize.get() > 0
          && (graphIndex == null || graphIndex.size() < ASYNC_REBUILD_MIN_GRAPH_SIZE))
        buildGraphFromScratch();
    } else if (mutations >= threshold && !asyncRebuildInProgress)
      // Large graph (>= 1000 vectors): async rebuild only when threshold reached.
      // Search uses the current graph (valid, just missing newest vectors) while rebuild runs in background.
      startAsyncGraphRebuild();
  }

  /**
   * Start an asynchronous graph rebuild in a background daemon thread (issue #3679).
   * The current graph remains available for searches while the rebuild is in progress.
   * When the rebuild completes, the new graph is hot-swapped atomically.
   * If a rebuild is already in progress, this method does nothing (the next search will check again).
   * <p>
   * A JVM-wide semaphore limits the number of concurrent rebuilds to prevent OOM kills
   * when multiple indexes trigger rebuilds simultaneously (issue #3868).
   */
  private synchronized void startAsyncGraphRebuild() {
    if (asyncRebuildInProgress)
      return; // Another rebuild is already running

    asyncRebuildInProgress = true;
    final int mutations = mutationsSinceSerialize.get();

    LogManager.instance().log(this, Level.INFO,
        "Starting async graph rebuild (accumulated %d mutations, threshold: %d, index: %s)",
        mutations, getEffectiveMutationsBeforeRebuild(), indexName);

    asyncRebuildThread = new Thread(() -> {
      try {
        // Acquire a rebuild permit to limit concurrent rebuilds across all indexes (issue #3868).
        // This prevents multiple large graph rebuilds from running simultaneously and exhausting
        // heap memory.  The thread blocks here until a permit becomes available.
        REBUILD_SEMAPHORE.acquire();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        asyncRebuildInProgress = false;
        asyncRebuildThread = null;
        return;
      }
      boolean completed = false;
      try {
        LogManager.instance().log(this, Level.INFO,
            "Acquired rebuild permit for index: %s (available permits: %d)",
            indexName, REBUILD_SEMAPHORE.availablePermits());
        buildGraphFromScratch();
        completed = true;
        LogManager.instance().log(this, Level.INFO,
            "Async graph rebuild completed for index: %s", indexName);
      } catch (final Exception | AssertionError e) {
        // AssertionError too: JVector validates with `assert`, and an escaping one would kill this daemon
        // thread past the point where the in-progress flag is cleared.
        if (Thread.currentThread().isInterrupted()) {
          LogManager.instance().log(this, Level.INFO,
              "Async graph rebuild cancelled for index: %s", indexName);
        } else {
          LogManager.instance().log(this, Level.SEVERE,
              "Error during async graph rebuild for index %s: %s", indexName, e.getMessage());
        }
      } finally {
        REBUILD_SEMAPHORE.release();
        asyncRebuildInProgress = false;
        asyncRebuildThread = null;
      }

      // A rebuild only absorbs the vectors that were already persisted when it started; anything ingested
      // while it ran stays in the RAM-resident delta buffer. Waiting for the next search to notice leaves the
      // graph frozen under sustained ingestion - every new vector then piles up in the delta buffer and every
      // query degrades into a full brute-force scan over it (issue #5391). Chain straight into another rebuild
      // instead. Only a rebuild that ran to completion chains, so a failing one cannot spin, and each chained
      // rebuild subtracts the mutations it snapshotted, so the counter cannot be re-served indefinitely without
      // new writes actually arriving.
      if (completed && !Thread.currentThread().isInterrupted() && isValid()
          && mutationsSinceSerialize.get() >= getEffectiveMutationsBeforeRebuild())
        startAsyncGraphRebuild();
    }, "VectorIndex-AsyncRebuild-" + indexName);
    asyncRebuildThread.setDaemon(true);
    asyncRebuildThread.start();
  }

  /**
   * Load vector location metadata from LSM-style pages.
   * Only reads metadata (page location, RID, deleted flag), NOT the actual vector data.
   * This dramatically reduces memory usage and speeds up loading.
   * Reads from all pages, later entries override earlier ones (LSM merge-on-read).
   */
  private void loadVectorsFromPages() {
    try {
      // NOTE: All metadata (dimensions, similarityFunction, maxConnections, beamWidth) comes from schema JSON
      // via applyMetadataFromSchema(). Pages contain only vector data, no metadata.

      LogManager.instance()
          .log(this, Level.FINE, "loadVectorsFromPages START: index=%s, totalPages=%d, vectorIndexSizeBefore=%d",
              null, indexName,
              getTotalPages(), vectorIndex.size());

      int entriesRead = 0;
      int maxVectorId = -1;

      // Load from compacted sub-index first (if it exists)
      if (compactedSubIndex != null) {
        final int compactedEntries = loadVectorsFromFile(compactedSubIndex.getFileId(),
            compactedSubIndex.getTotalPages(), true);
        entriesRead += compactedEntries;
        LogManager.instance()
            .log(this, Level.INFO, "Loaded %d entries from compacted sub-index (fileId=%d)", null, compactedEntries,
                compactedSubIndex.getFileId());
      }

      // Load from mutable index (always present)
      final int mutableEntries = loadVectorsFromFile(getFileId(), getTotalPages(), false);
      entriesRead += mutableEntries;

      // Compute nextId from the maximum vector ID found across both files. Tombstoned ids are not resident
      // (issue #5516) but they were still handed out, so the location index's own high-water mark - which every
      // addOrUpdate advances, tombstones included - is what guarantees an id is never reused.
      maxVectorId = vectorIndex.getAllVectorIds().max().orElse(-1);
      nextId.set(Math.max(maxVectorId + 1, vectorIndex.getNextId()));

      LogManager.instance().log(this, Level.FINE,
          "loadVectorsFromPages DONE: Loaded " + vectorIndex.size() + " vector locations (" + entriesRead
              + " total entries) for index: " + indexName + ", nextId=" + nextId.get() + ", fileId=" + getFileId() +
              ", totalPages="
              + getTotalPages() + (compactedSubIndex != null ?
              ", compactedFileId=" + compactedSubIndex.getFileId() + ", compactedPages=" + compactedSubIndex.getTotalPages() :
              ""));

      // Reset page tracking after loading from disk
      currentInsertPageNum = -1;

      // NOTE: Do NOT call initializeGraphIndex() here - it would cause infinite recursion
      // because buildGraphFromScratch() calls loadVectorsFromPages()
      // Graph initialization is handled separately by the constructor and ensureGraphAvailable()

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error loading vectors from pages", e);
      throw new IndexException("Error loading vectors from pages", e);
    }
  }

  /**
   * Load vector location metadata from a specific file's pages.
   * Uses LSMVectorIndexPageParser to parse page entries.
   *
   * @param fileId      The file ID to load from
   * @param totalPages  The number of pages in that file
   * @param isCompacted True if loading from compacted file, false if from mutable file
   *
   * @return Number of entries read
   */
  private int loadVectorsFromFile(final int fileId, final int totalPages, final boolean isCompacted) {
    LogManager.instance().log(this, Level.FINE,
        "loadVectorsFromFile: fileId=%d, totalPages=%d, isCompacted=%s", fileId, totalPages, isCompacted);

    final int entriesRead = LSMVectorIndexPageParser.parsePages(getDatabase(), fileId, totalPages, getPageSize(),
        isCompacted,
        entry -> vectorIndex.addOrUpdate(entry.vectorId, entry.isCompacted, entry.absoluteFileOffset, entry.rid,
            entry.deleted));

    LogManager.instance().log(this, Level.FINE,
        "loadVectorsFromFile DONE: fileId=%d, entriesRead=%d", fileId, entriesRead);

    return entriesRead;
  }

  /**
   * Persist a single vector and add its location to the vectorIndex.
   * Used during put() operations.
   */
  private void persistVectorWithLocation(final int id, final RID rid, final float[] vector) {
    try {
      // Quantize vector if quantization is enabled
      final VectorQuantizationMetadata qmeta = (VectorQuantizationMetadata) quantizeVector(vector);

      // Calculate variable entry size for this specific entry
      final int vectorIdSize = Binary.getNumberSpace(id);
      final int bucketIdSize = Binary.getNumberSpace(rid.getBucketId());
      final int positionSize = Binary.getNumberSpace(rid.getPosition());
      int entrySize = vectorIdSize + positionSize + bucketIdSize + 1; // +1 for deleted byte
      entrySize += 1; // +1 for quantization type flag (ALWAYS written, even if NONE)

      // Add size for quantized vector data if quantization is enabled
      if (qmeta != null) {
        if (qmeta.getType() == VectorQuantizationType.INT8) {
          final VectorQuantizationMetadata.Int8QuantizationMetadata int8meta =
              (VectorQuantizationMetadata.Int8QuantizationMetadata) qmeta;
          entrySize += 4; // vector length (int)
          entrySize += int8meta.quantized.length; // quantized bytes
          entrySize += 8; // min + max (2 floats)
        } else if (qmeta.getType() == VectorQuantizationType.BINARY) {
          final VectorQuantizationMetadata.BinaryQuantizationMetadata binmeta =
              (VectorQuantizationMetadata.BinaryQuantizationMetadata) qmeta;
          entrySize += 4; // original length (int)
          entrySize += binmeta.packed.length; // packed bytes
          entrySize += 4; // median (float)
        }
      }

      // CRITICAL FIX: Use tracked page number to handle transaction-local pages correctly
      // Initialize from persisted pages only if not set (-1)
      int lastPageNum = currentInsertPageNum;
      if (lastPageNum < 0) {
        // First insert in this session - initialize from persisted pages
        lastPageNum = getTotalPages() - 1;
        if (lastPageNum < 0) {
          lastPageNum = 0;
          createNewVectorDataPage(lastPageNum);
        }
        currentInsertPageNum = lastPageNum;
      }

      // Get current page
      MutablePage currentPage = getDatabase().getTransaction()
          .getPageToModify(new PageId(getDatabase(), getFileId(), lastPageNum), getPageSize(), false);

      // Read page header using MutablePage methods (accounts for PAGE_HEADER_SIZE automatically)
      int offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
      int numberOfEntries = currentPage.readInt(OFFSET_NUM_ENTRIES);

      // Validate offsetFreeContent is sane (detect old-format or corrupted pages)
      if (offsetFreeContent < HEADER_BASE_SIZE || offsetFreeContent > currentPage.getMaxContentSize()) {
        // Old format page or corrupted, create new page
        LogManager.instance()
            .log(this, Level.WARNING, "Invalid offsetFreeContent=%d in page %d (expected range: %d-%d), creating new page",
                offsetFreeContent, lastPageNum, HEADER_BASE_SIZE, currentPage.getMaxContentSize());
        currentPage.writeByte(OFFSET_MUTABLE, (byte) 0);
        lastPageNum++;
        currentInsertPageNum = lastPageNum; // Track the new page number
        currentPage = createNewVectorDataPage(lastPageNum);
        offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        numberOfEntries = 0;
      }

      // Calculate space needed (no pointer table - just header + sequential entries)
      final int availableSpace = currentPage.getMaxContentSize() - offsetFreeContent;

      if (availableSpace < entrySize) {
        // Page is full, mark it as immutable before creating a new page
        currentPage.writeByte(OFFSET_MUTABLE, (byte) 0); // mutable = 0

        lastPageNum++;
        currentInsertPageNum = lastPageNum; // Track the new page number
        currentPage = createNewVectorDataPage(lastPageNum);
        offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        numberOfEntries = 0;
      }

      // Calculate absolute file offset for this entry
      final long pageStartOffset = (long) lastPageNum * getPageSize();
      final long entryFileOffset = pageStartOffset + BasePage.PAGE_HEADER_SIZE + offsetFreeContent;

      // Write entry sequentially using variable-sized encoding
      int bytesWritten = 0;
      bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, id);
      bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, rid.getBucketId());
      bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, rid.getPosition());
      bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, (byte) 0); // not deleted

      // CRITICAL FIX: Always write quantization type byte, even if NONE
      // This ensures readVectorFromOffset() can always read a consistent format
      final VectorQuantizationType quantType = qmeta != null ? qmeta.getType() : VectorQuantizationType.NONE;
      final byte quantOrdinal = (byte) quantType.ordinal();
      bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, quantOrdinal);

      // Write quantized vector data if quantization is enabled
      if (qmeta != null) {

        if (qmeta.getType() == VectorQuantizationType.INT8) {
          final VectorQuantizationMetadata.Int8QuantizationMetadata int8meta =
              (VectorQuantizationMetadata.Int8QuantizationMetadata) qmeta;

          // Write vector length
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, int8meta.quantized.length);

          // Write quantized bytes (bulk array write for performance)
          currentPage.writeByteArray(offsetFreeContent + bytesWritten, int8meta.quantized);
          bytesWritten += int8meta.quantized.length;

          // Write min and max
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(int8meta.min));
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(int8meta.max));

        } else if (qmeta.getType() == VectorQuantizationType.BINARY) {
          final VectorQuantizationMetadata.BinaryQuantizationMetadata binmeta =
              (VectorQuantizationMetadata.BinaryQuantizationMetadata) qmeta;

          // Write original length
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, binmeta.originalLength);

          // Write packed bytes (bulk array write for performance)
          currentPage.writeByteArray(offsetFreeContent + bytesWritten, binmeta.packed);
          bytesWritten += binmeta.packed.length;

          // Write median
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(binmeta.median));
        }
      }

      // Update page header
      numberOfEntries++;
      offsetFreeContent += bytesWritten;
      currentPage.writeInt(OFFSET_FREE_CONTENT, offsetFreeContent);
      currentPage.writeInt(OFFSET_NUM_ENTRIES, numberOfEntries);

      // Add location to vectorIndex with absolute file offset (isCompacted=false for mutable file)
      vectorIndex.addOrUpdate(id, false, entryFileOffset, rid, false);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error persisting vector with location", e);
      throw new IndexException("Error persisting vector with location", e);
    }
  }

  /**
   * Persist deletion tombstones for deleted vectors.
   * Writes deleted entries to pages so they persist across restarts (LSM style).
   *
   * @param deletedIds The ids just tombstoned, all belonging to {@code rid}
   * @param rid        The RID the tombstoned ids point to. Passed in because the locations are released as soon as
   *                   they are tombstoned (issue #5516), so it can no longer be read back from the location index.
   */
  private void persistDeletionTombstones(final List<Integer> deletedIds, final RID rid) {
    try {
      if (deletedIds.isEmpty())
        return;

      // Use tracked page number to handle transaction-local pages correctly
      int lastPageNum = currentInsertPageNum;
      if (lastPageNum < 0) {
        // First operation in this session - initialize from persisted pages
        lastPageNum = getTotalPages() - 1;
        if (lastPageNum < 0) {
          lastPageNum = 0;
          createNewVectorDataPage(lastPageNum);
        }
        currentInsertPageNum = lastPageNum;
      }

      // Append deletion tombstones to pages
      for (final Integer vectorId : deletedIds) {
        // Calculate variable entry size for this specific entry
        final int vectorIdSize = Binary.getNumberSpace(vectorId);
        final int bucketIdSize = Binary.getNumberSpace(rid.getBucketId());
        final int positionSize = Binary.getNumberSpace(rid.getPosition());
        // FIX #3722: Include +1 for quantization type byte to match the format expected by
        // LSMVectorIndexPageParser.parsePages() which always calls skipQuantizationData()
        final int entrySize = vectorIdSize + positionSize + bucketIdSize + 1 + 1; // +1 deleted byte +1 quantType byte

        // Get current page
        MutablePage currentPage = getDatabase().getTransaction()
            .getPageToModify(new PageId(getDatabase(), getFileId(), lastPageNum), getPageSize(), false);

        // Read page header (accounts for PAGE_HEADER_SIZE automatically)
        int offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        int numberOfEntries = currentPage.readInt(OFFSET_NUM_ENTRIES);

        // Validate offsetFreeContent is sane (detect old-format or corrupted pages)
        if (offsetFreeContent < HEADER_BASE_SIZE || offsetFreeContent > currentPage.getMaxContentSize()) {
          // Old format page or corrupted, create new page
          LogManager.instance()
              .log(this, Level.WARNING, "Invalid offsetFreeContent=%d in page %d (expected range: %d-%d), creating new page",
                  offsetFreeContent, lastPageNum, HEADER_BASE_SIZE, currentPage.getMaxContentSize());
          currentPage.writeByte(OFFSET_MUTABLE, (byte) 0);
          lastPageNum++;
          currentInsertPageNum = lastPageNum; // Track the new page number
          currentPage = createNewVectorDataPage(lastPageNum);
          offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
          numberOfEntries = 0;
        }

        // Calculate space needed (no pointer table - just header + sequential entries)
        final int availableSpace = currentPage.getMaxContentSize() - offsetFreeContent;

        if (availableSpace < entrySize) {
          // Page is full, mark it as immutable before creating a new page
          currentPage.writeByte(OFFSET_MUTABLE, (byte) 0);

          lastPageNum++;
          currentInsertPageNum = lastPageNum; // Track the new page number
          currentPage = createNewVectorDataPage(lastPageNum);
          offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
          numberOfEntries = 0;
        }

        // Write deletion tombstone sequentially using variable-sized encoding
        int bytesWritten = 0;
        bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, vectorId);
        bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, rid.getBucketId());
        bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, rid.getPosition());
        bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, (byte) 1); // Mark as deleted
        // FIX #3722: Write quantization type byte (NONE for tombstones) to match the entry format
        // expected by LSMVectorIndexPageParser.skipQuantizationData(). Without this byte, the parser
        // reads into the next entry's data, corrupting all subsequent entries on the same page.
        bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, (byte) VectorQuantizationType.NONE.ordinal());

        // Update page header
        numberOfEntries++;
        offsetFreeContent += bytesWritten;

        currentPage.writeInt(OFFSET_FREE_CONTENT, offsetFreeContent);
        currentPage.writeInt(OFFSET_NUM_ENTRIES, numberOfEntries);
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error persisting deletion tombstones", e);
      throw new IndexException("Error persisting deletion tombstones", e);
    }
  }

  // ========== QUANTIZATION HELPER METHODS ==========

  /**
   * Quantizes a float vector according to the index's quantization type.
   * Returns a QuantizationResult containing the quantized data and metadata needed for dequantization.
   *
   * @param vector The float vector to quantize
   *
   * @return Quantization result with quantized bytes and metadata, or null if quantization is NONE
   */
  private Object quantizeVector(final float[] vector) {
    if (metadata.quantizationType == VectorQuantizationType.NONE)
      return null; // No quantization

    if (metadata.quantizationType == VectorQuantizationType.PRODUCT)
      return null; // PQ is handled separately via LSMVectorIndexPQFile, not in page storage

    if (metadata.quantizationType == VectorQuantizationType.INT8)
      return quantizeToInt8(vector);

    if (metadata.quantizationType == VectorQuantizationType.BINARY)
      return quantizeToBinary(vector);

    throw new IndexException("Unsupported quantization type: " + metadata.quantizationType);
  }

  /**
   * Quantizes a float vector to INT8 using min-max scaling.
   * Algorithm extracted from SQLFunctionVectorQuantizeInt8.
   *
   * @param vector The float vector to quantize
   *
   * @return Int8QuantizationMetadata containing quantized bytes and min/max values
   */
  private VectorQuantizationMetadata.Int8QuantizationMetadata quantizeToInt8(final float[] vector) {
    // Find min and max
    float min = vector[0];
    float max = vector[0];
    for (final float value : vector) {
      if (value < min)
        min = value;
      if (value > max)
        max = value;
    }

    // Quantize to int8 [-128, 127]
    final byte[] quantized = new byte[vector.length];
    if (min == max) {
      // All values are the same
      for (int i = 0; i < vector.length; i++) {
        quantized[i] = 0;
      }
    } else {
      final float range = max - min;
      for (int i = 0; i < vector.length; i++) {
        final float normalized = (vector[i] - min) / range; // [0, 1]
        final int scaled = Math.round(normalized * 255.0f); // [0, 255]
        final byte shifted = (byte) (scaled - 128); // [-128, 127]
        quantized[i] = shifted;
      }
    }

    return new VectorQuantizationMetadata.Int8QuantizationMetadata(quantized, min, max);
  }

  /**
   * Quantizes a float vector to BINARY using median threshold.
   * Algorithm extracted from SQLFunctionVectorQuantizeBinary.
   *
   * @param vector The float vector to quantize
   *
   * @return BinaryQuantizationMetadata containing packed bits and median value
   */
  private VectorQuantizationMetadata.BinaryQuantizationMetadata quantizeToBinary(final float[] vector) {
    // Calculate median
    final float median = calculateMedian(vector);

    // Quantize to binary
    final int byteCount = (vector.length + 7) / 8; // Round up to nearest byte
    final byte[] packed = new byte[byteCount];

    for (int i = 0; i < vector.length; i++) {
      if (vector[i] >= median) {
        // Set bit to 1
        final int byteIndex = i / 8;
        final int bitIndex = i % 8;
        packed[byteIndex] |= 1 << bitIndex;
      }
    }

    return new VectorQuantizationMetadata.BinaryQuantizationMetadata(packed, median, vector.length);
  }

  /**
   * Calculate median of array.
   * Helper method for binary quantization.
   */
  private float calculateMedian(final float[] values) {
    final float[] sorted = values.clone();
    Arrays.sort(sorted);
    if (sorted.length % 2 == 0) {
      return (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2.0f;
    } else {
      return sorted[sorted.length / 2];
    }
  }

  /**
   * Dequantizes a quantized vector back to float array.
   * Algorithm extracted from SQLFunctionVectorDequantizeInt8 and similar.
   *
   * @param quantized The quantized byte array
   * @param qmeta     The quantization metadata containing min/max or median
   *
   * @return The dequantized float vector
   */
  private float[] dequantizeVector(final byte[] quantized, final VectorQuantizationMetadata qmeta) {
    if (qmeta == null || qmeta.getType() == VectorQuantizationType.NONE)
      throw new IndexException("Cannot dequantize: no quantization metadata");

    if (qmeta.getType() == VectorQuantizationType.INT8)
      return dequantizeFromInt8(quantized, (VectorQuantizationMetadata.Int8QuantizationMetadata) qmeta);

    if (qmeta.getType() == VectorQuantizationType.BINARY)
      return dequantizeFromBinary(quantized, (VectorQuantizationMetadata.BinaryQuantizationMetadata) qmeta);

    throw new IndexException("Unsupported quantization type: " + qmeta.getType());
  }

  /**
   * Dequantizes an INT8 quantized vector back to float array.
   * Algorithm extracted from SQLFunctionVectorDequantizeInt8.
   *
   * @param quantized The quantized byte array
   * @param qmeta     The INT8 quantization metadata with min/max
   *
   * @return The dequantized float vector
   */
  private float[] dequantizeFromInt8(final byte[] quantized,
      final VectorQuantizationMetadata.Int8QuantizationMetadata qmeta) {
    final float[] result = new float[quantized.length];
    final float range = qmeta.max - qmeta.min;

    if (range == 0.0f) {
      // All values were the same, return min value for all
      for (int i = 0; i < quantized.length; i++) {
        result[i] = qmeta.min;
      }
    } else {
      for (int i = 0; i < quantized.length; i++) {
        // Reverse quantization: value = (((quantized + 128) / 255) * range) + min
        // Convert signed byte [-128, 127] back to [0, 255] range by adding 128
        final int scaled = (int) quantized[i] + 128; // Convert to [0, 255]
        final float normalized = scaled / 255.0f; // [0, 1]
        result[i] = normalized * range + qmeta.min;
      }
    }

    return result;
  }

  /**
   * Dequantizes a BINARY quantized vector back to float array.
   * Unpacks bits and converts back to float values using the median threshold.
   *
   * @param packed The packed binary data
   * @param qmeta  The BINARY quantization metadata with median
   *
   * @return The dequantized float vector
   */
  private float[] dequantizeFromBinary(final byte[] packed,
      final VectorQuantizationMetadata.BinaryQuantizationMetadata qmeta) {
    final float[] result = new float[qmeta.originalLength];

    for (int i = 0; i < qmeta.originalLength; i++) {
      final int byteIndex = i / 8;
      final int bitIndex = i % 8;
      final boolean bitSet = (packed[byteIndex] & (1 << bitIndex)) != 0;

      // Reconstruct value based on bit: 1 -> above median, 0 -> below median
      // This is a lossy approximation - we just use median or 0 as the values
      result[i] = bitSet ? qmeta.median : 0.0f;
    }

    return result;
  }

  /**
   * Reads a quantized vector from a file offset and dequantizes it.
   * This method reads the quantized vector data stored in index pages and converts it back to float[].
   *
   * @param fileOffset  The absolute file offset where the vector entry starts
   * @param isCompacted Whether to read from compacted or mutable file
   *
   * @return The dequantized float vector, or null if quantization is disabled or vector not found
   */
  protected float[] readVectorFromOffset(final long fileOffset, final boolean isCompacted) {
    try {
      // If no quantization is enabled, return null (caller should fetch from document)
      if (metadata.quantizationType == VectorQuantizationType.NONE)
        return null;

      // Calculate page number and offset within page
      final int pageSize = getPageSize();
      final int pageNum = (int) (fileOffset / pageSize);
      // NOTE: BasePage read methods automatically add PAGE_HEADER_SIZE, so we don't subtract it here
      final int offsetInPage = (int) (fileOffset % pageSize);

      // CRITICAL: BasePage.read methods automatically add PAGE_HEADER_SIZE to the index,
      // so we need to pass the offset relative to the start of the page CONTENT (after header)
      final int contentOffset = offsetInPage - BasePage.PAGE_HEADER_SIZE;

      // Get the appropriate file ID
      final int fileId = isCompacted ? compactedSubIndex.getFileId() : getFileId();

      // Read the page
      final BasePage page = getDatabase().getPageManager()
          .getImmutablePage(new PageId(getDatabase(), fileId, pageNum), getPageSize(), false, false);

      try {
        // Skip over the entry header (vectorId, bucketId, position, deleted flag)
        // These are variable-sized, so we need to read and skip them
        // NOTE: All positions here are relative to page content (after PAGE_HEADER_SIZE)
        int pos = contentOffset;

        // Read and skip vectorId
        final long[] vectorIdAndSize = page.readNumberAndSize(pos);
        pos += (int) vectorIdAndSize[1];
        // Read and skip bucketId
        final long[] bucketIdAndSize = page.readNumberAndSize(pos);
        pos += (int) bucketIdAndSize[1];
        // Read and skip position
        final long[] positionAndSize = page.readNumberAndSize(pos);
        pos += (int) positionAndSize[1];
        // Skip deleted flag
        pos += 1;

        // Read quantization type flag
        final byte quantTypeOrdinal = page.readByte(pos);
        pos += 1;

        // Validate quantization type ordinal before converting to enum
        if (quantTypeOrdinal < 0 || quantTypeOrdinal >= VectorQuantizationType.values().length)
          return null;

        final VectorQuantizationType quantType = VectorQuantizationType.values()[quantTypeOrdinal];

        if (quantType == VectorQuantizationType.INT8) {
          // Read vector length
          final int vectorLength = page.readInt(pos);
          pos += 4;

          // Read quantized bytes (bulk array read for performance)
          final byte[] quantized = new byte[vectorLength];
          page.readByteArray(pos, quantized, 0, vectorLength);
          pos += vectorLength;

          // Read min and max
          final float min = Float.intBitsToFloat(page.readInt(pos));
          pos += 4;
          final float max = Float.intBitsToFloat(page.readInt(pos));

          // Dequantize
          final VectorQuantizationMetadata.Int8QuantizationMetadata qmeta =
              new VectorQuantizationMetadata.Int8QuantizationMetadata(
                  quantized, min, max);
          return dequantizeFromInt8(quantized, qmeta);

        } else if (quantType == VectorQuantizationType.BINARY) {
          // Read original length
          final int originalLength = page.readInt(pos);
          pos += 4;

          // Read packed bytes (bulk array read for performance)
          final int byteCount = (originalLength + 7) / 8;
          final byte[] packed = new byte[byteCount];
          page.readByteArray(pos, packed, 0, byteCount);
          pos += byteCount;

          // Read median
          final float median = Float.intBitsToFloat(page.readInt(pos));

          // Dequantize
          final VectorQuantizationMetadata.BinaryQuantizationMetadata qmeta =
              new VectorQuantizationMetadata.BinaryQuantizationMetadata(
                  packed, median, originalLength);
          return dequantizeFromBinary(packed, qmeta);
        }

        // quantType is NONE - return null (caller should fetch from document)
        return null;

      } finally {
        // BasePage is managed by PageManager, no explicit close needed
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error reading vector from offset %d: %s", fileOffset,
          e.getMessage());
      return null;
    }
  }

  // ========== END QUANTIZATION HELPER METHODS ==========

  /**
   * Create a new vector data page with LSM-style header.
   * Page layout: [offsetFreeContent(4)][numberOfEntries(4)][mutable(1)][entries grow forward sequentially]
   */
  private MutablePage createNewVectorDataPage(final int pageNum) {
    final PageId pageId = new PageId(getDatabase(), getFileId(), pageNum);
    final MutablePage page = getDatabase().getTransaction().addPage(pageId, getPageSize());

    int pos = 0;
    // offsetFreeContent starts right after header (entries grow forward sequentially)
    pos += page.writeInt(pos, HEADER_BASE_SIZE);
    pos += page.writeInt(pos, 0);              // numberOfEntries = 0
    page.writeByte(pos, (byte) 1);         // mutable = 1 (page is actively being written to)

    // Track mutable pages for compaction trigger
    currentMutablePages.incrementAndGet();

    return page;
  }

  /** Map JVector similarity (larger = closer) back to a distance so ascending sort returns nearest first. */
  static float scoreToDistance(final VectorSimilarityFunction similarityFunction, final float score) {
    return switch (similarityFunction) {
      case COSINE -> 2.0f * (1.0f - score);
      case EUCLIDEAN -> score > 0 ? (1.0f / score) - 1.0f : Float.MAX_VALUE;
      case DOT_PRODUCT -> -score;
    };
  }

  /**
   * The similarity handed to a graph node whose vector can no longer be read. Every JVector similarity function has
   * {@code 0} as its floor: {@code (1 + cosine) / 2} bottoms out there at {@code cosine = -1}, {@code 1 / (1 + d^2)}
   * approaches it from above, and {@code (1 + dot) / 2} bottoms out there for the unit-length vectors JVector
   * documents as the precondition for using {@code DOT_PRODUCT} at all. A node scored this way therefore sorts at or
   * behind every real candidate.
   */
  private static final float UNREADABLE_NODE_SCORE = 0.0f;

  /**
   * The scoring function used to walk the graph: the configured similarity for a vector that can be read, and
   * {@link #UNREADABLE_NODE_SCORE} for one that cannot.
   * <p>
   * A deleted vector stays in the graph until the next rebuild and its pages are released as soon as it is
   * tombstoned, so {@link ArcadePageVectorValues} hands back a placeholder rather than null (issue #3715) and
   * {@link GrowableVectorValues} hands back null. Scoring either of those is meaningless, and for
   * {@code COSINE} it is worse than meaningless: the placeholder's squared magnitude underflows to zero in float,
   * so the similarity comes back {@code Infinity}. That made every tombstone the <i>best</i> candidate in the beam -
   * it displaced the real neighbours a query near deleted data was looking for, and tripped JVector's own
   * {@code 0 <= score <= 1} assertion when they were enabled (issue #5558).
   */
  ScoreFunction.ExactScoreFunction liveOnlyScoreFunction(final VectorFloat<?> queryVector,
      final RandomAccessVectorValues vectors) {
    final ArcadePageVectorValues pageValues = vectors instanceof final ArcadePageVectorValues p ? p : null;
    return node -> {
      final VectorFloat<?> vector = vectors.getVector(node);
      if (vector == null || (pageValues != null && pageValues.isDeletedSentinel(vector)))
        return UNREADABLE_NODE_SCORE;
      return metadata.similarityFunction.compare(queryVector, vector);
    };
  }

  /**
   * The vector values a search scores against: the live builder's in-memory set when the graph in use is the one that
   * builder produced (there the ordinals <i>are</i> vector ids), and the page-backed reader otherwise. The reader is
   * handed the index-scoped cache so a working set survives across queries (issue #5412).
   * <p>
   * The first branch is currently unreachable and kept as a guard: {@code graphIndex} is only ever assigned a graph
   * loaded from disk or one built by the local builder in {@code buildGraphFromScratch}, never
   * {@code liveBuilder.getGraph()}, so the identity check cannot hold. That matters to the callers because it means
   * the {@code ordinalMap} they pass is always the one published alongside the graph by the same rebuild - the
   * pairing issue #4581 exists to keep intact - and never a map whose ordinals mean something else.
   */
  private RandomAccessVectorValues searchVectorValues(final int[] ordinalMap) {
    if (liveVectorValues != null && liveBuilder != null && graphIndex == liveBuilder.getGraph())
      return liveVectorValues;

    final String vectorProp =
        metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ? metadata.propertyNames.getFirst() : "vector";
    return new ArcadePageVectorValues(getDatabase(), metadata.dimensions, vectorProp, vectorIndex, ordinalMap, this,
        getSearchVectorCache());
  }

  /** Visible for tests: the ordinal-to-vector-id map the next search would capture. */
  int[] getOrdinalToVectorIdForTest() {
    return ordinalToVectorId;
  }

  /**
   * Visible for tests: the similarity a search would compute for one graph ordinal, through the same vector values and
   * the same score function the query path builds. Lets a test assert what a node whose vector cannot be read scores,
   * which end-to-end search results cannot show - the {@link LiveVectorBitsFilter} keeps such a node out of the answer
   * whatever it scored.
   */
  float scoreOrdinalForTest(final float[] queryVector, final int ordinal) {
    return liveOnlyScoreFunction(vts.createFloatVector(queryVector), searchVectorValues(ordinalToVectorId))
        .similarityTo(ordinal);
  }

  /**
   * Brute-force scan of delta vectors (inserted since last graph rebuild) and merge with graph search results.
   * <p>
   * The delta buffer holds every vector ingested since the last graph rebuild, so under sustained ingestion it
   * can grow to hundreds of thousands of entries. The scan therefore keeps a bounded top-k heap and prunes on
   * the current k-th distance instead of appending every candidate and sorting the whole list: allocation is
   * O(k) per query rather than O(delta), and the sort is O(delta log k) rather than O(delta log delta)
   * (issue #5391). Entries are already stored as {@link VectorFloat}, so scoring allocates nothing at all.
   */
  private void mergeWithDeltaScan(final VectorFloat<?> queryVectorFloat, final int k,
      final Set<RID> allowedRIDs, final List<Pair<RID, Float>> results) {
    final List<DeltaVectorEntry> currentDelta = deltaVectors; // volatile snapshot
    if (currentDelta.isEmpty() || k <= 0)
      return;

    // Collect already-seen RIDs from graph results to avoid duplicates
    final RidHashSet seenRIDs = new RidHashSet(results.size());
    for (final Pair<RID, Float> r : results)
      seenRIDs.add(r.getFirst());

    // Max-heap on distance: the head is the current worst kept candidate, so the k-th best distance is O(1)
    // to read and the worst entry is O(log k) to evict.
    final PriorityQueue<Pair<RID, Float>> best = new PriorityQueue<>(Math.max(1, k),
        (a, b) -> Float.compare(b.getSecond(), a.getSecond()));
    best.addAll(results);
    while (best.size() > k)
      best.poll();

    boolean added = false;
    for (final DeltaVectorEntry delta : currentDelta) {
      if (seenRIDs.contains(delta.rid))
        continue;
      if (allowedRIDs != null && !allowedRIDs.isEmpty() && !allowedRIDs.contains(delta.rid))
        continue;
      // Check if deleted after being added to delta
      final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(delta.vectorId);
      if (loc != null && loc.deleted)
        continue;

      final float score = metadata.similarityFunction.compare(queryVectorFloat, delta.vector);
      final float distance = scoreToDistance(metadata.similarityFunction, score);

      // Prune before allocating: a candidate no better than the current k-th best cannot make the result set.
      if (best.size() >= k && distance >= best.peek().getSecond())
        continue;

      best.add(new Pair<>(bindRid(delta.rid), distance));
      if (best.size() > k)
        best.poll();
      added = true;
    }

    if (added) {
      results.clear();
      results.addAll(best);
      results.sort((a, b) -> Float.compare(a.getSecond(), b.getSecond()));
    }
  }

  /**
   * Brute-force scan of all indexed vectors to supplement graph search results.
   * Called as a fallback when graph search returns too few results (issue #3722),
   * e.g., after a rebuild with corrupted pages produced a poorly connected graph.
   */
  private void bruteForceScan(final VectorFloat<?> queryVectorFloat, final int k,
      final Set<RID> allowedRIDs, final List<Pair<RID, Float>> results,
      final RandomAccessVectorValues vectors, final int[] ordinalMap) {
    // Collect already-seen RIDs to avoid duplicates
    final RidHashSet seenRIDs = new RidHashSet(results.size());
    for (final Pair<RID, Float> r : results)
      seenRIDs.add(r.getFirst());

    // Use the ordinal->vectorId snapshot captured together with the vectors snapshot by the caller.
    // Re-reading the volatile this.ordinalToVectorId here would risk indexing into an array that was
    // reassigned by a concurrent rebuild, pairing each ordinal's RID with a vector from a different
    // mapping (issue #4581).
    final ArcadePageVectorValues pageValues = vectors instanceof final ArcadePageVectorValues p ? p : null;
    boolean added = false;

    if (allowedRIDs != null && !allowedRIDs.isEmpty() && allowedRIDs.size() < ordinalMap.length) {
      // Walk the allow-list instead of every ordinal (issue #5748). An allow-list narrower than k never meets the
      // caller's expected-result threshold, so the fallback fires on every query and the full ordinal walk costs
      // O(index) each time. Resolving each allowed RID to its ordinals costs O(allow-list * log index) and reads
      // only the vectors that can actually make the result set, which is where the time goes.
      // The guard on size() is the crossover: an allow-list wider than the index would pay more reverse lookups
      // than the plain scan pays ordinal steps, for the same answer.
      final int[] candidates = collectAllowedOrdinals(allowedRIDs, ordinalMap);
      for (final int ordinal : candidates)
        added |= scoreOrdinal(ordinal, queryVectorFloat, allowedRIDs, results, vectors, ordinalMap, seenRIDs, pageValues);
    } else {
      for (int ordinal = 0; ordinal < ordinalMap.length; ordinal++)
        added |= scoreOrdinal(ordinal, queryVectorFloat, allowedRIDs, results, vectors, ordinalMap, seenRIDs, pageValues);
    }

    if (added) {
      results.sort((a, b) -> Float.compare(a.getSecond(), b.getSecond()));
      if (results.size() > k)
        results.subList(k, results.size()).clear();
    }
  }

  /**
   * Resolve an allow-list to the ordinals it occupies in {@code ordinalMap}, ascending.
   * <p>
   * {@code ordinalMap} is always sorted ascending - every producer of {@code ordinalToVectorId} builds it with
   * {@code sorted()} because the ordinal order has to match the order the graph was persisted in - so the reverse
   * lookup is a binary search and needs no per-query map. A RID with no live vector id, or one whose vector was
   * ingested after the last rebuild and is therefore only in the delta buffer, simply contributes no ordinal.
   * <p>
   * The result is sorted so the caller scores in ordinal order, exactly the order the full scan uses. Distance ties
   * would otherwise be broken by the allow-list's iteration order and the two paths could truncate to different
   * RIDs at k.
   */
  private int[] collectAllowedOrdinals(final Set<RID> allowedRIDs, final int[] ordinalMap) {
    // Start small and grow: the allow-list can be much larger than the number of RIDs actually resident in this
    // index, and sizing the array for it up front would allocate for entries that never materialize.
    int[] ordinals = new int[Math.min(allowedRIDs.size(), 256)];
    int count = 0;
    for (final RID rid : allowedRIDs) {
      for (final int vectorId : vectorIndex.getVectorIdsForRid(rid)) {
        final int ordinal = Arrays.binarySearch(ordinalMap, vectorId);
        if (ordinal < 0)
          continue;
        if (count == ordinals.length)
          ordinals = Arrays.copyOf(ordinals, count * 2);
        ordinals[count++] = ordinal;
      }
    }
    if (count < ordinals.length)
      ordinals = Arrays.copyOf(ordinals, count);
    Arrays.sort(ordinals);
    return ordinals;
  }

  /**
   * Score one ordinal into {@code results}, returning whether it was added. Both brute-force paths funnel through
   * here so the allow-list walk and the full scan cannot drift apart on the deleted, duplicate and unreadable-vector
   * guards.
   */
  private boolean scoreOrdinal(final int ordinal, final VectorFloat<?> queryVectorFloat, final Set<RID> allowedRIDs,
      final List<Pair<RID, Float>> results, final RandomAccessVectorValues vectors, final int[] ordinalMap,
      final RidHashSet seenRIDs, final ArcadePageVectorValues pageValues) {
    final int vectorId = ordinalMap[ordinal];
    final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
    if (loc == null || loc.deleted)
      return false;
    if (seenRIDs.contains(loc.rid))
      return false;
    // Redundant on the allow-list walk, which only ever resolves allowed RIDs, but it is what keeps the full scan
    // filtered when the crossover guard sends a wide allow-list here, and it is the single place the membership rule
    // lives for both paths.
    if (allowedRIDs != null && !allowedRIDs.isEmpty() && !allowedRIDs.contains(loc.rid))
      return false;

    // The location says the vector is live, but the read can still fail - a document whose vector property was
    // removed or has the wrong type comes back as the placeholder, not as a vector. Scoring it would pair a real
    // RID with a meaningless distance, and under COSINE with a distance of minus infinity, i.e. first place.
    final VectorFloat<?> vec = vectors.getVector(ordinal);
    if (vec == null || (pageValues != null && pageValues.isDeletedSentinel(vec)))
      return false;

    final float score = metadata.similarityFunction.compare(queryVectorFloat, vec);
    results.add(new Pair<>(bindRid(loc.rid), scoreToDistance(metadata.similarityFunction, score)));
    return true;
  }

  /**
   * Search for k nearest neighbors to the given vector and return results with similarity scores.
   * This method is similar to HnswVectorIndex.findNeighborsFromVector and avoids the need to
   * recalculate distances after the search.
   *
   * @param queryVector The query vector to search for
   * @param k           The number of neighbors to return
   *
   * @return List of pairs containing RID and similarity score
   */
  public List<Pair<RID, Float>> findNeighborsFromVector(final float[] queryVector, final int k) {
    return findNeighborsFromVector(queryVector, k, -1, null);
  }

  public List<Pair<RID, Float>> findNeighborsFromVector(final float[] queryVector, final int k, final int efSearch) {
    return findNeighborsFromVector(queryVector, k, efSearch, null);
  }

  /**
   * Search for k nearest neighbors to the given vector within a filtered set of RIDs.
   * This method allows restricting the search space to specific records, useful for
   * filtering by user ID, category, or other criteria during graph traversal.
   *
   * @param queryVector The query vector to search for
   * @param k           The number of neighbors to return
   * @param allowedRIDs Optional set of RIDs to restrict search to (null means no filtering)
   *
   * @return List of pairs containing RID and similarity score
   */
  public List<Pair<RID, Float>> findNeighborsFromVector(final float[] queryVector, final int k,
      final Set<RID> allowedRIDs) {
    return findNeighborsFromVector(queryVector, k, -1, allowedRIDs);
  }

  /**
   * Search for k nearest neighbors with configurable efSearch for recall tuning.
   *
   * @param queryVector The query vector to search for
   * @param k           The number of neighbors to return
   * @param efSearch    Search beam width (-1 uses index default). Higher values improve recall at cost of latency.
   * @param allowedRIDs Optional set of RIDs to restrict search to (null means no filtering)
   *
   * @return List of pairs containing RID and similarity score
   */
  public List<Pair<RID, Float>> findNeighborsFromVector(final float[] queryVector, final int k, final int efSearch,
      final Set<RID> allowedRIDs) {
    // Track search metrics
    final long startTime = System.currentTimeMillis();
    metrics.incrementSearchOperations();

    try {
      if (queryVector == null)
        throw new IllegalArgumentException("Query vector cannot be null");

      if (queryVector.length != metadata.dimensions)
        throw new IllegalArgumentException(
            "Query vector dimension " + queryVector.length + " does not match index dimension " + metadata.dimensions);

      // Check if query vector is all zeros (would cause NaN with cosine similarity)
      if (metadata.similarityFunction == VectorSimilarityFunction.COSINE && VectorUtils.isZeroVector(queryVector))
        throw new IllegalArgumentException(
            "Query vector cannot be a zero vector when using COSINE similarity (causes undefined similarity)");

      // Ensure graph is available (lazy-load from disk if needed, or build if not persisted)
      ensureGraphAvailable();

      // Issue #3679: rebuild graph if needed (sync for first build or small graphs, async for large graphs)
      rebuildGraphBeforeSearch();

      boolean readLockHeld = false;
      lock.readLock().lock();
      readLockHeld = true;
      try {
        if (graphIndex == null || vectorIndex.size() == 0) {
          // No graph yet — still return delta-only results if available
          if (!deltaVectors.isEmpty()) {
            final VectorFloat<?> qvf = vts.createFloatVector(queryVector);
            final List<Pair<RID, Float>> results = new ArrayList<>(k);
            mergeWithDeltaScan(qvf, k, allowedRIDs, results);
            return results;
          }
          return Collections.emptyList();
        }

        // Convert query vector to VectorFloat
        final VectorFloat<?> queryVectorFloat = vts.createFloatVector(queryVector);

        // Snapshot the volatile ordinal->vectorId map once and use the same reference everywhere
        // below (filter, result extraction and brute-force fallback). The field can be reassigned
        // by a graph rebuild; reading it more than once would risk pairing an ordinal mapped through
        // a different array than the one captured by the vectors snapshot, returning RIDs scored
        // against the wrong vector (issue #4581).
        final int[] ordinalMap = this.ordinalToVectorId;

        final RandomAccessVectorValues vectors = searchVectorValues(ordinalMap);

        // Only live vectors may enter the result heap. Accepting tombstones lets a query aimed at a deleted
        // neighbourhood fill its beam with them and stop, which is what returned an empty list (issue #5558).
        final Bits bitsFilter = new LiveVectorBitsFilter(allowedRIDs, ordinalMap, vectorIndex);

        // Use instance GraphSearcher with SearchScoreProvider for efSearch control. The searcher is borrowed from
        // the index-scoped pool so its scratch state survives across queries (issue #5413).
        final SearchResult searchResult;
        final GraphSearcherPool pool = getSearcherPool();
        final long poolEpoch = searcherPoolEpoch();
        // Pin the graph reference: a concurrent rebuild may swap the volatile field, and borrow/release must
        // agree on which graph the searcher belongs to.
        final ImmutableGraphIndex pooledGraph = graphIndex;
        final GraphSearcher searcher = pool.borrow(pooledGraph, poolEpoch);
        try {
          final ScoreFunction.ExactScoreFunction exactScoreFunction = liveOnlyScoreFunction(queryVectorFloat, vectors);

          // Use exact scoring for graph traversal.
          // FusedPQ approximate scoring is currently disabled due to a bug where PQ codes
          // written through ArcadeDB's page-based storage produce incorrect approximate scores,
          // causing beam search to return too few results. The exact scoring path reads vectors
          // from the graph file (if storeVectorsInGraph=true), quantized pages (if INT8/BINARY),
          // or documents (if NONE), and provides correct results.
          // TODO: Re-enable FusedPQ once page-based PQ code persistence is verified correct
          final DefaultSearchScoreProvider ssp =
              new DefaultSearchScoreProvider(exactScoreFunction, exactScoreFunction);

          if (efSearch > 0) {
            // Explicit efSearch: use fixed beam width (per-query or index default override)
            final int effectiveEfSearch = Math.max(k, efSearch);
            searchResult = searcher.search(ssp, k, effectiveEfSearch, 0.0f, 0.0f, bitsFilter);
          } else if (metadata.efSearch != 100) {
            // User configured efSearch in index metadata: use their value
            final int effectiveEfSearch = Math.max(k, metadata.efSearch);
            searchResult = searcher.search(ssp, k, effectiveEfSearch, 0.0f, 0.0f, bitsFilter);
          } else {
            // Adaptive efSearch with resume for insufficient results (issue #3722).
            // Both small and large graphs use the same two-pass strategy: search first,
            // resume with wider beam if too few results are returned.
            final int graphSize = graphIndex.size();
            final int initialEfSearch;
            if (graphSize < 10_000)
              initialEfSearch = Math.max(k, 100);
            else
              initialEfSearch = Math.max(k * 2, 20);

            final SearchResult firstPass = searcher.search(ssp, k, initialEfSearch, 0.0f, 0.0f, bitsFilter);
            if (firstPass.getNodes().length < k && graphSize >= k) {
              // Graph has enough nodes but beam search found too few - widen the beam
              searchResult = searcher.resume(k, Math.max(k * 10, 100));
            } else {
              searchResult = firstPass;
            }
          }
        } finally {
          pool.release(searcher, pooledGraph, poolEpoch);
        }

        LogManager.instance()
            .log(this, Level.FINE, "GraphSearcher returned %d nodes, graphSize=%d, vectorsSize=%d, ordinalToVectorIdLength=%d",
                searchResult.getNodes().length, graphIndex.size(), vectors.size(), ordinalMap.length);

        // Extract RIDs and scores from search results using ordinal mapping
        final List<Pair<RID, Float>> results = new ArrayList<>(k);
        int skippedOutOfBounds = 0;
        int skippedDeletedOrNull = 0;
        for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
          final int ordinal = nodeScore.node;
          if (ordinal >= 0 && ordinal < ordinalMap.length) {
            final int vectorId = ordinalMap[ordinal];
            final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
            if (loc != null && !loc.deleted) {
              // Post-filter by allowed RIDs (JVector may include entry node despite Bits filter)
              if (allowedRIDs != null && !allowedRIDs.isEmpty() && !allowedRIDs.contains(loc.rid))
                continue;

              results.add(new Pair<>(bindRid(loc.rid), scoreToDistance(metadata.similarityFunction, nodeScore.score)));
            } else {
              skippedDeletedOrNull++;
            }
          } else {
            skippedOutOfBounds++;
          }
        }

        // Merge with delta vectors inserted since last graph rebuild
        mergeWithDeltaScan(queryVectorFloat, k, allowedRIDs, results);

        // Issue #3722: if graph search + delta merge could not fill the request, fall back to a brute-force scan.
        // This handles degraded graph quality after rebuilds with corrupted pages.
        // The budget is the LIVE vector count, not the ordinal map length: a tombstone-heavy graph carries ordinals
        // no query can ever return, and sizing the expectation on them asked for results that do not exist. It also
        // closes the case of a single surviving vector, where the old "less than 80% of what is available" guard
        // evaluated to `0 < 0` and suppressed the fallback that was the only thing left to answer the query.
        // VectorLocationIndex.size() is the live count only while the map does not evict, which since #5568 it never
        // does. On a bounded backend it is a lower bound, so this would under-estimate the budget and could leave the
        // fallback unfired when it should run - acceptable, since an evicting location map already makes the whole
        // index approximate, and the same assumption is what the auto-compaction ratio rests on.
        // It also counts vectors still in the delta buffer, which the scan below cannot reach because it walks graph
        // ordinals - mergeWithDeltaScan has already covered those. So a search left short only by delta vectors can
        // still pay for a scan that finds nothing new. Bounded, rare, and now visible through bruteForceScans.
        final int availableVectors = Math.min(ordinalMap.length, vectorIndex.size());
        final int expectedResults = Math.min(k, availableVectors);
        if (results.size() < expectedResults) {
          LogManager.instance()
              .log(this, Level.WARNING,
                  """
                  Graph search returned only %d results (expected %d, available %d) for index %s - \
                  falling back to brute-force scan (graph may need rebuilding)""",
                  results.size(), expectedResults, availableVectors, indexName);
          metrics.incrementBruteForceScans();
          bruteForceScan(queryVectorFloat, k, allowedRIDs, results, vectors, ordinalMap);
        }

        LogManager.instance()
            .log(this, Level.FINE, "Vector search returned %d results (skipped: %d out of bounds, %d deleted/null)",
                results.size(),
                skippedOutOfBounds, skippedDeletedOrNull);
        return results;

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error performing vector search", e);
        throw new IndexException("Error performing vector search", e);
      } finally {
        if (readLockHeld) {
          lock.readLock().unlock();
        }
      }
    } finally {
      // Track search latency
      final long elapsed = System.currentTimeMillis() - startTime;
      metrics.addSearchLatency(elapsed);
    }
  }

  /**
   * Search for k nearest neighbors with traversal-integrated {@code groupBy} (issue #4071). Pushes
   * the per-group cap into the JVector graph traversal via a {@link GroupedRIDBitsFilter}: nodes
   * whose group has already reached {@code groupSize} during the HNSW walk are filtered out before
   * scoring. The search budget is sized to {@code limit * groupSize} so JVector can fill the heap
   * even when groups are exhausted.
   * <p>
   * <b>Approximate best-per-group.</b> Unlike the sparse path's per-group min-heap, the dense
   * traversal cannot consult scores when the {@link Bits} filter runs (it gates eligibility before
   * scoring). Group admission therefore happens in HNSW visit order. HNSW visits approximately
   * best-first, so the first {@code groupSize} candidates per group are usually among the best,
   * but the integration is not score-exact - callers that need a strict best-per-group guarantee
   * should keep the SQL-layer post-filter, which re-applies {@code GroupAdmissionState} on the
   * search output and can fall back to the MVP behaviour at higher {@code efSearch}.
   * <p>
   * <b>Delta scan and brute-force fallback are skipped.</b> The two augmentations would re-admit
   * candidates outside the per-group cap; the delta path because it scores newly-inserted vectors
   * not yet visible to HNSW (no Bits filter applied), the brute-force path because it walks every
   * vector when the graph search returned too few hits. Skipping them is correct for the grouped
   * contract; callers that depend on either should use {@link #findNeighborsFromVector} and apply
   * the {@code GroupAdmissionState} post-filter at the SQL layer.
   *
   * @param queryVector      query embedding
   * @param limit            max number of distinct groups to return; must be {@code > 0}
   * @param groupSize        max members per group; must be {@code > 0}
   * @param efSearch         search beam width override; {@code -1} uses the index default
   * @param allowedRIDs      optional RID whitelist
   * @param groupKeyResolver maps a candidate RID to its group key
   *
   * @return list of (RID, distance) pairs sorted ascending by distance, capped at {@code limit *
   *         groupSize} entries with each distinct group key appearing at most {@code groupSize}
   *         times.
   */
  public List<Pair<RID, Float>> findNeighborsFromVectorGrouped(final float[] queryVector, final int limit, final int groupSize,
      final int efSearch, final Set<RID> allowedRIDs, final Function<RID, Object> groupKeyResolver) {
    if (limit <= 0 || groupSize <= 0)
      return Collections.emptyList();
    if (groupKeyResolver == null)
      throw new IllegalArgumentException("groupKeyResolver must not be null");

    final long startTime = System.currentTimeMillis();
    metrics.incrementSearchOperations();

    try {
      if (queryVector == null)
        throw new IllegalArgumentException("Query vector cannot be null");

      if (queryVector.length != metadata.dimensions)
        throw new IllegalArgumentException(
            "Query vector dimension " + queryVector.length + " does not match index dimension " + metadata.dimensions);

      if (metadata.similarityFunction == VectorSimilarityFunction.COSINE && VectorUtils.isZeroVector(queryVector))
        throw new IllegalArgumentException(
            "Query vector cannot be a zero vector when using COSINE similarity (causes undefined similarity)");

      ensureGraphAvailable();
      rebuildGraphBeforeSearch();

      final int searchK = limit * groupSize;
      boolean readLockHeld = false;
      lock.readLock().lock();
      readLockHeld = true;
      try {
        if (graphIndex == null || vectorIndex.size() == 0)
          return Collections.emptyList();

        final VectorFloat<?> queryVectorFloat = vts.createFloatVector(queryVector);

        final RandomAccessVectorValues vectors = searchVectorValues(ordinalToVectorId);

        // Group-aware Bits filter. Wraps the same RID validity + allowedRIDs gating that
        // LiveVectorBitsFilter applies, plus per-group counters that reject candidates whose group has
        // reached capacity. Per-search state lives on the filter instance; do not reuse the
        // instance across calls.
        final Bits bitsFilter = new GroupedRIDBitsFilter(allowedRIDs, ordinalToVectorId, vectorIndex,
            groupKeyResolver, limit, groupSize);

        final SearchResult searchResult;
        final GraphSearcherPool pool = getSearcherPool();
        final long poolEpoch = searcherPoolEpoch();
        // Pin the graph reference: a concurrent rebuild may swap the volatile field, and borrow/release must
        // agree on which graph the searcher belongs to.
        final ImmutableGraphIndex pooledGraph = graphIndex;
        final GraphSearcher searcher = pool.borrow(pooledGraph, poolEpoch);
        try {
          final ScoreFunction.ExactScoreFunction exactScoreFunction = liveOnlyScoreFunction(queryVectorFloat, vectors);
          final DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(exactScoreFunction, exactScoreFunction);

          // Choose efSearch the same way the non-grouped path does, but skip the
          // resume-on-too-few branch: the Bits filter intentionally rejects candidates whose
          // group has reached groupSize, so a "too few results" outcome is the contract, not a
          // beam-width problem. JVector's resume() resets the result state and runs another pass;
          // when the Bits filter has already locked groups full, the second pass admits nothing
          // new and we lose the first-pass survivors. Keep the first-pass result and let callers
          // raise efSearch explicitly when they need wider coverage.
          final int effectiveEfSearch;
          if (efSearch > 0) {
            effectiveEfSearch = Math.max(searchK, efSearch);
          } else if (metadata.efSearch != 100) {
            effectiveEfSearch = Math.max(searchK, metadata.efSearch);
          } else {
            final int graphSize = graphIndex.size();
            effectiveEfSearch = graphSize < 10_000 ? Math.max(searchK, 100) : Math.max(searchK * 2, 20);
          }
          searchResult = searcher.search(ssp, searchK, effectiveEfSearch, 0.0f, 0.0f, bitsFilter);
        } finally {
          pool.release(searcher, pooledGraph, poolEpoch);
        }

        LogManager.instance()
            .log(this, Level.INFO,
                "GraphSearcher (grouped) returned %d nodes, graphSize=%d, vectorsSize=%d, ordinalToVectorIdLength=%d, limit=%d, groupSize=%d",
                searchResult.getNodes().length, graphIndex.size(), vectors.size(), ordinalToVectorId.length, limit, groupSize);

        final List<Pair<RID, Float>> results = new ArrayList<>(searchK);
        int skippedOutOfBounds = 0;
        int skippedDeletedOrNull = 0;
        for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
          final int ordinal = nodeScore.node;
          if (ordinal < 0 || ordinal >= ordinalToVectorId.length) {
            skippedOutOfBounds++;
            continue;
          }
          final int vectorId = ordinalToVectorId[ordinal];
          final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
          if (loc == null || loc.deleted) {
            skippedDeletedOrNull++;
            continue;
          }
          // Defensive post-filter: JVector may include the entry node despite Bits, and the
          // GroupedRIDBitsFilter has already enforced the group cap, so we only need to re-check
          // the RID whitelist here for parity with findNeighborsFromVector.
          if (allowedRIDs != null && !allowedRIDs.isEmpty() && !allowedRIDs.contains(loc.rid))
            continue;

          results.add(new Pair<>(bindRid(loc.rid), scoreToDistance(metadata.similarityFunction, nodeScore.score)));
        }

        LogManager.instance()
            .log(this, Level.INFO,
                "Vector grouped search returned %d results (skipped: %d out of bounds, %d deleted/null)",
                results.size(), skippedOutOfBounds, skippedDeletedOrNull);
        return results;

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error performing grouped vector search", e);
        throw new IndexException("Error performing grouped vector search", e);
      } finally {
        if (readLockHeld)
          lock.readLock().unlock();
      }
    } finally {
      final long elapsed = System.currentTimeMillis() - startTime;
      metrics.addSearchLatency(elapsed);
    }
  }

  /**
   * Search for k nearest neighbors using zero-disk-I/O approximate search with Product Quantization.
   * <p>
   * This method uses pre-computed PQ vectors in memory for both HNSW navigation AND scoring,
   * completely bypassing disk I/O for the vector data. This enables microsecond-level latency
   * at the cost of slightly lower recall compared to exact search.
   * <p>
   * Requirements:
   * - Index must be configured with quantizationType=PRODUCT
   * - PQ data must be built and loaded (happens automatically during graph build)
   * <p>
   * If PQ is not available, falls back to the regular findNeighborsFromVector method.
   *
   * @param queryVector The query vector to search for
   * @param k           The number of neighbors to return
   *
   * @return List of pairs containing RID and approximate similarity score
   */
  public List<Pair<RID, Float>> findNeighborsFromVectorApproximate(final float[] queryVector, final int k) {
    return findNeighborsFromVectorApproximate(queryVector, k, null);
  }

  /**
   * Search for k nearest neighbors using zero-disk-I/O approximate search with Product Quantization,
   * optionally filtering to a specific set of RIDs.
   * <p>
   * This method uses pre-computed PQ vectors in memory for both HNSW navigation AND scoring,
   * completely bypassing disk I/O for the vector data. This enables microsecond-level latency
   * at the cost of slightly lower recall compared to exact search.
   *
   * @param queryVector The query vector to search for
   * @param k           The number of neighbors to return
   * @param allowedRIDs Optional set of RIDs to restrict search to (null means no filtering)
   *
   * @return List of pairs containing RID and approximate similarity score
   */
  public List<Pair<RID, Float>> findNeighborsFromVectorApproximate(final float[] queryVector, final int k,
      final Set<RID> allowedRIDs) {
    // Check if PQ is available
    if (pqVectors == null || productQuantization == null) {
      // Fall back to exact search
      LogManager.instance().log(this, Level.FINE,
          "PQ not available for index %s, falling back to exact search", indexName);
      return findNeighborsFromVector(queryVector, k, allowedRIDs);
    }

    // Track search metrics
    final long startTime = System.nanoTime(); // Use nanos for microsecond precision
    metrics.incrementSearchOperations();

    try {
      if (queryVector == null)
        throw new IllegalArgumentException("Query vector cannot be null");

      if (queryVector.length != metadata.dimensions)
        throw new IllegalArgumentException(
            "Query vector dimension " + queryVector.length + " does not match index dimension " + metadata.dimensions);

      // Check if query vector is all zeros (would cause NaN with cosine similarity)
      if (metadata.similarityFunction == VectorSimilarityFunction.COSINE && VectorUtils.isZeroVector(queryVector))
        throw new IllegalArgumentException(
            "Query vector cannot be a zero vector when using COSINE similarity (causes undefined similarity)");

      // Ensure graph is available (lazy-load from disk if needed)
      ensureGraphAvailable();

      lock.readLock().lock();
      try {
        if (graphIndex == null || vectorIndex.size() == 0) {
          // No graph yet — still return delta-only results if available
          if (!deltaVectors.isEmpty()) {
            final VectorFloat<?> qvf = vts.createFloatVector(queryVector);
            final List<Pair<RID, Float>> results = new ArrayList<>(k);
            mergeWithDeltaScan(qvf, k, allowedRIDs, results);
            return results;
          }
          return Collections.emptyList();
        }

        // Convert query vector to VectorFloat
        final VectorFloat<?> queryVectorFloat = vts.createFloatVector(queryVector);

        // Build the memory-resident PQ score function (uses SIMD/Panama if available)
        // This is the key to zero-disk-I/O: we use PQ scores for BOTH navigation AND final scoring
        final ScoreFunction.ApproximateScoreFunction scoreFunction =
            pqVectors.precomputedScoreFunctionFor(queryVectorFloat, metadata.similarityFunction);

        // Create a ReRanker that does NOT pull from disk - just returns PQ similarity
        // This is the critical optimization: we bypass RandomAccessVectorValues entirely
        final ScoreFunction.ExactScoreFunction approxReranker = ordinal -> scoreFunction.similarityTo(ordinal);

        // Wrap in a DefaultSearchScoreProvider (concrete implementation)
        final DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(scoreFunction, approxReranker);

        // Snapshot the volatile ordinal map once, the way the exact path does: the filter and the result loop below
        // must resolve an ordinal through the same array, or a concurrent rebuild between the two reads would pair
        // an ordinal's RID with a vector from a different mapping (issue #4581).
        final int[] ordinalMap = this.ordinalToVectorId;

        // Live-only (plus the optional RID allow-list): PQ scores a tombstone as happily as a live vector, so
        // without this the beam fills with nodes the post-filter below then drops (issue #5558).
        final Bits bitsFilter = new LiveVectorBitsFilter(allowedRIDs, ordinalMap, vectorIndex);

        // Execute search using the PQ-based score provider
        // The graph structure is typically small enough to stay in OS page cache
        // Note: JVector 4.0's search method uses (scoreProvider, topK, Bits) signature
        final SearchResult searchResult;
        final GraphSearcherPool pool = getSearcherPool();
        final long poolEpoch = searcherPoolEpoch();
        // Pin the graph reference: a concurrent rebuild may swap the volatile field, and borrow/release must
        // agree on which graph the searcher belongs to.
        final ImmutableGraphIndex pooledGraph = graphIndex;
        final GraphSearcher searcher = pool.borrow(pooledGraph, poolEpoch);
        try {
          searchResult = searcher.search(ssp, k, bitsFilter);
        } finally {
          pool.release(searcher, pooledGraph, poolEpoch);
        }

        // Extract RIDs and scores from search results
        final List<Pair<RID, Float>> results = new ArrayList<>(k);
        int skippedOutOfBounds = 0;
        int skippedDeletedOrNull = 0;
        for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
          final int ordinal = nodeScore.node;
          if (ordinal >= 0 && ordinal < ordinalMap.length) {
            final int vectorId = ordinalMap[ordinal];
            final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
            if (loc != null && !loc.deleted) {
              final float distance = scoreToDistance(metadata.similarityFunction, nodeScore.score);
              results.add(new Pair<>(bindRid(loc.rid), distance));
            } else {
              skippedDeletedOrNull++;
            }
          } else {
            skippedOutOfBounds++;
          }
        }

        // Merge with delta vectors inserted since last graph rebuild
        mergeWithDeltaScan(queryVectorFloat, k, allowedRIDs, results);

        // Log performance metrics
        final long elapsedNanos = System.nanoTime() - startTime;
        LogManager.instance().log(this, Level.INFO,
            "Zero-disk-I/O PQ search returned %d results in %.2f µs (skipped: %d out of bounds, %d deleted/null)",
            results.size(), elapsedNanos / 1000.0, skippedOutOfBounds, skippedDeletedOrNull);

        return results;

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error performing PQ approximate search", e);
        throw new IndexException("Error performing PQ approximate search", e);
      } finally {
        lock.readLock().unlock();
      }
    } finally {
      // Track search latency (convert nanos to ms for consistency)
      final long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
      metrics.addSearchLatency(elapsedMs);
    }
  }

  /**
   * Check if Product Quantization is available for approximate search.
   *
   * @return true if PQ data is loaded and ready for zero-disk-I/O search
   */
  public boolean isPQSearchAvailable() {
    return pqVectors != null && productQuantization != null;
  }

  /**
   * Get the number of vectors encoded in the PQ index.
   *
   * @return Number of PQ-encoded vectors, or 0 if PQ is not available
   */
  public int getPQVectorCount() {
    return pqVectors != null ? pqVectors.count() : 0;
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      throw new IllegalArgumentException("Expected float array, byte array (INT8), or supported vector type as key for vector search");

    final float[] queryVector;
    try {
      queryVector = VectorUtils.toFloatArray(keys[0], metadata.encoding);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Expected float array, byte array (INT8), or supported vector type as key for vector search, got " + keys[0].getClass(), e);
    }

    if (queryVector.length != metadata.dimensions)
      throw new IllegalArgumentException(
          "Query vector dimension " + queryVector.length + " does not match index dimension " + metadata.dimensions);

    // Ensure index is ready (not BUILDING or INVALID)
    ensureIndexReady();

    final int k = limit > 0 ? limit : 10; // Default to top 10 results

    // Ensure graph is available (lazy-load from disk if needed)
    ensureGraphAvailable();

    // Issue #3679: rebuild graph if needed (sync for first build or small graphs, async for large graphs)
    rebuildGraphBeforeSearch();

    boolean readLockHeld = false;
    lock.readLock().lock();
    readLockHeld = true;
    try {
      // Perform scored search via findNeighborsFromVector (includes delta scan)
      final List<RID> resultRIDs;
      if (graphIndex == null && deltaVectors.isEmpty()) {
        resultRIDs = Collections.emptyList();
      } else {
        final List<Pair<RID, Float>> scoredResults = findNeighborsFromVector(queryVector, k, null);
        resultRIDs = new ArrayList<>(scoredResults.size());
        for (final Pair<RID, Float> p : scoredResults)
          resultRIDs.add(p.getFirst());
      }

      return new IndexCursor() {
        private int position = 0;

        @Override
        public boolean hasNext() {
          return position < resultRIDs.size();
        }

        @Override
        public Identifiable next() {
          if (!hasNext())
            // #5635: an exhausted IndexCursor throws, it does not hand the caller a null element
            throw new NoSuchElementException();
          return resultRIDs.get(position++);
        }

        @Override
        public Identifiable getRecord() {
          if (position > 0 && position <= resultRIDs.size())
            return resultRIDs.get(position - 1);
          return null;
        }

        @Override
        public Object[] getKeys() {
          return new Object[] { queryVector };
        }

        @Override
        public byte[] getBinaryKeyTypes() {
          return new byte[0];
        }

        @Override
        public BinaryComparator getComparator() {
          return null;
        }

        @Override
        public long estimateSize() {
          return resultRIDs.size();
        }

        /**
         * #5662: a cursor iterates ITSELF, like every other {@link IndexCursor}. Handing back the backing list's own
         * iterator gave a for-each a second, independent traversal that left {@code position} untouched, so
         * {@link #getRecord()} reported nothing during the loop and mixing {@code next()} with a for-each read some
         * RIDs twice.
         */
        @Override
        public Iterator<Identifiable> iterator() {
          return this;
        }
      };
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error performing vector search", e);
      throw new IndexException("Error performing vector search", e);
    } finally {
      if (readLockHeld) {
        lock.readLock().unlock();
      }
    }
  }

  @Override
  public void put(final Object[] keys, final RID[] values) {
    put(keys, values, false);
  }

  /**
   * Replay entry point: the operation was already queued on the transaction, apply it to the index now.
   */
  @Override
  public void putReplay(final Object[] keys, final RID[] rids) {
    put(keys, rids, true);
  }

  private void put(final Object[] keys, final RID[] values, final boolean replay) {
    // Track insert metrics
    final long startTime = System.currentTimeMillis();
    metrics.incrementInsertOperations();

    try {
      if (keys == null || keys.length == 0)
        throw new IllegalArgumentException("Keys cannot be null or empty");

      // Handle null keys according to null strategy
      if (keys[0] == null) {
        // Vector indexes always use SKIP strategy - silently skip null values
        return;
      }

      if (values == null || values.length == 0)
        throw new IllegalArgumentException("Values cannot be null or empty");

      // ComparableVector (transaction replay), float[] (FLOAT32), or byte[] (INT8 only; rejected
      // for non-INT8 indexes by the encoding-aware overload).
      final float[] vector;
      if (keys[0] instanceof ComparableVector c)
        vector = c.vector;
      else {
        try {
          vector = VectorUtils.toFloatArray(keys[0], metadata.encoding);
        } catch (final IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "Expected float array, byte array (INT8), or ComparableVector as key for vector index, got " + keys[0].getClass(), e);
        }
      }

      if (vector.length != metadata.dimensions)
        throw new IllegalArgumentException(
            "Vector dimension does not match index dimension " + metadata.dimensions + ": got "
                + keys[0].getClass().getSimpleName() + " of length " + vector.length);

      final RID rid = values[0];

      if (!replay && isTransactionalCall()) {
        // Queue on TransactionIndexContext for file locking and transaction tracking.
        // Wrap vector in ComparableVector for TransactionIndexContext's TreeMap.
        // TransactionIndexContext will replay this operation during commit, which will hit the else branch below.
        getDatabase().getTransaction()
            .addIndexOperation(this, TransactionIndexContext.IndexKey.IndexKeyOperation.ADD,
                new Object[] { new ComparableVector(vector) }, rid);

      } else {
        // No transaction OR during commit replay: apply immediately
        // During commit phases, TransactionIndexContext.commit() calls this method directly
        lock.writeLock().lock();
        try {
          final int id = nextId.getAndIncrement();

          // Persist vector to page (will be added to vectorIndex inside persistVectorWithLocation)
          persistVectorWithLocation(id, rid, vector);

          // Track in liveVectorValues for metadata consistency (O(1) - just a map put)
          final VectorFloat<?> vf = vts.createFloatVector(vector);
          if (liveVectorValues != null)
            liveVectorValues.addVector(id, vf);

          // Add to delta buffer so the vector is visible in search via mergeWithDeltaScan.
          // Skipping expensive O(log n) HNSW graph inserts during commit replay (issue #3864):
          // the inactivity rebuild timer will incorporate delta vectors into the graph.
          // The already-converted VectorFloat is reused so the search path never re-converts (issue #5391).
          deltaVectors.add(new DeltaVectorEntry(id, rid, vf));

          if (graphState == GraphState.IMMUTABLE || graphState == GraphState.LOADING)
            this.graphState = GraphState.MUTABLE;

          // Increment mutation counter (used for periodic graph persistence)
          mutationsSinceSerialize.incrementAndGet();
        } finally {
          lock.writeLock().unlock();
        }

        // Schedule inactivity rebuild timer outside the lock (issue #3737)
        scheduleInactivityRebuild();
      }
    } finally {
      // Track insert latency (only for actual writes, not transaction registration)
      if (replay || !isTransactionalCall()) {
        final long elapsed = System.currentTimeMillis() - startTime;
        metrics.addInsertLatency(elapsed);
      }
    }
  }

  /**
   * Whether this call must be queued on the transaction instead of being applied to the index right away.
   * <p>
   * Every index entry of a transaction is applied once, by {@code TransactionIndexContext.commit()}. Both the write
   * phase of a transaction (BEGUN) and the record-serialization step of its commit (COMMIT_1ST_PHASE, which re-runs
   * {@code DocumentIndexer.updateDocument} for every record updated in the transaction) therefore only queue.
   * Including COMMIT_1ST_PHASE matters here and not for the other index types: an LSM key/value entry re-applied
   * twice collapses into the same key, while a vector put mints a NEW vector id every time, so the second pass used
   * to leave one extra vector plus one extra tombstone per updated record - doubling the growth of the index file
   * and of the in-memory location index on every re-embedding cycle (issue #5516). The replay itself comes in
   * through {@link #putReplay}/{@link #removeReplay} and bypasses this check.
   * <p>
   * A replica never replays the queue (the index pages arrive with the leader's changes), so there the commit-time
   * call stays the one that applies the change.
   */
  private boolean isTransactionalCall() {
    final TransactionContext tx = getDatabase().getTransaction();
    final TransactionContext.STATUS txStatus = tx.getStatus();
    return txStatus == TransactionContext.STATUS.BEGUN ||
        (txStatus == TransactionContext.STATUS.COMMIT_1ST_PHASE && tx.isIndexChangesReplayed());
  }

  /**
   * Batch insert multiple vectors in a single lock acquisition.
   * Called by TransactionIndexContext during commit replay for efficient batch processing (issue #3864).
   * Skips per-vector HNSW graph inserts and schedules a single inactivity rebuild at the end.
   * Vectors are immediately visible in search via delta scan (mergeWithDeltaScan).
   * <p>
   * <b>Failure handling differs from {@link #put(Object[], RID[])} on purpose.</b> {@code put}
   * throws on a bad key type because the caller is still on the stack and can react;
   * {@code putBatch} runs during commit replay where the originating caller has long returned, so
   * a thrown exception would abort the entire batch and lose every following row. Instead, each
   * bad row is logged at WARNING (with rid, type, and cause) and the batch continues - silent
   * index drift is the only failure mode worse than skipping a single row, and the WARNING gives
   * operators a signal to investigate.
   *
   * @param keysList list of key arrays, each containing a single ComparableVector or float[]
   * @param ridsList list of corresponding RIDs
   */
  public void putBatch(final List<Object[]> keysList, final List<RID> ridsList) {
    if (keysList.isEmpty())
      return;

    final long startTime = System.currentTimeMillis();

    lock.writeLock().lock();
    try {
      for (int i = 0; i < keysList.size(); i++) {
        final Object[] keys = keysList.get(i);
        final RID rid = ridsList.get(i);

        if (keys == null || keys.length == 0 || keys[0] == null)
          continue;

        final float[] vector;
        if (keys[0] instanceof ComparableVector c)
          vector = c.vector;
        else {
          try {
            vector = VectorUtils.toFloatArray(keys[0], metadata.encoding);
          } catch (final IllegalArgumentException e) {
            // Drop the row but log loudly: putBatch runs during commit replay where the originating
            // call site has long returned, so a swallowed conversion failure is the only thing
            // separating an operator from silent index drift. The cause is preserved for triage.
            LogManager.instance().log(this, Level.WARNING,
                "Vector index '%s' skipping batch entry for %s: unsupported key type %s (%s)",
                indexName, rid, keys[0].getClass().getSimpleName(), e.getMessage());
            continue;
          }
        }

        if (vector.length != metadata.dimensions) {
          LogManager.instance().log(this, Level.WARNING,
              "Vector index '%s' skipping batch entry for %s: %s of length %d does not match index dimension %d",
              indexName, rid, keys[0].getClass().getSimpleName(), vector.length, metadata.dimensions);
          continue;
        }

        final int id = nextId.getAndIncrement();
        persistVectorWithLocation(id, rid, vector);

        // Track in liveVectorValues for metadata consistency (O(1) - just a map put)
        final VectorFloat<?> vf = vts.createFloatVector(vector);
        if (liveVectorValues != null)
          liveVectorValues.addVector(id, vf);

        // Add to delta buffer for search visibility via mergeWithDeltaScan, reusing the already-converted
        // VectorFloat so the search path never re-converts the whole buffer per query (issue #5391).
        deltaVectors.add(new DeltaVectorEntry(id, rid, vf));

        mutationsSinceSerialize.incrementAndGet();
      }

      if (graphState == GraphState.IMMUTABLE || graphState == GraphState.LOADING)
        this.graphState = GraphState.MUTABLE;

      metrics.incrementInsertOperations(keysList.size());
    } finally {
      lock.writeLock().unlock();
    }

    // Schedule ONE inactivity rebuild for the entire batch (outside the lock)
    scheduleInactivityRebuild();

    final long elapsed = System.currentTimeMillis() - startTime;
    metrics.addInsertLatency(elapsed);
  }

  @Override
  public void remove(final Object[] keys) {
    // Not directly supported - use remove(keys, rid) instead
    throw new UnsupportedOperationException("Use remove(keys, rid) for vector index");
  }

  @Override
  public void remove(final Object[] keys, final Identifiable value) {
    remove(keys, value, false);
  }

  /**
   * Replay entry point: the operation was already queued on the transaction, apply it to the index now.
   */
  @Override
  public void removeReplay(final Object[] keys, final Identifiable rid) {
    remove(keys, rid, true);
  }

  private void remove(final Object[] keys, final Identifiable value, final boolean replay) {
    final RID rid = value.getIdentity();

    if (!replay && isTransactionalCall()) {
      // Queue on TransactionIndexContext for file locking and transaction tracking.
      // Use a dummy ComparableVector since we don't have the vector value for removes.
      // TransactionIndexContext will replay this operation during commit, which will hit the else branch below.
      getDatabase().getTransaction()
          .addIndexOperation(this, TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE,
              new Object[] { new ComparableVector(new float[metadata.dimensions]) }, rid);

    } else {
      // No transaction OR during commit replay: apply immediately
      // During commit phases, TransactionIndexContext.commit() calls this method directly
      lock.writeLock().lock();
      try {
        // Find all vectors with matching RID and mark as deleted. Resolve them in O(k) through the RID reverse index
        // instead of scanning every vector id in the index (issue #5318): the old full scan made any record update on
        // a vector-indexed type O(index size), so bulk updates degraded quadratically.
        final List<Integer> deletedIds = new ArrayList<>();
        for (final int vectorId : vectorIndex.getVectorIdsForRid(rid)) {
          final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
          if (loc != null && loc.rid.equals(rid) && !loc.deleted) {
            vectorIndex.markDeleted(vectorId);
            deletedIds.add(vectorId);
            // Do not let the shared search cache pin a vector that no longer exists (issue #5412)
            final VectorCache cache = searchVectorCache;
            if (cache != null)
              cache.remove(vectorId);
          }
        }

        // Persist deletion tombstones
        if (!deletedIds.isEmpty()) {
          persistDeletionTombstones(deletedIds, rid);

          // Remove matching entries from delta buffer
          if (!deltaVectors.isEmpty())
            deltaVectors.removeIf(entry -> entry.rid.equals(rid));

          // Phase 5+: Periodic rebuild strategy (amortizes cost over many operations)
          if (graphState == GraphState.IMMUTABLE || graphState == GraphState.LOADING) {
            // Transition to MUTABLE state to track ongoing mutations
            this.graphState = GraphState.MUTABLE;
          }

          // Increment mutation counter (count number of deletions)
          mutationsSinceSerialize.addAndGet(deletedIds.size());

          // Schedule inactivity rebuild timer (issue #3737)
          scheduleInactivityRebuild();
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  /**
   * Schedules a compaction when the data file has grown well past what its live vectors need.
   * <p>
   * The file is append-only: an update writes a new vector plus a tombstone for the one it supersedes, a delete
   * writes a tombstone, and nothing reclaims either - so a workload that keeps re-embedding the same vectors grows
   * the file without bound while the live set stays the same size. That was the other half of issue #5516, the half
   * a heap dump cannot show, and leaving it to an operator remembering to run COMPACT INDEX is not a fix.
   * <p>
   * The trigger is the garbage ratio rather than a page count, because a vector compaction is not the cheap merge an
   * LSM-tree compaction is: it rebuilds the graph and rewrites the file, so it has to pay for itself in reclaimed
   * space. The work happens on the async executor - never on the committing thread - and goes through
   * {@link #compact()}, so it is the same replication-safe path an explicit COMPACT INDEX takes.
   */
  public void onAfterCommit() {
    // The whole body is guarded, the decision included: this runs inside the commit, before it is marked
    // committed, so anything thrown here would fail a transaction whose data is already durable.
    try {
      if (!isCompactionDue())
        return;

      // scheduleCompaction() inside async().compact() flips AVAILABLE -> COMPACTION_SCHEDULED, so a second commit
      // arriving while one is still pending is a no-op instead of a queue of redundant rewrites.
      ((DatabaseAsyncExecutorImpl) getDatabase().async()).compact(this);
    } catch (final Exception e) {
      // Scheduling must never fail the commit that just succeeded - the file simply stays as it is and the next
      // commit, or an explicit COMPACT INDEX, picks it up. On a closing database (the async executor is already
      // gone) that is routine and stays quiet; anywhere else it means this index has stopped reclaiming on its
      // own, which nobody would notice at FINE.
      // toString() rather than getMessage(): an exception that only carries a cause has a null message, and the one
      // line that says this index stopped reclaiming on its own must not read "... : null".
      LogManager.instance().log(this, getDatabase().isOpen() ? Level.WARNING : Level.FINE,
          "Could not schedule the compaction of vector index '%s': %s", indexName, e.toString());
    }
  }

  /**
   * Whether the data file holds enough garbage to be worth rewriting: at least
   * {@link GlobalConfiguration#VECTOR_INDEX_COMPACTION_BLOAT_FACTOR} times the pages its live vectors need.
   * <p>
   * Runs after every commit, so it stays on counters and configuration lookups already in memory - the page count,
   * the number of resident locations, two settings read live - and never touches a page.
   */
  private boolean isCompactionDue() {
    if (!isCompactionAllowedOnThisNode(getDatabase()))
      return false;

    final ContextConfiguration configuration = getDatabase().getConfiguration();

    // Read both knobs live so they behave the same way: the cached minPagesToScheduleACompaction only reflects the
    // value an index was built with, which would make one of the two gates ignore a runtime change.
    final int minPages = configuration.getValueAsInteger(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
    if (minPages <= 0)
      // Automatic compaction disabled for every index type.
      return false;

    final int bloatFactor = configuration.getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR);
    if (bloatFactor <= 0)
      // Explicit COMPACT INDEX only.
      return false;

    // The ratio below reads VectorLocationIndex.size() as the live-vector count, which holds because that map never
    // evicts (issue #5559 deleted the bounded backend that could). Anything that gives it back an eviction policy
    // owes this an explicit live count instead - a map that evicts reports a lower bound, so an index holding more
    // live vectors than it caches reads as permanently bloated and rewrites itself after nearly every commit.
    // COMPACT INDEX already derives that count by parsing the pages.
    final int totalPages = totalPagesForBloatRatio();
    if (totalPages < minPages)
      // Too small to be worth a rewrite whatever its garbage ratio is.
      return false;

    final int pagesForLiveSet = estimatePagesForLiveSet();
    if (pagesForLiveSet < 1)
      return false;

    return totalPages >= (long) pagesForLiveSet * bloatFactor;
  }

  /**
   * Every page the ratio has to account for. {@link #getTotalPages()} is the mutable file alone, but the live set the
   * estimate is built from spans the compacted companion too when one is loaded ({@code loadVectorsFromFile} pulls its
   * locations into the same map), so counting only the mutable would compare a part against the whole and let the file
   * grow well past the configured factor before anything fired.
   * <p>
   * A companion only exists on a database whose index was compacted by a build older than #5521 - nothing writes one
   * any more, and the first compaction folds it into the new mutable and drops it - so this is the upgrade window
   * rather than the steady state. Cheap enough to be right in it anyway.
   * <p>
   * {@code compactedSubIndex} is not volatile and this reads it from the committing thread while a compaction may be
   * clearing it on an async one. Deliberate: the answer feeds a heuristic, so a stale read moves one compaction by
   * one commit and nothing else, and a volatile read on the commit path would cost more than that is worth.
   */
  private int totalPagesForBloatRatio() {
    return getTotalPages() + (compactedSubIndex != null ? compactedSubIndex.getTotalPages() : 0);
  }

  /**
   * Whether this node is the one that compacts. A Raft follower receives the compacted file from the leader, and
   * {@code runWithCompactionReplication} already declines on a follower - but it declines from inside an async task
   * that had to be queued, run and reset first. A write-heavy follower is exactly the node whose garbage ratio keeps
   * crossing the threshold, so without this gate every one of its commits schedules a task that does nothing.
   * Mirrors the leader check {@code TimeSeriesMaintenanceScheduler.runMaintenance} makes for the same reason.
   *
   * @return true when standalone (never replicated) or on the current leader
   */
  static boolean isCompactionAllowedOnThisNode(final DatabaseInternal database) {
    return !database.isReplicated() || database.isLeader();
  }

  /**
   * Pages the live vectors alone would occupy, derived from the entry layout {@code persistVectorWithLocation}
   * writes. An estimate on purpose: it feeds a ratio against a configurable factor, so being a few percent out
   * moves when a compaction happens, never whether the result is correct.
   */
  private int estimatePagesForLiveSet() {
    // size() is the resident location count, which is the live count: markDeleted() and an addOrUpdate() that
    // supersedes an id both remove the old entry from the map rather than flagging it (getActiveCount() filters
    // defensively, but on this backend it has nothing left to filter). Only the unbounded backend reaches here -
    // isCompactionDue() turns back a map that evicts, which is what would break that equivalence. Counting the
    // values instead would be a full map scan on every commit, which this check must not do.
    final int liveVectors = vectorIndex.size();
    if (liveVectors < 1)
      return 0;

    // vectorId + bucketId + position are zig-zag varints, so 4+4+8 is their typical size, not their maximum (a
    // 32-bit id can reach 5 bytes and a 64-bit position 10). Under-counting here over-counts how many entries fit
    // in a page, which makes the trigger marginally eager - harmless for a ratio against a configurable factor.
    // Plus the deleted flag and the quantization-type byte, both always written.
    // A quantized entry then carries the array length as an int, plus the array and its min/max (INT8) or median
    // (BINARY) - exactly what calculateQuantizedDataSize returns, so the size comes from the reader's own arithmetic
    // instead of a second copy of it here. NONE and PRODUCT return 0 there: they keep the vector in the document,
    // which leaves the page entry as the header above.
    final int quantizedSize = LSMVectorIndexPageParser.calculateQuantizedDataSize(metadata.dimensions,
        metadata.quantizationType);
    final int entrySize = 4 + 4 + 8 + 1 + 1 + (quantizedSize > 0 ? 4 + quantizedSize : 0);

    final int usablePerPage = getPageSize() - BasePage.PAGE_HEADER_SIZE - HEADER_BASE_SIZE;
    final int entriesPerPage = Math.max(1, usablePerPage / entrySize);
    return (liveVectors + entriesPerPage - 1) / entriesPerPage;
  }

  @Override
  public long countEntries() {
    // Use vectorIndex which already applies LSM merge-on-read semantics
    // (latest entry for each RID, filtering out deleted entries)
    return vectorIndex.getActiveCount();
  }

  @Override
  public String getName() {
    return indexName;
  }

  @Override
  public String getTypeName() {
    return metadata.typeName;
  }

  @Override
  public List<String> getPropertyNames() {
    return metadata.propertyNames;
  }

  @Override
  public List<Integer> getFileIds() {
    // #4937: the companion graph file receives page writes during the transaction too - it must be part of
    // the commit lock set, or its pages pass the version checks without their file lock held.
    if (graphFile != null)
      return List.of(mutable.getFileId(), graphFile.getFileId());
    return Collections.singletonList(mutable.getFileId());
  }

  @Override
  public int getPageSize() {
    return mutable.getPageSize();
  }

  public int getTotalPages() {
    return mutable.getTotalPages();
  }

  public int getFileId() {
    return mutable.getFileId();
  }

  public DatabaseInternal getDatabase() {
    return mutable.getDatabase();
  }

  /**
   * Upgrade a bare internal-storage RID to a {@link DatabaseRID} bound to this index's database so callers can do {@code result.getFirst().asDocument()} without
   * relying on thread-local database context.
   */
  private RID bindRid(final RID rid) {
    return rid instanceof DatabaseRID ? rid : getDatabase().newRID(rid.getBucketId(), rid.getPosition());
  }

  public String getComponentName() {
    return mutable.getName();
  }

  /**
   * Get the current graph index (for Phase 2: reading vectors from graph file).
   * Package-private to allow access from ArcadePageVectorValues.
   *
   * @return The current graph index (may be OnHeapGraphIndex or OnDiskGraphIndex)
   */
  ImmutableGraphIndex getGraphIndex() {
    return graphIndex;
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    return null;
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    // Not applicable for this index type
  }

  @Override
  public TypeIndex getTypeIndex() {
    return null;
  }

  @Override
  public void updateTypeName(final String newTypeName) {
    // Type name is immutable for vector indexes
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return new byte[0]; // Vector indexes use float arrays, not binary key types
  }

  @Override
  public String getMostRecentFileName() {
    // The CURRENT component, not the name this index was created with. The schema keys its index entries by this
    // (LocalDocumentType.toJSON) and a compaction swaps in a new data file with a new name, so answering the
    // creation name leaves the leader's schema pointing at a file that no longer exists while a follower - which
    // rebuilds the index from the file it received - names it correctly, and the two schemas diverge. Same
    // contract as LSMTreeIndex.getMostRecentFileName().
    return mutable.getName();
  }

  @Override
  public boolean scheduleCompaction() {
    checkIsValid();
    if (getDatabase().getPageManager().isPageFlushingSuspended(getDatabase()))
      return false;
    return status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.COMPACTION_SCHEDULED);
  }

  @Override
  public boolean isCompacting() {
    return status.get() == INDEX_STATUS.COMPACTION_IN_PROGRESS;
  }

  @Override
  public boolean isValid() {
    return valid && buildState != BUILD_STATE.INVALID;
  }

  /**
   * Ensures the index is in READY state before allowing queries.
   * Throws IndexException if index is BUILDING or INVALID.
   */
  private void ensureIndexReady() {
    if (buildState == BUILD_STATE.INVALID) {
      throw new IndexException("Index '" + indexName +
          "' is INVALID due to interrupted build. Run 'REBUILD INDEX " + indexName + "' to fix.");
    }
    if (buildState == BUILD_STATE.BUILDING) {
      throw new IndexException("Index '" + indexName +
          "' is currently being built. Wait for build to complete.");
    }
  }

  /**
   * Persists the build state to metadata and schema.
   * Called during index build lifecycle to track state across restarts.
   */
  private void persistBuildState(final BUILD_STATE state) {
    this.buildState = state;
    this.metadata.buildState = state.name();
    // Force schema save to persist state immediately
    try {
      getDatabase().getSchema().getEmbedded().saveConfiguration();
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.SEVERE, "Failed to persist build state %s for index %s", e, state, indexName);
      throw new IndexException("Failed to persist build state for index '" + indexName + "'", e);
    }
  }

  /**
   * FOR TESTING ONLY: Simulates a crash by setting build state to BUILDING.
   * This is used by tests to verify crash recovery logic.
   */
  void simulateCrashForTest() {
    persistBuildState(BUILD_STATE.BUILDING);
  }

  /**
   * FOR TESTING ONLY: Marks index as INVALID for testing query blocking.
   */
  void markInvalidForTest() {
    persistBuildState(BUILD_STATE.INVALID);
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.LSM_VECTOR;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return LSMTreeIndexAbstract.NULL_STRATEGY.SKIP;
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    // Not applicable for vector index
  }

  @Override
  public int getAssociatedBucketId() {
    if (metadata.associatedBucketId == -1)
      LogManager.instance().log(this, Level.WARNING, "getAssociatedBucketId() returning -1, metadata not set!");
    return metadata.associatedBucketId;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    // Every exit before the state moves to COMPACTION_IN_PROGRESS has to hand the scheduling slot back. A
    // compaction that gives up here - a backup suspended page flushing, the index went invalid - would otherwise
    // leave the status at COMPACTION_SCHEDULED for good, and since scheduleCompaction() only moves AVAILABLE ->
    // SCHEDULED, that silently disables every later compaction of this index, the explicit COMPACT INDEX included,
    // until the database is reopened. Rare while compaction was operator-driven; reachable on any backup window now
    // that a commit can schedule one.
    boolean compactionStarted = false;
    try {
      LogManager.instance().log(this, Level.FINE, "compact() called for index: %s", null, getName());
      checkIsValid();
      final DatabaseInternal database = getDatabase();

      if (database.getMode() == ComponentFile.MODE.READ_ONLY)
        throw new DatabaseIsReadOnlyException("Cannot update the index '" + getName() + "'");

      if (database.getPageManager().isPageFlushingSuspended(database)) {
        LogManager.instance().log(this, Level.FINE, "compact() returning false: page flushing suspended");
        // POSTPONE COMPACTING (DATABASE BACKUP IN PROGRESS?)
        return false;
      }

      LogManager.instance().log(this, Level.FINE,
          "compact() current status: %s, attempting compareAndSet from COMPACTION_SCHEDULED to COMPACTION_IN_PROGRESS",
          status.get());
      if (!status.compareAndSet(INDEX_STATUS.COMPACTION_SCHEDULED, INDEX_STATUS.COMPACTION_IN_PROGRESS)) {
        LogManager.instance()
            .log(this, Level.FINE, "compact() returning false: status compareAndSet failed (current status: %s)",
                status.get());
        // COMPACTION NOT SCHEDULED
        return false;
      }
      compactionStarted = true;

      try {
          // Compaction IS a rebuild that also rewrites the data file: the rebuild already reads every page, resolves
        // the LSM merge and produces the live set, so the compacted content is exactly what it computes. Running the
        // two as one pass is what keeps them from disagreeing about which vectors are live - a standalone compactor
        // with its own merge rules used to resurrect deleted vectors and duplicate updated ones.
        //
        // Same component-shipping pipeline as LSMTreeIndex compaction and PaginatedSparseVectorEngine flush: the
        // recording session captures registerFile + the page writes + the drop of the files they replace, and the
        // synthetic WAL ships the new component to followers atomically with the leader's commit. The compacted
        // pages are written synchronously, so the wait below is not what makes THEM durable: it is the guard that
        // the session ships nothing still pending from the rest of the rebuild (#4928).
        final boolean success = database.getWrappedDatabaseInstance().runWithCompactionReplication(() -> {
          final int fileIdBefore = getFileId();
          buildGraphFromScratch(null, true);
          // If the bounded wait gives up (#4928), the shipped component could contain unflushed (zero) pages:
          // fail the compaction, it is rescheduled later.
          if (!database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database))
            throw new IOException("Vector index compaction aborted: pages are still pending flush after the no-progress timeout");
          // The data file is swapped only when the rewrite actually ran (it is skipped on an empty or
          // partially-recovered live set), so the file id is what says whether anything was compacted.
          return getFileId() != fileIdBefore;
        });
        if (success) {
          // Track successful compaction
          metrics.incrementCompactionCount();
        }
        return success;
      } catch (final TimeoutException e) {
        LogManager.instance().log(this, Level.FINE, "compact() caught TimeoutException: %s", e.getMessage());
        // IGNORE IT, WILL RETRY LATER
        return false;
      } finally {
        status.set(INDEX_STATUS.AVAILABLE);
      }

    } finally {
      if (!compactionStarted)
        // Never reached COMPACTION_IN_PROGRESS: release the slot this attempt reserved so the next commit, or an
        // operator, can schedule again.
        status.compareAndSet(INDEX_STATUS.COMPACTION_SCHEDULED, INDEX_STATUS.AVAILABLE);
    }
  }

  @Override
  public JSONObject toJSON() {
    // Store complete vector index metadata in schema JSON for replication.
    // This single source of truth is used both for schema persistence and distributed replication.
    final JSONObject json = new JSONObject();

    // Add required fields for schema loading (matching LSMTreeIndex pattern)
    json.put("type", getType());
    json.put("bucket", getDatabase().getSchema().getBucketById(getAssociatedBucketId()).getName());

    // Add vector-specific metadata. The name is the CURRENT component, not the one this instance was built with:
    // a compaction swaps the data file, and a node that reloads the index from the file it received names it after
    // that file. Serializing the creation name instead would make the leader's schema and its followers' differ by
    // this field alone after every compaction. Nothing reads it back - it is informational, which is why
    // LSMTreeIndex writes no name at all.
    json.put("indexName", getMostRecentFileName());
    json.put("typeName", metadata.typeName);
    json.put("properties", metadata.propertyNames);
    json.put("dimensions", metadata.dimensions);
    json.put("similarityFunction", metadata.similarityFunction.name());

    if (metadata.quantizationType != VectorQuantizationType.NONE)
      json.put("quantization", metadata.quantizationType.name());
    if (metadata.encoding != VectorEncoding.FLOAT32)
      json.put("encoding", metadata.encoding.name());
    json.put("maxConnections", metadata.maxConnections);
    json.put("beamWidth", metadata.beamWidth);
    // Every remaining knob is written too, so the persisted definition is complete: a setting that only lives until
    // the next restart is barely better than one that was dropped at creation (issue #5639). LSMVectorIndexMetadata
    // reads each of these back, and a value equal to its default round-trips as itself.
    json.put("efSearch", metadata.efSearch);
    json.put("neighborOverflowFactor", metadata.neighborOverflowFactor);
    json.put("alphaDiversityRelaxation", metadata.alphaDiversityRelaxation);
    json.put("locationCacheSize", metadata.locationCacheSize);
    json.put("graphBuildCacheSize", metadata.graphBuildCacheSize);
    json.put("mutationsBeforeRebuild", metadata.mutationsBeforeRebuild);
    json.put("inactivityRebuildTimeoutMs", metadata.inactivityRebuildTimeoutMs);
    json.put("idPropertyName", metadata.idPropertyName);
    json.put("storeVectorsInGraph", metadata.storeVectorsInGraph);
    json.put("addHierarchy", metadata.addHierarchy);
    json.put("buildState", metadata.buildState);
    json.put("version", CURRENT_VERSION);

    // Product Quantization (PQ) configuration. Written whatever the quantization is: these are only meaningful under
    // PRODUCT, but emitting them conditionally left one exception to "the persisted definition carries every setting"
    // for a reader to work out, and four small integers are not worth the exception.
    json.put("pqSubspaces", metadata.pqSubspaces);
    json.put("pqClusters", metadata.pqClusters);
    json.put("pqCenterGlobally", metadata.pqCenterGlobally);
    json.put("pqTrainingLimit", metadata.pqTrainingLimit);

    return json;
  }

  /**
   * Applies metadata from the schema JSON to this vector index.
   * Called by LocalSchema.load() after the index is created to ensure metadata
   * from the central schema overrides any defaults or file-based values.
   * Particularly important during replication when metadata comes from the
   * replicated schema JSON rather than separate .metadata.json files.
   *
   * @param indexJSON The complete index JSON from the schema containing all configuration
   */
  @Override
  public void setMetadata(final JSONObject indexJSON) {
    if (indexJSON == null)
      return;

    final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy = LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(
        indexJSON.getString("nullStrategy", LSMTreeIndexAbstract.NULL_STRATEGY.ERROR.name()));

    setNullStrategy(nullStrategy);

    if (indexJSON.has("typeName"))
      this.metadata.typeName = indexJSON.getString("typeName");
    if (indexJSON.has("properties")) {
      final var jsonArray = indexJSON.getJSONArray("properties");
      this.metadata.propertyNames = new ArrayList<>();
      for (int i = 0; i < jsonArray.length(); i++)
        metadata.propertyNames.add(jsonArray.getString(i));
    }

    metadata.fromJSON(indexJSON);

    LogManager.instance().log(this, Level.FINE, "Applied metadata from schema to vector index: %s (dimensions=%d)",
        indexName,
        this.metadata.dimensions);
  }

  @Override
  public void flush() {
    if (status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE)) {

      // Build and persist graph if it hasn't been built yet
      // This ensures the graph is available on next database open (fast restart)
      // Build graph if it's in LOADING (never built) or MUTABLE (has pending changes) state
      //
      // LOADING means "not loaded into memory", which is not the same as "not
      // persisted on disk": the graph loads lazily on the first search, so a
      // session that never searched leaves it LOADING even when a complete
      // graph is already on disk, and rebuilding then only reproduces a file
      // that already exists. initializeGraphIndex() already draws exactly this
      // distinction with the same predicate.
      final boolean graphAlreadyOnDisk = graphFile != null && graphFile.hasPersistedGraph();
      if (vectorIndex.size() > 0 && (graphState == GraphState.MUTABLE
          || (graphState == GraphState.LOADING && !graphAlreadyOnDisk))) {
        try {
          LogManager.instance()
              .log(this, Level.FINE, "Building graph before close for index: %s (this may take 1-2 minutes for large datasets)",
                  indexName);
          final long startTime = System.currentTimeMillis();
          buildGraphFromScratch();
          final long elapsed = System.currentTimeMillis() - startTime;
          LogManager.instance().log(this, Level.FINE, "Graph building completed in %d seconds", elapsed / 1000);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Failed to build graph before close: " + e.getMessage(), e);
          // Don't fail close if graph building fails
        }
      } else {
        LogManager.instance()
            .log(this, Level.FINE, "Skipping graph build on close: vectorIndexSize=%d, graphState=%s",
                vectorIndex.size(),
                graphState);
      }
    }
  }

  @Override
  public void close() {
    releaseBackgroundResources();
    flush();
  }

  /**
   * Stops the inactivity rebuild timer, the graph build pool and the pooled graph searchers (issue #5418). Split
   * out of {@link #close()} because {@code LocalDatabase} must be able to stop them on every database close and
   * drop WITHOUT closing the index files, which stay open until the pending pages have been flushed.
   */
  @Override
  public void releaseBackgroundResources() {
    // Invalidate first so a concurrent timer thread that wins the monitor after
    // cancelInactivityRebuildTimer() nulls inactivityTimer cannot bypass the isValid()
    // guard and resurrect a fresh Timer.
    valid = false;
    cancelInactivityRebuildTimer();

    // Shut down the dedicated graph build pool to cancel any in-progress build operations.
    // This interrupts ForkJoinPool workers inside jvector's GraphIndexBuilder.build(),
    // which otherwise would not respond to Thread.interrupt() on the parent thread.
    final ForkJoinPool pool = graphBuildPool;
    if (pool != null && !pool.isShutdown()) {
      pool.shutdownNow();
      try {
        pool.awaitTermination(5, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // Cancel any in-progress async graph rebuild
    final Thread rebuildThread = asyncRebuildThread;
    if (rebuildThread != null && rebuildThread.isAlive()) {
      rebuildThread.interrupt();
      try {
        rebuildThread.join(5000); // Wait up to 5 seconds for clean shutdown
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      asyncRebuildThread = null;
      asyncRebuildInProgress = false;
    }

    // Release the pooled graph searchers (and the graph views they hold) - issue #5413.
    final GraphSearcherPool searchers = searcherPool;
    if (searchers != null)
      searchers.clear();
  }

  @Override
  public void drop() {
    lock.writeLock().lock();
    try {
      // Clear all vector locations
      vectorIndex.clear();
      searchVectorCache = null; // Release the shared search cache (issue #5412)
      final GraphSearcherPool searchers = searcherPool;
      if (searchers != null)
        searchers.clear(); // Release the pooled graph searchers (issue #5413)
      ordinalToVectorId = new int[0];
      currentInsertPageNum = -1;

      final DatabaseInternal db = mutable != null ? mutable.getDatabase() : null;

      // Drop compacted sub-index if it exists
      if (compactedSubIndex != null) {
        try {
          final int compactedFileId = compactedSubIndex.getFileId();
          if (db != null && db.isOpen()) {
            db.getPageManager().deleteFile(db, compactedFileId);
            db.getFileManager().dropFile(compactedFileId);
            db.getSchema().getEmbedded().removeFile(compactedFileId);
          } else {
            final File compactedFile = compactedSubIndex.getOSFile();
            if (compactedFile != null && compactedFile.exists() && !compactedFile.delete()) {
              LogManager.instance().log(this, Level.WARNING, "Error deleting compacted index file '%s'",
                  compactedFile.getPath());
            }
          }
        } catch (final Exception e) {
          LogManager.instance()
              .log(this, Level.WARNING, "Error dropping compacted sub-index for '%s': %s", indexName, e.getMessage());
        }
      }

      // Drop the mutable component (this properly deletes the physical file)
      if (mutable != null) {
        try {
          final int mutableFileId = mutable.getFileId();
          if (db != null && db.isOpen()) {
            db.getPageManager().deleteFile(db, mutableFileId);
            db.getFileManager().dropFile(mutableFileId);
            db.getSchema().getEmbedded().removeFile(mutableFileId);
          } else {
            final File mutableFile = mutable.getOSFile();
            if (mutableFile != null && mutableFile.exists() && !mutableFile.delete()) {
              LogManager.instance().log(this, Level.WARNING, "Error deleting mutable index file '%s'",
                  mutableFile.getPath());
            }
          }
        } catch (final Exception e) {
          LogManager.instance()
              .log(this, Level.WARNING, "Error dropping mutable component for '%s': %s", indexName, e.getMessage());
        }
      }

      // Delete graph file if it exists
      if (graphFile != null) {
        final File graphIndexFile = graphFile.getOSFile();
        if (graphIndexFile.exists())
          graphIndexFile.delete();
      }

      // NOTE: Metadata is now embedded in the schema JSON via toJSON() and is automatically
      // deleted when the schema is updated. We no longer need to delete separate .metadata.json files.

      // Close the component
      close();
    } finally {
      lock.writeLock().unlock();
      valid = false;
    }
  }

  @Override
  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();

    // Existing metrics. One read of the volatile field for the whole snapshot: a rebuild publishes a replacement
    // instance (issue #5568), and three separate reads could each land on a different generation and report three
    // numbers that never described the same index.
    final VectorLocationIndex locations = vectorIndex;
    stats.put("totalVectors", (long) locations.size());
    stats.put("activeVectors", locations.getActiveCount());
    // Ask the deleted-id set instead of subtracting: a tombstoned id keeps no resident location since issue #5516,
    // so size() - activeCount() is always 0 now and this stat would report "no deletions" on an index full of them.
    stats.put("deletedVectors", (long) locations.getDeletedCount());
    stats.put("dimensions", (long) metadata.dimensions);
    stats.put("maxConnections", (long) metadata.maxConnections);
    stats.put("beamWidth", (long) metadata.beamWidth);

    // NEW: Graph state metrics
    stats.put("graphState", (long) graphState.ordinal()); // LOADING=0, IMMUTABLE=1, MUTABLE=2
    stats.put("graphNodeCount", graphIndex != null ? (long) graphIndex.getIdUpperBound() : 0L);
    stats.put("mutationsSinceRebuild", (long) mutationsSinceSerialize.get());
    stats.put("asyncRebuildInProgress", asyncRebuildInProgress ? 1L : 0L);
    stats.put("rebuildSnapshotGeneration", rebuildSnapshotGeneration);

    // Calculate mutations threshold (use configured value or default)
    final int defaultMutationsThreshold = getDatabase().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD);
    stats.put("mutationsThreshold", metadata.mutationsBeforeRebuild > 0 ?
        (long) metadata.mutationsBeforeRebuild : (long) defaultMutationsThreshold);
    // The threshold actually used: the absolute one above, raised to a fraction of the graph size so a large
    // index is not fully rebuilt for a trickle of inserts (issue #5391).
    stats.put("effectiveMutationsThreshold", (long) getEffectiveMutationsBeforeRebuild());

    // Delta vectors cached in RAM for brute-force scan between rebuilds
    stats.put("deltaVectorsCount", (long) deltaVectors.size());

    // On-heap cache size of the live incremental builder (bounded, issue #3144)
    stats.put("liveVectorCacheSize", liveVectorValues != null ? (long) liveVectorValues.vectorCount() : 0L);

    // Populate metrics from LSMVectorIndexMetrics
    metrics.populateStats(stats);

    // Index-scoped search cache shared by every query (issue #5412). It is the authority on the vector cache
    // counters, so it overwrites the placeholders above. A hit ratio well below 1 on a steady workload means
    // the working set does not fit: raise arcadedb.vectorIndex.searchCacheSize.
    final VectorCache searchCache = searchVectorCache;
    stats.put("searchVectorCacheCapacity", searchCache != null ? (long) searchCache.capacity() : 0L);
    final GraphSearcherPool searchers = searcherPool;
    stats.put("pooledGraphSearchers", searchers != null ? (long) searchers.size() : 0L);
    stats.put("vectorCacheHits", searchCache != null ? searchCache.getHits() : 0L);
    stats.put("vectorCacheMisses", searchCache != null ? searchCache.getMisses() : 0L);

    // NEW: Memory estimates
    // Quote the retained heap, not the 24-byte payload: this stat is what an operator sizes a heap from, and the
    // payload-only figure it used to report under-estimated the location index several-fold (issue #5568).
    stats.put("estimatedLocationIndexBytes",
        (long) locations.size() * VectorLocationIndex.APPROX_RETAINED_BYTES_PER_LOCATION);
    stats.put("estimatedOrdinalMapBytes", ordinalToVectorId != null ?
        (long) ordinalToVectorId.length * 4L : 0L);

    // NEW: Page statistics
    stats.put("mutablePages", (long) currentMutablePages.get());
    stats.put("compactedPages", compactedSubIndex != null ? (long) compactedSubIndex.getTotalPages() : 0L);

    return stats;
  }

  @Override
  public boolean setStatus(final INDEX_STATUS[] expectedStatuses, final INDEX_STATUS newStatus) {
    for (final INDEX_STATUS expectedStatus : expectedStatuses)
      if (this.status.compareAndSet(expectedStatus, newStatus))
        return true;
    return false;
  }

  @Override
  public LSMVectorIndexMetadata getMetadata() {
    return metadata;
  }

  @Override
  public void setMetadata(final IndexMetadata metadata) {
    checkIsValid();
    this.metadata = (LSMVectorIndexMetadata) metadata;
  }

  @Override
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    return build(callback, null);
  }

  /**
   * Build the vector index with optional graph building progress callback.
   * Uses WAL bypass and transaction chunking for efficient bulk loading.
   *
   * @param callback      Callback for document indexing progress
   * @param graphCallback Callback for graph building progress
   *
   * @return Total number of records indexed
   */
  public long build(final BuildIndexCallback callback, final GraphBuildCallback graphCallback) {
    final long totalRecords;

    lock.writeLock().lock();
    try {
      if (status.compareAndSet(INDEX_STATUS.AVAILABLE, INDEX_STATUS.UNAVAILABLE)) {
        try {
          final DatabaseInternal db = getDatabase();

          // PHASE 1: Mark index as BUILDING and disable WAL
          persistBuildState(BUILD_STATE.BUILDING);

          final boolean startedTransaction =
              db.getTransaction().getStatus() != TransactionContext.STATUS.BEGUN;
          if (startedTransaction)
            db.getWrappedDatabaseInstance().begin();

          // Save original WAL setting and disable for bulk load
          final boolean originalWAL = db.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL);
          db.getTransaction().setUseWAL(false);

          LogManager.instance().log(this, Level.INFO,
              "Building vector index '%s' with WAL disabled and transaction chunking...", indexName);

          try {
            // PHASE 2: Bulk load vector data with chunking
            totalRecords = bulkLoadVectorData(callback);

            // PHASE 3: Build and persist graph with chunking
            if (vectorIndex.size() > 0 && graphState == GraphState.LOADING) {
              buildGraphWithChunking(graphCallback);
            }

            // PHASE 4: Final commit and mark READY
            if (startedTransaction)
              db.getWrappedDatabaseInstance().commit();

            persistBuildState(BUILD_STATE.READY);

            LogManager.instance().log(this, Level.INFO,
                "Vector index '%s' build complete: %d records, state=READY", indexName, totalRecords);

          } catch (final IOException e) {
            // On IO error during graph building: rollback and mark INVALID
            if (startedTransaction && db.getTransaction().getStatus() == TransactionContext.STATUS.BEGUN)
              db.getWrappedDatabaseInstance().rollback();

            persistBuildState(BUILD_STATE.INVALID);

            LogManager.instance().log(this, Level.SEVERE,
                "Vector index '%s' build FAILED (I/O error), marked INVALID: %s", e, indexName, e.getMessage());
            throw new IndexException("Failed to build vector index '" + indexName + "' due to I/O error", e);

          } catch (final Exception e) {
            // On other error: rollback and mark INVALID
            if (startedTransaction && db.getTransaction().getStatus() == TransactionContext.STATUS.BEGUN)
              db.getWrappedDatabaseInstance().rollback();

            persistBuildState(BUILD_STATE.INVALID);

            LogManager.instance().log(this, Level.SEVERE,
                "Vector index '%s' build FAILED, marked INVALID: %s", e, indexName, e.getMessage());
            throw e;

          } finally {
            // RESTORE WAL setting
            db.getTransaction().setUseWAL(originalWAL);
          }

        } finally {
          status.set(INDEX_STATUS.AVAILABLE);
        }
      } else
        throw new NeedRetryException("Error building vector index '" + indexName + "' because it is not available");
    } finally {
      lock.writeLock().unlock();
    }

    return totalRecords;
  }

  /**
   * Bulk load vector data with transaction chunking to avoid memory/size limits.
   * Called during index build with WAL disabled.
   *
   * @param callback Callback for document indexing progress
   *
   * @return Total number of vectors loaded
   */
  private long bulkLoadVectorData(final BuildIndexCallback callback) {
    final AtomicInteger total = new AtomicInteger();
    final long LOG_INTERVAL = 10000;
    final long startTime = System.currentTimeMillis();
    final DatabaseInternal db = getDatabase();

    if (metadata.propertyNames == null || metadata.propertyNames.isEmpty())
      throw new IndexException("Cannot rebuild vector index '" + indexName + "' because property names are missing");

    // Get chunk size from configuration (default 50MB). Guard against accidental 0/negative values to avoid huge
    // single commits.
    final long chunkSizeMB = getTxChunkSize();
    final long chunkSizeBytes = chunkSizeMB * 1024 * 1024;

    LogManager.instance().log(this, Level.INFO,
        "Building vector index '%s' with %dMB chunk size (WAL disabled)...", indexName, chunkSizeMB);

    // Track bytes written for chunking
    final AtomicLong bytesInCurrentChunk = new AtomicLong(0);

    // Scan the bucket and index all documents
    db.scanBucket(db.getSchema().getBucketById(metadata.associatedBucketId).getName(), record -> {
      // Add to index
      db.getIndexer().addToIndex(LSMVectorIndex.this, record.getIdentity(), (Document) record);
      total.incrementAndGet();

      // Estimate bytes written (rough approximation)
      // Each vector: dimensions * 4 bytes + metadata overhead
      final long estimatedBytes = (long) metadata.dimensions * 4 + 32;
      bytesInCurrentChunk.addAndGet(estimatedBytes);

      // Periodic logging
      if (total.get() % LOG_INTERVAL == 0) {
        final long elapsed = System.currentTimeMillis() - startTime;
        final double rate = total.get() / (elapsed / 1000.0);
        LogManager.instance().log(this, Level.INFO,
            "Building vector index '%s': %d records (%.0f rec/sec), chunk: %.1fMB...",
            indexName, total.get(), rate, bytesInCurrentChunk.get() / (1024.0 * 1024.0));
      }

      // Chunk boundary: commit and start new transaction
      if (bytesInCurrentChunk.get() >= chunkSizeBytes) {
        LogManager.instance().log(this, Level.INFO,
            "Committing chunk: %.1fMB written, %d vectors...",
            bytesInCurrentChunk.get() / (1024.0 * 1024.0), total.get());

        db.getWrappedDatabaseInstance().commit();
        db.getWrappedDatabaseInstance().begin();
        db.getTransaction().setUseWAL(false); // Re-disable WAL for new transaction

        bytesInCurrentChunk.set(0);
      }

      if (callback != null)
        callback.onDocumentIndexed((Document) record, total.get());

      return true;
    });

    final long elapsed = System.currentTimeMillis() - startTime;
    LogManager.instance().log(this, Level.INFO,
        "Completed loading vectors for index '%s': %d records in %dms (%.0f rec/sec)",
        indexName, total.get(), elapsed, total.get() / (elapsed / 1000.0));

    return total.get();
  }

  /**
   * Build graph from vectors and persist with chunking support.
   * Called during index build with WAL disabled.
   *
   * @param graphCallback Callback for graph building progress
   */
  private void buildGraphWithChunking(final GraphBuildCallback graphCallback) throws IOException {
    LogManager.instance().log(this, Level.INFO,
        "LSM Vector graph building with transaction chunking for: %s", indexName);

    final DatabaseInternal db = getDatabase();

    // Get chunk size from configuration (default 50MB). Guard against accidental 0/negative to ensure chunked graph
    // persistence.
    final long chunkSizeMB = getTxChunkSize();

    // Track if we started the transaction (for graph building)
    final boolean startedTransaction = db.getTransaction().getStatus() != TransactionContext.STATUS.BEGUN;
    if (startedTransaction) {
      db.begin();
      db.getTransaction().setUseWAL(false);
    }

    try {
      // Build graph from scratch (already reads from pages)
      buildGraphFromScratchWithRetry(graphCallback, false);

      // Persist graph with chunking callback
      final ChunkCommitCallback chunkCallback = bytesWritten -> {
        LogManager.instance().log(this, Level.INFO,
            "LSM Vector graph persistence chunk complete: %.1fMB written",
            bytesWritten / (1024.0 * 1024.0));

        // Commit current transaction
        db.commit();

        // Start new transaction and disable WAL
        db.begin();
        db.getTransaction().setUseWAL(false);
      };

      // Create vector values accessor for graph serialization
      final String vectorProp =
          metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ? metadata.propertyNames.getFirst() :
              "vector";
      // Serialization walks every ordinal exactly once, so feed the shared search cache while doing it: the
      // index comes out of a rebuild with its working set already resident instead of cold (issue #5412).
      final RandomAccessVectorValues vectors = new ArcadePageVectorValues(getDatabase(), metadata.dimensions,
          vectorProp,
          vectorIndex, ordinalToVectorId, this, getSearchVectorCache());

      graphFile.writeGraph(graphIndex, vectors, chunkSizeMB, chunkCallback);

      // Final commit for graph
      if (startedTransaction) {
        db.commit();
        LogManager.instance().log(this, Level.INFO,
            "LSM Vector graph persisted with chunking for index: %s", indexName);
      }

    } catch (final Exception e) {
      if (startedTransaction)
        db.rollback();
      throw e;
    }
  }

  @Override
  public PaginatedComponent getComponent() {
    return mutable;
  }

  @Override
  public Type[] getKeyTypes() {
    // Honour the index encoding: an INT8 index is built over a {@code byte[]} property (one
    // signed byte per dimension, stored as ArcadeDB's {@code BINARY} type which maps to a Java
    // {@code byte[]}). The default FLOAT32 path keeps the historical contract.
    return new Type[] {
        metadata.encoding == VectorEncoding.INT8 ? Type.BINARY : Type.ARRAY_OF_FLOATS
    };
  }

  public int getDimensions() {
    return metadata.dimensions;
  }

  public VectorSimilarityFunction getSimilarityFunction() {
    return metadata.similarityFunction;
  }

  public int getMaxConnections() {
    return metadata.maxConnections;
  }

  public int getBeamWidth() {
    return metadata.beamWidth;
  }

  public String getIdPropertyName() {
    return metadata.idPropertyName;
  }

  /**
   * Gets the current number of mutable pages.
   */
  public int getCurrentMutablePages() {
    return currentMutablePages.get();
  }

  /** The pages the live vectors alone would need - exposed so a test can express its threshold in those terms. */
  int estimatePagesForLiveSetForTest() {
    return estimatePagesForLiveSet();
  }

  /**
   * Get the VectorLocationIndex (used by the tests to inspect the resident locations)
   */
  protected VectorLocationIndex getVectorIndex() {
    return vectorIndex;
  }

  /**
   * Apply a replicated page update to VectorLocationIndex.
   * Called by TransactionManager.applyChanges() during HA replication to keep
   * in-memory VectorLocationIndex synchronized with replicated pages.
   * <p>
   * This ensures replicas don't have stale VectorLocationIndex that causes offset mismatches.
   *
   * @param page The page that was just replicated and written
   */
  public void applyReplicatedPageUpdate(final MutablePage page) {
    try {
      final int pageNum = page.getPageId().getPageNumber();
      final int fileId = page.getPageId().getFileId();

      // Determine if this page is in the compacted or mutable file
      final boolean isCompacted = compactedSubIndex != null && fileId == compactedSubIndex.getFileId();

      // Read page header
      final int offsetFreeContent = page.readInt(OFFSET_FREE_CONTENT);
      final int numberOfEntries = page.readInt(OFFSET_NUM_ENTRIES);

      LogManager.instance().log(this, Level.FINE,
          "applyReplicatedPageUpdate: index=%s, pageNum=%d, entries=%d, freeContent=%d, contentSize=%d",
          indexName, pageNum, numberOfEntries, offsetFreeContent, page.getContentSize());

      if (numberOfEntries == 0)
        return; // Empty page, nothing to update

      // Calculate header size (compacted page 0 has extra metadata)
      final int headerSize;
      if (isCompacted && pageNum == 0) {
        // Compacted page 0: base header + dimensions + similarity + maxConn + beamWidth
        headerSize = HEADER_BASE_SIZE + (4 * 4); // 9 + 16 = 25 bytes
      } else {
        headerSize = HEADER_BASE_SIZE; // 9 bytes
      }

      // Calculate absolute file offset for this page
      final long pageStartOffset = (long) pageNum * getPageSize();

      // Parse variable-sized entries sequentially (no pointer table)
      int currentOffset = headerSize;
      for (int i = 0; i < numberOfEntries; i++) {
        // Record absolute file offset for this entry
        final long entryFileOffset = pageStartOffset + BasePage.PAGE_HEADER_SIZE + currentOffset;

        // Read variable-sized vectorId
        final long[] vectorIdAndSize = page.readNumberAndSize(currentOffset);
        final int id = (int) vectorIdAndSize[0];
        currentOffset += (int) vectorIdAndSize[1];

        // Read variable-sized bucketId
        final long[] bucketIdAndSize = page.readNumberAndSize(currentOffset);
        final int bucketId = (int) bucketIdAndSize[0];
        currentOffset += (int) bucketIdAndSize[1];

        // Read variable-sized position
        final long[] positionAndSize = page.readNumberAndSize(currentOffset);
        final long position = positionAndSize[0];
        currentOffset += (int) positionAndSize[1];

        final RID rid = new RID(bucketId, position);

        // Read deleted flag (fixed 1 byte)
        final boolean deleted = page.readByte(currentOffset) == 1;
        currentOffset += 1;

        // Read quantization type byte (always written, even if NONE)
        final byte quantOrdinal = (byte) page.readByte(currentOffset);
        currentOffset += 1;
        final VectorQuantizationType quantType = quantOrdinal >= 0 && quantOrdinal < VectorQuantizationType.values().length
            ? VectorQuantizationType.values()[quantOrdinal] : VectorQuantizationType.NONE;

        // Skip quantized vector data based on type
        if (quantType == VectorQuantizationType.INT8) {
          final int vectorLength = page.readInt(currentOffset);
          currentOffset += 4; // vector length (int)
          currentOffset += vectorLength; // quantized bytes
          currentOffset += 8; // min + max (2 floats)
        } else if (quantType == VectorQuantizationType.BINARY) {
          final int originalLength = page.readInt(currentOffset);
          currentOffset += 4; // original length (int)
          currentOffset += (originalLength + 7) / 8; // packed bytes (1 bit per dimension)
          currentOffset += 4; // median (float)
        }

        // Update VectorLocationIndex with this entry's absolute file offset
        // LSM semantics: later entries override earlier ones
        vectorIndex.addOrUpdate(id, isCompacted, entryFileOffset, rid, deleted);
      }

      LogManager.instance()
          .log(this, Level.FINE, "Applied replicated page update: pageNum=%d, fileId=%d, isCompacted=%b, entries=%d",
              pageNum,
              fileId, isCompacted, numberOfEntries);

    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.SEVERE, "Error applying replicated page update for index %s: %s", e, indexName,
              e.getMessage());
    }
  }

  /**
   * Reports, once per index, that a configured location cache limit is being ignored (issues #5568 and #5559).
   * <p>
   * {@code arcadedb.vectorIndex.locationCacheSize} (and its per-index {@code locationCacheSize} metadata) used to
   * cap the location index and let it evict. That is not a cache bound - it is data loss. A location is the only
   * record of which RID a vector id belongs to and where its entry sits in the file; there is no vector id to
   * offset index on disk, so an evicted entry cannot be recovered. Measured: a cap of 100 over 1000 live vectors
   * makes {@code countEntries()} report 100 and {@link #getStats()} under-report by the same 900. Every reader
   * that resolves a vector id reads a missing location as deleted (see the result loops of the search paths), so
   * a query whose neighbours were evicted drops them.
   * <p>
   * The cap was introduced when the index held one location per WRITE, so it grew without bound on a re-embedding
   * workload. Issue #5516 removed that: a tombstoned id releases its location, so residency is O(live vectors) -
   * proportional to the data the user asked to index, and roughly 90 bytes each. Capping that buys a memory
   * ceiling by silently returning wrong results, which is never the right trade for a database.
   * <p>
   * {@code locationCacheSize} is refused outright when it arrives through DDL or a builder
   * ({@code LSMVectorIndexMetadata.applyUserMetadata}), so the only two ways to reach this warning are the global
   * setting and a schema persisted by an older version. Both are tolerated - refusing them would stop a server
   * starting or a database opening - and both are reported here instead. Bringing the footprint down is a storage
   * question, not an eviction one: laying the locations out in primitive arrays indexed by vector id would cost
   * ~20 bytes each instead of ~90, with no per-entry objects at all (issue #5588).
   *
   * @param database The database instance, since {@code mutable} may not be initialized yet
   */
  private void warnIfLocationCacheSizeConfigured(final DatabaseInternal database) {
    final int configured = metadata != null && metadata.locationCacheSize > -1 ?
        metadata.locationCacheSize :
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_LOCATION_CACHE_SIZE);

    if (configured > 0 && locationCacheCapReported.compareAndSet(false, true))
      LogManager.instance().log(this, Level.WARNING,
          """
          Ignoring a location cache limit of %d for vector index '%s': evicting a live vector location deletes the \
          only mapping from its vector id to its record, so a capped index silently drops vectors from searches. \
          Locations are resident only for live vectors since issue #5516, so the index costs ~90 bytes per indexed \
          vector regardless of this setting""",
          configured, indexName);
  }

  /**
   * Get the explicitly configured graph build cache size (per-index metadata or global setting).
   *
   * @return Maximum number of vectors to cache during graph building, or 0/-1 when left to auto-sizing
   */
  private int getGraphBuildCacheSize() {
    if (metadata != null && metadata.graphBuildCacheSize > -1) {
      return metadata.graphBuildCacheSize;
    }
    return mutable.getDatabase().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE);
  }

  /**
   * Computes how many vectors the graph-build cache should hold.
   * <p>
   * An explicit {@code arcadedb.vectorIndex.graphBuildCacheSize} (or the per-index metadata) wins. Otherwise the
   * capacity depends on what a cache miss costs during the build:
   * <ul>
   *   <li>inline-quantized indexes (INT8/BINARY) read a miss straight from an index page on any thread, so a
   *       small bound is enough and the heap is better spent elsewhere;</li>
   *   <li>document-based indexes (NONE/PRODUCT) pay a record lookup plus a full property deserialization on
   *       every miss, so the whole corpus is cached when the heap allows it. This is what the validation phase
   *       materializes anyway: bounding the cache below it (issue #3144) threw the vectors away right after
   *       reading them and made a from-scratch fp32 build re-read almost every vector, hundreds of times each.</li>
   * </ul>
   *
   * @param expectedSize        number of vectors the build will walk
   * @param inlineQuantization  whether vectors are readable from index pages without a record lookup
   *
   * @return the number of vectors to hold, always positive
   */
  int computeGraphBuildCacheCapacity(final int expectedSize, final boolean inlineQuantization) {
    final int configured = getGraphBuildCacheSize();
    if (configured > 0)
      return configured;

    if (inlineQuantization)
      return ArcadePageVectorValues.DEFAULT_CACHE_SIZE;

    // Per-entry cost: the float payload plus the VectorFloat wrapper, the cache entry and the array slot
    final long bytesPerVector = (long) metadata.dimensions * Float.BYTES + 64;

    int heapPercent = mutable.getDatabase().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT);
    if (heapPercent <= 0)
      return ArcadePageVectorValues.DEFAULT_CACHE_SIZE;
    if (heapPercent > 90)
      heapPercent = 90;

    final long heapBudget = Runtime.getRuntime().maxMemory() / 100 * heapPercent;
    final long affordable = Math.max(ArcadePageVectorValues.DEFAULT_CACHE_SIZE, heapBudget / bytesPerVector);
    final long wanted = Math.max(1, expectedSize);

    return (int) Math.min(Math.min(affordable, wanted), Integer.MAX_VALUE / 2);
  }

  /**
   * Returns the cache of materialized vectors shared by every search on this index (issue #5412), growing it
   * when the corpus has outgrown the current capacity. Capacity only ever moves in powers of two, so a growing
   * index reallocates O(log n) times over its whole lifetime.
   *
   * @return the shared cache, or {@code null} when caching is disabled by configuration
   */
  VectorCache getSearchVectorCache() {
    final int wanted = computeSearchCacheCapacity();
    if (wanted <= 0)
      return null;

    VectorCache cache = searchVectorCache;
    if (cache != null && cache.capacity() >= wanted)
      return cache;

    synchronized (searchVectorCacheLock) {
      cache = searchVectorCache;
      if (cache == null || cache.capacity() < wanted) {
        cache = new VectorCache(wanted);
        searchVectorCache = cache;
      }
      return cache;
    }
  }

  /**
   * Returns the pool of JVector graph searchers shared by every search on this index (issue #5413), creating it
   * on first use.
   */
  GraphSearcherPool getSearcherPool() {
    GraphSearcherPool pool = searcherPool;
    if (pool != null)
      return pool;

    synchronized (searcherPoolLock) {
      pool = searcherPool;
      if (pool == null) {
        final int configured = mutable.getDatabase().getConfiguration()
            .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_SEARCHER_POOL_SIZE);
        // 0 = auto: two searchers per core, so every core can keep one checked out while another request is
        // between borrow and release, without retaining one per (potentially 500) HTTP worker thread.
        final int maxIdle = configured == 0 ? Math.max(4, Runtime.getRuntime().availableProcessors() * 2) : configured;
        pool = new GraphSearcherPool(maxIdle);
        searcherPool = pool;
      }
      return pool;
    }
  }

  /**
   * Epoch used to decide whether a pooled searcher (and the graph view it holds) is still valid. It must change on
   * anything that can alter what a search would see: a graph rebuild swaps {@link #graphIndex} and advances
   * {@link #rebuildSnapshotGeneration}, and every insert/update/delete bumps {@link #mutationsSinceSerialize}.
   * <p>
   * The value never repeats. That matters because graph identity is not a sufficient guard on its own: the
   * live-builder path keeps serving searches from the same graph instance while it grows, so the epoch is the only
   * thing that can tell a pooled view its contents moved underneath it.
   */
  private long searcherPoolEpoch() {
    // The mutation counter alone cannot carry this: a rebuild subtracts back exactly what it absorbed
    // (mutationsAtBuildStart), so a settled index reads the same value after every rebuild and an epoch taken from it
    // repeats. Pairing it with the rebuild generation, which only ever increases, makes the epoch strictly monotonic
    // across both kinds of event. The generation occupies the high bits so ordinary mutations still move the low ones.
    return (rebuildSnapshotGeneration << 32) | (mutationsSinceSerialize.get() & 0xFFFFFFFFL);
  }

  /**
   * Computes how many vectors the shared search cache should hold.
   * <p>
   * An explicit {@code arcadedb.vectorIndex.searchCacheSize} wins. Otherwise the cache is sized to hold the
   * whole corpus - which is the point: a working set that fits never touches disk again - but capped at the
   * configured share of the heap so a corpus larger than RAM degrades to eviction instead of OOM.
   *
   * @return the number of vectors to hold, or 0 when caching is disabled
   */
  private int computeSearchCacheCapacity() {
    final int configured = mutable.getDatabase().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE);
    if (configured < 0)
      return 0;
    if (configured > 0)
      return configured;

    // Per-entry cost: the float payload plus the VectorFloat wrapper, the cache entry and the array slot
    final long bytesPerVector = (long) metadata.dimensions * Float.BYTES + 64;

    int heapPercent = mutable.getDatabase().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_MAX_HEAP_PERCENT);
    if (heapPercent <= 0)
      return 0;
    if (heapPercent > 90)
      heapPercent = 90;

    final long heapBudget = Runtime.getRuntime().maxMemory() / 100 * heapPercent;
    final long affordable = Math.max(MIN_SEARCH_CACHE_SIZE, heapBudget / bytesPerVector);

    final long corpus = Math.max(MIN_SEARCH_CACHE_SIZE, vectorIndex.size());

    return (int) Math.min(Math.min(affordable, corpus), Integer.MAX_VALUE / 2);
  }

  /**
   * Get the mutations before rebuild threshold from configuration (per-index metadata or global default).
   *
   * @return Number of mutations before rebuilding graph index
   */
  private int getMutationsBeforeRebuild() {
    if (metadata != null && metadata.mutationsBeforeRebuild > 0) {
      return metadata.mutationsBeforeRebuild;
    }
    return mutable.getDatabase().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD);
  }

  /**
   * Threshold actually used to decide whether the graph should be rebuilt.
   * <p>
   * A rebuild always re-indexes the whole graph, so triggering one every fixed number of mutations means
   * spending O(index size) to absorb a handful of vectors: at 200K vectors the engine rebuilds a 200K-node graph
   * for every 100 inserts, which is the escalating GC and CPU load reported in issue #5391 and makes bulk
   * ingestion quadratic. Scaling the threshold with the current graph size amortizes rebuilds geometrically
   * (a constant number of full rebuilds per doubling), while pending vectors stay exactly searchable through the
   * delta buffer in the meantime. The result is capped so the delta buffer's RAM stays bounded.
   *
   * @return Number of pending mutations that trigger a graph rebuild
   */
  private int getEffectiveMutationsBeforeRebuild() {
    final int absolute = getMutationsBeforeRebuild();
    final ImmutableGraphIndex graph = this.graphIndex;
    if (graph == null)
      return absolute;

    final ContextConfiguration configuration = mutable.getDatabase().getConfiguration();
    final float ratio = configuration.getValueAsFloat(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO);
    if (ratio <= 0f)
      return absolute;

    final long scaled = (long) (graph.size() * (double) ratio);
    final int maxPending = configuration.getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_MAX_PENDING_MUTATIONS);
    final long capped = maxPending > 0 ? Math.min(scaled, maxPending) : scaled;
    return (int) Math.max(absolute, capped);
  }

  /**
   * Get the inactivity rebuild timeout from configuration (per-index metadata or global default).
   *
   * @return Inactivity timeout in milliseconds, or 0 if disabled
   */
  private int getInactivityRebuildTimeoutMs() {
    if (metadata != null && metadata.inactivityRebuildTimeoutMs >= 0)
      return metadata.inactivityRebuildTimeoutMs;
    return mutable.getDatabase().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS);
  }

  /**
   * Schedule or reset the inactivity rebuild timer (issue #3737).
   * Called after each mutation when mutations are below the rebuild threshold.
   * If a timer is already scheduled, it is cancelled and a new one is started,
   * effectively resetting the inactivity window.
   * When the timer fires, it triggers an async graph rebuild regardless of the mutation count.
   */
  private synchronized void scheduleInactivityRebuild() {
    if (!isValid())
      return; // Index closed or dropped - no point scheduling

    final int timeoutMs = getInactivityRebuildTimeoutMs();
    if (timeoutMs <= 0)
      return; // Disabled

    if (mutationsSinceSerialize.get() <= 0)
      return; // Nothing to rebuild

    // Cancel any previously scheduled task (reset on new mutation) and purge the cancelled
    // entry from the Timer's queue so a high write rate does not let cancelled tasks pile up.
    final TimerTask existing = inactivityRebuildTask;
    if (existing != null) {
      existing.cancel();
      if (inactivityTimer != null)
        inactivityTimer.purge();
    }

    final TimerTask task = new TimerTask() {
      @Override
      public void run() {
        // Double-check: only rebuild if there are still pending mutations
        if (mutationsSinceSerialize.get() <= 0)
          return;

        LogManager.instance().log(this, Level.INFO,
            "Inactivity timeout expired (%d ms), triggering graph rebuild for %d pending mutations (index: %s)",
            timeoutMs, mutationsSinceSerialize.get(), indexName);

        try {
          if (graphIndex != null && graphIndex.size() >= ASYNC_REBUILD_MIN_GRAPH_SIZE) {
            // Large graph: async rebuild (semaphore acquired inside the async thread)
            startAsyncGraphRebuild();
          } else {
            // Small graph: synchronous rebuild on the timer thread.
            // Use tryAcquire to avoid blocking the timer thread indefinitely.
            // If another rebuild holds the permit, re-arm the timer so this index
            // retries at the next interval rather than staying stuck with pending mutations.
            if (REBUILD_SEMAPHORE.tryAcquire()) {
              try {
                buildGraphFromScratch();
              } finally {
                REBUILD_SEMAPHORE.release();
              }
            } else {
              LogManager.instance().log(this, Level.FINE,
                  "Skipping inactivity rebuild for index %s: another rebuild is already in progress, will retry in %d ms",
                  indexName, timeoutMs);
              scheduleInactivityRebuild();
            }
          }
        } catch (final Exception | AssertionError e) {
          // AssertionError too: JVector validates with `assert`, so a build over an index whose vectors can no
          // longer be read (a closing database, say) throws one straight through. Letting it escape kills the
          // shared TimerThread, and the failure then surfaces on whatever unrelated work runs next.
          LogManager.instance().log(this, Level.WARNING,
              "Error during inactivity rebuild for index %s: %s", indexName, e.getMessage());
        }
      }
    };

    inactivityRebuildTask = task;

    if (inactivityTimer == null)
      inactivityTimer = new Timer("VectorIndex-InactivityTimer-" + indexName, true);
    inactivityTimer.schedule(task, timeoutMs);
  }

  /**
   * Cancel the inactivity rebuild timer if one is scheduled.
   */
  private synchronized void cancelInactivityRebuildTimer() {
    final TimerTask task = inactivityRebuildTask;
    if (task != null) {
      task.cancel();
      inactivityRebuildTask = null;
    }
    final Timer timer = inactivityTimer;
    if (timer != null) {
      timer.cancel();
      inactivityTimer = null;
    }
  }

  private void checkIsValid() {
    if (!valid)
      throw new IndexException("Index '" + indexName + "' is not valid. Probably has been drop or rebuilt");
  }
}
