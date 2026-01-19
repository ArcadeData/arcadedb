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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.engine.BasePage;
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
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.LockManager;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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

  // Page header layout constants
  public static final int OFFSET_FREE_CONTENT = 0;  // 4 bytes
  public static final int OFFSET_NUM_ENTRIES  = 4;   // 4 bytes
  public static final int OFFSET_MUTABLE      = 8;       // 1 byte
  public static final int HEADER_BASE_SIZE    = 9;     // offsetFreeContent(4) + numberOfEntries(4) + mutable(1)

  private final String                 indexName;
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
  private final    VectorLocationIndex           vectorIndex;       // Lightweight pointer index
  private final    AtomicInteger                 nextId;
  private final    AtomicReference<INDEX_STATUS> status;

  // Graph file for persistent storage of graph topology
  // Allows lazy-loading graph from disk and avoiding expensive rebuilds
  private       LSMVectorIndexGraphFile graphFile;
  private final AtomicInteger           mutationsSinceSerialize;

  // Product Quantization (PQ) for zero-disk-I/O approximate search
  // PQ file stores codebooks and encoded vectors; pqVectors is cached in memory
  private          LSMVectorIndexPQFile pqFile;
  private volatile PQVectors            pqVectors;
  private volatile ProductQuantization  productQuantization;

  // Thresholds for graph state transitions
  // Note: JVector's addGraphNode() is meant for pre-build additions, not post-build incremental updates
  // Therefore we use periodic rebuilds which amortize cost over many operations (10x better than
  // rebuild-on-every-search)
  // Mutation threshold is now configurable via metadata.mutationsBeforeRebuild or GlobalConfiguration

  // Compaction support
  private final    AtomicInteger           currentMutablePages;
  private final    int                     minPagesToScheduleACompaction;
  private          LSMVectorIndexCompacted compactedSubIndex;
  private          boolean                 valid      = true;
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
   * Helper class for collecting vector entries during graph build.
   * Used to avoid race conditions with concurrent VectorLocationIndex modifications.
   */
  private static class VectorEntryForGraphBuild {
    final int     vectorId;
    final RID     rid;
    final boolean isCompacted;
    final long    absoluteFileOffset;

    VectorEntryForGraphBuild(final int vectorId, final RID rid, final boolean isCompacted,
        final long absoluteFileOffset) {
      this.vectorId = vectorId;
      this.rid = rid;
      this.isCompacted = isCompacted;
      this.absoluteFileOffset = absoluteFileOffset;
    }
  }

  public static class LSMVectorIndexFactoryHandler implements IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder<? extends Index> builder) {
      final BucketLSMVectorIndexBuilder vectorBuilder = (BucketLSMVectorIndexBuilder) builder;

      return new LSMVectorIndex(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(),
          ComponentFile.MODE.READ_WRITE,
          builder.getPageSize(), vectorBuilder.getTypeName(), vectorBuilder.getPropertyNames(),
          vectorBuilder.dimensions,
          vectorBuilder.similarityFunction, vectorBuilder.maxConnections, vectorBuilder.beamWidth,
          vectorBuilder.idPropertyName,
          vectorBuilder.quantizationType, vectorBuilder.locationCacheSize, vectorBuilder.graphBuildCacheSize,
          vectorBuilder.mutationsBeforeRebuild, vectorBuilder.storeVectorsInGraph, vectorBuilder.addHierarchy,
          vectorBuilder.pqSubspaces, vectorBuilder.pqClusters, vectorBuilder.pqCenterGlobally,
          vectorBuilder.pqTrainingLimit);
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
   * Constructor for creating a new index
   */
  public LSMVectorIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode,
      final int pageSize, final String typeName, final String[] propertyNames, final int dimensions,
      final VectorSimilarityFunction similarityFunction, final int maxConnections,
      final int beamWidth, final String idPropertyName,
      final VectorQuantizationType quantizationType, final int locationCacheSize,
      final int graphBuildCacheSize,
      final int mutationsBeforeRebuild, final boolean storeVectorsInGraph, final boolean addHierarchy,
      final int pqSubspaces, final int pqClusters, final boolean pqCenterGlobally,
      final int pqTrainingLimit) {
    try {
      this.indexName = name;

      this.metadata = new LSMVectorIndexMetadata(typeName, propertyNames, -1);
      this.metadata.dimensions = dimensions;
      this.metadata.similarityFunction = similarityFunction;
      this.metadata.quantizationType = quantizationType;
      this.metadata.maxConnections = maxConnections;
      this.metadata.beamWidth = beamWidth;
      this.metadata.idPropertyName = idPropertyName;
      this.metadata.locationCacheSize = locationCacheSize;
      this.metadata.graphBuildCacheSize = graphBuildCacheSize;
      this.metadata.mutationsBeforeRebuild = mutationsBeforeRebuild;
      this.metadata.storeVectorsInGraph = storeVectorsInGraph;
      this.metadata.addHierarchy = addHierarchy;
      this.metadata.pqSubspaces = pqSubspaces;
      this.metadata.pqClusters = pqClusters;
      this.metadata.pqCenterGlobally = pqCenterGlobally;
      this.metadata.pqTrainingLimit = pqTrainingLimit;

      this.lock = new ReentrantReadWriteLock();
      this.vectorIndex = new VectorLocationIndex(getLocationCacheSize(database));
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
    this.vectorIndex = new VectorLocationIndex(getLocationCacheSize(database));
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
        .log(this, Level.SEVERE, "loadVectorsAfterSchemaLoad called for index %s: dimensions=%d, mutablePages=%d, hasGraphFile=%s",
            indexName, metadata.dimensions, mutable.getTotalPages(), graphFile != null);

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
            .log(this, Level.SEVERE, "Loading vectors for index %s after schema load (dimensions=%d, pages=%d, fileId=%d)",
                indexName, metadata.dimensions, mutable.getTotalPages(), mutable.getFileId());

        loadVectorsFromPages();

        // Graph will be lazy-loaded on first search via ensureGraphAvailable()
        // Don't build it here - causes deadlock during database load when PageManager isn't fully ready
        LogManager.instance().log(this, Level.SEVERE,
            "Successfully loaded %d vector locations for index %s (graph will be lazy-loaded on first search)",
            vectorIndex.size(),
            indexName);

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
          .log(this, Level.SEVERE, "Skipping vector load for index %s (dimensions=%d, pages=%d)", indexName,
              metadata.dimensions,
              mutable.getTotalPages());
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
      LogManager.instance().log(this, Level.WARNING, "Error discovering compacted sub-index for %s: %s", indexName,
          e.getMessage());
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
      LogManager.instance()
          .log(this, Level.SEVERE, "DEBUG: initializeGraphIndex building graph for %s, vectorIndex.size=%d", indexName,
              vectorIndex.size());

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
      final boolean hasDeletedVectors = vectorIndex.getAllVectorIds().anyMatch(id -> {
        final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(id);
        return loc != null && loc.deleted;
      });
      if (hasDeletedVectors) {
        LogManager.instance().log(this, Level.INFO,
            """
                Deleted vectors detected in index %s - rebuilding graph from scratch to ensure ordinal consistency \
                (fixes issue #3135: stale ordinal mappings after vector updates)""",
            indexName);
      }

      if (graphFile != null && graphFile.hasPersistedGraph() && !needsGraphRebuildForPQ && !hasDeletedVectors) {
        try {
          this.graphIndex = graphFile.loadGraph();
          this.graphState = GraphState.IMMUTABLE;

          // Rebuild ordinalToVectorId from vectorIndex
          // IMPORTANT: Must match the validation logic used during graph building
          final String vectorProp =
              metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ?
                  metadata.propertyNames.getFirst() : "vector";

          this.ordinalToVectorId = vectorIndex.getAllVectorIds().filter(id -> {
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
                if (vector == null || vector.length != metadata.dimensions) {
                  return false;
                }
                // Check not all zeros
                for (float v : vector) {
                  if (v != 0.0f) {
                    return true;
                  }
                }
                return false;  // All zeros
              } catch (final Exception e) {
                return false;
              }
            } else {
              // Without quantization: validate by reading from document
              try {
                final Record record = getDatabase().lookupByRID(loc.rid, false);
                if (record == null) {
                  return false;
                }
                final Document doc = (Document) record;
                final Object vectorObj = doc.get(vectorProp);
                final float[] vector = VectorUtils.convertToFloatArray(vectorObj);
                if (vector == null || vector.length != metadata.dimensions) {
                  return false;
                }
                // Check not all zeros
                for (float v : vector) {
                  if (v != 0.0f) {
                    return true;
                  }
                }
                return false;  // All zeros
              } catch (final Exception e) {
                return false;
              }
            }
          }).sorted().toArray();

          LogManager.instance().log(this, Level.INFO,
              "Loaded graph from disk for index: %s, graphSize=%d, ordinalToVectorIdLength=%d, vectorIndexSize=%d",
              indexName,
              graphIndex != null ? graphIndex.size() : 0, ordinalToVectorId.length, vectorIndex.size());

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
                  vectorLocationSnapshot, ordinalToVectorId, this, getGraphBuildCacheSize());
              buildAndPersistPQ(vectors);
            } catch (final Exception e) {
              LogManager.instance().log(this, Level.WARNING,
                  "Failed to build PQ after graph load for index %s: %s", indexName, e.getMessage());
            }
          }
          return;
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
      buildGraphFromScratchWithRetry(graphCallback);
    } finally {
      status.set(INDEX_STATUS.AVAILABLE);
    }
  }

  /**
   * Build graph from scratch by reading all active vectors and constructing the graph index.
   * After building, persists the graph to disk and transitions to IMMUTABLE state.
   */
  private void buildGraphFromScratch() {
    buildGraphFromScratch(null);
  }

  /**
   * Build graph from scratch with optional progress callback.
   *
   * @param graphCallback Optional callback for graph build progress
   */
  private void buildGraphFromScratch(final GraphBuildCallback graphCallback) {
    // buildGraphFromScratchWithRetry() reads pages directly and rebuilds vectorIndex
    // No need to reload here - just call the retry logic directly
    buildGraphFromScratchWithRetry(graphCallback);
  }

  /**
   * Internal implementation of buildGraphFromScratch.
   * Always reads directly from pages to avoid race conditions with concurrent VectorLocationIndex modifications.
   *
   * @param graphCallback Optional callback for graph build progress
   */
  private void buildGraphFromScratchWithRetry(final GraphBuildCallback graphCallback) {
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
    final Map<RID, VectorEntryForGraphBuild> ridToLatestVector = new HashMap<>();
    final int[] totalEntriesRead = { 0 };
    final int[] filteredDeletedVectors = { 0 };

    final DatabaseInternal database = getDatabase();

    // Read from compacted sub-index if it exists
    if (compactedSubIndex != null) {
      LSMVectorIndexPageParser.parsePages(database, compactedSubIndex.getFileId(), compactedSubIndex.getTotalPages(),
          getPageSize(), true, entry -> {
            totalEntriesRead[0]++;
            if (entry.deleted) {
              filteredDeletedVectors[0]++;
              return;
            }
            // Keep latest (highest ID) vector for each RID
            final VectorEntryForGraphBuild existing = ridToLatestVector.get(entry.rid);
            if (existing == null || entry.vectorId > existing.vectorId)
              ridToLatestVector.put(entry.rid,
                  new VectorEntryForGraphBuild(entry.vectorId, entry.rid, true, entry.absoluteFileOffset));
          });
    }

    // Read from mutable index
    LSMVectorIndexPageParser.parsePages(database, getFileId(), getTotalPages(), getPageSize(), false, entry -> {
      totalEntriesRead[0]++;
      if (entry.deleted) {
        filteredDeletedVectors[0]++;
        return;
      }
      // Keep latest (highest ID) vector for each RID (mutable entries override compacted)
      final VectorEntryForGraphBuild existing = ridToLatestVector.get(entry.rid);
      if (existing == null || entry.vectorId > existing.vectorId)
        ridToLatestVector.put(entry.rid,
            new VectorEntryForGraphBuild(entry.vectorId, entry.rid, false, entry.absoluteFileOffset));
    });

    // Build ordinal mapping from deduplicated vectors read directly from pages
    final int[] activeVectorIds = ridToLatestVector.values().stream().mapToInt(v -> v.vectorId).sorted().toArray();

    // Log statistics
    if (filteredDeletedVectors[0] > 0)
      LogManager.instance().log(this, Level.INFO,
          "Graph build from pages: %d total entries, %d deleted, %d active for graph",
          totalEntriesRead[0], filteredDeletedVectors[0], activeVectorIds.length);

    // Acquire write lock for updating vectorIndex and preparing build
    lock.writeLock().lock();
    final RandomAccessVectorValues vectors;
    final int[] finalActiveVectorIds;
    try {
      // CRITICAL: If we couldn't read any entries from pages (e.g., during database close),
      // DON'T clear vectorIndex - use what's already in memory!
      final int[] vectorIds;
      if (!ridToLatestVector.isEmpty()) {
        // Update vectorIndex to match what we found on pages (sync it with disk state)
        // This ensures vectorIndex is consistent with the graph we're about to build
        vectorIndex.clear();
        for (final VectorEntryForGraphBuild entry : ridToLatestVector.values()) {
          vectorIndex.addOrUpdate(entry.vectorId, entry.isCompacted, entry.absoluteFileOffset, entry.rid, false);
        }
        vectorIds = activeVectorIds; // Use vector IDs from pages
      } else {
        LogManager.instance().log(this, Level.SEVERE,
            "FALLBACK: Could not read vectors from pages (database closing), using existing vectorIndex with %d entries",
            vectorIndex.size());
        // Build vector IDs from existing vectorIndex
        vectorIds = vectorIndex.getAllVectorIds().filter(id -> {
          final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(id);
          return loc != null && !loc.deleted;
        }).sorted().toArray();
        LogManager.instance()
            .log(this, Level.SEVERE, "FALLBACK: Built %d active vector IDs from in-memory vectorIndex",
                vectorIds.length);
      }

      // Create a SNAPSHOT of vectorIndex for JVector to use safely
      final String vectorProp =
          metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ? metadata.propertyNames.get(0) :
              "vector";

      // CRITICAL FIX: Validate vectors before building graph to filter out deleted documents
      // When a document is deleted, getVector() returns null which breaks JVector index building
      final Map<Integer, VectorLocationIndex.VectorLocation> vectorLocationSnapshot = new HashMap<>();
      final Map<Integer, VectorFloat<?>> preloadedVectors = new HashMap<>();
      final List<Integer> validVectorIds = new ArrayList<>();
      int skippedDeletedDocs = 0;

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
          final boolean hasInlineQuantization = metadata.quantizationType == VectorQuantizationType.INT8 ||
              metadata.quantizationType == VectorQuantizationType.BINARY;
          if (hasInlineQuantization) {
            // With INT8/BINARY quantization: vectors are in index pages, document validation not needed
            // Just validate that we can read the quantized vector
            try {
              final float[] vector = readVectorFromOffset(loc.absoluteFileOffset, loc.isCompacted);
              if (vector != null && vector.length == metadata.dimensions) {
                // Validate vector is not all zeros
                boolean hasNonZero = false;
                for (float v : vector) {
                  if (v != 0.0f) {
                    hasNonZero = true;
                    break;
                  }
                }
                if (hasNonZero) {
                  vectorLocationSnapshot.put(vectorId, loc);
                  validVectorIds.add(vectorId);
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
            // Without quantization: validate by reading from document
            try {
              final Record record = database.lookupByRID(loc.rid, false);

              final Document doc = (Document) record;
              final Object vectorObj = doc.get(vectorProp);

              final float[] vector = VectorUtils.convertToFloatArray(vectorObj);

              if (vector != null && vector.length == metadata.dimensions) {
                // Validate vector is not all zeros (would cause NaN in cosine similarity)
                boolean hasNonZero = false;
                for (float v : vector) {
                  if (v != 0.0f) {
                    hasNonZero = true;
                    break;
                  }
                }
                if (hasNonZero) {
                  vectorLocationSnapshot.put(vectorId, loc);
                  validVectorIds.add(vectorId);
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
      this.ordinalToVectorId = filteredVectorIds;
      finalActiveVectorIds = filteredVectorIds;

      if (filteredVectorIds.length == 0) {
        this.graphIndex = null;
        this.graphState = GraphState.IMMUTABLE;
        LogManager.instance().log(this, Level.INFO, "No vectors to index, graph is null for index: " + indexName);
        return;
      }

      final int graphBuildCacheSize = getGraphBuildCacheSize();
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

      // Mark that graph building is in progress to prevent new inserts
      this.graphState = GraphState.MUTABLE;
    } finally {
      lock.writeLock().unlock();
    }

    try {
      // Build the graph index using JVector 4.0 API (WITHOUT holding our lock - JVector uses parallel threads)
      LogManager.instance()
          .log(this, Level.INFO,
              "Building JVector graph index with " + vectors.size() + " vectors for index: " + indexName);

      // Create BuildScoreProvider for index construction
      final BuildScoreProvider scoreProvider = BuildScoreProvider.randomAccessScoreProvider(vectors,
          metadata.similarityFunction);

      // Build the graph index (parallel operation - no lock held)
      final ImmutableGraphIndex builtGraph;
      try (final GraphIndexBuilder builder = new GraphIndexBuilder(
          scoreProvider,
          metadata.dimensions,
          metadata.maxConnections,  // M parameter (graph degree)
          metadata.beamWidth,       // efConstruction (construction search depth)
          metadata.neighborOverflowFactor,    // neighbor overflow factor (default: 1.2)
          metadata.alphaDiversityRelaxation,  // alpha diversity relaxation (default: 1.2)
          metadata.addHierarchy,
          true)) {         // enable concurrent updates

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
          // Stop progress monitoring
          buildComplete.set(true);
          if (progressMonitor != null) {
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

      // Reacquire write lock to update graph state
      lock.writeLock().lock();
      try {
        this.graphIndex = builtGraph;
        this.graphState = GraphState.IMMUTABLE;
        // Track graph rebuild metric
        metrics.incrementGraphRebuildCount();
        // Reset page tracking since we rebuilt from persisted pages
        currentInsertPageNum = -1;
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

        final ChunkCommitCallback chunkCallback = (bytesWritten) -> {
          LogManager.instance().log(this, Level.INFO,
              "Graph persistence chunk complete: %.1fMB written", bytesWritten / (1024.0 * 1024.0));

          // Commit current transaction
          database.commit();

          // Start new transaction and disable WAL
          database.begin();
          database.getTransaction().setUseWAL(false);
        };

        try {
          gf.writeGraph(graphIndex, vectors, chunkSizeMB, chunkCallback);

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

          // Build and persist Product Quantization if PRODUCT quantization is enabled
          if (metadata.quantizationType == VectorQuantizationType.PRODUCT && pqFile != null) {
            buildAndPersistPQ(vectors);
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
      this.mutationsSinceSerialize.set(0);

      LogManager.instance().log(this, Level.INFO, "Built graph for index: " + indexName);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error building graph from scratch", e);
      throw new IndexException("Error building graph from scratch", e);
    }
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

      LogManager.instance().log(this, Level.INFO,
          "Building Product Quantization for index %s: %d vectors, %d dimensions",
          indexName, vectorCount, metadata.dimensions);

      final long startTime = System.currentTimeMillis();

      // Compute PQ subspaces (M) - auto-calculate if not specified
      int pqSubspaces = metadata.pqSubspaces;
      if (pqSubspaces <= 0) {
        // Auto-calculate: dimensions/4, capped at 512 subspaces
        pqSubspaces = Math.min(metadata.dimensions / 4, 512);
        // Ensure at least 1 subspace and dimensions are divisible
        pqSubspaces = Math.max(1, pqSubspaces);
        // Ensure dimensions are divisible by subspaces
        while (metadata.dimensions % pqSubspaces != 0 && pqSubspaces > 1) {
          pqSubspaces--;
        }
      }

      // Validate subspaces configuration
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
      for (int i = 0; i < vectorCount; i++) {
        final VectorFloat<?> vector = vectors.getVector(i);
        if (vector != null) {
          encodedVectors.encodeAndSet(i, vector);
        }
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
   * Rebuild the graph if mutation threshold reached (Phase 5+: Periodic Rebuilds).
   * Rebuilds every N mutations to amortize cost over many operations.
   * Assumes write lock is already held by caller.
   */
  private void rebuildGraphIfNeeded() {
    if (graphState != GraphState.MUTABLE)
      return;

    if (mutationsSinceSerialize.get() < getMutationsBeforeRebuild())
      return; // Not enough mutations yet

    LogManager.instance().log(this, Level.INFO,
        "Rebuilding graph after " + mutationsSinceSerialize.get() + " mutations (threshold: " + getMutationsBeforeRebuild()
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

      // Compute nextId from the maximum vector ID found across both files
      maxVectorId = vectorIndex.getAllVectorIds().max().orElse(-1);
      nextId.set(maxVectorId + 1);

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
      final VectorQuantizationType quantType = (qmeta != null) ? qmeta.getType() : VectorQuantizationType.NONE;
      final byte quantOrdinal = (byte) quantType.ordinal();
      bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, quantOrdinal);

      // Write quantized vector data if quantization is enabled
      if (qmeta != null) {

        if (qmeta.getType() == VectorQuantizationType.INT8) {
          final VectorQuantizationMetadata.Int8QuantizationMetadata int8meta =
              (VectorQuantizationMetadata.Int8QuantizationMetadata) qmeta;

          // Write vector length
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, int8meta.quantized.length);

          // Write quantized bytes
          for (final byte b : int8meta.quantized) {
            bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, b);
          }

          // Write min and max
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(int8meta.min));
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(int8meta.max));

        } else if (qmeta.getType() == VectorQuantizationType.BINARY) {
          final VectorQuantizationMetadata.BinaryQuantizationMetadata binmeta =
              (VectorQuantizationMetadata.BinaryQuantizationMetadata) qmeta;

          // Write original length
          bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, binmeta.originalLength);

          // Write packed bytes
          for (final byte b : binmeta.packed) {
            bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, b);
          }

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
   */
  private void persistDeletionTombstones(final List<Integer> deletedIds) {
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
        final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
        if (loc == null)
          continue;

        // Calculate variable entry size for this specific entry
        final int vectorIdSize = Binary.getNumberSpace(vectorId);
        final int bucketIdSize = Binary.getNumberSpace(loc.rid.getBucketId());
        final int positionSize = Binary.getNumberSpace(loc.rid.getPosition());
        final int entrySize = vectorIdSize + positionSize + bucketIdSize + 1; // +1 for deleted byte

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
        bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, loc.rid.getBucketId());
        bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, loc.rid.getPosition());
        bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, (byte) 1); // Mark as deleted

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
        packed[byteIndex] |= (1 << bitIndex);
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

          // Read quantized bytes
          final byte[] quantized = new byte[vectorLength];
          for (int i = 0; i < vectorLength; i++) {
            quantized[i] = page.readByte(pos);
            pos += 1;
          }

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

          // Read packed bytes
          final int byteCount = (originalLength + 7) / 8;
          final byte[] packed = new byte[byteCount];
          for (int i = 0; i < byteCount; i++) {
            packed[i] = page.readByte(pos);
            pos += 1;
          }

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
    return findNeighborsFromVector(queryVector, k, null);
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
      if (metadata.similarityFunction == VectorSimilarityFunction.COSINE) {
        boolean isZeroVector = true;
        for (final float v : queryVector) {
          if (v != 0.0f) {
            isZeroVector = false;
            break;
          }
        }
        if (isZeroVector)
          throw new IllegalArgumentException(
              "Query vector cannot be a zero vector when using COSINE similarity (causes undefined similarity)");
      }

      // Ensure graph is available (lazy-load from disk if needed, or build if not persisted)
      ensureGraphAvailable();

      boolean readLockHeld = false;
      lock.readLock().lock();
      readLockHeld = true;
      try {
        // Phase 5+: Check if graph needs rebuilding due to pending mutations
        // With periodic rebuilds (threshold=1000), we may have some pending mutations
        if (graphState == GraphState.MUTABLE && mutationsSinceSerialize.get() > 0) {
          // Graph is out of sync - need to rebuild before searching
          lock.readLock().unlock();
          readLockHeld = false;
          lock.writeLock().lock();
          try {
            // Double-check after acquiring write lock
            if (graphState == GraphState.MUTABLE && mutationsSinceSerialize.get() > 0) {
              LogManager.instance().log(this, Level.FINE,
                  "Rebuilding graph before search (accumulated " + mutationsSinceSerialize.get() + " mutations)");
              buildGraphFromScratch();
            }
            // Downgrade to read lock
            lock.readLock().lock();
            readLockHeld = true;
          } finally {
            lock.writeLock().unlock();
          }
        }

        if (graphIndex == null || vectorIndex.size() == 0)
          return Collections.emptyList();

        // Convert query vector to VectorFloat
        final VectorFloat<?> queryVectorFloat = vts.createFloatVector(queryVector);

        // Create lazy-loading RandomAccessVectorValues
        // Vector property name is the first property in the index
        final String vectorProp =
            metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ? metadata.propertyNames.getFirst() :
                "vector";

        final RandomAccessVectorValues vectors = new ArcadePageVectorValues(getDatabase(), metadata.dimensions,
            vectorProp,
            vectorIndex, ordinalToVectorId, this  // Pass LSM index reference for quantization support
        );

        // Perform search with optional RID filtering
        final Bits bitsFilter = (allowedRIDs != null && !allowedRIDs.isEmpty()) ?
            new RIDBitsFilter(allowedRIDs, ordinalToVectorId, vectorIndex) :
            Bits.ALL;

        // TODO: Use instance GraphSearcher method with metadata.efSearch parameter for better recall control
        // Current static method uses default efSearch behavior
        final SearchResult searchResult = GraphSearcher.search(queryVectorFloat, k, vectors,
            metadata.similarityFunction,
            graphIndex,
            bitsFilter);

        LogManager.instance()
            .log(this, Level.INFO, "GraphSearcher returned %d nodes, graphSize=%d, vectorsSize=%d, ordinalToVectorIdLength=%d",
                searchResult.getNodes().length, graphIndex.size(), vectors.size(), ordinalToVectorId.length);

        // Extract RIDs and scores from search results using ordinal mapping
        final List<Pair<RID, Float>> results = new ArrayList<>();
        int skippedOutOfBounds = 0;
        int skippedDeletedOrNull = 0;
        for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
          final int ordinal = nodeScore.node;
          if (ordinal >= 0 && ordinal < ordinalToVectorId.length) {
            final int vectorId = ordinalToVectorId[ordinal];
            final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
            if (loc != null && !loc.deleted) {
              // JVector returns similarity scores - convert to distance based on similarity function
              final float score = nodeScore.score;
              final float distance = switch (metadata.similarityFunction) {
                case COSINE ->
                  // For cosine, similarity is in [-1, 1], distance is 1 - similarity
                    1.0f - score;
                case EUCLIDEAN ->
                  // For euclidean, the score is already the distance
                    score;
                case DOT_PRODUCT ->
                  // For dot product, higher score is better (closer), so negate it
                    -score;
                default -> score;
              };
              results.add(new Pair<>(loc.rid, distance));
            } else {
              skippedDeletedOrNull++;
            }
          } else {
            skippedOutOfBounds++;
          }
        }

        LogManager.instance()
            .log(this, Level.INFO, "Vector search returned %d results (skipped: %d out of bounds, %d deleted/null)",
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
      if (metadata.similarityFunction == VectorSimilarityFunction.COSINE) {
        boolean isZeroVector = true;
        for (final float v : queryVector) {
          if (v != 0.0f) {
            isZeroVector = false;
            break;
          }
        }
        if (isZeroVector)
          throw new IllegalArgumentException(
              "Query vector cannot be a zero vector when using COSINE similarity (causes undefined similarity)");
      }

      // Ensure graph is available (lazy-load from disk if needed)
      ensureGraphAvailable();

      lock.readLock().lock();
      try {
        if (graphIndex == null || vectorIndex.size() == 0)
          return Collections.emptyList();

        // Convert query vector to VectorFloat
        final VectorFloat<?> queryVectorFloat = vts.createFloatVector(queryVector);

        // Build the memory-resident PQ score function (uses SIMD/Panama if available)
        // This is the key to zero-disk-I/O: we use PQ scores for BOTH navigation AND final scoring
        final ScoreFunction.ApproximateScoreFunction scoreFunction =
            pqVectors.precomputedScoreFunctionFor(queryVectorFloat, metadata.similarityFunction);

        // Create a ReRanker that does NOT pull from disk - just returns PQ similarity
        // This is the critical optimization: we bypass RandomAccessVectorValues entirely
        final ScoreFunction.ExactScoreFunction approxReranker = (ordinal) -> scoreFunction.similarityTo(ordinal);

        // Wrap in a DefaultSearchScoreProvider (concrete implementation)
        final DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(scoreFunction, approxReranker);

        // Create RID filter if needed
        final Bits bitsFilter = (allowedRIDs != null && !allowedRIDs.isEmpty()) ?
            new RIDBitsFilter(allowedRIDs, ordinalToVectorId, vectorIndex) :
            Bits.ALL;

        // Execute search using the PQ-based score provider
        // The graph structure is typically small enough to stay in OS page cache
        // Note: JVector 4.0's search method uses (scoreProvider, topK, Bits) signature
        final SearchResult searchResult;
        try (final GraphSearcher searcher = new GraphSearcher(graphIndex)) {
          searchResult = searcher.search(ssp, k, bitsFilter);
        }

        // Extract RIDs and scores from search results
        final List<Pair<RID, Float>> results = new ArrayList<>();
        int skippedOutOfBounds = 0;
        int skippedDeletedOrNull = 0;
        for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
          final int ordinal = nodeScore.node;
          if (ordinal >= 0 && ordinal < ordinalToVectorId.length) {
            final int vectorId = ordinalToVectorId[ordinal];
            final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
            if (loc != null && !loc.deleted) {
              // Convert similarity score to distance
              final float score = nodeScore.score;
              final float distance = switch (metadata.similarityFunction) {
                case COSINE -> 1.0f - score;
                case EUCLIDEAN -> score;
                case DOT_PRODUCT -> -score;
                default -> score;
              };
              results.add(new Pair<>(loc.rid, distance));
            } else {
              skippedDeletedOrNull++;
            }
          } else {
            skippedOutOfBounds++;
          }
        }

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
    if (keys == null || keys.length == 0 || !(keys[0] instanceof float[] queryVector))
      throw new IllegalArgumentException("Expected float array as key for vector search");

    if (queryVector.length != metadata.dimensions)
      throw new IllegalArgumentException(
          "Query vector dimension " + queryVector.length + " does not match index dimension " + metadata.dimensions);

    // Ensure index is ready (not BUILDING or INVALID)
    ensureIndexReady();

    final int k = limit > 0 ? limit : 10; // Default to top 10 results

    // Ensure graph is available (lazy-load from disk if needed)
    ensureGraphAvailable();

    boolean readLockHeld = false;
    lock.readLock().lock();
    readLockHeld = true;
    try {
      // Phase 5+: Check if graph needs rebuilding due to pending mutations
      // With periodic rebuilds (threshold=1000), we may have some pending mutations
      if (graphState == GraphState.MUTABLE && mutationsSinceSerialize.get() > 0) {
        // Graph is out of sync - need to rebuild before searching
        lock.readLock().unlock();
        readLockHeld = false;
        lock.writeLock().lock();
        try {
          // Double-check after acquiring write lock
          if (graphState == GraphState.MUTABLE && mutationsSinceSerialize.get() > 0) {
            LogManager.instance().log(this, Level.FINE,
                "Rebuilding graph before search (accumulated " + mutationsSinceSerialize.get() + " mutations)");
            buildGraphFromScratch();
          }
          // Downgrade to read lock
          lock.readLock().lock();
          readLockHeld = true;
        } finally {
          lock.writeLock().unlock();
        }
      }

      if (graphIndex == null)
        return new IndexCursor() {
          @Override
          public boolean hasNext() {
            return false;
          }

          @Override
          public Identifiable next() {
            return null;
          }

          @Override
          public Identifiable getRecord() {
            return null;
          }

          @Override
          public Object[] getKeys() {
            return new Object[0];
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
            return 0;
          }

          @Override
          public Iterator<Identifiable> iterator() {
            return Collections.emptyIterator();
          }
        };

      // Perform search using JVector 4.0 API
      final List<RID> resultRIDs = new ArrayList<>();

      if (vectorIndex.size() == 0) {
        LogManager.instance().log(this, Level.INFO, "No vectors in index, returning empty results");
      } else {
        // Convert query vector to VectorFloat
        final VectorFloat<?> queryVectorFloat = vts.createFloatVector(queryVector);

        // Create lazy-loading RandomAccessVectorValues
        // Vector property name is the first property in the index
        final String vectorProp =
            metadata.propertyNames != null && !metadata.propertyNames.isEmpty() ? metadata.propertyNames.get(0) :
                "vector";

        final RandomAccessVectorValues vectors = new ArcadePageVectorValues(getDatabase(), metadata.dimensions,
            vectorProp,
            vectorIndex, ordinalToVectorId, this  // Pass LSM index reference for quantization support
        );

        // Perform search
        final SearchResult searchResult = GraphSearcher.search(queryVectorFloat, k, vectors,
            metadata.similarityFunction,
            graphIndex, Bits.ALL);

        // Extract RIDs from search results using ordinal mapping
        for (final SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
          final int ordinal = nodeScore.node;
          if (ordinal >= 0 && ordinal < ordinalToVectorId.length) {
            final int vectorId = ordinalToVectorId[ordinal];
            final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
            if (loc != null && !loc.deleted) {
              resultRIDs.add(loc.rid);
            }
          }
        }

        LogManager.instance().log(this, Level.FINE, "Vector search returned " + resultRIDs.size() + " results");
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
            return null;
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

        @Override
        public Iterator<Identifiable> iterator() {
          return (Iterator<Identifiable>) (Iterator<?>) resultRIDs.iterator();
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

      // Validate vector - can be either float[] or ComparableVector (from transaction replay)
      final float[] vector;
      if (keys[0] instanceof ComparableVector c)
        vector = c.vector;
      else
        vector = VectorUtils.convertToFloatArray(keys[0]);

      if (vector == null) {
        throw new IllegalArgumentException(
            "Expected float array or ComparableVector as key for vector index, got " + keys[0].getClass());
      }

      if (vector.length != metadata.dimensions)
        throw new IllegalArgumentException(
            "Vector dimension " + vector.length + " does not match index dimension " + metadata.dimensions);

      final RID rid = values[0];
      final TransactionContext.STATUS txStatus = getDatabase().getTransaction().getStatus();

      if (txStatus == TransactionContext.STATUS.BEGUN) {
        // During BEGUN: Register with TransactionIndexContext for file locking and transaction tracking
        // Wrap vector in ComparableVector for TransactionIndexContext's TreeMap
        // TransactionIndexContext will replay this operation during commit, which will hit the else branch below
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

          // Phase 5+: Periodic rebuild strategy (amortizes cost over many operations)
          if (graphState == GraphState.IMMUTABLE || graphState == GraphState.LOADING) {
            // Transition to MUTABLE state to track ongoing mutations
            this.graphState = GraphState.MUTABLE;
          }

          // Increment mutation counter
          mutationsSinceSerialize.incrementAndGet();

          // DON'T trigger rebuild during transaction commit - defer until query time
          // rebuildGraphIfNeeded() calls buildGraphFromScratch() which clears vectorIndex and
          // tries to reload from pages, but pages aren't visible yet during commit phase
          // The graph will be rebuilt on the next query via ensureGraphAvailable() / get()
          // Phase 2: When storeVectorsInGraph is enabled, the rebuilt graph will fetch updated vectors
          // from documents/quantized pages and store them inline in the new graph file
          // rebuildGraphIfNeeded();
        } finally {
          lock.writeLock().unlock();
        }
      }
    } finally {
      // Track insert latency (only for actual writes, not transaction registration)
      final TransactionContext.STATUS txStatus = getDatabase().getTransaction().getStatus();
      if (txStatus != TransactionContext.STATUS.BEGUN) {
        final long elapsed = System.currentTimeMillis() - startTime;
        metrics.addInsertLatency(elapsed);
      }
    }
  }

  @Override
  public void remove(final Object[] keys) {
    // Not directly supported - use remove(keys, rid) instead
    throw new UnsupportedOperationException("Use remove(keys, rid) for vector index");
  }

  @Override
  public void remove(final Object[] keys, final Identifiable value) {
    final RID rid = value.getIdentity();
    final TransactionContext.STATUS txStatus = getDatabase().getTransaction().getStatus();

    if (txStatus == TransactionContext.STATUS.BEGUN) {
      // During BEGUN: Register with TransactionIndexContext for file locking and transaction tracking
      // Use a dummy ComparableVector since we don't have the vector value for removes
      // TransactionIndexContext will replay this operation during commit, which will hit the else branch below
      getDatabase().getTransaction()
          .addIndexOperation(this, TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE,
              new Object[] { new ComparableVector(new float[metadata.dimensions]) }, rid);

    } else {
      // No transaction OR during commit replay: apply immediately
      // During commit phases, TransactionIndexContext.commit() calls this method directly
      lock.writeLock().lock();
      try {
        // Find all vectors with matching RID and mark as deleted
        final List<Integer> deletedIds = new ArrayList<>();
        for (int vectorId : vectorIndex.getAllVectorIds().toArray()) {
          final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
          if (loc != null && loc.rid.equals(rid) && !loc.deleted) {
            vectorIndex.markDeleted(vectorId);
            deletedIds.add(vectorId);
          }
        }

        // Persist deletion tombstones
        if (!deletedIds.isEmpty()) {
          persistDeletionTombstones(deletedIds);

          // Phase 5+: Periodic rebuild strategy (amortizes cost over many operations)
          if (graphState == GraphState.IMMUTABLE || graphState == GraphState.LOADING) {
            // Transition to MUTABLE state to track ongoing mutations
            this.graphState = GraphState.MUTABLE;
          }

          // Increment mutation counter (count number of deletions)
          mutationsSinceSerialize.addAndGet(deletedIds.size());
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  public void onAfterCommit() {
    // DISABLED: Compaction for vector indexes is currently disabled
    // Vector indexes don't benefit much from compaction since vectors are rarely updated
    // Re-enable once compaction properly handles uninitialized pages

    // Check if compaction should be triggered after commit
    // Operations are applied immediately during TransactionIndexContext replay (not buffered here)
    // if (minPagesToScheduleACompaction > 1 && currentMutablePages.get() >= minPagesToScheduleACompaction) {
    //   LogManager.instance()
    //       .log(this, Level.FINE, "Scheduled compaction of vector index '%s' (currentMutablePages=%d totalPages=%d)",
    //           null, getComponentName(), currentMutablePages.get(), getTotalPages());
    //   ((com.arcadedb.database.async.DatabaseAsyncExecutorImpl) getDatabase().async()).compact(this);
    // }
  }

  @Override
  public long countEntries() {
    checkIsValid();
    // Count entries directly from pages instead of from in-memory cache
    // This ensures accurate counts even when using limited location cache size
    // Apply LSM merge-on-read semantics: latest entry for each RID, filter out deleted entries
    final Map<RID, Integer> ridToLatestVectorId = new HashMap<>();
    final DatabaseInternal database = getDatabase();

    // Read from compacted sub-index if it exists
    if (compactedSubIndex != null) {
      LSMVectorIndexPageParser.parsePages(database, compactedSubIndex.getFileId(),
          compactedSubIndex.getTotalPages(), getPageSize(), true, entry -> {
        if (!entry.deleted) {
          // Keep latest (highest ID) vector for each RID
          final Integer existing = ridToLatestVectorId.get(entry.rid);
          if (existing == null || entry.vectorId > existing) {
            ridToLatestVectorId.put(entry.rid, entry.vectorId);
          }
        } else {
          // Deleted entry - remove from map
          ridToLatestVectorId.remove(entry.rid);
        }
      });
    }

    // Read from mutable index (overrides compacted entries)
    LSMVectorIndexPageParser.parsePages(database, getFileId(), getTotalPages(),
        getPageSize(), false, entry -> {
      if (!entry.deleted) {
        // Keep latest (highest ID) vector for each RID
        final Integer existing = ridToLatestVectorId.get(entry.rid);
        if (existing == null || entry.vectorId > existing) {
          ridToLatestVectorId.put(entry.rid, entry.vectorId);
        }
      } else {
        // Deleted entry - remove from map
        ridToLatestVectorId.remove(entry.rid);
      }
    });

    return ridToLatestVectorId.size();
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
    return indexName;
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

    LogManager.instance().log(this, Level.INFO, "compact() called for index: %s", null, getName());
    checkIsValid();
    final DatabaseInternal database = getDatabase();

    if (database.getMode() == ComponentFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update the index '" + getName() + "'");

    if (database.getPageManager().isPageFlushingSuspended(database)) {
      LogManager.instance().log(this, Level.INFO, "compact() returning false: page flushing suspended");
      // POSTPONE COMPACTING (DATABASE BACKUP IN PROGRESS?)
      return false;
    }

    LogManager.instance().log(this, Level.INFO,
        "compact() current status: %s, attempting compareAndSet from COMPACTION_SCHEDULED to COMPACTION_IN_PROGRESS",
        status.get());
    if (!status.compareAndSet(INDEX_STATUS.COMPACTION_SCHEDULED, INDEX_STATUS.COMPACTION_IN_PROGRESS)) {
      LogManager.instance()
          .log(this, Level.INFO, "compact() returning false: status compareAndSet failed (current status: %s)",
              status.get());
      // COMPACTION NOT SCHEDULED
      return false;
    }

    try {
      LogManager.instance().log(this, Level.INFO, "compact() calling LSMVectorIndexCompactor.compact()");
      final boolean success = LSMVectorIndexCompactor.compact(this);
      if (success) {
        // Track successful compaction
        metrics.incrementCompactionCount();
      }
      return success;
    } catch (final TimeoutException e) {
      LogManager.instance().log(this, Level.INFO, "compact() caught TimeoutException: %s", e.getMessage());
      // IGNORE IT, WILL RETRY LATER
      return false;
    } finally {
      status.set(INDEX_STATUS.AVAILABLE);
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

    // Add vector-specific metadata
    json.put("indexName", indexName);
    json.put("typeName", metadata.typeName);
    json.put("properties", metadata.propertyNames);
    json.put("dimensions", metadata.dimensions);
    json.put("similarityFunction", metadata.similarityFunction.name());

    if (metadata.quantizationType != VectorQuantizationType.NONE)
      json.put("quantization", metadata.quantizationType.name());
    json.put("maxConnections", metadata.maxConnections);
    json.put("beamWidth", metadata.beamWidth);
    json.put("idPropertyName", metadata.idPropertyName);
    json.put("storeVectorsInGraph", metadata.storeVectorsInGraph);
    json.put("addHierarchy", metadata.addHierarchy);
    json.put("buildState", metadata.buildState);
    json.put("version", CURRENT_VERSION);

    // Product Quantization (PQ) configuration
    if (metadata.quantizationType == VectorQuantizationType.PRODUCT) {
      json.put("pqSubspaces", metadata.pqSubspaces);
      json.put("pqClusters", metadata.pqClusters);
      json.put("pqCenterGlobally", metadata.pqCenterGlobally);
      json.put("pqTrainingLimit", metadata.pqTrainingLimit);
    }

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
      if (vectorIndex.size() > 0 && (graphState == GraphState.LOADING || graphState == GraphState.MUTABLE)) {
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
    flush();
  }

  @Override
  public void drop() {
    lock.writeLock().lock();
    try {
      // Clear all vector locations
      vectorIndex.clear();
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

    // Existing metrics
    stats.put("totalVectors", (long) vectorIndex.size());
    stats.put("activeVectors", vectorIndex.getActiveCount());
    stats.put("deletedVectors", (long) vectorIndex.size() - vectorIndex.getActiveCount());
    stats.put("dimensions", (long) metadata.dimensions);
    stats.put("maxConnections", (long) metadata.maxConnections);
    stats.put("beamWidth", (long) metadata.beamWidth);

    // NEW: Graph state metrics
    stats.put("graphState", (long) graphState.ordinal()); // LOADING=0, IMMUTABLE=1, MUTABLE=2
    stats.put("graphNodeCount", graphIndex != null ? (long) graphIndex.getIdUpperBound() : 0L);
    stats.put("mutationsSinceRebuild", (long) mutationsSinceSerialize.get());

    // Calculate mutations threshold (use configured value or default)
    final int defaultMutationsThreshold = getDatabase().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD);
    stats.put("mutationsThreshold", metadata.mutationsBeforeRebuild > 0 ?
        (long) metadata.mutationsBeforeRebuild : (long) defaultMutationsThreshold);

    // Populate metrics from LSMVectorIndexMetrics
    metrics.populateStats(stats);

    // NEW: Memory estimates
    stats.put("estimatedLocationIndexBytes", (long) vectorIndex.size() * 24L);
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

    // DEBUG: Log metadata being set
    LogManager.instance().log(this, Level.SEVERE,
        "DEBUG: setMetadata called for index %s, quantizationType=%s", indexName, this.metadata.quantizationType);
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
      buildGraphFromScratchWithRetry(graphCallback);

      // Persist graph with chunking callback
      final ChunkCommitCallback chunkCallback = (bytesWritten) -> {
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
      final RandomAccessVectorValues vectors = new ArcadePageVectorValues(getDatabase(), metadata.dimensions,
          vectorProp,
          vectorIndex, ordinalToVectorId, this);

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
    return new Type[] { Type.ARRAY_OF_FLOATS };
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
   * Gets the compacted sub-index, if any.
   */
  public LSMVectorIndexCompacted getSubIndex() {
    return compactedSubIndex;
  }

  /**
   * Sets the compacted sub-index.
   */
  public void setSubIndex(final LSMVectorIndexCompacted subIndex) {
    this.compactedSubIndex = subIndex;
  }

  /**
   * Gets the current number of mutable pages.
   */
  public int getCurrentMutablePages() {
    return currentMutablePages.get();
  }

  /**
   * Atomically replaces this index with a new one that has the compacted sub-index.
   * Copies remaining mutable pages from startingFromPage onwards to the new index.
   *
   * @param startingFromPage The first page to copy from current index
   * @param compactedIndex   The compacted sub-index to attach
   *
   * @return The new index file ID
   */
  protected LSMVectorIndexMutable splitIndex(final int startingFromPage, final LSMVectorIndexCompacted compactedIndex)
      throws IOException, InterruptedException {

    final DatabaseInternal database = getDatabase();
    if (database.isTransactionActive())
      throw new IllegalStateException("Cannot replace compacted index because a transaction is active");

    final int fileId = getFileId();
    final LockManager.LOCK_STATUS locked = getDatabase().getTransactionManager().tryLockFile(fileId, 0,
        Thread.currentThread());

    if (locked == LockManager.LOCK_STATUS.NO)
      throw new IllegalStateException("Cannot replace compacted index because cannot lock index file " + fileId);

    final AtomicInteger lockedNewFileId = new AtomicInteger(-1);

    try {
      lock.writeLock().lock();
      try {
        // Create new index file with compacted sub-index
        final int last_ = getComponentName().lastIndexOf('_');
        final String newName = getComponentName().substring(0, last_) + "_" + System.nanoTime();

        final LSMVectorIndexMutable newMutableIndex = new LSMVectorIndexMutable(database, newName,
            database.getDatabasePath() + File.separator + newName, mutable.getDatabase().getMode(),
            mutable.getPageSize(),
            PaginatedComponent.TEMP_EXT + LSMVectorIndexMutable.FILE_EXT);

        database.getSchema().getEmbedded().registerFile(newMutableIndex);

        // LOCK NEW FILE
        database.getTransactionManager().tryLockFile(newMutableIndex.getFileId(), 0, Thread.currentThread());
        lockedNewFileId.set(newMutableIndex.getFileId());

        final List<MutablePage> modifiedPages = new ArrayList<>();

        // Copy remaining mutable pages from old index to new index
        final int pagesToCopy = getTotalPages() - startingFromPage;
        for (int i = 0; i < pagesToCopy; i++) {
          final BasePage currentPage = getDatabase().getTransaction()
              .getPage(new PageId(getDatabase(), fileId, i + startingFromPage), getPageSize());

          // Copy the entire page content
          final MutablePage newPage = new MutablePage(new PageId(getDatabase(), newMutableIndex.getFileId(), i + 1),
              getPageSize());

          final ByteBuffer oldContent = currentPage.getContent();
          oldContent.rewind();
          newPage.getContent().put(oldContent);

          modifiedPages.add(getDatabase().getPageManager().updatePageVersion(newPage, true));
        }

        // Write all pages
        if (!modifiedPages.isEmpty())
          getDatabase().getPageManager().writePages(modifiedPages, false);

        // SWAP OLD WITH NEW INDEX IN EXCLUSIVE LOCK (NO READ/WRITE ARE POSSIBLE IN THE MEANTIME)
        newMutableIndex.removeTempSuffix();

        mutable = newMutableIndex;

        // Set the compacted sub-index on the main index
        this.compactedSubIndex = compactedIndex;

        // Update schema with file migration
        ((LocalSchema) getDatabase().getSchema()).setMigratedFileId(fileId, newMutableIndex.getFileId());

        getDatabase().getSchema().getEmbedded().saveConfiguration();
        return newMutableIndex;

      } finally {
        lock.writeLock().unlock();
      }

    } finally {
      final int lockedFile = lockedNewFileId.get();
      if (lockedFile != -1)
        getDatabase().getTransactionManager().unlockFile(lockedFile, Thread.currentThread());

      if (locked == LockManager.LOCK_STATUS.YES)
        getDatabase().getTransactionManager().unlockFile(fileId, Thread.currentThread());
    }
  }

  /**
   * Get the VectorLocationIndex (used by compactor to reload after compaction)
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
      final boolean isCompacted = (compactedSubIndex != null && fileId == compactedSubIndex.getFileId());

      // Read page header
      final int offsetFreeContent = page.readInt(OFFSET_FREE_CONTENT);
      final int numberOfEntries = page.readInt(OFFSET_NUM_ENTRIES);

      LogManager.instance().log(this, Level.FINE,
          "applyReplicatedPageUpdate: index=%s, pageNum=%d, fileId=%d, entries=%d, freeContent=%d, vectorIndexSizeBefore=%d",
          indexName, pageNum, fileId, numberOfEntries, offsetFreeContent, vectorIndex.size());

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

        final RID rid = new RID(getDatabase(), bucketId, position);

        // Read deleted flag (fixed 1 byte)
        final boolean deleted = page.readByte(currentOffset) == 1;
        currentOffset += 1;

        // CRITICAL FIX: Read and skip quantization type byte (ALWAYS present in page format)
        final byte quantOrdinal = page.readByte(currentOffset);
        currentOffset += 1;
        final VectorQuantizationType quantType = VectorQuantizationType.values()[quantOrdinal];

        // Skip quantized vector data based on quantization type
        if (quantType == VectorQuantizationType.INT8) {
          // INT8: vector length (4 bytes) + quantized bytes + min (4 bytes) + max (4 bytes)
          final int vectorLength = page.readInt(currentOffset);
          currentOffset += 4;
          currentOffset += vectorLength; // Skip quantized bytes
          currentOffset += 8; // Skip min + max (2 floats)
        } else if (quantType == VectorQuantizationType.BINARY) {
          // BINARY: original length (4 bytes) + packed bytes + median (4 bytes)
          final int originalLength = page.readInt(currentOffset);
          currentOffset += 4;
          // Packed bytes = ceil(originalLength / 8)
          final int packedLength = (originalLength + 7) / 8;
          currentOffset += packedLength;
          currentOffset += 4; // Skip median (float)
        }
        // NONE and PRODUCT: no additional data to skip

        // Update VectorLocationIndex with this entry's absolute file offset
        // LSM semantics: later entries override earlier ones
        vectorIndex.addOrUpdate(id, isCompacted, entryFileOffset, rid, deleted);
      }

      LogManager.instance()
          .log(this, Level.FINE, "Applied replicated page update: pageNum=%d, fileId=%d, isCompacted=%b, entries=%d",
              pageNum,
              fileId, isCompacted, numberOfEntries);

    } catch (final Exception e) {
      // Log but don't fail replication - VectorLocationIndex will be rebuilt if needed
      LogManager.instance()
          .log(this, Level.WARNING, "Error applying replicated page update for index %s: %s", indexName,
              e.getMessage());
    }
  }

  /**
   * Reload vectors from pages after compaction.
   * Called by compactor after splitIndex to refresh VectorLocationIndex with new file structure.
   */
  protected void loadVectorsFromPagesAfterCompaction() {
    loadVectorsFromPages();
  }

  /**
   * Rebuild graph index after compaction.
   * Called by compactor after reloading VectorLocationIndex.
   */
  protected void rebuildGraphAfterCompaction() {
    initializeGraphIndex();
  }

  /**
   * Get the location cache size from configuration (per-index metadata or global default).
   *
   * @return Maximum number of vector locations to cache, or -1 for unlimited
   */
  private int getLocationCacheSize() {
    if (metadata != null && metadata.locationCacheSize > -1) {
      return metadata.locationCacheSize;
    }
    return mutable.getDatabase().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_LOCATION_CACHE_SIZE);
  }

  /**
   * Get the location cache size from configuration during initialization.
   * Used when mutable is not yet initialized.
   *
   * @param database The database instance
   *
   * @return Maximum number of vector locations to cache, or -1 for unlimited
   */
  private int getLocationCacheSize(final DatabaseInternal database) {
    if (metadata != null && metadata.locationCacheSize > -1) {
      return metadata.locationCacheSize;
    }
    return database.getConfiguration().getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_LOCATION_CACHE_SIZE);
  }

  /**
   * Get the graph build cache size from configuration (per-index metadata or global default).
   *
   * @return Maximum number of vectors to cache during graph building
   */
  private int getGraphBuildCacheSize() {
    if (metadata != null && metadata.graphBuildCacheSize > -1) {
      return metadata.graphBuildCacheSize;
    }
    return mutable.getDatabase().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE);
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
   * Get a vector location by ID, with fallback to page scanning if evicted from cache.
   * This method provides transparent cache miss handling for bounded location caches.
   *
   * @param vectorId The vector ID to look up
   *
   * @return The vector location, or null if not found
   */
  private VectorLocationIndex.VectorLocation getVectorLocation(final int vectorId) {
    // Try cache first (O(1) lookup)
    VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(vectorId);
    if (loc != null) {
      return loc;
    }

    // Cache miss - reconstruct by scanning pages (expensive but rare for LRU cache)
    loc = reconstructLocationFromPages(vectorId);
    if (loc != null) {
      // Add back to cache for future access
      vectorIndex.addOrUpdate(vectorId, loc.isCompacted, loc.absoluteFileOffset, loc.rid, loc.deleted);
    }
    return loc;
  }

  /**
   * Reconstruct a vector location from pages when evicted from cache.
   * Scans compacted index first (more likely for old vectors), then mutable index.
   *
   * @param vectorId The vector ID to find
   *
   * @return The reconstructed location, or null if not found
   */
  private VectorLocationIndex.VectorLocation reconstructLocationFromPages(final int vectorId) {
    // Scan compacted index first (more likely to contain old vectors)
    if (compactedSubIndex != null) {
      final VectorLocationIndex.VectorLocation loc = scanPagesForVectorId(compactedSubIndex.getFileId(),
          compactedSubIndex.getTotalPages(), vectorId, true);
      if (loc != null)
        return loc;
    }

    // Scan mutable index
    return scanPagesForVectorId(mutable.getFileId(), mutable.getTotalPages(), vectorId, false);
  }

  /**
   * Scan pages in a specific file to find a vector by ID.
   * Similar to loadVectorsFromFile() but stops at first match.
   *
   * @param fileId      The file ID to scan
   * @param totalPages  Number of pages in the file
   * @param vectorId    The vector ID to find
   * @param isCompacted True if scanning compacted file
   *
   * @return The vector location if found, null otherwise
   */
  private VectorLocationIndex.VectorLocation scanPagesForVectorId(final int fileId, final int totalPages,
      final int vectorId,
      final boolean isCompacted) {
    for (int pageNum = 0; pageNum < totalPages; pageNum++) {
      try {
        final BasePage currentPage = mutable.getDatabase().getPageManager()
            .getImmutablePage(new PageId(mutable.getDatabase(), fileId, pageNum), getPageSize(), false, false);

        if (currentPage == null)
          continue;

        final int offsetFreeContent = currentPage.readInt(OFFSET_FREE_CONTENT);
        final int numberOfEntries = currentPage.readInt(OFFSET_NUM_ENTRIES);

        if (numberOfEntries == 0)
          continue;

        // Calculate header size
        final int headerSize;
        if (isCompacted && pageNum == 0) {
          headerSize = HEADER_BASE_SIZE + (4 * 4); // base + 4 ints for metadata
        } else {
          headerSize = HEADER_BASE_SIZE;
        }

        // Calculate absolute file offset for this page's data
        final long pageBaseOffset = ((long) fileId << 32) | (pageNum * getPageSize());

        // Parse entries in this page
        int entryOffset = headerSize;
        for (int i = 0; i < numberOfEntries && entryOffset < offsetFreeContent; i++) {
          final long entryFileOffset = pageBaseOffset + entryOffset;
          final int id = currentPage.readInt(entryOffset);
          entryOffset += 4;

          // Check if this is the vector we're looking for
          if (id == vectorId) {
            // Found it! Read the rest of the entry
            final int bucketId = currentPage.readInt(entryOffset);
            entryOffset += 4;
            final long position = currentPage.readLong(entryOffset);
            entryOffset += 8;
            final RID rid = new RID(mutable.getDatabase(), bucketId, position);

            final byte flags = currentPage.readByte(entryOffset);
            entryOffset += 1;
            final boolean deleted = (flags & 0x01) != 0;

            // Skip vector data (if present)
            // Entry format: id(4) + bucketId(4) + position(8) + flags(1) + [quantized data]
            // We don't need to read the vector data, just return the location

            return new VectorLocationIndex.VectorLocation(isCompacted, entryFileOffset, rid, deleted);
          }

          // Skip to next entry: bucketId(4) + position(8) + flags(1) + vector data
          entryOffset += 4 + 8 + 1;

          // Skip quantized vector data if present
          if (metadata.quantizationType != VectorQuantizationType.NONE) {
            final int quantizedSize = calculateQuantizedSize(metadata.dimensions, metadata.quantizationType);
            entryOffset += quantizedSize;
          }
        }
      } catch (final Exception e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Error scanning page %d in file %d for vectorId %d: %s", pageNum, fileId,
                vectorId,
                e.getMessage());
      }
    }

    return null; // Not found in this file
  }

  /**
   * Calculate the size of quantized vector data in bytes.
   *
   * @param dimensions       Number of vector dimensions
   * @param quantizationType Type of quantization
   *
   * @return Size in bytes
   */
  private int calculateQuantizedSize(final int dimensions, final VectorQuantizationType quantizationType) {
    switch (quantizationType) {
    case INT8:
      return dimensions; // 1 byte per dimension
    case BINARY:
      return (dimensions + 7) / 8; // 1 bit per dimension, rounded up to bytes
    case NONE:
    default:
      return 0;
    }
  }

  private void checkIsValid() {
    if (!valid)
      throw new IndexException("Index '" + indexName + "' is not valid. Probably has been drop or rebuilt");
  }
}
