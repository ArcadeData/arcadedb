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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.disk.IndexWriter;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskSequentialGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.JVectorUtils;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.logging.Level;

/**
 * PaginatedComponent for storing JVector graph topology in ArcadeDB pages.
 * This allows OnDiskGraphIndex to lazy-load graph data from disk instead of keeping it all in RAM.
 * <p>
 * Graph data is written directly starting from page 0 (no metadata page needed).
 * JVector's format is self-describing and contains all necessary metadata internally.
 * Total size is computed on-demand from the file's page count.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexGraphFile extends PaginatedComponent {
  public static final String FILE_EXT        = "vecgraph";
  public static final int    CURRENT_VERSION = 0;

  // Graph data starts at page 0 (no metadata page needed)
  // totalGraphBytes is computed from file size - JVector format is self-describing

  private LSMVectorIndex mainIndex;

  /**
   * Bytes the last {@link #writeGraph} on this object wrote, in the gap-free logical address space
   * {@code ContiguousPageWriter} provides - i.e. the exact length {@code OnDiskGraphIndex.load()} has to measure
   * its footer back from (issue #7362). Handed to the manifest once the caller has committed, since the manifest
   * is what carries the fact into the next session; kept here because the two happen at different moments and on
   * different objects.
   */
  private volatile long lastWrittenGraphBytes = -1L;

  /**
   * Says which records the graph on these pages was built over. The graph itself is addressed by ordinal and carries
   * nothing that identifies them, so this is what makes reusing it safe (issue #6106).
   */
  private final LSMVectorIndexGraphManifest manifest;

  /**
   * Constructor for creating a new graph file
   */
  protected LSMVectorIndexGraphFile(final DatabaseInternal database, final String name, final String filePath,
                                    final ComponentFile.MODE mode, final int pageSize) throws IOException {
    super(database, name, filePath, FILE_EXT, mode, pageSize, CURRENT_VERSION);
    this.manifest = new LSMVectorIndexGraphManifest(getOSFile().getAbsolutePath());
  }

  /**
   * Constructor for loading an existing graph file
   */
  protected LSMVectorIndexGraphFile(final DatabaseInternal database, final String name, final String filePath, final int id,
                                    final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    this.manifest = new LSMVectorIndexGraphManifest(getOSFile().getAbsolutePath());
  }

  /**
   * @return the sidecar recording which records the persisted graph was built over
   */
  public LSMVectorIndexGraphManifest getManifest() {
    return manifest;
  }

  @Override
  public Object getMainComponent() {
    return mainIndex;
  }

  public void setMainIndex(final LSMVectorIndex mainIndex) {
    this.mainIndex = mainIndex;
  }

  /**
   * Length of the JVector payload on these pages.
   * <p>
   * Taken from the manifest whenever it records one, because that is the number the write actually ended on
   * (issue #7362). The page-count derivation below it stays as the answer for a graph persisted before that was
   * recorded, and it is only ever an approximation of the truth: the component's page count is raised
   * asynchronously by the flush thread while a multi-GB persist is still running, so a graph reloaded in the same
   * breath as it was written measured itself gigabytes short and JVector read its footer magic off the middle of
   * the payload; and the count can only grow, so a generation smaller than the one it replaced is measured with
   * its predecessor's size.
   */
  private long computeTotalGraphBytes() throws IOException {
    final int usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;

    final long recorded = recordedGraphBytes();
    final int totalPages;
    if (recorded > 0)
      // Ceiling division: the last byte of the payload sits on page (recorded - 1) / usablePageSize.
      totalPages = (int) ((recorded + usablePageSize - 1) / usablePageSize);
    else {
      totalPages = getTotalPages();
      if (totalPages == 0)
        return 0;
    }

    // Load the last page. Whichever number named it, the pages have to be there: the count reported by the
    // component can be ahead of what is actually on disk when the graph file has been truncated or the page was
    // never flushed/evicted, and a recorded length can outlive the pages it described just as easily (a restore
    // that brought the manifest and a truncated file, a file damaged after the persist). In that case the page
    // manager either returns null or raises IllegalArgumentException ("page does not exist"). Treat the persisted
    // graph as absent (return 0) so loadGraph() falls through to the "rebuild graph from scratch" recovery path
    // instead of aborting startup.
    final int lastPageId = totalPages - 1;
    final BasePage lastPage;
    try {
      lastPage = database.getPageManager()
              .getImmutablePage(new PageId(database, fileId, lastPageId), pageSize, false, false);
    } catch (final IllegalArgumentException e) {
      LogManager.instance().log(this, Level.WARNING,
              "Graph file '%s' reports %d page(s) but last page %d cannot be read (%s): treating persisted graph as absent (will rebuild)",
              getName(), totalPages, lastPageId, e.getMessage());
      return 0;
    }

    if (lastPage == null) {
      LogManager.instance().log(this, Level.WARNING,
              "Graph file '%s' reports %d page(s) but last page %d is missing on disk: treating persisted graph as absent (will rebuild)",
              getName(), totalPages, lastPageId);
      return 0;
    }

    if (recorded > 0)
      return recorded;

    // Compute contiguous logical size (excluding headers from logical address space)
    // Each full page contributes usablePageSize bytes, last page contributes its actual content
    return (long) usablePageSize * (totalPages - 1L) + lastPage.getContentSize();
  }

  /**
   * Write a graph to pages using JVector's serialization format (without chunking).
   * This persists an in-memory graph to disk for later loading as OnDiskGraphIndex.
   * <p>
   * IMPORTANT:
   * - This writes ONLY the graph topology (no vectors)
   * - Vectors are read on-demand from ArcadeDB documents via ArcadePageVectorValues
   * - MUST be called within an active transaction
   * - Caller is responsible for committing the transaction
   * - Graph data starts at page 0 (no metadata page needed - JVector format is self-describing)
   */
  public long writeGraph(final ImmutableGraphIndex graph, final RandomAccessVectorValues vectors) {
    return writeGraph(graph, vectors, 0, null, null, null);
  }

  /**
   * Write a graph to pages with chunking support for large bulk writes.
   * <p>
   * Chunking allows periodic commits during large writes to avoid exceeding
   * transaction memory/size limits when WAL is disabled.
   * <p>
   * IMPORTANT:
   * - MUST be called within an active transaction
   * - Caller is responsible for transaction lifecycle and commits
   * - chunkCallback handles commit/begin new transaction
   * - Graph data starts at page 0 (no metadata page needed - JVector format is self-describing)
   *
   * @param graph          The graph index to persist
   * @param vectors        Vector values to write (may be empty if storeVectorsInGraph=false)
   * @param chunkSizeMB    Chunk size in MB (0 = no chunking)
   * @param chunkCallback  Callback to invoke when chunk is complete (can be null if chunkSizeMB=0)
   */
  public long writeGraph(final ImmutableGraphIndex graph, final RandomAccessVectorValues vectors,
                         final long chunkSizeMB, final ChunkCommitCallback chunkCallback) {
    return writeGraph(graph, vectors, chunkSizeMB, chunkCallback, null, null);
  }

  /**
   * Write a graph to pages with optional FusedPQ for cache-friendly search traversal.
   * When PQ data is provided, PQ codes are stored inline with graph nodes so that during
   * search traversal, approximate distances are computed without separate I/O.
   */
  public long writeGraph(final ImmutableGraphIndex graph, final RandomAccessVectorValues vectors,
                         final long chunkSizeMB, final ChunkCommitCallback chunkCallback,
                         final ProductQuantization pq, final PQVectors pqVectors) {

    if (!database.isTransactionActive())
      throw new IllegalStateException("writeGraph() must be called within an active transaction");

    // The pages about to be overwritten are the ones the current manifest vouches for, and the write commits in
    // chunks, so from here until the caller has committed there is no generation of the graph anything can promise.
    // Dropping the manifest FIRST is what keeps a half-written graph from being described by the manifest of the
    // one it is replacing (issue #6106). A process killed anywhere in here therefore leaves no manifest at all,
    // which the load path reads as "cannot be verified" and judges by node count - so any failure this method can
    // still observe replaces it with a manifest that refuses the pages outright (see the catch below).
    manifest.invalidate();
    lastWrittenGraphBytes = -1L;

    try {
      if (chunkSizeMB > 0 && chunkCallback != null) {
        LogManager.instance().log(this, Level.INFO,
                "Starting graph write (sequential) with chunking: %d nodes, %dMB chunk size",
                graph.getIdUpperBound(), chunkSizeMB);
      } else {
        LogManager.instance().log(this, Level.INFO,
                "Starting graph write (sequential): %d nodes", graph.getIdUpperBound());
      }

      // Create contiguous writer that provides gap-free logical address space over physical pages
      // This is critical: JVector assumes contiguous file layout with no gaps
      final IndexWriter writer = new ContiguousPageWriter(database, getFileId(), getPageSize(),
              chunkSizeMB, chunkCallback);

      // Phase 2: Optionally store vectors inline in graph file
      // FIX for GitHub issue #3142: Use dimension=0 for InlineVectors when storeVectorsInGraph=false
      // This stores only graph topology without vectors, dramatically reducing file size
      final boolean storeVectors = mainIndex != null && mainIndex.metadata.storeVectorsInGraph;
      final int dimension = vectors.dimension();

      // JVector requires InlineVectors feature, but we can use dimension=0 to store no vector data
      final int storedDimension = storeVectors ? dimension : 0;
      final VectorFloat<?> emptyVector = JVectorUtils.createVectorFloat(storedDimension);

      if (storeVectors) {
        LogManager.instance().log(this, Level.INFO,
                "Writing graph WITH inline vectors (storeVectorsInGraph=true, quantization=%s)",
                mainIndex.metadata.quantizationType);
      } else {
        LogManager.instance().log(this, Level.INFO,
                "Writing graph WITHOUT inline vectors - topology only (vectors fetched from documents on-demand)");
      }

      // Add FusedPQ feature when PQ data is available — stores PQ codes inline with graph nodes
      // for cache-friendly approximate scoring during search traversal.
      // FusedPQ requires a 256-cluster PQ; with a custom pqClusters configuration (issue #3160)
      // we skip the FusedPQ optimization and persist topology + inline vectors only, otherwise
      // the JVector constructor throws and prevents the graph from being persisted.
      final boolean hasFusedPQ = pq != null && pqVectors != null && pq.getClusterCount() == 256;
      if (hasFusedPQ) {
        LogManager.instance().log(this, Level.INFO,
            "Writing graph WITH FusedPQ: PQ codes stored inline with graph nodes for cache-friendly search");
      } else if (pq != null && pqVectors != null) {
        LogManager.instance().log(this, Level.INFO,
            "Skipping FusedPQ inline storage: PQ has %d clusters (FusedPQ requires 256); graph persistence will store topology + inline vectors only",
            pq.getClusterCount());
      }

      // Build writer with InlineVectors (always) and FusedPQ (when PQ available)
      try (final OnDiskSequentialGraphIndexWriter indexWriter = hasFusedPQ ?
          new OnDiskSequentialGraphIndexWriter.Builder(graph, writer)
              .with(new InlineVectors(storedDimension))
              .with(new FusedPQ(graph.maxDegree(), pq))
              .build() :
          new OnDiskSequentialGraphIndexWriter.Builder(graph, writer)
              .with(new InlineVectors(storedDimension))
              .build()) {
        // Build feature states map
        final Map<FeatureId, IntFunction<Feature.State>> featureStates = new HashMap<>();

        // InlineVectors feature
        featureStates.put(FeatureId.INLINE_VECTORS,
            (IntFunction<Feature.State>) ordinal -> {
              if (storeVectors) {
                final VectorFloat<?> vector = vectors.getVector(ordinal);
                return new InlineVectors.State(vector != null ? vector : emptyVector);
              } else {
                return new InlineVectors.State(emptyVector);
              }
            });

        // FusedPQ feature — writes PQ-encoded vectors inline with each graph node
        if (hasFusedPQ) {
          final ImmutableGraphIndex.View graphView = graph.getView();
          featureStates.put(FeatureId.FUSED_PQ,
              (IntFunction<Feature.State>) ordinal -> new FusedPQ.State(graphView, pqVectors, ordinal));
        }

        indexWriter.write(featureStates);
      }

      writer.close();

      final long totalBytes = writer.position();
      lastWrittenGraphBytes = totalBytes;

      if (storeVectors) {
        // The inline vectors are float32 whatever the index quantization is: JVector's InlineVectors feature
        // reserves dimension * Float.BYTES per node and ArcadePageVectorValues hands it dequantized floats, so
        // naming the quantization on this line read as a claim about the bytes it had just counted (issue #7362).
        // The quantization is still worth reporting - it is what the delta scan and the PQ file use - but as the
        // index setting it is, next to the size the inline vectors actually cost.
        LogManager.instance().log(this, Level.INFO,
                "Graph written to pages (sequential): %d nodes, %d bytes, %d pages (WITH inline vectors, float32 x %d dims; "
                    + "index quantization=%s applies to the index data, not to these inline vectors)",
                graph.getIdUpperBound(), totalBytes, getTotalPages(), storedDimension, mainIndex.metadata.quantizationType);
      } else {
        LogManager.instance().log(this, Level.INFO,
                "Graph written to pages (sequential): %d nodes, %d bytes, %d pages (topology only, vectors in documents)",
                graph.getIdUpperBound(), totalBytes, getTotalPages());
      }

      return totalBytes;

    } catch (final Exception e) {
      // Dropped with the manifest and for the same reason: past a failure the manifest is the sole authority on
      // these pages, and recordedGraphBytes() prefers this field over it. A failure landing after the position
      // was captured - the logging below it, say - would otherwise leave a length in here that outlives the
      // markUnusable() on the next line and lets a later loadGraph() in this session trust pages the manifest
      // has just refused (issue #7362).
      lastWrittenGraphBytes = -1L;
      // The caller rolls back and carries on without a persisted graph. Whatever the rollback leaves on these
      // pages - the previous generation untouched, or a partial rewrite whose earlier chunks already committed -
      // nothing here knows which, so the manifest must refuse them rather than be simply absent: absent means
      // "unverifiable", and unverifiable falls back to the node count this whole mechanism replaces (issue #6106).
      manifest.markUnusable("graph persist failed: " + e);
      LogManager.instance().log(this, Level.SEVERE, "Error writing graph to pages: %s", e, e.getMessage());
      throw new IndexException("Error writing graph to pages", e);
    }
  }

  /**
   * Load a graph from pages as OnDiskGraphIndex for lazy-loading.
   */
  public OnDiskGraphIndex loadGraph() throws IOException {
    // The length alone decides whether there is a graph here: computeTotalGraphBytes() already answers 0 for
    // "no pages" and for "the pages this length describes are not readable", and gating on the component's page
    // count on top of it would put the very counter issue #7362 is about back on the load path.
    final long totalBytes = computeTotalGraphBytes();
    if (totalBytes == 0)
      return null;

    try {
      // Create reader supplier for lazy-loading
      // Use 0L offset since graph data starts at position 0 (jvector 4.0.0-rc.7+ handles header/footer automatically)
      final ArcadePageReaderSupplier supplier = new ArcadePageReaderSupplier(database, getFileId(), getPageSize(), totalBytes, 0L);

      // Load graph using JVector's OnDiskGraphIndex
      final OnDiskGraphIndex graph = OnDiskGraphIndex.load(supplier);

      LogManager.instance()
              .log(this, Level.INFO, "Loaded graph from disk: %d nodes, %d bytes (%d pages)", graph.getIdUpperBound(),
                      totalBytes, getTotalPages());

      return graph;

    } catch (final Exception e) {
      throw new IndexException("Error loading graph from pages", e);
    }
  }

  /**
   * Check if a persisted graph exists
   */
  public boolean hasPersistedGraph() {
    return getTotalPages() > 0;
  }

  /**
   * @return bytes the last {@link #writeGraph} on this object wrote, or {@code -1} when none has run (or the last
   * one failed). Read by the persist once it has committed, to hand the length to the manifest.
   */
  public long getLastWrittenGraphBytes() {
    return lastWrittenGraphBytes;
  }

  /**
   * The graph length as recorded, preferring what this session's own write measured over what the manifest on disk
   * says: within a session the two agree, but the field is the one that is right during the window between the
   * write and the manifest being written (issue #7362).
   *
   * @return the recorded length, or {@code 0} when nothing recorded one
   */
  private long recordedGraphBytes() {
    final long written = lastWrittenGraphBytes;
    if (written > 0)
      return written;

    final LSMVectorIndexGraphManifest.Content content = manifest.read();
    return content != null ? content.graphBytes() : 0L;
  }
}
