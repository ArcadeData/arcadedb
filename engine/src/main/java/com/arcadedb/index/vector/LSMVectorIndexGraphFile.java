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
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
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
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.vector.JVectorUtils;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.logging.*;

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
   * Constructor for creating a new graph file
   */
  protected LSMVectorIndexGraphFile(final DatabaseInternal database, final String name, final String filePath,
                                    final ComponentFile.MODE mode, final int pageSize) throws IOException {
    super(database, name, filePath, FILE_EXT, mode, pageSize, CURRENT_VERSION);
  }

  /**
   * Constructor for loading an existing graph file
   */
  protected LSMVectorIndexGraphFile(final DatabaseInternal database, final String name, final String filePath, final int id,
                                    final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
  }

  @Override
  public Object getMainComponent() {
    return mainIndex;
  }

  public void setMainIndex(final LSMVectorIndex mainIndex) {
    this.mainIndex = mainIndex;
  }

  private long computeTotalGraphBytes() throws IOException {
    final int totalPages = getTotalPages();
    if (totalPages == 0)
      return 0;

    final int usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;

    // Load last page to get actual content size
    final int lastPageId = totalPages - 1;
    final var lastPage = database.getPageManager()
            .getImmutablePage(new PageId(database, fileId, lastPageId), pageSize, false, false);

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
  public void writeGraph(final ImmutableGraphIndex graph, final RandomAccessVectorValues vectors) {
    writeGraph(graph, vectors, 0, null);
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
  public void writeGraph(final ImmutableGraphIndex graph, final RandomAccessVectorValues vectors,
                         final long chunkSizeMB, final ChunkCommitCallback chunkCallback) {

    if (!database.isTransactionActive())
      throw new IllegalStateException("writeGraph() must be called within an active transaction");

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

      // Build graph with InlineVectors - dimension=0 when not storing vectors
      try (final OnDiskSequentialGraphIndexWriter indexWriter = new OnDiskSequentialGraphIndexWriter.Builder(graph, writer).with(
              new InlineVectors(storedDimension)).build()) {
        // Write graph with vectors (actual vectors when storeVectors=true, empty 0-dimension when false)
        // Note: write() handles header/footer automatically in jvector 4.0.0+
        indexWriter.write(Map.of(FeatureId.INLINE_VECTORS,
                (IntFunction<Feature.State>) ordinal -> {
                  if (storeVectors) {
                    // Store actual vectors from documents/quantized pages
                    final VectorFloat<?> vector = vectors.getVector(ordinal);
                    return new InlineVectors.State(vector != null ? vector : emptyVector);
                  } else {
                    // Store 0-dimension empty vector (no space used)
                    return new InlineVectors.State(emptyVector);
                  }
                }));
      }

      writer.close();

      final long totalBytes = writer.position();

      if (storeVectors) {
        LogManager.instance().log(this, Level.INFO,
                "Graph written to pages (sequential): %d nodes, %d bytes, %d pages (WITH inline vectors, quantization=%s)",
                graph.getIdUpperBound(), totalBytes, getTotalPages(), mainIndex.metadata.quantizationType);
      } else {
        LogManager.instance().log(this, Level.INFO,
                "Graph written to pages (sequential): %d nodes, %d bytes, %d pages (topology only, vectors in documents)",
                graph.getIdUpperBound(), totalBytes, getTotalPages());
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error writing graph to pages: %s", e.getMessage());
      e.printStackTrace();
      throw new IndexException("Error writing graph to pages", e);
    }
  }

  /**
   * Load a graph from pages as OnDiskGraphIndex for lazy-loading.
   */
  public OnDiskGraphIndex loadGraph() throws IOException {
    final int totalPages = getTotalPages();
    final long totalBytes = computeTotalGraphBytes();
    if (totalPages == 0 || totalBytes == 0)
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
}
