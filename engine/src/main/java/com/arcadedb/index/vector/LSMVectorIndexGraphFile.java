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
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.disk.RandomAccessWriter;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;

import java.io.IOException;
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
   * Factory handler for loading graph files from disk during schema initialization.
   */
  public static class PaginatedComponentFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public Component createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new LSMVectorIndexGraphFile(database, name, filePath, id, mode, pageSize, version);
    }
  }

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

    // Load last page to get actual content size from 4-byte offset
    final int lastPageId = totalPages - 1;

    final var lastPage = database.getPageManager()
        .getImmutablePage(new PageId(database, fileId, lastPageId), pageSize, false, false);
    return (long) pageSize * (totalPages - 1L) + lastPage.getContentSize();
  }

  /**
   * Write a graph to pages using JVector's serialization format.
   * This persists an in-memory graph to disk for later loading as OnDiskGraphIndex.
   * <p>
   * IMPORTANT:
   * - This writes ONLY the graph topology (no vectors)
   * - Vectors are read on-demand from ArcadeDB documents via ArcadePageVectorValues
   * - MUST be called within an active transaction
   * - Caller is responsible for committing the transaction
   * - Graph data starts at page 0 (no metadata page needed - JVector format is self-describing)
   */
  public void writeGraph(final io.github.jbellis.jvector.graph.ImmutableGraphIndex graph,
      final RandomAccessVectorValues vectors) {

    if (!database.isTransactionActive())
      throw new IllegalStateException("writeGraph() must be called within an active transaction");

    try {
      LogManager.instance().log(this, Level.INFO,
          "Starting graph write: %d nodes", graph.getIdUpperBound());

      // Create writer that writes to our pages (starting from page 0)
      final RandomAccessWriter writer = new ArcadePageGraphWriter(database, getFileId(), getPageSize());

      // Write graph topology WITHOUT inline vectors
      // Vectors will be read on-demand from ArcadeDB documents (no duplication)
      // NOTE: JVector 4.0+ requires a vector feature for dimension info, but we don't write vector data
      try (final OnDiskGraphIndexWriter indexWriter = new OnDiskGraphIndexWriter.Builder(graph, writer)
          .withStartOffset(0)
          .with(new InlineVectors(vectors.dimension()))
          .build()) {
        // Write header
        indexWriter.writeHeader(graph.getView());

        // Write graph topology only (no inline vectors or other features)
        indexWriter.write(java.util.Map.of());
      }

      writer.close();

      final long totalBytes = writer.position();

      LogManager.instance().log(this, Level.INFO,
          "Graph written to pages: %d nodes, %d bytes, %d pages (topology only, vectors in documents)",
          graph.getIdUpperBound(), totalBytes, getTotalPages());

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Error writing graph to pages: %s", e.getMessage());
      e.printStackTrace();
      throw new IndexException("Error writing graph to pages", e);
    }
  }

  /**
   * Load a graph from pages as OnDiskGraphIndex for lazy-loading.
   */
  public OnDiskGraphIndex loadGraph() throws IOException {
    final long totalBytes = computeTotalGraphBytes();
    if (getTotalPages() == 0 || totalBytes == 0)
      return null;

    try {
      // Create reader supplier for lazy-loading
      final ArcadePageReaderSupplier supplier =
          new ArcadePageReaderSupplier(database, getFileId(), getPageSize(), totalBytes);

      // Load graph using JVector's OnDiskGraphIndex
      final OnDiskGraphIndex graph = OnDiskGraphIndex.load(supplier);

      com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.INFO,
          "Loaded graph from disk: %d nodes, %d bytes (%d pages)",
          graph.getIdUpperBound(), totalBytes, getTotalPages());

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
