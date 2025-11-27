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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexException;
import io.github.jbellis.jvector.disk.RandomAccessWriter;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;

import java.io.IOException;

/**
 * PaginatedComponent for storing JVector graph topology in ArcadeDB pages.
 * This allows OnDiskGraphIndex to lazy-load graph data from disk instead of keeping it all in RAM.
 *
 * Page 0 contains metadata about the graph:
 * - Total size in bytes
 * - Number of nodes
 * - Version info
 *
 * Pages 1+ contain the raw graph topology data as written by JVector's OnDiskGraphIndexWriter.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexGraphFile extends PaginatedComponent {
  public static final String FILE_EXT        = "vecgraph";
  public static final int    CURRENT_VERSION = 0;

  // Page 0 metadata layout
  private static final int OFFSET_TOTAL_BYTES = 0;  // 8 bytes: total graph data size
  private static final int OFFSET_NUM_NODES   = 8;  // 4 bytes: number of nodes
  private static final int OFFSET_ENTRY_NODE  = 12; // 4 bytes: entry point node ID
  private static final int METADATA_SIZE      = 16;

  private LSMVectorIndex mainIndex;
  private long           totalGraphBytes;

  /**
   * Constructor for creating a new graph file
   */
  protected LSMVectorIndexGraphFile(final DatabaseInternal database, final String name, final String filePath,
                                    final ComponentFile.MODE mode, final int pageSize) throws IOException {
    super(database, name, filePath, FILE_EXT, mode, pageSize, CURRENT_VERSION);
    this.totalGraphBytes = 0;
  }

  /**
   * Constructor for loading an existing graph file
   */
  protected LSMVectorIndexGraphFile(final DatabaseInternal database, final String name, final String filePath, final int id,
                                    final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    loadMetadata();
  }

  @Override
  public Object getMainComponent() {
    return mainIndex;
  }

  public void setMainIndex(final LSMVectorIndex mainIndex) {
    this.mainIndex = mainIndex;
  }

  public long getTotalBytes() {
    return totalGraphBytes;
  }

  /**
   * Write a graph to pages using JVector's serialization format.
   * This persists an in-memory graph to disk for later loading as OnDiskGraphIndex.
   */
  public void writeGraph(final io.github.jbellis.jvector.graph.ImmutableGraphIndex graph,
                          final RandomAccessVectorValues vectors) throws IOException {

    database.checkTransactionIsActive(false);

    try {
      // Create writer that writes to our pages
      final RandomAccessWriter writer = new ArcadePageGraphWriter(database, getFileId(), getPageSize());

      // Use JVector's OnDiskGraphIndexWriter with inline vectors
      // This stores vectors in the graph file for simplicity (Phase 3)
      // TODO: Optimize in future to use separated vectors stored only in our pages
      final io.github.jbellis.jvector.graph.disk.feature.InlineVectors inlineVectors =
          new io.github.jbellis.jvector.graph.disk.feature.InlineVectors(graph.getDimension());

      try (final OnDiskGraphIndexWriter indexWriter = new OnDiskGraphIndexWriter.Builder(graph, writer)
              .withStartOffset(0)
              .with(inlineVectors)
              .build()) {
        // Write header
        indexWriter.writeHeader(graph.getView());

        // Write graph topology with inline vectors
        indexWriter.writeInline(graph.size(), java.util.Map.of());

        // Get final size
        this.totalGraphBytes = writer.position();
      }

      writer.close();

      // Write metadata to page 0
      writeMetadata();

      com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.INFO,
          "Wrote graph to disk: %d nodes, %d bytes, %d pages",
          graph.getIdUpperBound(), totalGraphBytes, getTotalPages());

    } catch (final Exception e) {
      throw new IndexException("Error writing graph to pages", e);
    }
  }

  /**
   * Load a graph from pages as OnDiskGraphIndex for lazy-loading.
   */
  public OnDiskGraphIndex loadGraph() throws IOException {
    if (getTotalPages() == 0 || totalGraphBytes == 0)
      return null;

    try {
      // Create reader supplier for lazy-loading
      final ArcadePageReaderSupplier supplier =
          new ArcadePageReaderSupplier(database, getFileId(), getPageSize(), totalGraphBytes);

      // Load graph using JVector's OnDiskGraphIndex
      final OnDiskGraphIndex graph = OnDiskGraphIndex.load(supplier);

      com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.INFO,
          "Loaded graph from disk: %d nodes, %d bytes",
          graph.getIdUpperBound(), totalGraphBytes);

      return graph;

    } catch (final Exception e) {
      throw new IndexException("Error loading graph from pages", e);
    }
  }

  /**
   * Check if a persisted graph exists
   */
  public boolean hasPersistedGraph() {
    return getTotalPages() > 0 && totalGraphBytes > 0;
  }

  private void writeMetadata() throws IOException {
    final PageId pageId = new PageId(database, getFileId(), 0);
    final MutablePage page = database.getTransaction().addPage(pageId, getPageSize());

    page.writeLong(OFFSET_TOTAL_BYTES, totalGraphBytes);  // Total bytes
    page.writeInt(OFFSET_NUM_NODES, 0);                    // Placeholder for num nodes
    page.writeInt(OFFSET_ENTRY_NODE, 0);                   // Placeholder for entry node
  }

  private void loadMetadata() throws IOException {
    if (getTotalPages() == 0) {
      this.totalGraphBytes = 0;
      return;
    }

    final PageId pageId = new PageId(database, getFileId(), 0);
    final var page = database.getPageManager().getImmutablePage(pageId, getPageSize(), false, false);

    if (page != null) {
      this.totalGraphBytes = page.readLong(OFFSET_TOTAL_BYTES);
    } else {
      this.totalGraphBytes = 0;
    }
  }
}
