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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexException;

import java.io.IOException;

/**
 * Page-backed container for a single sealed sparse vector segment. One {@link PaginatedComponent}
 * per segment file, mirroring how {@link com.arcadedb.index.vector.LSMVectorIndexMutable} and
 * {@link com.arcadedb.index.vector.LSMVectorIndexCompacted} are independent components.
 * <p>
 * Living inside the {@code PaginatedComponent} infrastructure gives the segment ArcadeDB's page
 * cache (hot blocks stay in memory across queries), the page-level WAL (segment writes are durable
 * the moment the enclosing transaction commits - no separate fsync, no flush-on-commit hook), HA
 * replication via the standard component-shipping pipeline, and inclusion in backups, with no
 * sparse-vector-specific recovery code.
 * <p>
 * <b>On-page layout</b> (defined by {@link SparseSegmentBuilder} / {@link PaginatedSegmentReader}):
 * <ul>
 *   <li>Page 0: segment header (magic, version, params, segment_id, total_postings,
 *       total_dims, manifest_page_num, dim_index_page_num, crc).</li>
 *   <li>Pages 1..N: posting blocks packed contiguously. Multiple blocks per page; never spanning a
 *       page boundary so any block-skip is a single {@code readPage} round-trip.</li>
 *   <li>Following pages: per-dim trailers (dim header + block locators + skip list), packed.</li>
 *   <li>Then: dim_index page (sorted (dim_id, trailer_locator) entries).</li>
 *   <li>Last page: manifest (segment_id, parent segments, tombstone_floor, dim_index_locator, crc).</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SparseSegmentComponent extends PaginatedComponent {

  public static final String FILE_EXT          = "sparseseg";
  public static final int    CURRENT_VERSION   = 1;
  /** 64 KiB page - matches ArcadeDB's typical page-cache slot size and leaves &gt;= 65520 B for payload. */
  public static final int    DEFAULT_PAGE_SIZE = 65536;

  /** Constructor used at segment-creation time (a fresh component file). */
  protected SparseSegmentComponent(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize) throws IOException {
    super(database, name, filePath, FILE_EXT, mode, pageSize, CURRENT_VERSION);
  }

  /** Constructor used at segment-load time (open an existing file with a known file id). */
  protected SparseSegmentComponent(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
  }

  /**
   * {@link com.arcadedb.engine.ComponentFactory} handler so {@code .sparseseg} files registered with
   * the database are reopened as {@code SparseSegmentComponent} instances at startup.
   */
  public static class PaginatedComponentFactoryHandler
      implements com.arcadedb.engine.ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new SparseSegmentComponent(database, name, filePath, id, mode, pageSize, version);
    }
  }

  /** Read-only fetch of {@code pageNum}, served from ArcadeDB's page cache when hot. */
  public BasePage readPage(final int pageNum) {
    try {
      return database.getTransaction().getPage(new PageId(database, getFileId(), pageNum), getPageSize());
    } catch (final IOException e) {
      throw new IndexException("Sparse segment '" + getName() + "': error reading page " + pageNum, e);
    }
  }

  /**
   * Allocate a brand-new page at {@code pageNum}, registering it with the active transaction so
   * its content flows through the WAL on commit.
   */
  public MutablePage allocatePage(final int pageNum) {
    final PageId pageId = new PageId(database, getFileId(), pageNum);
    return database.isTransactionActive() ?
        database.getTransaction().addPage(pageId, getPageSize()) :
        new MutablePage(pageId, getPageSize());
  }

  /** Mutate an existing page; transaction integration captures the page delta for the WAL. */
  public MutablePage modifyPage(final int pageNum) {
    try {
      return database.getTransaction().getPageToModify(new PageId(database, getFileId(), pageNum), getPageSize(), false);
    } catch (final IOException e) {
      throw new IndexException("Sparse segment '" + getName() + "': error modifying page " + pageNum, e);
    }
  }

  /**
   * The bytes available for our payload on each page (page size minus ArcadeDB's
   * {@link BasePage#PAGE_HEADER_SIZE}).
   */
  public int pageContentSize() {
    return getPageSize() - BasePage.PAGE_HEADER_SIZE;
  }

  @Override
  public Object getMainComponent() {
    return this;
  }
}
