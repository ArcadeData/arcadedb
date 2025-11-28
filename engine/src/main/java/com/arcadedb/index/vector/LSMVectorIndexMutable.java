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
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexException;

import java.io.*;
import java.nio.*;

/**
 * PaginatedComponent for LSM-based vector index storage.
 * This class handles the low-level page management for vector data,
 * following the same pattern as LSMTreeIndexMutable for LSMTreeIndex.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexMutable extends PaginatedComponent {
  public static final String FILE_EXT        = "lsmvecidx";
  public static final int    CURRENT_VERSION = 0;
  public static final int    DEF_PAGE_SIZE   = 262_144;

  // Page header layout constants
  public static final int OFFSET_FREE_CONTENT = 0;  // 4 bytes
  public static final int OFFSET_NUM_ENTRIES  = 4;  // 4 bytes
  public static final int OFFSET_MUTABLE      = 8;  // 1 byte
  public static final int HEADER_BASE_SIZE    = 9;  // offsetFreeContent(4) + numberOfEntries(4) + mutable(1)

  private LSMVectorIndex mainIndex;

  /**
   * Constructor for creating a new component
   */
  protected LSMVectorIndexMutable(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize) throws IOException {
    super(database, name, filePath, FILE_EXT, mode, pageSize, CURRENT_VERSION);
  }

  /**
   * Constructor for splitting an existing component (during compaction)
   */
  protected LSMVectorIndexMutable(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final String ext) throws IOException {
    super(database, name, filePath, ext, mode, pageSize, CURRENT_VERSION);
    // Don't check for active transaction here - file creation doesn't require it
    // Transaction is required when writing pages, which happens in the compactor's transaction block
  }

  /**
   * Constructor for loading an existing component
   */
  protected LSMVectorIndexMutable(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
  }

  @Override
  public Object getMainComponent() {
    return mainIndex;
  }

  /**
   * Get the main index with proper typing.
   */
  public LSMVectorIndex getMainIndex() {
    return mainIndex;
  }

  /**
   * Set the main index reference (called after construction)
   */
  public void setMainIndex(final LSMVectorIndex mainIndex) {
    this.mainIndex = mainIndex;
  }

  /**
   * Called after schema is loaded. Delegates to main index to load vectors from pages
   * after metadata has been populated from schema.json.
   */
  @Override
  public void onAfterSchemaLoad() {
    if (mainIndex != null) {
      mainIndex.loadVectorsAfterSchemaLoad();
    }
  }

  /**
   * Creates a new vector data page with LSM-style header.
   * Returns a MutablePage ready for writing entries.
   */
  public MutablePage createNewVectorDataPage(final int pageNum) throws IOException {
    final PageId pageId = new PageId(database, getFileId(), pageNum);
    final MutablePage currentPage = database.isTransactionActive() ?
        database.getTransaction().addPage(pageId, getPageSize()) :
        new MutablePage(pageId, getPageSize());

    // IMPORTANT: Use MutablePage.writeInt/writeByte which account for PAGE_HEADER_SIZE offset
    // This ensures consistency with BasePage.readInt/readByte used elsewhere
    currentPage.writeInt(OFFSET_FREE_CONTENT, currentPage.getMaxContentSize()); // offsetFreeContent starts at max content size
    currentPage.writeInt(OFFSET_NUM_ENTRIES, 0);              // numberOfEntries = 0
    currentPage.writeByte(OFFSET_MUTABLE, (byte) 1);          // mutable = 1 (page is being written to)

    return currentPage;
  }

  /**
   * Get page for reading
   */
  public BasePage getPage(final int pageNum) {
    try {
      return database.getTransaction().getPage(new PageId(database, getFileId(), pageNum), getPageSize());
    } catch (final IOException e) {
      throw new IndexException("Error reading page " + pageNum, e);
    }
  }

  /**
   * Get page for modification
   */
  public BasePage getPageToModify(final int pageNum, final boolean addMissingPages) {
    try {
      return database.getTransaction().getPageToModify(new PageId(database, getFileId(), pageNum), getPageSize(), addMissingPages);
    } catch (final IOException e) {
      throw new IndexException("Error modifying page " + pageNum, e);
    }
  }

  @Override
  public void onAfterCommit() {
    // Delegate to main index to process buffered operations and schedule compaction
    if (mainIndex != null) {
      mainIndex.onAfterCommit();
    }
  }

  /**
   * Returns the PaginatedComponent reference for registration with schema
   */
  public PaginatedComponent getComponent() {
    return this;
  }

  /**
   * Returns the file path
   */
  public String getFilePath() {
    return filePath;
  }
}
