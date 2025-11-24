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
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.logging.*;

/**
 * PaginatedComponent for LSM-based vector index storage.
 * This class handles the low-level page management for vector data,
 * following the same pattern as LSMTreeIndexMutable for LSMTreeIndex.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexComponent extends PaginatedComponent {
  public static final  String FILE_EXT        = "lsmvecidx";
  public static final  int    CURRENT_VERSION = 0;
  public static final  int    DEF_PAGE_SIZE   = 262_144;

  // Page header layout constants
  public static final int OFFSET_FREE_CONTENT = 0;  // 4 bytes
  public static final int OFFSET_NUM_ENTRIES  = 4;  // 4 bytes
  public static final int OFFSET_MUTABLE      = 8;  // 1 byte
  public static final int HEADER_BASE_SIZE    = 9;  // offsetFreeContent(4) + numberOfEntries(4) + mutable(1)

  private LSMVectorIndex mainIndex;

  /**
   * Factory handler for loading components from disk
   */
  public static class PaginatedComponentFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public Component createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new LSMVectorIndexComponent(database, name, filePath, id, mode, pageSize, version);
    }
  }

  /**
   * Constructor for creating a new component
   */
  protected LSMVectorIndexComponent(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize) throws IOException {
    super(database, name, filePath, FILE_EXT, mode, pageSize, CURRENT_VERSION);

    // Initialize page 0 for new indexes (like LSMTreeIndexMutable does)
    database.checkTransactionIsActive(database.isAutoTransaction());
    initializePage0();
  }

  /**
   * Constructor for loading an existing component
   */
  protected LSMVectorIndexComponent(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
  }

  /**
   * Set the main index reference (called after construction)
   */
  public void setMainIndex(final LSMVectorIndex mainIndex) {
    this.mainIndex = mainIndex;
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

    final ByteBuffer pageBuffer = currentPage.getContent();
    pageBuffer.position(0);

    // Write initial header
    pageBuffer.putInt(OFFSET_FREE_CONTENT, getPageSize()); // offsetFreeContent starts at end
    pageBuffer.putInt(OFFSET_NUM_ENTRIES, 0);              // numberOfEntries = 0
    pageBuffer.put(OFFSET_MUTABLE, (byte) 1);              // mutable = 1 (page is being written to)

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

  /**
   * Initialize page 0 with metadata header.
   * Page 0 contains: nextId(4) + dimensions(4) + similarityFunction(4) + maxConnections(4) + beamWidth(4)
   * This is called when creating a new index to set up the metadata page.
   */
  private void initializePage0() throws IOException {
    final PageId pageId = new PageId(database, getFileId(), 0);
    final MutablePage page0 = database.isTransactionActive() ?
        database.getTransaction().addPage(pageId, getPageSize()) :
        new MutablePage(pageId, getPageSize());

    final ByteBuffer buffer = page0.getContent();
    buffer.position(0);

    // Initialize metadata with defaults (will be overwritten by LSMVectorIndex)
    buffer.putInt(0);  // nextId = 0
    buffer.putInt(0);  // dimensions (placeholder)
    buffer.putInt(0);  // similarityFunction (placeholder)
    buffer.putInt(0);  // maxConnections (placeholder)
    buffer.putInt(0);  // beamWidth (placeholder)
  }
}
