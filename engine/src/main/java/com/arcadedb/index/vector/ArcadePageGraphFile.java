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

import java.io.*;

/**
 * Base class for writer and reader
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class ArcadePageGraphFile {
  protected final DatabaseInternal database;
  protected final int              fileId;
  protected final int              pageSize;
  protected final long             totalBytes;  // Total graph data size
  protected final int              usablePageSize; // pageSize - BasePage.PAGE_HEADER_SIZE

  // Current state
  protected long     currentPosition;  // Logical position in the graph data
  protected BasePage currentPage;
  protected int      currentPageNum;

  public ArcadePageGraphFile(final DatabaseInternal database, final int fileId, final int pageSize, final long totalBytes) {
    this.database = database;
    this.fileId = fileId;
    this.pageSize = pageSize;
    this.totalBytes = totalBytes;
    this.usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;
    this.currentPosition = 0;
    this.currentPageNum = -1;
  }

  protected abstract void ensurePageLoaded(final int pageNum) throws IOException;

  public void seek(final long position) throws IOException {
    if (position < 0)
      throw new IOException("Invalid seek position: " + position + " (length=" + totalBytes + ")");

    this.currentPosition = position;

    int pageNum = (int) (position / pageSize);
    final int pageOffset = (int) (position % pageSize);

    if (pageOffset >= usablePageSize) {
      ++pageNum;
      currentPosition = (long) pageNum * pageSize;
    }

    // Load page if not already loaded
    ensurePageLoaded(pageNum);
  }

  public long getPosition() {
    return currentPosition;
  }

  public long length() {
    return totalBytes;
  }

  protected int ensureAvailable(final int bytes) throws IOException {
    int pageNum = (int) (currentPosition / pageSize);
    int pageOffset = (int) (currentPosition % pageSize);

    if (pageNum == 14 && pageOffset == 17944)
      System.out.println("DEBUG");

    if (pageOffset + bytes > usablePageSize) {
      ++pageNum;
      currentPosition = (long) pageNum * pageSize;
      pageOffset = 0;
    }
    ensurePageLoaded(pageNum);
    return pageOffset;
  }

  public void close() {
    // No-op: pages are managed by PageManager
    currentPage = null;
  }

  protected void updatePosition(final int bytesRead) throws IOException {
    int pageNum = (int) (currentPosition / pageSize);
    final int pageOffset = (int) (currentPosition % pageSize);

    currentPosition += bytesRead;

    if (pageOffset + bytesRead >= usablePageSize) {
      // MOVE TO THE NEXT PAGE
      ++pageNum;
      ensurePageLoaded(pageNum);
      currentPosition = (long) pageNum * pageSize;
    }
  }
}
