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
package com.arcadedb.engine;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.IndexException;

import java.io.*;
import java.util.concurrent.atomic.*;

/**
 * Extends a FileComponent by supporting pages.
 * <p>
 * HEADER = [recordCount(int:4)] CONTENT-PAGES = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(512*ushort=2048)]
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class PaginatedComponent extends Component {
  public static final String                 TEMP_EXT  = "temp_";
  protected final     PaginatedComponentFile file;
  protected final     int                    pageSize;
  protected final     AtomicInteger          pageCount = new AtomicInteger();

  protected PaginatedComponent(final DatabaseInternal database, final String name, final String filePath, final String ext,
      final ComponentFile.MODE mode,
      final int pageSize, final int version) throws IOException {
    this(database, name, filePath, ext, database.getFileManager().newFileId(), mode, pageSize, version);
  }

  private PaginatedComponent(final DatabaseInternal database, final String name, final String filePath, final String ext,
      final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    this(database, name, filePath + "." + id + "." + pageSize + ".v" + version + "." + ext, id, mode, pageSize, version);
  }

  protected PaginatedComponent(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode,
      final int pageSize, final int version) throws IOException {
    super(database, name, id, version, filePath);
    if (pageSize <= 0)
      throw new IllegalArgumentException("Invalid page size " + pageSize);

    this.pageSize = pageSize;
    this.file = (PaginatedComponentFile) database.getFileManager().getOrCreateFile(name, filePath, mode);

    final long fileSize = file.getSize();
    if (fileSize == 0)
      // NEW FILE, CREATE HEADER PAGE
      pageCount.set(0);
    else
      pageCount.set((int) (fileSize / pageSize));
  }

  public void rename(final String newName) throws IOException {
    PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(database);
    file.rename(newName);
    database.getFileManager().renameFile(componentName, newName);
    componentName = newName;
  }

  public PaginatedComponentFile getComponentFile() {
    return file;
  }

  public File getOSFile() {
    return file.getOSFile();
  }

  public int getPageSize() {
    return pageSize;
  }

  public void updatePageCount(final int totalPages) {
    // USE IF TO SPEED UP THE CHECK
    if (totalPages > pageCount.get())
      pageCount.updateAndGet(current -> Math.max(totalPages, current));
  }

  @Override
  public void close() {
    if (file != null)
      file.close();
  }

  public int getTotalPages() {
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null) {
      final Integer txPageCounter = tx.getPageCounter(fileId);
      if (txPageCounter != null)
        return txPageCounter;
    }
    return pageCount.get();
  }

  public void removeTempSuffix() {
    final String fileName = file.getFilePath();

    final int extPos = fileName.lastIndexOf('.');
    if (fileName.substring(extPos + 1).startsWith(TEMP_EXT)) {
      final String newFileName = fileName.substring(0, extPos) + "." + fileName.substring(extPos + TEMP_EXT.length() + 1);

      try {
        file.rename(newFileName);
        database.getFileManager().renameFile(fileName, newFileName);
      } catch (final IOException e) {
        throw new IndexException(
            "Cannot rename index file '" + file.getFilePath() + "' into temp file '" + newFileName + "' (exists=" + (new File(
                file.getFilePath()).exists()) + ")", e);
      }
    }
  }

}
