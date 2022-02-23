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

import java.io.*;
import java.util.concurrent.atomic.*;

/**
 * HEADER = [recordCount(int:4)] CONTENT-PAGES = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(512*ushort=2048)]
 */
public abstract class PaginatedComponent {
  protected final DatabaseInternal database;
  protected final String           name;
  protected final PaginatedFile    file;
  protected final int              id;
  protected final int              pageSize;
  protected final int              version;
  protected final AtomicInteger    pageCount = new AtomicInteger();

  protected PaginatedComponent(final DatabaseInternal database, final String name, String filePath, final String ext, final PaginatedFile.MODE mode,
      final int pageSize, final int version) throws IOException {
    this(database, name, filePath, ext, database.getFileManager().newFileId(), mode, pageSize, version);
  }

  private PaginatedComponent(final DatabaseInternal database, final String name, String filePath, final String ext, final int id, final PaginatedFile.MODE mode,
      final int pageSize, final int version) throws IOException {
    this(database, name, filePath + "." + id + "." + pageSize + ".v" + version + "." + ext, id, mode, pageSize, version);
  }

  protected PaginatedComponent(final DatabaseInternal database, final String name, String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize, final int version) throws IOException {
    if (pageSize <= 0)
      throw new IllegalArgumentException("Invalid page size " + pageSize);
    if (id < 0)
      throw new IllegalArgumentException("Invalid file id " + id);
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Invalid file name " + name);

    this.database = database;
    this.name = name;
    this.id = id;
    this.pageSize = pageSize;
    this.version = version;

    this.file = database.getFileManager().getOrCreateFile(name, filePath, mode);

    if (file.getSize() == 0)
      // NEW FILE, CREATE HEADER PAGE
      pageCount.set(0);
    else
      pageCount.set((int) (file.getSize() / getPageSize()));
  }

  public File getOSFile() {
    return file.getOSFile();
  }

  public void onAfterLoad() {
  }

  public void onAfterCommit() {
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageCount(final int value) {
    assert value > pageCount.get();
    pageCount.set(value);
  }

  public String getName() {
    return name;
  }

  public int getId() {
    return id;
  }

  public int getVersion() {
    return version;
  }

  public DatabaseInternal getDatabase() {
    return database;
  }

  public void close() {
    if (file != null)
      file.close();
  }

  public int getTotalPages() {
    final TransactionContext tx = database.getTransaction();
    if (tx != null) {
      final Integer txPageCounter = tx.getPageCounter(id);
      if (txPageCounter != null)
        return txPageCounter;
    }
    return pageCount.get();
  }

  public Object getMainComponent() {
    return this;
  }
}
