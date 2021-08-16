/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.engine;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HEADER = [recordCount(int:4)] CONTENT-PAGES = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(512*ushort=2048)]
 */
public abstract class PaginatedComponent {
  protected final DatabaseInternal database;
  protected final String           name;
  protected final PaginatedFile    file;
  protected final int              id;
  protected final int              pageSize;
  protected final AtomicInteger    pageCount = new AtomicInteger();

  protected PaginatedComponent(final DatabaseInternal database, final String name, String filePath, final String ext, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    this(database, name, filePath, ext, database.getFileManager().newFileId(), mode, pageSize);
  }

  protected PaginatedComponent(final DatabaseInternal database, final String name, String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    this.database = database;
    this.name = name;
    this.id = id;
    this.pageSize = pageSize;

    this.file = database.getFileManager().getOrCreateFile(name, filePath, mode);

    if (file.getSize() == 0)
      // NEW FILE, CREATE HEADER PAGE
      pageCount.set(0);
    else
      pageCount.set((int) (file.getSize() / getPageSize()));
  }

  private PaginatedComponent(final DatabaseInternal database, final String name, String filePath, final String ext, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    this(database, name, filePath + "." + id + "." + pageSize + "." + ext, id, mode, pageSize);
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
