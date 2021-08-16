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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PaginatedComponentFactory {
  private final Map<String, PaginatedComponentFactoryHandler> map = new HashMap<>();
  private final DatabaseInternal                              database;

  public interface PaginatedComponentFactoryHandler {
    PaginatedComponent createOnLoad(DatabaseInternal database, String name, String filePath, int id, PaginatedFile.MODE mode, int pageSize) throws IOException;
  }

  public PaginatedComponentFactory(final DatabaseInternal database) {
    this.database = database;
  }

  public void registerComponent(final String fileExt, final PaginatedComponentFactoryHandler handler) {
    map.put(fileExt, handler);
  }

  public PaginatedComponent createComponent(final PaginatedFile file, final PaginatedFile.MODE mode) throws IOException {
    final String fileName = file.getComponentName();
    final int fileId = file.getFileId();
    final String fileExt = file.getFileExtension();
    final int pageSize = file.getPageSize();

    final PaginatedComponentFactoryHandler handler = map.get(fileExt);
    if (handler != null)
      return handler.createOnLoad(database, fileName, file.getFilePath(), fileId, mode, pageSize);

    return null;
  }
}
