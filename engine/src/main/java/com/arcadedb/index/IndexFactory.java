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

package com.arcadedb.index;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexFactory {
  private final Map<String, IndexFactoryHandler> map = new HashMap<>();

  public void register(final String type, final IndexFactoryHandler handler) {
    map.put(type, handler);
  }

  public IndexInternal createIndex(final String indexType, final DatabaseInternal database, final String indexName, final boolean unique, final String filePath,
      final PaginatedFile.MODE mode, final byte[] keyTypes, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) throws IOException {
    final IndexFactoryHandler handler = map.get(indexType);
    if (handler == null)
      throw new IllegalArgumentException("Cannot create index of type '" + indexType + "'");

    return handler.create(database, indexName, unique, filePath, mode, keyTypes, pageSize, nullStrategy, callback);
  }
}
