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
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.security.SecurityDatabaseUser;

import java.io.*;
import java.util.concurrent.atomic.*;

/**
 * Builder class for manual indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ManualIndexBuilder extends IndexBuilder<Index> {
  protected ManualIndexBuilder(final DatabaseInternal database, final String indexName, final Type[] keyTypes) {
    super(database, Index.class);
    this.indexName = indexName;
    this.keyTypes = keyTypes;
  }

  public Index create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (database.isAsyncProcessing())
      throw new NeedRetryException("Cannot create a new index while asynchronous tasks are running");

    final LocalSchema schema = database.getSchema().getEmbedded();

    if (ignoreIfExists) {
      final IndexInternal index = schema.indexMap.get(indexName);
      if (index != null) {
        if (index.getNullStrategy() != null && index.getNullStrategy() == null ||//
            index.isUnique() != unique) {
          // DIFFERENT, DROP AND RECREATE IT
          index.drop();
        } else
          return index;
      }
    } else if (schema.indexMap.containsKey(indexName))
      throw new SchemaException("Cannot create index '" + indexName + "' because already exists");

    return schema.recordFileChanges(() -> {
      final AtomicReference<IndexInternal> result = new AtomicReference<>();
      database.transaction(() -> {

        filePath = database.getDatabasePath() + File.separator + indexName;

        final IndexInternal index = schema.indexFactory.createIndex(this);

        result.set(index);

        if (index instanceof PaginatedComponent)
          schema.registerFile((PaginatedComponent) index);

        schema.indexMap.put(indexName, index);

      }, false, 1, null, (error) -> {
        final IndexInternal indexToRemove = result.get();
        if (indexToRemove != null) {
          indexToRemove.drop();
        }
      });

      return result.get();
    });
  }
}
