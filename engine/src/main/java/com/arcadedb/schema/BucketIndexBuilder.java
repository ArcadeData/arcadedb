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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Builder class for bucket indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BucketIndexBuilder extends IndexBuilder<Index> {
  final String   typeName;
  final String   bucketName;
  final String[] propertyNames;

  protected BucketIndexBuilder(final DatabaseInternal database, final String typeName, final String bucketName, final String[] propertyNames) {
    super(database, Index.class);
    this.typeName = typeName;
    this.bucketName = bucketName;
    this.propertyNames = propertyNames;
  }

  @Override
  public Index create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (database.isAsyncProcessing())
      throw new NeedRetryException("Cannot create a new index while asynchronous tasks are running");

    final LocalSchema schema = database.getSchema().getEmbedded();

    if (propertyNames.length == 0)
      throw new DatabaseMetadataException("Cannot create index on type '" + typeName + "' because there are no property defined");

    final LocalDocumentType type = schema.getType(typeName);

    // CHECK ALL THE PROPERTIES EXIST
    final Type[] keyTypes = new Type[propertyNames.length];
    int i = 0;

    for (final String propertyName : propertyNames) {
      final Property property = type.getPolymorphicPropertyIfExists(propertyName);
      if (property == null)
        throw new SchemaException("Cannot create the index on type '" + typeName + "." + propertyName + "' because the property does not exist");

      keyTypes[i++] = property.getType();
    }

    return schema.recordFileChanges(() -> {
      final AtomicReference<Index> result = new AtomicReference<>();
      database.transaction(() -> {

        Bucket bucket = null;
        final List<Bucket> buckets = type.getBuckets(true);
        for (final Bucket b : buckets) {
          if (bucketName.equals(b.getName())) {
            bucket = b;
            break;
          }
        }

        final Index index = schema.createBucketIndex(type, keyTypes, bucket, typeName, indexType, unique, pageSize, nullStrategy, callback, propertyNames, null,
            batchSize);
        result.set(index);

        schema.saveConfiguration();

      }, false, maxAttempts, null, (error) -> {
        final Index indexToRemove = result.get();
        if (indexToRemove != null) {
          ((IndexInternal) indexToRemove).drop();
        }
      });

      return result.get();
    });
  }
}
