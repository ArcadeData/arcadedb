/*
 * Copyright 2023 Arcade Data Ltd
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
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.*;

/**
 * Builder class for type indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeIndexBuilder extends IndexBuilder<TypeIndex> {
  final String   typeName;
  final String[] propertyNames;

  protected TypeIndexBuilder(final DatabaseInternal database, final String typeName, final String[] propertyNames) {
    super(database, TypeIndex.class);
    this.typeName = typeName;
    this.propertyNames = propertyNames;
  }

  @Override
  public TypeIndex create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (database.isAsyncProcessing())
      throw new NeedRetryException("Cannot create a new index while asynchronous tasks are running");

    final LocalSchema schema = database.getSchema().getEmbedded();
    if (ignoreIfExists) {
      final DocumentType type = schema.getType(typeName);
      final TypeIndex index = type.getPolymorphicIndexByProperties(propertyNames);
      if (index != null) {
        if (index.getNullStrategy() != null && index.getNullStrategy() == null ||//
            index.isUnique() != unique) {
          // DIFFERENT, DROP AND RECREATE IT
          index.drop();
        } else
          return index;
      }
    }

    if (indexType == null)
      throw new DatabaseMetadataException("Cannot create index on type '" + typeName + "' because indexType was not specified");
    if (propertyNames.length == 0)
      throw new DatabaseMetadataException("Cannot create index on type '" + typeName + "' because there are no property defined");

    final LocalDocumentType type = schema.getType(typeName);

    final TypeIndex index = type.getPolymorphicIndexByProperties(propertyNames);
    if (index != null)
      throw new IllegalArgumentException(
          "Found the existent index '" + index.getName() + "' defined on the properties '" + Arrays.asList(propertyNames)
              + "' for type '" + typeName + "'");

    // CHECK ALL THE PROPERTIES EXIST
    final Type[] keyTypes = new Type[propertyNames.length];
    int i = 0;

    for (final String propertyName : propertyNames) {
      if (type instanceof LocalEdgeType && ("@out".equals(propertyName) || "@in".equals(propertyName))) {
        keyTypes[i++] = Type.LINK;
      } else {
        final Property property = type.getPolymorphicPropertyIfExists(propertyName);
        if (property == null)
          throw new SchemaException(
              "Cannot create the index on type '" + typeName + "." + propertyName + "' because the property does not exist");

        keyTypes[i++] = property.getType();
      }
    }

    final List<Bucket> buckets = type.getBuckets(true);
    final Index[] indexes = new Index[buckets.size()];

    try {
      schema.recordFileChanges(() -> {
        for (int idx = 0; idx < buckets.size(); ++idx) {
          final int finalIdx = idx;
          database.transaction(() -> {

            final LocalBucket bucket = (LocalBucket) buckets.get(finalIdx);
            indexes[finalIdx] = schema.createBucketIndex(type, keyTypes, bucket, typeName, indexType, unique, pageSize,
                nullStrategy, callback, propertyNames, null, batchSize);

          }, false, maxAttempts, null, (error) -> {
            for (int j = 0; j < indexes.length; j++) {
              final IndexInternal indexToRemove = (IndexInternal) indexes[j];
              if (indexToRemove != null)
                indexToRemove.drop();
            }
          });
        }

        schema.saveConfiguration();

        return null;
      });

      return type.getPolymorphicIndexByProperties(propertyNames);
    } catch (final NeedRetryException e) {
      schema.dropIndex(typeName + Arrays.toString(propertyNames));
      throw e;
    } catch (final Throwable e) {
      schema.dropIndex(typeName + Arrays.toString(propertyNames));
      throw new IndexException("Error on creating index on type '" + typeName + "', properties " + Arrays.toString(propertyNames),
          e);
    }
  }

  public String getTypeName() {
    return typeName;
  }

  public String[] getPropertyNames() {
    return propertyNames;
  }
}
