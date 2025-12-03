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
  public IndexMetadata metadata;

  protected TypeIndexBuilder(final DatabaseInternal database, final String typeName, final String[] propertyNames) {
    super(database, TypeIndex.class);
    this.metadata = new IndexMetadata(typeName, propertyNames, -1);
  }

  /**
   * Sets the index type. For LSM_VECTOR indexes, returns an LSMVectorIndexBuilder
   * to enable vector-specific configuration methods.
   *
   * @param indexType the index type
   *
   * @return appropriate builder for the index type
   */
  @Override
  public TypeIndexBuilder withType(final Schema.INDEX_TYPE indexType) {
    if (indexType == Schema.INDEX_TYPE.LSM_VECTOR && !(this instanceof TypeLSMVectorIndexBuilder))
      return new TypeLSMVectorIndexBuilder(this);
    super.withType(indexType);
    return this;
  }

  @Override
  public TypeIndex create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (database.isAsyncProcessing())
      throw new NeedRetryException("Cannot create a new index while asynchronous tasks are running");

    final LocalSchema schema = database.getSchema().getEmbedded();

    final LocalDocumentType type = schema.getType(metadata.typeName);
    final TypeIndex existingTypeIndex = type.getPolymorphicIndexByProperties(metadata.propertyNames);

    if (existingTypeIndex != null) {
      if (ignoreIfExists) {
        if (existingTypeIndex.getNullStrategy() != null && existingTypeIndex.getNullStrategy() == null ||//
            existingTypeIndex.isUnique() != unique) {
          // DIFFERENT, DROP AND RECREATE IT
          existingTypeIndex.drop();
        } else
          return existingTypeIndex;
      } else
        throw new IllegalArgumentException(
            "Found the existent index '" + existingTypeIndex.getName() + "' defined on the properties '" + Arrays.asList(
                metadata.propertyNames) + "' for type '" + metadata.typeName + "'");
    }

    if (indexType == null)
      throw new DatabaseMetadataException(
          "Cannot create index on type '" + metadata.typeName + "' because indexType was not specified");
    if (metadata.propertyNames.isEmpty())
      throw new DatabaseMetadataException(
          "Cannot create index on type '" + metadata.typeName + "' because there are no property defined");

    // CHECK ALL THE PROPERTIES EXIST
    final Type[] keyTypes = new Type[metadata.propertyNames.size()];
    int i = 0;

    for (final String propertyName : metadata.propertyNames) {
      if (type instanceof LocalEdgeType && ("@out".equals(propertyName) || "@in".equals(propertyName))) {
        keyTypes[i++] = Type.LINK;
      } else {
        // Check if property has " by item" modifier
        String actualPropertyName = propertyName;
        boolean isByItem = false;

        if (propertyName.endsWith(" by item")) {
          isByItem = true;
          actualPropertyName = propertyName.substring(0, propertyName.length() - 8);
        }

        // First, try to find the property with the exact name (handles properties with dots in their names)
        Property property = type.getPolymorphicPropertyIfExists(actualPropertyName);

        if (property == null && actualPropertyName.contains(".")) {
          // Property with exact name doesn't exist, check if this could be a nested path
          final String[] pathParts = actualPropertyName.split("\\.", 2); // Split into at most 2 parts
          final String rootPropertyName = pathParts[0];

          // Try to find the root property
          property = type.getPolymorphicPropertyIfExists(rootPropertyName);

          if (property != null) {
            // Found root property - this is a nested path
            // For nested paths with BY ITEM, the root must be a LIST
            if (isByItem && property.getType() != Type.LIST) {
              throw new SchemaException(
                  "Cannot create index with BY ITEM on nested property path '" + metadata.typeName + "." + actualPropertyName
                      + "' because the root property '" + rootPropertyName + "' is not a LIST type (found: " + property.getType()
                      + ")");
            }

            // For nested properties, we'll use STRING as the key type since we can't validate the nested structure at schema definition time
            // The actual type will be determined at runtime during indexing
            keyTypes[i++] = Type.STRING;
            continue;
          }
        }

        // If we still don't have a property, it doesn't exist
        if (property == null) {
          throw new SchemaException(
              "Cannot create the index on type '" + metadata.typeName + "." + actualPropertyName
                  + "' because the property does not exist");
        }

        // Validate BY ITEM is only used with LIST type
        if (isByItem && property.getType() != Type.LIST) {
          throw new SchemaException("Cannot create index with BY ITEM on property '" + metadata.typeName + "." + actualPropertyName
              + "' because it is not a LIST type (found: " + property.getType() + ")");
        }

        // For BY ITEM on LIST, the key type should be STRING (since list items are indexed individually)
        // Lists can contain heterogeneous types, so we use STRING as a generic type for list items
        if (isByItem) {
          keyTypes[i++] = Type.STRING;
        } else {
          keyTypes[i++] = property.getType();
        }
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

            indexes[finalIdx] = createBucketIndex(schema, type, keyTypes, bucket);

          }, false, maxAttempts, null, (error) -> {
            for (int j = 0; j < indexes.length; j++) {
              final IndexInternal indexToRemove = (IndexInternal) indexes[j];
              if (indexToRemove != null)
                indexToRemove.drop();
            }
          });
        }

        return null;
      });

      return type.getPolymorphicIndexByProperties(metadata.propertyNames);
    } catch (final NeedRetryException e) {
      schema.dropIndex(metadata.typeName + metadata.propertyNames);
      throw e;
    } catch (final Throwable e) {
      schema.dropIndex(metadata.typeName + metadata.propertyNames);
      throw new IndexException("Error on creating index on type '" + metadata.typeName + "', properties " + metadata.propertyNames,
          e);
    }
  }

  protected Index createBucketIndex(final LocalSchema schema, final LocalDocumentType type, final Type[] keyTypes,
      final LocalBucket bucket) {
    return schema.createBucketIndex(type, keyTypes, bucket, metadata.typeName, indexType, unique, pageSize, nullStrategy, callback,
        metadata.propertyNames.toArray(new String[0]), null, batchSize,
        metadata);
  }

  public String getTypeName() {
    return metadata.typeName;
  }

  public String[] getPropertyNames() {
    return metadata.propertyNames.toArray(new String[0]);
  }
}
