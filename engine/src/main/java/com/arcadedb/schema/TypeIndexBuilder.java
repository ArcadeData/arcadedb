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

import java.util.Arrays;
import java.util.List;

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

  /**
   * Sets the index type. For LSM_VECTOR indexes, returns an LSMVectorIndexBuilder
   * to enable vector-specific configuration methods.
   *
   * @param indexType the index type
   * @return appropriate builder for the index type
   */
  @Override
  public TypeIndexBuilder withType(final Schema.INDEX_TYPE indexType) {
    super.withType(indexType);

    // For vector indexes, return LSMVectorIndexBuilder to enable vector-specific methods
    if (indexType == Schema.INDEX_TYPE.LSM_VECTOR && !(this instanceof LSMVectorIndexBuilder)) {
      final LSMVectorIndexBuilder vectorBuilder = new LSMVectorIndexBuilder(database, typeName, propertyNames);
      // Copy settings from this builder
      vectorBuilder.withType(indexType);
      if (this.indexName != null)
        vectorBuilder.withIndexName(this.indexName);
      if (this.filePath != null)
        vectorBuilder.withFilePath(this.filePath);
      vectorBuilder.withUnique(this.unique);
      vectorBuilder.withPageSize(this.pageSize);
      vectorBuilder.withNullStrategy(this.nullStrategy);
      vectorBuilder.withIgnoreIfExists(this.ignoreIfExists);
      if (this.callback != null)
        vectorBuilder.withCallback(this.callback);
      vectorBuilder.withBatchSize(this.batchSize);
      vectorBuilder.withMaxAttempts(this.maxAttempts);
      return vectorBuilder;
    }

    return this;
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
                  "Cannot create index with BY ITEM on nested property path '" + typeName + "." + actualPropertyName +
                  "' because the root property '" + rootPropertyName + "' is not a LIST type (found: " + property.getType() + ")");
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
              "Cannot create the index on type '" + typeName + "." + actualPropertyName + "' because the property does not exist");
        }

        // Validate BY ITEM is only used with LIST type
        if (isByItem && property.getType() != Type.LIST) {
          throw new SchemaException(
              "Cannot create index with BY ITEM on property '" + typeName + "." + actualPropertyName +
              "' because it is not a LIST type (found: " + property.getType() + ")");
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

  /**
   * Sets the number of dimensions for vector indexes.
   * This method is for LSM_VECTOR indexes. When called on a base TypeIndexBuilder,
   * it does nothing. Override in LSMVectorIndexBuilder to actually set the dimensions.
   *
   * @param dimensions the number of dimensions
   * @return this builder
   */
  public TypeIndexBuilder withDimensions(final int dimensions) {
    // Base implementation does nothing - LSMVectorIndexBuilder will override
    return this;
  }

  /**
   * Sets the similarity function for vector indexes.
   * This method is for LSM_VECTOR indexes.
   *
   * @param similarity the similarity function name
   * @return this builder
   */
  public TypeIndexBuilder withSimilarity(final String similarity) {
    // Base implementation does nothing - LSMVectorIndexBuilder will override
    return this;
  }

  /**
   * Sets the maximum connections for vector indexes.
   * This method is for LSM_VECTOR indexes.
   *
   * @param maxConnections the maximum number of connections
   * @return this builder
   */
  public TypeIndexBuilder withMaxConnections(final int maxConnections) {
    // Base implementation does nothing - LSMVectorIndexBuilder will override
    return this;
  }

  /**
   * Sets the beam width for vector indexes.
   * This method is for LSM_VECTOR indexes.
   *
   * @param beamWidth the beam width
   * @return this builder
   */
  public TypeIndexBuilder withBeamWidth(final int beamWidth) {
    // Base implementation does nothing - LSMVectorIndexBuilder will override
    return this;
  }

  /**
   * Sets the ID property for vector indexes.
   * This method is for LSM_VECTOR indexes.
   *
   * @param idPropertyName the ID property name
   * @return this builder
   */
  public TypeIndexBuilder withIdProperty(final String idPropertyName) {
    // Base implementation does nothing - LSMVectorIndexBuilder will override
    return this;
  }
}
