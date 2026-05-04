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
import com.arcadedb.index.IndexException;
import com.arcadedb.serializer.json.JSONObject;

/**
 * Builder for {@link Schema.INDEX_TYPE#LSM_SPARSE_VECTOR LSM_SPARSE_VECTOR} indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeLSMSparseVectorIndexBuilder extends TypeIndexBuilder {

  protected TypeLSMSparseVectorIndexBuilder(final TypeIndexBuilder copyFrom) {
    super(copyFrom.database, copyFrom.metadata.typeName, copyFrom.metadata.propertyNames.toArray(new String[0]));

    this.metadata = new LSMSparseVectorIndexMetadata(
        copyFrom.metadata.typeName,
        copyFrom.metadata.propertyNames.toArray(new String[0]),
        copyFrom.metadata.associatedBucketId);

    this.indexType = Schema.INDEX_TYPE.LSM_SPARSE_VECTOR;
    this.unique = copyFrom.unique;
    this.pageSize = copyFrom.pageSize;
    this.nullStrategy = copyFrom.nullStrategy;
    this.callback = copyFrom.callback;
    this.ignoreIfExists = copyFrom.ignoreIfExists;
    this.indexName = copyFrom.indexName;
    this.filePath = copyFrom.filePath;
    this.keyTypes = copyFrom.keyTypes;
    this.batchSize = copyFrom.batchSize;
    this.maxAttempts = copyFrom.maxAttempts;
  }

  protected TypeLSMSparseVectorIndexBuilder(final DatabaseInternal database, final String typeName,
      final String[] propertyNames) {
    super(database, typeName, propertyNames);
    this.indexType = Schema.INDEX_TYPE.LSM_SPARSE_VECTOR;
  }

  /**
   * Sets the maximum dimensionality of the sparse vectors. Used as an upper bound for index sizing
   * and validation. A value of 0 (default) means dimensions are inferred from the data.
   */
  public TypeLSMSparseVectorIndexBuilder withDimensions(final int dimensions) {
    if (dimensions < 0)
      throw new IllegalArgumentException("dimensions must be >= 0");
    ((LSMSparseVectorIndexMetadata) metadata).dimensions = dimensions;
    return this;
  }

  /**
   * Sets the scoring modifier. Currently supported: NONE (default), IDF.
   */
  public TypeLSMSparseVectorIndexBuilder withModifier(final String modifier) {
    final String normalized = modifier == null ? LSMSparseVectorIndexMetadata.MODIFIER_NONE : modifier.toUpperCase();
    if (!LSMSparseVectorIndexMetadata.MODIFIER_NONE.equals(normalized)
        && !LSMSparseVectorIndexMetadata.MODIFIER_IDF.equals(normalized))
      throw new IndexException("Invalid sparse vector index modifier: " + modifier + ". Supported values: NONE, IDF");
    ((LSMSparseVectorIndexMetadata) metadata).modifier = normalized;
    return this;
  }

  @Override
  public TypeLSMSparseVectorIndexBuilder withMetadata(final IndexMetadata metadata) {
    this.metadata = (LSMSparseVectorIndexMetadata) metadata;
    return this;
  }

  public TypeLSMSparseVectorIndexBuilder withMetadata(final JSONObject json) {
    final LSMSparseVectorIndexMetadata meta = (LSMSparseVectorIndexMetadata) metadata;
    meta.dimensions = json.getInt("dimensions", meta.dimensions);
    meta.modifier = json.getString("modifier", meta.modifier).toUpperCase();
    return this;
  }
}
