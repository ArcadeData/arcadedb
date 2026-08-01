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
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
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
    // Install the sparse metadata here too: the superclass constructor leaves a plain IndexMetadata, and every setter
    // on this builder writes into the sparse one.
    this.metadata = new LSMSparseVectorIndexMetadata(typeName, propertyNames, -1);
  }

  /**
   * Sets the maximum dimensionality of the sparse vectors. Used as an upper bound for index sizing
   * and validation. A value of 0 (default) means dimensions are inferred from the data.
   */
  public TypeLSMSparseVectorIndexBuilder withDimensions(final int dimensions) {
    sparseMetadata().setDimensions(dimensions);
    return this;
  }

  /**
   * Sets the scoring modifier. Currently supported: NONE (default), IDF.
   */
  public TypeLSMSparseVectorIndexBuilder withModifier(final String modifier) {
    sparseMetadata().setModifier(modifier);
    return this;
  }

  /**
   * Sets the posting-weight quantization: INT8 (default, 1 byte/weight), FP16 (2 bytes) or FP32
   * (4 bytes, exact scoring). Mirrors the dense vector index's {@code quantization} knob.
   */
  public TypeLSMSparseVectorIndexBuilder withWeightQuantization(final WeightQuantization weightQuantization) {
    sparseMetadata().weightQuantization =
        weightQuantization == null ? LSMSparseVectorIndexMetadata.DEFAULT_WEIGHT_QUANTIZATION : weightQuantization;
    return this;
  }

  /**
   * Sets the posting-weight quantization from its name (FP32, FP16 or INT8), case-insensitive.
   */
  public TypeLSMSparseVectorIndexBuilder withWeightQuantization(final String weightQuantization) {
    return withWeightQuantization(LSMSparseVectorIndexMetadata.parseWeightQuantization(weightQuantization));
  }

  @Override
  public TypeLSMSparseVectorIndexBuilder withMetadata(final IndexMetadata metadata) {
    // Guarded rather than cast, for the same reason as sparseMetadata(): an actionable error beats a
    // ClassCastException attributed to an unrelated line.
    if (metadata != null && !(metadata instanceof LSMSparseVectorIndexMetadata))
      throw new IllegalArgumentException(
          "An LSM_SPARSE_VECTOR index requires LSMSparseVectorIndexMetadata but got " + metadata.getClass().getName());
    this.metadata = metadata;
    return this;
  }

  /**
   * Configures the builder from the {@code METADATA} clause of {@code CREATE INDEX}. Unknown keys are rejected rather
   * than dropped, so a typo such as {@code {"modifer": "IDF"}} is reported instead of yielding an index with the
   * default scoring (issue #5639).
   *
   * @param json the JSON object containing the metadata configuration
   *
   * @return this builder for chaining
   */
  public TypeLSMSparseVectorIndexBuilder withMetadata(final JSONObject json) {
    sparseMetadata().fromUserMetadata(json, Schema.INDEX_TYPE.LSM_SPARSE_VECTOR);
    return this;
  }

  /**
   * Returns the builder's metadata as {@link LSMSparseVectorIndexMetadata}. The constructors always create one, but
   * guard the cast so that, if the metadata were ever replaced with a non-sparse instance through
   * {@link #withMetadata(IndexMetadata)}, callers get an actionable error instead of a {@link ClassCastException}.
   */
  private LSMSparseVectorIndexMetadata sparseMetadata() {
    if (metadata instanceof LSMSparseVectorIndexMetadata m)
      return m;
    throw new IndexException("Sparse vector index metadata expected but was "
        + (metadata == null ? "null" : metadata.getClass().getSimpleName()));
  }
}
