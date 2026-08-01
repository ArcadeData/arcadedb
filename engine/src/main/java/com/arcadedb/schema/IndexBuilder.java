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
import com.arcadedb.index.Index;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;

/**
 * Builder class for index types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class IndexBuilder<T extends Index> {
  public static final int BUILD_BATCH_SIZE = 5_000;

  /**
   * Value of {@link #pageSize} meaning "the caller did not ask for a page size", so each index implementation is free
   * to pick its own default.
   * <p>
   * This used to be expressed by initialising the field to {@link LSMTreeIndexAbstract#DEF_PAGE_SIZE} and having
   * {@code HashIndex} read that exact value back as "unset". That conflated the two, and made 262144 the one page size
   * a hash index could never actually be given - see issue #5713.
   */
  public static final int PAGE_SIZE_UNSET = -1;

  final DatabaseInternal       database;
  final Class<? extends Index> indexImplementation;
  Schema.INDEX_TYPE                  indexType;
  boolean                            unique;
  int                                pageSize       = PAGE_SIZE_UNSET;
  LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy   = LSMTreeIndexAbstract.NULL_STRATEGY.SKIP;
  Index.BuildIndexCallback           callback;
  boolean                            ignoreIfExists = false;
  String                             indexName      = null;
  String                             filePath       = null;
  Type[]                             keyTypes;
  int                                batchSize      = BUILD_BATCH_SIZE;
  int                                maxAttempts    = 1;
  /**
   * The one and only metadata slot of the builder hierarchy. {@link TypeIndexBuilder} used to declare a field of the
   * same name, which SHADOWED this one: {@code withMetadata()} wrote here while {@code create()} read there, so every
   * index type without a dedicated builder subclass (GEOSPATIAL, see #5478) silently lost the metadata it was given.
   * Keep it single - and reach it through {@link #withMetadata(IndexMetadata)} / {@link #getMetadata()} from outside
   * this package, so a second slot cannot be reintroduced unnoticed.
   */
  IndexMetadata                      metadata;

  protected IndexBuilder(final DatabaseInternal database, final Class<? extends Index> indexImplementation) {
    this.database = database;
    this.indexImplementation = indexImplementation;
  }

  public abstract T create();

  public IndexBuilder<T> withType(final Schema.INDEX_TYPE indexType) {
    this.indexType = indexType;
    return this;
  }

  public TypeLSMVectorIndexBuilder withLSMVectorType() {
    if (this instanceof TypeLSMVectorIndexBuilder v)
      return v;

    return new TypeLSMVectorIndexBuilder((TypeIndexBuilder) this);
  }

  public TypeLSMSparseVectorIndexBuilder withSparseVectorType() {
    if (this instanceof TypeLSMSparseVectorIndexBuilder v)
      return v;

    return new TypeLSMSparseVectorIndexBuilder((TypeIndexBuilder) this);
  }

  public IndexBuilder<T> withUnique(final boolean unique) {
    this.unique = unique;
    return this;
  }

  public IndexBuilder<T> withIgnoreIfExists(final boolean ignoreIfExists) {
    this.ignoreIfExists = ignoreIfExists;
    return this;
  }

  /**
   * Requests an explicit page size for the index file. Any value below 1 means "unset", leaving the choice to the
   * index implementation - see {@link #getPageSize(int)}, which is the single place that resolves it. Normalising
   * here as well would be redundant, and the builder subclasses that copy the field verbatim
   * ({@code TypeLSMVectorIndexBuilder}, {@code TypeLSMSparseVectorIndexBuilder}) would bypass it anyway.
   */
  public IndexBuilder<T> withPageSize(final int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  public IndexBuilder<T> withNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    this.nullStrategy = nullStrategy;
    return this;
  }

  public IndexBuilder<T> withCallback(final Index.BuildIndexCallback callback) {
    this.callback = callback;
    return this;
  }

  public DatabaseInternal getDatabase() {
    return database;
  }

  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return nullStrategy;
  }

  /**
   * Returns the requested page size, or the LSM default when none was requested. Kept for the index implementations
   * whose default IS the LSM one; anything with a different default must use {@link #getPageSize(int)} so it can tell
   * "the caller asked for 262144" from "the caller asked for nothing".
   */
  public int getPageSize() {
    return getPageSize(LSMTreeIndexAbstract.DEF_PAGE_SIZE);
  }

  /**
   * Returns the page size the caller explicitly requested, or {@code defaultIfUnset} when none was requested.
   */
  public int getPageSize(final int defaultIfUnset) {
    return pageSize > 0 ? pageSize : defaultIfUnset;
  }

  public Schema.INDEX_TYPE getIndexType() {
    return indexType;
  }

  public Class<? extends Index> getIndexImplementation() {
    return indexImplementation;
  }

  public Index.BuildIndexCallback getCallback() {
    return callback;
  }

  public boolean isUnique() {
    return unique;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getFilePath() {
    return filePath;
  }

  public Type[] getKeyTypes() {
    return keyTypes;
  }

  public IndexMetadata getMetadata() {
    return metadata;
  }

  public IndexBuilder<T> withIndexName(final String indexName) {
    this.indexName = indexName;
    return this;
  }

  public IndexBuilder<T> withFilePath(final String path) {
    this.filePath = path;
    return this;
  }

  public IndexBuilder<T> withKeyTypes(final Type[] keyTypes) {
    this.keyTypes = keyTypes;
    return this;
  }

  public IndexBuilder<T> withBatchSize(final int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public IndexBuilder<T> withMaxAttempts(final int maxAttempts) {
    this.maxAttempts = maxAttempts;
    return this;
  }

  public IndexBuilder<T> withMetadata(final IndexMetadata metadata) {
    this.metadata = metadata;
    return this;
  }
}
