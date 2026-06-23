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
import com.arcadedb.serializer.json.JSONObject;

/**
 * Builder class for full-text indexes with analyzer configuration.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TypeFullTextIndexBuilder extends TypeIndexBuilder {

  protected TypeFullTextIndexBuilder(final TypeIndexBuilder copyFrom) {
    super(copyFrom.database, copyFrom.metadata.typeName, copyFrom.metadata.propertyNames.toArray(new String[0]));

    this.metadata = new FullTextIndexMetadata(
        copyFrom.metadata.typeName,
        copyFrom.metadata.propertyNames.toArray(new String[0]),
        copyFrom.metadata.associatedBucketId);

    this.indexType = Schema.INDEX_TYPE.FULL_TEXT;
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

  protected TypeFullTextIndexBuilder(final DatabaseInternal database, final String typeName, final String[] propertyNames) {
    super(database, typeName, propertyNames);
    this.indexType = Schema.INDEX_TYPE.FULL_TEXT;
    this.metadata = new FullTextIndexMetadata(typeName, propertyNames, -1);
  }

  /**
   * Sets the analyzer class name.
   *
   * @param analyzerClass the fully qualified class name of the Lucene analyzer
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withAnalyzer(final String analyzerClass) {
    ftMetadata().setAnalyzerClass(analyzerClass);
    return this;
  }

  /**
   * Sets the analyzer class for indexing.
   *
   * @param analyzerClass the fully qualified class name of the Lucene analyzer for indexing
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withIndexAnalyzer(final String analyzerClass) {
    ftMetadata().setIndexAnalyzerClass(analyzerClass);
    return this;
  }

  /**
   * Sets the analyzer class for querying.
   *
   * @param analyzerClass the fully qualified class name of the Lucene analyzer for querying
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withQueryAnalyzer(final String analyzerClass) {
    ftMetadata().setQueryAnalyzerClass(analyzerClass);
    return this;
  }

  /**
   * Sets whether leading wildcards are allowed in queries.
   *
   * @param allow true to allow leading wildcards
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withAllowLeadingWildcard(final boolean allow) {
    ftMetadata().setAllowLeadingWildcard(allow);
    return this;
  }

  /**
   * Sets the default boolean operator (AND or OR).
   *
   * @param operator "AND" or "OR"
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withDefaultOperator(final String operator) {
    ftMetadata().setDefaultOperator(operator);
    return this;
  }

  /**
   * Sets an analyzer for a specific field.
   *
   * @param fieldName     the field name
   * @param analyzerClass the fully qualified class name of the Lucene analyzer
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withFieldAnalyzer(final String fieldName, final String analyzerClass) {
    ftMetadata().setFieldAnalyzer(fieldName, analyzerClass);
    return this;
  }

  @Override
  public TypeFullTextIndexBuilder withMetadata(final IndexMetadata metadata) {
    if (metadata != null && !(metadata instanceof FullTextIndexMetadata))
      throw new IllegalArgumentException(
          "A FULL_TEXT index requires FullTextIndexMetadata but got " + metadata.getClass().getName());
    this.metadata = (FullTextIndexMetadata) metadata;
    return this;
  }

  /**
   * Configures the builder from a JSON metadata object.
   * Supports the following properties:
   * <ul>
   *   <li>analyzer - default analyzer class</li>
   *   <li>index_analyzer - analyzer class for indexing</li>
   *   <li>query_analyzer - analyzer class for querying</li>
   *   <li>allowLeadingWildcard - whether to allow leading wildcards</li>
   *   <li>defaultOperator - "AND" or "OR"</li>
   *   <li>[fieldName]_analyzer - per-field analyzer class</li>
   * </ul>
   *
   * @param json the JSON object containing metadata configuration
   */
  public void withMetadata(final JSONObject json) {
    ftMetadata().fromJSON(json);
  }

  /**
   * Returns the builder's metadata as {@link FullTextIndexMetadata}. The full-text builder constructors always create one, but
   * guard the cast so that, if the metadata were ever replaced with a non-full-text instance, callers get an actionable error
   * instead of a bare {@link ClassCastException} at configuration time.
   */
  private FullTextIndexMetadata ftMetadata() {
    if (metadata instanceof FullTextIndexMetadata m)
      return m;
    throw new IllegalStateException(
        "Full-text index metadata expected but was " + (metadata == null ? "null" : metadata.getClass().getName())
            + "; configure BM25/analyzer options on a FULL_TEXT index builder");
  }

  /**
   * Sets the similarity (ranking) model: {@code "BM25"} (default for new indexes) or {@code "CLASSIC"} (legacy term coordination).
   *
   * @param similarity the similarity name
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withSimilarity(final String similarity) {
    ftMetadata().setSimilarity(similarity);
    return this;
  }

  /**
   * Enables BM25 similarity with the given parameters.
   *
   * @param k1 term-frequency saturation parameter (typical 1.2)
   * @param b  document-length normalization parameter in [0,1] (typical 0.75)
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withBM25(final float k1, final float b) {
    final FullTextIndexMetadata m = ftMetadata();
    m.setSimilarity(FullTextIndexMetadata.SIMILARITY_BM25);
    m.setBm25K1(k1);
    m.setBm25B(b);
    return this;
  }

  /**
   * Sets a per-field boost multiplier applied to BM25 contributions of field-qualified matches on that field.
   *
   * @param fieldName the field name
   * @param boost     the multiplier (> 1.0 increases relevance)
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withFieldBoost(final String fieldName, final float boost) {
    ftMetadata().setFieldBoost(fieldName, boost);
    return this;
  }
}
