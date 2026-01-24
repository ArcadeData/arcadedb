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
 * @author ArcadeDB Team
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
    ((FullTextIndexMetadata) metadata).setAnalyzerClass(analyzerClass);
    return this;
  }

  /**
   * Sets the analyzer class for indexing.
   *
   * @param analyzerClass the fully qualified class name of the Lucene analyzer for indexing
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withIndexAnalyzer(final String analyzerClass) {
    ((FullTextIndexMetadata) metadata).setIndexAnalyzerClass(analyzerClass);
    return this;
  }

  /**
   * Sets the analyzer class for querying.
   *
   * @param analyzerClass the fully qualified class name of the Lucene analyzer for querying
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withQueryAnalyzer(final String analyzerClass) {
    ((FullTextIndexMetadata) metadata).setQueryAnalyzerClass(analyzerClass);
    return this;
  }

  /**
   * Sets whether leading wildcards are allowed in queries.
   *
   * @param allow true to allow leading wildcards
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withAllowLeadingWildcard(final boolean allow) {
    ((FullTextIndexMetadata) metadata).setAllowLeadingWildcard(allow);
    return this;
  }

  /**
   * Sets the default boolean operator (AND or OR).
   *
   * @param operator "AND" or "OR"
   * @return this builder for chaining
   */
  public TypeFullTextIndexBuilder withDefaultOperator(final String operator) {
    ((FullTextIndexMetadata) metadata).setDefaultOperator(operator);
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
    ((FullTextIndexMetadata) metadata).setFieldAnalyzer(fieldName, analyzerClass);
    return this;
  }

  @Override
  public TypeFullTextIndexBuilder withMetadata(final IndexMetadata metadata) {
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
    ((FullTextIndexMetadata) metadata).fromJSON(json);
  }
}
