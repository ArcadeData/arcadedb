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

import com.arcadedb.serializer.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Metadata class for full-text indexes, storing Lucene analyzer configuration.
 * <p>
 * Supports configuring:
 * <ul>
 *   <li>Default analyzer for all fields</li>
 *   <li>Separate analyzers for indexing and querying</li>
 *   <li>Per-field analyzer overrides</li>
 *   <li>Query parser options (leading wildcard, default operator)</li>
 * </ul>
 *
 * @author ArcadeDB Team
 */
public class FullTextIndexMetadata extends IndexMetadata {

  /**
   * Default analyzer class - Lucene's StandardAnalyzer.
   */
  public static final String DEFAULT_ANALYZER = "org.apache.lucene.analysis.standard.StandardAnalyzer";

  private static final String ANALYZER_SUFFIX = "_analyzer";

  private String              analyzerClass        = DEFAULT_ANALYZER;
  private String              indexAnalyzerClass   = null;
  private String              queryAnalyzerClass   = null;
  private boolean             allowLeadingWildcard = false;
  private String              defaultOperator      = "OR";
  private Map<String, String> fieldAnalyzers       = new HashMap<>();

  /**
   * Creates a new FullTextIndexMetadata instance.
   *
   * @param typeName      the name of the type this index belongs to
   * @param propertyNames the property names indexed
   * @param bucketId      the associated bucket ID
   */
  public FullTextIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  @Override
  public void fromJSON(final JSONObject metadata) {
    super.fromJSON(metadata);

    if (metadata.has("analyzer"))
      this.analyzerClass = metadata.getString("analyzer");

    if (metadata.has("index_analyzer"))
      this.indexAnalyzerClass = metadata.getString("index_analyzer");

    if (metadata.has("query_analyzer"))
      this.queryAnalyzerClass = metadata.getString("query_analyzer");

    if (metadata.has("allowLeadingWildcard"))
      this.allowLeadingWildcard = metadata.getBoolean("allowLeadingWildcard");

    if (metadata.has("defaultOperator"))
      this.defaultOperator = metadata.getString("defaultOperator");

    // Parse per-field analyzers (pattern: *_analyzer)
    for (final String key : metadata.keySet()) {
      if (key.endsWith(ANALYZER_SUFFIX) && !key.equals("analyzer") && !key.equals("index_analyzer") && !key.equals("query_analyzer")) {
        final String fieldName = key.substring(0, key.length() - ANALYZER_SUFFIX.length());
        this.fieldAnalyzers.put(fieldName, metadata.getString(key));
      }
    }
  }

  /**
   * Returns the default analyzer class.
   *
   * @return the analyzer class name
   */
  public String getAnalyzerClass() {
    return analyzerClass;
  }

  /**
   * Returns the analyzer class for a specific field.
   * If a field-specific analyzer is configured, returns that; otherwise returns the default analyzer.
   *
   * @param fieldName the field name
   * @return the analyzer class name for the field
   */
  public String getAnalyzerClass(final String fieldName) {
    return fieldAnalyzers.getOrDefault(fieldName, analyzerClass);
  }

  /**
   * Returns the analyzer class for indexing.
   * If a specific index analyzer is configured, returns that; otherwise returns the default analyzer.
   *
   * @return the index analyzer class name
   */
  public String getIndexAnalyzerClass() {
    return indexAnalyzerClass != null ? indexAnalyzerClass : analyzerClass;
  }

  /**
   * Returns the analyzer class for querying.
   * If a specific query analyzer is configured, returns that; otherwise returns the default analyzer.
   *
   * @return the query analyzer class name
   */
  public String getQueryAnalyzerClass() {
    return queryAnalyzerClass != null ? queryAnalyzerClass : analyzerClass;
  }

  /**
   * Returns whether leading wildcards are allowed in queries.
   *
   * @return true if leading wildcards are allowed
   */
  public boolean isAllowLeadingWildcard() {
    return allowLeadingWildcard;
  }

  /**
   * Returns the default operator for query parsing.
   *
   * @return "OR" or "AND"
   */
  public String getDefaultOperator() {
    return defaultOperator;
  }

  /**
   * Returns an unmodifiable view of the per-field analyzer map.
   *
   * @return map of field name to analyzer class name
   */
  public Map<String, String> getFieldAnalyzers() {
    return Collections.unmodifiableMap(fieldAnalyzers);
  }

  /**
   * Sets the default analyzer class.
   *
   * @param analyzerClass the analyzer class name
   */
  public void setAnalyzerClass(final String analyzerClass) {
    this.analyzerClass = analyzerClass;
  }

  /**
   * Sets the index analyzer class.
   *
   * @param indexAnalyzerClass the analyzer class name for indexing, or null to use default
   */
  public void setIndexAnalyzerClass(final String indexAnalyzerClass) {
    this.indexAnalyzerClass = indexAnalyzerClass;
  }

  /**
   * Sets the query analyzer class.
   *
   * @param queryAnalyzerClass the analyzer class name for querying, or null to use default
   */
  public void setQueryAnalyzerClass(final String queryAnalyzerClass) {
    this.queryAnalyzerClass = queryAnalyzerClass;
  }

  /**
   * Sets whether leading wildcards are allowed.
   *
   * @param allowLeadingWildcard true to allow leading wildcards
   */
  public void setAllowLeadingWildcard(final boolean allowLeadingWildcard) {
    this.allowLeadingWildcard = allowLeadingWildcard;
  }

  /**
   * Sets the default operator for query parsing.
   *
   * @param defaultOperator "OR" or "AND"
   */
  public void setDefaultOperator(final String defaultOperator) {
    this.defaultOperator = defaultOperator;
  }

  /**
   * Sets an analyzer for a specific field.
   *
   * @param fieldName     the field name
   * @param analyzerClass the analyzer class name
   */
  public void setFieldAnalyzer(final String fieldName, final String analyzerClass) {
    this.fieldAnalyzers.put(fieldName, analyzerClass);
  }
}
