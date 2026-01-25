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
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.fulltext;

import com.arcadedb.serializer.json.JSONObject;

import java.util.Objects;

/**
 * Configuration for More Like This (MLT) queries. Holds all parameters that control
 * term selection and query generation for finding similar documents.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MoreLikeThisConfig {
  private int minTermFreq = 2;
  private int minDocFreq = 5;
  private Float maxDocFreqPercent = null;
  private int maxQueryTerms = 25;
  private int minWordLen = 0;
  private int maxWordLen = 0;
  private boolean boostByScore = true;
  private boolean excludeSource = true;
  private int maxSourceDocs = 25;

  /**
   * Creates a configuration with default values.
   */
  public MoreLikeThisConfig() {
  }

  /**
   * Creates a configuration from JSON parameters. Missing parameters use defaults.
   *
   * @param json JSON object with configuration parameters (can be null for defaults)
   * @return configuration instance
   */
  public static MoreLikeThisConfig fromJSON(final JSONObject json) {
    final MoreLikeThisConfig config = new MoreLikeThisConfig();

    if (json == null) {
      return config;
    }

    if (json.has("minTermFreq")) {
      config.minTermFreq = json.getInt("minTermFreq");
    }
    if (json.has("minDocFreq")) {
      config.minDocFreq = json.getInt("minDocFreq");
    }
    if (json.has("maxDocFreqPercent")) {
      final Object value = json.get("maxDocFreqPercent");
      if (value instanceof Number) {
        config.maxDocFreqPercent = ((Number) value).floatValue();
      }
    }
    if (json.has("maxQueryTerms")) {
      config.maxQueryTerms = json.getInt("maxQueryTerms");
    }
    if (json.has("minWordLen")) {
      config.minWordLen = json.getInt("minWordLen");
    }
    if (json.has("maxWordLen")) {
      config.maxWordLen = json.getInt("maxWordLen");
    }
    if (json.has("boostByScore")) {
      config.boostByScore = json.getBoolean("boostByScore");
    }
    if (json.has("excludeSource")) {
      config.excludeSource = json.getBoolean("excludeSource");
    }
    if (json.has("maxSourceDocs")) {
      config.maxSourceDocs = json.getInt("maxSourceDocs");
    }

    return config;
  }

  /**
   * Minimum term frequency in the source document(s) to consider a term.
   * Terms appearing less frequently are ignored.
   * Default: 2
   */
  public int getMinTermFreq() {
    return minTermFreq;
  }

  /**
   * Minimum document frequency across the index for a term to be considered.
   * Terms appearing in fewer documents are ignored (too rare).
   * Default: 5
   */
  public int getMinDocFreq() {
    return minDocFreq;
  }

  /**
   * Maximum document frequency as a percentage (0.0-1.0) of total documents.
   * Terms appearing in more documents are ignored (too common).
   * Null means no limit.
   * Default: null
   */
  public Float getMaxDocFreqPercent() {
    return maxDocFreqPercent;
  }

  /**
   * Maximum number of query terms to select from the source document(s).
   * Limits the complexity of the generated query.
   * Default: 25
   */
  public int getMaxQueryTerms() {
    return maxQueryTerms;
  }

  /**
   * Minimum word length to consider. Words shorter than this are ignored.
   * 0 means no minimum.
   * Default: 0
   */
  public int getMinWordLen() {
    return minWordLen;
  }

  /**
   * Maximum word length to consider. Words longer than this are ignored.
   * 0 means no maximum.
   * Default: 0
   */
  public int getMaxWordLen() {
    return maxWordLen;
  }

  /**
   * Whether to boost terms by their TF-IDF score in the source document.
   * When true, more important terms get higher weight in the query.
   * Default: true
   */
  public boolean isBoostByScore() {
    return boostByScore;
  }

  /**
   * Whether to exclude the source document(s) from results.
   * Default: true
   */
  public boolean isExcludeSource() {
    return excludeSource;
  }

  /**
   * Maximum number of source documents to analyze for term extraction.
   * Only applies when searching from multiple documents.
   * Default: 25
   */
  public int getMaxSourceDocs() {
    return maxSourceDocs;
  }

  public MoreLikeThisConfig setMinTermFreq(final int minTermFreq) {
    this.minTermFreq = minTermFreq;
    return this;
  }

  public MoreLikeThisConfig setMinDocFreq(final int minDocFreq) {
    this.minDocFreq = minDocFreq;
    return this;
  }

  public MoreLikeThisConfig setMaxDocFreqPercent(final Float maxDocFreqPercent) {
    this.maxDocFreqPercent = maxDocFreqPercent;
    return this;
  }

  public MoreLikeThisConfig setMaxQueryTerms(final int maxQueryTerms) {
    this.maxQueryTerms = maxQueryTerms;
    return this;
  }

  public MoreLikeThisConfig setMinWordLen(final int minWordLen) {
    this.minWordLen = minWordLen;
    return this;
  }

  public MoreLikeThisConfig setMaxWordLen(final int maxWordLen) {
    this.maxWordLen = maxWordLen;
    return this;
  }

  public MoreLikeThisConfig setBoostByScore(final boolean boostByScore) {
    this.boostByScore = boostByScore;
    return this;
  }

  public MoreLikeThisConfig setExcludeSource(final boolean excludeSource) {
    this.excludeSource = excludeSource;
    return this;
  }

  public MoreLikeThisConfig setMaxSourceDocs(final int maxSourceDocs) {
    this.maxSourceDocs = maxSourceDocs;
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final MoreLikeThisConfig that = (MoreLikeThisConfig) o;

    return minTermFreq == that.minTermFreq &&
           minDocFreq == that.minDocFreq &&
           maxQueryTerms == that.maxQueryTerms &&
           minWordLen == that.minWordLen &&
           maxWordLen == that.maxWordLen &&
           boostByScore == that.boostByScore &&
           excludeSource == that.excludeSource &&
           maxSourceDocs == that.maxSourceDocs &&
           Objects.equals(maxDocFreqPercent, that.maxDocFreqPercent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(minTermFreq, minDocFreq, maxDocFreqPercent, maxQueryTerms,
                        minWordLen, maxWordLen, boostByScore, excludeSource, maxSourceDocs);
  }
}
