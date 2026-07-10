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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FullTextIndexMetadata}.
 */
class FullTextIndexMetadataTest {

  @Test
  void defaultValues() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("TestType", new String[] { "content" }, 0);

    assertThat(metadata.getAnalyzerClass()).isEqualTo(FullTextIndexMetadata.DEFAULT_ANALYZER);
    assertThat(metadata.getIndexAnalyzerClass()).isEqualTo(FullTextIndexMetadata.DEFAULT_ANALYZER);
    assertThat(metadata.getQueryAnalyzerClass()).isEqualTo(FullTextIndexMetadata.DEFAULT_ANALYZER);
    assertThat(metadata.isAllowLeadingWildcard()).isFalse();
    assertThat(metadata.getDefaultOperator()).isEqualTo("OR");
    assertThat(metadata.getFieldAnalyzers()).isEmpty();
  }

  @Test
  void writeToJSONOmitsDefaultBM25ParametersButKeepsTunedOnes() {
    // Default BM25 index: k1/b are not emitted (they read back as the defaults), keeping the schema JSON terse.
    final FullTextIndexMetadata defaults = new FullTextIndexMetadata("TestType", new String[] { "content" }, 0);
    final JSONObject defaultJson = defaults.writeToJSON(new JSONObject());
    assertThat(defaultJson.getString("similarity", null)).isEqualTo(FullTextIndexMetadata.SIMILARITY_BM25);
    assertThat(defaultJson.has("bm25_k1")).isFalse();
    assertThat(defaultJson.has("bm25_b")).isFalse();

    // Tuned values are emitted so they survive a round-trip.
    final FullTextIndexMetadata tuned = new FullTextIndexMetadata("TestType", new String[] { "content" }, 0);
    tuned.setBm25K1(1.7f);
    tuned.setBm25B(0.3f);
    final JSONObject tunedJson = tuned.writeToJSON(new JSONObject());
    assertThat(tunedJson.getFloat("bm25_k1")).isEqualTo(1.7f);
    assertThat(tunedJson.getFloat("bm25_b")).isEqualTo(0.3f);

    // A reload of the terse JSON restores the defaults.
    final FullTextIndexMetadata reloaded = new FullTextIndexMetadata("TestType", new String[] { "content" }, 0);
    reloaded.fromJSON(defaultJson);
    assertThat(reloaded.getBm25K1()).isEqualTo(FullTextIndexMetadata.DEFAULT_BM25_K1);
    assertThat(reloaded.getBm25B()).isEqualTo(FullTextIndexMetadata.DEFAULT_BM25_B);
  }

  @Test
  void fromJSONReplacesStalePerFieldConfig() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Article", new String[] { "title", "body" }, 0);
    metadata.setFieldAnalyzer("title", "org.apache.lucene.analysis.en.EnglishAnalyzer");
    metadata.setFieldBoost("title", 3.0f);

    // A second fromJSON (e.g. a schema reload onto a reused instance) must REPLACE the per-field config, not merge stale entries.
    final JSONObject json = new JSONObject();
    json.put("similarity", "BM25");
    json.put("body_boost", 2.0f);
    metadata.fromJSON(json);

    assertThat(metadata.getFieldBoost("title")).isEqualTo(1.0f); // stale title boost cleared
    assertThat(metadata.getFieldBoost("body")).isEqualTo(2.0f);  // new boost applied
    assertThat(metadata.getAnalyzerClass("title")).isEqualTo(metadata.getAnalyzerClass()); // stale title analyzer cleared -> default
  }

  @Test
  void setSimilarityRejectsNull() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Article", new String[] { "title" }, 0);
    assertThatThrownBy(() -> metadata.setSimilarity(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null");
  }

  @Test
  void fromJSONWithAnalyzer() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("TestType", new String[] { "content" }, 0);

    final JSONObject json = new JSONObject();
    json.put("typeName", "TestType");
    json.put("properties", new String[] { "content" });
    json.put("associatedBucketId", 0);
    json.put("analyzer", "org.apache.lucene.analysis.en.EnglishAnalyzer");

    metadata.fromJSON(json);

    assertThat(metadata.getAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
    assertThat(metadata.getIndexAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
    assertThat(metadata.getQueryAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
  }

  @Test
  void fromJSONWithSeparateIndexAndQueryAnalyzers() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("TestType", new String[] { "content" }, 0);

    final JSONObject json = new JSONObject();
    json.put("typeName", "TestType");
    json.put("properties", new String[] { "content" });
    json.put("associatedBucketId", 0);
    json.put("analyzer", "org.apache.lucene.analysis.standard.StandardAnalyzer");
    json.put("index_analyzer", "org.apache.lucene.analysis.core.WhitespaceAnalyzer");
    json.put("query_analyzer", "org.apache.lucene.analysis.core.SimpleAnalyzer");

    metadata.fromJSON(json);

    assertThat(metadata.getAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.standard.StandardAnalyzer");
    assertThat(metadata.getIndexAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.core.WhitespaceAnalyzer");
    assertThat(metadata.getQueryAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.core.SimpleAnalyzer");
  }

  @Test
  void fromJSONWithAllowLeadingWildcard() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("TestType", new String[] { "content" }, 0);

    final JSONObject json = new JSONObject();
    json.put("typeName", "TestType");
    json.put("properties", new String[] { "content" });
    json.put("associatedBucketId", 0);
    json.put("allowLeadingWildcard", true);

    metadata.fromJSON(json);

    assertThat(metadata.isAllowLeadingWildcard()).isTrue();
  }

  @Test
  void fromJSONWithDefaultOperator() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("TestType", new String[] { "content" }, 0);

    final JSONObject json = new JSONObject();
    json.put("typeName", "TestType");
    json.put("properties", new String[] { "content" });
    json.put("associatedBucketId", 0);
    json.put("defaultOperator", "AND");

    metadata.fromJSON(json);

    assertThat(metadata.getDefaultOperator()).isEqualTo("AND");
  }

  @Test
  void fromJSONWithFieldAnalyzers() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("TestType", new String[] { "title", "body" }, 0);

    final JSONObject json = new JSONObject();
    json.put("typeName", "TestType");
    json.put("properties", new String[] { "title", "body" });
    json.put("associatedBucketId", 0);
    json.put("title_analyzer", "org.apache.lucene.analysis.core.KeywordAnalyzer");
    json.put("body_analyzer", "org.apache.lucene.analysis.en.EnglishAnalyzer");

    metadata.fromJSON(json);

    assertThat(metadata.getFieldAnalyzers()).hasSize(2);
    assertThat(metadata.getFieldAnalyzers().get("title")).isEqualTo("org.apache.lucene.analysis.core.KeywordAnalyzer");
    assertThat(metadata.getFieldAnalyzers().get("body")).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
  }

  @Test
  void getAnalyzerClassForField() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("TestType", new String[] { "title", "body" }, 0);

    final JSONObject json = new JSONObject();
    json.put("typeName", "TestType");
    json.put("properties", new String[] { "title", "body" });
    json.put("associatedBucketId", 0);
    json.put("analyzer", "org.apache.lucene.analysis.standard.StandardAnalyzer");
    json.put("title_analyzer", "org.apache.lucene.analysis.core.KeywordAnalyzer");

    metadata.fromJSON(json);

    // Field with specific analyzer
    assertThat(metadata.getAnalyzerClass("title")).isEqualTo("org.apache.lucene.analysis.core.KeywordAnalyzer");
    // Field without specific analyzer - falls back to default
    assertThat(metadata.getAnalyzerClass("body")).isEqualTo("org.apache.lucene.analysis.standard.StandardAnalyzer");
    // Unknown field - falls back to default
    assertThat(metadata.getAnalyzerClass("unknown")).isEqualTo("org.apache.lucene.analysis.standard.StandardAnalyzer");
  }

  @Test
  void fromJSONWithAllOptions() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("TestType", new String[] { "title", "body" }, 0);

    final JSONObject json = new JSONObject();
    json.put("typeName", "TestType");
    json.put("properties", new String[] { "title", "body" });
    json.put("associatedBucketId", 0);
    json.put("analyzer", "org.apache.lucene.analysis.en.EnglishAnalyzer");
    json.put("index_analyzer", "org.apache.lucene.analysis.core.WhitespaceAnalyzer");
    json.put("query_analyzer", "org.apache.lucene.analysis.core.SimpleAnalyzer");
    json.put("allowLeadingWildcard", true);
    json.put("defaultOperator", "AND");
    json.put("title_analyzer", "org.apache.lucene.analysis.core.KeywordAnalyzer");

    metadata.fromJSON(json);

    assertThat(metadata.getAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
    assertThat(metadata.getIndexAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.core.WhitespaceAnalyzer");
    assertThat(metadata.getQueryAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.core.SimpleAnalyzer");
    assertThat(metadata.isAllowLeadingWildcard()).isTrue();
    assertThat(metadata.getDefaultOperator()).isEqualTo("AND");
    assertThat(metadata.getFieldAnalyzers()).hasSize(1);
    assertThat(metadata.getAnalyzerClass("title")).isEqualTo("org.apache.lucene.analysis.core.KeywordAnalyzer");
  }
}
