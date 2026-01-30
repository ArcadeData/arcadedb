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
