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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class TypeFullTextIndexBuilderTest extends TestHelper {

  @Test
  void withMetadataFromJSON() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
      database.getSchema().getType("Article").createProperty("body", String.class);
    });

    database.transaction(() -> {
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Article", new String[]{"title"});
      final TypeFullTextIndexBuilder ftBuilder = builder.withType(Schema.INDEX_TYPE.FULL_TEXT).withFullTextType();

      final JSONObject json = new JSONObject();
      json.put("analyzer", "org.apache.lucene.analysis.en.EnglishAnalyzer");
      ftBuilder.withMetadata(json);

      assertThat(ftBuilder).isNotNull();
      assertThat(((FullTextIndexMetadata) ftBuilder.metadata).getAnalyzerClass())
          .isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
    });
  }

  @Test
  void withTypeReturnsFullTextBuilder() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
    });

    database.transaction(() -> {
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Article", new String[]{"title"});
      final TypeIndexBuilder result = builder.withType(Schema.INDEX_TYPE.FULL_TEXT);

      assertThat(result).isInstanceOf(TypeFullTextIndexBuilder.class);
    });
  }

  @Test
  void withFullTextTypeThrowsIfNotFullTextType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
    });

    database.transaction(() -> {
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Article", new String[]{"title"});
      // Don't call withType(FULL_TEXT)
      assertThatThrownBy(builder::withFullTextType)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("withFullTextType() can only be called after withType(FULL_TEXT)");
    });
  }

  @Test
  void withAnalyzerSetsAnalyzerClass() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
    });

    database.transaction(() -> {
      final TypeFullTextIndexBuilder ftBuilder = (TypeFullTextIndexBuilder) database.getSchema()
          .buildTypeIndex("Article", new String[]{"title"})
          .withType(Schema.INDEX_TYPE.FULL_TEXT);

      ftBuilder.withAnalyzer("org.apache.lucene.analysis.de.GermanAnalyzer");

      final FullTextIndexMetadata meta = (FullTextIndexMetadata) ftBuilder.metadata;
      assertThat(meta.getAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.de.GermanAnalyzer");
    });
  }

  @Test
  void withIndexAndQueryAnalyzers() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
    });

    database.transaction(() -> {
      final TypeFullTextIndexBuilder ftBuilder = (TypeFullTextIndexBuilder) database.getSchema()
          .buildTypeIndex("Article", new String[]{"title"})
          .withType(Schema.INDEX_TYPE.FULL_TEXT);

      ftBuilder.withIndexAnalyzer("org.apache.lucene.analysis.en.EnglishAnalyzer")
               .withQueryAnalyzer("org.apache.lucene.analysis.standard.StandardAnalyzer");

      final FullTextIndexMetadata meta = (FullTextIndexMetadata) ftBuilder.metadata;
      assertThat(meta.getIndexAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
      assertThat(meta.getQueryAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.standard.StandardAnalyzer");
    });
  }

  @Test
  void withAllowLeadingWildcard() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
    });

    database.transaction(() -> {
      final TypeFullTextIndexBuilder ftBuilder = (TypeFullTextIndexBuilder) database.getSchema()
          .buildTypeIndex("Article", new String[]{"title"})
          .withType(Schema.INDEX_TYPE.FULL_TEXT);

      ftBuilder.withAllowLeadingWildcard(true);

      final FullTextIndexMetadata meta = (FullTextIndexMetadata) ftBuilder.metadata;
      assertThat(meta.isAllowLeadingWildcard()).isTrue();
    });
  }

  @Test
  void withDefaultOperator() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
    });

    database.transaction(() -> {
      final TypeFullTextIndexBuilder ftBuilder = (TypeFullTextIndexBuilder) database.getSchema()
          .buildTypeIndex("Article", new String[]{"title"})
          .withType(Schema.INDEX_TYPE.FULL_TEXT);

      ftBuilder.withDefaultOperator("AND");

      final FullTextIndexMetadata meta = (FullTextIndexMetadata) ftBuilder.metadata;
      assertThat(meta.getDefaultOperator()).isEqualTo("AND");
    });
  }

  @Test
  void withFieldAnalyzer() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
      database.getSchema().getType("Article").createProperty("body", String.class);
    });

    database.transaction(() -> {
      final TypeFullTextIndexBuilder ftBuilder = (TypeFullTextIndexBuilder) database.getSchema()
          .buildTypeIndex("Article", new String[]{"title"})
          .withType(Schema.INDEX_TYPE.FULL_TEXT);

      ftBuilder.withFieldAnalyzer("title", "org.apache.lucene.analysis.en.EnglishAnalyzer")
               .withFieldAnalyzer("body", "org.apache.lucene.analysis.de.GermanAnalyzer");

      final FullTextIndexMetadata meta = (FullTextIndexMetadata) ftBuilder.metadata;
      assertThat(meta.getAnalyzerClass("title")).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
      assertThat(meta.getAnalyzerClass("body")).isEqualTo("org.apache.lucene.analysis.de.GermanAnalyzer");
    });
  }

  @Test
  void withMetadataFromJSONWithAllOptions() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
      database.getSchema().getType("Article").createProperty("body", String.class);
    });

    database.transaction(() -> {
      final TypeFullTextIndexBuilder ftBuilder = (TypeFullTextIndexBuilder) database.getSchema()
          .buildTypeIndex("Article", new String[]{"title"})
          .withType(Schema.INDEX_TYPE.FULL_TEXT);

      final JSONObject json = new JSONObject();
      json.put("analyzer", "org.apache.lucene.analysis.en.EnglishAnalyzer");
      json.put("index_analyzer", "org.apache.lucene.analysis.standard.StandardAnalyzer");
      json.put("query_analyzer", "org.apache.lucene.analysis.core.KeywordAnalyzer");
      json.put("allowLeadingWildcard", true);
      json.put("defaultOperator", "AND");
      json.put("title_analyzer", "org.apache.lucene.analysis.de.GermanAnalyzer");

      ftBuilder.withMetadata(json);

      final FullTextIndexMetadata meta = (FullTextIndexMetadata) ftBuilder.metadata;
      assertThat(meta.getAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
      assertThat(meta.getIndexAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.standard.StandardAnalyzer");
      assertThat(meta.getQueryAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.core.KeywordAnalyzer");
      assertThat(meta.isAllowLeadingWildcard()).isTrue();
      assertThat(meta.getDefaultOperator()).isEqualTo("AND");
      assertThat(meta.getAnalyzerClass("title")).isEqualTo("org.apache.lucene.analysis.de.GermanAnalyzer");
    });
  }

  @Test
  void builderChainingWorks() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
    });

    database.transaction(() -> {
      final TypeFullTextIndexBuilder ftBuilder = (TypeFullTextIndexBuilder) database.getSchema()
          .buildTypeIndex("Article", new String[]{"title"})
          .withType(Schema.INDEX_TYPE.FULL_TEXT);

      // Test fluent API chaining
      TypeFullTextIndexBuilder result = ftBuilder
          .withAnalyzer("org.apache.lucene.analysis.en.EnglishAnalyzer")
          .withAllowLeadingWildcard(true)
          .withDefaultOperator("AND");

      assertThat(result).isSameAs(ftBuilder);
    });
  }

  @Test
  void withBM25AndFieldBoostSetMetadata() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
      database.getSchema().getType("Article").createProperty("body", String.class);
    });

    database.transaction(() -> {
      final TypeFullTextIndexBuilder ftBuilder = (TypeFullTextIndexBuilder) database.getSchema()
          .buildTypeIndex("Article", new String[] { "title", "body" })
          .withType(Schema.INDEX_TYPE.FULL_TEXT);

      final TypeFullTextIndexBuilder result = ftBuilder.withBM25(1.4f, 0.55f).withFieldBoost("title", 3.0f);
      assertThat(result).isSameAs(ftBuilder); // fluent chaining

      final FullTextIndexMetadata meta = (FullTextIndexMetadata) ftBuilder.metadata;
      assertThat(meta.getSimilarity()).isEqualTo(FullTextIndexMetadata.SIMILARITY_BM25);
      assertThat(meta.getBm25K1()).isCloseTo(1.4f, within(1e-6f));
      assertThat(meta.getBm25B()).isCloseTo(0.55f, within(1e-6f));
      assertThat(meta.getFieldBoost("title")).isCloseTo(3.0f, within(1e-6f));
      assertThat(meta.getFieldBoost("body")).isCloseTo(1.0f, within(1e-6f)); // unset field defaults to 1.0
    });
  }

  @Test
  void builderCreatedBM25IndexRanksAndBoosts() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("name", String.class);
      database.getSchema().getType("Article").createProperty("title", String.class);
      database.getSchema().getType("Article").createProperty("body", String.class);

      // Create the index entirely through the Java builder API (no raw JSON metadata).
      database.getSchema().buildTypeIndex("Article", new String[] { "title", "body" })
          .withType(Schema.INDEX_TYPE.FULL_TEXT).withFullTextType()
          .withBM25(1.2f, 0.75f).withFieldBoost("title", 5.0f).create();

      database.command("sql", "INSERT INTO Article SET name = 'inTitle', title = 'java', body = 'something else entirely'");
      database.command("sql", "INSERT INTO Article SET name = 'inBody', title = 'something else', body = 'java'");
    });

    database.transaction(() -> {
      final Map<String, Float> titleScores = new HashMap<>();
      final ResultSet ts = database.query("sql",
          "SELECT name, $score FROM Article WHERE SEARCH_INDEX('Article[title,body]', 'title:java') = true");
      while (ts.hasNext()) {
        final Result r = ts.next();
        titleScores.put(r.getProperty("name"), ((Number) r.getProperty("$score")).floatValue());
      }

      final Map<String, Float> bodyScores = new HashMap<>();
      final ResultSet bs = database.query("sql",
          "SELECT name, $score FROM Article WHERE SEARCH_INDEX('Article[title,body]', 'body:java') = true");
      while (bs.hasNext()) {
        final Result r = bs.next();
        bodyScores.put(r.getProperty("name"), ((Number) r.getProperty("$score")).floatValue());
      }

      // The builder-configured title_boost=5.0 makes a title match outrank the same term in the unboosted body.
      assertThat(titleScores.get("inTitle")).isNotNull();
      assertThat(bodyScores.get("inBody")).isNotNull();
      assertThat(titleScores.get("inTitle")).isGreaterThan(bodyScores.get("inBody"));
    });
  }

  @Test
  void defaultValuesAreCorrect() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article");
      database.getSchema().getType("Article").createProperty("title", String.class);
    });

    database.transaction(() -> {
      final TypeFullTextIndexBuilder ftBuilder = (TypeFullTextIndexBuilder) database.getSchema()
          .buildTypeIndex("Article", new String[]{"title"})
          .withType(Schema.INDEX_TYPE.FULL_TEXT);

      final FullTextIndexMetadata meta = (FullTextIndexMetadata) ftBuilder.metadata;

      // Check defaults
      assertThat(meta.getAnalyzerClass()).isEqualTo(FullTextIndexMetadata.DEFAULT_ANALYZER);
      assertThat(meta.isAllowLeadingWildcard()).isFalse();
      assertThat(meta.getDefaultOperator()).isEqualTo("OR");
    });
  }
}
