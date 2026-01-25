/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for full-text index analyzer configuration.
 */
class FullTextAnalyzerConfigTest extends TestHelper {

  @Test
  void defaultAnalyzerIsStandard() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      assertThat(index).isNotNull();
      assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);

      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      assertThat(ftIndex.getAnalyzer()).isInstanceOf(StandardAnalyzer.class);
    });
  }

  @Test
  void englishAnalyzerStemmingWorks() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql",
          """
              CREATE INDEX ON Article (content) FULL_TEXT METADATA {
              "analyzer": "org.apache.lucene.analysis.en.EnglishAnalyzer"}
              """);

      // Insert document with "running"
      database.command("sql", "INSERT INTO Article SET content = 'I am running in the park'");
    });

    database.transaction(() -> {
      // Search for "run" should match "running" due to stemming
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', 'run') = true");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext()) {
        results.add(result.next());
      }

      assertThat(results).hasSize(1);
      assertThat(results.get(0).getProperty("content").toString()).contains("running");
    });
  }

  @Test
  void perFieldAnalyzersApplied() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      // title uses EnglishAnalyzer (stemming), body uses StandardAnalyzer (no stemming)
      database.command("sql",
          """
              CREATE INDEX ON Article (title, body) FULL_TEXT METADATA {
              "analyzer": "org.apache.lucene.analysis.standard.StandardAnalyzer",
              "title_analyzer": "org.apache.lucene.analysis.en.EnglishAnalyzer"}""");

      // Insert document where stemming matters
      database.command("sql", "INSERT INTO Article SET title = 'Running Fast', body = 'running slow'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[title,body]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];

      // Verify the index was created with the expected analyzers
      assertThat(ftIndex.getFullTextMetadata()).isNotNull();
      assertThat(ftIndex.getFullTextMetadata().getAnalyzerClass("title"))
          .isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
      assertThat(ftIndex.getFullTextMetadata().getAnalyzerClass("body"))
          .isEqualTo("org.apache.lucene.analysis.standard.StandardAnalyzer");
    });
  }

  @Test
  void allowLeadingWildcardEnabled() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql",
          """
              CREATE INDEX ON Article (content) FULL_TEXT METADATA {"allowLeadingWildcard": true}""");

      database.command("sql", "INSERT INTO Article SET content = 'database management system'");
    });

    database.transaction(() -> {
      // Leading wildcard query parses without exception when enabled
      // Note: Current LSM-Tree implementation strips wildcards, so *base becomes "base"
      // which won't match "database". Full wildcard iteration is a future enhancement.
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', '*base') = true");

      // Query should execute without throwing (unlike when disabled)
      final List<Result> results = new ArrayList<>();
      while (result.hasNext()) {
        results.add(result.next());
      }
      // Results may be empty due to LSM-Tree wildcard limitations
      assertThat(results).isEmpty();
    });
  }

  @Test
  void allowLeadingWildcardDisabledByDefault() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'database management system'");
    });

    // Leading wildcard should throw exception when disabled (default)
    assertThatThrownBy(() -> database.transaction(() -> {
      database.query("sql",
          "SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', '*base') = true");
    })).isInstanceOf(IndexException.class)
        .hasMessageContaining("Invalid search query");
  }

  @Test
  void defaultOperatorAND() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql",
          """
              CREATE INDEX ON Article (content) FULL_TEXT METADATA {"defaultOperator": "AND"}""");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'programming tutorial'");
    });

    database.transaction(() -> {
      // With AND operator, "java programming" requires BOTH terms
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', 'java programming') = true");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext()) {
        results.add(result.next());
      }

      // Only Doc1 has both "java" AND "programming"
      assertThat(results).hasSize(1);
      assertThat(results.get(0).getProperty("content").toString()).contains("java programming");
    });
  }

  @Test
  void defaultOperatorOR() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      // Default is OR, but let's be explicit
      database.command("sql",
          """
              CREATE INDEX ON Article (content) FULL_TEXT METADATA {"defaultOperator": "OR"}""");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'programming tutorial'");
    });

    database.transaction(() -> {
      // With OR operator, "java programming" matches EITHER term
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', 'java programming') = true");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext()) {
        results.add(result.next());
      }

      // All 3 docs match: Doc1 has both, Doc2 has java, Doc3 has programming
      assertThat(results).hasSize(3);
    });
  }
}
