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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for backward compatibility of full-text index features.
 * Verifies that existing/legacy behavior continues to work after new features are added.
 */
class FullTextBackwardCompatIT extends TestHelper {

  @Test
  void containsTextStillWorks() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming tutorial'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting guide'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'java database integration'");
    });

    database.transaction(() -> {
      // CONTAINSTEXT should still work as before
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE content CONTAINSTEXT 'java'");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Both Doc1 and Doc3 contain "java"
      assertThat(titles).containsExactlyInAnyOrder("Doc1", "Doc3");
    });
  }

  @Test
  void indexWithoutMetadataUsesDefaults() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      // Create index without any METADATA clause - should use defaults
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming'");
    });

    database.transaction(() -> {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      assertThat(typeIndex).isNotNull();
      assertThat(typeIndex.getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);

      final LSMTreeFullTextIndex index = (LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0];

      // Verify default analyzer is StandardAnalyzer
      assertThat(index.getAnalyzer()).isInstanceOf(StandardAnalyzer.class);

      // Metadata may be null for indexes without explicit configuration
      // If metadata exists, verify defaults
      if (index.getFullTextMetadata() != null) {
        // Default operator should be OR (not AND)
        assertThat(index.getFullTextMetadata().getDefaultOperator()).isIn(null, "OR");
        // Leading wildcards should be disabled by default
        assertThat(index.getFullTextMetadata().isAllowLeadingWildcard()).isFalse();
      }

      // Search should work with OR semantics (default)
      // "java python" should return Doc1 because it contains "java" (OR match)
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'java python') = true");

      assertThat(result.hasNext()).isTrue();
      final Result r = result.next();
      assertThat(r.<String>getProperty("title")).isEqualTo("Doc1");
    });
  }

  @Test
  void searchIndexOnLegacyIndexWorks() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      // Create "legacy" style index - simple FULL_TEXT without metadata
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language tutorial'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting guide basics'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'java database integration howto'");
    });

    database.transaction(() -> {
      // SEARCH_INDEX function should work on legacy indexes

      // Basic term search
      final ResultSet basicResult = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'java') = true");

      final Set<String> basicTitles = new HashSet<>();
      while (basicResult.hasNext()) {
        basicTitles.add(basicResult.next().getProperty("title"));
      }
      assertThat(basicTitles).containsExactlyInAnyOrder("Doc1", "Doc3");

      // Boolean query with MUST
      final ResultSet booleanResult = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', '+java +database') = true");

      final Set<String> booleanTitles = new HashSet<>();
      while (booleanResult.hasNext()) {
        booleanTitles.add(booleanResult.next().getProperty("title"));
      }
      assertThat(booleanTitles).containsExactly("Doc3");

      // $score should be available
      final ResultSet scoreResult = database.query("sql",
          "SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[content]', 'java') = true ORDER BY $score DESC");

      int count = 0;
      while (scoreResult.hasNext()) {
        final Result r = scoreResult.next();
        final Float score = r.getProperty("$score");
        assertThat(score).isNotNull();
        assertThat(score).isGreaterThan(0f);
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }
}
