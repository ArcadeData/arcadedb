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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FullTextSearchTest extends TestHelper {

  private void createArticles() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting'");
    });
  }

  @Test
  void searchReturnsScoredRids() {
    createArticles();

    database.transaction(() -> {
      final Map<RID, Float> hits = FullTextSearch.search(database, "Article[content]", "java");

      assertThat(hits).hasSize(1);
      assertThat(hits.values().iterator().next()).isGreaterThan(0f);
    });
  }

  @Test
  void resolveReturnsTypeIndex() {
    createArticles();

    database.transaction(() -> {
      final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Article[content]");

      assertThat(index.getName()).isEqualTo("Article[content]");
      assertThat(index.getTypeName()).isEqualTo("Article");
    });
  }

  @Test
  void similarityDefaultsToBM25() {
    createArticles();

    database.transaction(() -> {
      final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Article[content]");

      assertThat(FullTextSearch.getSimilarity(index)).isEqualTo(FullTextIndexMetadata.SIMILARITY_BM25);
    });
  }

  @Test
  void similarityReportsClassicWhenPinned() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Legacy");
      database.command("sql", "CREATE PROPERTY Legacy.txt STRING");
      database.command("sql", "CREATE INDEX ON Legacy (txt) FULL_TEXT METADATA {\"similarity\": \"CLASSIC\"}");
      database.command("sql", "INSERT INTO Legacy SET txt = 'hello world'");
    });

    database.transaction(() -> {
      final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Legacy[txt]");

      assertThat(FullTextSearch.getSimilarity(index)).isEqualTo(FullTextIndexMetadata.SIMILARITY_CLASSIC);
    });
  }

  @Test
  void listsOnlyFullTextIndexes() {
    createArticles();

    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON Article (title) UNIQUE");

      final List<String> indexes = FullTextSearch.listFullTextIndexes(database);

      assertThat(indexes).containsExactly("Article[content]");
    });
  }

  @Test
  void missingIndexThrowsSchemaException() {
    createArticles();

    database.transaction(() -> assertThatThrownBy(() -> FullTextSearch.resolveFullTextIndex(database, "Article[nope]"))
        .isInstanceOf(SchemaException.class));
  }

  @Test
  void nonFullTextIndexThrowsCommandExecutionException() {
    createArticles();

    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON Article (title) UNIQUE");

      assertThatThrownBy(() -> FullTextSearch.resolveFullTextIndex(database, "Article[title]"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("is not a full-text index");
    });
  }
}
