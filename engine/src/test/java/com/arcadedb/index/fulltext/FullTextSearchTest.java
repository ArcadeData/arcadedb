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

  private void createArticlesWithSharedTerm() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting language'");
      // Same length as Doc1/Doc2 (three tokens) so BM25 length normalization is identical across all three; Doc3
      // outranks the others purely on term frequency for 'language' (3 occurrences against 1).
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'language language language'");
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
  void databaseOverloadSearchesUnbounded() {
    createArticlesWithSharedTerm();

    database.transaction(() -> {
      final Map<RID, Float> hits = FullTextSearch.search(database, "Article[content]", "language");

      // All three articles contain "language"; the (Database, String, String) overload must still return every
      // match, exactly as before the limit-bounded overload was introduced.
      assertThat(hits).hasSize(3);
    });
  }

  @Test
  void boundedSearchLimitsResultsWithoutLosingTopHit() {
    createArticlesWithSharedTerm();

    database.transaction(() -> {
      final TypeIndex typeIndex = FullTextSearch.resolveFullTextIndex(database, "Article[content]");

      final Map<RID, Float> unbounded = FullTextSearch.search(typeIndex, "language", -1);
      assertThat(unbounded).hasSize(3);

      final Map<RID, Float> bounded = FullTextSearch.search(typeIndex, "language", 1);
      // The single bucket's bounded min-heap keeps only the requested number of entries.
      assertThat(bounded).hasSize(1);

      final RID bestUnbounded = bestScoring(unbounded);
      final RID bestBounded = bestScoring(bounded);
      // Pushing the limit down to the bucket cursor must not lose the best-scoring match.
      assertThat(bestBounded).isEqualTo(bestUnbounded);
    });
  }

  private static RID bestScoring(final Map<RID, Float> hits) {
    RID best = null;
    float bestScore = Float.NEGATIVE_INFINITY;
    for (final Map.Entry<RID, Float> entry : hits.entrySet()) {
      if (entry.getValue() > bestScore) {
        bestScore = entry.getValue();
        best = entry.getKey();
      }
    }
    return best;
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

  @Test
  void bucketSubIndexThrowsCommandExecutionException() {
    createArticles();

    database.transaction(() -> {
      final TypeIndex typeIndex = FullTextSearch.resolveFullTextIndex(database, "Article[content]");
      // Bucket-level sub-indexes are registered in the schema's index map under their own name
      // (distinct from the TypeIndex wrapper's name), so resolving one directly must be rejected.
      final String bucketIndexName = typeIndex.getIndexesOnBuckets()[0].getName();

      assertThatThrownBy(() -> FullTextSearch.resolveFullTextIndex(database, bucketIndexName))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("is not a type index");
    });
  }
}
