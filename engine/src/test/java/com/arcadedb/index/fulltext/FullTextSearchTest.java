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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  @Test
  void zeroOrNegativeLimitOtherThanMinusOneIsTreatedAsUnbounded() {
    createArticlesWithSharedTerm();

    database.transaction(() -> {
      final TypeIndex typeIndex = FullTextSearch.resolveFullTextIndex(database, "Article[content]");

      // limit == 0 must not reach the BM25 bounded min-heap (whose PriorityQueue constructor rejects a 0 initial
      // capacity) nor silently mean "no limit enforced by accident"; both 0 and an arbitrary negative value other
      // than -1 are normalized to unbounded, exactly like passing -1 directly.
      assertThat(FullTextSearch.search(typeIndex, "language", 0)).hasSize(3);
      assertThat(FullTextSearch.search(typeIndex, "language", -5)).hasSize(3);
    });
  }

  @Test
  void boundedSearchMatchesUnboundedTopKAcrossBuckets() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Multi BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Multi.content STRING");
      database.command("sql", "CREATE INDEX ON Multi (content) FULL_TEXT");

      // Round-robin bucket selection sends insert i to bucket index (i % 4) of this type's own bucket list, so every
      // 4th insert lands back in bucket 0. Placing the 4 "strong" documents (decreasing but still very high term
      // frequency for 'target') at insert positions 0, 4, 8 and 12 concentrates all of them in bucket 0, while the
      // remaining 12 "weak" documents (term frequency 1) round-robin through every bucket, including bucket 0. This
      // makes bucket 0's own top-k a strict subset of its matches, and the other buckets' matches uniformly weak -
      // the one case where a per-bucket top-k push-down could silently drop a globally-top hit if it were unsound.
      // The bucket-id assertion below confirms the actual, resulting distribution rather than assuming it.
      final int[] strongRepeats = { 20, 18, 16, 14 };
      int strongIndex = 0;
      for (int i = 0; i < 16; i++) {
        if (i % 4 == 0)
          database.command("sql", "INSERT INTO Multi SET content = '" + "target ".repeat(strongRepeats[strongIndex++]).trim() + "'");
        else
          database.command("sql", "INSERT INTO Multi SET content = 'target filler" + i + "'");
      }
    });

    database.transaction(() -> {
      final TypeIndex typeIndex = FullTextSearch.resolveFullTextIndex(database, "Multi[content]");

      final Map<RID, Float> unbounded = FullTextSearch.search(typeIndex, "target", -1);
      assertThat(unbounded).hasSize(16);

      final List<Map.Entry<RID, Float>> rankedAll = new ArrayList<>(unbounded.entrySet());
      rankedAll.sort(Map.Entry.<RID, Float>comparingByValue().reversed().thenComparing(Map.Entry::getKey));

      // Confirm this fixture genuinely spreads matches across more than one bucket, and does so unevenly: the four
      // highest-scoring ("strong") documents all share one bucket id, while the lowest-scoring ("weak") document
      // sits in a different bucket. Without this, a per-bucket top-k push-down could never lose anything, and the
      // test below would pass vacuously.
      final int strongCount = 4;
      final Set<Integer> strongBucketIds = new HashSet<>();
      for (int i = 0; i < strongCount; i++)
        strongBucketIds.add(rankedAll.get(i).getKey().getBucketId());
      assertThat(strongBucketIds).hasSize(1);
      final int weakestBucketId = rankedAll.get(rankedAll.size() - 1).getKey().getBucketId();
      assertThat(weakestBucketId).isNotEqualTo(strongBucketIds.iterator().next());

      final int limit = 3;
      final Map<RID, Float> bounded = FullTextSearch.search(typeIndex, "target", limit);

      // The bounded, per-bucket-pushed-down search must return exactly the same top-k RID sequence (order included)
      // as sorting and truncating the unbounded search, converting the cross-bucket soundness argument into a
      // regression guard.
      assertThat(topKRids(bounded, limit)).isEqualTo(topKRids(unbounded, limit));
    });
  }

  /**
   * Merges hits by score descending, then RID ascending for deterministic tie-breaking - the same ranking
   * {@code FullTextSearchTool} applies to its own merged results - and returns the RID sequence of the first
   * {@code limit} entries.
   */
  private static List<RID> topKRids(final Map<RID, Float> hits, final int limit) {
    final List<Map.Entry<RID, Float>> ranked = new ArrayList<>(hits.entrySet());
    ranked.sort(Map.Entry.<RID, Float>comparingByValue().reversed().thenComparing(Map.Entry::getKey));

    final List<RID> rids = new ArrayList<>();
    for (final Map.Entry<RID, Float> entry : ranked) {
      if (rids.size() >= limit)
        break;
      rids.add(entry.getKey());
    }
    return rids;
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
