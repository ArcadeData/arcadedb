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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.FullTextPostingRID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * End-to-end tests for native BM25 full-text scoring (issue #4687): IDF (rare terms outrank common ones), document-length
 * normalization, field boosts, the CLASSIC fallback, and survival of BM25 configuration + corpus counters across a restart.
 */
class FullTextBM25Test extends TestHelper {

  private Map<String, Float> searchScores(final String index, final String query) {
    final Map<String, Float> scores = new HashMap<>();
    final ResultSet rs = database.query("sql",
        "SELECT name, $score FROM Doc WHERE SEARCH_INDEX('" + index + "', '" + query + "') = true");
    while (rs.hasNext()) {
      final Result r = rs.next();
      scores.put(r.getProperty("name"), ((Number) r.getProperty("$score")).floatValue());
    }
    return scores;
  }

  @Test
  void rareTermOutranksCommonTerm() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");

      // "data" is common (appears in many documents -> low IDF); "quantum" is rare (high IDF).
      database.command("sql", "INSERT INTO Doc SET name = 'rare', content = 'quantum data'");
      for (int i = 0; i < 12; i++)
        database.command("sql", "INSERT INTO Doc SET name = 'common" + i + "', content = 'data data record'");
    });

    database.transaction(() -> {
      final Map<String, Float> scores = searchScores("Doc[content]", "quantum data");
      // The document containing the rare term must rank above any document matching only the common term.
      assertThat(scores.get("rare")).isNotNull();
      final float rare = scores.get("rare");
      for (final Map.Entry<String, Float> e : scores.entrySet())
        if (!"rare".equals(e.getKey()))
          assertThat(rare).isGreaterThan(e.getValue());
    });
  }

  @Test
  void shorterDocumentScoresHigherForSameTerm() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");

      // Both documents contain "lucene" exactly once but one is much longer.
      database.command("sql", "INSERT INTO Doc SET name = 'short', content = 'lucene'");
      database.command("sql", "INSERT INTO Doc SET name = 'long', content = 'lucene alpha beta gamma delta epsilon zeta eta theta iota kappa'");
      // Add filler docs so the term has a stable IDF.
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO Doc SET name = 'filler" + i + "', content = 'unrelated text here'");
    });

    database.transaction(() -> {
      final Map<String, Float> scores = searchScores("Doc[content]", "lucene");
      assertThat(scores).containsKeys("short", "long");
      assertThat(scores.get("short")).isGreaterThan(scores.get("long"));
    });
  }

  @Test
  void unqualifiedTermAggregatesTermFrequencyAcrossFields() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "CREATE PROPERTY Doc.body STRING");
      database.command("sql", "CREATE INDEX ON Doc (title, body) FULL_TEXT");

      // 'both' has "java" once in title AND once in body (global tf = 2); 'one' has it once, in title only (global tf = 1). Both
      // documents have the same total length (2 tokens), so the only differentiator for an unqualified query is the aggregated tf.
      database.command("sql", "INSERT INTO Doc SET name = 'both', title = 'java', body = 'java'");
      database.command("sql", "INSERT INTO Doc SET name = 'one',  title = 'java', body = 'cooking'");
    });

    database.transaction(() -> {
      // Unqualified query: the field-agnostic posting carries the summed tf, so 'both' (tf=2) outranks 'one' (tf=1).
      final Map<String, Float> unqualified = searchScores("Doc[title,body]", "java");
      assertThat(unqualified.keySet()).containsExactlyInAnyOrder("both", "one");
      assertThat(unqualified.get("both")).isGreaterThan(unqualified.get("one"));

      // A field-qualified query sees only that field's tf (1 for each in title), so they tie.
      final Map<String, Float> titleScoped = searchScores("Doc[title,body]", "title:java");
      assertThat(titleScoped.get("both")).isCloseTo(titleScoped.get("one"), within(1e-4f));
    });
  }

  @Test
  void fieldBoostRaisesFieldQualifiedMatches() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "CREATE PROPERTY Doc.body STRING");
      database.command("sql",
          "CREATE INDEX ON Doc (title, body) FULL_TEXT METADATA {\"similarity\": \"BM25\", \"title_boost\": 5.0}");

      database.command("sql", "INSERT INTO Doc SET name = 'inTitle', title = 'java', body = 'something else entirely'");
      database.command("sql", "INSERT INTO Doc SET name = 'inBody', title = 'something else', body = 'java'");
    });

    database.transaction(() -> {
      // A field-qualified query on the boosted field scores its match higher than the same term in the unboosted field.
      final Map<String, Float> titleScores = searchScores("Doc[title,body]", "title:java");
      final Map<String, Float> bodyScores = searchScores("Doc[title,body]", "body:java");
      assertThat(titleScores.get("inTitle")).isNotNull();
      assertThat(bodyScores.get("inBody")).isNotNull();
      assertThat(titleScores.get("inTitle")).isGreaterThan(bodyScores.get("inBody"));
    });
  }

  @Test
  void bm25RankingHoldsAcrossMultipleBuckets() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");

      // Inserts alternate between the two buckets. Bucket 0 receives ten documents matching both query terms, while bucket 1
      // receives one document matching only "alpha" plus nine non-matches. With bucket-local statistics, the one-term match gets
      // a very high local IDF for alpha and incorrectly outranks every two-term match from bucket 0. Type-wide statistics make
      // alpha common across the corpus and restore the expected two-term > one-term ordering.
      for (int i = 0; i < 20; i++) {
        final String name;
        final String content;
        if (i == 0) {
          name = "twoTerms";
          content = "alpha beta apple";
        } else if (i == 1) {
          name = "oneTerm";
          content = "alpha filler apricot";
        } else if (i % 2 == 0) {
          name = "twoTerms" + i;
          content = "alpha beta apple";
        } else {
          name = "noise" + i;
          content = "noise filler filler";
        }
        database.command("sql", "INSERT INTO Doc SET name = ?, content = ?", name, content);
      }
    });

    database.transaction(() -> {
      final ResultSet records = database.query("sql", "SELECT FROM Doc WHERE name IN ['twoTerms', 'oneTerm']");
      final Map<String, Integer> buckets = new HashMap<>();
      while (records.hasNext()) {
        final Result record = records.next();
        buckets.put(record.getProperty("name"), record.getIdentity().orElseThrow().getBucketId());
      }
      assertThat(buckets.get("twoTerms")).isNotEqualTo(buckets.get("oneTerm"));

      final Map<String, Float> scores = searchScores("Doc[content]", "alpha beta");
      assertThat(scores).containsKeys("twoTerms", "oneTerm");
      assertThat(scores.get("twoTerms")).isGreaterThan(scores.get("oneTerm"));

      // Wildcard terms differ by bucket ("apple" vs "apricot"). Both must be included in the global scoring-token union, while
      // Boolean matching remains document-local rather than combining terms found in different buckets.
      final Map<String, Float> wildcardScores = searchScores("Doc[content]", "ap*");
      assertThat(wildcardScores.get("twoTerms")).isPositive();
      assertThat(wildcardScores.get("oneTerm")).isPositive();
      assertThat(searchScores("Doc[content]", "+apple +apricot")).isEmpty();

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
      final Map<String, Float> directScores = new HashMap<>();
      final IndexCursor direct = typeIndex.get(new Object[] { "alpha beta" });
      while (direct.hasNext()) {
        final Document document = (Document) direct.next().getRecord();
        directScores.put(document.getString("name"), direct.getFloatScore());
      }
      assertThat(directScores.get("twoTerms")).isGreaterThan(directScores.get("oneTerm"));

      final Map<String, Float> fieldScores = new HashMap<>();
      final ResultSet fields = database.query("sql",
          "SELECT name, $score FROM Doc WHERE SEARCH_FIELDS(['content'], 'alpha beta') = true");
      while (fields.hasNext()) {
        final Result result = fields.next();
        fieldScores.put(result.getProperty("name"), ((Number) result.getProperty("$score")).floatValue());
      }
      assertThat(fieldScores.get("twoTerms")).isGreaterThan(fieldScores.get("oneTerm"));

      final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT name FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha beta') = true");
      final String plan = explain.next().getProperty("executionPlanAsString");
      assertThat(plan)
          .contains("\"corpusScope\":\"type\"")
          .contains("\"bucketCount\":2")
          .contains("\"totalDocs\":20")
          .contains("\"df\":11")
          .doesNotContain("scored per bucket");
    });
  }

  private void recomputeCounters() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
      if (bucketIndex instanceof LSMTreeFullTextIndex ftIndex)
        ftIndex.recomputeBM25Counters();
  }

  @Test
  void removalUpdatesScoringStatistics() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      // Four documents all containing "java", each with one distinct extra token (so all have the same length).
      for (int i = 1; i <= 4; i++)
        database.command("sql", "INSERT INTO Doc SET name='d" + i + "', content='java word" + i + "'");
    });

    final float[] before = new float[1];
    database.transaction(() -> before[0] = searchScores("Doc[content]", "java").get("d1"));

    // Removing a document containing "java" lowers its document frequency and N, raising IDF for the survivors.
    database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE name = 'd4'"));

    database.transaction(() -> {
      final Map<String, Float> after = searchScores("Doc[content]", "java");
      assertThat(after).containsOnlyKeys("d1", "d2", "d3");
      assertThat(after.get("d1")).isGreaterThan(before[0]); // statistics (df/N) reflect the removal
    });
  }

  @Test
  void scoringSurvivesDocumentRemovalAndRecompute() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name='a', content='java tutorial'");
      database.command("sql", "INSERT INTO Doc SET name='b', content='java guide'");
      database.command("sql", "INSERT INTO Doc SET name='c', content='python guide'");
    });

    database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE name = 'a'"));

    database.transaction(() -> {
      final Map<String, Float> scores = searchScores("Doc[content]", "java");
      assertThat(scores).containsOnlyKeys("b"); // only the surviving "java" document
      assertThat(scores.get("b")).isGreaterThan(0f);
    });

    // recompute on a non-empty type rebuilds counters exactly; runs outside a transaction (it may persist the schema).
    recomputeCounters();
    database.transaction(() -> assertThat(searchScores("Doc[content]", "java")).containsOnlyKeys("b"));

    // delete everything, recompute on the now-empty type must be safe and queries return nothing.
    database.transaction(() -> database.command("sql", "DELETE FROM Doc"));
    recomputeCounters();
    database.transaction(() -> assertThat(searchScores("Doc[content]", "java")).isEmpty());
  }

  @Test
  void rolledBackInsertIsNotIndexedAndRecomputeRepairsCounters() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name='a', content='java tutorial'");
    });

    // A rolled-back insert: its postings are not committed, but the in-memory corpus counters were already bumped (they are not
    // transactionally reversed) - so they drift until repaired.
    try {
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Doc SET name='ghost', content='java java java'");
        throw new IllegalStateException("force rollback");
      });
    } catch (final IllegalStateException ignore) {
      // expected
    }

    database.transaction(() -> {
      final Map<String, Float> scores = searchScores("Doc[content]", "java");
      assertThat(scores).containsOnlyKeys("a"); // the rolled-back "ghost" was never indexed
      assertThat(scores.get("a")).isGreaterThan(0f);
    });

    // recomputeBM25Counters rebuilds the counters from committed data, so scoring keeps working after the drift.
    recomputeCounters();
    database.transaction(() -> {
      final Map<String, Float> scores = searchScores("Doc[content]", "java");
      assertThat(scores).containsOnlyKeys("a");
      assertThat(scores.get("a")).isGreaterThan(0f);
    });
  }

  @Test
  void updateReplacesPostingsAndKeepsScoringConsistent() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name = 'doc', content = 'java tutorial'");
      database.command("sql", "INSERT INTO Doc SET name = 'other', content = 'python guide'");
    });

    // The document initially matches "java" but not "python".
    database.transaction(() -> {
      assertThat(searchScores("Doc[content]", "java")).containsKey("doc");
      assertThat(searchScores("Doc[content]", "python")).doesNotContainKey("doc");
    });

    // Update the indexed field: the old token ("java") must stop matching and the new one ("python") must start matching.
    database.transaction(() -> database.command("sql", "UPDATE Doc SET content = 'python scripting' WHERE name = 'doc'"));

    database.transaction(() -> {
      assertThat(searchScores("Doc[content]", "java")).doesNotContainKey("doc"); // old posting removed
      final Map<String, Float> python = searchScores("Doc[content]", "python");
      assertThat(python).containsKey("doc"); // new posting added
      assertThat(python.get("doc")).isGreaterThan(0f);
    });

    // After an update the corpus length counters must not be double-counted: recompute (the source of truth) must agree with the
    // incrementally maintained counters, so scores are unchanged by a recompute.
    final Map<String, Float> beforeRecompute = new HashMap<>();
    database.transaction(() -> beforeRecompute.putAll(searchScores("Doc[content]", "python")));
    recomputeCounters();
    database.transaction(() -> {
      final Map<String, Float> afterRecompute = searchScores("Doc[content]", "python");
      assertThat(afterRecompute.keySet()).isEqualTo(beforeRecompute.keySet());
      for (final Map.Entry<String, Float> e : afterRecompute.entrySet())
        assertThat(e.getValue()).isCloseTo(beforeRecompute.get(e.getKey()), within(1e-4f));
    });
  }

  @Test
  void fullTextPostingRIDRejectsNegativeStatistics() {
    // Corrupt statistics must fail fast at construction rather than silently yield a nonsensical BM25 contribution.
    assertThatThrownBy(() -> new FullTextPostingRID(database, 1, 1L, -1, 5))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("non-negative");
    assertThatThrownBy(() -> new FullTextPostingRID(database, 1, 1L, 2, -1))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("non-negative");
    // A valid posting is accepted and exposes its statistics.
    final FullTextPostingRID posting = new FullTextPostingRID(database, 1, 1L, 2, 5);
    assertThat(posting.getTf()).isEqualTo(2);
    assertThat(posting.getDocLength()).isEqualTo(5);
  }

  @Test
  void rebuildIndexStatsOnlyRepairsCountersViaSQL() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name='a', content='java tutorial'");
    });

    // Induce counter drift: a rolled-back insert bumps the in-memory corpus counters but commits no postings.
    try {
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Doc SET name='ghost', content='java java java'");
        throw new IllegalStateException("force rollback");
      });
    } catch (final IllegalStateException ignore) {
      // expected
    }

    // Repair the BM25 corpus counters from SQL - no Java, and no full (expensive) index rebuild.
    try (final ResultSet rs = database.command("sql", "REBUILD INDEX `Doc[content]` WITH statsOnly = true")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("operation")).isEqualTo("rebuild index stats");
      assertThat(((Number) r.getProperty("statsRecomputed")).intValue()).isEqualTo(1);
    }

    database.transaction(() -> {
      final Map<String, Float> scores = searchScores("Doc[content]", "java");
      assertThat(scores).containsOnlyKeys("a");
      assertThat(scores.get("a")).isGreaterThan(0f);
    });

    // A CLASSIC index keeps no recomputable statistics: statsOnly must report that clearly rather than silently succeeding.
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Plain");
      database.command("sql", "CREATE PROPERTY Plain.content STRING");
      database.command("sql", "CREATE INDEX ON Plain (content) FULL_TEXT METADATA {\"similarity\": \"CLASSIC\"}");
    });
    assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `Plain[content]` WITH statsOnly = true"))
        .hasMessageContaining("no recomputable statistics");

    // An unknown index name reports "not found" rather than throwing a NullPointerException.
    assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `Nope[content]` WITH statsOnly = true"))
        .hasMessageContaining("not found");

    // Wildcard form: recompute every index that keeps statistics. Only the BM25 index (Doc[content]) qualifies; the CLASSIC one
    // (Plain[content]) is skipped, so exactly one index is reported.
    try (final ResultSet rs = database.command("sql", "REBUILD INDEX * WITH statsOnly = true")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("operation")).isEqualTo("rebuild index stats");
      assertThat(((Number) r.getProperty("statsRecomputed")).intValue()).isEqualTo(1);
      assertThat(r.<List<String>>getProperty("indexes")).containsExactly("Doc[content]");
    }
  }

  @Test
  void bm25GetHonorsLimitReturningTopKInOrder() {
    database.transaction(() -> {
      // Single bucket so the bucket-index cursor sees every document and the top-K selection is deterministic.
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      // 'rare' matches the discriminative term and is short -> highest BM25; the rest match only the common term.
      database.command("sql", "INSERT INTO Doc SET name = 'rare', content = 'quantum data'");
      for (int i = 0; i < 8; i++)
        database.command("sql", "INSERT INTO Doc SET name = 'common" + i + "', content = 'data data record number " + i + "'");
    });

    database.transaction(() -> {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
      final LSMTreeFullTextIndex bucket = (LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0];

      // limit (2) smaller than the candidate set exercises the bounded-heap top-K path.
      final List<String> top = new ArrayList<>();
      float previous = Float.MAX_VALUE;
      final IndexCursor cursor = bucket.get(new Object[] { "quantum data" }, 2);
      while (cursor.hasNext()) {
        final var entry = cursor.next();
        top.add(((Document) entry.getRecord()).getString("name"));
        final float score = cursor.getFloatScore();
        assertThat(score).isLessThanOrEqualTo(previous); // results come back already sorted, most-relevant first
        previous = score;
      }
      assertThat(top).hasSize(2);
      assertThat(top.get(0)).isEqualTo("rare"); // the discriminative, short document is the top result
    });
  }

  @Test
  void directGetWithLuceneSyntaxIsGracefulNotLuceneParsed() {
    // The direct index.get() path is token-based (it also backs CONTAINSTEXT), so Lucene syntax is NOT parsed - it is handed to
    // the analyzer as literal text. This documents that 'java^3' does not crash and is not treated as a boost: the StandardAnalyzer
    // simply tokenizes around the caret into [java, 3], so the document matches via the 'java' token but the ^3 boost is silently
    // ignored. Use SEARCH_INDEX for real caret/boolean/phrase/wildcard semantics.
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name = 'a', content = 'java tutorial'");
    });

    database.transaction(() -> {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
      final LSMTreeFullTextIndex bucket = (LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0];

      // Caret syntax on the direct path: no exception. It matches via the analyzer-tokenized 'java' (the ^3 is not a Lucene boost,
      // just a token separator), confirming graceful, non-Lucene handling rather than a crash or a parsed boost query.
      int caretMatches = 0;
      final IndexCursor caret = bucket.get(new Object[] { "java^3" });
      while (caret.hasNext()) {
        caret.next();
        caretMatches++;
      }
      assertThat(caretMatches).isEqualTo(1);

      // A plain term on the same path matches the same document.
      int plainMatches = 0;
      final IndexCursor plain = bucket.get(new Object[] { "java" });
      while (plain.hasNext()) {
        plain.next();
        plainMatches++;
      }
      assertThat(plainMatches).isEqualTo(1);
    });
  }

  @Test
  void reservedDefaultFieldPropertyNameIsRejected() {
    // The query parser reserves an internal sentinel for the unqualified default field; a real property with that exact name
    // would be ambiguous on a multi-property index, so index creation must reject it rather than mis-score silently.
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.`__arcadedb_default_field__` STRING");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
    });
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
        "CREATE INDEX ON Doc (`__arcadedb_default_field__`, title) FULL_TEXT")))
        .hasStackTraceContaining("reserved"); // the IllegalArgumentException is wrapped by the index builder
  }

  @Test
  void invalidBM25ParametersAreRejected() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
    });

    // k1 must be >= 0 and b must be in [0,1]: misconfiguration is surfaced at index creation, not silently mis-scored.
    assertThatThrownBy(() -> database.transaction(() ->
            database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"bm25_b\": 2.0}")))
        .hasMessageContaining("b must be in [0, 1]");

    assertThatThrownBy(() -> database.transaction(() ->
            database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"bm25_k1\": -1.0}")))
        .hasMessageContaining("k1 must be >= 0");

    // An unknown similarity is rejected rather than silently treated as CLASSIC.
    assertThatThrownBy(() -> database.transaction(() ->
            database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"LUCENE\"}")))
        .hasMessageContaining("Unknown full-text similarity");

    // A negative per-field boost is rejected: it would produce negative term contributions and invert ranking.
    assertThatThrownBy(() -> database.transaction(() ->
            database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"content_boost\": -5.0}")))
        .hasMessageContaining("boost for 'content' must be >= 0");
  }

  @Test
  void pureNegativeQueryReturnsComplement() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name = 'a', content = 'java tutorial'");
      database.command("sql", "INSERT INTO Doc SET name = 'b', content = 'python guide'");
      database.command("sql", "INSERT INTO Doc SET name = 'c', content = 'java reference'");
    });

    database.transaction(() -> {
      // A pure-negative query materializes the whole index (collectAllIndexedRids) and subtracts the matches of the negated term.
      // '-java' must return exactly the documents that do NOT contain 'java'.
      final Map<String, Float> scores = searchScores("Doc[content]", "-java");
      assertThat(scores.keySet()).containsExactly("b");
    });
  }

  @Test
  void nestedNegationDoesNotWronglyExcludeDoubleNegatedTerms() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name = 'd1', content = 'java tutorial'");
      database.command("sql", "INSERT INTO Doc SET name = 'd2', content = 'java database'");
    });

    database.transaction(() -> {
      // java AND NOT (database AND NOT tutorial) == java AND (NOT database OR tutorial).
      // d1 (java, tutorial): java && (NOT database OR tutorial) -> matches. d2 (java, database): java && (false OR false) -> no.
      // The nested NOT tutorial is a double negation: 'tutorial' must NOT be excluded, so d1 must survive (the bug excluded it).
      final Map<String, Float> scores = searchScores("Doc[content]", "java -(database -tutorial)");
      assertThat(scores.keySet()).containsExactly("d1");
    });
  }

  @Test
  void caretInBooleanQueries() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "CREATE PROPERTY Doc.body STRING");
      database.command("sql", "CREATE INDEX ON Doc (title, body) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name='d1', title='java guide',   body='database tuning'");
      database.command("sql", "INSERT INTO Doc SET name='d2', title='python guide', body='database tuning'");
      database.command("sql", "INSERT INTO Doc SET name='d3', title='java',          body='cooking recipes'");
    });

    database.transaction(() -> {
      // MUST database (any field) + boosted SHOULD on title:java. Only d1,d2 have "database"; d1 also matches the boosted
      // title:java, so d1 outranks d2; d3 is excluded (no "database").
      final Map<String, Float> r1 = searchScores("Doc[title,body]", "+database title:java^5");
      assertThat(r1.keySet()).containsExactlyInAnyOrder("d1", "d2");
      assertThat(r1.get("d1")).isGreaterThan(r1.get("d2"));
      r1.values().forEach(s -> assertThat(s).isGreaterThan(0f));

      // Group boost over an OR, combined with another term: (java OR python)^3 cooking
      final Map<String, Float> r2 = searchScores("Doc[title,body]", "(title:java OR title:python)^3 body:cooking");
      assertThat(r2.keySet()).contains("d1", "d2", "d3");

      // NOT with a boosted positive term: boosted java, excluding anything mentioning cooking -> d3 removed.
      final Map<String, Float> r3 = searchScores("Doc[title,body]", "title:java^4 -body:cooking");
      assertThat(r3.keySet()).contains("d1");
      assertThat(r3.keySet()).doesNotContain("d3");
    });
  }

  @Test
  void caretBoostRaisesTermWeight() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name = 'java', content = 'java tutorial'");
      database.command("sql", "INSERT INTO Doc SET name = 'python', content = 'python tutorial'");
    });

    database.transaction(() -> {
      // Caret boost amplifies the "java" term, so the java document outranks the python one for "java^4 python".
      final Map<String, Float> boosted = searchScores("Doc[content]", "java^4 python");
      assertThat(boosted).containsKeys("java", "python");
      assertThat(boosted.get("java")).isGreaterThan(boosted.get("python"));

      // Without the boost (symmetric query) the two are tied.
      final Map<String, Float> neutral = searchScores("Doc[content]", "java python");
      assertThat(neutral.get("java")).isCloseTo(neutral.get("python"), within(1e-4f));
    });
  }

  @Test
  void explainSurfacesBM25ScoringMetadata() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name = 'a', content = 'java database tuning'");
      database.command("sql", "INSERT INTO Doc SET name = 'b', content = 'python scripting'");
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "EXPLAIN SELECT name, $score FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'java database') = true");
      final String plan = rs.next().getProperty("executionPlanAsString");
      // The full-text fetch step carries the BM25 scoring metadata so it can be inspected via EXPLAIN.
      assertThat(plan).contains("SCORING");
      assertThat(plan).contains("BM25");
      assertThat(plan).contains("java").contains("database");
      assertThat(plan).contains("\"k1\"").contains("\"b\"");
    });
  }

  @Test
  void explainTruncatesAtTermCapForHugeExpansions() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      // 80 distinct tokens sharing the prefix 'term', so a 'term*' wildcard expands past the 64-term EXPLAIN cap.
      for (int i = 0; i < 80; i++)
        database.command("sql", "INSERT INTO Doc SET name = 'd" + i + "', content = 'term" + i + "'");
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "EXPLAIN SELECT name FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'term*') = true");
      final String plan = rs.next().getProperty("executionPlanAsString");
      // The scoring breakdown is capped and flagged rather than scanning all 80 expanded terms' postings.
      assertThat(plan).contains("termsTruncated");
      // The breakdown also reports how many terms were omitted (80 matched - 64 shown = 16) so the partial view is explicit.
      assertThat(plan).contains("termsOmitted").contains("\"termsOmitted\":16");
    });
  }

  @Test
  void explainOnEmptyCorpus() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
    });

    // EXPLAIN on an index with no documents must not fail; the query term reports df 0.
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "EXPLAIN SELECT name, $score FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'java') = true");
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).contains("SCORING").contains("BM25").contains("java");
      assertThat(plan).contains("\"df\":0"); // the query term has zero document frequency on an empty corpus
    });
  }

  @Test
  void classicSimilarityKeepsCoordinationScoring() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"CLASSIC\"}");

      database.command("sql", "INSERT INTO Doc SET name = 'all3', content = 'java programming language'");
      database.command("sql", "INSERT INTO Doc SET name = 'one', content = 'java database system'");
    });

    database.transaction(() -> {
      final Map<String, Float> scores = searchScores("Doc[content]", "java programming language");
      // CLASSIC coordination: integer match counts widened to float.
      assertThat(scores.get("all3")).isCloseTo(3.0f, within(1e-4f));
      assertThat(scores.get("one")).isCloseTo(1.0f, within(1e-4f));

      // $score is a Float now even for CLASSIC indexes (was Integer before BM25): pin the type so the breaking change is tested.
      final ResultSet rs = database.query("sql",
          "SELECT $score AS s FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'java') = true LIMIT 1");
      assertThat(rs.next().<Object>getProperty("s")).isInstanceOf(Float.class);
    });
  }

  @Test
  void nullIndexedFieldUnderBM25IsSkippedAndDoesNotInflateLength() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT"); // BM25, default NULL_STRATEGY (skip)
      // One document has a null indexed field: it must be skipped cleanly (no phantom posting, no length inflation) while the
      // others score normally.
      database.command("sql", "INSERT INTO Doc SET name = 'nullDoc', content = null");
      database.command("sql", "INSERT INTO Doc SET name = 'a', content = 'java tutorial'");
      database.command("sql", "INSERT INTO Doc SET name = 'b', content = 'java guide reference'");
    });

    database.transaction(() -> {
      final Map<String, Float> scores = searchScores("Doc[content]", "java");
      // The null-content document never matches; the two real documents score finite, positive BM25 values.
      assertThat(scores.keySet()).containsExactlyInAnyOrder("a", "b");
      scores.values().forEach(s -> {
        assertThat(s).isGreaterThan(0f);
        assertThat(s.isNaN()).isFalse();
      });
      // The shorter document ('a', 2 tokens) outranks the longer one ('b', 3 tokens) for the same term - confirming the null
      // field did not distort the average document length used for normalization.
      assertThat(scores.get("a")).isGreaterThan(scores.get("b"));
    });
  }

  @Test
  void singleDocumentScoresWithoutLengthBias() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      // A single-document corpus: avgdl equals that document's length, so the length-normalization denominator collapses to k1+1
      // and the score must still be a finite positive number (no divide-by-zero on avgdl).
      database.command("sql", "INSERT INTO Doc SET name = 'only', content = 'java database tuning'");
    });

    database.transaction(() -> {
      final Map<String, Float> scores = searchScores("Doc[content]", "java");
      assertThat(scores).containsOnlyKeys("only");
      assertThat(scores.get("only")).isGreaterThan(0f);
      assertThat(scores.get("only").isNaN()).isFalse();
      assertThat(scores.get("only").isInfinite()).isFalse();
    });
  }

  @Test
  void contentFieldNameDoesNotCollideWithDefaultFieldSentinel() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      // A multi-property index that includes a property literally named "content" must resolve a content:term clause to that field
      // - the default-field sentinel is no longer "content", so the former silent collision is fixed.
      database.command("sql", "CREATE INDEX ON Doc (content, title) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET name = 'inContent', content = 'java tutorial', title = 'something else'");
      database.command("sql", "INSERT INTO Doc SET name = 'inTitle',   content = 'something else', title = 'java tutorial'");
    });

    database.transaction(() -> {
      // Unqualified term: both documents match (the field-agnostic token), scores are finite and positive.
      final Map<String, Float> unqualified = searchScores("Doc[content,title]", "java");
      assertThat(unqualified.keySet()).containsExactlyInAnyOrder("inContent", "inTitle");
      unqualified.values().forEach(s -> assertThat(s).isGreaterThan(0f));

      // content:java resolves to the content field ONLY: inContent matches, inTitle (java only in its title) does not.
      final Map<String, Float> contentScoped = searchScores("Doc[content,title]", "content:java");
      assertThat(contentScoped.keySet()).containsExactly("inContent");

      // title:java symmetrically resolves to the title field ONLY.
      final Map<String, Float> titleScoped = searchScores("Doc[content,title]", "title:java");
      assertThat(titleScoped.keySet()).containsExactly("inTitle");
    });
  }

  @Test
  void contentFieldBoostIsAppliedOnMultiPropertyIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      // A boost on a field literally named "content" must now take effect (previously silently dropped because "content" was the
      // default-field sentinel).
      database.command("sql",
          "CREATE INDEX ON Doc (content, title) FULL_TEXT METADATA {\"similarity\": \"BM25\", \"content_boost\": 5.0}");
      database.command("sql", "INSERT INTO Doc SET name = 'inContent', content = 'java', title = 'something else entirely'");
      database.command("sql", "INSERT INTO Doc SET name = 'inTitle',   content = 'something else', title = 'java'");
    });

    database.transaction(() -> {
      final Map<String, Float> contentScores = searchScores("Doc[content,title]", "content:java");
      final Map<String, Float> titleScores = searchScores("Doc[content,title]", "title:java");
      assertThat(contentScores.get("inContent")).isNotNull();
      assertThat(titleScores.get("inTitle")).isNotNull();
      // The content_boost=5.0 raises the content-field match above the equivalent unboosted title-field match.
      assertThat(contentScores.get("inContent")).isGreaterThan(titleScores.get("inTitle"));
    });
  }

  @Test
  @Tag("slow")
  void bm25ConfigAndCountersSurviveRestart() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\", \"bm25_k1\": 1.5, \"bm25_b\": 0.6}");
      database.command("sql", "INSERT INTO Doc SET name = 'rare', content = 'quantum data'");
      for (int i = 0; i < 12; i++)
        database.command("sql", "INSERT INTO Doc SET name = 'common" + i + "', content = 'data data record'");
    });

    final Map<String, Float> before = new HashMap<>();
    database.transaction(() -> before.putAll(searchScores("Doc[content]", "quantum data")));

    reopenDatabase();

    database.transaction(() -> {
      // Directly assert the persisted BM25 configuration and corpus counters round-tripped, so a silent regression in
      // writeToJSON/fromJSON (e.g. k1 stops persisting) is caught even if it would not move the scores past the epsilon below.
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
      for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
        if (bucketIndex instanceof LSMTreeFullTextIndex ftIndex) {
          final var meta = ftIndex.getFullTextMetadata();
          assertThat(meta.getSimilarity()).isEqualTo("BM25");
          assertThat(meta.getBm25K1()).isCloseTo(1.5f, within(1e-6f));
          assertThat(meta.getBm25B()).isCloseTo(0.6f, within(1e-6f));
        }
      // The type-wide counters feeding avgdl must also survive: 13 documents totalling 2 + 12*3 = 38 tokens. The counters are
      // shared type-wide (the same metadata object is passed to every bucket index), so read them from a single bucket index.
      final LSMTreeFullTextIndex anyBucket = (LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0];
      assertThat(anyBucket.getFullTextMetadata().getTotalDocs()).isEqualTo(13L);
      assertThat(anyBucket.getFullTextMetadata().getSumDocLength()).isEqualTo(38L);

      final Map<String, Float> after = searchScores("Doc[content]", "quantum data");
      // The ranking (rare term wins) and the actual scores must be identical after restart.
      assertThat(after.get("rare")).isNotNull();
      assertThat(after.get("rare")).isCloseTo(before.get("rare"), within(1e-4f));
      for (final Map.Entry<String, Float> e : after.entrySet())
        if (!"rare".equals(e.getKey()))
          assertThat(after.get("rare")).isGreaterThan(e.getValue());
    });
  }

  @Test
  void classicGetReturnsMostRelevantFirstAndHonorsLimit() {
    database.transaction(() -> {
      // Single bucket so every document lands in the one bucket index whose cursor order we assert.
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"CLASSIC\"}");
      // d3 matches all three query terms, d2 two, d1 one: CLASSIC coordination score 3 > 2 > 1.
      database.command("sql", "INSERT INTO Doc SET name = 'd1', content = 'java'");
      database.command("sql", "INSERT INTO Doc SET name = 'd2', content = 'java database'");
      database.command("sql", "INSERT INTO Doc SET name = 'd3', content = 'java database tuning'");
    });

    database.transaction(() -> {
      // The cursor itself must return the most-relevant document first (no explicit ORDER BY): the order is the index's, not the
      // engine's. This pins the fix for the previously ascending (least-relevant-first) classic sort.
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
      final LSMTreeFullTextIndex bucket = (LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0];

      final List<String> order = new ArrayList<>();
      final IndexCursor cursor = bucket.get(new Object[] { "java database tuning" });
      while (cursor.hasNext())
        order.add(((Document) cursor.next().getRecord()).getString("name"));
      assertThat(order).containsExactly("d3", "d2", "d1");

      // limit must truncate to the top-N by score, not be ignored.
      final List<String> top = new ArrayList<>();
      final IndexCursor limited = bucket.get(new Object[] { "java database tuning" }, 2);
      while (limited.hasNext())
        top.add(((Document) limited.next().getRecord()).getString("name"));
      assertThat(top).containsExactly("d3", "d2");
    });
  }

  @Test
  void bm25SearchIndexReturnsCanonicalRid() {
    // Issue #4731: a BM25 SEARCH_INDEX match must expose the canonical record identity (#bucket:offset) in @rid, never the
    // internal FullTextPostingRID debug form (#bucket:offset{tf=..,docLength=..}). The posting subtype carries per-posting BM25
    // statistics and must stay confined to the index internals; relevance is already exposed separately via $score.
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "CREATE INDEX ON Doc (title) FULL_TEXT"); // default similarity => BM25
      database.command("sql", "INSERT INTO Doc SET title = 'java database'");
    });

    database.transaction(() -> {
      // The control RID (selected without SEARCH_INDEX) is the canonical identity the BM25 path must match.
      final String canonical;
      try (final ResultSet control = database.query("sql", "SELECT @rid AS rid FROM Doc")) {
        canonical = control.next().getProperty("rid").toString();
        assertThat(canonical).matches("#\\d+:\\d+");
      }

      // SELECT * : the implicit @rid must be canonical, not the {tf=..,docLength=..} debug string.
      try (final ResultSet rs = database.query("sql",
          "SELECT *, $score AS _score FROM Doc WHERE SEARCH_INDEX('Doc[title]', 'java') = true")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("@rid").toString()).isEqualTo(canonical).doesNotContain("tf=", "docLength=", "{");
        // The record identity itself (not just its rendered form) must be a plain RID, not the posting subtype.
        assertThat(r.getElement().get().getIdentity()).isNotInstanceOf(FullTextPostingRID.class);
        // The scoring detail stays available through the dedicated channel.
        assertThat(((Number) r.getProperty("_score")).floatValue()).isGreaterThan(0f);
      }

      // Explicit @rid projection must be canonical too.
      try (final ResultSet rs = database.query("sql",
          "SELECT @rid AS rid FROM Doc WHERE SEARCH_INDEX('Doc[title]', 'java') = true")) {
        assertThat(rs.next().getProperty("rid").toString()).isEqualTo(canonical);
      }
    });
  }
}
