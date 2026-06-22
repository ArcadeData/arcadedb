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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
        if (!e.getKey().equals("rare"))
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
      // Multiple buckets: BM25 is scored per bucket, so N (doc count) and df must both be per-bucket to keep IDF unbiased.
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Doc SET name = 'rare', content = 'quantum data analysis'");
      for (int i = 0; i < 40; i++)
        database.command("sql", "INSERT INTO Doc SET name = 'common" + i + "', content = 'data record number " + i + "'");
    });

    database.transaction(() -> {
      final Map<String, Float> scores = searchScores("Doc[content]", "quantum data");
      assertThat(scores.get("rare")).isNotNull();
      // The document with the rare, discriminative term must rank above every common-only document, regardless of which bucket
      // each landed in.
      for (final Map.Entry<String, Float> e : scores.entrySet())
        if (!e.getKey().equals("rare"))
          assertThat(scores.get("rare")).isGreaterThan(e.getValue());
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
      assertThat(neutral.get("java")).isCloseTo(neutral.get("python"), org.assertj.core.api.Assertions.within(1e-4f));
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
      assertThat(scores.get("all3")).isEqualTo(3.0f);
      assertThat(scores.get("one")).isEqualTo(1.0f);
    });
  }

  @Test
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
      final Map<String, Float> after = searchScores("Doc[content]", "quantum data");
      // The ranking (rare term wins) and the actual scores must be identical after restart.
      assertThat(after.get("rare")).isNotNull();
      assertThat(after.get("rare")).isCloseTo(before.get("rare"), org.assertj.core.api.Assertions.within(1e-4f));
      for (final Map.Entry<String, Float> e : after.entrySet())
        if (!e.getKey().equals("rare"))
          assertThat(after.get("rare")).isGreaterThan(e.getValue());
    });
  }
}
