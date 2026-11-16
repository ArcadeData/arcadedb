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
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12165: a phrase matches its terms at the positions the query gives them, within the slop, as Lucene's PhraseQuery does,
 * and a fuzzy term counts a transposition of two adjacent characters as one edit, as Lucene's FuzzyQuery does by default. The
 * expected answers are the ones Lucene 10.6 gives on the same documents with the same analyzer.
 */
class Issue12165FullTextPhraseFuzzyTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 3");
      database.command("sql", "CREATE PROPERTY Doc.k INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.t STRING");
      database.command("sql", "CREATE INDEX ON Doc (t) FULL_TEXT");
      insert("Doc", 1, "machine learning");
      insert("Doc", 2, "learning machine");
      insert("Doc", 3, "machine one two three learning");
      insert("Doc", 4, "learning about machine learning");
      insert("Doc", 5, "machine machine learning");
      insert("Doc", 6, "the form");
      insert("Doc", 7, "tigre");

      database.command("sql", "CREATE DOCUMENT TYPE En");
      database.command("sql", "CREATE PROPERTY En.k INTEGER");
      database.command("sql", "CREATE PROPERTY En.t STRING");
      database.command("sql",
          "CREATE INDEX ON En (t) FULL_TEXT METADATA {\"analyzer\": \"org.apache.lucene.analysis.en.EnglishAnalyzer\"}");
      insert("En", 1, "machine of learning");
      insert("En", 2, "machine learning");
      insert("En", 3, "machines of the learning");

      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.k INTEGER");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");
      database.command("sql", "INSERT INTO Article SET k = 1, title = 'machine', body = 'learning'");
      database.command("sql", "INSERT INTO Article SET k = 2, title = 'notes', body = 'machine learning'");
    });
  }

  @Test
  void aPhraseMatchesItsTermsInOrderAndAdjacent() {
    assertThat(search("Doc", "\"machine learning\"").keySet()).containsExactlyInAnyOrder(1, 4, 5);
    assertThat(search("Doc", "\"learning machine\"").keySet()).containsExactly(2);
    assertThat(search("Doc", "\"machine machine\"").keySet()).containsExactly(5);
  }

  @Test
  void theSlopBoundsHowFarTheTermsMayMove() {
    assertThat(search("Doc", "\"machine learning\"~1").keySet()).containsExactlyInAnyOrder(1, 4, 5);
    assertThat(search("Doc", "\"machine learning\"~2").keySet()).containsExactlyInAnyOrder(1, 2, 4, 5);
    assertThat(search("Doc", "\"machine learning\"~3").keySet()).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
    assertThat(search("Doc", "\"learning machine\"~2").keySet()).containsExactlyInAnyOrder(1, 2, 4, 5);
    // a term repeated in the phrase needs as many occurrences
    assertThat(search("Doc", "\"machine machine\"~1").keySet()).containsExactly(5);
    assertThat(search("Doc", "\"learning learning\"~3").keySet()).containsExactly(4);
  }

  /**
   * A phrase repeating one term many times, with a large slop, over a record holding the term as often: placing the repeated term
   * must not try every order of its positions (review of #12180). The timeout is a hang detector only.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aPhraseRepeatingATermIsPlacedWithoutTryingEveryOrder() {
    final String machines = "machine ".repeat(12);
    database.transaction(() -> {
      insert("Doc", 20, machines + "filler ".repeat(200) + "learning");
      insert("Doc", 21, machines + "learning");
    });
    // the twelve terms and learning fit within the slop in record 21 only: in record 20 learning is 200 positions away
    assertThat(search("Doc", "\"" + machines + "learning\"~100").keySet()).containsExactly(21);
    assertThat(search("Doc", "\"" + machines + "learning\"").keySet()).containsExactly(21);
    // repeated terms around another one, as Lucene 10.6 answers them on the same records
    assertThat(search("Doc", "\"machine learning machine\"~2").keySet()).containsExactlyInAnyOrder(5, 21);
    assertThat(search("Doc", "\"machine machine machine\"~3").keySet()).containsExactlyInAnyOrder(20, 21);
    assertThat(search("Doc", "\"machine machine\"~1").keySet()).containsExactlyInAnyOrder(5, 20, 21);
  }

  @Test
  void aStopWordKeepsItsPlaceInThePhrase() {
    assertThat(search("En", "\"machine of learning\"").keySet()).containsExactly(1);
    assertThat(search("En", "\"machine the learning\"").keySet()).containsExactly(1);
    assertThat(search("En", "\"machine learning\"").keySet()).containsExactly(2);
    assertThat(search("En", "\"machine learning\"~1").keySet()).containsExactlyInAnyOrder(1, 2);
    assertThat(search("En", "\"machines learning\"").keySet()).containsExactly(2);
  }

  @Test
  void aPhraseDoesNotSpanTwoProperties() {
    assertThat(searchIndex("Article", "Article[title,body]", "\"machine learning\"").keySet()).containsExactly(2);
    assertThat(searchIndex("Article", "Article[title,body]", "body:\"machine learning\"").keySet()).containsExactly(2);
    assertThat(searchIndex("Article", "Article[title,body]", "title:\"machine learning\"").keySet()).isEmpty();
  }

  @Test
  void aFuzzyTermCountsATranspositionAsOneEdit() {
    assertThat(search("Doc", "from~1").keySet()).containsExactly(6);
    assertThat(search("Doc", "teh~1").keySet()).containsExactly(6);
    assertThat(search("Doc", "tiger~1").keySet()).containsExactly(7);
    // two transpositions are two edits
    assertThat(search("Doc", "mahcien~1").keySet()).isEmpty();
    assertThat(search("Doc", "mahcien~2").keySet()).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
  }

  @Test
  void thePerRecordEvaluationAgreesWithTheIndex() {
    final String rids = database.query("sql", "SELECT @rid AS rid FROM Doc").stream()
        .map(r -> r.getProperty("rid").toString()).collect(Collectors.joining(", ", "[", "]"));
    for (final String query : List.of("\"machine learning\"", "\"learning machine\"~2", "machine \"learning machine\"", "from~1"))
      assertThat(search(rids, query)).as(query).isEqualTo(search("Doc", query));
  }

  @Test
  void aPhraseSeesTheRecordsOfTheOpenTransaction() {
    database.transaction(() -> {
      insert("Doc", 8, "deep machine learning");
      database.command("sql", "UPDATE Doc SET t = 'learning, then machine' WHERE k = 1");
      assertThat(search("Doc", "\"machine learning\"").keySet()).containsExactlyInAnyOrder(4, 5, 8);
    });
  }

  private void insert(final String type, final int k, final String text) {
    database.command("sql", "INSERT INTO " + type + " SET k = ?, t = ?", k, text);
  }

  private Map<Integer, Float> search(final String target, final String query) {
    return searchIndex(target, target.startsWith("[") ? "Doc[t]" : target + "[t]", query);
  }

  private Map<Integer, Float> searchIndex(final String target, final String index, final String query) {
    final ResultSet rs = database.query("sql",
        "SELECT k, $score AS s FROM " + target + " WHERE SEARCH_INDEX('" + index + "', :q) = true", Map.of("q", query));
    final Map<Integer, Float> scores = new HashMap<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      scores.put(row.<Integer>getProperty("k"), ((Number) row.getProperty("s")).floatValue());
    }
    return scores;
  }
}
