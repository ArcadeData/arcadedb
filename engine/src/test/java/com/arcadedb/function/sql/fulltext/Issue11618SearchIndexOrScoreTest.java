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
package com.arcadedb.function.sql.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11618: a record matching several SEARCH_INDEX arms of an OR got its best score when the branches were
 * merged by score, but the score of the first arm that matched when the OR was evaluated per row (a RID list target) or when
 * the branches ran one after the other and a DISTINCT kept the first copy (another ORDER BY, DISTINCT, a plain arm).
 */
class Issue11618SearchIndexOrScoreTest extends TestHelper {
  private static final String A = "SEARCH_INDEX('Chunk[text]', 'capital') = true";
  private static final String B = "SEARCH_INDEX('Chunk[questions]', 'capital') = true";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Chunk");
      database.command("sql", "CREATE PROPERTY Chunk.name STRING");
      database.command("sql", "CREATE PROPERTY Chunk.text STRING");
      database.command("sql", "CREATE PROPERTY Chunk.questions STRING");
      database.command("sql", "CREATE INDEX ON Chunk (text) FULL_TEXT");
      database.command("sql", "CREATE INDEX ON Chunk (questions) FULL_TEXT");
      insert("both", "capital requirements overview", "capital capital capital ratio");
      insert("bothTextBest", "capital", "capital requirements overview and more words");
      insert("textOnly", "capital", "unrelated words here");
      insert("questionsOnly", "unrelated words here", "capital");
      for (int i = 0; i < 20; i++)
        insert("filler" + i, "filler text " + i, "filler question " + i);
    });
  }

  @Test
  void aRecordMatchingSeveralArmsGetsItsBestScoreWhateverThePlan() {
    final Map<String, Float> text = scores("SELECT name, $score AS score FROM Chunk WHERE " + A);
    final Map<String, Float> questions = scores("SELECT name, $score AS score FROM Chunk WHERE " + B);
    final Map<String, Float> best = new HashMap<>(text);
    questions.forEach((name, score) -> best.merge(name, score, Math::max));
    assertThat(best).containsOnlyKeys("both", "bothTextBest", "textOnly", "questionsOnly");
    // the corpus has a record whose best arm is each of the two
    assertThat(questions.get("both")).isGreaterThan(text.get("both"));
    assertThat(text.get("bothTextBest")).isGreaterThan(questions.get("bothTextBest"));

    final String rids = "[" + database.query("sql", "SELECT @rid AS rid FROM Chunk WHERE name NOT LIKE 'filler%'").stream()
        .map(r -> r.getProperty("rid").toString()).collect(Collectors.joining(", ")) + "]";
    for (final String query : new String[] {
        "SELECT name, $score AS score FROM Chunk WHERE " + A + " OR " + B,
        "SELECT name, $score AS score FROM Chunk WHERE " + A + " OR " + B + " ORDER BY $score DESC LIMIT 10",
        "SELECT name, $score AS score FROM " + rids + " WHERE " + A + " OR " + B,
        "SELECT name, $score AS score FROM " + rids + " WHERE " + B + " OR " + A,
        "SELECT name, $score AS score FROM " + rids + " WHERE (" + A + " OR " + B + ") ORDER BY $score DESC",
        "SELECT name, $score AS score FROM Chunk WHERE " + A + " OR " + B + " ORDER BY name",
        "SELECT name, $score AS score FROM Chunk WHERE " + B + " OR " + A + " ORDER BY name",
        "SELECT name, $score AS score FROM Chunk WHERE " + A + " OR " + B + " ORDER BY $score ASC",
        "SELECT DISTINCT name, $score AS score FROM Chunk WHERE " + A + " OR " + B,
        "SELECT name, $score AS score FROM Chunk WHERE " + A + " OR " + B + " OR name = 'nope'",
        "SELECT name, $score AS score FROM Chunk WHERE " + A + " OR (" + B + " AND name IS NOT NULL)" })
      assertThat(scores(query)).as(query).isEqualTo(best);
  }

  /** An arm that matches nothing does not lower or clear the score of the arm that matched. */
  @Test
  void aNonMatchingArmKeepsTheScoreOfTheMatchingOne() {
    final Map<String, Float> text = scores("SELECT name, $score AS score FROM Chunk WHERE " + A);
    final String rid = database.query("sql", "SELECT @rid AS rid FROM Chunk WHERE name = 'textOnly'").next().getProperty("rid")
        .toString();
    assertThat(scores("SELECT name, $score AS score FROM [" + rid + "] WHERE " + A + " OR " + B)).isEqualTo(
        Map.of("textOnly", text.get("textOnly")));
  }

  /** A scoring call on the right-hand side of its condition scores as one on the left does. */
  @Test
  void aScoringCallOnTheRightOfItsConditionKeepsItsScore() {
    final Map<String, Float> text = scores("SELECT name, $score AS score FROM Chunk WHERE " + A);
    final String rid = database.query("sql", "SELECT @rid AS rid FROM Chunk WHERE name = 'textOnly'").next().getProperty("rid")
        .toString();
    assertThat(scores("SELECT name, $score AS score FROM [" + rid + "] WHERE true = SEARCH_INDEX('Chunk[text]', 'capital') OR " + B))
        .isEqualTo(Map.of("textOnly", text.get("textOnly")));
  }

  /** A record matched only by an arm that does not score gets 0, not the score left by the record before it. */
  @Test
  void aRecordMatchedOnlyByAPlainArmGetsZero() {
    final Map<String, Float> text = scores("SELECT name, $score AS score FROM Chunk WHERE " + A);
    final String rids = "[" + database.query("sql",
            "SELECT @rid AS rid FROM Chunk WHERE name IN ['textOnly', 'questionsOnly', 'filler1'] ORDER BY name DESC").stream()
        .map(r -> r.getProperty("rid").toString()).collect(Collectors.joining(", ")) + "]";
    final Map<String, Float> expected = Map.of("textOnly", text.get("textOnly"), "questionsOnly", 0f, "filler1", 0f);
    assertThat(scores("SELECT name, $score AS score FROM " + rids + " WHERE name IN ['questionsOnly', 'filler1'] OR " + A))
        .isEqualTo(expected);
    final Map<String, Float> everyMatch = new HashMap<>(expected);
    everyMatch.put("both", text.get("both"));
    everyMatch.put("bothTextBest", text.get("bothTextBest"));
    assertThat(scores("SELECT name, $score AS score FROM Chunk WHERE name IN ['questionsOnly', 'filler1'] OR " + A
        + " ORDER BY name")).isEqualTo(everyMatch);
  }

  /**
   * Setting the scores again does not filter the rows again: an arm an index answered with its own semantics, as a full-text
   * index answers CONTAINSTEXT ignoring case, keeps every row it returned.
   */
  @Test
  void rescoringKeepsTheRowsAnIndexedArmReturned() {
    final Set<String> containsText = scores("SELECT name, 0 AS score FROM Chunk WHERE questions CONTAINSTEXT 'CAPITAL'").keySet();
    assertThat(containsText).contains("questionsOnly");
    final Set<String> expected = new HashSet<>(scores("SELECT name, $score AS score FROM Chunk WHERE " + A).keySet());
    expected.addAll(containsText);
    for (final String orderBy : new String[] { "", " ORDER BY name" })
      assertThat(scores("SELECT name, $score AS score FROM Chunk WHERE " + A + " OR questions CONTAINSTEXT 'CAPITAL'" + orderBy)
          .keySet()).as(orderBy).isEqualTo(expected);
  }

  private void insert(final String name, final String text, final String questions) {
    database.command("sql", "INSERT INTO Chunk SET name = ?, text = ?, questions = ?", name, text, questions);
  }

  private Map<String, Float> scores(final String query) {
    final Map<String, Float> scores = new HashMap<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        scores.put(row.getProperty("name"), row.<Number>getProperty("score").floatValue());
      }
    }
    return scores;
  }
}
