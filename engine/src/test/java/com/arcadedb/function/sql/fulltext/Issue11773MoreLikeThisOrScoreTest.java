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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11773: since #11618 an OR evaluates every arm that sets {@code $score} and gives a matching record the
 * best of their scores, but only SEARCH_INDEX / SEARCH_FIELDS were known to set it. A record matched only by a
 * SEARCH_INDEX_MORE / SEARCH_FIELDS_MORE arm was then given {@code $score = 0}, and its {@code $similarity} was not combined.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11773MoreLikeThisOrScoreTest extends TestHelper {
  private static final String CONFIG = "{'minTermFreq': 1, 'minDocFreq': 1}";

  private RID source;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");
      source = insert("graph database", "graph database engine with vertices and edges");
      insert("graph engine", "a graph database engine for vertices");
      insert("cooking", "pasta recipes and sauces");
      insert("graph theory", "vertices edges graph database");
    });
  }

  private String indexMore() {
    return "SEARCH_INDEX_MORE('Article[title,body]', [" + source + "], " + CONFIG + ") = true";
  }

  private String fieldsMore() {
    return "SEARCH_FIELDS_MORE(['title', 'body'], [" + source + "], " + CONFIG + ") = true";
  }

  @Test
  void aSearchIndexMoreArmKeepsItsScoreAndSimilarityInAScoringOr() {
    final Map<String, List<Float>> alone = scores(indexMore());
    assertThat(alone).isNotEmpty();
    assertThat(alone.values()).allSatisfy(s -> assertThat(s.get(0)).isGreaterThan(0f));

    // An arm that matches nothing: every record keeps the more-like-this score and similarity.
    assertThat(scores(indexMore() + " OR SEARCH_INDEX('Article[title,body]', 'zzzz')")).isEqualTo(alone);
    assertThat(scores("SEARCH_INDEX('Article[title,body]', 'zzzz') OR " + indexMore())).isEqualTo(alone);
    // Inside an AND.
    assertThat(scores("(" + indexMore() + " OR SEARCH_INDEX('Article[title,body]', 'zzzz')) AND title IS NOT NULL")).isEqualTo(
        alone);
  }

  @Test
  void aSearchFieldsMoreArmKeepsItsScoreAndSimilarityInAScoringOr() {
    final Map<String, List<Float>> alone = scores(fieldsMore());
    assertThat(alone).isNotEmpty();
    assertThat(alone.values()).allSatisfy(s -> assertThat(s.get(0)).isGreaterThan(0f));

    assertThat(scores(fieldsMore() + " OR SEARCH_FIELDS(['title', 'body'], 'zzzz')")).isEqualTo(alone);
    assertThat(scores("SEARCH_INDEX('Article[title,body]', 'zzzz') OR " + fieldsMore())).isEqualTo(alone);
  }

  @Test
  void aRecordMatchedByBothKindsOfArmGetsTheBestScore() {
    final Map<String, List<Float>> more = scores(indexMore());
    final Map<String, List<Float>> pasta = scores("SEARCH_INDEX('Article[title,body]', 'pasta')");
    assertThat(pasta).containsOnlyKeys("cooking");

    final Map<String, Float> expected = new HashMap<>();
    more.forEach((title, s) -> expected.put(title, s.get(0)));
    pasta.forEach((title, s) -> expected.merge(title, s.get(0), Math::max));

    for (final String where : new String[] { "SEARCH_INDEX('Article[title,body]', 'pasta') OR " + indexMore(),
        indexMore() + " OR SEARCH_INDEX('Article[title,body]', 'pasta')" }) {
      final Map<String, List<Float>> actual = scores(where);
      assertThat(actual.keySet()).as(where).isEqualTo(expected.keySet());
      expected.forEach((title, score) -> assertThat(actual.get(title).get(0)).as(where + " " + title).isEqualTo(score));
      // the similarity of a more-like-this match is the one it has alone, whatever the arm order
      more.forEach((title, s) -> assertThat(actual.get(title).get(1)).as(where + " " + title).isEqualTo(s.get(1)));
    }
  }

  /** Two more-like-this arms: the second one, not matching, must not reset the similarity the first one gave. */
  @Test
  void aNonMatchingMoreLikeThisArmDoesNotResetTheSimilarity() {
    final Map<String, List<Float>> alone = scores(indexMore());
    final String cooking = database.query("sql", "SELECT @rid AS rid FROM Article WHERE title = 'cooking'").next()
        .getProperty("rid").toString();
    final String other = "SEARCH_INDEX_MORE('Article[title,body]', [" + cooking + "], " + CONFIG + ") = true";
    final Map<String, List<Float>> otherAlone = scores(other);

    final Map<String, List<Float>> expected = new HashMap<>();
    expected.putAll(alone);
    otherAlone.forEach((title, s) -> expected.merge(title, s,
        (a, b) -> List.of(Math.max(a.get(0), b.get(0)), Math.max(a.get(1), b.get(1)))));

    assertThat(scores(indexMore() + " OR " + other)).isEqualTo(expected);
    assertThat(scores(other + " OR " + indexMore())).isEqualTo(expected);
  }

  @Test
  void anOrderByScoreRanksTheMoreLikeThisMatchesByTheirScore() {
    final Map<String, List<Float>> alone = scores(indexMore());
    final float best = alone.values().stream().map(s -> s.get(0)).max(Float::compare).orElseThrow();
    try (final ResultSet rs = database.query("sql",
        "SELECT title, $score AS s FROM Article WHERE " + indexMore() + " OR SEARCH_INDEX('Article[title,body]', 'zzzz')"
            + " ORDER BY $score DESC")) {
      assertThat(rs.next().<Number>getProperty("s").floatValue()).isEqualTo(best);
    }
  }

  private RID insert(final String title, final String body) {
    return database.newDocument("Article").set("title", title).set("body", body).save().getIdentity();
  }

  /** title -> [score, similarity]; similarity is NaN when the query does not produce one. */
  private Map<String, List<Float>> scores(final String where) {
    final Map<String, List<Float>> result = new HashMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT title, $score AS s, $similarity AS sim FROM Article WHERE " + where)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Object sim = row.getProperty("sim");
        result.put(row.getProperty("title"), List.of(row.<Number>getProperty("s").floatValue(),
            sim instanceof Number n ? n.floatValue() : Float.NaN));
      }
    }
    return result;
  }
}
