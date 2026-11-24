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
package com.arcadedb.function.sql.text;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code SELECT ... FROM Chunk WHERE SEARCH_INDEX(...) = true ORDER BY $score DESC LIMIT k} keeps only the best {@code k} matches
 * of each bucket in the search (issue #9895). The answer must be the first {@code k} rows of the complete ranking, for every
 * {@code k} (one, a boundary of the matches, more than the matches) and with many records tied on one score: the best first, equal
 * scores in ascending RID order, which is how {@code FullTextFunctionSearch} documents the order it hands the plan. The model is
 * the unlimited answer of the same statement sorted by score then RID.
 */
class SearchIndexTopKOracleTest extends TestHelper {
  private static final String[] TEXTS = { "alpha beta", "alpha", "beta beta alpha", "alpha gamma gamma gamma", "beta", "delta",
      "alpha beta gamma delta epsilon", "gamma" };

  private record Ranked(RID rid, float score) {
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      final Random random = new Random(11);
      for (int i = 0; i < 240; i++)
        database.newDocument("Doc").set("content", TEXTS[random.nextInt(TEXTS.length)]).save();
    });
  }

  private List<Ranked> ranking(final String query, final String tail) {
    final List<Ranked> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, $score AS s FROM Doc WHERE SEARCH_INDEX('Doc[content]', :q) = true ORDER BY $score DESC" + tail,
        Map.of("q", query))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(new Ranked(row.getProperty("rid"), row.getProperty("s")));
      }
    }
    return rows;
  }

  @Test
  void limitKeepsTheFirstRowsOfTheCompleteRankingWithManyTies() {
    for (final String query : List.of("alpha beta", "alpha", "beta gamma", "delta epsilon alpha")) {
      final List<Ranked> model = new ArrayList<>(ranking(query, ""));
      model.sort(Comparator.comparing(Ranked::score, Comparator.reverseOrder())
          .thenComparing(r -> r.rid().getBucketId()).thenComparing(r -> r.rid().getPosition()));
      assertThat(model.size()).as(query).isGreaterThan(60);
      assertThat(model.stream().map(Ranked::score).distinct().count()).as("distinct scores of " + query).isLessThan(model.size() / 4);

      for (final int k : new int[] { 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, model.size() - 1, model.size(), model.size() + 1, 1000 }) {
        final List<Ranked> got = ranking(query, " LIMIT " + k);
        assertThat(got).as("%s LIMIT %d", query, k).containsExactlyElementsOf(model.subList(0, Math.min(k, model.size())));
      }
    }
  }

  @Test
  void skipAndLimitSliceTheCompleteRanking() {
    final List<Ranked> model = new ArrayList<>(ranking("alpha beta", ""));
    model.sort(Comparator.comparing(Ranked::score, Comparator.reverseOrder())
        .thenComparing(r -> r.rid().getBucketId()).thenComparing(r -> r.rid().getPosition()));
    for (final int skip : new int[] { 0, 1, 7, 50 })
      for (final int limit : new int[] { 1, 10, 33 })
        assertThat(ranking("alpha beta", " SKIP " + skip + " LIMIT " + limit)).as("SKIP %d LIMIT %d", skip, limit)
            .containsExactlyElementsOf(model.subList(skip, skip + limit));
  }
}
