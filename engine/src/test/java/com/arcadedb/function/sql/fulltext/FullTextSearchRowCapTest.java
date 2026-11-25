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
import com.arcadedb.query.ResultRowCapScope;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12016. {@code SEARCH_INDEX(...)} scores, sorts and loads every match unless the statement provably
 * consumes only the best n of them, and the HTTP endpoints used to give it that bound through the LIMIT they appended to the statement
 * text. The row cap of the caller reaches the fetch through {@link ResultRowCapScope} instead, under the conditions the statement's own
 * LIMIT would have, and without making the fetch the order of an {@code ORDER BY $score DESC} (that is a decision of the plan, which
 * must not depend on the cap): the sort stays, over the rows the fetch produced.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class FullTextSearchRowCapTest extends TestHelper {
  private static final int    DOCUMENTS = 500;
  private static final int    CAP       = 10;
  private static final String SEARCH    = "SEARCH_INDEX('Doc[content]', 'common') = true";

  private record Run(List<Result> rows, String plan) {
  }

  @BeforeEach
  void createCorpus() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.n INTEGER");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      for (int i = 0; i < DOCUMENTS; i++)
        database.newDocument("Doc").set("content", "common word" + i + " " + "common ".repeat(i % 7), "n", i).save();
    });
  }

  /** Runs the statement under the cap, and reads the rows the way a caller with that cap does: no more than the cap. */
  private Run run(final String sql, final int cap) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultRowCapScope ignored = ResultRowCapScope.open(cap); final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext() && (cap <= 0 || rows.size() < cap))
        rows.add(rs.next());
      return new Run(rows, rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
    }
  }

  private static List<Object> rids(final Run run) {
    final List<Object> rids = new ArrayList<>();
    for (final Result row : run.rows())
      rids.add(row.getIdentity().orElseThrow());
    return rids;
  }

  @Test
  void theFetchProducesOnlyTheRowsTheCallerReads() {
    final Run capped = run("SELECT FROM Doc WHERE " + SEARCH, CAP);
    assertThat(capped.plan()).contains("(best " + CAP + " needed)");
    assertThat(capped.rows()).hasSize(CAP);

    final Run uncapped = run("SELECT FROM Doc WHERE " + SEARCH, 0);
    assertThat(uncapped.plan()).doesNotContain(" needed)");
    assertThat(uncapped.rows()).hasSize(DOCUMENTS);
    // THE BEST ROWS THE FUNCTION PRODUCES ARE THE FIRST ONES OF ITS OWN ORDER
    assertThat(rids(capped)).isEqualTo(rids(uncapped).subList(0, CAP));
  }

  @Test
  void theSkipIsCountedBeforeTheCap() {
    final Run capped = run("SELECT FROM Doc WHERE " + SEARCH + " SKIP 5", CAP);
    assertThat(capped.plan()).contains("(best " + (5 + CAP) + " needed)");
    assertThat(capped.rows()).hasSize(CAP);
  }

  @Test
  void anOrderByScoreIsStillASortOfTheRowsTheFetchProduces() {
    final Run capped = run("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC", CAP);
    // THE PLAN HAS THE SORT WITH ITS BUFFER, AS THE PLAN OF A CALLER WITH NO CAP, AND THE FETCH IS BOUNDED
    assertThat(capped.plan()).contains("(best " + CAP + " needed)").contains("+ ORDER BY").contains("buffer size: " + CAP);
    assertThat(capped.rows()).hasSize(CAP);

    final Run limited = run("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT " + CAP, 0);
    assertThat(rids(capped)).containsExactlyInAnyOrderElementsOf(rids(limited));
  }

  @Test
  void aStatementWithItsOwnLimitIgnoresTheCap() {
    final Run run = run("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 7", CAP);
    assertThat(run.plan()).contains("(best 7 needed)").doesNotContain("(best " + CAP + " needed)");
    assertThat(run.rows()).hasSize(7);
  }

  @Test
  void aFetchThatCanLoseARowAfterItIsNotBounded() {
    // THE FILTER AFTER THE FETCH CAN REJECT A ROW THE BOUND COUNTED
    final Run residual = run("SELECT FROM Doc WHERE " + SEARCH + " AND n > 100", CAP);
    assertThat(residual.plan()).doesNotContain(" needed)");
    assertThat(residual.rows()).hasSize(CAP);
    for (final Result row : residual.rows())
      assertThat(row.<Integer>getProperty("n")).isGreaterThan(100);

    final Run distinct = run("SELECT DISTINCT n FROM Doc WHERE " + SEARCH, CAP);
    assertThat(distinct.plan()).doesNotContain(" needed)");
  }
}
