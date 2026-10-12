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
package com.arcadedb.query.sql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9818: repeating the same SQL text cost 13 to 18 percent more per call once its literals were extracted into
 * parameters (#8307). The two measurements here are the ratios a repeat must keep: the statement lookup of a text already
 * seen, and the whole query. Each compares the feature on against off, best of several rounds, interleaved.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue9818RepeatedLiteralTextCostBenchmark extends TestHelper {
  private static final int ROUNDS  = 7;
  private static final int LOOKUPS = 5_000_000;
  private static final int QUERIES = 200_000;

  private static final String ONE_LITERAL   = "SELECT count(*) AS c FROM V WHERE a = 77";
  private static final String FIVE_LITERALS = "SELECT FROM V WHERE a = 77 AND a > 5 AND a < 999999 AND a <> 3 AND a <> 4";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.a LONG");
    database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newVertex("V").set("a", (long) i).save();
    });
  }

  @Test
  void theLookupOfARepeatedTextCostsAFewTimesAnExactTextHit() {
    // an exact-text hit is one probe; a repeated text with extracted literals reads the remembered text and then the statement
    // under its key, under one lock, and fills a map of its own with the values. Measured: 3.5 for one literal and 4.8 for five.
    // Three locks, a scan of the text for the generated-name prefix and a copy of the map through its entry iterator (the cost
    // before #9818) measured 5.7 and 10.8. The bounds are tripwires between that cost and this one, not latency budgets
    assertThat(lookupRatio(ONE_LITERAL)).as("1 literal, lookup of a repeated text, on / off").isLessThan(4.5);
    assertThat(lookupRatio(FIVE_LITERALS)).as("5 literals, lookup of a repeated text, on / off").isLessThan(7.5);
  }

  @Test
  void aRepeatedTextRunsAboutAsFastAsWithoutParameterization() {
    // a cheap query with a cached plan is the sensitive one (1.6 microseconds per call). Measured: 1.03. The lookup and the
    // parameter reads before #9818 measured 1.12, and lexing the text again on every call (before #9714) is far above that
    assertThat(queryRatio("SELECT FROM V WHERE a = 77")).as("repeated query, on / off").isLessThan(1.07);
  }

  /**
   * Switches the mode with empty caches. Without that, the text cached as written by the run with the feature off is found by
   * the exact-text lookup that comes first, and the run with it on measures the same path twice.
   */
  private void selectMode(final boolean parameterize) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION, parameterize);
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getStatementCache().clear();
    db.getExecutionPlanCache().invalidate();
  }

  private double lookupRatio(final String text) {
    final DatabaseInternal db = (DatabaseInternal) database;
    long bestOn = Long.MAX_VALUE;
    long bestOff = Long.MAX_VALUE;
    for (int round = -1; round < ROUNDS; round++) {
      final long off = lookups(db, text, false);
      final long on = lookups(db, text, true);
      if (round >= 0) {
        bestOff = Math.min(bestOff, off);
        bestOn = Math.min(bestOn, on);
      }
    }
    return (double) bestOn / bestOff;
  }

  private long lookups(final DatabaseInternal db, final String text, final boolean parameterize) {
    selectMode(parameterize);
    // the first sightings decide the policy and build the parameterized statement
    for (int i = 0; i < 3; i++)
      db.getStatementCache().getParameterized(text);
    long sink = 0;
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    for (int i = 0; i < LOOKUPS; i++)
      sink += db.getStatementCache().getParameterized(text).cacheKey().length();
    final long ms = Math.max(1, watch.effectiveMs());
    assertThat(sink).isPositive();
    return ms;
  }

  private double queryRatio(final String text) {
    long bestOn = Long.MAX_VALUE;
    long bestOff = Long.MAX_VALUE;
    for (int round = -1; round < ROUNDS; round++) {
      final long off = queries(text, false);
      final long on = queries(text, true);
      if (round >= 0) {
        bestOff = Math.min(bestOff, off);
        bestOn = Math.min(bestOn, on);
      }
    }
    return (double) bestOn / bestOff;
  }

  private long queries(final String text, final boolean parameterize) {
    selectMode(parameterize);
    // the first runs of a text decide the policy, build the parameterized statement and plan it
    for (int i = 0; i < 3; i++)
      try (final ResultSet rs = database.query("sql", text)) {
        while (rs.hasNext())
          rs.next();
      }
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    for (int i = 0; i < QUERIES; i++)
      try (final ResultSet rs = database.query("sql", text)) {
        while (rs.hasNext())
          rs.next();
      }
    return Math.max(1, watch.effectiveMs());
  }
}
