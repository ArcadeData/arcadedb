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
package com.arcadedb.engine.timeseries.promql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.promql.PromQLResult.InstantVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7696: {@code PromQLEvaluator} read its selectors through {@code TimeSeriesEngine#iterateQuery}, whose
 * own javadoc says the sealed layer materialises every matching row of the range before the caller sees the
 * first one - so an instant or range selector over a wide lookback held the whole window in heap to produce an
 * answer that is only O(series).
 * <p>
 * The fix folds rows straight from {@link com.arcadedb.engine.timeseries.TimeSeriesEngine#forEachRow} instead,
 * which is the same swap {@code Issue7354BoundedTagScanTest} pins for the label-discovery endpoints. That swap
 * is not a drop-in replacement here the way it is there, because {@code forEachRow} gives up something
 * {@code iterateQuery} used to guarantee for free: rows arrive shard by shard, NOT merged by timestamp, and
 * {@code TimeSeriesEngine#appendBatch} round-robins samples across shards regardless of their tags - so a
 * SINGLE label combination's samples are scattered across every shard, and the order they reach the visitor is
 * no longer chronological. Two things in {@code PromQLEvaluator} silently depended on that chronological order:
 * <ul>
 *   <li>{@code evaluateVectorSelector}'s "keep the latest sample" fold used to just overwrite on every row,
 *   which only picks out the true latest because {@code iterateQuery} visited rows oldest-to-newest. Folding
 *   the same way over {@code forEachRow} would keep whichever row happened to be visited LAST across all
 *   shards, not the one with the greatest timestamp.</li>
 *   <li>{@code evaluateMatrixSelector}'s per-series point list is read by {@code PromQLFunctions.rate}/
 *   {@code irate}/{@code increase}, which index its first, last and consecutive elements assuming ascending
 *   time order. A list built by appending in scan order would hand them the wrong pair.</li>
 * </ul>
 * These tests are built so the naive swap - fold in scan order, keep overwriting, don't re-sort - gives an
 * observably WRONG answer, not just a differently-shaped correct one: every sample below shares one label
 * combination, spread over 2 shards by a single {@code appendBatch} call, with the true latest / most recent
 * pair placed so the shard-by-shard scan visits it before its own last row.
 * <p>
 * A third case pins a design decision the swap also had to get right: {@code excludesEverySeries}'s contract -
 * that a malformed or ReDoS-shaped regex matcher must propagate rather than read as an empty result - has to
 * keep holding once the row-level post-filter that raises it runs INSIDE the {@code forEachRow} visitor, on the
 * same call stack as the {@code IOException} the scan itself can throw. A catch that is too broad there would
 * silently turn a rejected query into an empty one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7696PromQLForEachRowOrderingTest extends TestHelper {

  @Test
  void theInstantVectorPicksTheTrueLatestSampleNotTheLastOneVisited() throws IOException {
    // index -> shard (round robin, base 0): 0,2,4 -> shard 0; 1,3 -> shard 1. The true latest sample (ts=4000)
    // lands on shard 0, which forEachRow visits BEFORE shard 1 - so the last row the scan visits overall is
    // ts=3000 on shard 1, not the true latest. A fold that just overwrites on every row would report THAT one.
    database.command("sql",
        "CREATE TIMESERIES TYPE Latest7696 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Latest7696");

    database.begin();
    tsType.getEngine().appendBatch(new long[] { 0L, 1_000L, 2_000L, 3_000L, 4_000L },
        new Object[][] {
            { "h1", "h1", "h1", "h1", "h1" },
            { 100.0, 101.0, 102.0, 103.0, 104.0 }
        });
    database.commit();

    final PromQLExpr expr = new PromQLParser("Latest7696{host=\"h1\"}").parse();
    final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateInstant(expr, 5_000L);

    assertThat(result).isInstanceOf(InstantVector.class);
    final InstantVector iv = (InstantVector) result;
    assertThat(iv.samples()).hasSize(1);
    final VectorSample sample = iv.samples().getFirst();
    assertThat(sample.timestampMs()).as("the true latest timestamp").isEqualTo(4_000L);
    assertThat(sample.value()).as("the value paired with the true latest sample, not the last one scanned")
        .isEqualTo(104.0);
  }

  @Test
  void theMatrixSelectorRestoresTimestampOrderAcrossShardsForIrate() throws IOException {
    // Same round-robin placement as above. irate() reads the LAST TWO points of the series in chronological
    // order. In scan order (shard 0 fully, then shard 1) the last two rows visited are ts=3000/v=0 and
    // ts=5000/v=100 - a 2-second gap. In true chronological order they are ts=4000/v=0 and ts=5000/v=100 - a
    // 1-second gap. Both give a diff of 100, so the two orderings produce two different, unambiguous rates
    // (50 vs 100) rather than coincidentally agreeing.
    database.command("sql",
        "CREATE TIMESERIES TYPE Irate7696 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Irate7696");

    database.begin();
    tsType.getEngine().appendBatch(new long[] { 0L, 1_000L, 2_000L, 3_000L, 4_000L, 5_000L },
        new Object[][] {
            { "h1", "h1", "h1", "h1", "h1", "h1" },
            { 0.0, 0.0, 0.0, 0.0, 0.0, 100.0 }
        });
    database.commit();

    final PromQLExpr expr = new PromQLParser("irate(Irate7696{host=\"h1\"}[6s])").parse();
    final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateInstant(expr, 5_000L);

    assertThat(result).isInstanceOf(InstantVector.class);
    final InstantVector iv = (InstantVector) result;
    assertThat(iv.samples()).hasSize(1);
    assertThat(iv.samples().getFirst().value())
        .as("the 1-second, chronologically-correct gap, not the 2-second scan-order gap")
        .isEqualTo(100.0);
  }

  @Test
  void aMalformedRegexOnAColumnTheTypeDoesDeclareStillPropagatesRatherThanReadingAsEmpty() {
    // matchesPostFilters() - which validates =~/!~ patterns via compilePattern() - now runs INSIDE the
    // forEachRow visitor, on the same call stack as the scan's own IOException. The catch around that scan
    // must stay narrow enough that this still throws instead of being read as an empty vector.
    database.command("sql",
        "CREATE TIMESERIES TYPE BadRegex7696 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.command("sql", "INSERT INTO BadRegex7696 SET ts = 1000, host = 'h1', value = 1.0");

    assertThatThrownBy(() -> new PromQLEvaluator(getDatabaseInternal())
        .evaluateInstant(new PromQLParser("BadRegex7696{host=~\"(a+)+\"}").parse(), 5_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ReDoS");
  }

  private DatabaseInternal getDatabaseInternal() {
    return (DatabaseInternal) database;
  }
}
