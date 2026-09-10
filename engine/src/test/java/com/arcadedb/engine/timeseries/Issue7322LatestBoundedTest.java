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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7322: {@code TimeSeriesGateway.latest} answered "the newest sample" by merging every shard's whole
 * range into one list, sorting it and keeping the last row - O(series) heap and O(series log series) time for
 * a question about one row, on the endpoint a Grafana single-stat panel polls. It now asks
 * {@link TimeSeriesEngine#queryDescending} for a single row, which stops walking blocks as soon as its own
 * limit is satisfied.
 * <p>
 * The two forms disagree on exactly one thing, and only on that: which row wins when several samples share the
 * newest timestamp. The whole-series scan sorted ascending (stably) and took the last of the tied rows; the
 * newest-first scan sorts descending (stably) and keeps the first. That change is deliberate and is pinned
 * below - it is the reason the swap was tracked as its own issue rather than folded into #7305.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7322LatestBoundedTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;
  private static final long STEP_MS = 1_000L;

  private TimeSeriesEngine createType(final String name, final int shards) {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + shards);
    return ((LocalTimeSeriesType) database.getSchema().getType(name)).getEngine();
  }

  /**
   * Appends {@code ticks} samples for each of {@code hosts}, one tick per {@link #STEP_MS}.
   */
  private void append(final TimeSeriesEngine engine, final int ticks, final String... hosts) throws IOException {
    final int rows = ticks * hosts.length;
    final long[] timestamps = new long[rows];
    final Object[] hostValues = new Object[rows];
    final Object[] values = new Object[rows];

    int i = 0;
    for (int t = 0; t < ticks; t++)
      for (final String host : hosts) {
        timestamps[i] = BASE_TS + (long) t * STEP_MS;
        hostValues[i] = host;
        values[i] = (double) i;
        i++;
      }
    engine.appendBatch(timestamps, new Object[][] { hostValues, values });
  }

  /**
   * {@code Object[].equals} is identity, so rows are compared by content.
   */
  private static List<Object> content(final Object[] row) {
    return row == null ? null : Arrays.asList(row);
  }

  /**
   * An INDEPENDENT oracle: the exhaustive ascending scan, which is the implementation {@code latest} used
   * before #7322. Deliberately not {@code queryDescending(..., 1, ...)} - that is the expression under test,
   * and comparing the two would assert that A equals A (claude-review on PR #7376). The two implementations
   * agree on every selection whose newest timestamp is unique, which is what the callers below construct, so
   * this is a real cross-check between two different ways of finding the same row.
   * <p>
   * Uniqueness is asserted rather than assumed: a fixture edited into a tie would otherwise start comparing
   * against the wrong row in silence, since the two implementations part company on exactly that case.
   */
  private Object[] exhaustiveNewest(final TimeSeriesEngine engine, final TagFilter tagFilter) throws IOException {
    final List<Object[]> all = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, tagFilter);
    if (all.isEmpty())
      return null;

    final Object[] newest = all.getLast();
    assertThat(all).as("this oracle is only valid where the newest timestamp is unique")
        .filteredOn(row -> (long) row[0] == (long) newest[0]).hasSize(1);
    return newest;
  }

  @Test
  void latestIsTheNewestRowOnASingleShard() throws Exception {
    final TimeSeriesEngine engine = createType("Single", 1);
    append(engine, 500, "host_0");

    final Object[] latest = TimeSeriesGateway.latest(engine, null);

    assertThat(latest).isNotNull();
    assertThat((long) latest[0]).isEqualTo(BASE_TS + 499 * STEP_MS);
    assertThat(content(latest)).isEqualTo(content(exhaustiveNewest(engine, null)));
  }

  /**
   * One host only, so every timestamp appears exactly once and the answer is a single row rather than a tie -
   * this test is about merging the two storage layers of four shards, not about the tie-break.
   */
  @Test
  void latestIsTheNewestRowAcrossShardsAndAcrossBothStorageLayers() throws Exception {
    final TimeSeriesEngine engine = createType("Layers", 4);
    append(engine, 400, "host_0");
    // Everything appended so far moves to the sealed layer; the tail that follows stays mutable, so the
    // newest-first walk has to merge both layers of every shard - and the newest row is in the SEALED one,
    // which is the direction a walk that stops at the first mutable row it finds would get wrong.
    engine.compactAll();
    append(engine, 40, "host_0");

    final Object[] latest = TimeSeriesGateway.latest(engine, null);

    assertThat(latest).isNotNull();
    // Ticks restart at 0 on the second append, so the newest timestamp is the older, now-sealed run's last tick.
    assertThat((long) latest[0]).isEqualTo(BASE_TS + 399 * STEP_MS);
    assertThat(content(latest)).isEqualTo(content(exhaustiveNewest(engine, null)));
  }

  @Test
  void latestHonoursTheTagFilter() throws Exception {
    final TimeSeriesEngine engine = createType("Tagged", 2);
    append(engine, 300, "host_0", "host_1");
    // host_2 stops earlier, so a filter on it must not answer with the other hosts' newer samples.
    append(engine, 100, "host_2");

    final TagFilter onlyHost2 = TagFilter.eq(0, "host_2");
    final Object[] latest = TimeSeriesGateway.latest(engine, onlyHost2);

    assertThat(latest).isNotNull();
    assertThat(latest[1]).isEqualTo("host_2");
    assertThat((long) latest[0]).isEqualTo(BASE_TS + 99 * STEP_MS);
    assertThat(content(latest)).isEqualTo(content(exhaustiveNewest(engine, onlyHost2)));
  }

  @Test
  void latestIsNullWhenTheSelectionHoldsNoRow() throws Exception {
    final TimeSeriesEngine engine = createType("Empty", 2);

    assertThat(TimeSeriesGateway.latest(engine, null)).isNull();

    append(engine, 10, "host_0");
    assertThat(TimeSeriesGateway.latest(engine, TagFilter.eq(0, "absent"))).isNull();
  }

  /**
   * The documented tie-break, and the one behavioural difference this issue introduces.
   * <p>
   * The tied pair is appended FIRST, while the type's round-robin append counter is still 0, so sample
   * {@code i} lands in shard {@code i} - the placement the assertions below depend on, and asserted rather
   * than assumed. Both shards then hold a row at the newest timestamp.
   * <p>
   * {@code TimeSeriesEngine.query} sorts the shard-ordered list ascending and the old {@code latest} took the
   * LAST of the tied rows - shard 1's. {@code queryDescending} sorts it descending, and both sorts are stable,
   * so the survivor at limit 1 is the FIRST in shard order - shard 0's. Asserting both halves is what makes
   * this test fail against the old implementation rather than pass against either.
   */
  @Test
  void aTieAtTheNewestTimestampGoesToTheRowTheNewestFirstScanYieldsFirst() throws Exception {
    final TimeSeriesEngine engine = createType("Tied", 2);

    final long tiedTs = BASE_TS + 100 * STEP_MS;
    engine.appendBatch(new long[] { tiedTs, tiedTs },
        new Object[][] { new Object[] { "in_shard_0", "in_shard_1" }, new Object[] { 1.0d, 2.0d } });
    // Older filler, so the answer is not simply "the only row there is".
    append(engine, 5, "older");

    // The placement the rest of this test reads as "shard order", checked rather than inferred.
    assertThat(engine.getShard(0).scanRange(tiedTs, tiedTs, null, null)).singleElement()
        .satisfies(row -> assertThat(row[1]).isEqualTo("in_shard_0"));
    assertThat(engine.getShard(1).scanRange(tiedTs, tiedTs, null, null)).singleElement()
        .satisfies(row -> assertThat(row[1]).isEqualTo("in_shard_1"));

    final Object[] latest = TimeSeriesGateway.latest(engine, null);

    assertThat(latest).isNotNull();
    assertThat((long) latest[0]).isEqualTo(tiedTs);

    // No oracle here: the tie is the one case where the two implementations disagree by design, so the
    // expected row is spelled out instead.
    assertThat(latest[1]).isEqualTo("in_shard_0");

    // And it is NOT what the whole-series ascending scan used to answer. This assertion is the regression
    // guard: restoring engine.query(...).get(size - 1) makes it fail.
    final List<Object[]> everything = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(everything.getLast()[1]).isEqualTo("in_shard_1");
    assertThat(content(latest)).isNotEqualTo(content(everything.getLast()));
  }

  /**
   * A tie-break nobody can rely on is not a tie-break: repeated calls against an unchanged layout must answer
   * the same row, and sealing the tied rows must not move the answer either.
   */
  @Test
  void theTieBreakIsStableAcrossCallsAndAcrossCompaction() throws Exception {
    final TimeSeriesEngine engine = createType("StableTie", 2);
    final long tiedTs = BASE_TS + 7 * STEP_MS;
    engine.appendBatch(new long[] { tiedTs, tiedTs },
        new Object[][] { new Object[] { "shard0", "shard1" }, new Object[] { 1.0d, 2.0d } });

    final Object[] first = TimeSeriesGateway.latest(engine, null);
    final Object[] second = TimeSeriesGateway.latest(engine, null);
    assertThat(content(first)).isEqualTo(content(second));

    engine.compactAll();

    final Object[] afterCompaction = TimeSeriesGateway.latest(engine, null);
    assertThat(content(afterCompaction)).isEqualTo(content(first));
  }
}
