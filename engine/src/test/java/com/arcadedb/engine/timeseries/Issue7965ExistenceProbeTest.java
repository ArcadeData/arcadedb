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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7965: an existence check answered by the sealed layer must not read the mutable bucket.
 * <p>
 * {@code hasRowsInRange} backs {@code GET /prom/api/v1/label/__name__/values} scoped to a range (#7709) and is
 * meant to be cheap: the sealed layer drops a block whose directory entry puts it outside the window, so a "yes"
 * costs one block read and a "no" costs none. It used to be folded over {@link TimeSeriesEngine#forEachRow} with
 * a visitor stopping on the first row, and #7897 made that fold pay a FULL mutable-bucket scan per shard whatever
 * the visitor did: the two layers are read in one lock window, up front, because a compaction landing between
 * them would seal the bucket's rows into blocks the sealed walk then hands over as well.
 * <p>
 * The probe no longer goes through {@code forEachRow} at all. It has a walk of its own that visits the layers one
 * after the other and stops at the first row either produces, under the shard's {@code compactionLock} read lock -
 * which {@code forEachRow} cannot hold, because the visitor there is the CALLER's code and its cost is unbounded
 * from inside the shard, while here the "visitor" is a constant {@code false} and the whole probe is bounded by
 * one block decode plus one page.
 * <p>
 * <b>Why the cheap assertion is paired with an expensive one.</b> Asserting that a sealed-answered probe scanned
 * zero mutable pages would have passed before this change as well - not because the bucket went unread, but
 * because {@code forEachRow} passed {@code null} metrics to the bucket scan, so the pages it read were never
 * counted. {@link #aMutableAnswerIsChargedForThePagesItReads} is what says the counter is wired to the probe's
 * own bucket read, and only with it does a zero in {@link #aSealedAnswerReadsNoMutablePage} mean the bucket was
 * not opened.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7965ExistenceProbeTest extends TestHelper {

  private static final long BASE_TS   = 1_700_000_000_000L;
  private static final long STEP_MS   = 10L;
  private static final long BUCKET_MS = 1_000L;

  /** The defect: the sealed layer has the answer, and the mutable bucket is never opened to confirm it. */
  @Test
  void aSealedAnswerReadsNoMutablePage() throws Exception {
    final TimeSeriesEngine engine = createMetric("sealed_answer", 1);
    append(engine, 0, 500);
    engine.compactAll();
    // Enough rows to span several bucket pages, all INSIDE the queried range, so a scan of them is not something
    // the page headers could have skipped.
    append(engine, 100_000, 5_000);

    final AggregationMetrics metrics = new AggregationMetrics();
    assertThat(engine.hasRowsInRange(BASE_TS, Long.MAX_VALUE, metrics)).isTrue();

    assertThat(metrics.getFastPathBlocks() + metrics.getSlowPathBlocks())
        .as("one block read is what the answer costs")
        .isEqualTo(1);
    assertThat(metrics.getScannedPages() + metrics.getSkippedPages())
        .as("the mutable bucket is not opened at all once the sealed layer has answered")
        .isZero();
  }

  /**
   * The anti-vacuity half: when the answer DOES come from the mutable bucket, the pages it took to find it are
   * counted. Without this the zero above would say nothing about whether the bucket was read.
   */
  @Test
  void aMutableAnswerIsChargedForThePagesItReads() throws Exception {
    final TimeSeriesEngine engine = createMetric("mutable_answer", 1);
    append(engine, 0, 500);

    final AggregationMetrics metrics = new AggregationMetrics();
    assertThat(engine.hasRowsInRange(BASE_TS, Long.MAX_VALUE, metrics)).isTrue();

    assertThat(metrics.getFastPathBlocks() + metrics.getSlowPathBlocks())
        .as("nothing is sealed yet, so no block answers")
        .isZero();
    assertThat(metrics.getScannedPages()).as("the bucket page the row was found on").isEqualTo(1);
  }

  /**
   * A "no" consults BOTH layers, and says so: the sealed layer drops every block on its directory entry and the
   * bucket drops every page on its header, but neither is skipped as a layer.
   */
  @Test
  void aNoAnswerConsultsBothLayers() throws Exception {
    final TimeSeriesEngine engine = createMetric("no_answer", 1);
    append(engine, 0, 500);
    engine.compactAll();
    append(engine, 100_000, 500);

    final AggregationMetrics metrics = new AggregationMetrics();
    assertThat(engine.hasRowsInRange(BASE_TS + 10_000_000, BASE_TS + 20_000_000, metrics)).isFalse();

    assertThat(metrics.getFastPathBlocks() + metrics.getSlowPathBlocks())
        .as("no block is read to answer 'nothing here'")
        .isZero();
    assertThat(metrics.getScannedPages()).as("no page is examined either").isZero();
    assertThat(metrics.getSkippedPages()).as("but the bucket WAS consulted, page header by page header")
        .isGreaterThanOrEqualTo(1);
  }

  /** The answers themselves, over every combination of the two layers, and over more than one shard. */
  @Test
  void theAnswerIsTheOneAFullScanWouldGive() throws Exception {
    final TimeSeriesEngine engine = createMetric("answers", 3);
    append(engine, 0, 500);
    engine.compactAll();
    append(engine, 900_000, 100);

    assertRangeAgreesWithScan(engine, Long.MIN_VALUE, Long.MAX_VALUE);
    assertRangeAgreesWithScan(engine, BASE_TS, BASE_TS + 100);
    assertRangeAgreesWithScan(engine, BASE_TS + 900_000, Long.MAX_VALUE);
    assertRangeAgreesWithScan(engine, BASE_TS + 10_000_000, BASE_TS + 20_000_000);
    // A window between the sealed rows and the mutable ones: both layers hold rows, neither holds one here.
    assertRangeAgreesWithScan(engine, BASE_TS + 500_000, BASE_TS + 600_000);

    final TimeSeriesEngine empty = createMetric("answers_empty", 2);
    assertThat(empty.hasRowsInRange(Long.MIN_VALUE, Long.MAX_VALUE, null))
        .as("a type holding no sample at all")
        .isFalse();
  }

  /**
   * A single sample on the far edge of the mutable bucket: the probe must walk past every page that cannot hold it
   * rather than stop at the first one, which is what a {@code hasNext()} on a lazy iterator gets wrong if the
   * iterator is not advanced across empty pages.
   */
  @Test
  void aLoneMutableRowIsFoundBehindManyPages() throws Exception {
    final TimeSeriesEngine engine = createMetric("lone_row", 1);
    append(engine, 0, 5_000);

    final long lastTs = BASE_TS + (5_000 - 1) * STEP_MS;
    final AggregationMetrics metrics = new AggregationMetrics();
    assertThat(engine.hasRowsInRange(lastTs, lastTs, metrics)).isTrue();
    assertThat(metrics.getScannedPages()).as("exactly the one page that holds it").isEqualTo(1);
    assertThat(metrics.getSkippedPages()).as("the pages before it are dropped on their headers")
        .isGreaterThanOrEqualTo(1);
  }

  // --- helpers ---

  private void assertRangeAgreesWithScan(final TimeSeriesEngine engine, final long fromTs, final long toTs)
      throws IOException {
    final boolean scanned = !engine.forEachRow(fromTs, toTs, new int[0], null, null, row -> false);
    assertThat(engine.hasRowsInRange(fromTs, toTs, null))
        .as("range [%d, %d]", fromTs, toTs)
        .isEqualTo(scanned);
  }

  private TimeSeriesEngine createMetric(final String typeName, final int shards) {
    new TimeSeriesTypeBuilder((DatabaseInternal) database)
        .withName(typeName)
        .withTimestamp("ts")
        .withTag("host", Type.STRING)
        .withField("value", Type.DOUBLE)
        .withShards(shards)
        .withCompactionBucketInterval(BUCKET_MS)
        .create();
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private void append(final TimeSeriesEngine engine, final long startOffsetMs, final int count) throws IOException {
    final long[] timestamps = new long[count];
    final Object[] hosts = new Object[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      timestamps[i] = BASE_TS + startOffsetMs + i * STEP_MS;
      hosts[i] = "host_" + (i % 4);
      values[i] = (double) i;
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }
}
