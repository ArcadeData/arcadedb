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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7733: a projection that left out a filtered tag column answered nothing, and WHICH rows it dropped
 * depended on where the data happened to live.
 * <p>
 * {@code POST /ts/{database}/query} with {@code fields:["value"]} and {@code tags:{"host":"web1"}} returned
 * {@code {"rows":[],"count":0,"truncated":false}} with HTTP 200 - indistinguishable, to the caller, from a filter
 * that matched nothing - and adding {@code "host"} to {@code fields} returned the rows. The mutable bucket and
 * sealed SLOW_PATH blocks both refused the condition; sealed FAST_PATH blocks, which decide on the block's tag
 * metadata and never look at the projection, answered it. So the answer to one unchanged query changed as
 * compaction moved the data between them.
 * <p>
 * The reason given for the refusal - that the row handed back would not carry the column - described the wrong
 * row. The filter is evaluated BEFORE the projection is applied: off the page in the mutable bucket, and off the
 * decompressed block columns in the sealed one. So the fix is not a wider contract, it is the filter reading what
 * it always read: the mutable scans take the condition straight to the page, and the sealed scans decompress the
 * filter's own columns alongside the projection and narrow the row once it has passed ({@code TagProjection}).
 * <p>
 * These tests pin the property the bug broke: <b>one query, one answer, on every layer</b>.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7733">issue #7733</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7733ProjectionOmittingATagColumnTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;

  /** Non-timestamp column indices: host is 0, value is 1. The projection asks for the value alone. */
  private static final int[] VALUE_ONLY = new int[] { 1 };
  private static final int   HOST       = 0;

  private TimeSeriesEngine create(final String typeName) {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  /** Two hosts interleaved, so a sealed block of them is a SLOW_PATH block: its tag column is not single-valued. */
  private static void appendTwoHosts(final TimeSeriesEngine engine, final long firstTs, final int pairs)
      throws IOException {
    final long[] timestamps = new long[pairs * 2];
    final Object[] hosts = new Object[pairs * 2];
    final Object[] values = new Object[pairs * 2];
    for (int i = 0; i < pairs; i++) {
      timestamps[i * 2] = firstTs + i * 2L;
      hosts[i * 2] = "web1";
      values[i * 2] = (double) i;
      timestamps[i * 2 + 1] = firstTs + i * 2L + 1;
      hosts[i * 2 + 1] = "web2";
      values[i * 2 + 1] = 100.0d + i;
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  private static List<Double> valuesOf(final List<Object[]> rows) {
    final List<Double> values = new ArrayList<>(rows.size());
    for (final Object[] row : rows) {
      assertThat(row).as("the projection still hands back only what was asked for: {timestamp, value}").hasSize(2);
      values.add((Double) row[1]);
    }
    return values;
  }

  @Test
  void theMutableBucketAppliesAConditionOnAColumnTheProjectionLeavesOut() throws Exception {
    final TimeSeriesEngine engine = create("Mutable");
    appendTwoHosts(engine, BASE_TS, 3);

    database.begin();
    try {
      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, VALUE_ONLY, TagFilter.eq(HOST, "web1"));
      assertThat(valuesOf(rows))
          .as("the filter is evaluated on the page, where the tag column is there whatever the projection asks for")
          .containsExactly(0.0d, 1.0d, 2.0d);
    } finally {
      database.commit();
    }
  }

  @Test
  void aSealedSlowPathBlockAnswersTheSameQueryTheSameWay() throws Exception {
    final TimeSeriesEngine engine = create("SlowPath");
    appendTwoHosts(engine, BASE_TS, 3);
    engine.compactAll();

    database.begin();
    try {
      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, VALUE_ONLY, TagFilter.eq(HOST, "web1"));
      assertThat(valuesOf(rows))
          .as("a mixed-tag block is filtered row by row, and the rows it tests carry the filter's columns")
          .containsExactly(0.0d, 1.0d, 2.0d);
    } finally {
      database.commit();
    }
  }

  @Test
  void aSealedFastPathBlockStillAnswersIt() throws Exception {
    final TimeSeriesEngine engine = create("FastPath");
    engine.appendBatch(new long[] { BASE_TS, BASE_TS + 1 },
        new Object[][] { new Object[] { "web1", "web1" }, new Object[] { 1.0d, 2.0d } });
    engine.compactAll();

    database.begin();
    try {
      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, VALUE_ONLY, TagFilter.eq(HOST, "web1"));
      assertThat(valuesOf(rows))
          .as("the layer that was already right stays right: this is the answer the other two now agree with")
          .containsExactly(1.0d, 2.0d);
    } finally {
      database.commit();
    }
  }

  /**
   * The divergence itself: sealed rows and mutable rows of one type, read by one query. Before the fix this
   * returned the sealed fast-path rows only, so the answer moved every time compaction ran.
   */
  @Test
  void oneQuerySpanningBothLayersReturnsEveryMatchingRow() throws Exception {
    final TimeSeriesEngine engine = create("BothLayers");
    appendTwoHosts(engine, BASE_TS, 3);
    engine.compactAll();
    appendTwoHosts(engine, BASE_TS + 1_000, 2);

    database.begin();
    try {
      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, VALUE_ONLY, TagFilter.eq(HOST, "web1"));
      assertThat(valuesOf(rows)).containsExactly(0.0d, 1.0d, 2.0d, 0.0d, 1.0d);
    } finally {
      database.commit();
    }
  }

  /** The bounded readers take the same filter through their own scans, so they cannot drift from {@code query}. */
  @Test
  void theBoundedAndStreamingReadersAgreeWithIt() throws Exception {
    final TimeSeriesEngine engine = create("EveryReader");
    appendTwoHosts(engine, BASE_TS, 3);
    engine.compactAll();
    appendTwoHosts(engine, BASE_TS + 1_000, 2);

    database.begin();
    try {
      final TagFilter web1 = TagFilter.eq(HOST, "web1");

      assertThat(valuesOf(engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, VALUE_ONLY, web1, 0, null)))
          .containsExactly(0.0d, 1.0d, 2.0d, 0.0d, 1.0d);

      assertThat(valuesOf(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, VALUE_ONLY, web1, 0, null)))
          .containsExactly(1.0d, 0.0d, 2.0d, 1.0d, 0.0d);

      final List<Object[]> iterated = new ArrayList<>();
      engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, VALUE_ONLY, web1).forEachRemaining(iterated::add);
      assertThat(valuesOf(iterated)).containsExactlyInAnyOrder(0.0d, 1.0d, 2.0d, 0.0d, 1.0d);

      final List<Object[]> visited = new ArrayList<>();
      engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, VALUE_ONLY, web1, null, row -> visited.add(row));
      assertThat(valuesOf(visited)).containsExactlyInAnyOrder(0.0d, 1.0d, 2.0d, 0.0d, 1.0d);
    } finally {
      database.commit();
    }
  }

  /** A filter that matches nothing still matches nothing: the fix widens what is evaluated, not what passes. */
  @Test
  void aConditionNoRowSatisfiesStillAnswersNothing() throws Exception {
    final TimeSeriesEngine engine = create("NoMatch");
    appendTwoHosts(engine, BASE_TS, 3);
    engine.compactAll();
    appendTwoHosts(engine, BASE_TS + 1_000, 2);

    database.begin();
    try {
      assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, VALUE_ONLY, TagFilter.eq(HOST, "web9"))).isEmpty();
    } finally {
      database.commit();
    }
  }

  /** A projection that DOES carry the tag column is unchanged, and the two answers are the same rows. */
  @Test
  void aProjectionCarryingTheTagColumnAnswersTheSameRows() throws Exception {
    final TimeSeriesEngine engine = create("WidestProjection");
    appendTwoHosts(engine, BASE_TS, 3);
    engine.compactAll();
    appendTwoHosts(engine, BASE_TS + 1_000, 2);

    database.begin();
    try {
      final List<Object[]> withTag = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, new int[] { HOST, 1 },
          TagFilter.eq(HOST, "web1"));
      final List<Object[]> withoutTag = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, VALUE_ONLY,
          TagFilter.eq(HOST, "web1"));

      assertThat(withTag).hasSameSizeAs(withoutTag);
      for (int i = 0; i < withTag.size(); i++) {
        assertThat(withTag.get(i)[0]).isEqualTo(withoutTag.get(i)[0]);
        assertThat(withTag.get(i)[1]).as("{timestamp, host, value}").isEqualTo("web1");
        assertThat(withTag.get(i)[2]).isEqualTo(withoutTag.get(i)[1]);
      }
    } finally {
      database.commit();
    }
  }
}
