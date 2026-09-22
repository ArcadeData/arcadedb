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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8190: a caller mistake must not change its exception type because of how many shards the type has.
 * <p>
 * {@code aggregateMulti} fans the sealed reads out over {@code shardExecutor} only when {@code shardCount > 1};
 * with one shard the identical work runs inline. So an {@code IllegalArgumentException} - a {@code columnIndex}
 * that names no value column, or one out of range - reached the caller as itself on a 1-shard type and, on a
 * multi-shard one, as {@code IOException("Parallel shard aggregation failed", <the real cause>)}. The two map to
 * a 400 / {@code INVALID_ARGUMENT} and a 500 / {@code INTERNAL} respectively, so the same mistake was a client
 * error on one declaration and a server fault on another, with the message one {@code getCause()} away.
 * <p>
 * Not reachable from a wire protocol or from the SQL push-down - {@code requireAggregatableColumn} and the
 * planner both refuse a TIMESTAMP column before the engine is asked, which
 * {@code Issue8140AggregationColumnIndexConventionTest} pins. It is reachable for an EMBEDDED caller building a
 * request by hand, which is exactly the caller with no 400 to read.
 * <p>
 * The wrapping is older than #8140: {@code findNonTsColumnSchemaIndex} has always refused an out-of-range index
 * the same way, and took the same route. Both shapes are asserted below, on 1 shard and on 4, so the fix is
 * pinned as "the answer does not depend on the shard count" rather than as one exception type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8190">issue #8190</a>
 */
class Issue8190MultiShardAggregationCallerErrorTest extends TestHelper {

  private static final long TS   = 1_700_000_000_000L;
  private static final long HOUR = 3_600_000L;

  /** Row position 0 is the timestamp: refused by name, whatever the shard count. */
  @Test
  void aTimestampRowIndexIsTheSameCallerErrorOnOneShardAndOnFour() throws Exception {
    final TimeSeriesEngine single = seriesWith("Single8190", 1);
    final TimeSeriesEngine sharded = seriesWith("Sharded8190", 4);

    final List<MultiColumnAggregationRequest> overTheTimestamp =
        List.of(new MultiColumnAggregationRequest(0, AggregationType.SUM, "sum_ts"));

    assertThatThrownBy(() -> single.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, overTheTimestamp, HOUR, null))
        .as("1 shard runs the sealed read inline, so the refusal always arrived as itself")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not name a value column");

    assertThatThrownBy(() -> sharded.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, overTheTimestamp, HOUR, null))
        .as("4 shards fan out, and used to answer IOException for the identical mistake")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not name a value column")
        .isNotInstanceOf(IOException.class);
  }

  /** The pre-existing shape of the same wrapping: a row index past the last value column. */
  @Test
  void anOutOfRangeRowIndexIsTheSameCallerErrorOnOneShardAndOnFour() throws Exception {
    final TimeSeriesEngine single = seriesWith("SingleOor8190", 1);
    final TimeSeriesEngine sharded = seriesWith("ShardedOor8190", 4);

    final List<MultiColumnAggregationRequest> pastTheEnd =
        List.of(new MultiColumnAggregationRequest(99, AggregationType.SUM, "sum_nothing"));

    assertThatThrownBy(() -> single.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, pastTheEnd, HOUR, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("out of range");

    assertThatThrownBy(() -> sharded.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, pastTheEnd, HOUR, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("out of range")
        .isNotInstanceOf(IOException.class);
  }

  /** A valid request over four shards still answers, so the unwrapping did not swallow the success path. */
  @Test
  void aValidMultiShardAggregationStillAnswers() throws Exception {
    final TimeSeriesEngine sharded = seriesWith("Ok8190", 4);

    // The engine row is [ts, host, v] - timestamp first, then the non-TIMESTAMP columns in SCHEMA order - so
    // 'v' is row position 2 and the TAG declared before it is position 1 (issue #8140).
    final MultiColumnAggregationResult result = sharded.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
        List.of(new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum_v")), HOUR, null);

    assertThat(result.getValue(Math.floorDiv(TS, HOUR) * HOUR, 0)).isEqualTo(55.0);
  }

  // ---- Helpers ----

  /**
   * A series whose samples are compacted into the sealed stores, because the refusal lives in
   * {@code aggregateMultiBlocks} and only the sealed half of the pass is what the shard executor runs.
   */
  private TimeSeriesEngine seriesWith(final String typeName, final int shards) throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName
        + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS " + shards);
    for (int i = 0; i < 10; i++)
      database.command("sql", "INSERT INTO " + typeName + " SET ts = " + (TS + i * 1000L)
          + ", host = 'h', v = " + (i + 1) + ".0");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
    engine.compactAll();
    return engine;
  }
}
