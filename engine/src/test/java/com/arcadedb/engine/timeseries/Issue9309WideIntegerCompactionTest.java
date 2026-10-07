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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9309: an integral field wider than 60 bits (an epoch-nanosecond instant, a snowflake id, a 64-bit hash) was
 * accepted by the mutable layer and then made every compaction of the type fail, permanently.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9309WideIntegerCompactionTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;

  @Test
  void wideValuesSurviveCompaction() throws Exception {
    final long[] wide = { 1_700_000_123_456_789_000L, 1_500_000_000_000_000_000L, (1L << 59), -(1L << 59) - 1, Long.MAX_VALUE,
        Long.MIN_VALUE };
    for (final String declared : new String[] { "LONG", "DATETIME_NANOS" }) {
      final String typeName = "Wide_" + declared;
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (v " + declared + ") SHARDS 1");
      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();

      final long[] timestamps = new long[wide.length];
      final Object[] values = new Object[wide.length];
      for (int i = 0; i < wide.length; i++) {
        timestamps[i] = BASE_TS + i;
        values[i] = wide[i];
      }
      engine.appendSamples(timestamps, values);

      // Compacting twice: the failure used to repeat because the poisoned sample stayed in the mutable layer
      engine.compactAll();
      engine.compactAll();

      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).as(declared).hasSize(wide.length);
      for (int i = 0; i < wide.length; i++)
        assertThat(rows.get(i)[1]).as("%s row %d", declared, i).isEqualTo(wide[i]);
    }
  }

  @Test
  void oneWideValueDoesNotStopTheTypeSealing() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Mixed TIMESTAMP ts FIELDS (v LONG) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Mixed")).getEngine();

    final int n = 1000;
    final long[] timestamps = new long[n];
    final Object[] values = new Object[n];
    for (int i = 0; i < n; i++) {
      timestamps[i] = BASE_TS + i;
      values[i] = i == 500 ? 1L << 60 : (long) i;
    }
    engine.appendSamples(timestamps, values);
    engine.compactAll();

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(n);
    for (int i = 0; i < n; i++)
      assertThat(rows.get(i)[1]).as("row %d", i).isEqualTo(values[i]);
  }
}
