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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7899: an engine row is {@code [timestamp, non-TIMESTAMP columns in schema order...]} whatever position
 * the TIMESTAMP column occupies in the declaration - {@code TimeSeriesBucket.readRow} writes {@code result[0]}
 * from the row's own timestamp slot and then skips the TIMESTAMP column while walking the schema. A consumer
 * that maps row position {@code i} onto {@code getTsColumns().get(i)} is therefore only right when the TIMESTAMP
 * column happens to be declared first, which issue #7702 stopped being a property of every declaration the
 * grammar can spell.
 * <p>
 * Both cases below declare a column BEFORE the timestamp. The STRING one makes a mis-mapping loud (a tag value
 * lands on the timestamp), the LONG one makes it silent (two numbers swap) - which is the variant that ships.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7899">issue #7899</a>
 */
class Issue7899NonFirstTimestampRowMappingTest extends TestHelper {

  private static final long TS = 1_700_000_000_000L;

  /**
   * The SQL read path. {@code SaveElementStep.saveToTimeSeries} resolves every column BY NAME, so the INSERT is
   * already correct and anything wrong in the row that comes back is the read mapping alone.
   */
  @Test
  void aSelectStarMapsEveryValueToItsOwnColumnWhenATagIsDeclaredBeforeTheTimestamp() {
    database.command("sql", "CREATE TIMESERIES TYPE TagFirst TAGS (host STRING) TIMESTAMP ts FIELDS (v DOUBLE)");
    database.command("sql", "INSERT INTO TagFirst SET ts = " + TS + ", host = 'srv-1', v = 42.5");

    try (final ResultSet rs = database.query("sql", "SELECT * FROM TagFirst")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("host")).as("the TAG value must not land on the timestamp").isEqualTo("srv-1");
      assertThat((Object) row.getProperty("ts")).isInstanceOf(LocalDateTime.class);
      assertThat((Object) row.getProperty("v")).isEqualTo(42.5);
    }
  }

  /** The silent variant: a LONG column before the timestamp swaps two numbers and raises nothing. */
  @Test
  void aSelectStarMapsEveryValueToItsOwnColumnWhenANumericFieldIsDeclaredBeforeTheTimestamp() {
    database.command("sql", "CREATE TIMESERIES TYPE SeqFirst FIELDS (seq LONG) TIMESTAMP ts FIELDS (v DOUBLE)");
    database.command("sql", "INSERT INTO SeqFirst SET ts = " + TS + ", seq = 7, v = 42.5");

    try (final ResultSet rs = database.query("sql", "SELECT * FROM SeqFirst")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("seq")).longValue()).as("seq must not receive the timestamp").isEqualTo(7L);
      assertThat((Object) row.getProperty("v")).isEqualTo(42.5);
    }
  }

  /**
   * The projection helper every wire surface names its response columns from. Its own javadoc already promises
   * "the timestamp column first, then the selected non-timestamp columns in schema order" - the {@code null}
   * (every column) branch returned the raw schema instead, so the names GET /ts/latest, POST /ts/query, the
   * Grafana frames and both gRPC time-series RPCs publish were off against the values beside them.
   */
  @Test
  void theFullRowProjectionListsTheTimestampFirstWhateverTheDeclarationSays() {
    database.command("sql", "CREATE TIMESERIES TYPE Projected TAGS (host STRING) TIMESTAMP ts FIELDS (v DOUBLE)");
    final List<ColumnDefinition> columns =
        ((LocalTimeSeriesType) database.getSchema().getType("Projected")).getTsColumns();

    assertThat(TimeSeriesGateway.columnNames(columns, null))
        .as("null means every column, in the order the engine returns their values")
        .containsExactly("ts", "host", "v");
    // The projecting branch was already right; pinned here so the two branches cannot drift apart again.
    assertThat(TimeSeriesGateway.columnNames(columns, new int[] { 1 })).containsExactly("ts", "v");
  }

  /**
   * The row the helper's names describe, read straight off the engine: proves the ordering above is the engine's
   * and not this test's opinion of it.
   */
  @Test
  void theEngineRowItselfCarriesTheTimestampInPositionZero() throws Exception {
    database.getSchema().buildTimeSeriesType().withName("Raw")
        .withTag("host", Type.STRING).withTimestamp("ts").withField("v", Type.DOUBLE).create();

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Raw");
    database.begin();
    tsType.getEngine().appendBatch(new long[] { TS }, new Object[][] { { "srv-1" }, { 42.5 } });
    database.commit();

    final List<Object[]> rows = tsType.getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsExactly(TS, "srv-1", 42.5);
  }
}
