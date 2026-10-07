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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9310: {@code CREATE TIMESERIES TYPE ... CODEC x} validated the codec NAME only, so a codec the sealed store has no
 * encoder for (or one that reads numbers, on a text column) was accepted and then failed in every later compaction,
 * permanently and for the whole type. The combination is now refused where the column is built.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9310TimeSeriesCodecValidationTest extends TestHelper {

  @Test
  void aCodecWithNoEncoderIsRefusedAtCreate() {
    assertRefused("CREATE TIMESERIES TYPE T1 TIMESTAMP ts FIELDS (v DOUBLE CODEC NONE)", "NONE");
    assertRefused("CREATE TIMESERIES TYPE T2 TIMESTAMP ts FIELDS (v DOUBLE CODEC DELTA_OF_DELTA)", "DELTA_OF_DELTA");
    assertRefused("CREATE TIMESERIES TYPE T3 TIMESTAMP ts TAGS (h STRING CODEC NONE) FIELDS (v DOUBLE)", "NONE");
    assertRefused("CREATE TIMESERIES TYPE T4 TIMESTAMP ts TAGS (h STRING CODEC DELTA_OF_DELTA) FIELDS (v DOUBLE)", "DELTA_OF_DELTA");
  }

  @Test
  void aNumericCodecOnATextColumnIsRefused() {
    assertRefused("CREATE TIMESERIES TYPE T5 TIMESTAMP ts TAGS (h STRING CODEC GORILLA_XOR) FIELDS (v DOUBLE)", "GORILLA_XOR");
    assertRefused("CREATE TIMESERIES TYPE T6 TIMESTAMP ts TAGS (h STRING CODEC SIMPLE8B) FIELDS (v DOUBLE)", "SIMPLE8B");
  }

  @Test
  void theTimestampColumnOnlyTakesDeltaOfDelta() {
    assertRefused("CREATE TIMESERIES TYPE T7 TIMESTAMP ts CODEC GORILLA_XOR FIELDS (v DOUBLE)", "TIMESTAMP");
    assertRefused("CREATE TIMESERIES TYPE T8 TIMESTAMP ts CODEC NONE FIELDS (v DOUBLE)", "TIMESTAMP");
    database.command("sql", "CREATE TIMESERIES TYPE T9 TIMESTAMP ts CODEC DELTA_OF_DELTA FIELDS (v DOUBLE)");
    assertThat(database.getSchema().existsType("T9")).isTrue();
  }

  @Test
  void theBuilderRefusesItToo() {
    final TimeSeriesTypeBuilder builder = new TimeSeriesTypeBuilder(database).withName("B1").withTimestamp("ts")
        .withColumn(new ColumnDefinition("v", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, TimeSeriesCodec.NONE));
    assertThatThrownBy(builder::create).isInstanceOf(SchemaException.class).hasMessageContaining("NONE");
    assertThat(database.getSchema().existsType("B1")).isFalse();
  }

  @Test
  void everyHonouredCombinationIsAcceptedAndCompacts() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Ok1 TIMESTAMP ts TAGS (h STRING CODEC DICTIONARY, z INTEGER CODEC SIMPLE8B, "
        + "y LONG CODEC GORILLA_XOR) FIELDS (a DOUBLE CODEC GORILLA_XOR, b LONG CODEC SIMPLE8B, c DOUBLE CODEC DICTIONARY) SHARDS 1");
    database.transaction(() -> database.command("sql",
        "INSERT INTO Ok1 SET ts = 1700000000000, h = 'x', z = 5, y = 5, a = 7.0, b = 7, c = 7.0"));
    ((LocalTimeSeriesType) database.getSchema().getType("Ok1")).getEngine().compactAll();
    try (final ResultSet rs = database.query("sql", "SELECT FROM Ok1")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  private void assertRefused(final String sql, final String mention) {
    assertThatThrownBy(() -> database.command("sql", sql)).hasMessageContaining(mention);
  }
}
