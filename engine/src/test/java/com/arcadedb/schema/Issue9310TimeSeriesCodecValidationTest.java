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
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

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
  void aNumericCodecIsOnlyAcceptedForTypesThatRoundTrip() {
    assertRefused("CREATE TIMESERIES TYPE R1 TIMESTAMP ts FIELDS (v DOUBLE CODEC SIMPLE8B)", "SIMPLE8B");
    assertRefused("CREATE TIMESERIES TYPE R2 TIMESTAMP ts FIELDS (v FLOAT CODEC SIMPLE8B)", "SIMPLE8B");
    assertRefused("CREATE TIMESERIES TYPE R3 TIMESTAMP ts FIELDS (v LONG CODEC GORILLA_XOR)", "GORILLA_XOR");
    assertRefused("CREATE TIMESERIES TYPE R4 TIMESTAMP ts TAGS (z INTEGER CODEC GORILLA_XOR) FIELDS (v DOUBLE)", "GORILLA_XOR");
  }

  @Test
  void everyHonouredCombinationIsAcceptedCompactsAndReadsBackUnchanged() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Ok1 TIMESTAMP ts TAGS (h STRING CODEC DICTIONARY, z INTEGER CODEC SIMPLE8B, "
        + "y LONG CODEC DICTIONARY) FIELDS (a DOUBLE CODEC GORILLA_XOR, b LONG CODEC SIMPLE8B, c DOUBLE CODEC DICTIONARY, "
        + "f BOOLEAN CODEC SIMPLE8B) SHARDS 1");
    database.transaction(() -> database.command("sql",
        "INSERT INTO Ok1 SET ts = 1700000000000, h = 'x', z = 5, y = 6, a = 7.5, b = 9007199254740993, c = 1.5, f = true"));
    ((LocalTimeSeriesType) database.getSchema().getType("Ok1")).getEngine().compactAll();
    try (final ResultSet rs = database.query("sql", "SELECT FROM Ok1")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("h")).isEqualTo("x");
      assertThat(r.<Number>getProperty("z").longValue()).isEqualTo(5L);
      assertThat(r.<Number>getProperty("a").doubleValue()).isEqualTo(7.5);
      assertThat(r.<Number>getProperty("b").longValue()).isEqualTo(9007199254740993L);
      assertThat(r.<Number>getProperty("c").doubleValue()).isEqualTo(1.5);
      assertThat(r.<Boolean>getProperty("f")).isTrue();
    }
  }

  /**
   * The check guards creation only: a database written before it, whose schema names a codec the encoders cannot run,
   * must still open (refusing to open would be worse than the failing compaction it already has).
   */
  @Test
  void aPersistedSchemaWithARefusedCodecStillOpens() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Legacy TIMESTAMP ts FIELDS (v DOUBLE CODEC DICTIONARY)");
    database.close();

    final Path schemaFile = Path.of(database.getDatabasePath(), "schema.json");
    final String schema = Files.readString(schemaFile);
    assertThat(schema).containsPattern("\"compression\"\\s*:\\s*\"DICTIONARY\"");
    Files.writeString(schemaFile, schema.replaceAll("(\"compression\"\\s*:\\s*)\"DICTIONARY\"", "$1\"NONE\""));

    reopenDatabase();
    assertThat(((LocalTimeSeriesType) database.getSchema().getType("Legacy")).getTsColumn("v").getCompressionHint())
        .isEqualTo(TimeSeriesCodec.NONE);
  }

  private void assertRefused(final String sql, final String mention) {
    assertThatThrownBy(() -> database.command("sql", sql)).hasMessageContaining(mention);
  }
}
