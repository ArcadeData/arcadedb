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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every shard of a TIMESERIES type owns two files, a decoded-block cache and a worker thread, so the number of shards a type
 * declares has to stay in a range a database can serve. These tests pin that a shard count above the maximum, or below zero, is
 * refused when the type is created, from SQL and from the builder alike, that nothing is written for a refused type, and that the
 * maximum itself, the default and every smaller count keep working.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class TimeSeriesShardCountTest extends TestHelper {
  private static final String TYPE = "Readings";

  @Test
  void sqlShardCountAboveTheMaximumIsRefusedBeforeAnyFileIsCreated() {
    final String[] before = databaseFiles();

    assertThatThrownBy(() -> database.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS " + (TimeSeriesTypeBuilder.MAX_SHARDS + 1)))
        .isInstanceOf(SchemaException.class).hasMessageContaining("shard count");

    assertThat(database.getSchema().existsType(TYPE)).isFalse();
    assertThat(databaseFiles()).containsExactlyInAnyOrder(before);
  }

  @Test
  void sqlShardCountOfTheLargestIntegerIsRefusedBeforeAnyFileIsCreated() {
    final String[] before = databaseFiles();

    assertThatThrownBy(() -> database.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS " + Integer.MAX_VALUE))
        .isInstanceOf(SchemaException.class).hasMessageContaining("shard count");

    assertThat(database.getSchema().existsType(TYPE)).isFalse();
    assertThat(databaseFiles()).containsExactlyInAnyOrder(before);
  }

  @Test
  void sqlShardCountBeyondTheIntegerRangeIsAParseError() {
    assertThatThrownBy(() -> database.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 99999999999"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("SHARDS");

    assertThat(database.getSchema().existsType(TYPE)).isFalse();
  }

  @Test
  void builderShardCountAboveTheMaximumIsRefusedBeforeAnyFileIsCreated() {
    final String[] before = databaseFiles();

    for (final int shards : new int[] { TimeSeriesTypeBuilder.MAX_SHARDS + 1, 100_000, Integer.MAX_VALUE }) {
      assertThatThrownBy(() -> builder().withShards(shards).create()).as("shards " + shards).isInstanceOf(SchemaException.class)
          .hasMessageContaining("shard count");

      assertThat(database.getSchema().existsType(TYPE)).isFalse();
      assertThat(databaseFiles()).containsExactlyInAnyOrder(before);
    }
  }

  @Test
  void builderRendersNoStatementForAShardCountAboveTheMaximum() {
    // the renderer is what a remote schema executes: it has to refuse what the embedded create() refuses
    assertThatThrownBy(() -> builder().withShards(TimeSeriesTypeBuilder.MAX_SHARDS + 1).toSQL()).isInstanceOf(SchemaException.class)
        .hasMessageContaining("shard count");
  }

  @Test
  void builderNegativeShardCountIsRefused() {
    final String[] before = databaseFiles();

    assertThatThrownBy(() -> builder().withShards(-1).create()).isInstanceOf(SchemaException.class).hasMessageContaining("shard count");
    assertThatThrownBy(() -> builder().withShards(Integer.MIN_VALUE).create()).isInstanceOf(SchemaException.class)
        .hasMessageContaining("shard count");
    assertThatThrownBy(() -> builder().withShards(-1).toSQL()).isInstanceOf(SchemaException.class).hasMessageContaining("shard count");

    assertThat(database.getSchema().existsType(TYPE)).isFalse();
    assertThat(databaseFiles()).containsExactlyInAnyOrder(before);
  }

  @Test
  void theMaximumShardCountIsAccepted() {
    final TimeSeriesType type = builder().withShards(TimeSeriesTypeBuilder.MAX_SHARDS).create();

    assertThat(type.getShardCount()).isEqualTo(TimeSeriesTypeBuilder.MAX_SHARDS);
    database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET ts = 1000, host = 'a', v = 1.5"));
    assertThat(database.query("sql", "SELECT count(*) AS c FROM " + TYPE).next().<Long>getProperty("c")).isEqualTo(1L);
  }

  @Test
  void sqlShardCountsUpToTheMaximumKeepWorking() {
    database.command("sql", "CREATE TIMESERIES TYPE One TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.command("sql",
        "CREATE TIMESERIES TYPE Many TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS " + TimeSeriesTypeBuilder.MAX_SHARDS);

    assertThat(((TimeSeriesType) database.getSchema().getType("One")).getShardCount()).isEqualTo(1);
    assertThat(((TimeSeriesType) database.getSchema().getType("Many")).getShardCount()).isEqualTo(TimeSeriesTypeBuilder.MAX_SHARDS);
  }

  @Test
  void theDefaultShardCountStaysTheNumberOfAsyncWorkerThreads() {
    final TimeSeriesType unnamed = builder().create();
    assertThat(unnamed.getShardCount()).isEqualTo(database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS));

    // zero is the builder's own spelling of "use the default"
    database.command("sql", "CREATE TIMESERIES TYPE Zero TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 0");
    assertThat(((TimeSeriesType) database.getSchema().getType("Zero")).getShardCount())
        .isEqualTo(database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS));
  }

  @Test
  void aDefaultAboveTheMaximumOnAWideMachineIsStillAccepted() {
    // an export of a type created with the default on a machine with more cores than the maximum names that many shards, and
    // restoring it there has to keep working
    final int wide = TimeSeriesTypeBuilder.MAX_SHARDS + 2;
    database.getConfiguration().setValue(GlobalConfiguration.ASYNC_WORKER_THREADS, wide);

    assertThat(builder().withShards(wide).toSQL()).hasSize(1);
    assertThatThrownBy(() -> builder().withShards(wide + 1).toSQL()).isInstanceOf(SchemaException.class)
        .hasMessageContaining("shard count");
  }

  private TimeSeriesTypeBuilder builder() {
    return database.getSchema().buildTimeSeriesType().withName(TYPE).withTimestamp("ts").withTag("host", Type.STRING)
        .withField("v", Type.DOUBLE);
  }

  private String[] databaseFiles() {
    final String[] names = new File(getDatabasePath()).list();
    return names != null ? names : new String[0];
  }
}
