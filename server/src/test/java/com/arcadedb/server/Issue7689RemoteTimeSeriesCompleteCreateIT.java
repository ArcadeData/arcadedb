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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7689: a remote {@code buildTimeSeriesType()} used to be unable to say the whole type.
 * <p>
 * {@code CREATE TIMESERIES TYPE} had no downsampling clause, so {@code RemoteTimeSeriesTypeBuilder} rendered a
 * CREATE and an {@code ALTER ... ADD DOWNSAMPLING POLICY} and issued them in order: a failure after the first left
 * the type on the server WITHOUT its policy while the caller got an exception. And it had no per-column codec
 * clause at all, so a builder carrying one was refused outright - which meant a TIMESERIES type could not be
 * restored from a logical export against a remote database, the export recording the codec per column precisely
 * because it is not re-derivable (issue #5475).
 * <p>
 * The grammar now carries both, so the remote create is ONE statement. The engine-side half of the acceptance
 * criteria - the grammar, the AST and the rendering - is
 * {@code Issue7689CreateTimeSeriesTypeCompleteDeclarationTest}.
 */
class Issue7689RemoteTimeSeriesCompleteCreateIT extends BaseGraphServerTest {

  /**
   * The shape a logical restore hands the builder: every column already resolved, codec included, plus the
   * downsampling policy. It names no implementation, so the same body runs embedded and remotely.
   */
  private static TimeSeriesTypeBuilder restoreRecipe(final Schema schema, final String typeName) {
    return schema.buildTimeSeriesType()
        .withName(typeName)
        .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP,
            TimeSeriesCodec.SIMPLE8B))
        .withPrecision("MICROSECOND")
        .withColumn(new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.NONE))
        .withColumn(new ColumnDefinition("zone", Type.INTEGER, ColumnDefinition.ColumnRole.TAG,
            TimeSeriesCodec.SIMPLE8B))
        .withColumn(new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD,
            TimeSeriesCodec.DICTIONARY))
        .withField("mem", Type.LONG)
        .withShards(3)
        .withRetention(90L * 86_400_000L)
        .withCompactionBucketInterval(2L * 3_600_000L)
        .withDownsamplingTiers(List.of(new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
            new DownsamplingTier(30L * 86_400_000L, 86_400_000L)));
  }

  /**
   * The server's OWN database instance, not {@code getDatabase(0)}: that one is the fixture's separate
   * {@code DatabaseFactory} handle, which {@link BaseGraphServerTest} closes before starting the servers.
   */
  private Database embedded() {
    return getServerDatabase(0, getDatabaseName());
  }

  /**
   * The port the test server actually bound, not the 2480 the range starts at: a server already listening there
   * would otherwise take these requests and fail the test as an authentication error rather than a port conflict.
   */
  private RemoteDatabase remote() {
    return new RemoteDatabase("127.0.0.1", getServer(0).getHttpServer().getPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  @Test
  void aRemoteCreateCarryingCodecsAndAPolicyIsASingleStatement() {
    // The window this issue is about was between two statements. With one there is nothing to tear, which is a
    // property of the DDL the remote builder issues rather than of any failure the test could stage.
    try (final RemoteDatabase database = remote()) {
      final List<String> statements = restoreRecipe(database.getSchema(), "SingleStatement").toSQL();

      assertThat(statements).hasSize(1);
      assertThat(statements.getFirst())
          .startsWith("CREATE TIMESERIES TYPE `SingleStatement`")
          .contains("CODEC")
          .contains("DOWNSAMPLING POLICY")
          .doesNotContain("ALTER");
    }
  }

  @Test
  void theSameRestoreRecipeProducesTheSameTypeEmbeddedAndRemotely() {
    // Before the CODEC clause existed this test could not be written at all: the remote half threw a SchemaException
    // from toSQL() rather than creating anything.
    final TimeSeriesType viaEmbedded = restoreRecipe(embedded().getSchema(), "RestoreEmbedded").create();

    try (final RemoteDatabase database = remote()) {
      final TimeSeriesType viaRemote = restoreRecipe(database.getSchema(), "RestoreRemote").create();

      assertThat(viaRemote.getTimestampColumn()).isEqualTo(viaEmbedded.getTimestampColumn());
      assertThat(viaRemote.getPrecision()).isEqualTo(viaEmbedded.getPrecision());
      assertThat(viaRemote.getShardCount()).isEqualTo(viaEmbedded.getShardCount());
      assertThat(viaRemote.getRetentionMs()).isEqualTo(viaEmbedded.getRetentionMs());
      assertThat(viaRemote.getCompactionBucketIntervalMs()).isEqualTo(viaEmbedded.getCompactionBucketIntervalMs());
      assertThat(viaRemote.getDownsamplingTiers()).isEqualTo(viaEmbedded.getDownsamplingTiers());
      assertThat(viaRemote.getTsColumnNames()).isEqualTo(viaEmbedded.getTsColumnNames());

      for (final ColumnDefinition expected : viaEmbedded.getTsColumns()) {
        final ColumnDefinition actual = viaRemote.getTsColumn(expected.getName());
        assertThat(actual).as(expected.getName()).isNotNull();
        assertThat(actual.getDataType()).as(expected.getName()).isEqualTo(expected.getDataType());
        assertThat(actual.getRole()).as(expected.getName()).isEqualTo(expected.getRole());
        assertThat(actual.getCompressionHint()).as(expected.getName()).isEqualTo(expected.getCompressionHint());
      }
    }
  }

  @Test
  void theCodecsAndThePolicyAreWhatTheServerStored() {
    try (final RemoteDatabase database = remote()) {
      restoreRecipe(database.getSchema(), "ServerStored").create();
    }

    // A second connection, with a schema cache that was never told anything.
    try (final RemoteDatabase database = remote()) {
      final TimeSeriesType readBack = (TimeSeriesType) database.getSchema().getType("ServerStored");

      assertThat(readBack.getTsColumn("ts").getCompressionHint()).isEqualTo(TimeSeriesCodec.SIMPLE8B);
      assertThat(readBack.getTsColumn("host").getCompressionHint()).isEqualTo(TimeSeriesCodec.NONE);
      assertThat(readBack.getTsColumn("zone").getCompressionHint()).isEqualTo(TimeSeriesCodec.SIMPLE8B);
      assertThat(readBack.getTsColumn("cpu").getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
      assertThat(readBack.getTsColumn("mem").getCompressionHint())
          .isEqualTo(ColumnDefinition.defaultCodecFor(Type.LONG, ColumnDefinition.ColumnRole.FIELD));

      assertThat(readBack.getDownsamplingTiers()).containsExactly(
          new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
          new DownsamplingTier(30L * 86_400_000L, 86_400_000L));
    }
  }

  @Test
  void aTypeCreatedRemotelyWithCodecsAndAPolicyStillIngests() {
    // A declaration the server stored but cannot ingest into would pass every assertion above. The codecs here are
    // deliberately not the defaults, so this also exercises the write path with the codec the caller chose.
    try (final RemoteDatabase database = remote()) {
      restoreRecipe(database.getSchema(), "IngestibleWithCodecs").create();

      database.command("sql", "INSERT INTO IngestibleWithCodecs SET ts = 1000, host = 'srv1', zone = 1, cpu = 42.0, mem = 7");
      database.command("sql", "INSERT INTO IngestibleWithCodecs SET ts = 2000, host = 'srv2', zone = 2, cpu = 84.0, mem = 9");

      try (final var result = database.query("sql", "SELECT FROM IngestibleWithCodecs ORDER BY ts")) {
        assertThat(result.hasNext()).isTrue();
        assertThat((Double) result.next().getProperty("cpu")).isEqualTo(42.0);
        assertThat(result.hasNext()).isTrue();
        assertThat((Double) result.next().getProperty("cpu")).isEqualTo(84.0);
      }
    }
  }

  @Test
  void aRefusedRemoteCreateLeavesNoTypeBehind() {
    // The all-or-nothing claim from the caller's side: when the one statement is refused, there is no half-created
    // type to clean up and no policy-less leftover.
    embedded().getSchema().buildTimeSeriesType()
        .withName("AlreadyThere").withTimestamp("ts").withField("v", Type.DOUBLE).withShards(1).create();

    try (final RemoteDatabase database = remote()) {
      assertThatThrownBy(() -> restoreRecipe(database.getSchema(), "AlreadyThere").create())
          .isInstanceOf(RemoteException.class)
          .hasMessageContaining("already exists");

      // The pre-existing type is untouched: the refused statement declared a policy and codecs, and none of it
      // reached the type that was already there.
      final TimeSeriesType untouched = (TimeSeriesType) database.getSchema().getType("AlreadyThere");
      assertThat(untouched.getDownsamplingTiers()).isEmpty();
      assertThat(untouched.getTsColumnNames()).containsExactly("ts", "v");
    }
  }
}
