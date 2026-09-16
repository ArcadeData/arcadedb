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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7399: {@code Schema.buildTimeSeriesType()} used to be implementable only by the embedded schema - the
 * builder demanded a {@code DatabaseInternal} and its terminal operation returned {@code LocalTimeSeriesType}, so
 * {@code RemoteSchema} could do nothing but throw. Code written against the embedded API with a builder did not
 * port to {@link RemoteDatabase} by swapping the {@code Database} instance.
 * <p>
 * The acceptance test the issue asks for is {@link #applyTheSameRecipe}: ONE body of builder code, written against
 * the {@link Schema} interface, run twice - once against the embedded database, once against a
 * {@link RemoteDatabase} - with the two resulting declarations compared attribute by attribute. A test that only
 * exercised the remote path would prove the code runs, not that the two implementations agree.
 */
class Issue7399RemoteTimeSeriesTypeBuilderIT extends BaseGraphServerTest {

  /**
   * The body of builder code under test. It names no implementation: whatever {@link Schema} it is handed decides
   * whether this creates the type in place or renders it as DDL and ships it to the server.
   */
  private static TimeSeriesType applyTheSameRecipe(final Schema schema, final String typeName) {
    return schema.buildTimeSeriesType()
        .withName(typeName)
        .withTimestamp("ts")
        .withPrecision("MICROSECOND")
        .withTag("host", Type.STRING)
        .withTag("zone", Type.INTEGER)
        .withField("cpu", Type.DOUBLE)
        .withField("mem", Type.LONG)
        .withShards(3)
        .withRetention(90L * 86_400_000L)
        .withCompactionBucketInterval(2L * 3_600_000L)
        .withDownsamplingTiers(List.of(new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
            new DownsamplingTier(30L * 86_400_000L, 86_400_000L)))
        .create();
  }

  /**
   * The server's OWN database instance, not {@code getDatabase(0)}: that one is the fixture's separate
   * {@code DatabaseFactory} handle, which {@link BaseGraphServerTest} closes before starting the servers, so
   * touching it here fails with {@code DatabaseIsClosedException} rather than reaching the schema under test.
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
  void theSameBuilderCodeProducesTheSameTypeEmbeddedAndRemotely() {
    final TimeSeriesType viaEmbedded = applyTheSameRecipe(embedded().getSchema(), "ParityEmbedded");

    try (final RemoteDatabase database = remote()) {
      final TimeSeriesType viaRemote = applyTheSameRecipe(database.getSchema(), "ParityRemote");

      assertThat(viaRemote.getName()).isEqualTo("ParityRemote");
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
  void aTypeBuiltRemotelySurvivesAFreshReadThroughRemoteSchema() {
    try (final RemoteDatabase database = remote()) {
      applyTheSameRecipe(database.getSchema(), "RoundTrip");
    }

    // A second connection, with a schema cache that was never told anything: what it reports is what the SERVER
    // stored, not what the builder happened to remember.
    try (final RemoteDatabase database = remote()) {
      final Object type = database.getSchema().getType("RoundTrip");
      assertThat(type).isInstanceOf(TimeSeriesType.class);
      final TimeSeriesType readBack = (TimeSeriesType) type;

      assertThat(readBack.getTimestampColumn()).isEqualTo("ts");
      assertThat(readBack.getPrecision()).isEqualTo("MICROSECOND");
      assertThat(readBack.getShardCount()).isEqualTo(3);
      assertThat(readBack.getRetentionMs()).isEqualTo(90L * 86_400_000L);
      assertThat(readBack.getCompactionBucketIntervalMs()).isEqualTo(2L * 3_600_000L);
      assertThat(readBack.getDownsamplingTiers()).containsExactly(
          new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
          new DownsamplingTier(30L * 86_400_000L, 86_400_000L));

      assertThat(readBack.getTsColumnNames()).containsExactly("ts", "host", "zone", "cpu", "mem");
      assertThat(readBack.getTsColumn("host").getRole()).isEqualTo(ColumnDefinition.ColumnRole.TAG);
      assertThat(readBack.getTsColumn("zone").getDataType()).isEqualTo(Type.INTEGER);
      assertThat(readBack.getTsColumn("cpu").getRole()).isEqualTo(ColumnDefinition.ColumnRole.FIELD);
      assertThat(readBack.getTsColumn("mem").getDataType()).isEqualTo(Type.LONG);
      assertThat(readBack.isDeclaredColumn("nosuch")).isFalse();
    }
  }

  @Test
  void aTypeBuiltWithNoPrecisionReadsBackAsNullOnBothSides() {
    // The other tests all declare a precision, so none of them would notice the absent case going wrong - and
    // "a remote client cannot tell NANOSECOND from the default" is one of the two defects this change fixes.
    // schema:types omits the key entirely when the type declared none, so what is pinned here is that the omission
    // reads back as null on the remote side and not as "", and that the embedded side agrees.
    final TimeSeriesType viaEmbedded = embedded().getSchema().buildTimeSeriesType()
        .withName("NoPrecisionEmbedded").withTimestamp("ts").withField("value", Type.DOUBLE).withShards(1).create();
    assertThat(viaEmbedded.getPrecision()).isNull();

    try (final RemoteDatabase database = remote()) {
      final TimeSeriesType viaRemote = database.getSchema().buildTimeSeriesType()
          .withName("NoPrecisionRemote").withTimestamp("ts").withField("value", Type.DOUBLE).withShards(1).create();
      assertThat(viaRemote.getPrecision()).isNull();
    }

    // And again on a connection whose cache was never told anything, so this is the server's answer.
    try (final RemoteDatabase database = remote()) {
      assertThat(((TimeSeriesType) database.getSchema().getType("NoPrecisionRemote")).getPrecision()).isNull();
      assertThat(((TimeSeriesType) database.getSchema().getType("NoPrecisionEmbedded")).getPrecision()).isNull();
    }
  }

  @Test
  void aLowerCasePrecisionReadsBackTheSameEmbeddedAndRemotely() {
    // The parity recipe above declares an already-canonical "MICROSECOND", so it would not notice the builder
    // storing the caller's spelling verbatim on one side and the SQL-canonicalized form on the other.
    final TimeSeriesType viaEmbedded = embedded().getSchema().buildTimeSeriesType()
        .withName("LowerCaseEmbedded").withTimestamp("ts").withPrecision("nanosecond")
        .withField("value", Type.DOUBLE).withShards(1).create();

    try (final RemoteDatabase database = remote()) {
      final TimeSeriesType viaRemote = database.getSchema().buildTimeSeriesType()
          .withName("LowerCaseRemote").withTimestamp("ts").withPrecision("nanosecond")
          .withField("value", Type.DOUBLE).withShards(1).create();

      assertThat(viaEmbedded.getPrecision()).isEqualTo("NANOSECOND");
      assertThat(viaRemote.getPrecision()).isEqualTo(viaEmbedded.getPrecision());
    }
  }

  @Test
  void aTypeBuiltRemotelyAcceptsSamples() {
    // A declaration the server stored but cannot ingest into would pass every assertion above. This one writes.
    try (final RemoteDatabase database = remote()) {
      database.getSchema().buildTimeSeriesType()
          .withName("Ingestible")
          .withTimestamp("ts")
          .withTag("host", Type.STRING)
          .withField("value", Type.DOUBLE)
          .withShards(1)
          .create();

      database.command("sql", "INSERT INTO Ingestible SET ts = 1000, host = 'srv1', value = 42.0");
      database.command("sql", "INSERT INTO Ingestible SET ts = 2000, host = 'srv2', value = 84.0");

      try (final ResultSet result = database.query("sql", "SELECT FROM Ingestible ORDER BY ts")) {
        assertThat(result.hasNext()).isTrue();
        assertThat((Double) result.next().getProperty("value")).isEqualTo(42.0);
        assertThat(result.hasNext()).isTrue();
        assertThat((Double) result.next().getProperty("value")).isEqualTo(84.0);
      }
    }
  }

  @Test
  void aBuilderStateWithNoSQLExpressionFailsBeforeAnythingReachesTheServer() {
    try (final RemoteDatabase database = remote()) {
      assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType()
          .withName("SubSecondRetention")
          .withTimestamp("ts")
          .withField("value", Type.DOUBLE)
          .withRetention(1_500L)
          .create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("whole number of seconds");

      // Nothing was sent: the type must not half-exist on the server.
      assertThat(database.getSchema().existsType("SubSecondRetention")).isFalse();
    }
  }

  @Test
  void anIncompleteBuilderFailsRemotelyExactlyAsItDoesEmbedded() {
    try (final RemoteDatabase database = remote()) {
      assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withTimestamp("ts").create())
          .isInstanceOf(SchemaException.class).hasMessageContaining("name is required");
      assertThatThrownBy(() -> embedded().getSchema().buildTimeSeriesType().withTimestamp("ts").create())
          .isInstanceOf(SchemaException.class).hasMessageContaining("name is required");

      assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("NoTs").create())
          .isInstanceOf(SchemaException.class).hasMessageContaining("TIMESTAMP column");
      assertThatThrownBy(() -> embedded().getSchema().buildTimeSeriesType().withName("NoTs").create())
          .isInstanceOf(SchemaException.class).hasMessageContaining("TIMESTAMP column");
    }
  }
}
