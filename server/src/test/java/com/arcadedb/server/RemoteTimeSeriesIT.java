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
import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteSchema;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.remote.timeseries.TimeSeriesBucket;
import com.arcadedb.remote.timeseries.TimeSeriesLatestResult;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.remote.timeseries.TimeSeriesQuery.Aggregation;
import com.arcadedb.remote.timeseries.TimeSeriesQuery;
import com.arcadedb.remote.timeseries.TimeSeriesQueryResult;
import com.arcadedb.remote.timeseries.TimeSeriesWriteSummary;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Time series through the Java remote driver ({@link com.arcadedb.remote.RemoteDatabase}): the time-series API,
 * the type builder and its complete create, a logical restore through remote handles only, the refusal messages,
 * and a schema reload that sees a type change kind into a time-series type.
 */
class RemoteTimeSeriesIT {

  /**
   * Issue #7305: the server has had {@code /ts/write}, {@code /ts/query} and {@code /ts/latest} for a long time,
   * and {@link RemoteDatabase} could reach none of them - a Java application had to hand-roll HTTP against its
   * own database's time-series store. These tests drive the four new client methods end to end against a real
   * server, one per entry point.
   * <p>
   * They are also the HTTP half of the cross-protocol equivalence assertion: the gRPC half lives in
   * {@code Issue7305TimeSeriesGrpcIT}, which runs the same calls against {@code RemoteGrpcDatabase} and compares
   * the two answers.
   */
  @Nested
  class TimeSeriesApi extends BaseGraphServerTest {

    private static final String TYPE = "weather";

    /**
     * The port the test server actually bound, not the 2480 the range starts at: the configured range is
     * 2480-2489, so a server already listening on 2480 - a developer's own instance, another run - pushes this
     * one up, and a hard-coded 2480 would send these requests to that stranger and fail as an authentication
     * error rather than as a port conflict.
     */
    private RemoteDatabase remote() {
      return new RemoteDatabase("127.0.0.1", getServer(0).getHttpServer().getPort(), getDatabaseName(), "root",
          DEFAULT_PASSWORD_FOR_TESTS);
    }

    private void createType(final RemoteDatabase database) {
      database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
          + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");
    }

    private static List<TimeSeriesPoint> threeSamples() {
      return List.of(
          new TimeSeriesPoint(TYPE, 1_000L, Map.of("location", "us-east"), Map.of("temperature", 22.5)),
          new TimeSeriesPoint(TYPE, 2_000L, Map.of("location", "us-west"), Map.of("temperature", 18.3)),
          new TimeSeriesPoint(TYPE, 3_000L, Map.of("location", "us-east"), Map.of("temperature", 23.1)));
    }

    @Test
    void writeAndQueryRawSamplesThroughTheHttpClient() {
      try (final RemoteDatabase database = remote()) {
        createType(database);

        final TimeSeriesWriteSummary summary = database.timeSeriesWrite(threeSamples());
        assertThat(summary.isComplete()).isTrue();
        assertThat(summary.written()).isEqualTo(3);
        assertThat(summary.dropped()).isZero();

        final TimeSeriesQueryResult result = database.timeSeriesQuery(new TimeSeriesQuery(TYPE).from(1_000).to(3_000));
        assertThat(result.isAggregated()).isFalse();
        assertThat(result.count()).isEqualTo(3);
        assertThat(result.columns()).containsExactly("ts", "location", "temperature");
        assertThat(result.truncated()).isFalse();

        // Timestamps come back in ascending order, which is what makes a row index meaningful at all.
        final List<Long> timestamps = new ArrayList<>();
        for (final Object[] row : result.rows())
          timestamps.add(((Number) row[0]).longValue());
        assertThat(timestamps).containsExactly(1_000L, 2_000L, 3_000L);
      }
    }

    @Test
    void aTagFilterAndAFieldProjectionNarrowTheAnswer() {
      try (final RemoteDatabase database = remote()) {
        createType(database);
        database.timeSeriesWrite(threeSamples());

        final TimeSeriesQueryResult filtered = database.timeSeriesQuery(
            new TimeSeriesQuery(TYPE).tag("location", "us-east"));
        assertThat(filtered.count()).isEqualTo(2);

        final TimeSeriesQueryResult projected = database.timeSeriesQuery(
            new TimeSeriesQuery(TYPE).fields("temperature"));
        // The timestamp column is always returned and always first, whether or not it was named.
        assertThat(projected.columns()).containsExactly("ts", "temperature");

        // The VALUES have to line up with those names, which is what was broken before this change: the
        // projection's indices count non-timestamp columns, and the resolver was producing full-schema ones, so
        // the row came back as [ts, location, null] - the neighbouring column's value under 'temperature', and a
        // trailing null - while the column names looked right. Asserting only the row's width would have passed.
        final Object[] first = projected.rows().getFirst();
        assertThat(first).hasSize(2);
        assertThat(((Number) first[0]).longValue()).isEqualTo(1_000L);
        assertThat(((Number) first[1]).doubleValue()).isEqualTo(22.5);
      }
    }

    @Test
    void aLimitCutsTheAnswerAndSaysSo() {
      try (final RemoteDatabase database = remote()) {
        createType(database);
        database.timeSeriesWrite(threeSamples());

        final TimeSeriesQueryResult truncated = database.timeSeriesQuery(new TimeSeriesQuery(TYPE).limit(2));
        assertThat(truncated.count()).isEqualTo(2);
        // A response the limit cut short must not look like a complete one (issue #5711): a client that ignored
        // this would silently disagree with the same query run embedded.
        assertThat(truncated.truncated()).isTrue();

        final TimeSeriesQueryResult complete = database.timeSeriesQuery(new TimeSeriesQuery(TYPE).limit(3));
        assertThat(complete.count()).isEqualTo(3);
        assertThat(complete.truncated()).isFalse();
      }
    }

    @Test
    void anAggregatedQueryReturnsBucketsThroughTheHttpClient() {
      try (final RemoteDatabase database = remote()) {
        createType(database);
        database.timeSeriesWrite(threeSamples());

        final TimeSeriesQueryResult result = database.timeSeriesQuery(new TimeSeriesQuery(TYPE)
            .aggregate(5_000L, new TimeSeriesQuery.Aggregation("temperature", AggregationType.AVG, "avg_temp")));

        assertThat(result.isAggregated()).isTrue();
        assertThat(result.aggregations()).containsExactly("avg_temp");
        assertThat(result.buckets()).isNotEmpty();

        final TimeSeriesBucket bucket = result.buckets().getFirst();
        assertThat(bucket.values()).hasSize(1);
        assertThat(((Number) bucket.value(0)).doubleValue()).isCloseTo((22.5 + 18.3 + 23.1) / 3, within(1e-9));
      }
    }

    @Test
    void latestReturnsTheNewestSampleAndHonoursOneTag() {
      try (final RemoteDatabase database = remote()) {
        createType(database);
        database.timeSeriesWrite(threeSamples());

        final TimeSeriesLatestResult latest = database.timeSeriesLatest(TYPE);
        assertThat(latest.isPresent()).isTrue();
        assertThat(((Number) latest.value("ts")).longValue()).isEqualTo(3_000L);

        final TimeSeriesLatestResult western = database.timeSeriesLatest(TYPE, "location", "us-west");
        assertThat(western.isPresent()).isTrue();
        assertThat(((Number) western.value("ts")).longValue()).isEqualTo(2_000L);
      }
    }

    @Test
    void latestOfAnEmptyTypeIsAbsentRatherThanAnError() {
      try (final RemoteDatabase database = remote()) {
        database.command("sql", "CREATE TIMESERIES TYPE emptyts TIMESTAMP ts TAGS (t STRING) FIELDS (v DOUBLE)");

        final TimeSeriesLatestResult latest = database.timeSeriesLatest("emptyts");
        assertThat(latest.isPresent()).isFalse();
        assertThat(latest.latest()).isNull();
      }
    }

    @Test
    void aChunkedWriteAppendsEveryChunk() {
      try (final RemoteDatabase database = remote()) {
        createType(database);

        final List<TimeSeriesPoint> points = new ArrayList<>();
        for (int i = 0; i < 250; i++)
          points.add(new TimeSeriesPoint(TYPE, 10_000L + i, Map.of("location", "us-east"),
              Map.of("temperature", (double) i)));

        // 250 points at 100 per chunk is three requests, so this also pins that the per-chunk summaries add up
        // rather than the last one replacing the others.
        final TimeSeriesWriteSummary summary = database.timeSeriesWriteStream(points, 100);
        assertThat(summary.received()).isEqualTo(250);
        assertThat(summary.written()).isEqualTo(250);
        assertThat(summary.isComplete()).isTrue();

        assertThat(database.timeSeriesQuery(new TimeSeriesQuery(TYPE).limit(1_000)).count()).isEqualTo(250);
      }
    }

    @Test
    void writingToAnUnknownTypeIsReportedAsAPartialWriteRatherThanThrowing() {
      try (final RemoteDatabase database = remote()) {
        createType(database);

        final TimeSeriesWriteSummary summary = database.timeSeriesWrite(List.of(
            new TimeSeriesPoint(TYPE, 1_000L, Map.of("location", "us-east"), Map.of("temperature", 22.5)),
            new TimeSeriesPoint("nosuchtype", 1_000L, Map.of(), Map.of("v", 1.0))));

        // The samples that did apply are durable: this is a partial write, not a rollback, and a client that read
        // it as a total failure would re-send data it already stored.
        assertThat(summary.written()).isEqualTo(1);
        assertThat(summary.dropped()).isEqualTo(1);
        assertThat(summary.isComplete()).isFalse();
        assertThat(summary.unknownTypes()).containsExactly("nosuchtype");
      }
    }
  }

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
  @Nested
  class TypeBuilder extends BaseGraphServerTest {

    /**
     * The body of builder code under test. It names no implementation: whatever {@link Schema} it is handed decides
     * whether this creates the type in place or renders it as DDL and ships it to the server.
     */
    private static TimeSeriesType applyTheSameRecipe(final Schema schema, final String typeName) {
      return schema.buildTimeSeriesType()
          .withName(typeName)
          .withTimestamp("ts")
          .withPrecision("MILLISECOND")
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
        assertThat(readBack.getPrecision()).isEqualTo("MILLISECOND");
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
      // The parity recipe above declares an already-canonical "MILLISECOND", so it would not notice the builder
      // storing the caller's spelling verbatim on one side and the SQL-canonicalized form on the other.
      final TimeSeriesType viaEmbedded = embedded().getSchema().buildTimeSeriesType()
          .withName("LowerCaseEmbedded").withTimestamp("ts").withPrecision("millisecond")
          .withField("value", Type.DOUBLE).withShards(1).create();

      try (final RemoteDatabase database = remote()) {
        final TimeSeriesType viaRemote = database.getSchema().buildTimeSeriesType()
            .withName("LowerCaseRemote").withTimestamp("ts").withPrecision("millisecond")
            .withField("value", Type.DOUBLE).withShards(1).create();

        assertThat(viaEmbedded.getPrecision()).isEqualTo("MILLISECOND");
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
    void aDuplicateTypeNameIsRefusedOnBothPathsWithTheSameExceptionType() {
      // Both paths refuse the duplicate in the same words and, since issue #10153, with the same type: the embedded one
      // answers before touching storage, the remote one rebuilds it from the server's 409. They used to differ (a
      // RemoteException remotely), so a caller catching "already exists" had to catch two types.
      embedded().getSchema().buildTimeSeriesType()
          .withName("Duplicate").withTimestamp("ts").withField("value", Type.DOUBLE).withShards(1).create();

      assertThatThrownBy(() -> embedded().getSchema().buildTimeSeriesType()
          .withName("Duplicate").withTimestamp("ts").withField("value", Type.DOUBLE).withShards(1).create())
          .isInstanceOf(AlreadyExistsException.class)
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Type 'Duplicate' already exists");

      try (final RemoteDatabase database = remote()) {
        assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType()
            .withName("Duplicate").withTimestamp("ts").withField("value", Type.DOUBLE).withShards(1).create())
            .isInstanceOf(AlreadyExistsException.class)
            .isInstanceOf(SchemaException.class)
            .hasMessageContaining("Type 'Duplicate' already exists");
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
   * {@code TimeSeriesTypeBuilderTest.CreateTimeSeriesTypeCompleteDeclaration}.
   */
  @Nested
  class CompleteCreate extends BaseGraphServerTest {

    /**
     * The shape a logical restore hands the builder: every column already resolved, codec included, plus the
     * downsampling policy. It names no implementation, so the same body runs embedded and remotely.
     */
    private static TimeSeriesTypeBuilder restoreRecipe(final Schema schema, final String typeName) {
      return schema.buildTimeSeriesType()
          .withName(typeName)
          .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP,
              TimeSeriesCodec.DELTA_OF_DELTA))
          .withPrecision("MILLISECOND")
          .withColumn(new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.DICTIONARY))
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

        assertThat(readBack.getTsColumn("ts").getCompressionHint()).isEqualTo(TimeSeriesCodec.DELTA_OF_DELTA);
        assertThat(readBack.getTsColumn("host").getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
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
        // the server's 409 is rebuilt as the conflict it is (issue #10153), as the embedded create() raises it
        assertThatThrownBy(() -> restoreRecipe(database.getSchema(), "AlreadyThere").create())
            .isInstanceOf(AlreadyExistsException.class)
            .hasMessageContaining("already exists");

        // The pre-existing type is untouched: the refused statement declared a policy and codecs, and none of it
        // reached the type that was already there.
        final TimeSeriesType untouched = (TimeSeriesType) database.getSchema().getType("AlreadyThere");
        assertThat(untouched.getDownsamplingTiers()).isEmpty();
        assertThat(untouched.getTsColumnNames()).containsExactly("ts", "v");
      }
    }
  }

  /**
   * Issue #7701: a logical (JSONL) export carrying TIMESERIES samples IS restorable into a database a client only
   * reaches over the network - through {@code IMPORT DATABASE}, which the client issues and the server runs.
   * <p>
   * The issue reads the gap the other way round, as {@code JsonlImporterFormat} being typed against
   * {@code DatabaseInternal} end to end and {@code loadTimeSeriesSamples} writing through
   * {@code LocalTimeSeriesType.getEngine()}, which a remote schema has no equivalent of. That is true, and it is not
   * specific to TIMESERIES: {@code AbstractImporter} opens its target with a local {@code DatabaseFactory} and every
   * record type it loads goes through {@code DatabaseInternal}, so the CLIENT-SIDE importer has never driven a
   * {@code RemoteDatabase} for a document, a vertex or an edge either. Teaching it a second, non-embedded write path
   * would be a throughput and transaction design of its own, and it would duplicate a path that already exists.
   * <p>
   * The path that exists is the one below. {@code IMPORT DATABASE} is a SQL statement, so a {@code RemoteDatabase}
   * can issue it like any other command; the server executes it against its own embedded database, where the
   * importer is exactly where it needs to be. What #7689 and #7702 removed were the two DDL-side reasons the type
   * itself could not be recreated on that path - the per-column codecs a restore names because they are not
   * re-derivable (issue #5475), and the column ORDER, which is what the positional samples are read by. This drives
   * a type carrying both, so the claim is checked rather than argued.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7701">issue #7701</a>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LogicalRestore extends BaseGraphServerTest {

    private static final String TYPE_NAME    = "remotereading";
    private static final String RESTORED_DB  = "ts-logical-restore";
    private static final String EXPORT_FILE  = "ts-logical-restore.jsonl.tgz";
    /** {@code EXPORT DATABASE} always writes under 'exports/' relative to the server process' working directory. */
    private static final File   EXPORT_DIR   = new File("./exports");

    @AfterEach
    void dropTheRestoredDatabase() {
      try {
        final RemoteServer server = remoteServer();
        if (server.exists(RESTORED_DB))
          server.drop(RESTORED_DB);
      } finally {
        FileUtils.deleteRecursively(new File(EXPORT_DIR, EXPORT_FILE));
      }
    }

    private int httpPort() {
      // The port the test server actually bound, not the 2480 the range starts at: a server already listening there
      // would otherwise take these requests and fail the test as an authentication error rather than a port conflict.
      return getServer(0).getHttpServer().getPort();
    }

    private RemoteServer remoteServer() {
      return new RemoteServer("127.0.0.1", httpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    }

    private RemoteDatabase remote(final String databaseName) {
      return new RemoteDatabase("127.0.0.1", httpPort(), databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
    }

    /**
     * The whole round trip, driven end to end by a client that holds nothing but {@code RemoteDatabase} handles:
     * create the type, write samples, export, create a second database, import into it, read the samples back.
     * <p>
     * The type interleaves its TAG and FIELD columns on purpose. That is the layout whose restore used to be
     * refused outright, because the DDL could not spell the order and the samples are positional arrays
     * (issues #7702/#7740); {@code f1} and {@code f2} share a data type with nothing between them but {@code t1},
     * so a shift would read back as plausible numbers rather than as an error.
     */
    @Test
    void aTimeSeriesTypeAndItsSamplesSurviveAnExportAndImportDrivenEntirelyRemotely() {
      try (final RemoteDatabase source = remote(getDatabaseName())) {
        source.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts "
            + "FIELDS (f1 DOUBLE CODEC DICTIONARY) TAGS (t1 STRING) FIELDS (f2 DOUBLE) SHARDS 1");

        for (int i = 1; i <= 3; i++)
          source.command("sql", "INSERT INTO " + TYPE_NAME + " SET ts = " + (i * 1_000L)
              + ", f1 = " + (i + 0.5) + ", t1 = 'west', f2 = " + (i * 10 + 0.5));

        source.command("sql", "EXPORT DATABASE 'file://" + EXPORT_FILE + "' WITH format = 'jsonl', overwrite = true");
      }

      final File exported = new File(EXPORT_DIR, EXPORT_FILE);
      assertThat(exported).as("the export the client asked for has to be on the server").exists();

      remoteServer().create(RESTORED_DB);

      try (final RemoteDatabase restored = remote(RESTORED_DB)) {
        restored.command("sql", "IMPORT DATABASE 'file://" + exported.getAbsolutePath() + "'");

        // The schema, read back through the remote schema - so the assertion is about what a remote client sees.
        final TimeSeriesType type = (TimeSeriesType) restored.getSchema().getType(TYPE_NAME);
        assertThat(columnNames(type))
            .as("a sample is a positional array, so the restored order has to be the exported one")
            .containsExactly("ts", "f1", "t1", "f2");

        // And the samples themselves, under the names they were written to.
        final List<Result> rows = rows(restored.query("sql",
            "SELECT ts, f1, t1, f2 FROM " + TYPE_NAME + " ORDER BY ts"));
        assertThat(rows).hasSize(3);
        for (int i = 0; i < rows.size(); i++) {
          // The timestamps are asserted as an ascending sequence rather than as epoch millis: the remote result
          // carries a datetime rendered in the database's own zone, and what this test is about is which COLUMN
          // each value landed in, not how a timestamp is formatted on the wire.
          assertThat(rows.get(i).<Comparable>getProperty("ts")).as("row %d keeps its timestamp", i).isNotNull();
          if (i > 0)
            assertThat(rows.get(i).<Comparable>getProperty("ts"))
                .isGreaterThan(rows.get(i - 1).getProperty("ts"));
          assertThat(rows.get(i).<Number>getProperty("f1").doubleValue()).isEqualTo(i + 1 + 0.5);
          assertThat(rows.get(i).<String>getProperty("t1")).isEqualTo("west");
          assertThat(rows.get(i).<Number>getProperty("f2").doubleValue()).isEqualTo((i + 1) * 10 + 0.5);
        }
      }
    }

    /**
     * The per-column codec a restore names because it cannot be re-derived (issue #5475) arrives on the restored
     * type too, which is the property that made this path unusable before issue #7689: the DDL the restore issues
     * had no way to say it, so a re-derived default would have silently re-encoded the column.
     */
    @Test
    void anExplicitCodecSurvivesTheRemoteRestore() {
      try (final RemoteDatabase source = remote(getDatabaseName())) {
        source.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts CODEC DELTA_OF_DELTA "
            + "TAGS (t1 STRING) FIELDS (f1 DOUBLE CODEC DICTIONARY) SHARDS 1");
        source.command("sql", "INSERT INTO " + TYPE_NAME + " SET ts = 1000, t1 = 'west', f1 = 1.5");
        source.command("sql", "EXPORT DATABASE 'file://" + EXPORT_FILE + "' WITH format = 'jsonl', overwrite = true");
      }

      remoteServer().create(RESTORED_DB);

      try (final RemoteDatabase restored = remote(RESTORED_DB)) {
        restored.command("sql",
            "IMPORT DATABASE 'file://" + new File(EXPORT_DIR, EXPORT_FILE).getAbsolutePath() + "'");

        final TimeSeriesType type = (TimeSeriesType) restored.getSchema().getType(TYPE_NAME);
        assertThat(type.getTsColumn("ts").getCompressionHint().name()).isEqualTo("DELTA_OF_DELTA");
        assertThat(type.getTsColumn("f1").getCompressionHint().name()).isEqualTo("DICTIONARY");
      }
    }

    private static List<String> columnNames(final TimeSeriesType type) {
      final List<String> names = new ArrayList<>(type.getTsColumns().size());
      for (final ColumnDefinition column : type.getTsColumns())
        names.add(column.getName());
      return names;
    }

    private static List<Result> rows(final ResultSet resultSet) {
      final List<Result> rows = new ArrayList<>();
      while (resultSet.hasNext())
        rows.add(resultSet.next());
      return rows;
    }
  }

  /**
   * Issue #7716: {@link RemoteDatabase} hid the server's time-series refusal in the exception CAUSE, while
   * {@code RemoteGrpcDatabase} put it in the message.
   * <p>
   * The server names the offending member in its refusal - which is the whole point of issues #7334, #7340 and
   * #7675 - and the HTTP client used to answer a fixed {@code "Error on time series query"} with the server's
   * {@code error} body hung off it as the cause. Nothing was lost, but what a caller SEES first said nothing it
   * could act on: an application logging {@code e.getMessage()}, which is the common thing to do, logged
   * {@code Error on time series query} for a typo the server had been careful enough to name, and the same
   * assertion could not be written against both protocols -
   * {@code Issue7675GrpcTimeSeriesProjectionIT} had to use {@code hasStackTraceContaining} for exactly this.
   * <p>
   * Every case below is asserted with {@code hasMessageContaining}, which is the assertion that used to fail.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RefusalMessage extends BaseGraphServerTest {

    private static final String TYPE = "refusalmetric";

    @Override
    protected int getServerCount() {
      return 1;
    }

    private RemoteDatabase remote() {
      return new RemoteDatabase("127.0.0.1", getServer(0).getHttpServer().getPort(), getDatabaseName(), "root",
          DEFAULT_PASSWORD_FOR_TESTS);
    }

    private void createType(final RemoteDatabase database) {
      if (!database.query("sql", "SELECT FROM schema:types WHERE name = '" + TYPE + "'").hasNext())
        database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
            + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");
    }

    /**
     * The case #7716 was reported for: a projection naming a column the type does not have. The server answers
     * 400 with that sentence in {@code error} since issue #7675, and the caller has to be able to read it off
     * {@code getMessage()} - which is the assertion {@code Issue7675GrpcTimeSeriesProjectionIT} had to write as
     * {@code hasStackTraceContaining} instead, and said so.
     */
    @Test
    void anUnresolvableProjectionIsNamedInTheMessage() {
      try (final RemoteDatabase database = remote()) {
        createType(database);

        assertThatThrownBy(() -> database.timeSeriesQuery(new TimeSeriesQuery(TYPE).fields("nosuchcolumn")))
            .hasMessageContaining("nosuchcolumn");
      }
    }

    /** A tag name that resolves to no TAG column, refused by name since issue #7334. */
    @Test
    void anUnresolvableTagIsNamedInTheMessage() {
      try (final RemoteDatabase database = remote()) {
        createType(database);

        assertThatThrownBy(() -> database.timeSeriesQuery(new TimeSeriesQuery(TYPE).tag("nosuchtag", "x")))
            .hasMessageContaining("nosuchtag");
      }
    }

    /** And an aggregation over a field the type does not carry. */
    @Test
    void anUnknownAggregationFieldIsNamedInTheMessage() {
      try (final RemoteDatabase database = remote()) {
        createType(database);

        assertThatThrownBy(() -> database.timeSeriesQuery(
            new TimeSeriesQuery(TYPE).aggregate(1_000L, new Aggregation("nosuchfield", AggregationType.SUM))))
            .hasMessageContaining("nosuchfield");
      }
    }

    /**
     * Issue #7725 over the wire: aggregating a column no storage layer can read as a number is now refused by
     * name, where it used to answer zeros until compaction ran and an internal error afterwards.
     */
    @Test
    void aggregatingATagIsRefusedByNameOverTheWire() {
      try (final RemoteDatabase database = remote()) {
        createType(database);

        assertThatThrownBy(() -> database.timeSeriesQuery(
            new TimeSeriesQuery(TYPE).aggregate(1_000L, new Aggregation("location", AggregationType.SUM))))
            .hasMessageContaining("location")
            .hasMessageContaining("is not stored as a number");
      }
    }

    /** The {@code /ts/latest} endpoint takes the same path through the client, so it gets the same treatment. */
    @Test
    void theLatestEndpointCarriesItsReasonInTheMessageToo() {
      try (final RemoteDatabase database = remote()) {
        createType(database);
        database.timeSeriesWrite(List.of(
            new TimeSeriesPoint(TYPE, 1_000L, Map.of("location", "us-east"), Map.of("temperature", 21.0))));

        assertThatThrownBy(() -> database.timeSeriesLatest(TYPE, "nosuchtag", "x"))
            .hasMessageContaining("nosuchtag");
      }
    }
  }

  /**
   * Issue #7740, item 3: {@code RemoteSchema.reload()} reused a cached type instance by NAME alone, so the kind
   * switch that decides its Java class ran only for a name the previous snapshot did not carry.
   * <p>
   * A drop-and-recreate that changes a type's kind is therefore invisible to a live connection: the same name coming
   * back as a TIMESERIES type left {@code getType(name) instanceof TimeSeriesType} false forever on that connection,
   * an explicit {@code reload()} included, and the only recovery was a new {@link RemoteDatabase}. The line is
   * pre-existing; what made it consequential is that a remote type acquired a kind to change into.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7740">issue #7740</a>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaReloadKind extends BaseGraphServerTest {

    private RemoteDatabase remote() {
      return new RemoteDatabase("127.0.0.1", getServer(0).getHttpServer().getPort(), getDatabaseName(), "root",
          DEFAULT_PASSWORD_FOR_TESTS);
    }

    @Test
    void aTypeRecreatedAsATimeSeriesTypeComesBackAsOne() {
      try (final RemoteDatabase database = remote()) {
        database.command("sql", "CREATE DOCUMENT TYPE Metric");
        assertThat(database.getSchema().getType("Metric")).isNotInstanceOf(TimeSeriesType.class);

        database.command("sql", "DROP TYPE Metric");
        database.command("sql", "CREATE TIMESERIES TYPE Metric TIMESTAMP ts FIELDS (value DOUBLE)");
        // Explicitly, because that is the strong form of the claim: even asking for a reload did not change it.
        ((RemoteSchema) database.getSchema()).reload();

        assertThat(database.getSchema().getType("Metric"))
            .as("the kind is re-read on the connection that changed it, not only on a fresh one")
            .isInstanceOf(TimeSeriesType.class);
      }
    }

    /** The same in the other direction, and on a kind pair that predates TIMESERIES types entirely. */
    @Test
    void aTypeRecreatedAsAVertexTypeComesBackAsOne() {
      try (final RemoteDatabase database = remote()) {
        database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts FIELDS (value DOUBLE)");
        assertThat(database.getSchema().getType("Reading")).isInstanceOf(TimeSeriesType.class);

        database.command("sql", "DROP TYPE Reading");
        database.command("sql", "CREATE VERTEX TYPE Reading");
        ((RemoteSchema) database.getSchema()).reload();

        final DocumentType recreated = database.getSchema().getType("Reading");
        assertThat(recreated).isInstanceOf(VertexType.class);
        assertThat(recreated).isNotInstanceOf(TimeSeriesType.class);
      }
    }

    /** A type that did not change kind keeps being reloaded in place, which is what the cache is for. */
    @Test
    void aTypeThatKeepsItsKindIsReloadedInPlace() {
      try (final RemoteDatabase database = remote()) {
        database.command("sql", "CREATE DOCUMENT TYPE Stable");
        final DocumentType first = database.getSchema().getType("Stable");

        database.command("sql", "CREATE PROPERTY Stable.name STRING");
        ((RemoteSchema) database.getSchema()).reload();
        final DocumentType second = database.getSchema().getType("Stable");

        assertThat(second).as("same kind, same instance").isSameAs(first);
        assertThat(second.existsProperty("name")).isTrue();
      }
    }
  }
}
