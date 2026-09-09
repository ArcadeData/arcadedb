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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.timeseries.TimeSeriesBucket;
import com.arcadedb.remote.timeseries.TimeSeriesLatestResult;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.remote.timeseries.TimeSeriesQuery;
import com.arcadedb.remote.timeseries.TimeSeriesQueryResult;
import com.arcadedb.remote.timeseries.TimeSeriesWriteSummary;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #7305: the gRPC proto had no time-series surface at all, so the only way to reach time series over
 * gRPC was generic SQL text through {@code ExecuteCommand}/{@code ExecuteQuery}. These tests drive each of the
 * four new RPCs - {@code TimeSeriesWrite}, {@code TimeSeriesWriteStream}, {@code TimeSeriesQuery} and
 * {@code TimeSeriesLatest} - through {@link RemoteGrpcDatabase} against a live server.
 * <p>
 * The equivalence tests are the point of the whole design: {@code RemoteGrpcDatabase extends RemoteDatabase}
 * and overrides these four methods, so the same calls run over either protocol, and the server side of both
 * reaches the samples through one shared {@code TimeSeriesGateway}. A test that writes over one protocol and
 * reads over the other is what stops the two from drifting.
 */
class Issue7305TimeSeriesGrpcIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT = 50051;
  private static final String TYPE      = "weather";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;
  private RemoteDatabase     http;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (grpc != null) {
      grpc.close();
      grpc = null;
    }
    if (http != null) {
      http.close();
      http = null;
    }
    if (grpcServer != null) {
      grpcServer.close();
      grpcServer = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  /**
   * The port the test server actually bound, not the 2480 the configured range starts at: a server already
   * listening there pushes this one up, and a hard-coded port would send these requests to that stranger and
   * fail as an authentication error rather than as a port conflict.
   */
  private int httpPort() {
    return getServer(0).getHttpServer().getPort();
  }

  private RemoteGrpcDatabase grpcClient() {
    if (grpc == null) {
      grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
      grpc = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, httpPort(), getDatabaseName(), "root",
          DEFAULT_PASSWORD_FOR_TESTS);
    }
    return grpc;
  }

  private RemoteDatabase httpClient() {
    if (http == null)
      http = new RemoteDatabase("127.0.0.1", httpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    return http;
  }

  private void createType() {
    grpcClient().command("sql", "CREATE TIMESERIES TYPE " + TYPE
        + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");
  }

  private static List<TimeSeriesPoint> threeSamples() {
    return List.of(
        new TimeSeriesPoint(TYPE, 1_000L, Map.of("location", "us-east"), Map.of("temperature", 22.5)),
        new TimeSeriesPoint(TYPE, 2_000L, Map.of("location", "us-west"), Map.of("temperature", 18.3)),
        new TimeSeriesPoint(TYPE, 3_000L, Map.of("location", "us-east"), Map.of("temperature", 23.1)));
  }

  private static List<Long> timestampsOf(final TimeSeriesQueryResult result) {
    final List<Long> timestamps = new ArrayList<>();
    for (final Object[] row : result.rows())
      timestamps.add(((Number) row[0]).longValue());
    return timestamps;
  }

  @Test
  void unaryWriteAndStreamingQueryRoundTrip() {
    createType();

    final TimeSeriesWriteSummary summary = grpcClient().timeSeriesWrite(threeSamples());
    assertThat(summary.isComplete()).isTrue();
    assertThat(summary.received()).isEqualTo(3);
    assertThat(summary.written()).isEqualTo(3);

    final TimeSeriesQueryResult result = grpcClient().timeSeriesQuery(
        new TimeSeriesQuery(TYPE).from(1_000).to(3_000));
    assertThat(result.isAggregated()).isFalse();
    assertThat(result.columns()).containsExactly("ts", "location", "temperature");
    assertThat(timestampsOf(result)).containsExactly(1_000L, 2_000L, 3_000L);
    assertThat(result.truncated()).isFalse();

    // gRPC carries the value types, so a DOUBLE column arrives as a Double rather than as whatever a JSON
    // parser chose for its text.
    assertThat(result.rows().getFirst()[2]).isInstanceOf(Double.class).isEqualTo(22.5);
  }

  @Test
  void theStreamingWriteCrossesAChunkBoundary() {
    createType();

    final List<TimeSeriesPoint> points = new ArrayList<>();
    for (int i = 0; i < 2_500; i++)
      points.add(new TimeSeriesPoint(TYPE, 100_000L + i, Map.of("location", "us-east"),
          Map.of("temperature", (double) i)));

    // 2500 points at 500 per chunk is five chunks, so this exercises the server's per-chunk append and the
    // accumulation of the totals across them - not just the single-chunk case a smaller batch would take.
    final TimeSeriesWriteSummary summary = grpcClient().timeSeriesWriteStream(points, 500);
    assertThat(summary.received()).isEqualTo(2_500);
    assertThat(summary.written()).isEqualTo(2_500);
    assertThat(summary.isComplete()).isTrue();

    // Reading them back also crosses the server's own response batch boundary (1000 rows per message by
    // default), so this covers the streamed query's multi-message path as well as the streamed write's.
    final TimeSeriesQueryResult result = grpcClient().timeSeriesQuery(new TimeSeriesQuery(TYPE).limit(10_000));
    assertThat(result.count()).isEqualTo(2_500);
    assertThat(result.truncated()).isFalse();
    assertThat(timestampsOf(result).getFirst()).isEqualTo(100_000L);
    assertThat(timestampsOf(result).getLast()).isEqualTo(102_499L);
  }

  @Test
  void aLimitCutsTheStreamAndTheLastMessageSaysSo() {
    createType();
    grpcClient().timeSeriesWrite(threeSamples());

    final TimeSeriesQueryResult truncated = grpcClient().timeSeriesQuery(new TimeSeriesQuery(TYPE).limit(2));
    assertThat(truncated.count()).isEqualTo(2);
    // Without this the client cannot tell a complete answer from one the limit cut short.
    assertThat(truncated.truncated()).isTrue();

    final TimeSeriesQueryResult complete = grpcClient().timeSeriesQuery(new TimeSeriesQuery(TYPE).limit(3));
    assertThat(complete.count()).isEqualTo(3);
    assertThat(complete.truncated()).isFalse();
  }

  @Test
  void aTagFilterAndAFieldProjectionNarrowTheAnswer() {
    createType();
    grpcClient().timeSeriesWrite(threeSamples());

    final TimeSeriesQueryResult filtered = grpcClient().timeSeriesQuery(
        new TimeSeriesQuery(TYPE).tag("location", "us-east"));
    assertThat(timestampsOf(filtered)).containsExactly(1_000L, 3_000L);

    final TimeSeriesQueryResult projected = grpcClient().timeSeriesQuery(
        new TimeSeriesQuery(TYPE).fields("temperature"));
    assertThat(projected.columns()).containsExactly("ts", "temperature");

    // The values must line up with those names: the projection's indices count non-timestamp columns, and a
    // resolver using full-schema ones answered [ts, location, null] under the header [ts, temperature].
    final Object[] first = projected.rows().getFirst();
    assertThat(first).hasSize(2);
    assertThat(((Number) first[0]).longValue()).isEqualTo(1_000L);
    assertThat(first[1]).isEqualTo(22.5);
  }

  @Test
  void anAggregatedQueryReturnsBuckets() {
    createType();
    grpcClient().timeSeriesWrite(threeSamples());

    final TimeSeriesQueryResult result = grpcClient().timeSeriesQuery(new TimeSeriesQuery(TYPE)
        .aggregate(5_000L,
            new TimeSeriesQuery.Aggregation("temperature", AggregationType.AVG, "avg_temp"),
            new TimeSeriesQuery.Aggregation("temperature", AggregationType.MAX)));

    assertThat(result.isAggregated()).isTrue();
    // The second aggregate states no alias, so it gets the same default the HTTP endpoint applies.
    assertThat(result.aggregations()).containsExactly("avg_temp", "temperature_max");
    assertThat(result.buckets()).isNotEmpty();

    final TimeSeriesBucket bucket = result.buckets().getFirst();
    assertThat(((Number) bucket.value(0)).doubleValue()).isCloseTo((22.5 + 18.3 + 23.1) / 3, within(1e-9));
    assertThat(((Number) bucket.value(1)).doubleValue()).isCloseTo(23.1, within(1e-9));
  }

  @Test
  void latestReturnsTheNewestSampleAndHonoursATag() {
    createType();
    grpcClient().timeSeriesWrite(threeSamples());

    final TimeSeriesLatestResult latest = grpcClient().timeSeriesLatest(TYPE);
    assertThat(latest.isPresent()).isTrue();
    assertThat(latest.columns()).containsExactly("ts", "location", "temperature");
    assertThat(((Number) latest.value("ts")).longValue()).isEqualTo(3_000L);

    final TimeSeriesLatestResult western = grpcClient().timeSeriesLatest(TYPE, "location", "us-west");
    assertThat(((Number) western.value("ts")).longValue()).isEqualTo(2_000L);
  }

  @Test
  void latestOfAnEmptyTypeIsAbsentRatherThanAnError() {
    grpcClient().command("sql", "CREATE TIMESERIES TYPE emptyts TIMESTAMP ts TAGS (t STRING) FIELDS (v DOUBLE)");

    final TimeSeriesLatestResult latest = grpcClient().timeSeriesLatest("emptyts");
    assertThat(latest.isPresent()).isFalse();
    assertThat(latest.latest()).isNull();
  }

  @Test
  void writingToAnUnknownTypeIsReportedAsAPartialWriteRatherThanThrowing() {
    createType();

    final TimeSeriesWriteSummary summary = grpcClient().timeSeriesWrite(List.of(
        new TimeSeriesPoint(TYPE, 1_000L, Map.of("location", "us-east"), Map.of("temperature", 22.5)),
        new TimeSeriesPoint("nosuchtype", 1_000L, Map.of(), Map.of("v", 1.0))));

    // The sample that did apply is durable: a partial write, not a rollback. A client that read this as a
    // total failure would re-send data it already stored.
    assertThat(summary.written()).isEqualTo(1);
    assertThat(summary.dropped()).isEqualTo(1);
    assertThat(summary.unknownTypes()).containsExactly("nosuchtype");
  }

  @Test
  void queryingATypeThatDoesNotExistFailsRatherThanReturningNothing() {
    // An empty answer and "there is no such type" are different facts, and a client that cannot tell them
    // apart silently treats a typo as "no data".
    assertThatThrownBy(() -> grpcClient().timeSeriesQuery(new TimeSeriesQuery("nosuchtype")))
        .hasMessageContaining("nosuchtype");

    grpcClient().command("sql", "CREATE DOCUMENT TYPE notts");
    assertThatThrownBy(() -> grpcClient().timeSeriesQuery(new TimeSeriesQuery("notts")))
        .hasMessageContaining("not a TimeSeries type");
  }

  @Test
  void samplesWrittenOverHttpAreReadableOverGrpcAndTheAnswersAgree() {
    createType();

    // Written through the HTTP client (line protocol), read through both.
    httpClient().timeSeriesWrite(threeSamples());

    final TimeSeriesQueryResult overHttp = httpClient().timeSeriesQuery(new TimeSeriesQuery(TYPE));
    final TimeSeriesQueryResult overGrpc = grpcClient().timeSeriesQuery(new TimeSeriesQuery(TYPE));

    assertThat(overGrpc.columns()).isEqualTo(overHttp.columns());
    assertThat(timestampsOf(overGrpc)).isEqualTo(timestampsOf(overHttp));
    assertRowsAgree(overHttp, overGrpc);

    final TimeSeriesLatestResult latestHttp = httpClient().timeSeriesLatest(TYPE);
    final TimeSeriesLatestResult latestGrpc = grpcClient().timeSeriesLatest(TYPE);
    assertThat(latestGrpc.columns()).isEqualTo(latestHttp.columns());
    assertValuesAgree(latestHttp.latest(), latestGrpc.latest());
  }

  @Test
  void samplesWrittenOverGrpcAreReadableOverHttpAndTheAnswersAgree() {
    createType();

    grpcClient().timeSeriesWrite(threeSamples());

    final TimeSeriesQueryResult overHttp = httpClient().timeSeriesQuery(new TimeSeriesQuery(TYPE));
    final TimeSeriesQueryResult overGrpc = grpcClient().timeSeriesQuery(new TimeSeriesQuery(TYPE));

    assertThat(overHttp.count()).isEqualTo(3);
    assertRowsAgree(overHttp, overGrpc);

    // An aggregate computed over gRPC-written samples must match the one computed over the same samples read
    // by the other protocol: the aggregation runs server-side in both cases, on one shared implementation.
    final TimeSeriesQuery aggregated = new TimeSeriesQuery(TYPE)
        .aggregate(5_000L, new TimeSeriesQuery.Aggregation("temperature", AggregationType.AVG, "avg_temp"));
    final TimeSeriesQueryResult bucketsHttp = httpClient().timeSeriesQuery(aggregated);
    final TimeSeriesQueryResult bucketsGrpc = grpcClient().timeSeriesQuery(aggregated);

    assertThat(bucketsGrpc.aggregations()).isEqualTo(bucketsHttp.aggregations());
    assertThat(bucketsGrpc.buckets()).hasSameSizeAs(bucketsHttp.buckets());
    for (int i = 0; i < bucketsHttp.buckets().size(); i++) {
      assertThat(bucketsGrpc.buckets().get(i).timestampMs()).isEqualTo(bucketsHttp.buckets().get(i).timestampMs());
      assertValuesAgree(bucketsHttp.buckets().get(i).values(), bucketsGrpc.buckets().get(i).values());
    }
  }

  @Test
  void aSealedShardIsReadableOverGrpc() throws Exception {
    createType();
    grpcClient().timeSeriesWrite(threeSamples());

    // Compaction moves the samples out of the paginated mutable bucket into the .ts.sealed store, which is raw
    // FileChannel I/O outside the page cache - a different reader entirely. A query that only ever saw mutable
    // data would never exercise it.
    sealEveryShard();

    final TimeSeriesQueryResult result = grpcClient().timeSeriesQuery(new TimeSeriesQuery(TYPE));
    assertThat(timestampsOf(result)).containsExactly(1_000L, 2_000L, 3_000L);
    assertThat(result.rows().getFirst()[2]).isEqualTo(22.5);

    // The tag filter and the latest RPC read the sealed layer through their own code paths, so both are worth
    // driving once the data is no longer where the earlier tests left it.
    final TimeSeriesQueryResult eastern = grpcClient().timeSeriesQuery(
        new TimeSeriesQuery(TYPE).tag("location", "us-east"));
    assertThat(timestampsOf(eastern)).containsExactly(1_000L, 3_000L);

    assertThat(((Number) grpcClient().timeSeriesLatest(TYPE).value("ts")).longValue()).isEqualTo(3_000L);

    // And the two protocols still agree once the data is sealed.
    assertRowsAgree(httpClient().timeSeriesQuery(new TimeSeriesQuery(TYPE)), result);
  }

  /**
   * Compacts every shard of the test type on the server, moving its samples into the sealed store, and proves
   * it happened before the caller asserts anything about reading it.
   * <p>
   * Without that proof the sealed-blob test is worthless: if {@code compactAll()} were ever a no-op for this
   * fixture - too few samples, a changed threshold - every assertion after it would still pass, against the
   * mutable bucket, while claiming to cover the raw {@code FileChannel} reader.
   */
  private void sealEveryShard() throws Exception {
    final DatabaseInternal database = (DatabaseInternal) getServer(0).getDatabase(getDatabaseName());
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
    type.requireEngine().compactAll();

    try (final Stream<Path> files = Files.walk(Path.of(database.getDatabasePath()))) {
      final List<Path> sealed = files.filter(p -> p.getFileName().toString().endsWith(".ts.sealed"))
          .filter(p -> {
            try {
              return Files.size(p) > 0;
            } catch (final IOException e) {
              return false;
            }
          })
          .toList();
      assertThat(sealed).as("compactAll() must have produced a non-empty .ts.sealed store to read from")
          .isNotEmpty();
    }
  }

  private static void assertRowsAgree(final TimeSeriesQueryResult expected, final TimeSeriesQueryResult actual) {
    assertThat(actual.rows()).hasSameSizeAs(expected.rows());
    for (int i = 0; i < expected.rows().size(); i++)
      assertValuesAgree(expected.rows().get(i), actual.rows().get(i));
  }

  /**
   * Compares two rows numerically rather than by {@code equals}.
   * <p>
   * The two protocols carry the same VALUES, not the same boxed types: gRPC ships a DOUBLE column as a
   * {@code double} and a timestamp as an {@code int64}, while the HTTP answer is JSON text whose numbers the
   * parser boxes as whatever fits. Asserting {@code equals} would fail on 1000 as Integer versus Long and say
   * nothing about whether the databases agree, which is the question.
   */
  private static void assertValuesAgree(final Object[] expected, final Object[] actual) {
    assertThat(actual).hasSameSizeAs(expected);
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null || actual[i] == null) {
        assertThat(actual[i]).as("value %d", i).isEqualTo(expected[i]);
      } else if (expected[i] instanceof Number expectedNumber && actual[i] instanceof Number actualNumber) {
        assertThat(actualNumber.doubleValue()).as("value %d", i).isCloseTo(expectedNumber.doubleValue(), within(1e-9));
      } else {
        assertThat(actual[i]).as("value %d", i).isEqualTo(expected[i]);
      }
    }
  }
}
