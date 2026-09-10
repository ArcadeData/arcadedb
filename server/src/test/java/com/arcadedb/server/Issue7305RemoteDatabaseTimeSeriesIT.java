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

import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.timeseries.TimeSeriesBucket;
import com.arcadedb.remote.timeseries.TimeSeriesLatestResult;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.remote.timeseries.TimeSeriesQuery;
import com.arcadedb.remote.timeseries.TimeSeriesQueryResult;
import com.arcadedb.remote.timeseries.TimeSeriesWriteSummary;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

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
class Issue7305RemoteDatabaseTimeSeriesIT extends BaseGraphServerTest {

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
