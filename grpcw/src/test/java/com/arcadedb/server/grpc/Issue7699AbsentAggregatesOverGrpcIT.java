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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7699: the gRPC twin of {@code Issue7584AbsentSumAvgOverHttpIT} and {@code Issue7043AbsentMinMaxOverHttpIT},
 * driving the streaming {@code TimeSeriesQuery} RPC over a real channel.
 * <p>
 * Issue #7694 added {@code Issue7694AbsentSumAvgGrpcEncodingTest}, which composes the real
 * {@code MultiColumnAggregationResult} with the real {@link GrpcTimeSeriesSupport#toSampleValue} exactly as the RPC
 * does. What it cannot establish is that the RPC still CALLS it: replacing that one line with
 * {@code GrpcTypeConverter.toGrpcValue(...)} left every test in the repository green, and a client would then read
 * an absent SUM as {@code 0.0} and an absent MIN/MAX as the number {@code NaN}. The HTTP twins have no such hole
 * because they go over the wire; this closes it for gRPC.
 * <p>
 * The distinction being held is the one the whole NaN-as-absent policy exists for (issues #4596/#7043/#7089/#7694):
 * a bucket with no real measurement arrives as {@code KIND_NOT_SET}, and a bucket whose real samples add up to zero
 * arrives as a set {@code 0}. A {@code counts == 0} guard could express neither.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7699">issue #7699</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7699AbsentAggregatesOverGrpcIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT = 50051;
  private static final String TYPE_NAME = "nanreading";
  /** One bucket wide enough to hold every sample, so the answer is a single bucket to assert on. */
  private static final long   BUCKET_MS = 1_000_000L;

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel,
        new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName()));
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);

    authenticatedStub.executeCommand(command(
        "CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1"));
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    try {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    } finally {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
    }
  }

  /**
   * The regression: over the wire, with nothing real in the bucket, all four aggregates must arrive UNSET. Each is
   * asserted separately because they reach {@code toSampleValue} carrying different absences - SUM and AVG the one
   * issue #7089 gave them, MIN and MAX the seed of issue #7043 - and a change that broke only one of the two would
   * otherwise hide behind the other.
   */
  @Test
  void anAllNaNBucketArrivesUnsetForSumAvgMinAndMax() {
    appendSamples(new long[] { 1_000L, 2_000L }, new Object[] { Double.NaN, Double.NaN });

    final List<GrpcValue> values = aggregate();

    assertThat(values).hasSize(4);
    assertThat(values.get(0).getKindCase()).as("SUM over an all-NaN bucket").isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(1).getKindCase()).as("AVG over an all-NaN bucket").isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(2).getKindCase()).as("MIN over an all-NaN bucket").isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(3).getKindCase()).as("MAX over an all-NaN bucket").isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
  }

  /**
   * The counter-case, and the reason the encoding cannot simply be "send nothing for a zero": a real total of zero
   * is a measurement and must arrive as a set {@code 0}. Without this a client could not tell the two apart in the
   * other direction either.
   */
  @Test
  void aRealTotalOfZeroArrivesAsTheNumberZero() {
    appendSamples(new long[] { 1_000L, 2_000L, 3_000L, 4_000L },
        new Object[] { Double.NaN, Double.NaN, 2.5, -2.5 });

    final List<GrpcValue> values = aggregate();

    assertThat(values.get(0).getKindCase()).as("a real total of zero is data, not a gap")
        .isEqualTo(GrpcValue.KindCase.DOUBLE_VALUE);
    assertThat(values.get(0).getDoubleValue()).isEqualTo(0.0);
    // The two NaN samples are skipped, so AVG divides by the 2 real ones rather than by 4.
    assertThat(values.get(1).getDoubleValue()).isEqualTo(0.0);
    assertThat(values.get(2).getDoubleValue()).as("MIN of the real samples only").isEqualTo(-2.5);
    assertThat(values.get(3).getDoubleValue()).as("MAX of the real samples only").isEqualTo(2.5);
  }

  /** And a populated bucket still answers real numbers, with the absent samples skipped rather than propagated. */
  @Test
  void realSamplesStillAnswerNumbersWithTheAbsentOnesSkipped() {
    appendSamples(new long[] { 1_000L, 2_000L, 3_000L, 4_000L },
        new Object[] { Double.NaN, Double.NaN, 4.0, 6.0 });

    final List<GrpcValue> values = aggregate();

    assertThat(values.get(0).getDoubleValue()).as("SUM of the real samples only").isEqualTo(10.0);
    assertThat(values.get(1).getDoubleValue()).as("AVG divides by the 2 real samples, not by 4").isEqualTo(5.0);
    assertThat(values.get(2).getDoubleValue()).isEqualTo(4.0);
    assertThat(values.get(3).getDoubleValue()).isEqualTo(6.0);
  }

  /**
   * A client CAN send an absent sample: {@code GrpcValue.double_value} carries {@code NaN} perfectly well, so the
   * write RPC is a second way into the same state and is worth pinning as such. What comes back must be
   * indistinguishable from the samples written through the server's own engine above.
   */
  @Test
  void aNaNSampleSentThroughTimeSeriesWriteReadsBackAsAbsent() {
    final TimeSeriesWriteRequest.Builder write = TimeSeriesWriteRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME);
    for (final long timestamp : new long[] { 1_000L, 2_000L })
      write.addPoints(TimeSeriesPoint.newBuilder()
          .setTimestamp(timestamp)
          .putFields("value", GrpcValue.newBuilder().setDoubleValue(Double.NaN).build()));

    assertThat(authenticatedStub.timeSeriesWrite(write.build()).getWritten()).isEqualTo(2);

    final List<GrpcValue> values = aggregate();
    assertThat(values.get(0).getKindCase()).as("a client-written NaN is an absence, not a measurement")
        .isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(2).getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
  }

  /**
   * The raw row path carries the same absence through the same encoder, so the two halves of the RPC cannot drift:
   * a NaN sample read back as a row arrives unset rather than as a NaN double.
   */
  @Test
  void anAbsentSampleOnTheRawRowPathArrivesUnsetToo() {
    appendSamples(new long[] { 1_000L, 2_000L }, new Object[] { Double.NaN, 3.5 });

    final List<TimeSeriesRow> rows = new ArrayList<>();
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(query().build());
    while (stream.hasNext())
      rows.addAll(stream.next().getRowsList());

    assertThat(rows).hasSize(2);
    // Column 0 is the timestamp, column 1 the value.
    assertThat(rows.getFirst().getValues(1).getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(rows.get(1).getValues(1).getDoubleValue()).isEqualTo(3.5);
  }

  /** The single bucket's four aggregate values, in request order: SUM, AVG, MIN, MAX. */
  private List<GrpcValue> aggregate() {
    final TimeSeriesAggregation.Builder aggregation = TimeSeriesAggregation.newBuilder()
        .setBucketIntervalMs(BUCKET_MS);
    for (final TimeSeriesAggregationType type : new TimeSeriesAggregationType[] {
        TimeSeriesAggregationType.TS_AGG_SUM, TimeSeriesAggregationType.TS_AGG_AVG,
        TimeSeriesAggregationType.TS_AGG_MIN, TimeSeriesAggregationType.TS_AGG_MAX })
      aggregation.addRequests(TimeSeriesAggregationRequest.newBuilder().setField("value").setType(type));

    final List<TimeSeriesBucket> buckets = new ArrayList<>();
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(
        query().setAggregation(aggregation).build());
    while (stream.hasNext())
      buckets.addAll(stream.next().getBucketsList());

    assertThat(buckets).as("one bucket wide enough for every sample").hasSize(1);
    return buckets.getFirst().getValuesList();
  }

  /**
   * A NaN sample has no line-protocol or SQL literal, so these go in through the server's own database - the same
   * way {@code Issue7584AbsentSumAvgOverHttpIT} writes them.
   */
  private void appendSamples(final long[] timestamps, final Object[] values) {
    final Database database = getServerDatabase(0, getDatabaseName());
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    database.begin();
    try {
      tsType.getEngine().appendSamples(timestamps, values);
    } catch (final Exception e) {
      throw new RuntimeException("Cannot append the test samples", e);
    }
    database.commit();
  }

  private TimeSeriesQueryRequest.Builder query() {
    return TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME)
        .setFromTimestamp(0L)
        .setToTimestamp(10_000L);
  }

  private ExecuteCommandRequest command(final String sql) {
    return ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand(sql)
        .setLanguage("sql")
        .build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
  }
}
