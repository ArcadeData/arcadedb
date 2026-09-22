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
import com.arcadedb.schema.LocalTimeSeriesType;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The gRPC side of issues #7725 and #7717, which the engine and HTTP tests could not reach.
 * <p>
 * Both fixes are wired into {@code ArcadeDbGrpcService} by hand - the refusal is translated into a gRPC status
 * of its own, and the metrics publish sits in a {@code finally} on two different RPCs - so the shared logic
 * being tested at the engine and HTTP levels says nothing about whether THIS wiring is right. That gap is what
 * this class closes: a caller-supplied column no storage layer can read as a number must be refused with a
 * status a client can act on, and the read counters must reach the sink for the aggregated stream and for
 * {@code TimeSeriesLatest} alike - the latter being the RPC that was silently missing its publish.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7725GrpcTimeSeriesRefusalAndMetricsIT extends BaseGrpcServerTest {

  private static final String TYPE_NAME = "grpcreading";
  private static final int    SAMPLES   = 40;
  private static final long   BASE_TS   = 1_700_000_000_000L;

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    // Without this the sink stays disabled and every publish is a no-op, so the metric assertions below would
    // pass vacuously against an empty registry.
    GlobalConfiguration.SERVER_METRICS.setValue(true);
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel,
        new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName()));
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);

    authenticatedStub.executeCommand(command("CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE, active BOOLEAN)"));

    final TimeSeriesWriteRequest.Builder write = TimeSeriesWriteRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME);
    for (int i = 0; i < SAMPLES; i++)
      write.addPoints(TimeSeriesPoint.newBuilder()
          .setTimestamp(BASE_TS + i * 1_000L)
          .putTags("host", GrpcValue.newBuilder().setStringValue("web1").build())
          .putFields("value", GrpcValue.newBuilder().setDoubleValue(i).build())
          .putFields("active", GrpcValue.newBuilder().setBoolValue(i % 2 == 0).build()));
    assertThat(authenticatedStub.timeSeriesWrite(write.build()).getWritten()).isEqualTo(SAMPLES);

    // Sealed before anything is read, so the read has BLOCKS to decide about: the block counters are the ones
    // that say whether the push-downs are working, and a type whose samples are all still in the mutable bucket
    // reports zero of them however well the sink is wired. Done through the server's own database because the
    // wire protocol has no compaction call - the HTTP twin of this test does the same.
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) getServerDatabase(0, getDatabaseName())
        .getSchema().getType(TYPE_NAME);
    try {
      tsType.getEngine().compactAll();
    } catch (final IOException e) {
      throw new IllegalStateException("cannot seal the test samples", e);
    }
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    try {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    } finally {
      GlobalConfiguration.SERVER_METRICS.reset();
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
    }
  }

  /**
   * Issue #7725 over gRPC. A TAG is dictionary-encoded whatever its declared type, so no storage layer can read
   * it as a number: the mutable layer used to answer a column of zeros and the sealed layer to fail inside its
   * decoder. It is now refused before the engine is asked for anything, as INVALID_ARGUMENT - the same status
   * the sibling refusals on this RPC use - and the description names the column, so the client can tell which
   * of several requests was the wrong one.
   */
  @Test
  void aggregatingATagIsRefusedAsInvalidArgumentNamingTheColumn() {
    final StatusRuntimeException ex = catchThrowableOfType(StatusRuntimeException.class,
        () -> drain(aggregated("host", TimeSeriesAggregationType.TS_AGG_SUM)));

    assertThat(ex).isNotNull();
    assertThat(ex.getStatus().getCode())
        .as("a request the caller stated wrong is INVALID_ARGUMENT, not an internal failure")
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(ex.getStatus().getDescription())
        .contains("host")
        .contains("is not stored as a number");
  }

  /**
   * COUNT never reads the column, so it is exempt on this surface too - and this is what keeps the refusal
   * above from being a blanket ban on naming a tag in an aggregation request.
   */
  @Test
  void countingOverATagIsStillAccepted() {
    assertThat(drain(aggregated("host", TimeSeriesAggregationType.TS_AGG_COUNT)))
        .as("COUNT counts rows and never decodes the column").isPositive();
  }

  /**
   * A BOOLEAN field is readable as a number on both layers since issue #5475, so it must be accepted rather
   * than caught by the refusal - the codec predicate has to separate it from the tag above, which a check on
   * the declared type could not do.
   */
  @Test
  void aggregatingABooleanFieldIsAccepted() {
    assertThat(drain(aggregated("active", TimeSeriesAggregationType.TS_AGG_SUM)))
        .as("BOOLEAN is a SIMPLE8B column, so both layers read it as 1 and 0").isPositive();
  }

  /**
   * Issue #7717 on the aggregated stream: the read counters reach the sink, tagged with this RPC's own surface.
   */
  @Test
  void theAggregatedStreamPublishesItsReadCounters() {
    drain(aggregated("value", TimeSeriesAggregationType.TS_AGG_AVG));

    assertThat(blocksCounted("grpc")).as("the aggregated RPC reports the blocks it decided about").isPositive();
  }

  /**
   * Issue #7717 on {@code TimeSeriesLatest}, the RPC that had no publish at all: its HTTP twin was wired in the
   * same batch and {@code SURFACE_TS_LATEST} exists for exactly this, so without it the counters answered for
   * {@code GET /ts/{db}/latest} and stayed silent for the RPC asking the identical question.
   */
  @Test
  void theLatestRpcPublishesItsReadCountersToo() {
    final TimeSeriesLatestResponse latest = authenticatedStub.timeSeriesLatest(TimeSeriesLatestRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME)
        .build());
    assertThat(latest.getFound()).isTrue();

    assertThat(blocksCounted("ts-latest"))
        .as("the latest RPC must report to the same sink as its HTTP twin, under its own surface tag")
        .isPositive();
  }

  // ---- helpers ----

  /** Total blocks this database's type reported under one surface tag, across every outcome. */
  private double blocksCounted(final String surface) {
    double total = 0;
    for (final Counter counter : Search.in(Metrics.globalRegistry).name("arcadedb.timeseries.read.blocks")
        .tag("db", getDatabaseName()).tag("type", TYPE_NAME).tag("surface", surface).counters())
      total += counter.count();
    return total;
  }

  /** Consumes an aggregated stream and answers how many buckets it carried. */
  private int drain(final TimeSeriesQueryRequest request) {
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(request);
    int buckets = 0;
    while (stream.hasNext())
      buckets += stream.next().getBucketsCount();
    return buckets;
  }

  private TimeSeriesQueryRequest aggregated(final String field, final TimeSeriesAggregationType type) {
    return TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME)
        .setAggregation(TimeSeriesAggregation.newBuilder()
            // One bucket for the whole range, so no ceiling is anywhere near being reached: this class is about
            // the refusal and the metrics, not about issue #7724's bound.
            .setBucketIntervalMs(SAMPLES * 10_000L)
            .addRequests(TimeSeriesAggregationRequest.newBuilder().setField(field).setType(type)))
        .build();
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
