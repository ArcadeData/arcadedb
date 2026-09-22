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
import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8140 on the gRPC aggregation stream, the fourth surface that turns a caller-supplied field name into a
 * {@code MultiColumnAggregationRequest}.
 * <p>
 * {@code ArcadeDbGrpcService} resolved the name to a SCHEMA index and handed that number to the engine, which
 * reads it as a position in the ENGINE ROW. The type below declares the TIMESTAMP column LAST, so {@code value}
 * has schema index 0 and row index 1 and the schema reading aimed the engine's mutable half at the timestamp:
 * five samples of 1..5 summed to about 8.5e12 while they were mutable and to 15 once they were sealed.
 * <p>
 * Both readings are asserted - before and after {@code compactAll()} - because either half alone passes against
 * a fix that corrects only the other.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8140">issue #8140</a>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class Issue8140NonFirstTimestampAggregationIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT = 50051;
  private static final String TYPE_NAME = "grpcagg8140";
  private static final int    SAMPLES   = 5;
  private static final long   BASE_TS   = 1_700_000_000_000L;
  private static final long   HOUR      = 3_600_000L;

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

    // TIMESTAMP declared LAST: schema order is [value, host, ts], engine row order is [ts, value, host].
    authenticatedStub.executeCommand(command("CREATE TIMESERIES TYPE " + TYPE_NAME
        + " FIELDS (value DOUBLE) TAGS (host STRING) TIMESTAMP ts"));

    final TimeSeriesWriteRequest.Builder write = TimeSeriesWriteRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME);
    for (int i = 0; i < SAMPLES; i++)
      write.addPoints(TimeSeriesPoint.newBuilder()
          .setTimestamp(BASE_TS + i * 1_000L)
          .putTags("host", GrpcValue.newBuilder().setStringValue("web1").build())
          .putFields("value", GrpcValue.newBuilder().setDoubleValue(i + 1).build()));
    assertThat(authenticatedStub.timeSeriesWrite(write.build()).getWritten()).isEqualTo(SAMPLES);
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

  @Test
  void theAggregationRpcReadsTheNamedFieldBeforeAndAfterCompaction() throws IOException {
    assertThat(aggregate("value", TimeSeriesAggregationType.TS_AGG_SUM))
        .as("mutable: SUM must sum the FIELD, not the timestamps beside it").containsExactly(15.0);
    assertThat(aggregate("value", TimeSeriesAggregationType.TS_AGG_MAX))
        .as("mutable: MAX must read the FIELD").containsExactly(5.0);

    compact();

    assertThat(aggregate("value", TimeSeriesAggregationType.TS_AGG_SUM))
        .as("sealed: the same request must name the same column").containsExactly(15.0);
    assertThat(aggregate("value", TimeSeriesAggregationType.TS_AGG_MAX)).containsExactly(5.0);
  }

  /** COUNT reads no column on either half, so the request it builds names none: unaffected either way. */
  @Test
  void countIsUnaffectedByTheColumnConvention() throws IOException {
    assertThat(aggregate("value", TimeSeriesAggregationType.TS_AGG_COUNT)).containsExactly((double) SAMPLES);
    compact();
    assertThat(aggregate("value", TimeSeriesAggregationType.TS_AGG_COUNT)).containsExactly((double) SAMPLES);
  }

  // ---- helpers ----

  /** Seals the samples through the server's own database: the wire protocol has no compaction call. */
  private void compact() throws IOException {
    ((LocalTimeSeriesType) getServerDatabase(0, getDatabaseName()).getSchema().getType(TYPE_NAME))
        .getEngine().compactAll();
  }

  /** The one value per bucket the aggregated stream carries, in bucket order. */
  private List<Double> aggregate(final String field, final TimeSeriesAggregationType type) {
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME)
        // One bucket for the whole sample range, so the assertion is about the value and not about bucketing.
        .setAggregation(TimeSeriesAggregation.newBuilder()
            .setBucketIntervalMs(HOUR)
            .addRequests(TimeSeriesAggregationRequest.newBuilder().setField(field).setType(type)))
        .build());

    final List<Double> values = new ArrayList<>();
    while (stream.hasNext())
      for (final TimeSeriesBucket bucket : stream.next().getBucketsList())
        values.add(bucket.getValues(0).getDoubleValue());
    return values;
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
