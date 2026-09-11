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
import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #7390: the server-streaming {@code TimeSeriesQuery} read its row ceiling from
 * {@code arcadedb.server.grpcQueryMaxResultRows} - the setting written for the UNARY {@code ExecuteQuery},
 * whose own description explains that its default is deliberately the low one <i>because</i> the unary response
 * is a single gRPC message. That is exactly the property a streaming RPC does not have.
 * <p>
 * Two consequences, both held here. The cap fired MID-STREAM, after the client had already consumed part of the
 * series, so a client that treats "no more messages" as "end of data" silently got truncated data. And the same
 * range that streams over the HTTP twin - {@code POST /api/v1/ts/{database}/query}, bounded by
 * {@code httpQueryMaxResultRows}, ten times higher by default - was refused over gRPC, while the proto comment
 * told the client that {@code httpQueryMaxResultRows} was the setting that applied to it. It was not read at all.
 * <p>
 * The RPC now reads its own ceiling, {@code arcadedb.server.grpcTimeSeriesMaxResultRows}, whose default matches
 * the HTTP twin, and refuses an over-limit request BEFORE the first message rather than part-way through it.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7390">issue #7390</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7390GrpcTimeSeriesRowCeilingIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT     = 50051;
  private static final String TYPE_NAME     = "reading";
  private static final int    MAX_TS_ROWS   = 10;
  /** Deliberately far BELOW the time-series ceiling: reading it here is the defect. */
  private static final int    MAX_UNARY_ROWS = 3;
  private static final int    SAMPLES        = 40;
  private static final long   BASE_TS        = 1_700_000_000_000L;

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    GlobalConfiguration.SERVER_GRPC_TIMESERIES_MAX_RESULT_ROWS.setValue(MAX_TS_ROWS);
    GlobalConfiguration.SERVER_GRPC_QUERY_MAX_RESULT_ROWS.setValue(MAX_UNARY_ROWS);
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel,
        new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName()));
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);

    authenticatedStub.executeCommand(command("CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)"));

    final TimeSeriesWriteRequest.Builder write = TimeSeriesWriteRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME);
    for (int i = 0; i < SAMPLES; i++)
      write.addPoints(TimeSeriesPoint.newBuilder()
          .setTimestamp(BASE_TS + i * 1_000L)
          .putTags("host", GrpcValue.newBuilder().setStringValue("web1").build())
          .putFields("value", GrpcValue.newBuilder().setDoubleValue(i).build()));
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
      GlobalConfiguration.SERVER_GRPC_TIMESERIES_MAX_RESULT_ROWS.reset();
      GlobalConfiguration.SERVER_GRPC_QUERY_MAX_RESULT_ROWS.reset();
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
    }
  }

  /**
   * The regression: a range larger than the UNARY ceiling but within the streaming one must stream in full.
   * Before the fix this failed with RESOURCE_EXHAUSTED at row {@code MAX_UNARY_ROWS}.
   */
  @Test
  void theStreamIsNotBoundedByTheUnaryCeiling() {
    final int rows = countRows(query().setLimit(MAX_TS_ROWS).build());

    assertThat(rows)
        .as("the streaming RPC must honour its own ceiling (%d), not the unary one (%d)", MAX_TS_ROWS,
            MAX_UNARY_ROWS)
        .isEqualTo(MAX_TS_ROWS);
  }

  /**
   * A limit above the streaming ceiling is refused before the first message. The assertion that matters is not
   * merely that it fails - it did before - but that NOTHING was delivered: the old failure arrived after the
   * client had already consumed a partial series it could not tell apart from a complete one.
   */
  @Test
  void anOverLimitRequestIsRefusedBeforeTheFirstMessage() {
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(
        query().setLimit(MAX_TS_ROWS + 1).build());

    final StatusRuntimeException ex = catchThrowableOfType(StatusRuntimeException.class, stream::hasNext);

    assertThat(ex).isNotNull();
    assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    assertThat(ex.getStatus().getDescription())
        .as("the refusal must name the setting that actually applies, or raising it does nothing")
        .contains("grpcTimeSeriesMaxResultRows");
  }

  /**
   * A request with NO limit is the one case the ceiling still has to fire mid-stream - a lazy walk cannot know
   * the row count in advance - and it must fail loudly rather than end the stream as if the data ran out.
   */
  @Test
  void anUnlimitedRequestExceedingTheCeilingFailsLoudlyRatherThanEndingQuietly() {
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(query().build());

    final StatusRuntimeException ex = catchThrowableOfType(StatusRuntimeException.class, () -> {
      while (stream.hasNext())
        stream.next();
    });

    assertThat(ex).as("silently stopping at the cap is the truncation this ceiling must never produce").isNotNull();
    assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  /** A limit at or below the ceiling is the client's own bound and is honoured silently, as before. */
  @Test
  void aLimitWithinTheCeilingIsHonouredAndReportsTruncation() {
    final int limit = MAX_TS_ROWS - 5;
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(
        query().setLimit(limit).build());

    int rows = 0;
    boolean truncated = false;
    while (stream.hasNext()) {
      final TimeSeriesQueryResult message = stream.next();
      rows += message.getRowsCount();
      if (message.getLast())
        truncated = message.getTruncated();
    }

    assertThat(rows).isEqualTo(limit);
    assertThat(truncated).as("the answer really was cut short, and the client has to be able to see that").isTrue();
  }

  /** The aggregated path shares the ceiling: its buckets are materialized before any is emitted. */
  @Test
  void theAggregatedPathUsesTheSameCeiling() {
    final StatusRuntimeException ex = catchThrowableOfType(StatusRuntimeException.class, () -> {
      final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(query()
          .setAggregation(TimeSeriesAggregation.newBuilder()
              // One bucket per sample, so the bucket count exceeds the ceiling.
              .setBucketIntervalMs(1_000L)
              .addRequests(TimeSeriesAggregationRequest.newBuilder()
                  .setField("value")
                  .setType(TimeSeriesAggregationType.TS_AGG_AVG)))
          .build());
      while (stream.hasNext())
        stream.next();
    });

    assertThat(ex).isNotNull();
    assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    assertThat(ex.getStatus().getDescription()).contains("grpcTimeSeriesMaxResultRows");
  }

  private int countRows(final TimeSeriesQueryRequest request) {
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(request);
    int rows = 0;
    while (stream.hasNext())
      rows += stream.next().getRowsCount();
    return rows;
  }

  private TimeSeriesQueryRequest.Builder query() {
    return TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME)
        // Small batches so a mid-stream failure really does arrive after several delivered messages.
        .setBatchSize(2);
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
