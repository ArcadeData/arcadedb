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
 * Issue #11983, over gRPC: the aggregation branch of TimeSeriesQuery encoded every aggregate from the nearest double, so MIN/MAX/SUM
 * of a LONG column past 2^53 arrived as a different number than SQL answers. An exact request now arrives as an int64.
 */
public class Issue11983ExactLongAggregatesOverGrpcIT extends BaseGrpcServerTest {

  private static final String TYPE_NAME = "exactlong11983";
  private static final long   BIG       = 9_007_199_254_740_993L; // 2^53 + 1

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel,
        new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName()));
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);

    authenticatedStub.executeCommand(command("CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts FIELDS (n LONG) SHARDS 1"));
    authenticatedStub.executeCommand(command("INSERT INTO " + TYPE_NAME + " SET ts = 1000, n = " + BIG));
    authenticatedStub.executeCommand(command("INSERT INTO " + TYPE_NAME + " SET ts = 2000, n = 1"));
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
  void minMaxAndSumOfALongColumnArriveAsExactInt64() {
    final TimeSeriesAggregation.Builder aggregation = TimeSeriesAggregation.newBuilder().setBucketIntervalMs(1_000_000L);
    for (final TimeSeriesAggregationType type : new TimeSeriesAggregationType[] {
        TimeSeriesAggregationType.TS_AGG_MIN, TimeSeriesAggregationType.TS_AGG_MAX, TimeSeriesAggregationType.TS_AGG_SUM })
      aggregation.addRequests(TimeSeriesAggregationRequest.newBuilder().setField("n").setType(type));

    final List<TimeSeriesBucket> buckets = new ArrayList<>();
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME)
        .setFromTimestamp(0L)
        .setToTimestamp(10_000L)
        .setAggregation(aggregation)
        .build());
    while (stream.hasNext())
      buckets.addAll(stream.next().getBucketsList());

    assertThat(buckets).hasSize(1);
    final List<GrpcValue> values = buckets.getFirst().getValuesList();
    assertThat(values).extracting(GrpcValue::getKindCase).containsOnly(GrpcValue.KindCase.INT64_VALUE);
    assertThat(values).extracting(GrpcValue::getInt64Value).containsExactly(1L, BIG, BIG + 1);
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
