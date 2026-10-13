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

import java.util.Iterator;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gRPC side of issue #9879: the {@code TimeSeriesQuery} and {@code TimeSeriesLatest} RPCs read a DATETIME tag value given as
 * text in the JVM's default time zone, while the {@code INSERT} that wrote the tag read it in the database's, so on a JVM whose zone
 * is not the database's the tag filter selected nothing.
 * <p>
 * The database captures its zone when the server opens it; the JVM default is moved afterwards, to a zone with an offset on the
 * stored date, so the two differ wherever the test runs.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class Issue9879GrpcTimeSeriesTagFilterDatabaseTimeZoneIT extends BaseGrpcServerTest {
  private static final String TYPE_NAME = "grpc9879";
  private static final String STORED    = "2024-01-01 00:00:00";

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;
  private TimeZone                                        jvmZone;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClientAndMoveTheJvmZone() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel,
        new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName()));
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);

    jvmZone = TimeZone.getDefault();
    final TimeZone databaseZone = getServerDatabase(0, getDatabaseName()).getSchema().getTimeZone();
    TimeZone.setDefault(TimeZone.getTimeZone(databaseZone.getRawOffset() == 9 * 3_600_000 ? "America/New_York" : "Asia/Tokyo"));

    authenticatedStub.executeCommand(command(
        "CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts TAGS (code DATETIME) FIELDS (value DOUBLE)"));
    authenticatedStub.executeCommand(command("INSERT INTO " + TYPE_NAME + " SET ts = 1000, code = '" + STORED + "', value = 4.0"));
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    try {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    } finally {
      TimeZone.setDefault(jvmZone);
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
    }
  }

  @Test
  void timeSeriesQuerySelectsTheRowTheInsertWrote() {
    assertThat(rows(STORED)).isEqualTo(1);
    assertThat(rows("2025-06-01 00:00:00")).isZero();
  }

  @Test
  void timeSeriesLatestSelectsTheRowTheInsertWrote() {
    final TimeSeriesLatestResponse latest = authenticatedStub.timeSeriesLatest(TimeSeriesLatestRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME)
        .setTags(tags(STORED))
        .build());
    assertThat(latest.getFound()).isTrue();
  }

  private int rows(final String code) {
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME)
        .setTags(tags(code))
        .build());
    int rows = 0;
    while (stream.hasNext())
      rows += stream.next().getRowsCount();
    return rows;
  }

  private static TimeSeriesTagFilter tags(final String code) {
    return TimeSeriesTagFilter.newBuilder().putEquals("code", GrpcValue.newBuilder().setStringValue(code).build()).build();
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
