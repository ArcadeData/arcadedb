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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: a single-row SQL {@code INSERT} into a TIME SERIES type, executed over gRPC with
 * {@code returnRows=true}, must return a result record carrying the type name. Previously the
 * server emitted an empty type field, so the client (RemoteImmutableDocument) threw
 * "Type with name '' was not found" on every TS insert while materialising the response, even though
 * the server had already committed the point.
 */
class TimeSeriesGrpcInsertResultTypeIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT = 50051;
  private static final String TYPE_NAME = "sensor";

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

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
  }

  @Test
  void tsInsertOverGrpcReturnsTypeName() {
    authenticatedStub.executeCommand(
        ExecuteCommandRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setCommand("CREATE TIMESERIES TYPE " + TYPE_NAME
                + " TIMESTAMP ts TAGS (sensor_id STRING, region STRING) FIELDS (temperature DOUBLE, humidity DOUBLE)")
            .build());

    final ExecuteCommandResponse resp = authenticatedStub.executeCommand(
        ExecuteCommandRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setCommand("INSERT INTO " + TYPE_NAME
                + " SET ts = ?, sensor_id = ?, region = ?, temperature = ?, humidity = ?")
            .setLanguage("sql")
            .setReturnRows(true)
            .putParameters("0", GrpcValue.newBuilder().setInt64Value(1_000L).build())
            .putParameters("1", GrpcValue.newBuilder().setStringValue("sensor-0").build())
            .putParameters("2", GrpcValue.newBuilder().setStringValue("region-0").build())
            .putParameters("3", GrpcValue.newBuilder().setDoubleValue(15.0).build())
            .putParameters("4", GrpcValue.newBuilder().setDoubleValue(40.0).build())
            .build());

    assertThat(resp.getSuccess()).as("server must commit the TS insert").isTrue();
    assertThat(resp.getRecordsList()).as("INSERT with returnRows must return the inserted row").isNotEmpty();

    final GrpcRecord record = resp.getRecords(0);
    final String dump = "type='" + record.getType() + "' rid='" + record.getRid() + "' props=" + record.getPropertiesMap();

    assertThat(record.getType())
        .as("TS insert result row must carry the type name, not empty. Server emitted: %s", dump)
        .isEqualTo(TYPE_NAME);
  }
}
