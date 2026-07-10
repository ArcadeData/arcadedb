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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.server.ha.raft.BaseRaftHATest;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: a single-row time-series {@code INSERT} sent over gRPC to a FOLLOWER (so the
 * command is forwarded to the Raft leader) must return a result row that carries the type name.
 * <p>
 * The leader returns each forwarded row as a plain projection (the follower rebuilds it from the
 * leader's JSON as a non-element {@code ResultInternal} with {@code @type} as a property). The gRPC
 * row converter only set the record type field for element results, so the forwarded TS-insert row
 * was emitted with an empty type. The client {@code RemoteImmutableDocument} then threw
 * "Type with name '' was not found" on every TS insert while materialising the response, even though
 * the leader had already committed the point.
 */
@Tag("slow")
class TimeSeriesGrpcForwardedInsertTypeIT extends BaseRaftHATest {

  private static final int    BASE_GRPC_PORT = 51091;
  private static final String TYPE_NAME      = "sensor";

  private ManagedChannel channel;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));
    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue("arcadedb.grpc.port", String.valueOf(BASE_GRPC_PORT + index));
    config.setValue("arcadedb.grpc.host", "localhost");
    config.setValue("arcadedb.grpc.reflection.enabled", "false");
    config.setValue("arcadedb.grpc.health.enabled", "false");

    final String existingPlugins = config.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    final String pluginEntry = "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin";
    if (existingPlugins == null || existingPlugins.isEmpty())
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, pluginEntry);
    else if (!existingPlugins.contains(pluginEntry))
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, existingPlugins + "," + pluginEntry);
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      channel = null;
    }
  }

  @Test
  void forwardedTsInsertReturnsTypeName() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE_NAME
            + " TIMESTAMP ts TAGS (sensor_id STRING, region STRING) FIELDS (temperature DOUBLE, humidity DOUBLE)");

    waitForAllServers();

    // Connect to a FOLLOWER so the INSERT is forwarded to the leader (the path that lost the type).
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    channel = ManagedChannelBuilder
        .forAddress("localhost", BASE_GRPC_PORT + followerIndex)
        .usePlaintext()
        .maxInboundMessageSize(64 * 1024 * 1024)
        .build();
    final Channel authenticated = ClientInterceptors.intercept(channel,
        new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName()));
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);

    final DatabaseCredentials credentials = DatabaseCredentials.newBuilder()
        .setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();

    final ExecuteCommandResponse resp = stub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCommand("INSERT INTO " + TYPE_NAME + " SET ts = ?, sensor_id = ?, region = ?, temperature = ?, humidity = ?")
        .setLanguage("sql")
        .setReturnRows(true)
        .setCredentials(credentials)
        .putParameters("0", GrpcValue.newBuilder().setInt64Value(1_000L).build())
        .putParameters("1", GrpcValue.newBuilder().setStringValue("sensor-0").build())
        .putParameters("2", GrpcValue.newBuilder().setStringValue("region-0").build())
        .putParameters("3", GrpcValue.newBuilder().setDoubleValue(15.0).build())
        .putParameters("4", GrpcValue.newBuilder().setDoubleValue(40.0).build())
        .build());

    assertThat(resp.getSuccess()).as("leader must commit the forwarded TS insert").isTrue();
    assertThat(resp.getRecordsList()).as("forwarded INSERT with returnRows must return the inserted row").isNotEmpty();

    final GrpcRecord record = resp.getRecords(0);
    final String dump = "type='" + record.getType() + "' rid='" + record.getRid() + "' props=" + record.getPropertiesMap();

    assertThat(record.getType())
        .as("forwarded TS insert result row must carry the type name, not empty. Server emitted: %s", dump)
        .isEqualTo(TYPE_NAME);
  }
}
