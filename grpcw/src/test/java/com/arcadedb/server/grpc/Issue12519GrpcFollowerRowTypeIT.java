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
 * Issue #12519: on a follower, the gRPC service recovers the type of a row the follower rebuilt from the JSON its leader answered a
 * forwarded command with (issue #4688), and of nothing else. A command forwarded to the leader answers its rows as records, and a
 * local projection with a {@code @type} column, which the follower runs itself, stays a projection.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue12519GrpcFollowerRowTypeIT extends BaseRaftHATest {

  private static final String TYPE_NAME = "P12519";

  // One gRPC port per server, drawn free for every test instance rather than fixed (issue #7496), from the same
  // ledger as the Raft ports so the two can never coincide (issue #8203).
  private final int[] grpcPorts = allocateFixturePorts(2);

  private ManagedChannel channel;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));
    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue("arcadedb.grpc.port", String.valueOf(grpcPorts[index]));
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
  void aForwardedRecordIsTypedAndALocalProjectionWithATypeColumnIsNot() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE VERTEX TYPE " + TYPE_NAME);
    leaderDb.transaction(() -> leaderDb.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = 'a'"));

    waitForAllServers();

    // Connect to a FOLLOWER: the INSERT is forwarded to the leader, the SELECT runs on the follower
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    channel = ManagedChannelBuilder
        .forAddress("localhost", grpcPorts[followerIndex])
        .usePlaintext()
        .maxInboundMessageSize(64 * 1024 * 1024)
        .build();
    final Channel authenticated = ClientInterceptors.intercept(channel,
        new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName()));
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);

    final DatabaseCredentials credentials = DatabaseCredentials.newBuilder()
        .setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();

    // THE FORWARDED ROW IS A RECORD ON THE LEADER: ITS TYPE IS RECOVERED FROM THE JSON THE FOLLOWER REBUILT IT FROM
    final ExecuteCommandResponse inserted = stub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCommand("INSERT INTO " + TYPE_NAME + " SET name = 'b'")
        .setLanguage("sql")
        .setReturnRows(true)
        .setCredentials(credentials)
        .build());
    assertThat(inserted.getSuccess()).as("the leader must commit the forwarded insert").isTrue();
    assertThat(inserted.getRecordsList()).hasSize(1);
    assertThat(inserted.getRecords(0).getType()).isEqualTo(TYPE_NAME);

    waitForAllServers();

    // A LOCAL PROJECTION WITH A @type COLUMN IS NOT A RECORD, ON A FOLLOWER AS ANYWHERE
    final ExecuteQueryResponse projected = stub.executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setQuery("SELECT @type, name FROM " + TYPE_NAME + " ORDER BY name")
        .setCredentials(credentials)
        .build());
    assertThat(projected.getResultsCount()).isEqualTo(1);
    assertThat(projected.getResults(0).getRecordsList()).hasSize(2);
    for (final GrpcRecord record : projected.getResults(0).getRecordsList()) {
      assertThat(record.getType()).as("a projection row has no type of its own").isEmpty();
      assertThat(record.getRid()).isEmpty();
      assertThat(record.getPropertiesMap()).containsOnlyKeys("@type", "name");
      assertThat(record.getPropertiesMap().get("@type").getStringValue()).isEqualTo(TYPE_NAME);
    }
  }
}
