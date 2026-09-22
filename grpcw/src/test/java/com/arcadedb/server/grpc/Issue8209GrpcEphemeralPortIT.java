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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8209: the gRPC tests used to start the plugin on the fixed production port 50051 and
 * connect to that same port, so anything already listening there failed the module or answered the tests' calls.
 * This test occupies 50051 itself before the server starts (when nothing else already does) and proves the plugin
 * still starts, on an OS-assigned port the tests can discover, and serves real calls there, while the production
 * default of the setting stays 50051.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8209GrpcEphemeralPortIT extends BaseGrpcServerTest {

  private static final int PRODUCTION_DEFAULT_PORT = 50051;

  private ServerSocket occupier;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    try {
      occupier = new ServerSocket(PRODUCTION_DEFAULT_PORT);
    } catch (final IOException e) {
      // SOMETHING ELSE ALREADY HOLDS THE PORT: THE CONDITION UNDER TEST IS IN PLACE ANYWAY
      occupier = null;
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      super.endTest();
    } finally {
      if (occupier != null) {
        try {
          occupier.close();
        } catch (final IOException ignore) {
          // BEST EFFORT
        }
        occupier = null;
      }
    }
  }

  @Test
  void pluginListensOnAnEphemeralPortWhenTheProductionDefaultIsTaken() throws InterruptedException {
    assertThat(getServer(0)).isNotNull();
    assertThat(getServer(0).isStarted()).isTrue();

    final int port = getServerGrpcPort();
    assertThat(port).isGreaterThan(0);
    assertThat(port).isNotEqualTo(PRODUCTION_DEFAULT_PORT);

    final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    try {
      final ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel)
          .withDeadlineAfter(30, TimeUnit.SECONDS);
      final PingResponse response = adminStub.ping(PingRequest.newBuilder()
          .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
          .build());
      assertThat(response.getOk()).isTrue();
    } finally {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }

    assertThat(GlobalConfiguration.GRPC_PORT.getDefValue()).isEqualTo(PRODUCTION_DEFAULT_PORT);
  }
}
