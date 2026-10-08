/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9449 over gRPC on a server without HA: the override is root-only, and with no HA there is no quarantine to lift,
 * which is a FAILED_PRECONDITION rather than a silent OK.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9449GrpcAcceptDivergedWithoutHaIT extends BaseGrpcServerTest {

  private static final String LIMITED_USER = "limited9449";
  private static final String LIMITED_PASS = "limited9449pass";

  private ManagedChannel                                            channel;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupUserAndChannel() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (!security.existsUser(LIMITED_USER)) {
      final JSONObject config = new JSONObject();
      config.put("name", LIMITED_USER);
      config.put("password", security.encodePassword(LIMITED_PASS));
      config.put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")));
      security.createUser(config);
    }

    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void teardown() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void withoutHaTheOverrideFailsThePrecondition() {
    assertThatThrownBy(() -> accept("root", DEFAULT_PASSWORD_FOR_TESTS))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("FAILED_PRECONDITION")
        .hasMessageContaining("High Availability");
  }

  @Test
  void anAuthenticatedNonRootCallerIsDenied() {
    assertThatThrownBy(() -> accept(LIMITED_USER, LIMITED_PASS))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("PERMISSION_DENIED");
  }

  private AcceptDivergedDatabaseResponse accept(final String user, final String password) {
    return adminStub.acceptDivergedDatabase(AcceptDivergedDatabaseRequest.newBuilder()
        .setCredentials(DatabaseCredentials.newBuilder().setUsername(user).setPassword(password).build())
        .setDatabase(getDatabaseName()).build());
  }
}
