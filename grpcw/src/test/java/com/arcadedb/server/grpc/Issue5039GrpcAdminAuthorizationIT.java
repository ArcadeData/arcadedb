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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5039.
 * <p>
 * The gRPC admin service authenticates the caller's credentials but never checks that the caller
 * holds an administrative (root) role. Any user who owns a valid server account could therefore
 * create or drop any database on the server - a privilege escalation that is also destructive.
 * <p>
 * This test creates a non-admin user (authorized only for a dedicated database) and asserts that
 * mutating admin RPCs ({@code CreateDatabase}, {@code DropDatabase}) are rejected with
 * {@code PERMISSION_DENIED}. A positive control confirms the {@code root} user still succeeds.
 */
public class Issue5039GrpcAdminAuthorizationIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT    = 50051;
  private static final String ALLOWED_DB   = "allowed5039db";
  private static final String LIMITED_USER = "limited5039";
  private static final String LIMITED_PASS = "limited5039pass";

  private ManagedChannel channel;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupUserAndChannel() {
    final ServerSecurity security = getServer(0).getSecurity();

    // The database the limited user IS authorized for (still no server-admin role).
    getServer(0).getOrCreateDatabase(ALLOWED_DB);

    if (!security.existsUser(LIMITED_USER)) {
      final JSONObject config = new JSONObject();
      config.put("name", LIMITED_USER);
      config.put("password", security.encodePassword(LIMITED_PASS));
      config.put("databases", new JSONObject().put(ALLOWED_DB, new JSONArray().put("admin")));
      security.createUser(config);
    }

    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void teardown() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private DatabaseCredentials limitedCredentials() {
    return DatabaseCredentials.newBuilder().setUsername(LIMITED_USER).setPassword(LIMITED_PASS).build();
  }

  private DatabaseCredentials rootCredentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  @Test
  void nonAdminCreateDatabaseIsDenied() {
    final String newDb = "grpc_5039_should_not_exist_" + System.currentTimeMillis();

    final CreateDatabaseRequest request = CreateDatabaseRequest.newBuilder()
        .setCredentials(limitedCredentials())
        .setName(newDb)
        .setType("graph")
        .build();

    assertThatThrownBy(() -> adminStub.createDatabase(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("PERMISSION_DENIED");

    // The database must NOT have been created by the unauthorized caller.
    assertThat(getServer(0).existsDatabase(newDb)).isFalse();
  }

  @Test
  void nonAdminDropDatabaseIsDenied() {
    // Target an existing database the limited user must never be able to destroy.
    final String targetDb = getDatabaseName();
    assertThat(getServer(0).existsDatabase(targetDb)).isTrue();

    final DropDatabaseRequest request = DropDatabaseRequest.newBuilder()
        .setCredentials(limitedCredentials())
        .setName(targetDb)
        .build();

    assertThatThrownBy(() -> adminStub.dropDatabase(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("PERMISSION_DENIED");

    // The database must still exist after the denied drop.
    assertThat(getServer(0).existsDatabase(targetDb)).isTrue();
  }

  @Test
  void adminCreateAndDropDatabaseStillSucceeds() {
    final String newDb = "grpc_5039_admin_db_" + System.currentTimeMillis();

    final CreateDatabaseRequest createRequest = CreateDatabaseRequest.newBuilder()
        .setCredentials(rootCredentials())
        .setName(newDb)
        .setType("document")
        .build();

    adminStub.createDatabase(createRequest);
    assertThat(getServer(0).existsDatabase(newDb)).isTrue();

    final DropDatabaseRequest dropRequest = DropDatabaseRequest.newBuilder()
        .setCredentials(rootCredentials())
        .setName(newDb)
        .build();

    adminStub.dropDatabase(dropRequest);
    assertThat(getServer(0).existsDatabase(newDb)).isFalse();
  }
}
