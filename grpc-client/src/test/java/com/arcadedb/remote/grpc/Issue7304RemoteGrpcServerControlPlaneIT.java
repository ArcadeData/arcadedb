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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.grpc.UserInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7304: the control plane reached {@code ArcadeDbAdminService} but nothing in
 * {@link RemoteGrpcServer}, and an RPC no client can call is one nobody can use. This drives each
 * new client method against a live server, so the proto, the service and the client are proved to
 * agree rather than only to compile.
 */
class Issue7304RemoteGrpcServerControlPlaneIT extends BaseGraphServerTest {

  private RemoteGrpcServer server;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void connect() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterEach
  void disconnect() {
    if (server != null) {
      server.close();
      server = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
  }

  @Test
  void closeAndOpenDatabaseRoundTrip() {
    final String database = "client7304_lifecycle";
    server.createDatabaseIfMissing(database);

    server.closeDatabase(database);
    assertThat(getServer(0).getDatabaseNames()).doesNotContain(database);

    server.openDatabase(database);
    assertThat(getServer(0).getDatabaseNames()).contains(database);
  }

  @Test
  void settingsAreStoredCoerced() {
    final GlobalConfiguration setting = GlobalConfiguration.TX_RETRIES;
    final Object previous = getServer(0).getConfiguration().getValue(setting);
    try {
      server.setServerSetting(setting.getKey(), "9");
      assertThat(getServer(0).getConfiguration().getValueAsInteger(setting)).isEqualTo(9);

      server.setDatabaseSetting(getDatabaseName(), setting.getKey(), "5");
      assertThat(getServer(0).getDatabase(getDatabaseName()).getConfiguration().getValueAsInteger(setting)).isEqualTo(5);
    } finally {
      getServer(0).getConfiguration().setValue(setting.getKey(), previous);
    }
  }

  @Test
  void userManagementRoundTrip() {
    final String user = "client7304user";
    try {
      server.createUser(user, "client7304password", Map.of(getDatabaseName(), List.of("admin")));
      assertThat(getServer(0).getSecurity().existsUser(user)).isTrue();

      final List<UserInfo> users = server.listUsers();
      assertThat(users).extracting(UserInfo::getName).contains(user);

      final UserInfo created = users.stream().filter(u -> user.equals(u.getName())).findFirst().orElseThrow();
      assertThat(created.getDatabasesMap().get(getDatabaseName()).getGroupsList()).contains("admin");

      server.dropUser(user);
      assertThat(getServer(0).getSecurity().existsUser(user)).isFalse();
    } finally {
      if (getServer(0).getSecurity().existsUser(user))
        getServer(0).getSecurity().dropUser(user);
    }
  }

  @Test
  void backupConfigRoundTripAndListing() {
    server.setBackupConfig(new JSONObject().put("backupDirectory", "backups7304client"));

    assertThat(new JSONObject(server.getBackupConfig().getConfigJson()).getString("backupDirectory"))
        .isEqualTo("backups7304client");

    // Auto-backup is not running in this fixture, so the listing is empty rather than absent.
    assertThat(server.listBackups(getDatabaseName())).isEmpty();
  }

  @Test
  void profilerRoundTrip() {
    server.profilerReset();
    server.profilerStart(0);

    assertThat(server.profilerResults()).isNotNull();
    assertThat(server.profilerStop()).isNotNull();

    // Stopping saves the run, so the listing reports it with its file metadata.
    assertThat(server.profilerList()).isNotEmpty();
    assertThat(server.profilerList().getFirst().getFileName()).startsWith("profiler-run-");
    assertThat(server.profilerLoad(server.profilerList().getFirst().getFileName())).isNotNull();

    server.profilerReset();
  }

  @Test
  void serverEventsAreReadable() {
    assertThat(server.getServerEvents("").getEventsJson()).startsWith("[");
  }

  /**
   * The probe methods build a request with no credentials field at all - the field the admin
   * interceptor authenticates from - so they exercise the server-side exemption from the client.
   * That the exemption holds for a caller with no credentials whatsoever is asserted on the server
   * side, by {@code Issue7304GrpcControlPlaneAuthorizationIT}, with a raw stub.
   */
  @Test
  void probesAnswerWithNoCredentialsInTheRequest() {
    assertThat(server.health()).isTrue();
    assertThat(server.ready().getReady()).isTrue();
    assertThat(server.ready().getReason()).isEmpty();
  }

  /**
   * Outside HA the operation cannot run, and the client surfaces that rather than swallowing it.
   * <p>
   * The type matters as much as the message: admin failures go through
   * {@code GrpcClientErrorMapper}, the same mapper the data plane uses, rather than being wrapped in
   * a bare {@code RuntimeException} carrying only a rendered string. That is what lets a follower's
   * leader refusal arrive as a {@code ServerIsNotTheLeaderException} holding the leader's address
   * from the trailers instead of losing it.
   */
  @Test
  void disconnectClusterWithoutHaIsReportedThroughTheSharedErrorMapper() {
    assertThatThrownBy(() -> server.disconnectCluster())
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("High Availability")
        // The server's own description, not a "Failed to disconnect cluster" wrapper around it.
        .hasMessageContaining("-Darcadedb.ha.enabled=true");
  }
}
