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
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7304: the control plane is the surface where a missing authorization check costs the most,
 * so every RPC the issue added is asserted here to enforce the same root-user gate that
 * {@code PostServerCommandHandler} enforces through {@code checkRootUser}.
 * <p>
 * The RPCs are exercised as a table rather than as one test method each: the point of the assertion
 * is that <em>no</em> RPC is missing its gate, and a table makes adding an RPC without adding its row
 * the visible omission. A new control-plane RPC belongs in {@link #mutatingCalls()}.
 * <p>
 * Two callers are used: an authenticated non-root user (must be denied with
 * {@code PERMISSION_DENIED}) and a caller with an invalid password (must be denied with
 * {@code UNAUTHENTICATED}, by the central interceptor, before the handler runs at all).
 */
public class Issue7304GrpcControlPlaneAuthorizationIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT    = 50051;
  private static final String ALLOWED_DB   = "allowed7304db";
  private static final String LIMITED_USER = "limited7304";
  private static final String LIMITED_PASS = "limited7304pass";

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

  private static DatabaseCredentials limited() {
    return DatabaseCredentials.newBuilder().setUsername(LIMITED_USER).setPassword(LIMITED_PASS).build();
  }

  private static DatabaseCredentials wrongPassword() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword("not-the-root-password").build();
  }

  /**
   * Every control-plane RPC added by #7304 that changes server or database state, plus the reads that
   * are root-only on HTTP. Keyed by RPC name so a failure names the RPC that is missing its gate.
   */
  private Map<String, Consumer<DatabaseCredentials>> mutatingCalls() {
    final Map<String, Consumer<DatabaseCredentials>> calls = new LinkedHashMap<>();

    calls.put("OpenDatabase",
        c -> adminStub.openDatabase(OpenDatabaseRequest.newBuilder().setCredentials(c).setName(ALLOWED_DB).build()));
    calls.put("CloseDatabase",
        c -> adminStub.closeDatabase(CloseDatabaseRequest.newBuilder().setCredentials(c).setName(ALLOWED_DB).build()));
    calls.put("AlignDatabase",
        c -> adminStub.alignDatabase(AlignDatabaseRequest.newBuilder().setCredentials(c).setName(ALLOWED_DB).build()));

    calls.put("SetServerSetting", c -> adminStub.setServerSetting(SetServerSettingRequest.newBuilder().setCredentials(c)
        .setKey(GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.getKey()).setValue("false").build()));
    calls.put("SetDatabaseSetting", c -> adminStub.setDatabaseSetting(SetDatabaseSettingRequest.newBuilder().setCredentials(c)
        .setDatabase(ALLOWED_DB).setKey(GlobalConfiguration.DATE_TIME_IMPLEMENTATION.getKey())
        .setValue("java.time.LocalDateTime").build()));

    calls.put("GetBackupConfig",
        c -> adminStub.getBackupConfig(GetBackupConfigRequest.newBuilder().setCredentials(c).build()));
    calls.put("SetBackupConfig", c -> adminStub.setBackupConfig(SetBackupConfigRequest.newBuilder().setCredentials(c)
        .setConfigJson(new JSONObject().put("backupDirectory", "backups7304auth").toString()).build()));
    calls.put("ListBackups",
        c -> adminStub.listBackups(ListBackupsRequest.newBuilder().setCredentials(c).setDatabase(ALLOWED_DB).build()));
    calls.put("TriggerBackup",
        c -> adminStub.triggerBackup(TriggerBackupRequest.newBuilder().setCredentials(c).setDatabase(ALLOWED_DB).build()));
    calls.put("DeleteBackup", c -> adminStub.deleteBackup(DeleteBackupRequest.newBuilder().setCredentials(c)
        .setDatabase(ALLOWED_DB).setFileName("anything-backup-20260101.zip").build()));

    calls.put("CreateUser", c -> adminStub.createUser(CreateUserRequest.newBuilder().setCredentials(c)
        .setUser("escalated7304").setPassword("escalated7304pass").build()));
    calls.put("DeleteUser",
        c -> adminStub.deleteUser(DeleteUserRequest.newBuilder().setCredentials(c).setUser("root").build()));
    calls.put("ListUsers", c -> adminStub.listUsers(ListUsersRequest.newBuilder().setCredentials(c).build()));

    calls.put("ProfilerStart",
        c -> adminStub.profilerStart(ProfilerStartRequest.newBuilder().setCredentials(c).setTimeoutSeconds(1).build()));
    calls.put("ProfilerStop", c -> adminStub.profilerStop(ProfilerStopRequest.newBuilder().setCredentials(c).build()));
    calls.put("ProfilerReset", c -> adminStub.profilerReset(ProfilerResetRequest.newBuilder().setCredentials(c).build()));
    calls.put("ProfilerResults",
        c -> adminStub.profilerResults(ProfilerResultsRequest.newBuilder().setCredentials(c).build()));
    calls.put("ProfilerList", c -> adminStub.profilerList(ProfilerListRequest.newBuilder().setCredentials(c).build()));
    calls.put("ProfilerLoad", c -> adminStub.profilerLoad(
        ProfilerLoadRequest.newBuilder().setCredentials(c).setFileName("anything.json").build()));

    calls.put("GetServerEvents",
        c -> adminStub.getServerEvents(GetServerEventsRequest.newBuilder().setCredentials(c).setFileName("").build()));
    calls.put("DisconnectCluster",
        c -> adminStub.disconnectCluster(DisconnectClusterRequest.newBuilder().setCredentials(c).build()));
    // Empty server name is the LOCAL shutdown. It must be denied before it is scheduled: were the
    // gate missing, this row would stop the JVM the rest of the suite runs in.
    calls.put("Shutdown",
        c -> adminStub.shutdown(ShutdownRequest.newBuilder().setCredentials(c).setServerName("").build()));

    return calls;
  }

  @TestFactory
  Stream<DynamicTest> everyControlPlaneRpcDeniesAnAuthenticatedNonRootCaller() {
    return mutatingCalls().entrySet().stream().map(entry -> DynamicTest.dynamicTest(
        entry.getKey() + " denies a non-root caller",
        () -> assertThatThrownBy(() -> entry.getValue().accept(limited()))
            .isInstanceOf(StatusRuntimeException.class)
            .hasMessageContaining("PERMISSION_DENIED")));
  }

  @TestFactory
  Stream<DynamicTest> everyControlPlaneRpcDeniesAnInvalidPassword() {
    return mutatingCalls().entrySet().stream().map(entry -> DynamicTest.dynamicTest(
        entry.getKey() + " denies an invalid password",
        () -> assertThatThrownBy(() -> entry.getValue().accept(wrongPassword()))
            .isInstanceOf(StatusRuntimeException.class)
            .hasMessageContaining("UNAUTHENTICATED")));
  }

  /**
   * The denials above must be denials and nothing else: none of them may have changed server state
   * on the way to the error.
   */
  @Test
  void aDeniedCallerChangedNothing() {
    mutatingCalls().values().forEach(call -> {
      try {
        call.accept(limited());
      } catch (final StatusRuntimeException expected) {
        // asserted by the factories above
      }
    });

    assertThat(getServer(0).getSecurity().existsUser("root")).isTrue();
    assertThat(getServer(0).getSecurity().existsUser("escalated7304")).isFalse();
    assertThat(getServer(0).getDatabaseNames()).contains(ALLOWED_DB);
    assertThat(getServer(0).getStatus()).isEqualTo(com.arcadedb.server.ArcadeDBServer.STATUS.ONLINE);
  }

  /**
   * Listing is not root-only on either transport, so the gate on it is the answer's contents: HTTP
   * filters {@code list databases} and {@code GET /api/v1/databases} by what the caller may access,
   * and gRPC must too. Before #7304 this RPC handed every authenticated caller every database name
   * on the server.
   */
  @Test
  void listDatabasesShowsANonRootCallerOnlyWhatItMayAccess() {
    final ListDatabasesResponse limitedView = adminStub.listDatabases(
        ListDatabasesRequest.newBuilder().setCredentials(limited()).build());

    assertThat(limitedView.getDatabasesList()).containsExactly(ALLOWED_DB);
    assertThat(limitedView.getDatabasesList()).doesNotContain(getDatabaseName());

    // The positive control: root, which may access everything, still sees the database the limited
    // caller does not - so the assertion above is about the filter and not about an empty server.
    final ListDatabasesResponse rootView = adminStub.listDatabases(ListDatabasesRequest.newBuilder()
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root")
            .setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .build());
    assertThat(rootView.getDatabasesList()).contains(ALLOWED_DB, getDatabaseName());
  }

  /**
   * Schema shape and record counts are database content, so a caller with no grant on the database
   * gets the answer it gets for a name that does not exist.
   */
  @Test
  void getDatabaseInfoHidesADatabaseTheCallerMayNotAccess() {
    assertThatThrownBy(() -> adminStub.getDatabaseInfo(
        GetDatabaseInfoRequest.newBuilder().setCredentials(limited()).setName(getDatabaseName()).build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("NOT_FOUND");

    // ... and still answers for the database it was granted.
    assertThat(adminStub.getDatabaseInfo(
        GetDatabaseInfoRequest.newBuilder().setCredentials(limited()).setName(ALLOWED_DB).build()).getDatabase())
        .isEqualTo(ALLOWED_DB);
  }

  /**
   * The two probes are the deliberate exception: they answer an unauthenticated caller, because a
   * container orchestrator holds no server credentials. Asserted here, next to the gate they are
   * exempt from, so the exemption cannot be widened without this test being read.
   */
  @Test
  void theProbesAreTheOnlyRpcsThatAnswerWithoutCredentials() {
    assertThat(adminStub.health(HealthRequest.newBuilder().build()).getOk()).isTrue();
    assertThat(adminStub.ready(ReadyRequest.newBuilder().build()).getReady()).isTrue();

    // A request with no credentials at all is stopped by the central interceptor for everything else.
    assertThatThrownBy(() -> adminStub.listUsers(ListUsersRequest.newBuilder().build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }
}
