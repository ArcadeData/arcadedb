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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
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
 * Issue #7304: the gRPC admin service exposed database create/drop and nothing else of the control
 * plane, so a gRPC-only deployment had to keep the HTTP port open purely for administration.
 * <p>
 * One test per control-plane group added by that change, each driving the operation through its own
 * gRPC entry point rather than through the shared implementation directly - a test that called
 * {@code ServerControlPlane} would pass against a proto whose RPC was never wired up.
 * <p>
 * Authorization is the subject of {@link Issue7304GrpcControlPlaneAuthorizationIT}; this class runs
 * as {@code root} throughout, except for the two probes, which deliberately carry no credentials.
 */
public class Issue7304GrpcControlPlaneIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private ManagedChannel                                            channel;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  // -------------------------------------------------------------------------------------------
  // Database lifecycle
  // -------------------------------------------------------------------------------------------

  @Test
  void closeDatabaseRemovesItFromTheServerAndOpenDatabaseBringsItBack() {
    final String database = "grpc7304_lifecycle";
    getServer(0).getOrCreateDatabase(database);
    assertThat(getServer(0).getDatabaseNames()).contains(database);

    adminStub.closeDatabase(CloseDatabaseRequest.newBuilder().setCredentials(root()).setName(database).build());
    assertThat(getServer(0).getDatabaseNames()).doesNotContain(database);

    adminStub.openDatabase(OpenDatabaseRequest.newBuilder().setCredentials(root()).setName(database).build());
    assertThat(getServer(0).getDatabaseNames()).contains(database);
  }

  @Test
  void openDatabaseWithAnEmptyNameIsARejectedArgumentAndNotAServerFault() {
    assertThatThrownBy(() -> adminStub.openDatabase(
        OpenDatabaseRequest.newBuilder().setCredentials(root()).setName("").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  /**
   * Aligning is an HA operation: {@code LocalDatabase.align} refuses with "Align Database not
   * supported" on a standalone server, which is what this fixture is. The assertion is therefore that
   * the RPC reaches the SQL command and reports its refusal, rather than answering UNIMPLEMENTED the
   * way the RPC did not exist at all before - the wiring is what is under test, and the aligning
   * itself is covered in HA by {@code RaftServerDatabaseAlignIT}.
   */
  @Test
  void alignDatabaseReachesTheSqlCommandWhichRefusesOutsideHa() {
    assertThatThrownBy(() -> adminStub.alignDatabase(
        AlignDatabaseRequest.newBuilder().setCredentials(root()).setName(getDatabaseName()).build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("Align Database not supported");
  }

  // -------------------------------------------------------------------------------------------
  // Settings
  // -------------------------------------------------------------------------------------------

  @Test
  void setServerSettingStoresACoercedValue() {
    final GlobalConfiguration setting = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION;
    try {
      adminStub.setServerSetting(SetServerSettingRequest.newBuilder().setCredentials(root())
          .setKey(setting.getKey()).setValue("false").build());

      assertThat(getServer(0).getConfiguration().getValueAsBoolean(setting)).isFalse();

      adminStub.setServerSetting(SetServerSettingRequest.newBuilder().setCredentials(root())
          .setKey(setting.getKey()).setValue("TRUE").build());

      assertThat(getServer(0).getConfiguration().getValueAsBoolean(setting)).isTrue();
    } finally {
      getServer(0).getConfiguration().setValue(setting.getKey(), null);
    }
  }

  /**
   * The strict coercion of issue #7124 has to be on this transport too: a boolean typo that read as
   * {@code false} is how the Prometheus endpoint was once published unauthenticated by a 200.
   */
  @Test
  void setServerSettingRefusesAValueTheSettingsTypeCannotRead() {
    final GlobalConfiguration setting = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION;

    assertThatThrownBy(() -> adminStub.setServerSetting(SetServerSettingRequest.newBuilder().setCredentials(root())
        .setKey(setting.getKey()).setValue("ture").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");

    assertThat(getServer(0).getConfiguration().getValueAsBoolean(setting)).isTrue();
  }

  @Test
  void setDatabaseSettingStoresACoercedValueOnTheNamedDatabase() {
    final GlobalConfiguration setting = GlobalConfiguration.TX_RETRIES;
    final Object previous = getServer(0).getDatabase(getDatabaseName()).getConfiguration().getValue(setting);
    try {
      adminStub.setDatabaseSetting(SetDatabaseSettingRequest.newBuilder().setCredentials(root())
          .setDatabase(getDatabaseName()).setKey(setting.getKey()).setValue("7").build());

      // Coerced to the setting's declared type, not stored as the string "7".
      assertThat(getServer(0).getDatabase(getDatabaseName()).getConfiguration().getValueAsInteger(setting)).isEqualTo(7);
    } finally {
      getServer(0).getDatabase(getDatabaseName()).getConfiguration().setValue(setting.getKey(), previous);
    }
  }

  @Test
  void setDatabaseSettingRefusesAValueTheSettingsTypeCannotRead() {
    final GlobalConfiguration setting = GlobalConfiguration.TX_RETRIES;

    assertThatThrownBy(() -> adminStub.setDatabaseSetting(SetDatabaseSettingRequest.newBuilder().setCredentials(root())
        .setDatabase(getDatabaseName()).setKey(setting.getKey()).setValue("not-a-number").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  // -------------------------------------------------------------------------------------------
  // Security
  // -------------------------------------------------------------------------------------------

  @Test
  void createUserListUsersAndDeleteUserRoundTrip() {
    final String user = "grpc7304user";
    try {
      adminStub.createUser(CreateUserRequest.newBuilder().setCredentials(root())
          .setUser(user).setPassword("grpc7304password").build());

      assertThat(getServer(0).getSecurity().existsUser(user)).isTrue();

      final ListUsersResponse users = adminStub.listUsers(ListUsersRequest.newBuilder().setCredentials(root()).build());
      assertThat(users.getUsersList()).extracting(UserInfo::getName).contains(user, "root");

      adminStub.deleteUser(DeleteUserRequest.newBuilder().setCredentials(root()).setUser(user).build());
      assertThat(getServer(0).getSecurity().existsUser(user)).isFalse();
    } finally {
      if (getServer(0).getSecurity().existsUser(user))
        getServer(0).getSecurity().dropUser(user);
    }
  }

  /**
   * A user's authority is the groups it holds per database, so a create that cannot carry them can
   * only make an ungranted account. The grants the request names must reach the stored user, and
   * {@code ListUsers} must report them back.
   */
  @Test
  void createUserCarriesThePerDatabaseGroupsAndListUsersReportsThem() {
    final String user = "grpc7304granted";
    try {
      adminStub.createUser(CreateUserRequest.newBuilder().setCredentials(root())
          .setUser(user).setPassword("grpc7304password")
          .putDatabases(getDatabaseName(), UserGroups.newBuilder().addGroups("admin").build())
          .build());

      final UserInfo created = adminStub.listUsers(ListUsersRequest.newBuilder().setCredentials(root()).build())
          .getUsersList().stream().filter(u -> user.equals(u.getName())).findFirst().orElseThrow();

      assertThat(created.getDatabasesMap()).containsKey(getDatabaseName());
      assertThat(created.getDatabasesMap().get(getDatabaseName()).getGroupsList()).contains("admin");
    } finally {
      if (getServer(0).getSecurity().existsUser(user))
        getServer(0).getSecurity().dropUser(user);
    }
  }

  /**
   * The RPC used to answer {@code UNIMPLEMENTED} and told callers to use the HTTP API. It now runs
   * the same credentials policy the REST create-user path runs, so a short password is refused here
   * exactly as it is there - and with the same meaning: HTTP answers that refusal 403
   * ({@code PostServerCommandHandlerIT.createUserRejectsShortPassword}), whose gRPC equivalent is
   * PERMISSION_DENIED.
   */
  @Test
  void createUserAppliesTheSharedCredentialsPolicy() {
    assertThatThrownBy(() -> adminStub.createUser(CreateUserRequest.newBuilder().setCredentials(root())
        .setUser("grpc7304short").setPassword("short").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("PERMISSION_DENIED")
        .hasMessageContaining("User password too short");

    assertThat(getServer(0).getSecurity().existsUser("grpc7304short")).isFalse();
  }

  @Test
  void deleteUserOfAnUnknownPrincipalIsARejectedArgument() {
    assertThatThrownBy(() -> adminStub.deleteUser(
        DeleteUserRequest.newBuilder().setCredentials(root()).setUser("grpc7304nobody").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  @Test
  void listUsersNeverCarriesAPasswordHash() {
    final ListUsersResponse users = adminStub.listUsers(ListUsersRequest.newBuilder().setCredentials(root()).build());

    assertThat(users.getUsersList()).isNotEmpty();
    // UserInfo has no password field at all; this asserts the serialized message carries no hash
    // under any name, which is what a client would actually receive.
    assertThat(users.toString()).doesNotContain(getServer(0).getSecurity().getUser("root").getPassword());
  }

  // -------------------------------------------------------------------------------------------
  // Backup
  // -------------------------------------------------------------------------------------------

  @Test
  void backupConfigWrittenOverGrpcIsReadBackOverGrpc() {
    final GetBackupConfigResponse before = adminStub.getBackupConfig(
        GetBackupConfigRequest.newBuilder().setCredentials(root()).build());
    assertThat(before.getEnabled()).isFalse();

    final JSONObject config = new JSONObject().put("backupDirectory", "backups7304").put("enabled", false);
    adminStub.setBackupConfig(SetBackupConfigRequest.newBuilder().setCredentials(root())
        .setConfigJson(config.toString()).build());

    final GetBackupConfigResponse after = adminStub.getBackupConfig(
        GetBackupConfigRequest.newBuilder().setCredentials(root()).build());

    // The plugin is not running in this fixture, so the config is saved but not in effect - the same
    // answer GET "get backup config" gives over HTTP in that state.
    assertThat(after.getEnabled()).isFalse();
    assertThat(new JSONObject(after.getConfigJson()).getString("backupDirectory")).isEqualTo("backups7304");
    assertThat(after.getMessage()).contains("restart");
  }

  /**
   * The backup directory is validated before anything is written, and the rejection reaches the
   * caller as a bad argument rather than as a server fault - a path outside the server root is the
   * traversal this check exists to stop.
   */
  @Test
  void setBackupConfigRefusesABackupDirectoryOutsideTheServerRoot() {
    final JSONObject config = new JSONObject().put("backupDirectory", "../../escaped7304");

    assertThatThrownBy(() -> adminStub.setBackupConfig(SetBackupConfigRequest.newBuilder().setCredentials(root())
        .setConfigJson(config.toString()).build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  @Test
  void setBackupConfigRefusesAnEmptyDocument() {
    assertThatThrownBy(() -> adminStub.setBackupConfig(
        SetBackupConfigRequest.newBuilder().setCredentials(root()).setConfigJson("").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  @Test
  void listBackupsAnswersAnEmptyListWhenAutoBackupIsNotRunning() {
    final ListBackupsResponse response = adminStub.listBackups(
        ListBackupsRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName()).build());

    assertThat(response.getDatabase()).isEqualTo(getDatabaseName());
    assertThat(response.getBackupsList()).isEmpty();
  }

  @Test
  void listBackupsWithAnEmptyDatabaseNameIsARejectedArgument() {
    assertThatThrownBy(() -> adminStub.listBackups(
        ListBackupsRequest.newBuilder().setCredentials(root()).setDatabase("").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  /**
   * The archive name is validated before the backup directory is even resolved, so a name carrying a
   * traversal sequence is refused whether or not auto-backup is configured.
   */
  @Test
  void deleteBackupRefusesAFileNameThatLeavesTheBackupDirectory() {
    assertThatThrownBy(() -> adminStub.deleteBackup(DeleteBackupRequest.newBuilder().setCredentials(root())
        .setDatabase(getDatabaseName()).setFileName("../../etc/passwd").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  // -------------------------------------------------------------------------------------------
  // Query profiler
  // -------------------------------------------------------------------------------------------

  @Test
  void profilerRecordsAQueryAndStopReturnsTheRun() {
    adminStub.profilerReset(ProfilerResetRequest.newBuilder().setCredentials(root()).build());

    final ProfilerStateResponse started = adminStub.profilerStart(
        ProfilerStartRequest.newBuilder().setCredentials(root()).setTimeoutSeconds(0).build());
    assertThat(started.getRecording()).isTrue();

    try (final var rs = getServer(0).getDatabase(getDatabaseName()).query("sql", "select from " + VERTEX1_TYPE_NAME)) {
      rs.stream().count();
    }

    final ProfilerDocumentResponse results = adminStub.profilerResults(
        ProfilerResultsRequest.newBuilder().setCredentials(root()).build());
    assertThat(new JSONObject(results.getResultsJson())).isNotNull();

    final ProfilerDocumentResponse stopped = adminStub.profilerStop(
        ProfilerStopRequest.newBuilder().setCredentials(root()).build());
    assertThat(new JSONObject(stopped.getResultsJson())).isNotNull();

    // Reset leaves the profiler not recording, which is the state the RPC reports back.
    final ProfilerStateResponse reset = adminStub.profilerReset(
        ProfilerResetRequest.newBuilder().setCredentials(root()).build());
    assertThat(reset.getRecording()).isFalse();
  }

  /**
   * A saved run is a document on disk, not a bare name: {@code listSavedRuns} reports file name, size
   * and modification time for each. Stopping a recording first makes the list non-empty, so the
   * mapping of those fields is asserted rather than only the empty case - the shape only shows up
   * once there is a run to report.
   */
  @Test
  void profilerListAnswersTheSavedRunsWithTheirFileMetadata() {
    adminStub.profilerStart(ProfilerStartRequest.newBuilder().setCredentials(root()).setTimeoutSeconds(0).build());
    adminStub.profilerStop(ProfilerStopRequest.newBuilder().setCredentials(root()).build());

    final ProfilerListResponse response = adminStub.profilerList(
        ProfilerListRequest.newBuilder().setCredentials(root()).build());

    assertThat(response.getRunsList()).isNotEmpty();
    assertThat(response.getRunsList()).allSatisfy(run -> {
      assertThat(run.getFileName()).startsWith("profiler-run-").endsWith(".json");
      assertThat(run.getSizeBytes()).isPositive();
      assertThat(run.getLastModifiedMs()).isPositive();
    });

    // And the run the list names can be loaded back by that name.
    final ProfilerDocumentResponse loaded = adminStub.profilerLoad(ProfilerLoadRequest.newBuilder().setCredentials(root())
        .setFileName(response.getRuns(0).getFileName()).build());
    assertThat(new JSONObject(loaded.getResultsJson())).isNotNull();
  }

  @Test
  void profilerLoadOfAnUnknownRunFails() {
    assertThatThrownBy(() -> adminStub.profilerLoad(ProfilerLoadRequest.newBuilder().setCredentials(root())
        .setFileName("grpc7304-no-such-run.json").build()))
        .isInstanceOf(StatusRuntimeException.class);
  }

  // -------------------------------------------------------------------------------------------
  // Server lifecycle and cluster
  // -------------------------------------------------------------------------------------------

  @Test
  void getServerEventsReturnsTheCurrentEventFile() {
    final GetServerEventsResponse response = adminStub.getServerEvents(
        GetServerEventsRequest.newBuilder().setCredentials(root()).setFileName("").build());

    // events_json is a JSON array; parsing it is the assertion that the RPC did not hand back a
    // rendering of some other shape.
    assertThat(new com.arcadedb.serializer.json.JSONArray(response.getEventsJson())).isNotNull();
  }

  /**
   * HA is off in this fixture, so the operation cannot run - and that is a precondition failure, not
   * a server fault and not a bad argument.
   */
  @Test
  void disconnectClusterWithoutHaFailsThePrecondition() {
    assertThatThrownBy(() -> adminStub.disconnectCluster(
        DisconnectClusterRequest.newBuilder().setCredentials(root()).build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("FAILED_PRECONDITION");
  }

  /**
   * Shutting down a named peer needs HA. The local shutdown - an empty server name - is exercised
   * only by {@link Issue7304GrpcControlPlaneAuthorizationIT}, which proves an unauthorized caller
   * cannot reach it; a test may not stop the JVM the rest of the suite runs in.
   */
  @Test
  void shutdownOfANamedPeerWithoutHaFailsThePrecondition() {
    assertThatThrownBy(() -> adminStub.shutdown(
        ShutdownRequest.newBuilder().setCredentials(root()).setServerName("someOtherNode").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("FAILED_PRECONDITION");
  }

  // -------------------------------------------------------------------------------------------
  // Probes
  // -------------------------------------------------------------------------------------------

  /**
   * Both probes must answer without credentials, exactly as {@code GET /api/v1/health} and
   * {@code GET /api/v1/ready} do. A probe that needs a password is one a container orchestrator
   * cannot use, which is the whole reason they are exempt from the admin authentication gate.
   */
  @Test
  void healthAnswersWithoutCredentials() {
    assertThat(adminStub.health(HealthRequest.newBuilder().build()).getOk()).isTrue();
  }

  @Test
  void readyAnswersWithoutCredentialsAndReportsAServingNode() {
    final ReadyResponse response = adminStub.ready(ReadyRequest.newBuilder().build());

    assertThat(response.getReady()).isTrue();
    assertThat(response.getReason()).isEmpty();
  }
}
