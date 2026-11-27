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
import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.parser.AlignDatabaseStatement;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The gRPC admin control plane (database lifecycle, server settings, security, progress, sessions and cluster membership)
 * exercised as {@code root}.
 * Each nested class is the regression of one issue and keeps that issue's own client fixture; they share the server,
 * started with the gRPC plugin and nothing else configured.
 */
public class GrpcControlPlaneIT extends BaseGrpcServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  // Regression: #7304
  /**
   * Issue #7304: the gRPC admin service exposed database create/drop and nothing else of the control
   * plane, so a gRPC-only deployment had to keep the HTTP port open purely for administration.
   * <p>
   * One test per control-plane group added by that change, each driving the operation through its own
   * gRPC entry point rather than through the shared implementation directly - a test that called
   * {@code ServerControlPlane} would pass against a proto whose RPC was never wired up.
   * <p>
   * Authorization is the subject of {@link GrpcControlPlaneAuthorizationIT.ControlPlaneAuthorization}; this class runs
   * as {@code root} throughout, except for the two probes, which deliberately carry no credentials.
   */
  @Nested
  class ControlPlane {
    private ManagedChannel                                            channel;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

    @BeforeEach
    void setupGrpcClient() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
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
     * Aligning is refused by every database: the {@code ALIGN DATABASE} statement answers
     * {@link AlignDatabaseStatement#ALIGN_DATABASE_NOT_SUPPORTED} (issue #11796), on the standalone server this fixture
     * is as on a Raft cluster. The assertion is therefore that
     * the RPC reaches the SQL command and reports its refusal, rather than answering UNIMPLEMENTED the
     * way the RPC did not exist at all before - the wiring is what is under test, and the aligning
     * itself is covered in HA by {@code RaftServerDatabaseAlignIT}.
     */
    @Test
    void alignDatabaseReachesTheSqlCommandWhichRefusesOutsideHa() {
      assertThatThrownBy(() -> adminStub.alignDatabase(
          AlignDatabaseRequest.newBuilder().setCredentials(root()).setName(getDatabaseName()).build()))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining(AlignDatabaseStatement.ALIGN_DATABASE_NOT_SUPPORTED)
          // The caller's request is refused, as HTTP answers it 400: not INTERNAL, a server fault (issue #12464)
          .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
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
      final Object previous = getServer(0).getDatabaseForOperator(getDatabaseName()).getConfiguration().getValue(setting);
      try {
        adminStub.setDatabaseSetting(SetDatabaseSettingRequest.newBuilder().setCredentials(root())
            .setDatabase(getDatabaseName()).setKey(setting.getKey()).setValue("7").build());

        // Coerced to the setting's declared type, not stored as the string "7".
        assertThat(getServer(0).getDatabaseForOperator(getDatabaseName()).getConfiguration().getValueAsInteger(setting)).isEqualTo(7);
      } finally {
        getServer(0).getDatabaseForOperator(getDatabaseName()).getConfiguration().setValue(setting.getKey(), previous);
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

    /**
     * Root is the only principal that administers the server, so dropping it leaves a server nobody can administer
     * (issue #12813). The REST {@code DELETE /server/users} route always refused it; this RPC did not.
     */
    @Test
    void deleteUserOfRootIsARejectedArgument() {
      assertThatThrownBy(() -> adminStub.deleteUser(
          DeleteUserRequest.newBuilder().setCredentials(root()).setUser("root").build()))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("INVALID_ARGUMENT")
          .hasMessageContaining("Cannot delete the root user");

      assertThat(getServer(0).getSecurity().existsUser("root")).isTrue();
      // root still authenticates: the next admin call is answered
      assertThat(adminStub.listUsers(ListUsersRequest.newBuilder().setCredentials(root()).build()).getUsersList())
          .extracting(UserInfo::getName).contains("root");
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

      try (final var rs = getServer(0).getDatabaseForOperator(getDatabaseName()).query("sql", "select from " + VERTEX1_TYPE_NAME)) {
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
     * only by {@link GrpcControlPlaneAuthorizationIT.ControlPlaneAuthorization}, which proves an unauthorized caller
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

  // Regression: #7309
  /**
   * Issue #7309: the rows of the {@code /server/*} security table that #7304 left HTTP-only - updating
   * a user, the three group routes and the three API-token routes - reachable over gRPC.
   * <p>
   * One test per RPC, each driving it against a real server and then reading the result back through a
   * DIFFERENT surface than the one that wrote it wherever possible: a group written by {@code SaveGroup}
   * is read through {@code ListGroups}, a token minted by {@code CreateApiToken} is looked for in
   * {@code ListApiTokens}, and a user updated by {@code UpdateUser} is checked against
   * {@code ServerSecurity} itself. A test that asserted only on the RPC's own response would pass
   * against a handler that built a convincing answer and wrote nothing.
   * <p>
   * The channel is loopback, so {@code CreateApiToken}'s transport gate permits the mint here - which
   * makes the successful mint a positive control for the gate as well as for the RPC. The refusal side
   * is {@link #createApiTokenMintsOnlyOverAProtectedTransport}, which drives the verdict into the
   * handler directly because every channel an in-process test can open is loopback, and
   * {@link GrpcTransportSecurityInterceptorTest}, which covers how that verdict is reached.
   */
  @Nested
  class SecurityControlPlane {
    private ManagedChannel                                              channel;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub   admin;

    @BeforeEach
    void openChannel() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      admin = ArcadeDbAdminServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
    }

    @AfterEach
    void closeChannel() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
        channel = null;
      }
    }

    private static DatabaseCredentials root() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    // -------------------------------------------------------------------------------------------
    // Row 1 - PUT /server/users
    // -------------------------------------------------------------------------------------------

    /**
     * The distinction that makes UpdateUser a separate RPC rather than a re-run of CreateUser: each
     * mutable field carries presence, so an update naming only one of them leaves the other alone. A
     * handler that rebuilt the user from the request would pass a password-only assertion and silently
     * drop the grants, so both halves are asserted after each of the two updates.
     */
    @Test
    void updateUserChangesOnlyTheFieldsTheRequestCarries() {
      admin.createUser(CreateUserRequest.newBuilder().setCredentials(root())
          .setUser("elena").setPassword("initialPassword1")
          .putDatabases("*", UserGroups.newBuilder().addGroups("admin").build()).build());

      // Password only: the grants must survive it.
      admin.updateUser(UpdateUserRequest.newBuilder().setCredentials(root())
          .setUser("elena").setPassword("replacedPassword1").build());

      assertThat(grantsOf("elena")).containsExactly("admin");
      assertThat(getServer(0).getSecurity().authenticate("elena", "replacedPassword1", null)).isNotNull();

      // Grants only: the password must survive it.
      admin.updateUser(UpdateUserRequest.newBuilder().setCredentials(root())
          .setUser("elena")
          .setDatabases(UserDatabases.newBuilder()
              .putDatabases("*", UserGroups.newBuilder().addGroups("reader").build()).build())
          .build());

      assertThat(grantsOf("elena")).containsExactly("reader");
      assertThat(getServer(0).getSecurity().authenticate("elena", "replacedPassword1", null)).isNotNull();
    }

    @Test
    void updateUserRejectsAPasswordShorterThanThePolicy() {
      admin.createUser(CreateUserRequest.newBuilder().setCredentials(root())
          .setUser("shortpw").setPassword("longEnoughPassword").build());

      final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
          () -> admin.updateUser(UpdateUserRequest.newBuilder().setCredentials(root())
              .setUser("shortpw").setPassword("short").build()));

      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(failure.getStatus().getDescription()).contains("at least 8 characters");
    }

    @Test
    void updateUserAnswersNotFoundForAnAbsentUser() {
      final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
          () -> admin.updateUser(UpdateUserRequest.newBuilder().setCredentials(root())
              .setUser("nobody").setPassword("longEnoughPassword").build()));

      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    }

    // -------------------------------------------------------------------------------------------
    // Rows 2-4 - the group routes
    // -------------------------------------------------------------------------------------------

    @Test
    void listGroupsReturnsTheGroupDocument() {
      final JSONObject groups = new JSONObject(admin.listGroups(
          ListGroupsRequest.newBuilder().setCredentials(root()).build()).getGroupsJson());

      assertThat(groups.getJSONObject("databases").getJSONObject("*").getJSONObject("groups").has("admin")).isTrue();
    }

    /**
     * A group saved over gRPC has to be readable as a group, not merely stored: the assertion reads the
     * normalized defaults back, which is the part the HTTP handler used to apply and the shared
     * implementation now applies for both transports.
     */
    @Test
    void saveGroupWritesAGroupReadableThroughListGroups() {
      admin.saveGroup(SaveGroupRequest.newBuilder().setCredentials(root())
          .setDatabase("*").setName("analyst")
          .setGroupJson(new JSONObject().put("resultSetLimit", 250L)
              .put("types", new JSONObject().put("*", new JSONObject()
                  .put("access", new JSONArray().put("readRecord"))))
              .toString())
          .build());

      final JSONObject analyst = wildcardGroups().getJSONObject("analyst");
      assertThat(analyst.getLong("resultSetLimit")).isEqualTo(250L);
      // Not supplied by the caller, so it must carry the shared default rather than being absent.
      assertThat(analyst.getLong("readTimeout")).isEqualTo(-1L);
      assertThat(analyst.getJSONObject("types").getJSONObject("*").getJSONArray("access").getString(0))
          .isEqualTo("readRecord");
    }

    @Test
    void deleteGroupRemovesIt() {
      admin.saveGroup(SaveGroupRequest.newBuilder().setCredentials(root())
          .setDatabase("*").setName("temporary").setGroupJson("{}").build());
      assertThat(wildcardGroups().has("temporary")).isTrue();

      admin.deleteGroup(DeleteGroupRequest.newBuilder().setCredentials(root())
          .setDatabase("*").setName("temporary").build());

      assertThat(wildcardGroups().has("temporary")).isFalse();
    }

    @Test
    void deleteGroupAnswersNotFoundForAnAbsentGroup() {
      final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
          () -> admin.deleteGroup(DeleteGroupRequest.newBuilder().setCredentials(root())
              .setDatabase("*").setName("neverExisted").build()));

      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    }

    /**
     * The refusal that keeps a deployment from locking itself out. It lives in the shared
     * implementation, so proving gRPC inherits it is proving it was not left behind in the HTTP handler.
     */
    @Test
    void deleteGroupRefusesTheAdminGroupOfTheDefaultDatabase() {
      final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
          () -> admin.deleteGroup(DeleteGroupRequest.newBuilder().setCredentials(root())
              .setDatabase("*").setName("admin").build()));

      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(failure.getStatus().getDescription()).contains("admin group");
      assertThat(wildcardGroups().has("admin")).isTrue();
    }

    // -------------------------------------------------------------------------------------------
    // Rows 5-7 - the API-token routes
    // -------------------------------------------------------------------------------------------

    @Test
    void createApiTokenReturnsTheMaterialOnceAndListsTheTokenWithout() {
      final CreateApiTokenResponse minted = admin.createApiToken(CreateApiTokenRequest.newBuilder()
          .setCredentials(root()).setName("etl").setDatabase("graph").setExpiresAt(0)
          .setPermissionsJson(new JSONObject()
              .put("types", new JSONObject().put("*", new JSONObject()
                  .put("access", new JSONArray().put("readRecord")))).toString())
          .build());

      assertThat(minted.getToken()).startsWith("at-");
      assertThat(minted.getInfo().getName()).isEqualTo("etl");
      assertThat(minted.getInfo().getDatabase()).isEqualTo("graph");
      assertThat(minted.getInfo().getTokenHash()).isNotEmpty();
      // The listing carries the hash needed to revoke the token and never the token itself - the
      // property that makes the mint's one-time answer meaningful.
      final List<ApiTokenInfo> listed = admin.listApiTokens(
          ListApiTokensRequest.newBuilder().setCredentials(root()).build()).getTokensList();

      final ApiTokenInfo etl = listed.stream().filter(t -> "etl".equals(t.getName())).findFirst().orElseThrow();
      assertThat(etl.getTokenHash()).isEqualTo(minted.getInfo().getTokenHash());
      assertThat(etl.getTokenSuffix()).isEqualTo(minted.getToken().substring(minted.getToken().length() - 4));
      assertThat(etl.toString()).doesNotContain(minted.getToken());
      assertThat(new JSONObject(etl.getPermissionsJson()).getJSONObject("types").getJSONObject("*")
          .getJSONArray("access").getString(0)).isEqualTo("readRecord");
    }

    /** Issue #8060: the listing carries the server-derived {@code expired} flag over gRPC, as HTTP and Studio do. */
    @Test
    void listApiTokensCarriesTheExpiredFlag() throws InterruptedException {
      admin.createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root()).setName("forever").build());
      admin.createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root()).setName("shortLived")
          .setExpiresAt(System.currentTimeMillis() + 1_000).build());

      final long deadline = System.currentTimeMillis() + 30_000;
      ApiTokenInfo shortLived;
      do {
        Thread.sleep(200);
        shortLived = admin.listApiTokens(ListApiTokensRequest.newBuilder().setCredentials(root()).build())
            .getTokensList().stream().filter(t -> "shortLived".equals(t.getName())).findFirst().orElseThrow();
      } while (!shortLived.getExpired() && System.currentTimeMillis() < deadline);

      assertThat(shortLived.getExpired()).as("a token past its expiry is reported as expired").isTrue();
      assertThat(admin.listApiTokens(ListApiTokensRequest.newBuilder().setCredentials(root()).build()).getTokensList()
          .stream().filter(t -> "forever".equals(t.getName())).findFirst().orElseThrow().getExpired())
          .as("a token with no expiry is never expired").isFalse();
    }

    /**
     * Issue #10073: a taken user name is the same conflict a taken token name is. It answered PERMISSION_DENIED, the status
     * a non-admin caller gets, because the duplicate was raised as a security failure.
     */
    @Test
    void createUserRefusesATakenNameWithAlreadyExists() {
      final CreateUserRequest request = CreateUserRequest.newBuilder().setCredentials(root())
          .setUser("grpc_dup_user").setPassword("a-long-password").build();
      admin.createUser(request);

      final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class, () -> admin.createUser(request));

      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    }

    @Test
    void createApiTokenRefusesADuplicateNameWithAlreadyExists() {
      admin.createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root()).setName("unique").build());

      final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
          () -> admin.createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root())
              .setName("unique").build()));

      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    }

    /**
     * The permission-document check that used to be private to {@code PostApiTokenHandler}. An access
     * verb the engine does not define would grant nothing at all, so it is refused rather than stored.
     */
    @Test
    void createApiTokenRefusesAnUnknownAccessVerb() {
      final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
          () -> admin.createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root())
              .setName("bogus")
              .setPermissionsJson(new JSONObject().put("types", new JSONObject().put("*", new JSONObject()
                  .put("access", new JSONArray().put("readAllTheThings")))).toString())
              .build()));

      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(failure.getStatus().getDescription()).contains("readAllTheThings");
    }

    @Test
    void deleteApiTokenRevokesByHash() {
      final CreateApiTokenResponse minted = admin.createApiToken(CreateApiTokenRequest.newBuilder()
          .setCredentials(root()).setName("revokeMe").build());

      admin.deleteApiToken(DeleteApiTokenRequest.newBuilder().setCredentials(root())
          .setTokenHash(minted.getInfo().getTokenHash()).build());

      assertThat(admin.listApiTokens(ListApiTokensRequest.newBuilder().setCredentials(root()).build())
          .getTokensList()).noneMatch(t -> "revokeMe".equals(t.getName()));
    }

    /**
     * Revocation by plaintext token is refused for the reason revocation exists: the token would then
     * appear in whatever recorded the request.
     */
    @Test
    void deleteApiTokenRefusesAPlaintextToken() {
      final CreateApiTokenResponse minted = admin.createApiToken(CreateApiTokenRequest.newBuilder()
          .setCredentials(root()).setName("plaintextRevoke").build());

      final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
          () -> admin.deleteApiToken(DeleteApiTokenRequest.newBuilder().setCredentials(root())
              .setTokenHash(minted.getToken()).build()));

      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      // Still live: a refused revocation must not half-happen.
      assertThat(admin.listApiTokens(ListApiTokensRequest.newBuilder().setCredentials(root()).build())
          .getTokensList()).anyMatch(t -> "plaintextRevoke".equals(t.getName()));
    }

    @Test
    void deleteApiTokenAnswersNotFoundForAnUnknownHash() {
      final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
          () -> admin.deleteApiToken(DeleteApiTokenRequest.newBuilder().setCredentials(root())
              .setTokenHash("0".repeat(64)).build()));

      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    }

    // -------------------------------------------------------------------------------------------
    // Authorization - the gate every one of these RPCs shares
    // -------------------------------------------------------------------------------------------

    /**
     * Every route in this group is {@code checkRootUser}-gated on HTTP, so every RPC has to be
     * {@code requireServerAdmin}-gated here. Driven once per RPC rather than once overall: the gate is a
     * line in each handler, and a handler that forgot it would still pass a test that only checked its
     * neighbours.
     */
    @Test
    void everyNewRpcRefusesANonRootAccount() {
      admin.createUser(CreateUserRequest.newBuilder().setCredentials(root())
          .setUser("plainuser").setPassword("plainPassword1").build());

      final DatabaseCredentials plain = DatabaseCredentials.newBuilder()
          .setUsername("plainuser").setPassword("plainPassword1").build();

      assertPermissionDenied(() -> admin.updateUser(
          UpdateUserRequest.newBuilder().setCredentials(plain).setUser("plainuser").build()));
      assertPermissionDenied(() -> admin.listGroups(
          ListGroupsRequest.newBuilder().setCredentials(plain).build()));
      assertPermissionDenied(() -> admin.saveGroup(
          SaveGroupRequest.newBuilder().setCredentials(plain).setDatabase("*").setName("x").build()));
      assertPermissionDenied(() -> admin.deleteGroup(
          DeleteGroupRequest.newBuilder().setCredentials(plain).setDatabase("*").setName("x").build()));
      assertPermissionDenied(() -> admin.listApiTokens(
          ListApiTokensRequest.newBuilder().setCredentials(plain).build()));
      assertPermissionDenied(() -> admin.createApiToken(
          CreateApiTokenRequest.newBuilder().setCredentials(plain).setName("x").build()));
      assertPermissionDenied(() -> admin.deleteApiToken(
          DeleteApiTokenRequest.newBuilder().setCredentials(plain).setTokenHash("0".repeat(64)).build()));
    }

    private static void assertPermissionDenied(final Runnable call) {
      final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class, call::run);
      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    // -------------------------------------------------------------------------------------------
    // The two questions the issue attaches to the mint
    // -------------------------------------------------------------------------------------------

    /**
     * 6a - the mint is refused unless the call's transport protects the answer.
     * <p>
     * Every channel an in-process test can open is loopback, so the three transport verdicts are driven
     * into the handler through the context key that {@link GrpcTransportSecurityInterceptor} publishes,
     * against the real service and the real server. The <b>absent</b> case is the one that matters most:
     * it is what a future refactor that drops the interceptor registration produces, and it must read as
     * "nothing vouched for this connection", not as "no objection".
     * <p>
     * The permitted case is the positive control. Without it, a handler that refused unconditionally
     * would pass the two refusal assertions.
     */
    @Test
    void createApiTokenMintsOnlyOverAProtectedTransport() {
      final ArcadeDbGrpcAdminService service = new ArcadeDbGrpcAdminService(getServer(0),
          getServer(0).getSecurity().getCredentialsValidator());

      final CreateApiTokenRequest request = CreateApiTokenRequest.newBuilder()
          .setCredentials(root()).setName("gated").build();

      assertThat(mintUnder(service, request, null))
          .as("no interceptor ran, so nothing vouched for the transport")
          .isEqualTo(Status.Code.FAILED_PRECONDITION);

      assertThat(mintUnder(service, request, false))
          .as("the interceptor read the transport as unprotected")
          .isEqualTo(Status.Code.FAILED_PRECONDITION);

      // Nothing was minted by either refusal.
      assertThat(admin.listApiTokens(ListApiTokensRequest.newBuilder().setCredentials(root()).build())
          .getTokensList()).noneMatch(t -> "gated".equals(t.getName()));

      // Positive control: the same call over a transport the interceptor vouched for succeeds.
      final CapturingObserver<CreateApiTokenResponse> permitted = new CapturingObserver<>();
      contextWith(true).run(() -> service.createApiToken(request, permitted));
      assertThat(permitted.error).isNull();
      assertThat(permitted.value.getToken()).startsWith("at-");
    }

    /**
     * 6b - a minted token reaches no log sink and no metric tag.
     * <p>
     * The two interceptors that see every message read the method name, the status and, for one specific
     * response type, a boolean; neither stringifies a payload. That is a property of today's code that
     * nothing enforces, which is what this test is for: it mints a real token over the real server with
     * every engine log call captured, and searches the captured text - and every meter the gRPC
     * interceptors registered - for the token material.
     * <p>
     * The capture is installed through {@link LogManager#setLogger}, the engine's own logging seam,
     * rather than by attaching a {@code java.util.logging} handler to the root logger. That distinction
     * decides whether this test works at all: the engine logs through its pluggable
     * {@link Logger}, so a JUL handler sees whatever that implementation chooses to
     * forward and at whatever level it is configured for - which was verified to miss an INFO-level leak
     * deliberately injected into {@code GrpcLoggingInterceptor.sendMessage}. Capturing at the seam sees
     * every call the engine makes, before any level or handler filtering.
     */
    @Test
    void aMintedTokenReachesNoLogSinkAndNoMetricTag() {
      final LogManager logManager = LogManager.instance();
      final Logger previousLogger = logManager.getLogger();
      final CapturingLogger captured = new CapturingLogger(previousLogger);

      final String token;
      try {
        logManager.setLogger(captured);
        token = admin.createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root())
            .setName("loggingProbe").build()).getToken();
        // Emitted while the capture is still installed, obviously - but stated explicitly because
        // getting this wrong is silent: a canary logged after the restore proves nothing, and the
        // assertion below would then fail for a reason that has nothing to do with token material.
        logManager.log(this, Level.FINE, "canary %s", "value");
      } finally {
        logManager.setLogger(previousLogger);
      }

      assertThat(token).startsWith("at-");
      // The capture has to have seen SOMETHING, or this test would pass however loudly the token leaked.
      // The gRPC interceptors' own FINE lines would satisfy that, so the canary is explicit rather than
      // inherited from them - it also pins that arguments, not just format strings, are captured.
      assertThat(captured.text()).contains("canary").contains("value");

      assertThat(captured.text()).doesNotContain(token);
      // The suffix is four characters and could collide with anything; the hash could not, and it is the
      // value an over-helpful log line would most plausibly carry.
      assertThat(captured.text()).doesNotContain(ApiTokenConfiguration.hashToken(token));

      final String meters = Metrics.globalRegistry.getMeters().stream()
          .map(meter -> meter.getId().toString())
          .collect(Collectors.joining(" "));
      assertThat(meters).doesNotContain(token);
    }

    /**
     * Calls {@code createApiToken} with the transport verdict set to {@code safe} - or, when it is null,
     * with the key never set at all - and returns the failure.
     */
    private static Status.Code mintUnder(final ArcadeDbGrpcAdminService service,
        final CreateApiTokenRequest request, final Boolean safe) {
      final CapturingObserver<CreateApiTokenResponse> observer = new CapturingObserver<>();
      contextWith(safe).run(() -> service.createApiToken(request, observer));

      assertThat(observer.error).as("the call must have failed").isNotNull();
      // Called directly rather than over a channel, so the service's own StatusException arrives here -
      // a channel would have re-thrown it to the caller as the StatusRuntimeException the other tests
      // in this class catch. Reading the status off either shape keeps the assertion about the status.
      return Status.fromThrowable(observer.error).getCode();
    }

    private static Context contextWith(final Boolean safe) {
      return safe == null ? Context.current()
          : Context.current().withValue(GrpcTransportSecurityInterceptor.SECRET_SAFE_TRANSPORT_KEY, safe);
    }

    /** Records what the service wrote back, so a unary call can be driven without a channel. */
    private static final class CapturingObserver<T> implements StreamObserver<T> {
      private T         value;
      private Throwable error;

      @Override
      public void onNext(final T value) {
        this.value = value;
      }

      @Override
      public void onError(final Throwable error) {
        this.error = error;
      }

      @Override
      public void onCompleted() {
      }
    }

    /**
     * Records every engine log call, message and arguments alike, and forwards it to the logger it
     * replaced so a failing run still shows its normal output.
     */
    private static final class CapturingLogger implements Logger {
      private final Logger delegate;
      private final StringBuilder           text = new StringBuilder();

      private CapturingLogger(final Logger delegate) {
        this.delegate = delegate;
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        record(message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11,
            arg12, arg13, arg14, arg15, arg16, arg17);
        delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8,
            arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        record(message, exception, context, args);
        delegate.log(requester, level, message, exception, context, args);
      }

      @Override
      public void flush() {
        delegate.flush();
      }

      private void record(final String message, final Throwable exception, final String context, final Object... values) {
        synchronized (text) {
          text.append(message).append(' ').append(context).append(' ');
          if (exception != null)
            text.append(exception).append(' ');
          for (final Object value : values)
            text.append(value).append(' ');
        }
      }

      private String text() {
        synchronized (text) {
          return text.toString();
        }
      }
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    /** The groups {@code userName} holds on the wildcard database, read from the server's own security. */
    private List<String> grantsOf(final String userName) {
      final JSONObject databases = getServer(0).getSecurity().getUser(userName).toJSON().getJSONObject("databases");
      final JSONArray groups = databases.getJSONArray("*");
      return IntStream.range(0, groups.length()).mapToObj(groups::getString).toList();
    }

    private JSONObject wildcardGroups() {
      return new JSONObject(admin.listGroups(ListGroupsRequest.newBuilder().setCredentials(root()).build())
          .getGroupsJson()).getJSONObject("databases").getJSONObject("*").getJSONObject("groups");
    }
  }

  // Regression: #7310
  /**
   * Issue #7310: the two discovery routes the HTTP control plane kept to itself after #7304 -
   * {@code GET /api/v1/progress/{database}} and {@code GET /api/v1/sessions} - now have RPCs on
   * {@code ArcadeDbAdminService}.
   * <p>
   * Each RPC is driven through a real channel rather than through {@code ServerControlPlane} directly: a
   * test against the shared implementation would pass against a proto whose RPC was never wired into the
   * service. Authorization is the subject of {@link GrpcControlPlaneAuthorizationIT.ProgressAndSessionsAuthorization}; this
   * class runs as {@code root}.
   */
  @Nested
  class ProgressAndSessions {
    private ManagedChannel                                            channel;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

    @BeforeEach
    void setupGrpcClient() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
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
    // GetProgress
    // -------------------------------------------------------------------------------------------

    /**
     * The registry is fed the way a real maintenance statement feeds it - register, publish a step,
     * unregister in a finally - so the RPC reads the same live snapshot {@code CHECK DATABASE} publishes.
     */
    @Test
    void getProgressReportsTheStepOfARunningOperation() {
      final OperationProgress operation = OperationProgressRegistry.instance()
          .register(getDatabaseName(), "check database fix");
      try {
        operation.onProgress("Checking vertices 'Account'", 3, 9, 42, 100);

        final GetProgressResponse response = adminStub.getProgress(
            GetProgressRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName()).build());

        assertThat(response.getOperationsList()).hasSize(1);
        final OperationProgressInfo info = response.getOperations(0);
        assertThat(info.getDatabase()).isEqualTo(getDatabaseName());
        assertThat(info.getOperation()).isEqualTo("check database fix");
        assertThat(info.getStepName()).isEqualTo("Checking vertices 'Account'");
        assertThat(info.getStepIndex()).isEqualTo(3);
        assertThat(info.getTotalSteps()).isEqualTo(9);
        assertThat(info.getDone()).isEqualTo(42);
        assertThat(info.getTotal()).isEqualTo(100);
        assertThat(info.getPercentage()).isEqualTo(42);
        assertThat(info.getId()).isPositive();
        assertThat(info.getStartedOn()).isPositive();
        assertThat(info.getElapsedMs()).isNotNegative();
      } finally {
        OperationProgressRegistry.instance().unregister(operation);
      }
    }

    /**
     * An unknown step total is reported as -1 rather than as 0, the same encoding
     * {@code OperationProgress.toJSON()} uses, so a client cannot mistake "unknown" for "nothing done".
     */
    @Test
    void getProgressReportsMinusOneWhenTheStepTotalIsUnknown() {
      final OperationProgress operation = OperationProgressRegistry.instance()
          .register(getDatabaseName(), "rebuild index");
      try {
        operation.onProgress("Scanning", 1, 2, 7, -1);

        final GetProgressResponse response = adminStub.getProgress(
            GetProgressRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName()).build());

        assertThat(response.getOperationsList()).hasSize(1);
        assertThat(response.getOperations(0).getTotal()).isEqualTo(-1);
        assertThat(response.getOperations(0).getPercentage()).isEqualTo(-1);
      } finally {
        OperationProgressRegistry.instance().unregister(operation);
      }
    }

    /**
     * Nothing running is an empty answer, not an error: the endpoint is meant to be polled.
     */
    @Test
    void getProgressIsEmptyWhenNothingIsRunning() {
      final GetProgressResponse response = adminStub.getProgress(
          GetProgressRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName()).build());

      assertThat(response.getOperationsList()).isEmpty();
    }

    /**
     * The registry is keyed by database name, so an operation on one database must not appear under
     * another - the filter the HTTP route applies through {@code getOperations(databaseName)}.
     */
    @Test
    void getProgressDoesNotLeakOperationsOfAnotherDatabase() {
      final String otherDatabase = "grpc7310_other";
      getServer(0).getOrCreateDatabase(otherDatabase);

      final OperationProgress operation = OperationProgressRegistry.instance().register(otherDatabase, "compact index");
      try {
        operation.onProgress("Compacting", 1, 1, 1, 1);

        assertThat(adminStub.getProgress(
            GetProgressRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName()).build())
            .getOperationsList()).isEmpty();

        assertThat(adminStub.getProgress(
            GetProgressRequest.newBuilder().setCredentials(root()).setDatabase(otherDatabase).build())
            .getOperationsList()).hasSize(1);
      } finally {
        OperationProgressRegistry.instance().unregister(operation);
      }
    }

    /**
     * A missing database name is the caller's mistake, the 400 the HTTP route answers, and must not read
     * as a server fault.
     */
    @Test
    void getProgressWithAnEmptyDatabaseNameIsARejectedArgument() {
      assertThatThrownBy(() -> adminStub.getProgress(
          GetProgressRequest.newBuilder().setCredentials(root()).setDatabase("").build()))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("INVALID_ARGUMENT");
    }

    // -------------------------------------------------------------------------------------------
    // ListSessions
    // -------------------------------------------------------------------------------------------

    @Test
    void listSessionsReportsTheServersOpenHttpSessions() {
      final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
      final HttpAuthSession session = getServer(0).getHttpServer().getAuthSessionManager()
          .createSession(user, "10.1.2.3", "arcade-tests/1.0", "IT", "Rome");
      try {
        final ListSessionsResponse response = adminStub.listSessions(
            ListSessionsRequest.newBuilder().setCredentials(root()).build());

        assertThat(response.getCount()).isEqualTo(response.getSessionsCount());
        final SessionInfo info = response.getSessionsList().stream()
            .filter(s -> session.getToken().equals(s.getToken())).findFirst().orElseThrow();
        assertThat(info.getUser()).isEqualTo("root");
        assertThat(info.getSourceIp()).isEqualTo("10.1.2.3");
        assertThat(info.getUserAgent()).isEqualTo("arcade-tests/1.0");
        assertThat(info.getCountry()).isEqualTo("IT");
        assertThat(info.getCity()).isEqualTo("Rome");
        assertThat(info.getCreatedAt()).isPositive();
        assertThat(info.getLastUpdate()).isPositive();
        assertThat(info.getElapsedMs()).isNotNegative();
      } finally {
        getServer(0).getHttpServer().getAuthSessionManager().removeSession(session.getToken());
      }
    }

    /**
     * A session logged in without the optional client metadata carries nulls for those four fields;
     * proto3 has no null, so they must arrive as empty strings rather than failing the call.
     */
    @Test
    void listSessionsRendersAbsentClientMetadataAsEmptyStrings() {
      final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
      final HttpAuthSession session = getServer(0).getHttpServer().getAuthSessionManager().createSession(user);
      try {
        final SessionInfo info = adminStub
            .listSessions(ListSessionsRequest.newBuilder().setCredentials(root()).build())
            .getSessionsList().stream().filter(s -> session.getToken().equals(s.getToken())).findFirst().orElseThrow();

        assertThat(info.getSourceIp()).isEmpty();
        assertThat(info.getUserAgent()).isEmpty();
        assertThat(info.getCountry()).isEmpty();
        assertThat(info.getCity()).isEmpty();
      } finally {
        getServer(0).getHttpServer().getAuthSessionManager().removeSession(session.getToken());
      }
    }

    /**
     * A closed session disappears from the listing, proving the RPC reads the manager live rather than a
     * snapshot taken when the service was built.
     */
    @Test
    void listSessionsStopsReportingAClosedSession() {
      final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
      final HttpAuthSession session = getServer(0).getHttpServer().getAuthSessionManager().createSession(user);

      assertThat(adminStub.listSessions(ListSessionsRequest.newBuilder().setCredentials(root()).build())
          .getSessionsList()).anyMatch(s -> session.getToken().equals(s.getToken()));

      getServer(0).getHttpServer().getAuthSessionManager().removeSession(session.getToken());

      assertThat(adminStub.listSessions(ListSessionsRequest.newBuilder().setCredentials(root()).build())
          .getSessionsList()).noneMatch(s -> session.getToken().equals(s.getToken()));
    }
  }

  // Regression: #7400
  /**
   * Issue #7400: {@code ArcadeDbAdminService} shipped {@code DisconnectCluster} with #7304 and not its
   * pair-mate, so a gRPC-only client could take a server out of a cluster and then had to reopen the
   * HTTP port to attempt the other half.
   * <p>
   * What is asserted here is protocol <em>parity</em>: the gRPC caller receives the same answer from the
   * same method the HTTP verb calls, rather than {@code UNIMPLEMENTED} for a verb the other transport
   * accepts. This fixture runs no HA at all, so the answers it drives are the refusals; the join itself
   * arrived with #7401 and is exercised against a live Raft cluster by
   * {@code ClusterRejoinMembershipIT}.
   * <p>
   * The pair is driven together throughout, as the issue asks: a test that called connect alone would
   * pass against a service that had lost disconnect.
   */
  @Nested
  class ConnectCluster {
    private static final String LIMITED_USER = "limited7400";
    private static final String LIMITED_PASS = "limited7400pass";
    private static final String PEER_ADDRESS = "localhost:2425";

    private ManagedChannel                                            channel;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

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

    private static DatabaseCredentials root() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    private static DatabaseCredentials limited() {
      return DatabaseCredentials.newBuilder().setUsername(LIMITED_USER).setPassword(LIMITED_PASS).build();
    }

    private static DatabaseCredentials wrongPassword() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword("not-the-root-password").build();
    }

    private ConnectClusterResponse connect(final DatabaseCredentials credentials, final String address) {
      return adminStub.connectCluster(
          ConnectClusterRequest.newBuilder().setCredentials(credentials).setServerAddress(address).build());
    }

    private DisconnectClusterResponse disconnect(final DatabaseCredentials credentials) {
      return adminStub.disconnectCluster(DisconnectClusterRequest.newBuilder().setCredentials(credentials).build());
    }

    /**
     * The RPC exists and reaches the shared implementation. Before this change the generated stub had
     * no such method at all; a service that declared the RPC and left it unimplemented would answer
     * {@code UNIMPLEMENTED} here rather than the refusal below.
     * <p>
     * {@code FAILED_PRECONDITION} is the mapper's arm for
     * {@code ServerControlPlane.OperationNotAvailableException} - the operation cannot run in this
     * server's configuration at all, as opposed to having been attempted and failed - and the
     * description is the shared implementation's own message, not a rendering invented here. Since #7401
     * made the verb a real join, this fixture's refusal is that HA is not enabled here at all; what the
     * test is for is unchanged, and is that the refusal comes from the shared implementation.
     */
    @Test
    void connectClusterIsRefusedByTheSharedImplementation() {
      assertThatThrownBy(() -> connect(root(), PEER_ADDRESS))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("FAILED_PRECONDITION")
          .hasMessageContaining("not running with High Availability module enabled");
    }

    /**
     * The address the caller sent is the address the shared implementation received.
     * <p>
     * Without this the parameter is invisible from outside: every other assertion in this class passes
     * just as well against a handler that dropped {@code req.getServerAddress()} and called
     * {@code connectCluster("")}, because the refusal would be identical. The shared implementation
     * echoes the address into its message - as it names the user or backup file every other command
     * could not act on - which is what makes the wiring observable end to end while the operation
     * itself still does nothing with the value.
     */
    @Test
    void theAddressReachesTheSharedImplementationUnmodified() {
      assertThatThrownBy(() -> connect(root(), PEER_ADDRESS))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining(PEER_ADDRESS);
    }

    /**
     * HTTP's {@code extractTarget} yields {@code ""} for a bare {@code connect cluster}. Since #7401 the
     * verb has an argument it genuinely needs, so that is the caller's error on both transports: HTTP
     * answers 400 and this answers {@code INVALID_ARGUMENT}. The RPC still invents no gate of its own -
     * the status comes from the shared implementation's {@code IllegalArgumentException}, which is what
     * keeps the two transports from disagreeing on the same input, the drift this issue is about.
     */
    @Test
    void connectClusterWithAnEmptyAddressIsRefusedTheSameWay() {
      assertThatThrownBy(() -> connect(root(), ""))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("INVALID_ARGUMENT")
          .hasMessageContaining("requires the address of the server to join");
    }

    /**
     * Both halves of the pair reach an HA stack that is not enabled in this fixture, and both say so
     * the same way. Driving them together is what makes this a parity assertion rather than two
     * unrelated ones.
     */
    @Test
    void theClusterPairFailsThePreconditionAlike() {
      assertThatThrownBy(() -> connect(root(), PEER_ADDRESS))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("FAILED_PRECONDITION");

      assertThatThrownBy(() -> disconnect(root()))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("FAILED_PRECONDITION");
    }

    /**
     * Cluster membership is a privileged operation: an authenticated but non-root caller is denied by
     * the handler's {@code requireServerAdmin} gate, exactly as {@code DisconnectCluster} is and as
     * {@code PostServerCommandHandler}'s {@code checkRootUser} denies the HTTP verb.
     * <p>
     * The denial must arrive <em>instead of</em> the precondition failure above: were the gate missing,
     * this caller would reach the shared implementation and get {@code FAILED_PRECONDITION}, which
     * would still be an exception and would still look like a passing test if the status were not
     * asserted.
     */
    @Test
    void theClusterPairDeniesAnAuthenticatedNonRootCaller() {
      assertThatThrownBy(() -> connect(limited(), PEER_ADDRESS))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("PERMISSION_DENIED");

      assertThatThrownBy(() -> disconnect(limited()))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("PERMISSION_DENIED");
    }

    /**
     * A bad password is refused by the central {@code GrpcAuthInterceptor} before the handler runs at
     * all, so the pair answers {@code UNAUTHENTICATED} rather than either of the statuses above.
     */
    @Test
    void theClusterPairDeniesAnInvalidPassword() {
      assertThatThrownBy(() -> connect(wrongPassword(), PEER_ADDRESS))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("UNAUTHENTICATED");

      assertThatThrownBy(() -> disconnect(wrongPassword()))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("UNAUTHENTICATED");
    }
  }
}
