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
import com.arcadedb.server.security.ServerSecurity;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Authorization of the gRPC services: per-database rights on the data service and server-admin rights on the admin control
 * plane, including the streaming restore/import RPCs and the progress/session RPCs.
 * Each nested class is the regression of one issue and keeps that issue's own users and client fixture; they share the
 * server, started with the gRPC plugin and nothing else configured.
 */
public class GrpcControlPlaneAuthorizationIT extends BaseGrpcServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  // Regression: #4794
  /**
   * Regression test for issue #4794.
   * <p>
   * The gRPC auth interceptor authenticates the caller against the database named in the
   * {@code x-arcade-database} metadata header, but the service methods operate on the database named
   * in the request body. Without per-database authorization at the body-database resolution point a
   * caller can authenticate with a header naming a database it may access and then read/write a
   * different database it is NOT authorized for.
   * <p>
   * This test creates a user authorized only for a dedicated database, then issues operations whose
   * request body targets a different (forbidden) database while the header still names the allowed
   * one. The forbidden operations must be rejected with {@code PERMISSION_DENIED}.
   * <p>
   * The read ({@code executeQuery}), command ({@code executeCommand}) and write ({@code createRecord})
   * cases below are representative, not exhaustive: every database-touching RPC resolves its target
   * database through the single {@code ArcadeDbGrpcService.getDatabase(...)} chokepoint, where
   * {@code validateCredentials(...)} authorizes the request-body database. The untested RPCs
   * (e.g. {@code lookupByRid}, {@code updateRecord}, {@code deleteRecord}, {@code bulkInsert}) are not
   * intentionally excluded; they share that same gate and so are covered transitively.
   */
  @Nested
  class PerDatabaseAuthorization {
    private static final String ALLOWED_DB      = "allowed4794db";
    private static final String LIMITED_USER    = "limited4794";
    private static final String LIMITED_PASS    = "limited4794pass";

    private static final Metadata.Key<String> USER_HEADER     =
        Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> PASSWORD_HEADER =
        Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DATABASE_HEADER =
        Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

    private ManagedChannel channel;

    @BeforeEach
    void setupRestrictedUserAndChannel() {
      final ServerSecurity security = getServer(0).getSecurity();

      // The database the limited user IS authorized for.
      getServer(0).getOrCreateDatabase(ALLOWED_DB);

      // Create a user authorized ONLY for ALLOWED_DB (no wildcard, no access to the default test db).
      if (!security.existsUser(LIMITED_USER)) {
        final JSONObject config = new JSONObject();
        config.put("name", LIMITED_USER);
        config.put("password", security.encodePassword(LIMITED_PASS));
        config.put("databases", new JSONObject().put(ALLOWED_DB, new JSONArray().put("admin")));
        security.createUser(config);
      }

      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    }

    @AfterEach
    void teardown() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    /**
     * Builds a stub whose metadata header names {@code headerDb} but that otherwise carries the
     * limited user's basic-auth credentials.
     */
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stubWithHeaderDatabase(final String headerDb) {
      final Channel authenticated = ClientInterceptors.intercept(channel, new ClientInterceptor() {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions, final Channel next) {
          return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
              headers.put(USER_HEADER, LIMITED_USER);
              headers.put(PASSWORD_HEADER, LIMITED_PASS);
              headers.put(DATABASE_HEADER, headerDb);
              super.start(responseListener, headers);
            }
          };
        }
      });
      return ArcadeDbServiceGrpc.newBlockingStub(authenticated);
    }

    private DatabaseCredentials limitedCredentials() {
      return DatabaseCredentials.newBuilder().setUsername(LIMITED_USER).setPassword(LIMITED_PASS).build();
    }

    @Test
    void crossDatabaseQueryIsDenied() {
      // Header names the database the user CAN access; request body targets the default test database
      // (getDatabaseName()) which the limited user is NOT authorized for. The query RPC surfaces the
      // failure as a gRPC error status, and the denial must come from the database-authorization gate
      // (not, e.g., a leaked record-level error) so the bypass is closed for every operation.
      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubWithHeaderDatabase(ALLOWED_DB);

      final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(limitedCredentials())
          .setQuery("SELECT FROM V1")
          .build();

      assertThatThrownBy(() -> stub.executeQuery(request))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("has not access to database '" + getDatabaseName() + "'");
    }

    @Test
    void crossDatabaseCommandIsDenied() {
      // The command RPC now reports execution errors as a gRPC error status through GrpcErrorMapper, the
      // same as every other RPC (issue #6192). The denial must be raised by the per-database authorization
      // gate before any work happens against the request-body database.
      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubWithHeaderDatabase(ALLOWED_DB);

      final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(limitedCredentials())
          .setCommand("INSERT INTO Person SET name = 'intruder'")
          .build();

      assertThatThrownBy(() -> stub.executeCommand(request))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("has not access to database '" + getDatabaseName() + "'");
    }

    @Test
    void crossDatabaseCreateRecordIsDenied() {
      // Write path: createRecord resolves its target database from the request body through the same
      // getDatabase() authorization chokepoint. A header naming the allowed database must not grant a
      // write into a different body database the user cannot access. createRecord surfaces failures via
      // a gRPC error status (its catch block masks the PERMISSION_DENIED code as INTERNAL but preserves
      // the authorization message), so the denial is asserted on the message.
      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubWithHeaderDatabase(ALLOWED_DB);

      final CreateRecordRequest request = CreateRecordRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(limitedCredentials())
          .setType("Person")
          .setRecord(GrpcRecord.newBuilder()
              .putProperties("name", GrpcValue.newBuilder().setStringValue("intruder").build())
              .build())
          .build();

      assertThatThrownBy(() -> stub.createRecord(request))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("has not access to database '" + getDatabaseName() + "'");
    }

    @Test
    void authorizedDatabaseAccessSucceeds() {
      // Positive control: same user, body database == the database it is authorized for.
      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubWithHeaderDatabase(ALLOWED_DB);

      final ExecuteCommandRequest createType = ExecuteCommandRequest.newBuilder()
          .setDatabase(ALLOWED_DB)
          .setCredentials(limitedCredentials())
          .setCommand("CREATE DOCUMENT TYPE Doc4794 IF NOT EXISTS")
          .build();

      final ExecuteCommandResponse response = stub.executeCommand(createType);
      assertThat(response.getSuccess()).isTrue();
    }
  }

  // Regression: #7304
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
  @Nested
  class ControlPlaneAuthorization {
    private static final String ALLOWED_DB   = "allowed7304db";
    private static final String LIMITED_USER = "limited7304";
    private static final String LIMITED_PASS = "limited7304pass";

    private ManagedChannel                                            channel;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

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
      // The other half of the cluster pair, added by #7400. Its own behaviour is
      // GrpcControlPlaneIT.ConnectCluster's subject; the row is here because this table is where a
      // control-plane RPC missing its gate becomes visible, and cluster membership is privileged.
      calls.put("ConnectCluster", c -> adminStub.connectCluster(
          ConnectClusterRequest.newBuilder().setCredentials(c).setServerAddress("localhost:2425").build()));
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
     * {@code ExistsDatabase} answers the same predicate {@code GET /api/v1/exists/{database}} answers:
     * installed AND accessible to the caller. {@code GetExistsDatabaseHandler} evaluates
     * {@code canAccessToDatabase} for the single name rather than building the whole authorized set,
     * which is a cost decision, not a decision to skip authorization - so a database the caller has no
     * grant on reads as absent on both transports, not as present.
     */
    @Test
    void existsDatabaseAnswersFalseForADatabaseTheCallerMayNotAccess() {
      // The database is really there ...
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();

      // ... and root sees it.
      assertThat(adminStub.existsDatabase(ExistsDatabaseRequest.newBuilder()
          .setCredentials(DatabaseCredentials.newBuilder().setUsername("root")
              .setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
          .setName(getDatabaseName()).build()).getExists()).isTrue();

      // The limited caller, which holds no grant on it, must not be able to tell it apart from a name
      // that does not exist at all.
      assertThat(adminStub.existsDatabase(ExistsDatabaseRequest.newBuilder().setCredentials(limited())
          .setName(getDatabaseName()).build()).getExists()).isFalse();
      assertThat(adminStub.existsDatabase(ExistsDatabaseRequest.newBuilder().setCredentials(limited())
          .setName("no_such_database_7304").build()).getExists()).isFalse();

      // And it still sees the one it was granted.
      assertThat(adminStub.existsDatabase(ExistsDatabaseRequest.newBuilder().setCredentials(limited())
          .setName(ALLOWED_DB).build()).getExists()).isTrue();
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

  // Regression: #7308
  /**
   * Issue #7308: the three restore/import RPCs are the most destructive operations on the control
   * plane - each of them creates or replaces a whole database - so each has to enforce the same
   * root-user gate {@code PostServerCommandHandler} enforces through {@code checkRootUser}, and to
   * enforce it <i>before</i> doing anything.
   * <p>
   * The sibling of {@code GrpcControlPlaneAuthorizationIT.ControlPlaneAuthorization} for the RPCs that stream. They
   * need their own class because a server-streaming call reports nothing until its iterator is read:
   * the refusal arrives when the stream is drained, not when the stub method returns, so the table's
   * entries have to consume the stream to observe it.
   */
  @Nested
  class RestoreImportAuthorization {
    private static final String ALLOWED_DB   = "allowed7308db";
    private static final String LIMITED_USER = "limited7308";
    private static final String LIMITED_PASS = "limited7308pass";

    private ManagedChannel                                            channel;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

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

    private static DatabaseCredentials limited() {
      return DatabaseCredentials.newBuilder().setUsername(LIMITED_USER).setPassword(LIMITED_PASS).build();
    }

    private static DatabaseCredentials wrongPassword() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword("not-the-root-password").build();
    }

    /**
     * The streaming control-plane RPCs, keyed by name so a failure names the RPC that is missing its
     * gate. Every one of them names a database that does not exist and a URL that is never fetched: if
     * the gate holds, none of that is ever reached.
     */
    private Map<String, Function<DatabaseCredentials, Iterator<?>>> streamingCalls() {
      final Map<String, Function<DatabaseCredentials, Iterator<?>>> calls = new LinkedHashMap<>();

      calls.put("RestoreBackup", c -> adminStub.restoreBackup(RestoreBackupRequest.newBuilder().setCredentials(c)
          .setDatabase(ALLOWED_DB).setFileName("anything-backup-20260101.zip").setTargetDatabase("escalated7308")
          .setOverwrite(true).build()));
      calls.put("RestoreDatabase", c -> adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(c)
          .setDatabase("escalated7308").setUrl("https://example.invalid/archive.zip").build()));
      calls.put("ImportDatabase", c -> adminStub.importDatabase(ImportDatabaseRequest.newBuilder().setCredentials(c)
          .setDatabase("escalated7308").setUrl("https://example.invalid/data.csv").build()));

      return calls;
    }

    @TestFactory
    Stream<DynamicTest> everyRestoreOrImportRpcDeniesAnAuthenticatedNonRootCaller() {
      return streamingCalls().entrySet().stream().map(entry -> DynamicTest.dynamicTest(
          entry.getKey() + " denies a non-root caller",
          () -> assertThatThrownBy(() -> drain(entry.getValue().apply(limited())))
              .isInstanceOf(StatusRuntimeException.class)
              .hasMessageContaining("PERMISSION_DENIED")));
    }

    @TestFactory
    Stream<DynamicTest> everyRestoreOrImportRpcDeniesAnInvalidPassword() {
      return streamingCalls().entrySet().stream().map(entry -> DynamicTest.dynamicTest(
          entry.getKey() + " denies an invalid password",
          () -> assertThatThrownBy(() -> drain(entry.getValue().apply(wrongPassword())))
              .isInstanceOf(StatusRuntimeException.class)
              .hasMessageContaining("UNAUTHENTICATED")));
    }

    /**
     * The denials must be denials and nothing else. {@code ImportDatabase} is the one that would show
     * a gate applied too late: it creates the database before it fetches anything, so a caller who got
     * past the check would leave an empty {@code escalated7308} behind even though the fetch failed.
     */
    @Test
    void aDeniedCallerCreatedNoDatabase() {
      streamingCalls().values().forEach(call -> {
        try {
          drain(call.apply(limited()));
        } catch (final StatusRuntimeException expected) {
          // asserted by the factories above
        }
      });

      assertThat(getServer(0).existsDatabase("escalated7308")).isFalse();
      assertThat(getServer(0).getDatabaseNames()).contains(ALLOWED_DB);
    }

    private static void drain(final Iterator<?> stream) {
      while (stream.hasNext())
        stream.next();
    }
  }

  // Regression: #7310
  /**
   * Issue #7310: the two new discovery RPCs carry different gates, and each has to be the gate its HTTP
   * counterpart applies rather than whichever was easier to reach.
   * <ul>
   *   <li>{@code GetProgress} mirrors {@code checkAuthorizationOnDatabase}: any authenticated account,
   *       narrowed to the databases it is granted. It is deliberately NOT root-only - Studio and the
   *       console poll it as the logged-in user.</li>
   *   <li>{@code ListSessions} mirrors {@code checkRootUser}: root and nobody else.</li>
   * </ul>
   * A caller with an invalid password must be refused {@code UNAUTHENTICATED} by the central
   * {@link GrpcAuthInterceptor} before either handler runs.
   */
  @Nested
  class ProgressAndSessionsAuthorization {
    private static final String ALLOWED_DB   = "allowed7310db";
    private static final String LIMITED_USER = "limited7310";
    private static final String LIMITED_PASS = "limited7310pass";

    private ManagedChannel                                            channel;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

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

    private static DatabaseCredentials root(final String password) {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(password).build();
    }

    private static DatabaseCredentials limited() {
      return DatabaseCredentials.newBuilder().setUsername(LIMITED_USER).setPassword(LIMITED_PASS).build();
    }

    private static DatabaseCredentials wrongPassword() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword("not-the-root-password").build();
    }

    private GetProgressResponse progress(final DatabaseCredentials credentials, final String database) {
      return adminStub.getProgress(
          GetProgressRequest.newBuilder().setCredentials(credentials).setDatabase(database).build());
    }

    private ListSessionsResponse sessions(final DatabaseCredentials credentials) {
      return adminStub.listSessions(ListSessionsRequest.newBuilder().setCredentials(credentials).build());
    }

    // -------------------------------------------------------------------------------------------
    // GetProgress: granted per database, not root-only
    // -------------------------------------------------------------------------------------------

    @Test
    void getProgressIsAllowedForANonRootUserOnADatabaseItIsGranted() {
      assertThatCode(() -> progress(limited(), ALLOWED_DB)).doesNotThrowAnyException();
      assertThat(progress(limited(), ALLOWED_DB).getOperationsList()).isEmpty();
    }

    @Test
    void getProgressIsDeniedForANonRootUserOnADatabaseItIsNotGranted() {
      assertThatThrownBy(() -> progress(limited(), getDatabaseName()))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("PERMISSION_DENIED");
    }

    @Test
    void getProgressWithAnInvalidPasswordIsRefusedBeforeTheHandlerRuns() {
      assertThatThrownBy(() -> progress(wrongPassword(), ALLOWED_DB))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("UNAUTHENTICATED");
    }

    // -------------------------------------------------------------------------------------------
    // ListSessions: root-only
    // -------------------------------------------------------------------------------------------

    @Test
    void listSessionsIsAllowedForRoot() {
      assertThatCode(() -> sessions(root(DEFAULT_PASSWORD_FOR_TESTS))).doesNotThrowAnyException();
    }

    @Test
    void listSessionsIsDeniedForAnAuthenticatedNonRootUser() {
      assertThatThrownBy(() -> sessions(limited()))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("PERMISSION_DENIED");
    }

    @Test
    void listSessionsWithAnInvalidPasswordIsRefusedBeforeTheHandlerRuns() {
      assertThatThrownBy(() -> sessions(wrongPassword()))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("UNAUTHENTICATED");
    }
  }
}
