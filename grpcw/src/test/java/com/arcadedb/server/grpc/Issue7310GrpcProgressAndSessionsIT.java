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
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.security.ServerSecurityUser;
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
 * Issue #7310: the two discovery routes the HTTP control plane kept to itself after #7304 -
 * {@code GET /api/v1/progress/{database}} and {@code GET /api/v1/sessions} - now have RPCs on
 * {@code ArcadeDbAdminService}.
 * <p>
 * Each RPC is driven through a real channel rather than through {@code ServerControlPlane} directly: a
 * test against the shared implementation would pass against a proto whose RPC was never wired into the
 * service. Authorization is the subject of {@link Issue7310GrpcProgressAndSessionsAuthorizationIT}; this
 * class runs as {@code root}.
 */
public class Issue7310GrpcProgressAndSessionsIT extends BaseGraphServerTest {

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
