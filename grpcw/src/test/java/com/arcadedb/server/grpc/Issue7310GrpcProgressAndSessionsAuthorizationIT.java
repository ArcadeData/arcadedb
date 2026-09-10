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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
public class Issue7310GrpcProgressAndSessionsAuthorizationIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT    = 50051;
  private static final String ALLOWED_DB   = "allowed7310db";
  private static final String LIMITED_USER = "limited7310";
  private static final String LIMITED_PASS = "limited7310pass";

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
