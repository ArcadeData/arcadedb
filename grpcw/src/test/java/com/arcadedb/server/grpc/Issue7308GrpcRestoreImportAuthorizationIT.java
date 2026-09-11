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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7308: the three restore/import RPCs are the most destructive operations on the control
 * plane - each of them creates or replaces a whole database - so each has to enforce the same
 * root-user gate {@code PostServerCommandHandler} enforces through {@code checkRootUser}, and to
 * enforce it <i>before</i> doing anything.
 * <p>
 * The sibling of {@code Issue7304GrpcControlPlaneAuthorizationIT} for the RPCs that stream. They
 * need their own class because a server-streaming call reports nothing until its iterator is read:
 * the refusal arrives when the stream is drained, not when the stub method returns, so the table's
 * entries have to consume the stream to observe it.
 */
public class Issue7308GrpcRestoreImportAuthorizationIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT    = 50051;
  private static final String ALLOWED_DB   = "allowed7308db";
  private static final String LIMITED_USER = "limited7308";
  private static final String LIMITED_PASS = "limited7308pass";

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
