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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.remote.RemoteAlreadyExistsException;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10075: the server answers a taken user, API token or database name with gRPC {@code ALREADY_EXISTS} and no
 * exception-class trailer. The client's generic mapping reads a trailer-less {@code ALREADY_EXISTS} as the data plane's
 * duplicate-key answer, so a taken user name came back as a {@link DuplicatedKeyException} naming an index that does not
 * exist. {@link RemoteGrpcServer}'s admin calls now raise {@link RemoteAlreadyExistsException} instead, the same type
 * the HTTP client raises for the same conflict.
 */
class Issue10075GrpcAlreadyExistsIT extends BaseGrpcClientServerTest {

  private static final String PASSWORD = "a-long-password";

  private RemoteGrpcServer server;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    server = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterEach
  @Override
  public void endTest() {
    if (server != null) {
      server.close();
      server = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aTakenUserNameIsATypedAlreadyExistsException() {
    server.createUser("grpc_dup_user", PASSWORD);

    assertThatThrownBy(() -> server.createUser("grpc_dup_user", PASSWORD))
        .isInstanceOf(RemoteAlreadyExistsException.class)
        .isInstanceOf(RemoteException.class)
        .isNotInstanceOf(DuplicatedKeyException.class)
        .hasMessageContaining("already exists")
        .hasMessageContaining("grpc_dup_user");
  }

  @Test
  void aTakenApiTokenNameIsATypedAlreadyExistsException() {
    server.createApiToken("grpc_dup_token", getDatabaseName(), 0, new JSONObject());

    assertThatThrownBy(() -> server.createApiToken("grpc_dup_token", getDatabaseName(), 0, new JSONObject()))
        .isInstanceOf(RemoteAlreadyExistsException.class)
        .isNotInstanceOf(DuplicatedKeyException.class)
        .hasMessageContaining("already exists")
        .hasMessageContaining("grpc_dup_token");
  }

  @Test
  void aTakenDatabaseNameIsATypedAlreadyExistsExceptionThatKeepsItsStatusInTheMessage() {
    final String name = "grpc_dup_db_10075";
    assertThat(server.createDatabaseIfMissing(name)).isTrue();
    try {
      assertThatThrownBy(() -> server.createDatabase(name))
          .isInstanceOf(RemoteAlreadyExistsException.class)
          .isNotInstanceOf(DuplicatedKeyException.class)
          // callers already match on the status name in this message (RemoteGrpcServerIT)
          .hasMessageContaining("ALREADY_EXISTS")
          .hasMessageContaining(name);
    } finally {
      assertThat(server.dropDatabaseIfExists(name)).isTrue();
    }
  }

  @Test
  void aNewNameIsStillCreated() {
    server.createUser("grpc_fresh_user", PASSWORD);

    assertThat(server.listUsers()).anyMatch(user -> "grpc_fresh_user".equals(user.getName()));
  }
}
