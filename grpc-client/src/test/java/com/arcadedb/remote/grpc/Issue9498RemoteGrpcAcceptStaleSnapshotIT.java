/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9498: {@link RemoteGrpcServer#acceptStaleSnapshot()} reaches the {@code AcceptStaleSnapshot} RPC, and the server's
 * refusal comes back through the shared client error mapper. The lift itself on a one-voter cluster is covered by
 * {@code Issue9498GrpcAcceptStaleSnapshotIT} in grpcw.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9498RemoteGrpcAcceptStaleSnapshotIT extends BaseGrpcClientServerTest {

  private RemoteGrpcServer server;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void connect() {
    server = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
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
  void withoutHaTheRefusalReachesTheClient() {
    assertThatThrownBy(() -> server.acceptStaleSnapshot())
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("High Availability");
  }
}
