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
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7400: {@link RemoteGrpcServer} had {@code disconnectCluster()} and no {@code connectCluster},
 * so the client half of the pair was as one-sided as the service half. An RPC no client can call is
 * one nobody can use, which is why #7304 drove every service method from here too.
 * <p>
 * What is proved is that the proto, the service and the client agree over a real channel - and that
 * the server's own refusal survives the trip. The current HA stack does not implement the verb on
 * either transport (issue #7401); parity of the answer is the subject here, not a working join.
 */
class Issue7400RemoteGrpcConnectClusterIT extends BaseGraphServerTest {

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

  /**
   * The type matters as much as the message, for the same reason it does on
   * {@code Issue7304RemoteGrpcServerControlPlaneIT.disconnectClusterWithoutHaIsReportedThroughTheSharedErrorMapper}:
   * admin failures go through {@code GrpcClientErrorMapper}, so a follower's leader refusal keeps the
   * leader's address from the trailers instead of being flattened into a rendered string. The
   * assertion on the server's own wording is what proves the client did not wrap it in a bare
   * "Failed to connect cluster".
   */
  @Test
  void connectClusterIsReportedThroughTheSharedErrorMapper() {
    assertThatThrownBy(() -> server.connectCluster("localhost:2425"))
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("not supported by the current HA implementation");
  }

  /**
   * Both halves of the pair are callable from the client and both surface the server's refusal.
   * Driving them together is the assertion the issue asked for: connect alone would pass against a
   * client that had lost disconnect.
   */
  @Test
  void theClusterPairIsCallableFromTheClient() {
    assertThatThrownBy(() -> server.connectCluster("localhost:2425"))
        .isInstanceOf(RemoteException.class);

    assertThatThrownBy(() -> server.disconnectCluster())
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("High Availability");
  }
}
