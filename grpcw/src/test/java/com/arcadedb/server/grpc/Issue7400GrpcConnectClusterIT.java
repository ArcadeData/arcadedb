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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7400: {@code ArcadeDbAdminService} shipped {@code DisconnectCluster} with #7304 and not its
 * pair-mate, so a gRPC-only client could take a server out of a cluster and then had to reopen the
 * HTTP port to attempt the other half.
 * <p>
 * What is asserted here is protocol <em>parity</em>, not a working join. The shared implementation
 * {@code ServerControlPlane.connectCluster} refuses unconditionally - the current HA stack has never
 * implemented the verb - and {@code PostServerCommandHandler} has always surfaced that refusal on
 * HTTP. The gRPC caller must now receive the same refusal from the same method, rather than
 * {@code UNIMPLEMENTED} for a verb the other transport accepts. Making the verb actually join a
 * cluster is issue #7401.
 * <p>
 * The pair is driven together throughout, as the issue asks: a test that called connect alone would
 * pass against a service that had lost disconnect.
 */
public class Issue7400GrpcConnectClusterIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT    = 50051;
  private static final String LIMITED_USER = "limited7400";
  private static final String LIMITED_PASS = "limited7400pass";
  private static final String PEER_ADDRESS = "localhost:2425";

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

    if (!security.existsUser(LIMITED_USER)) {
      final JSONObject config = new JSONObject();
      config.put("name", LIMITED_USER);
      config.put("password", security.encodePassword(LIMITED_PASS));
      config.put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")));
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
   * description is the shared implementation's own message, not a rendering invented here.
   */
  @Test
  void connectClusterIsRefusedByTheSharedImplementation() {
    assertThatThrownBy(() -> connect(root(), PEER_ADDRESS))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("FAILED_PRECONDITION")
        .hasMessageContaining("not supported by the current HA implementation");
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
   * HTTP's {@code extractTarget} yields {@code ""} for a bare {@code connect cluster}, and the shared
   * implementation refuses before it looks at the argument. The RPC must not invent an
   * {@code INVALID_ARGUMENT} gate the HTTP verb does not have, or the two transports disagree on the
   * same input - which is the drift this issue is about.
   */
  @Test
  void connectClusterWithAnEmptyAddressIsRefusedTheSameWay() {
    assertThatThrownBy(() -> connect(root(), ""))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("FAILED_PRECONDITION")
        .hasMessageContaining("not supported by the current HA implementation");
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
