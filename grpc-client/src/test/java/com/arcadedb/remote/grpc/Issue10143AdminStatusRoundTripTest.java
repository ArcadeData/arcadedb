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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.remote.RemoteAlreadyExistsException;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.DatabaseAlreadyExistsException;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.grpc.AdminStatusTestBridge;
import com.arcadedb.server.grpc.ArcadeDbGrpcAdminService;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.UserAlreadyExistsException;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10143: the admin service built every classified answer as a bare status, with no trailer, so its two deliberate
 * {@code UNAVAILABLE} answers - the {@code NeedRetryException} arm (a quorum that could not be reached: nothing was
 * committed) and {@code ConnectCluster}'s unseeded join (re-running the call is the fix) - were indistinguishable on the
 * client from a lost connection. Every admin RPC that changes server state maps through {@code toAutoCommitWriteException},
 * which reads a status-only {@code UNAVAILABLE} as "may already have been applied, not retryable": the opposite of what the
 * server said.
 * <p>
 * Nothing tested the pair. The server tests pinned the status, the client tests pinned the mapping of hand-built statuses,
 * and a client rule keyed on a trailer was satisfied by a server that sent none. This test takes the status the server
 * BUILDS for every arm of {@code toStatus} and asks the client mapper what it rebuilds from it, so an arm added later has
 * to declare both halves.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue10143AdminStatusRoundTripTest {
  @TempDir
  Path root;

  /** For every classified admin refusal, the type the client must rebuild from the server's answer to it on a write. */
  private static Map<Exception, Class<? extends Throwable>> clientTypes() {
    final Map<Exception, Class<? extends Throwable>> types = new LinkedHashMap<>();
    types.put(AdminStatusTestBridge.adminAuthorizationRefusal("Only root can do that"), SecurityException.class);
    types.put(new ServerControlPlane.RestoreImportUrlNotAllowedException("URL scheme 'file' is not allowed"), SecurityException.class);
    types.put(new SecurityException("Invalid credentials"), RemoteException.class);
    types.put(new ServerSecurityException("Password must be at least 8 characters"), SecurityException.class);
    types.put(new ServerControlPlane.NotFoundException("User 'ghost' not found"), RecordNotFoundException.class);
    types.put(new ServerControlPlane.AlreadyExistsException("API token 'ci' already issued"), RemoteAlreadyExistsException.class);
    types.put(new UserAlreadyExistsException("bob"), RemoteAlreadyExistsException.class);
    types.put(new DatabaseAlreadyExistsException("Database 'shop' already exists"), RemoteAlreadyExistsException.class);
    types.put(new DatabaseOperationInProgressException("A backup of database 'shop' is already running"),
        ConcurrentModificationException.class);
    types.put(new IllegalArgumentException("Database name is empty"), RemoteException.class);
    types.put(new ServerControlPlane.OperationNotAvailableException("HA is not enabled on this server"), RemoteException.class);
    // The arms this issue is about: retryable, never "may already have been applied"
    types.put(new NeedRetryException("Resync in progress"), NeedRetryException.class);
    types.put(new QuorumNotReachedException("Quorum 2 not reached on database 'db'"), NeedRetryException.class);
    types.put(new ConcurrentModificationException("Concurrent modification on page 3"), ConcurrentModificationException.class);
    return types;
  }

  @Test
  void everyAdminArmIsRebuiltAsTheTypeTheServerMeant() {
    final ArcadeDbGrpcAdminService service = adminService();
    for (final Map.Entry<Exception, Class<? extends Throwable>> refusal : clientTypes().entrySet()) {
      final String name = refusal.getKey().getClass().getSimpleName();
      final StatusException wire = AdminStatusTestBridge.toStatus(service, "createUser", refusal.getKey());

      final RuntimeException write = GrpcClientErrorMapper.toAdminException(wire, "create user", true);
      assertThat(write).as("%s on a write", name).isInstanceOf(refusal.getValue());
      assertThat(write.getMessage()).as("%s keeps the server's sentence", name).contains(refusal.getKey().getMessage());

      // and on a read, where a lost answer is retryable anyway, the type is the same
      assertThat(GrpcClientErrorMapper.toAdminException(wire, "get user", false)).as("%s on a read", name)
          .isInstanceOf(refusal.getValue());
    }
  }

  @Test
  void aQuorumThatCouldNotBeReachedIsNotAnUnknownOutcome() {
    final StatusException wire = AdminStatusTestBridge.toStatus(adminService(), "saveGroup", new NeedRetryException("try again"));

    assertThat(wire.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(GrpcClientErrorMapper.outcomeUnknown(wire)).as("the server answered, so it applied nothing").isFalse();
    final RuntimeException mapped = GrpcClientErrorMapper.toAutoCommitWriteException(wire, "save group");
    assertThat(mapped).isInstanceOf(NeedRetryException.class);
    assertThat(mapped.getMessage()).doesNotContain("may already have applied");
  }

  @Test
  void aRetryableSubtypeThisClientDoesNotKnowIsStillRetryable() {
    // a later server may name a NeedRetryException subtype this client has no case for: the status decides, not the name
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, "com.arcadedb.network.binary.SomeFutureRetryableException");
    final StatusException wire = Status.UNAVAILABLE.withDescription("try later").asException(trailers);

    final RuntimeException mapped = GrpcClientErrorMapper.toAdminException(wire, "create user", true);
    assertThat(mapped).isInstanceOf(NeedRetryException.class).hasMessage("try later");
  }

  @Test
  void anUnseededJoinIsRetryableAsTheConnectClusterJavadocSays() {
    final StatusException wire = AdminStatusTestBridge.seedFailureStatus(
        new ServerControlPlane.ConnectClusterResult("db2:2435", List.of("users", "API tokens")));

    assertThat(wire.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    final RuntimeException mapped = GrpcClientErrorMapper.toAdminException(wire, "connect cluster", true);
    assertThat(mapped).isInstanceOf(NeedRetryException.class);
    assertThat(mapped.getMessage()).contains("db2:2435", "users", "API tokens").doesNotContain("may already have applied");
  }

  @Test
  void aStatusOnlyUnavailableIsStillAnUnknownOutcome() {
    // the other half of the line: what a lost connection looks like must keep reading as one
    final StatusException lost = Status.UNAVAILABLE.withDescription("Connection reset").asException();

    assertThat(GrpcClientErrorMapper.toAdminException(lost, "create user", true)).isInstanceOf(RemoteException.class)
        .isNotInstanceOf(NeedRetryException.class).hasMessageContaining("may already have applied");
  }

  @Test
  void anUnexpectedFaultStaysARemoteException() {
    final StatusException wire = AdminStatusTestBridge.toStatus(adminService(), "createUser", new RuntimeException("boom"));

    assertThat(wire.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    assertThat(GrpcClientErrorMapper.toAdminException(wire, "create user", true)).isInstanceOf(RemoteException.class);
  }

  @Test
  void theTestCoversEveryArmOfTheLadder() {
    // A new arm added to the table without a client type here fails this, so it has to declare both halves
    final List<Class<?>> covered = clientTypes().keySet().stream().<Class<?>>map(Exception::getClass).toList();
    for (final Class<? extends Throwable> armType : AdminStatusTestBridge.refusalArmTypes())
      assertThat(covered).as("a client type for the %s arm", armType.getSimpleName()).contains(armType);
  }

  private ArcadeDbGrpcAdminService adminService() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, new ContextConfiguration());
    return new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator());
  }
}
