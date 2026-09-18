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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7532 (absorbing #7550) on the gRPC transport: the status a residual seed failure reaches the client as.
 * <p>
 * The HTTP add-peer route has answered 503 for a joined-but-unseeded peer since issue #7521. The gRPC
 * {@code ConnectCluster} RPC answered OK and left the failure in a SEVERE log line, so an operator joining a
 * peer over gRPC had nothing their automation could branch on. {@code UNAVAILABLE} is this transport's 503 and,
 * as there, re-issuing the call is idempotent on the membership change and reissues the seed.
 * <p>
 * The second half is the {@code toStatus} gap the issue names: a {@link NeedRetryException} - concretely a
 * {@link QuorumNotReachedException} out of any admin operation that has to commit a Raft entry - had no arm of
 * its own and came out {@code INTERNAL}, telling a client the server broke rather than to send the same request
 * again. HTTP answers every one of them 503.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7532ConnectClusterSeedFailureStatusTest {

  @Test
  void aQuorumThatCouldNotBeReachedIsRetryableRatherThanAnInternalError() {
    final StatusException mapped = adminService().toStatus("createUser",
        new QuorumNotReachedException("Quorum 2 not reached on database 'db'"));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(mapped.getStatus().getDescription()).contains("Quorum 2 not reached");
  }

  /**
   * The arm is on the supertype, so every retryable engine failure - a concurrent modification, a lock that
   * timed out, a replication queue that filled - reaches the client as the same retryable status rather than
   * three different ones.
   */
  @Test
  void theArmIsOnTheSupertypeSoEveryRetryableFailureAgrees() {
    assertThat(adminService().toStatus("saveGroup", new NeedRetryException("try again")).getStatus().getCode())
        .isEqualTo(Status.Code.UNAVAILABLE);
  }

  /**
   * Order: {@code ServerIsNotTheLeaderException} also extends {@link NeedRetryException}, and it must keep the
   * arm further up that gives it the {@code LeaderRedirectProtocol} trailers a client redirects itself with.
   * A new supertype arm placed above it would have swallowed that silently - the status would still look
   * retryable and the redirect would simply be gone.
   */
  @Test
  void theNotLeaderRefusalKeepsItsOwnArmDespiteExtendingTheSupertype() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getHA()).thenReturn(null);

    final StatusException mapped = new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator())
        .toStatus("createDatabase",
            new com.arcadedb.network.binary.ServerIsNotTheLeaderException("Not the leader", "db2:2480"));

    assertThat(mapped.getStatus().getCode())
        .as("routed through GrpcErrorMapper, not through the new NeedRetryException arm")
        .isNotEqualTo(Status.Code.UNAVAILABLE);
  }

  /**
   * The shared control-plane result the RPC reads. A clean join carries nothing to report; a residual failure
   * carries the document names and the remediation, and both halves have to survive into the status
   * description because a gRPC client has no response body to read them from.
   */
  @Test
  void theResultTheRpcReportsFromCarriesBothHalvesOfTheMessage() {
    final ServerControlPlane.ConnectClusterResult clean =
        new ServerControlPlane.ConnectClusterResult("db2:2435", List.of());
    assertThat(clean.hasFailedSeeds()).isFalse();

    final ServerControlPlane.ConnectClusterResult failed =
        new ServerControlPlane.ConnectClusterResult("db2:2435", List.of("users", "API tokens"));
    assertThat(failed.hasFailedSeeds()).isTrue();

    final String description = failed.errorMessage() + " " + failed.detailMessage();
    assertThat(description).contains("db2:2435", "users", "API tokens", "connect cluster");
  }

  private static ArcadeDbGrpcAdminService adminService() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    return new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator());
  }
}
