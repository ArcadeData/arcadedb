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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7511 on the gRPC transport: a group or API-token change refused because a peer cannot decode the entry it
 * would be replicated as reaches the client as {@code FAILED_PRECONDITION}, not {@code INTERNAL}.
 * <p>
 * The distinction is the client's to act on. {@code INTERNAL} says the server broke and tells a generated client
 * nothing about whether to retry; {@code FAILED_PRECONDITION} says the request was refused because the SYSTEM is
 * not in a state where it can succeed, which is exactly true here and clears itself when the last node is
 * upgraded. It is the status HTTP's {@code 409} is paired with everywhere else in this service.
 * <p>
 * Driven through {@code toStatus}, the mapper every admin RPC's {@code respond} wrapper delegates to, so
 * {@code SaveGroup}, {@code DeleteGroup}, {@code CreateApiToken} and {@code DeleteApiToken} are all covered by the
 * one arm rather than four tests of the same line.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7511GrpcClusterNotReadyStatusTest {

  @Test
  void aClusterNotReadyRefusalIsAPreconditionFailureRatherThanAnInternalError() {
    final StatusException mapped = adminService().toStatus("saveGroup", new ClusterCapabilityNotReadyException(
        "Refusing to replicate the group document: peer(s) [arcadedb2] have not advertised the "
            + "'security-groups-entry' capability"));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(mapped.getStatus().getDescription())
        .as("the peer holding the cluster back is the only actionable half, so it must survive the mapping")
        .contains("arcadedb2");
  }

  @Test
  void theSameArmAnswersATokenRevocationRefusal() {
    final StatusException mapped = adminService().toStatus("deleteApiToken", new ClusterCapabilityNotReadyException(
        "Refusing to replicate the API-token document: peer(s) [arcadedb2] have not advertised the "
            + "'security-api-tokens-entry' capability"));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  private static ArcadeDbGrpcAdminService adminService() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    return new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator());
  }
}
