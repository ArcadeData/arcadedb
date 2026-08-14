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

import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.HAServerPlugin;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A leader-only refusal names the leader wherever it is raised (issue #6183). Until now only
 * {@code graphBatchLoad} built the redirect trailers, by hand; an engine-raised refusal - a schema change the
 * replicated database rejects on a follower - reached the client as a bare retryable conflict, so a caller could
 * not tell "the leader is unknown, wait for the election" from "the leader is known and nobody told you".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6183LeaderRedirectMapperTest {

  @Test
  @DisplayName("A leader refusal maps to FAILED_PRECONDITION, not the ABORTED its NeedRetryException ancestry implies")
  void leaderRefusalIsNotRetryableAsIs() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", null), "ExecuteCommand",
        ha("localhost:50051", "localhost:2480"));

    // ABORTED would invite the caller to retry the same call against the same follower, forever.
    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
        .isEqualTo(ServerIsNotTheLeaderException.class.getName());
  }

  @Test
  @DisplayName("Both leader addresses are advertised, and the dialable one is the one the message names")
  void bothAddressesAreAdvertised() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", null), "ExecuteCommand",
        ha("db0:50051", "db0:2480"));

    final Metadata trailers = sre.getTrailers();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS)).isEqualTo("db0:50051");
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo("db0:2480");
    assertThat(sre.getStatus().getDescription())
        .startsWith("ExecuteCommand: schema changes must run on the leader")
        .contains("'db0:50051' (gRPC address)");
  }

  @Test
  @DisplayName("With no resolvable gRPC address the HTTP one is named, with the port caveat")
  void httpAddressIsTheFallback() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", null), "ExecuteCommand",
        ha(null, "db0:2480"));

    final Metadata trailers = sre.getTrailers();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS)).isNull();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo("db0:2480");
    assertThat(sre.getStatus().getDescription()).contains("'db0:2480' (HTTP address; use its gRPC port)");
  }

  @Test
  @DisplayName("A refusal raised mid-election names nothing and says so")
  void anUnknownLeaderIsReportedAsUnknown() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", null), "ExecuteCommand",
        ha(null, null));

    final Metadata trailers = sre.getTrailers();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS)).isNull();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isNull();
    assertThat(sre.getStatus().getDescription()).contains("The leader is currently unknown");
  }

  /**
   * The ambiguity guard of issue #6183 answers null rather than an address that may be this very node's. The
   * refusal must then degrade to the HTTP address, never to "the leader is unknown" - it is known, only not
   * dialable over gRPC.
   */
  @Test
  @DisplayName("A suppressed routing table degrades to the HTTP address, not to an unknown leader")
  void aSuppressedRoutingTableStillNamesTheLeader() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC)).thenReturn(null);
    when(ha.getLeaderAddress()).thenReturn("db0:2480");

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", null), "ExecuteCommand", ha);

    assertThat(sre.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo("db0:2480");
    assertThat(sre.getStatus().getDescription()).doesNotContain("currently unknown");
  }

  /**
   * The engine builds the refusal with the leader's HTTP address already in it. Where no plugin is in reach the
   * mapper still has that much to report, rather than dropping it on the floor.
   */
  @Test
  @DisplayName("Without an HA plugin the address the exception carries is still advertised")
  void theExceptionsOwnAddressIsTheLastResort() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", "db0:2480"), "ExecuteCommand");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(sre.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo("db0:2480");
  }

  /** A blank address is what "resolved to nothing" looks like coming out of the cluster; it is not an address. */
  @Test
  @DisplayName("Blank addresses are treated as absent")
  void blankAddressesAreNotAdvertised() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", "  "), "ExecuteCommand",
        ha("  ", ""));

    final Metadata trailers = sre.getTrailers();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS)).isNull();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isNull();
    assertThat(sre.getStatus().getDescription()).contains("The leader is currently unknown");
  }

  private static HAServerPlugin ha(final String grpcWriter, final String httpLeader) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC)).thenReturn(
        new HAServerPlugin.RoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC, grpcWriter, List.of()));
    when(ha.getLeaderAddress()).thenReturn(httpLeader);
    return ha;
  }
}
