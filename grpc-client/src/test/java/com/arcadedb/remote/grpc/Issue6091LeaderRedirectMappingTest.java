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

import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.grpc.LeaderRedirectProtocol;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A follower that refuses work only the leader may run says where to go instead, and it says it in a form a
 * client can act on rather than only in prose (issue #6091). This covers the client half: the refusal comes back
 * as the same {@code ServerIsNotTheLeaderException} the HTTP protocol raises for the same situation, carrying the
 * address to redirect to, so a caller switches on a type and reads a field instead of pattern-matching an error
 * message that was never a contract.
 * <p>
 * No server is required.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6091LeaderRedirectMappingTest {

  private static StatusRuntimeException refusal(final String grpcAddress, final String httpAddress) {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, ServerIsNotTheLeaderException.class.getName());
    if (grpcAddress != null)
      trailers.put(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS, grpcAddress);
    if (httpAddress != null)
      trailers.put(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS, httpAddress);
    return Status.FAILED_PRECONDITION.withDescription("this server is not the cluster leader")
        .asRuntimeException(trailers);
  }

  @Test
  @DisplayName("the gRPC address wins: it is the only one of the two this client can dial")
  void grpcAddressIsPreferredOverTheHttpOne() {
    final RuntimeException rebuilt = GrpcClientErrorMapper.toException(
        refusal("leader.example.com:50051", "leader.example.com:2480"));

    assertThat(rebuilt).isInstanceOf(ServerIsNotTheLeaderException.class)
        .isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not the cluster leader");
    assertThat(((ServerIsNotTheLeaderException) rebuilt).getLeaderAddress()).isEqualTo("leader.example.com:50051");
  }

  @Test
  @DisplayName("with no gRPC address resolvable the HTTP one is still better than nothing")
  void fallsBackToTheHttpAddressWhenNoGrpcAddressIsAdvertised() {
    final RuntimeException rebuilt = GrpcClientErrorMapper.toException(refusal(null, "leader.example.com:2480"));

    assertThat(((ServerIsNotTheLeaderException) rebuilt).getLeaderAddress()).isEqualTo("leader.example.com:2480");
  }

  @Test
  @DisplayName("a refusal issued mid-election names no leader, and that is not an error either")
  void leaderAddressIsNullWhenTheClusterKnowsOfNone() {
    final RuntimeException rebuilt = GrpcClientErrorMapper.toException(refusal(null, null));

    assertThat(rebuilt).isInstanceOf(ServerIsNotTheLeaderException.class);
    assertThat(((ServerIsNotTheLeaderException) rebuilt).getLeaderAddress()).isNull();
  }

  /**
   * The exception class trailer is what selects the type. Without it - an older server - the FAILED_PRECONDITION
   * status has no mapping of its own and the caller gets the generic remote error, exactly as before this change:
   * a leader-address trailer alone must not start inventing types.
   */
  @Test
  @DisplayName("no exception-class trailer: unchanged legacy mapping, no type invented from the address alone")
  void addressTrailerAloneDoesNotChangeTheMappedType() {
    final Metadata trailers = new Metadata();
    trailers.put(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS, "leader.example.com:50051");
    final RuntimeException rebuilt = GrpcClientErrorMapper.toException(
        Status.FAILED_PRECONDITION.withDescription("nope").asRuntimeException(trailers));

    assertThat(rebuilt).isInstanceOf(RemoteException.class).isNotInstanceOf(ServerIsNotTheLeaderException.class);
  }
}
