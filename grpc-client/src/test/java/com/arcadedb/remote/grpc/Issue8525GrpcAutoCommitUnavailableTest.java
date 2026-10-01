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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.ExecuteCommandRequest;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #8525: a status-only {@code UNAVAILABLE} on an RPC that commits a write on its own may follow a request the
 * server already applied, so it must not surface as a {@link NeedRetryException}. A failure to connect proves the
 * request never left the client and keeps the retryable mapping, on the commit path too. No server is required.
 */
class Issue8525GrpcAutoCommitUnavailableTest {

  @Test
  void lostResponseOnAnAutoCommittedWriteIsAnUnknownOutcome() {
    final RuntimeException e = GrpcClientErrorMapper.toAutoCommitWriteException(
        Status.UNAVAILABLE.withDescription("connection reset").asRuntimeException(), "ExecuteCommand");

    assertThat(e).isInstanceOf(RemoteException.class).isNotInstanceOf(NeedRetryException.class);
    assertThat(e.getMessage()).contains("ExecuteCommand").contains("may already have applied it").contains("connection reset");
  }

  @Test
  void refusedConnectionOnAnAutoCommittedWriteStaysRetryable() {
    final RuntimeException e = GrpcClientErrorMapper.toAutoCommitWriteException(
        Status.UNAVAILABLE.withDescription("io exception").withCause(new ConnectException("Connection refused")).asRuntimeException(),
        "ExecuteCommand");

    assertThat(e).isInstanceOf(NeedRetryException.class);
  }

  @Test
  void unresolvedHostOnAnAutoCommittedWriteStaysRetryable() {
    final RuntimeException e = GrpcClientErrorMapper.toAutoCommitWriteException(
        Status.UNAVAILABLE.withCause(new UnknownHostException("nowhere")).asRuntimeException(), "CreateRecord");

    assertThat(e).isInstanceOf(NeedRetryException.class);
  }

  @Test
  void refusedConnectionOnCommitStaysRetryable() {
    final RuntimeException e = GrpcClientErrorMapper.toCommitException(
        Status.UNAVAILABLE.withCause(new ConnectException("Connection refused")).asRuntimeException());

    assertThat(e).isInstanceOf(NeedRetryException.class).isNotInstanceOf(TransactionException.class);
  }

  @Test
  void aServerClassifiedUnavailableOnAnAutoCommittedWriteKeepsItsType() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, "com.arcadedb.exception.NeedRetryException");
    final RuntimeException e = GrpcClientErrorMapper.toAutoCommitWriteException(
        Status.UNAVAILABLE.withDescription("election in progress").asRuntimeException(trailers), "ExecuteCommand");

    assertThat(e).isInstanceOf(NeedRetryException.class);
  }

  @Test
  void otherStatusesOnAnAutoCommittedWriteKeepTheirMapping() {
    assertThat(GrpcClientErrorMapper.toAutoCommitWriteException(Status.ABORTED.asRuntimeException(), "UpdateRecord"))
        .isInstanceOf(ConcurrentModificationException.class);
  }

  /**
   * The carve-out above is only worth anything if a real refused connection carries the cause it looks for: gRPC
   * reports it as {@code UNAVAILABLE} whatever happened, so the cause chain is the only evidence the request never
   * left the client. Nothing listens on the allocated port.
   */
  @Test
  void aRealRefusedConnectionIsRecognisedAsNeverSent() throws InterruptedException {
    final int port = StaticBaseServerTest.allocateFreePorts(1)[0];
    final ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    try {
      final StatusRuntimeException e = catchThrowableOfType(StatusRuntimeException.class,
          () -> ArcadeDbServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS)
              .executeCommand(ExecuteCommandRequest.newBuilder().setDatabase("none").setCommand("SELECT 1").build()));

      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
      assertThat(GrpcClientErrorMapper.provablyNeverSent(e)).isTrue();
      assertThat(GrpcClientErrorMapper.toAutoCommitWriteException(e, "ExecuteCommand")).isInstanceOf(NeedRetryException.class);
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
