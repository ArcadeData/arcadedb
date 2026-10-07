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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.remote.RemoteException;
import io.grpc.Metadata;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8822: the unknown-outcome mapping of #8525 extended to the streaming writes, and to a client deadline that
 * expires on a write that commits on its own. No server is required.
 */
class Issue8822GrpcUnknownOutcomeMappingTest {

  @Test
  void lostResponseOnAStreamingWriteSaysItMayBePartiallyApplied() {
    final RuntimeException e = GrpcClientErrorMapper.toStreamingWriteException(
        Status.UNAVAILABLE.withDescription("connection reset").asRuntimeException(), "InsertStream");

    assertThat(e).isInstanceOf(RemoteException.class).isNotInstanceOf(NeedRetryException.class);
    assertThat(e.getMessage()).contains("InsertStream").contains("may already have applied").contains("partially")
        .contains("connection reset");
  }

  @Test
  void refusedConnectionOnAStreamingWriteStaysRetryable() {
    final RuntimeException e = GrpcClientErrorMapper.toStreamingWriteException(
        Status.UNAVAILABLE.withCause(new ConnectException("Connection refused")).asRuntimeException(), "InsertStream");

    assertThat(e).isInstanceOf(NeedRetryException.class);
  }

  @Test
  void aServerClassifiedErrorOnAStreamingWriteKeepsItsType() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, "com.arcadedb.exception.NeedRetryException");
    assertThat(GrpcClientErrorMapper.toStreamingWriteException(
        Status.UNAVAILABLE.withDescription("election in progress").asRuntimeException(trailers), "InsertStream"))
        .isInstanceOf(NeedRetryException.class);

    assertThat(GrpcClientErrorMapper.toStreamingWriteException(Status.ABORTED.asRuntimeException(), "InsertStream"))
        .isInstanceOf(ConcurrentModificationException.class);
  }

  /**
   * A client deadline expiring on a self-committing write keeps its {@link TimeoutException} type, which nothing
   * replays, but the message must now say that the write may have landed (raised in the review of #8823).
   */
  @Test
  void deadlineOnAnAutoCommittedWriteSaysTheOutcomeIsUnknown() {
    final RuntimeException e = GrpcClientErrorMapper.toAutoCommitWriteException(
        Status.DEADLINE_EXCEEDED.withDescription("deadline exceeded after 30s").asRuntimeException(), "ExecuteCommand");

    assertThat(e).isInstanceOf(TimeoutException.class);
    assertThat(e.getMessage()).contains("ExecuteCommand").contains("may already have applied").contains("deadline exceeded after 30s");
    assertThat(e.getCause()).isNotNull();
  }

  @Test
  void deadlineOnAStreamingWriteSaysItMayBePartiallyApplied() {
    final RuntimeException e = GrpcClientErrorMapper.toStreamingWriteException(
        Status.DEADLINE_EXCEEDED.asRuntimeException(), "TimeSeriesWriteStream");

    assertThat(e).isInstanceOf(TimeoutException.class);
    assertThat(e.getMessage()).contains("TimeSeriesWriteStream").contains("may already have applied").contains("partially");
  }

  @Test
  void deadlineOnCommitSaysTheTransactionMayHaveBeenCommitted() {
    final RuntimeException e = GrpcClientErrorMapper.toCommitException(Status.DEADLINE_EXCEEDED.asRuntimeException());

    assertThat(e).isInstanceOf(TimeoutException.class).isNotInstanceOf(TransactionException.class);
    assertThat(e.getMessage()).contains("may have been committed");
  }

  /** A server-classified timeout (a class-name trailer) was raised by the server before it applied anything. */
  @Test
  void aServerClassifiedTimeoutKeepsItsOwnMessage() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, "com.arcadedb.exception.TimeoutException");
    final RuntimeException e = GrpcClientErrorMapper.toAutoCommitWriteException(
        Status.DEADLINE_EXCEEDED.withDescription("lock timeout").asRuntimeException(trailers), "ExecuteCommand");

    assertThat(e).isInstanceOf(TimeoutException.class);
    assertThat(e.getMessage()).isEqualTo("lock timeout");
  }

  /** Reads keep the plain mapping: a lost response on a read is retryable and a timeout says only that. */
  @Test
  void readsKeepThePlainMapping() {
    assertThat(GrpcClientErrorMapper.toException(Status.UNAVAILABLE.asRuntimeException())).isInstanceOf(NeedRetryException.class);
    assertThat(GrpcClientErrorMapper.toException(Status.DEADLINE_EXCEEDED.withDescription("slow").asRuntimeException()))
        .isInstanceOf(TimeoutException.class).hasMessage("slow");
  }
}
