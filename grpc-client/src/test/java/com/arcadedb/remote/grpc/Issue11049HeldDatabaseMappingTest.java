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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.server.grpc.GrpcErrorMapper;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11049, without a server: the data plane answers a database it has and does not serve now (closed by
 * {@code close database}, held by a restore, being dropped) {@code UNAVAILABLE}, which the client's status row reads as a
 * retryable {@link NeedRetryException}, so {@code transaction()} spun its whole retry budget against a database that stays
 * closed until an operator opens it. Every answer here is the one the server's {@link GrpcErrorMapper} BUILDS, so a change
 * to either half that breaks the pair fails.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11049HeldDatabaseMappingTest {

  @Test
  void aHeldDatabaseIsRebuiltWithItsReasonOnEveryPath() {
    for (final DatabaseNotAvailableException.Reason reason : DatabaseNotAvailableException.Reason.values())
      for (final boolean conceal : new boolean[] { false, true }) {
        final StatusRuntimeException wire = GrpcErrorMapper.toStatusRuntimeException(
            new DatabaseNotAvailableException("Database 'held' is not available", reason), "Query execution failed", null, conceal);
        assertThat(wire.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);

        for (final RuntimeException rebuilt : new RuntimeException[] { GrpcClientErrorMapper.toException(wire),
            GrpcClientErrorMapper.toAutoCommitWriteException(wire, "executeCommand"), GrpcClientErrorMapper.toCommitException(wire),
            GrpcClientErrorMapper.toAdminException(wire, "backup database", true) })
          assertThat(rebuilt).as("reason=%s conceal=%s", reason, conceal).isExactlyInstanceOf(DatabaseNotAvailableException.class)
              .isNotInstanceOf(NeedRetryException.class)
              .satisfies(e -> assertThat(((DatabaseNotAvailableException) e).getReason()).isEqualTo(reason));
      }
  }

  /** Wrapped by the engine, the reason is still the one the frame that classified it carries. */
  @Test
  void aWrappedHeldDatabaseKeepsItsReason() {
    final StatusRuntimeException wire = GrpcErrorMapper.toStatusRuntimeException(new CommandExecutionException("Cannot run",
        new DatabaseNotAvailableException("Database 'held' is being restored", DatabaseNotAvailableException.Reason.RESTORING)), null);

    assertThat(wire.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(GrpcClientErrorMapper.toException(wire)).isExactlyInstanceOf(DatabaseNotAvailableException.class)
        .satisfies(e -> assertThat(((DatabaseNotAvailableException) e).getReason()).isEqualTo(
            DatabaseNotAvailableException.Reason.RESTORING));
  }

  /** A handle closed under the request: the engine classifies it like the held database, and it comes back as itself. */
  @Test
  void aHandleClosedUnderTheRequestIsRebuiltAsItself() {
    final StatusRuntimeException wire = GrpcErrorMapper.toStatusRuntimeException(new DatabaseIsClosedException("Database is closed"),
        null);

    assertThat(wire.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(wire.getTrailers().get(GrpcClientErrorMapper.UNAVAILABLE_REASON_KEY)).isEqualTo(
        DatabaseNotAvailableException.Reason.UNAVAILABLE.name());
    assertThat(GrpcClientErrorMapper.toException(wire)).isExactlyInstanceOf(DatabaseIsClosedException.class)
        .isNotInstanceOf(NeedRetryException.class);
  }

  /**
   * A held-database type a newer server names and this client does not know: the reason trailer carries the verdict, so it is
   * not read as the retryable status row.
   */
  @Test
  void anUnknownClassWithAReasonIsStillAHeldDatabase() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, "com.arcadedb.exception.DatabaseIsHibernatingException");
    trailers.put(GrpcClientErrorMapper.UNAVAILABLE_REASON_KEY, "HIBERNATING");

    assertThat(GrpcClientErrorMapper.toException(Status.UNAVAILABLE.withDescription("Database 'held' sleeps").asRuntimeException(trailers)))
        .isExactlyInstanceOf(DatabaseNotAvailableException.class)
        .satisfies(e -> assertThat(((DatabaseNotAvailableException) e).getReason()).isEqualTo(
            DatabaseNotAvailableException.Reason.UNAVAILABLE));
  }

  /** The other verdict UNAVAILABLE carries, and the transport's own, are untouched. */
  @Test
  void theRetryableUnavailablesStayRetryable() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, LockTimeoutException.class.getName());
    assertThat(GrpcClientErrorMapper.toException(Status.UNAVAILABLE.withDescription("Timeout on locking").asRuntimeException(trailers)))
        .isExactlyInstanceOf(LockTimeoutException.class);

    assertThat(GrpcClientErrorMapper.toException(Status.UNAVAILABLE.withDescription("connection refused").asRuntimeException()))
        .isExactlyInstanceOf(NeedRetryException.class);
  }
}
