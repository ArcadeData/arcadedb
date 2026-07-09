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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.remote.RemoteException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the client reconstructs the exact ArcadeDB engine exception type from the server's
 * status + {@code arcadedb-exception-class} trailer (issue #5043). This is what makes
 * {@code RemoteDatabase.transaction()}'s retry-on-{@link NeedRetryException} behave over gRPC as over HTTP,
 * and what keeps a permanent {@link DuplicatedKeyException} from being mis-typed as a retryable conflict.
 * No server is required.
 */
class Issue5043GrpcErrorTypeReconstructionTest {

  private static StatusRuntimeException withClass(final Status status, final String exceptionClass,
      final Metadata extra) {
    final Metadata trailers = extra != null ? extra : new Metadata();
    if (exceptionClass != null)
      trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, exceptionClass);
    return status.asRuntimeException(trailers);
  }

  @Test
  @DisplayName("ABORTED + ConcurrentModificationException trailer rebuilds a retryable CME")
  void abortedWithCmeTrailer_rebuildsConcurrentModification() {
    final StatusRuntimeException wire = withClass(Status.ABORTED.withDescription("write-write conflict"),
        ConcurrentModificationException.class.getName(), null);

    final RuntimeException rebuilt = GrpcClientErrorMapper.toException(wire);

    // CME extends NeedRetryException, so transaction() retry fires.
    assertThat(rebuilt).isInstanceOf(ConcurrentModificationException.class)
        .isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("write-write conflict");
  }

  @Test
  @DisplayName("ALREADY_EXISTS + DuplicatedKeyException trailer rebuilds a NON-retryable duplicate key with index/keys")
  void alreadyExistsWithDupTrailer_rebuildsDuplicatedKeyWithDetails() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.DUP_INDEX_KEY, "Person[email]");
    trailers.put(GrpcClientErrorMapper.DUP_KEYS_KEY, "[a@b.com]");
    final StatusRuntimeException wire = withClass(Status.ALREADY_EXISTS.withDescription("dup"),
        DuplicatedKeyException.class.getName(), trailers);

    final RuntimeException rebuilt = GrpcClientErrorMapper.toException(wire);

    assertThat(rebuilt).isInstanceOf(DuplicatedKeyException.class);
    // A permanent conflict must NOT be retryable.
    assertThat(rebuilt).isNotInstanceOf(NeedRetryException.class);
    final DuplicatedKeyException dup = (DuplicatedKeyException) rebuilt;
    assertThat(dup.getIndexName()).isEqualTo("Person[email]");
    assertThat(dup.getKeys()).isEqualTo("[a@b.com]");
  }

  @Test
  @DisplayName("NeedRetryException trailer rebuilds a retryable NeedRetryException")
  void needRetryTrailer_rebuildsNeedRetry() {
    final StatusRuntimeException wire = withClass(Status.ABORTED.withDescription("retry me"),
        NeedRetryException.class.getName(), null);

    final RuntimeException rebuilt = GrpcClientErrorMapper.toException(wire);

    assertThat(rebuilt).isInstanceOf(NeedRetryException.class)
        // exact type, not the CME subclass
        .isNotInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  @DisplayName("RecordNotFoundException trailer rebuilds RecordNotFoundException")
  void recordNotFoundTrailer_rebuilds() {
    final StatusRuntimeException wire = withClass(Status.NOT_FOUND.withDescription("#12:0 not found"),
        RecordNotFoundException.class.getName(), null);

    assertThat(GrpcClientErrorMapper.toException(wire)).isInstanceOf(RecordNotFoundException.class);
  }

  @Test
  @DisplayName("SecurityException trailer rebuilds SecurityException")
  void securityTrailer_rebuilds() {
    final StatusRuntimeException wire = withClass(Status.PERMISSION_DENIED.withDescription("denied"),
        SecurityException.class.getName(), null);

    assertThat(GrpcClientErrorMapper.toException(wire)).isInstanceOf(SecurityException.class);
  }

  @Test
  @DisplayName("TimeoutException trailer rebuilds TimeoutException")
  void timeoutTrailer_rebuilds() {
    final StatusRuntimeException wire = withClass(Status.DEADLINE_EXCEEDED.withDescription("timed out"),
        TimeoutException.class.getName(), null);

    assertThat(GrpcClientErrorMapper.toException(wire)).isInstanceOf(TimeoutException.class);
  }

  @Test
  @DisplayName("No trailer (older server) falls back to status-code mapping")
  void noTrailer_fallsBackToStatusCode() {
    final StatusRuntimeException aborted = Status.ABORTED.withDescription("conflict").asRuntimeException();
    assertThat(GrpcClientErrorMapper.toException(aborted)).isInstanceOf(ConcurrentModificationException.class);

    final StatusRuntimeException notFound = Status.NOT_FOUND.withDescription("gone").asRuntimeException();
    assertThat(GrpcClientErrorMapper.toException(notFound)).isInstanceOf(RecordNotFoundException.class);

    final StatusRuntimeException unavailable = Status.UNAVAILABLE.withDescription("down").asRuntimeException();
    assertThat(GrpcClientErrorMapper.toException(unavailable)).isInstanceOf(NeedRetryException.class);
  }

  @Test
  @DisplayName("Unknown status without a trailer wraps as RemoteException")
  void unknownStatus_wrapsAsRemote() {
    final StatusRuntimeException wire = Status.DATA_LOSS.withDescription("corruption").asRuntimeException();

    assertThat(GrpcClientErrorMapper.toException(wire)).isInstanceOf(RemoteException.class)
        .hasMessageContaining("corruption");
  }

  @Test
  @DisplayName("Unrecognized trailer class falls back to status-code mapping")
  void unknownTrailerClass_fallsBack() {
    final StatusRuntimeException wire = withClass(Status.NOT_FOUND.withDescription("x"),
        "com.example.SomethingElse", null);

    assertThat(GrpcClientErrorMapper.toException(wire)).isInstanceOf(RecordNotFoundException.class);
  }
}
