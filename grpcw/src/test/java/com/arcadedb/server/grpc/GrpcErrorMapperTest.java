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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the server-side exception -> gRPC Status mapping (issue #5043). No server needed.
 */
class GrpcErrorMapperTest {

  @Test
  @DisplayName("DuplicatedKeyException maps to ALREADY_EXISTS and carries index + keys trailers")
  void duplicatedKey_mapsToAlreadyExistsWithDetails() {
    final DuplicatedKeyException dup = new DuplicatedKeyException("Person[name]", "[John]", null);

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(dup, "Commit failed");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    final Metadata trailers = sre.getTrailers();
    assertThat(trailers).isNotNull();
    assertThat(trailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(DuplicatedKeyException.class.getName());
    assertThat(trailers.get(GrpcErrorMapper.DUP_INDEX_KEY)).isEqualTo("Person[name]");
    assertThat(trailers.get(GrpcErrorMapper.DUP_KEYS_KEY)).isEqualTo("[John]");
  }

  @Test
  @DisplayName("ConcurrentModificationException maps to ABORTED (retryable) with its class name")
  void concurrentModification_mapsToAborted() {
    final ConcurrentModificationException cme = new ConcurrentModificationException("record modified");

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(cme, null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
        .isEqualTo(ConcurrentModificationException.class.getName());
  }

  @Test
  @DisplayName("NeedRetryException maps to ABORTED with its class name")
  void needRetry_mapsToAborted() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new NeedRetryException("retry"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
        .isEqualTo(NeedRetryException.class.getName());
  }

  @Test
  @DisplayName("RecordNotFoundException maps to NOT_FOUND")
  void recordNotFound_mapsToNotFound() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new RecordNotFoundException("missing", null), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
  }

  @Test
  @DisplayName("TimeoutException maps to DEADLINE_EXCEEDED")
  void timeout_mapsToDeadlineExceeded() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new TimeoutException("slow"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
  }

  @Test
  @DisplayName("SecurityException maps to PERMISSION_DENIED")
  void security_mapsToPermissionDenied() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new SecurityException("nope"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  @DisplayName("IllegalArgumentException maps to INVALID_ARGUMENT")
  void illegalArgument_mapsToInvalidArgument() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new IllegalArgumentException("bad"),
        null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  @DisplayName("Unknown exception maps to INTERNAL")
  void unknown_mapsToInternal() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new IllegalStateException("boom"),
        null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
  }

  @Test
  @DisplayName("An already-mapped StatusRuntimeException is passed through, not re-wrapped")
  void alreadyMappedStatus_passedThrough() {
    final StatusRuntimeException original = Status.PERMISSION_DENIED
        .withDescription("User has not access to database").asRuntimeException();

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(original, "Failed to begin transaction");

    assertThat(sre).isSameAs(original);
    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  @DisplayName("An ExecutionException wrapping the cause is unwrapped before mapping")
  void executionExceptionIsUnwrapped() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ExecutionException(new DuplicatedKeyException("idx", "[k]", null)), "Commit failed");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.DUP_INDEX_KEY)).isEqualTo("idx");
  }
}
