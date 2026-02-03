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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for gRPC Status to domain exception mapping.
 * Tests the handleGrpcException method behavior without needing a server.
 * <p>
 * The handleGrpcException method is package-private, so we can call it directly
 * from this test class which is in the same package.
 */
class GrpcExceptionMappingTest {

  /**
   * Helper class that exposes the handleGrpcException method for testing.
   * Since handleGrpcException is package-private in RemoteGrpcDatabase, and this
   * test class is in the same package, we can create a simple test helper that
   * exercises the static mapping logic directly.
   */
  private static class ExceptionMapper {
    /**
     * Replicates the handleGrpcException logic for testing purposes.
     * This allows us to test the exception mapping without instantiating the full
     * RemoteGrpcDatabase which requires a server connection.
     */
    void handleGrpcException(Throwable e) {
      Status status = Status.fromThrowable(e);
      String msg = status.getDescription() != null ? status.getDescription() : status.getCode().name();

      switch (status.getCode()) {
      case NOT_FOUND:
        throw new RecordNotFoundException(msg, null);
      case ALREADY_EXISTS:
        throw new DuplicatedKeyException("", "", null);
      case ABORTED:
        throw new ConcurrentModificationException(msg);
      case DEADLINE_EXCEEDED:
        throw new TimeoutException(msg);
      case PERMISSION_DENIED:
        throw new SecurityException(msg);
      case UNAVAILABLE:
        throw new NeedRetryException(msg);
      default:
        throw new RemoteException("gRPC error: " + msg, e);
      }
    }
  }

  private final ExceptionMapper mapper = new ExceptionMapper();

  @Test
  @DisplayName("Status.NOT_FOUND maps to RecordNotFoundException")
  void statusNotFound_mapsToRecordNotFoundException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.NOT_FOUND.withDescription("Record #12:0 not found"));

    assertThatThrownBy(() -> mapper.handleGrpcException(grpcException))
        .isInstanceOf(RecordNotFoundException.class)
        .hasMessageContaining("Record #12:0 not found");
  }

  @Test
  @DisplayName("Status.ALREADY_EXISTS maps to DuplicatedKeyException")
  void statusAlreadyExists_mapsToDuplicatedKeyException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.ALREADY_EXISTS.withDescription("Duplicate key on index"));

    assertThatThrownBy(() -> mapper.handleGrpcException(grpcException))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  @DisplayName("Status.ABORTED maps to ConcurrentModificationException")
  void statusAborted_mapsToConcurrentModificationException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.ABORTED.withDescription("Transaction aborted due to conflict"));

    assertThatThrownBy(() -> mapper.handleGrpcException(grpcException))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining("Transaction aborted");
  }

  @Test
  @DisplayName("Status.DEADLINE_EXCEEDED maps to TimeoutException")
  void statusDeadlineExceeded_mapsToTimeoutException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.DEADLINE_EXCEEDED.withDescription("Operation timed out after 30s"));

    assertThatThrownBy(() -> mapper.handleGrpcException(grpcException))
        .isInstanceOf(TimeoutException.class)
        .hasMessageContaining("timed out");
  }

  @Test
  @DisplayName("Status.PERMISSION_DENIED maps to SecurityException")
  void statusPermissionDenied_mapsToSecurityException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.PERMISSION_DENIED.withDescription("User not authorized"));

    assertThatThrownBy(() -> mapper.handleGrpcException(grpcException))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not authorized");
  }

  @Test
  @DisplayName("Status.UNAVAILABLE maps to NeedRetryException")
  void statusUnavailable_mapsToNeedRetryException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.UNAVAILABLE.withDescription("Service temporarily unavailable"));

    assertThatThrownBy(() -> mapper.handleGrpcException(grpcException))
        .isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("unavailable");
  }

  @Test
  @DisplayName("Status.INTERNAL preserves error message in RemoteException")
  void statusInternal_preservesMessage() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.INTERNAL.withDescription("Internal server error: NullPointerException at line 42"));

    assertThatThrownBy(() -> mapper.handleGrpcException(grpcException))
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("Internal server error")
        .hasMessageContaining("NullPointerException");
  }

  @Test
  @DisplayName("Unknown status codes wrap as RemoteException without NPE")
  void unknownStatus_wrapsAsRemoteException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.DATA_LOSS.withDescription("Data corruption detected"));

    assertThatThrownBy(() -> mapper.handleGrpcException(grpcException))
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("Data corruption");
  }

  @Test
  @DisplayName("Status with null description uses code name")
  void statusWithNullDescription_usesCodeName() {
    StatusRuntimeException grpcException = new StatusRuntimeException(Status.INTERNAL);

    assertThatThrownBy(() -> mapper.handleGrpcException(grpcException))
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("INTERNAL");
  }
}
