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

/**
 * Rebuilds the original ArcadeDB engine exception from a gRPC error returned by the server.
 * <p>
 * A server that maps its exceptions through {@code GrpcErrorMapper} ships the fully-qualified exception
 * class name in a metadata trailer. When that trailer is present the exact type is reconstructed - this is
 * what lets {@code RemoteDatabase.transaction()}'s retry-on-{@link NeedRetryException} behave over gRPC as
 * it does over HTTP, and what keeps a permanent {@link DuplicatedKeyException} from being mis-typed as a
 * retryable conflict. When the trailer is absent (an older server) the legacy status-code mapping is used,
 * so this is backward compatible.
 * <p>
 * The trailer key strings must stay in sync with the server-side {@code GrpcErrorMapper}.
 */
final class GrpcClientErrorMapper {
  static final Metadata.Key<String> EXCEPTION_CLASS_KEY = Metadata.Key.of("arcadedb-exception-class",
      Metadata.ASCII_STRING_MARSHALLER);
  static final Metadata.Key<String> DUP_INDEX_KEY        = Metadata.Key.of("arcadedb-dup-index",
      Metadata.ASCII_STRING_MARSHALLER);
  static final Metadata.Key<String> DUP_KEYS_KEY         = Metadata.Key.of("arcadedb-dup-keys",
      Metadata.ASCII_STRING_MARSHALLER);

  private GrpcClientErrorMapper() {
  }

  /**
   * Converts a gRPC failure into the matching ArcadeDB runtime exception. Never returns {@code null}.
   */
  static RuntimeException toException(final Throwable e) {
    final Status status = Status.fromThrowable(e);
    final Metadata trailers = Status.trailersFromThrowable(e);
    final String msg = status.getDescription() != null ? status.getDescription() : status.getCode().name();

    final String exceptionClass = trailers != null ? trailers.get(EXCEPTION_CLASS_KEY) : null;
    if (exceptionClass != null) {
      final RuntimeException reconstructed = reconstructFromClassName(exceptionClass, msg, trailers);
      if (reconstructed != null)
        return reconstructed;
    }

    // Legacy / trailer-less fallback: map by status code only.
    return switch (status.getCode()) {
      case NOT_FOUND -> new RecordNotFoundException(msg, null);
      case ALREADY_EXISTS -> new DuplicatedKeyException(dupIndex(trailers), dupKeys(trailers), null);
      case ABORTED -> new ConcurrentModificationException(msg);
      case DEADLINE_EXCEEDED -> new TimeoutException(msg);
      case PERMISSION_DENIED -> new SecurityException(msg);
      case UNAVAILABLE -> new NeedRetryException(msg);
      default -> new RemoteException("gRPC error: " + msg, e);
    };
  }

  private static RuntimeException reconstructFromClassName(final String exceptionClass, final String msg,
      final Metadata trailers) {
    return switch (exceptionClass) {
      case "com.arcadedb.exception.DuplicatedKeyException" ->
          new DuplicatedKeyException(dupIndex(trailers), dupKeys(trailers), null);
      case "com.arcadedb.exception.ConcurrentModificationException" -> new ConcurrentModificationException(msg);
      case "com.arcadedb.exception.NeedRetryException" -> new NeedRetryException(msg);
      case "com.arcadedb.exception.RecordNotFoundException" -> new RecordNotFoundException(msg, null);
      case "com.arcadedb.exception.TimeoutException" -> new TimeoutException(msg);
      case "java.lang.SecurityException" -> new SecurityException(msg);
      // Unknown class: let the caller fall back to status-code mapping.
      default -> null;
    };
  }

  private static String dupIndex(final Metadata trailers) {
    final String v = trailers != null ? trailers.get(DUP_INDEX_KEY) : null;
    return v != null ? v : "";
  }

  private static String dupKeys(final Metadata trailers) {
    final String v = trailers != null ? trailers.get(DUP_KEYS_KEY) : null;
    return v != null ? v : "";
  }
}
