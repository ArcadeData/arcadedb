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

import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutionException;

/**
 * Central, consistent mapping from ArcadeDB engine exceptions to {@link io.grpc.Status} codes.
 * <p>
 * The response protobufs only carry a free-text message, which erases the original exception type. That
 * breaks type-driven client behavior, most importantly {@code RemoteDatabase.transaction()}'s automatic
 * retry on {@link NeedRetryException}. To preserve the type across the wire (the way the HTTP protocol
 * ships the exception class name), this mapper attaches the fully-qualified exception class name in a
 * metadata trailer so the client can reconstruct the exact type; for a {@link DuplicatedKeyException} the
 * index name and keys are carried as well.
 */
public final class GrpcErrorMapper {
  /** Trailer carrying the fully-qualified engine exception class name so the client can rebuild the type. */
  public static final Metadata.Key<String> EXCEPTION_CLASS_KEY = Metadata.Key.of("arcadedb-exception-class",
      Metadata.ASCII_STRING_MARSHALLER);
  /**
   * Trailer carrying the index name for a {@link DuplicatedKeyException}, Base64-encoded.
   * The value is Base64 so arbitrary index names (unicode, control characters) survive the ASCII-only
   * gRPC metadata channel intact; the client Base64-decodes it back.
   */
  public static final Metadata.Key<String> DUP_INDEX_KEY        = Metadata.Key.of("arcadedb-dup-index",
      Metadata.ASCII_STRING_MARSHALLER);
  /**
   * Trailer carrying the offending keys for a {@link DuplicatedKeyException}, Base64-encoded.
   * The value is Base64 because indexed key values can be arbitrary user data (unicode, non-Latin
   * scripts, control characters) that ASCII gRPC metadata cannot carry losslessly.
   */
  public static final Metadata.Key<String> DUP_KEYS_KEY         = Metadata.Key.of("arcadedb-dup-keys",
      Metadata.ASCII_STRING_MARSHALLER);

  private GrpcErrorMapper() {
  }

  /**
   * Unwraps an {@link ExecutionException} (raised when work runs on a transaction's dedicated executor) to
   * expose the real engine cause.
   */
  public static Throwable unwrap(final Throwable t) {
    if (t instanceof ExecutionException && t.getCause() != null)
      return t.getCause();
    return t;
  }

  /**
   * Maps a throwable to a {@link StatusRuntimeException} suitable for {@code StreamObserver.onError()}.
   * An already-mapped gRPC status (e.g. the security status produced by {@code getDatabase}) is passed
   * through unchanged so it is never masked as {@code INTERNAL}.
   *
   * @param t             the throwable to map (may be an {@link ExecutionException} wrapping the cause)
   * @param contextPrefix optional short prefix for the client-facing description (e.g. "Commit failed")
   */
  public static StatusRuntimeException toStatusRuntimeException(final Throwable t, final String contextPrefix) {
    final Throwable cause = unwrap(t);

    // Pass through statuses already chosen upstream (security, resource-exhausted, etc.).
    if (cause instanceof StatusRuntimeException sre)
      return sre;
    if (cause instanceof StatusException se)
      return new StatusRuntimeException(se.getStatus(), se.getTrailers());

    final Metadata trailers = new Metadata();
    trailers.put(EXCEPTION_CLASS_KEY, cause.getClass().getName());

    final Status.Code code;
    if (cause instanceof DuplicatedKeyException dup) {
      code = Status.Code.ALREADY_EXISTS;
      if (dup.getIndexName() != null)
        trailers.put(DUP_INDEX_KEY, encodeTrailer(dup.getIndexName()));
      if (dup.getKeys() != null)
        trailers.put(DUP_KEYS_KEY, encodeTrailer(dup.getKeys()));
    } else if (cause instanceof NeedRetryException) {
      // Covers ConcurrentModificationException (a NeedRetryException subclass): retryable conflict.
      code = Status.Code.ABORTED;
    } else if (cause instanceof RecordNotFoundException) {
      code = Status.Code.NOT_FOUND;
    } else if (cause instanceof TimeoutException) {
      code = Status.Code.DEADLINE_EXCEEDED;
    } else if (cause instanceof SecurityException) {
      // Matches java.lang.SecurityException. ArcadeDB's ServerSecurityException does NOT extend it, but
      // security failures are already pre-mapped to a StatusRuntimeException by getDatabase and returned
      // via the pass-through branch above, so they never reach here as a raw exception. If a future
      // onError path hands a raw ServerSecurityException to this mapper it would fall through to INTERNAL.
      code = Status.Code.PERMISSION_DENIED;
    } else if (cause instanceof IllegalArgumentException) {
      code = Status.Code.INVALID_ARGUMENT;
    } else {
      code = Status.Code.INTERNAL;
    }

    final String msg = cause.getMessage() != null ? cause.getMessage() : cause.toString();
    final String description = contextPrefix != null && !contextPrefix.isBlank() ? contextPrefix + ": " + msg : msg;

    return code.toStatus().withDescription(description).withCause(cause).asRuntimeException(trailers);
  }

  /**
   * Base64-encodes a trailer value so arbitrary (possibly non-ASCII) index names and key values survive
   * the ASCII-only gRPC metadata channel. The client decodes it with the mirror method.
   */
  static String encodeTrailer(final String value) {
    return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }
}
