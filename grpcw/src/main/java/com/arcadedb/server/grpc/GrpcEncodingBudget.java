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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * The size one outgoing message may reach while its values are being encoded, counted in approximate wire bytes. The value
 * encoders copy every element they reach, so what they build is measured as the client reads it. Every value is charged
 * {@link #VALUE_COST} and every text or byte string its length, and the encoding stops with {@code RESOURCE_EXHAUSTED} once the
 * total passes the ceiling, which is {@link GlobalConfiguration#GRPC_MAX_MESSAGE_SIZE}: a message larger than that is not one
 * the server's own limit accepts.
 * <p>
 * One budget covers one outgoing message (a unary response, a streamed batch, a materialized result), created where the message
 * starts and handed down the recursion. It is single-threaded and keeps no state besides its counter, so charging a value is a
 * subtraction and a comparison.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class GrpcEncodingBudget {
  /** Approximate wire bytes of one value: its tag, its length prefix and a small payload. */
  static final int VALUE_COST = 8;

  private static final long MEGABYTE = 1024L * 1024L;

  private final long limitBytes;
  private       long remaining;

  GrpcEncodingBudget(final long limitBytes) {
    this.limitBytes = limitBytes;
    this.remaining = limitBytes;
  }

  /** A budget of {@code megabytes} MB, never less than the 1 MB the setting itself is bounded by. */
  static GrpcEncodingBudget ofMegabytes(final int megabytes) {
    return new GrpcEncodingBudget(Math.max(1, megabytes) * MEGABYTE);
  }

  /** A budget sized by the server's configuration of {@link GlobalConfiguration#GRPC_MAX_MESSAGE_SIZE}. */
  static GrpcEncodingBudget of(final ContextConfiguration configuration) {
    return ofMegabytes(configuration.getValueAsInteger(GlobalConfiguration.GRPC_MAX_MESSAGE_SIZE));
  }

  /** A budget for an encoder that has no server configuration at hand: the process-wide value of the setting. */
  static GrpcEncodingBudget ofGlobalSetting() {
    return ofMegabytes(GlobalConfiguration.GRPC_MAX_MESSAGE_SIZE.getValueAsInteger());
  }

  /** Charges one value. */
  void chargeValue() {
    charge(VALUE_COST);
  }

  /** Charges {@code bytes} more, and fails the encoding once the ceiling is passed. Stays failed for every later charge. */
  void charge(final long bytes) {
    remaining -= bytes;
    if (remaining < 0)
      throw exceeded();
  }

  /** Charges the text or the bytes a scalar carries; a scalar of any other kind is covered by {@link #chargeValue()}. */
  GrpcValue chargeScalar(final GrpcValue encoded) {
    switch (encoded.getKindCase()) {
    case STRING_VALUE -> charge(encoded.getStringValue().length());
    case BYTES_VALUE -> charge(encoded.getBytesValue().size());
    default -> {
    }
    }
    return encoded;
  }

  private StatusRuntimeException exceeded() {
    return Status.RESOURCE_EXHAUSTED.withDescription(
        "The result is too large to encode: it exceeds " + limitBytes / MEGABYTE + " MB (" + GlobalConfiguration.GRPC_MAX_MESSAGE_SIZE.getKey()
            + "); select fewer or smaller values").asRuntimeException();
  }
}
