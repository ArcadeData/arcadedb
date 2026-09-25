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
package com.arcadedb.server.http;

import com.arcadedb.exception.ArcadeDBException;

/**
 * A request was refused because an identical request - same {@code X-Request-Id}, method, path, database and body - is
 * still executing (issue #8324). Nothing was executed for the refused request: the client retries it as it is, with
 * the same id, after {@link #getRetryAfterSeconds()}, and receives the first execution's answer once it settles.
 * Answered {@code 409 Conflict} with a {@code Retry-After} header.
 * <p>
 * A typed exception rather than a status written straight onto the exchange so the refusal survives a hop: a follower
 * that forwarded a SQL write to the leader rebuilds it from the leader's error body
 * ({@code RaftReplicatedDatabase.reconstructLeaderException}) and answers its own client with the same 409 and
 * {@code Retry-After}, instead of the generic 500 a body naming no exception collapsed into (issue #8343).
 * <p>
 * Deliberately NOT a {@link com.arcadedb.exception.NeedRetryException}: the retry has to come from the client, with
 * the same id. A server-side retry loop that caught it would resend the forward under a new forward ordinal, which the
 * leader keys separately, and run the write a second time - exactly what the refusal exists to prevent.
 */
public class RequestStillInFlightException extends ArcadeDBException {
  private final long retryAfterSeconds;

  public RequestStillInFlightException(final String message, final long retryAfterSeconds) {
    super(message);
    this.retryAfterSeconds = Math.max(1L, retryAfterSeconds);
  }

  /** How long the client is told to wait before retrying, at least one second. */
  public long getRetryAfterSeconds() {
    return retryAfterSeconds;
  }
}
