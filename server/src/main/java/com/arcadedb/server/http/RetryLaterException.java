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

import com.arcadedb.exception.NeedRetryException;

/**
 * A request was refused before it ran, by a node that is temporarily unable to serve it and said how long to wait - a
 * node installing a snapshot answers {@code 503} + {@code Retry-After} before any handler runs. Answered
 * {@code 503 Service Unavailable} with a {@code Retry-After} header.
 * <p>
 * A typed exception so the refusal survives a hop: a follower that forwarded a SQL write to such a node rebuilds it
 * from the answer ({@code RaftReplicatedDatabase.reconstructLeaderException}) and answers its own client the same
 * 503 and {@code Retry-After}, instead of the 500 "Error on transaction commit" an answer naming no exception
 * collapsed into (issue #8355). The back-off also travels in the body's {@code exceptionArgs}, so one more hop can
 * rebuild it from the body alone.
 * <p>
 * Deliberately a {@link NeedRetryException}, unlike {@link RequestStillInFlightException}: nothing ran for the refused
 * request and the refusing node did not reserve its {@code X-Request-Id}, so a server-side retry loop that catches it
 * and forwards the request again, under a new forward ordinal (issue #8323), sends it for the first time rather than
 * the second.
 */
public class RetryLaterException extends NeedRetryException {
  private final long retryAfterSeconds;

  public RetryLaterException(final String message, final long retryAfterSeconds) {
    super(message);
    this.retryAfterSeconds = Math.max(1L, retryAfterSeconds);
  }

  /** How long the client is told to wait before retrying, at least one second. */
  public long getRetryAfterSeconds() {
    return retryAfterSeconds;
  }
}
