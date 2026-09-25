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
package com.arcadedb.server;

/**
 * Carries, for the duration of one HTTP request, the client's {@code X-Request-Id} to a follower-to-leader forward
 * taken deep inside the engine, where the HTTP exchange is no longer in reach (issue #8323).
 * <p>
 * {@code AbstractServerHttpHandler} publishes the id here only for a request it treats as idempotent - a POST with a
 * non-blank id, outside a client session, in an encoding the replay cache serves - and clears it in its finally block.
 * {@code RaftReplicatedDatabase} reads it when it forwards a SQL write to the leader, so the leader executes that
 * write inside its own idempotency cache: a retry after a lost answer, landing on any node, is replayed there instead
 * of executed a second time.
 * <p>
 * One request can forward more than one statement. Every forward carries the client's id unchanged, and every forward
 * after the first also carries its ordinal in {@link #FORWARD_ORDINAL_HEADER}. The leader's cache key already includes
 * the forwarded body, so two different statements never collide; the ordinal is for two forwards of the SAME
 * statement in one request, which under the bare id would share one key and the second would be answered with the
 * first one's cached result instead of running. A retry of the whole request that forwards the same statements in the
 * same order - what a deterministic execution does - maps each one back to the key it had the first time.
 * <p>
 * The ordinal travels in its own header, never folded into the id, and the leader folds it into the key only when the
 * request carries a valid cluster token: a client can send any {@code X-Request-Id} it likes, so an ordinal encoded in
 * the id itself ({@code order#2}) would be a value a client could also send, and its request would then share a key
 * with another request's second forward of the same statement.
 */
public final class ForwardedRequestIdContext {
  /**
   * Request header carrying the ordinal of a forward after the first within one request (2, 3, ...). Sent only beside
   * the cluster token, and honored only under it.
   */
  public static final String FORWARD_ORDINAL_HEADER = "X-ArcadeDB-Forward-Ordinal";

  private static final ThreadLocal<State> STATE = ThreadLocal.withInitial(State::new);

  private static final class State {
    private String requestId;
    private int    forwards;
  }

  private ForwardedRequestIdContext() {
  }

  /**
   * Publishes the client's request id for the request being served on this thread.
   *
   * @param requestId the raw {@code X-Request-Id}; null or blank publishes nothing
   */
  public static void set(final String requestId) {
    final State state = STATE.get();
    state.requestId = requestId != null && !requestId.isBlank() ? requestId : null;
    state.forwards = 0;
  }

  /** The client's {@code X-Request-Id} published for the request being served on this thread, or null. */
  public static String requestId() {
    return STATE.get().requestId;
  }

  /**
   * Counts one forward to the leader and returns its 1-based ordinal within the request being served on this thread,
   * or 0 - counting nothing - when that request published no id.
   */
  public static int nextForwardOrdinal() {
    final State state = STATE.get();
    return state.requestId == null ? 0 : ++state.forwards;
  }

  /**
   * The ordinal a {@link #FORWARD_ORDINAL_HEADER} value names, or 0 when it names none a forward could have sent: absent,
   * not a number, or below 2 (the first forward sends no header).
   */
  public static int parseForwardOrdinal(final String headerValue) {
    if (headerValue == null || headerValue.isEmpty() || headerValue.length() > 9)
      return 0;
    int ordinal = 0;
    for (int i = 0; i < headerValue.length(); i++) {
      final char c = headerValue.charAt(i);
      if (c < '0' || c > '9')
        return 0;
      ordinal = ordinal * 10 + (c - '0');
    }
    return ordinal >= 2 ? ordinal : 0;
  }

  /**
   * Starts counting forwards from the first again, keeping the published id. Called at the top of every attempt of
   * an auto-commit retry: a forward the retried attempt takes repeats one the previous attempt took, so it must carry
   * the ordinal that forward had rather than the next one, or the leader would see a fresh key and run it again.
   */
  public static void restartOrdinals() {
    STATE.get().forwards = 0;
  }

  /** Clears the id. Must run in a finally block: HTTP worker threads are pooled and reused. */
  public static void clear() {
    final State state = STATE.get();
    state.requestId = null;
    state.forwards = 0;
  }
}
