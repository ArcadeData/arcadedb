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

import java.util.Objects;

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
 * <p>
 * The leader's key is built from the body IT receives, and a forward's body is not the client's: the engine-level
 * forward rebuilds it from the statement alone, and {@code LeaderCommandForwarder} re-serializes the parsed payload. So
 * a retry the client sends STRAIGHT to the leader - its own body, byte for byte - did not match the key its forwarded
 * first attempt was cached under, and executed a second time (issue #8347). The node that served the client publishes
 * here, beside the id, the key it computed for the client's own request, and a forward that IS the client's whole
 * request relays it in {@link #CLIENT_KEY_HEADER}, beside the cluster token and honored only under it. The leader then
 * claims that key too, so the client's key is settled on the leader by whichever of the two arrives first. A forward
 * that is only a part of the request - a write issued by a command this node executed locally, or any forward after
 * the first - relays no key: the client's key there names the whole request, and settling it with the answer to one
 * statement inside it would replay that answer to a direct retry of the whole.
 * <p>
 * The key settles one answer, so that answer has to be the one the client's request asks for. A forward rebuilt from
 * the statement is answered in the leader's default rendering ({@code serializer: "record"}, the default row limit, no
 * type hints), not in the {@code serializer}, {@code limit} or {@code typeHints} the client's body names, and a direct
 * retry was replayed from it in the wrong shape; the other way round, a forward that found the client's key settled by a
 * direct attempt got an answer in the client's rendering, which the follower then parsed as the default one (issue
 * #8359). So a forward that relays the key is the client's request itself: the handler declares, with
 * {@link #declareWholeRequestCommand}, the statement it is about to run and the body it came in, the forward of exactly
 * that statement posts that body unchanged, and the leader's answer - rendered for that body, whichever node settled the
 * key - is handed back through {@link #publishWholeRequestAnswer} and sent to the client as it is.
 */
public final class ForwardedRequestIdContext {
  /**
   * Request header carrying the ordinal of a forward after the first within one request (2, 3, ...). Sent only beside
   * the cluster token, and honored only under it.
   */
  public static final String FORWARD_ORDINAL_HEADER = "X-ArcadeDB-Forward-Ordinal";

  /**
   * Request header carrying the idempotency key the forwarding node computed for the client's own request (issue
   * #8347): a SHA-256 digest in lower-case hex. Sent only beside the cluster token, and honored only under it.
   */
  public static final String CLIENT_KEY_HEADER = "X-ArcadeDB-Client-Request-Key";

  // Length of the lower-case hex SHA-256 digest an idempotency key is.
  private static final int CLIENT_KEY_LENGTH = 64;

  private static final ThreadLocal<State> STATE = ThreadLocal.withInitial(State::new);

  private static final class State {
    private String  requestId;
    private int     forwards;
    private String  clientKey;
    // Whether an engine-level command forward can be the client's whole request: true only for a route whose body
    // is one command, and cleared as soon as this node executes a command locally (a forward then is a part of it).
    private boolean commandForwardIsWholeRequest;
    // The statement the handler runs as the whole request, and the client's own body it came in (issue #8359).
    private String  wholeRequestLanguage;
    private String  wholeRequestCommand;
    private String  wholeRequestBody;
    // The leader's answer to the forward that posted that body, to be sent to the client as it is.
    private String  wholeRequestAnswer;
  }

  /**
   * An engine-level command forward that is the client's whole request (issue #8359): the key the client's request has
   * on this node, and the client's own body, which the forward posts unchanged so the leader answers - and settles the
   * key with - exactly what the client asked for.
   */
  public record WholeRequest(String clientKey, String clientBody) {
  }

  private ForwardedRequestIdContext() {
  }

  /**
   * Publishes the client's request id for the request being served on this thread.
   *
   * @param requestId the raw {@code X-Request-Id}; null or blank publishes nothing
   */
  public static void set(final String requestId) {
    set(requestId, null, false);
  }

  /**
   * Publishes the client's request id and the idempotency key this node computed for the request (issue #8347).
   *
   * @param requestId                    the raw {@code X-Request-Id}; null or blank publishes nothing, key included
   * @param clientKey                    the idempotency key of the client's request on this node, or null
   * @param commandForwardIsWholeRequest true when the route's body is exactly one command, so an engine-level forward
   *                                     of that command is the whole request and may relay the key
   */
  public static void set(final String requestId, final String clientKey, final boolean commandForwardIsWholeRequest) {
    final State state = STATE.get();
    state.requestId = requestId != null && !requestId.isBlank() ? requestId : null;
    state.forwards = 0;
    state.clientKey = state.requestId != null && isClientKey(clientKey) ? clientKey : null;
    state.commandForwardIsWholeRequest = state.clientKey != null && commandForwardIsWholeRequest;
    clearWholeRequest(state);
  }

  /**
   * The idempotency key of the client's request, for a forward that relays the whole request as it is (the HTTP-level
   * {@code LeaderCommandForwarder}), or null when none was published.
   */
  public static String clientKey() {
    return STATE.get().clientKey;
  }

  /**
   * Declares the statement the handler is about to run as the client's whole request, and the body the client sent it in
   * (issue #8359). A forward of exactly that statement then posts that body instead of one rebuilt from the statement.
   * Also drops the answer a previous attempt of the request may have left: an auto-commit retry declares again. Records
   * nothing when no forward of this request can be the whole of it, so the body is held only when it can be used.
   *
   * @param language   the language the statement is run in
   * @param command    the statement exactly as the handler passes it to the database
   * @param clientBody the client's request body, unchanged
   */
  public static void declareWholeRequestCommand(final String language, final String command, final String clientBody) {
    final State state = STATE.get();
    if (!state.commandForwardIsWholeRequest) {
      clearWholeRequest(state);
      return;
    }
    state.wholeRequestLanguage = language;
    state.wholeRequestCommand = command;
    state.wholeRequestBody = clientBody;
    state.wholeRequestAnswer = null;
  }

  /**
   * The client's key and body, for the engine-level command forward with the given ordinal of the given statement, when
   * that forward is the client's whole request: only the first forward, only on a route whose body is one command, only
   * while this node has executed no command of the request locally, and only for the statement the handler declared
   * with {@link #declareWholeRequestCommand}. Null otherwise: a write that something else issues - a function the
   * statement calls, a query that runs locally - is a part of the request even when it is the first to be forwarded.
   */
  public static WholeRequest wholeRequestForward(final int forwardOrdinal, final String language, final String query) {
    final State state = STATE.get();
    if (forwardOrdinal != 1 || !state.commandForwardIsWholeRequest || state.wholeRequestBody == null
        || state.wholeRequestCommand == null || !state.wholeRequestCommand.equals(query)
        || !Objects.equals(state.wholeRequestLanguage, language))
      return null;
    return new WholeRequest(state.clientKey, state.wholeRequestBody);
  }

  /**
   * Records the leader's answer to the forward that posted the client's whole request (issue #8359): the handler sends
   * it to the client as it is, rather than rendering again what this node parsed back out of it.
   */
  public static void publishWholeRequestAnswer(final String answer) {
    STATE.get().wholeRequestAnswer = answer;
  }

  /**
   * The leader's answer to the client's whole request, or null when the request was not forwarded whole. Consumed: a
   * second read answers null.
   */
  public static String takeWholeRequestAnswer() {
    final State state = STATE.get();
    final String answer = state.wholeRequestAnswer;
    state.wholeRequestAnswer = null;
    return answer;
  }

  /**
   * Records that this node executes a command of the request being served locally, so any command it forwards from
   * now on is a part of the request rather than the whole of it and relays no client key. Kept across
   * {@link #restartOrdinals()}: the retried attempt executes the same command locally again.
   */
  public static void markExecutedLocally() {
    STATE.get().commandForwardIsWholeRequest = false;
  }

  private static void clearWholeRequest(final State state) {
    state.wholeRequestLanguage = null;
    state.wholeRequestCommand = null;
    state.wholeRequestBody = null;
    state.wholeRequestAnswer = null;
  }

  /**
   * The client key a {@link #CLIENT_KEY_HEADER} value names, or null when it is not one a peer could have sent: a
   * lower-case hex SHA-256 digest.
   */
  public static String parseClientKey(final String headerValue) {
    return isClientKey(headerValue) ? headerValue : null;
  }

  private static boolean isClientKey(final String value) {
    if (value == null || value.length() != CLIENT_KEY_LENGTH)
      return false;
    for (int i = 0; i < CLIENT_KEY_LENGTH; i++) {
      final char c = value.charAt(i);
      if ((c < '0' || c > '9') && (c < 'a' || c > 'f'))
        return false;
    }
    return true;
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

  /** Clears the id and the key. Must run in a finally block: HTTP worker threads are pooled and reused. */
  public static void clear() {
    final State state = STATE.get();
    state.requestId = null;
    state.forwards = 0;
    state.clientKey = null;
    state.commandForwardIsWholeRequest = false;
    clearWholeRequest(state);
  }
}
