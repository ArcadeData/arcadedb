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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.RetryBackoff;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLSession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8617: {@link RemoteDatabase#transaction} re-issued a refused attempt immediately, with no backoff and ignoring
 * the {@code Retry-After} the server sent with its 503, so against a node installing a snapshot the whole retry budget
 * was spent within milliseconds. Issue #8618: a {@code /commit} the server answered with a failure cleared the session id
 * without releasing the session the server may still hold.
 * <p>
 * No server is involved: every request is answered from a script, and the pauses the retry loop takes are recorded
 * instead of slept, so the assertions are exact and the test waits for nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8617RemoteTransactionPacingTest {
  private static final String RETRY_LATER = "com.arcadedb.server.http.RetryLaterException";

  private final List<ScriptedDatabase> opened = new ArrayList<>();

  @AfterEach
  void closeDatabases() {
    for (final ScriptedDatabase database : opened)
      database.close();
  }

  private ScriptedDatabase open(final ContextConfiguration configuration) {
    final ScriptedDatabase database = new ScriptedDatabase(configuration);
    opened.add(database);
    return database;
  }

  @Test
  void aRefusalWithRetryAfterPausesAtLeastThatLong() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    database.script("begin", retryLater(5), retryLater(5));

    final AtomicInteger executions = new AtomicInteger();
    database.transaction(executions::incrementAndGet, false, 3);

    assertThat(executions.get()).isEqualTo(1);
    // THE 5 SECONDS THE SERVER ASKED FOR DWARF THE BACKOFF WINDOW (2 AND 4 MS): EACH PAUSE IS THE RETRY-AFTER, SPREAD BY
    // UP TO A TENTH OF IT
    assertRetryAfterPauses(database.pauses, 5_000L, 2);
  }

  @Test
  void theRetryAfterIsBoundedByTheConfiguredCap() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.NETWORK_RETRY_AFTER_MAX_WAIT, 1_200L);
    final ScriptedDatabase database = open(configuration);
    database.script("begin", scripted(503, typed(RETRY_LATER, "installing").put("exceptionArgs", "3600"), "3600"));

    database.transaction(() -> {
    }, false, 3);

    assertRetryAfterPauses(database.pauses, 1_200L, 1);
  }

  @Test
  void aZeroCapIgnoresRetryAfterAndKeepsTheBackoff() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.NETWORK_RETRY_AFTER_MAX_WAIT, 0L);
    final ScriptedDatabase database = open(configuration);
    database.script("begin", retryLater(5), retryLater(5));

    database.transaction(() -> {
    }, false, 3);

    assertBackoffPauses(database.pauses, configuration, 2);
  }

  /**
   * A plain MVCC conflict carries no Retry-After: its attempts are paced by the backoff window
   * {@code LocalDatabase.transaction()} uses, and no pause follows the last attempt.
   */
  @Test
  void aConflictIsPacedByTheBackoffAndTheLastAttemptIsNotFollowedByAPause() {
    final ContextConfiguration configuration = new ContextConfiguration();
    final ScriptedDatabase database = open(configuration);
    final Scripted conflict = scripted(503, typed(ConcurrentModificationException.class.getName(), "conflict"), null);
    database.script("commit", conflict, conflict, conflict, conflict);

    final AtomicInteger executions = new AtomicInteger();
    assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, false, 4))
        .isInstanceOf(ConcurrentModificationException.class);

    assertThat(executions.get()).isEqualTo(4);
    assertBackoffPauses(database.pauses, configuration, 3);
  }

  @Test
  void theRetryAfterIsReadFromTheBodyWhenTheHeaderIsMissing() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    database.script("begin", scripted(503, typed(RETRY_LATER, "installing").put("exceptionArgs", "3"), null));

    database.transaction(() -> {
    }, false, 3);

    assertRetryAfterPauses(database.pauses, 3_000L, 1);
  }

  @Test
  void anInterruptDuringThePauseEndsTheRetries() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    database.interruptPauses = true;
    database.script("begin", retryLater(5), retryLater(5));

    final AtomicInteger executions = new AtomicInteger();
    try {
      assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, false, 3))
          .isInstanceOf(NeedRetryException.class);
      assertThat(Thread.currentThread().isInterrupted()).as("the interrupt must be restored for the caller").isTrue();
    } finally {
      Thread.interrupted();
    }

    assertThat(database.requests("begin")).isEqualTo(1);
    assertThat(executions.get()).isZero();
  }

  /**
   * #4959, the rule {@code LocalDatabase.transaction()} applies: a duplicate that survives one retry is deterministic,
   * so the remaining attempts, now each preceded by a pause, are not spent on it.
   */
  @Test
  void aDuplicatedKeyIsRetriedOnlyOnce() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    final Scripted duplicate = scripted(409,
        typed(DuplicatedKeyException.class.getName(), "duplicate").put("exceptionArgs", "Account[id]|[1]|#1:0"), null);
    database.script("commit", duplicate, duplicate, duplicate);

    final AtomicInteger executions = new AtomicInteger();
    assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, false, 5))
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(executions.get()).isEqualTo(2);
    assertThat(database.pauses).hasSize(1);
  }

  /**
   * A connection that could not be established proves the begin never reached the server, the same verdict gRPC's
   * UNAVAILABLE carries: retried like any other refusal, where it used to fail the whole transaction at once.
   */
  @Test
  void aRefusedConnectionOnBeginIsRetried() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    database.failures.put("begin", new ArrayDeque<>(List.of(new ConnectException("Connection refused"))));

    final AtomicInteger executions = new AtomicInteger();
    database.transaction(executions::incrementAndGet, false, 3);

    assertThat(executions.get()).isEqualTo(1);
    assertThat(database.requests("begin")).isEqualTo(2);
    assertThat(database.pauses).hasSize(1);
  }

  /** A begin that timed out may have opened a session on the server: it is not retried, as over gRPC. */
  @Test
  void aBeginThatTimedOutIsNotRetried() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    database.failures.put("begin", new ArrayDeque<>(List.of(new HttpTimeoutException("request timed out"))));

    final AtomicInteger executions = new AtomicInteger();
    assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, false, 3))
        .isInstanceOf(TransactionException.class);

    assertThat(executions.get()).isZero();
    assertThat(database.requests("begin")).isEqualTo(1);
  }

  /**
   * Issue #8618: a refused commit leaves the transaction open on the server. It is rolled back, on the session it was
   * refused for, before the retry opens the next one.
   */
  @Test
  void aRefusedCommitReleasesItsSessionBeforeTheRetry() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    database.script("commit", retryLater(1));

    database.transaction(() -> {
    }, false, 3);

    assertThat(database.log).containsExactly("begin", "commit AS-1", "rollback AS-1", "begin", "commit AS-2");
  }

  /** The raw call releases the session too: its caller is left without a transaction, as before. */
  @Test
  void aRawCommitThatFailsReleasesItsSession() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    database.script("commit", scripted(503, typed(QuorumNotReachedException.class.getName(), "no quorum"), null));

    database.begin();
    assertThatThrownBy(database::commit).isInstanceOf(QuorumNotReachedException.class);

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.log).containsExactly("begin", "commit AS-1", "rollback AS-1");
  }

  /**
   * A commit the server reports as committed (409) or possibly committed (a dispatched replication that timed out) is
   * not retried, and not followed by a rollback either: it would roll back nothing and count as a rollback.
   */
  @Test
  void aCommitThatLandedOrMayHaveLandedIsNotFollowedByARollback() {
    final Scripted[] outcomes = {
        scripted(409, typed(TransactionCommittedRemotelyException.class.getName(), "committed cluster-wide"), null),
        scripted(500, typed("com.arcadedb.server.ha.raft.ReplicationDispatchedTimeoutException", "dispatched, timed out"),
            null) };
    for (final Scripted outcome : outcomes) {
      final ScriptedDatabase database = open(new ContextConfiguration());
      database.script("commit", outcome);

      database.begin();
      assertThatThrownBy(database::commit).as("commit answered %d", outcome.status())
          .isInstanceOf(TransactionException.class).isNotInstanceOf(NeedRetryException.class);

      assertThat(database.isTransactionActive()).isFalse();
      assertThat(database.log).as("commit answered %d", outcome.status()).containsExactly("begin", "commit AS-1");
    }
  }

  /** A server that says it already ended the session leaves nothing to release: no rollback round trip. */
  @Test
  void aFailedCommitWhoseSessionTheServerClosedIsNotFollowedByARollback() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    database.script("commit",
        new Scripted(503, typed(ConcurrentModificationException.class.getName(), "conflict").toString(), null, true));

    database.begin();
    assertThatThrownBy(database::commit).isInstanceOf(ConcurrentModificationException.class);

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.log).containsExactly("begin", "commit AS-1");
  }

  /**
   * A commit that failed in transport may still be running on a server that may not be reachable: it is not followed by
   * a rollback that could only wait on the same failure.
   */
  @Test
  void aCommitThatFailedInTransportIsNotFollowedByARollback() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    database.failures.put("commit", new ArrayDeque<>(List.of(new HttpTimeoutException("request timed out"))));

    database.begin();
    assertThatThrownBy(database::commit).isInstanceOf(TransactionException.class);

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.log).containsExactly("begin", "commit AS-1");
  }

  @Test
  void retryAfterAcceptsBothFormsOfTheHeader() {
    final long now = Instant.parse("2026-09-28T10:00:00Z").toEpochMilli();
    final String inTenSeconds = DateTimeFormatter.RFC_1123_DATE_TIME.format(Instant.ofEpochMilli(now + 10_000L).atZone(ZoneOffset.UTC));
    final String tenSecondsAgo = DateTimeFormatter.RFC_1123_DATE_TIME.format(Instant.ofEpochMilli(now - 10_000L).atZone(ZoneOffset.UTC));

    assertThat(RemoteHttpComponent.retryAfterMs("5", now)).isEqualTo(5_000L);
    assertThat(RemoteHttpComponent.retryAfterMs(" 12 ", now)).isEqualTo(12_000L);
    assertThat(RemoteHttpComponent.retryAfterMs("0", now)).isZero();
    assertThat(RemoteHttpComponent.retryAfterMs(inTenSeconds, now)).isEqualTo(10_000L);
    assertThat(RemoteHttpComponent.retryAfterMs(tenSecondsAgo, now)).isZero();
    assertThat(RemoteHttpComponent.retryAfterMs("99999999999999999", now)).isEqualTo(Long.MAX_VALUE);

    for (final String unreadable : new String[] { null, "", "  ", "-5", "1.5", "soon", "5s" })
      assertThat(RemoteHttpComponent.retryAfterMs(unreadable, now)).as("'%s'", unreadable).isZero();
  }

  /** The hint reaches the retry loop whatever type the refusal was rebuilt as, and only on a retryable one. */
  @Test
  void theHintTravelsOnEveryRetryableTypeAndNoOther() {
    final ScriptedDatabase database = open(new ContextConfiguration());

    final Exception untyped = database.manageException(new ScriptedResponse(null, 503, "", headers("7")), "begin");
    assertThat(untyped).isInstanceOf(NeedRetryException.class);
    assertThat(((NeedRetryException) untyped).getRetryAfterMs()).isEqualTo(7_000L);

    final Exception quorum = database.manageException(
        new ScriptedResponse(null, 503, typed(QuorumNotReachedException.class.getName(), "no quorum").toString(), headers("2")),
        "commit");
    assertThat(quorum).isInstanceOf(QuorumNotReachedException.class);
    assertThat(((NeedRetryException) quorum).getRetryAfterMs()).isEqualTo(2_000L);

    // THE HTTP-DATE FORM, AS A PROXY IN FRONT OF THE SERVER MAY SEND IT
    final String inAMinute = DateTimeFormatter.RFC_1123_DATE_TIME.format(Instant.now().plusSeconds(60).atZone(ZoneOffset.UTC));
    final Exception dated = database.manageException(new ScriptedResponse(null, 503, "", headers(inAMinute)), "begin");
    assertThat(((NeedRetryException) dated).getRetryAfterMs()).isBetween(1L, 60_000L);

    // THE HEADER WINS OVER THE BODY
    final Exception both = database.manageException(new ScriptedResponse(null, 503,
        typed(RETRY_LATER, "installing").put("exceptionArgs", "5").toString(), headers("8")), "begin");
    assertThat(((NeedRetryException) both).getRetryAfterMs()).isEqualTo(8_000L);

    final Exception notRetryable = database.manageException(
        new ScriptedResponse(null, 409, typed("com.arcadedb.server.http.RequestStillInFlightException", "busy").toString(),
            headers("3")), "command");
    assertThat(notRetryable).isNotInstanceOf(NeedRetryException.class);
  }

  /**
   * The clients a node refuses together get the same hint: the spread keeps them from coming back at the same instant.
   */
  @Test
  void theRetryAfterPauseIsSpreadByUpToATenthOfTheHint() {
    final ScriptedDatabase database = open(new ContextConfiguration());
    final NeedRetryException refusal = new NeedRetryException("installing");
    refusal.setRetryAfterMs(10_000L);

    final Set<Long> pauses = new HashSet<>();
    for (int i = 0; i < 200; i++) {
      final long pause = database.retryAfterPauseMs(refusal);
      assertThat(pause).isBetween(10_000L, 11_000L);
      pauses.add(pause);
    }
    assertThat(pauses).as("200 draws from a 1000 ms spread").hasSizeGreaterThan(1);
    assertThat(database.retryAfterPauseMs(new NeedRetryException("no hint"))).isZero();
  }

  private static void assertRetryAfterPauses(final List<Long> pauses, final long retryAfterMs, final int expected) {
    assertThat(pauses).hasSize(expected);
    for (final long pause : pauses)
      assertThat(pause).isBetween(retryAfterMs, retryAfterMs + retryAfterMs / 10);
  }

  private static void assertBackoffPauses(final List<Long> pauses, final ContextConfiguration configuration,
      final int expected) {
    assertThat(pauses).hasSize(expected);
    for (int attempt = 0; attempt < pauses.size(); attempt++)
      assertThat(pauses.get(attempt)).as("pause after attempt %d", attempt + 1)
          .isBetween(1L, RetryBackoff.windowMs(attempt, configuration.getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY_BASE),
              configuration.getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY)));
  }

  private static JSONObject typed(final String exception, final String detail) {
    return new JSONObject().put("error", detail).put("detail", detail).put("exception", exception);
  }

  private static Scripted retryLater(final long seconds) {
    return scripted(503, typed(RETRY_LATER, "Server is installing a snapshot, please retry")
        .put("exceptionArgs", String.valueOf(seconds)), String.valueOf(seconds));
  }

  private static Scripted scripted(final int status, final JSONObject body, final String retryAfter) {
    return new Scripted(status, body.toString(), retryAfter, false);
  }

  private static HttpHeaders headers(final String retryAfter) {
    return headers(retryAfter, false);
  }

  private static HttpHeaders headers(final String retryAfter, final boolean sessionClosed) {
    final Map<String, List<String>> map = new HashMap<>();
    if (retryAfter != null)
      map.put("Retry-After", List.of(retryAfter));
    if (sessionClosed)
      map.put(RemoteDatabase.ARCADEDB_SESSION_CLOSED, List.of("true"));
    return HttpHeaders.of(map, (name, value) -> true);
  }

  /** A scripted answer. {@code sessionClosed} sets the header a server sends when the request ended its session. */
  private record Scripted(int status, String body, String retryAfter, boolean sessionClosed) {
  }

  /**
   * Answers every transaction route from a script (falling back to success) and records the pauses the retry loop asks
   * for instead of sleeping them.
   */
  private static final class ScriptedDatabase extends RemoteDatabase {
    final List<Long>                     pauses   = new ArrayList<>();
    final List<String>                   log      = new ArrayList<>();
    final Map<String, Deque<Scripted>>   scripts  = new HashMap<>();
    final Map<String, Deque<IOException>> failures = new HashMap<>();
    final Map<String, AtomicInteger>     counts   = new HashMap<>();
    boolean                              interruptPauses;
    private int                          sessions;

    ScriptedDatabase(final ContextConfiguration configuration) {
      super("127.0.0.1", 2480, "db", "root", "password", configuration);
    }

    void script(final String route, final Scripted... answers) {
      scripts.put(route, new ArrayDeque<>(List.of(answers)));
    }

    int requests(final String route) {
      return counts.computeIfAbsent(route, k -> new AtomicInteger()).get();
    }

    @Override
    void requestClusterConfiguration() {
      // NO SERVER TO ASK
    }

    @Override
    protected void sleepBeforeRetry(final long delayMs) throws InterruptedException {
      pauses.add(delayMs);
      if (interruptPauses)
        throw new InterruptedException("interrupted by the test");
    }

    @Override
    HttpResponse<String> sendWithWatchdog(final HttpRequest request) throws IOException {
      final String path = request.uri().getPath();
      final String route = path.substring("/api/v1/".length(), path.lastIndexOf('/'));
      counts.computeIfAbsent(route, k -> new AtomicInteger()).incrementAndGet();
      log.add(route + request.headers().firstValue(ARCADEDB_SESSION_ID).map(id -> " " + id).orElse(""));

      final Deque<IOException> failure = failures.get(route);
      if (failure != null && !failure.isEmpty())
        throw failure.poll();

      final Deque<Scripted> script = scripts.get(route);
      if (script != null && !script.isEmpty()) {
        final Scripted answer = script.poll();
        return new ScriptedResponse(request, answer.status(), answer.body(),
            headers(answer.retryAfter(), answer.sessionClosed()));
      }

      final Map<String, List<String>> headers = new HashMap<>();
      if ("begin".equals(route))
        headers.put(ARCADEDB_SESSION_ID, List.of("AS-" + ++sessions));
      return new ScriptedResponse(request, 204, "", HttpHeaders.of(headers, (name, value) -> true));
    }
  }

  private record ScriptedResponse(HttpRequest request, int statusCode, String body, HttpHeaders headers)
      implements HttpResponse<String> {
    @Override
    public Optional<HttpResponse<String>> previousResponse() {
      return Optional.empty();
    }

    @Override
    public Optional<SSLSession> sslSession() {
      return Optional.empty();
    }

    @Override
    public URI uri() {
      return request.uri();
    }

    @Override
    public HttpClient.Version version() {
      return HttpClient.Version.HTTP_1_1;
    }
  }
}
