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

import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.RetryLaterException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issues #8618 and #8617 against a real server.
 * <p>
 * #8618: a {@code /commit} refused before its handler ran (the 503 a node installing a snapshot answers) left the
 * session's transaction open on the server, while {@link RemoteDatabase#commit()} cleared the client's session id, so the
 * retry loop could not release it: one abandoned session per deflected attempt, until the idle timeout. A commit that
 * failed INSIDE the handler left its session registered too, empty, for the same timeout.
 * <p>
 * #8617: the retry loop re-issued a deflected attempt immediately, ignoring the {@code Retry-After} the server sent.
 * <p>
 * The deflection is the real one, from {@code AbstractServerHttpHandler}'s gate, opened around exactly the request the
 * test wants refused. The pauses the retry loop asks for are recorded instead of slept.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8618RemoteCommitSessionReleaseIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-tx-8618";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }

  @Test
  void aDeflectedCommitLeavesNoSessionBehind() {
    try (final DeflectingDatabase database = newDatabase("commit", 1)) {
      database.command("sql", "CREATE DOCUMENT TYPE Person");

      final AtomicInteger executions = new AtomicInteger();
      database.transaction(() -> {
        executions.incrementAndGet();
        database.command("sql", "INSERT INTO Person SET name = 'Alice'");
      }, false, 3);

      assertThat(database.deflected.get()).isEqualTo(1);
      assertThat(executions.get()).isEqualTo(2);
      assertThat(database.countType("Person", false)).isEqualTo(1);
      assertThat(activeSessions()).as("the session of the refused commit must have been released").isZero();
      // THE SERVER'S Retry-After (#8617), READ FROM THE REAL ANSWER, SPREAD BY UP TO A TENTH OF IT
      assertRetryAfterPauses(database.pauses, 1);
    }
  }

  @Test
  void aDeflectedBeginWaitsTheRetryAfterTheServerSent() {
    try (final DeflectingDatabase database = newDatabase("begin", 2)) {
      final AtomicInteger executions = new AtomicInteger();
      database.transaction(executions::incrementAndGet, false, 3);

      assertThat(database.deflected.get()).isEqualTo(2);
      assertThat(executions.get()).isEqualTo(1);
      assertRetryAfterPauses(database.pauses, 2);
      assertThat(activeSessions()).isZero();
    }
  }

  /**
   * The server-side half: a commit that fails inside the handler ends its session, with no help from the client, and
   * says so in the answer, so the client sends no rollback to release it. The client here would not send one anyway.
   */
  @Test
  void aCommitThatFailsInsideTheHandlerEndsItsSession() {
    try (final DeflectingDatabase database = newDatabase("none", 0)) {
      database.command("sql", "CREATE DOCUMENT TYPE Account");
      database.command("sql", "CREATE PROPERTY Account.id INTEGER");
      database.command("sql", "CREATE INDEX ON Account (id) UNIQUE");

      database.releaseSessions = false;
      database.begin();
      database.command("sql", "INSERT INTO Account SET id = 1");
      assertThat(activeSessions()).isEqualTo(1);

      // ANOTHER CLIENT COMMITS THE SAME KEY FIRST
      try (final RemoteDatabase other = new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root",
          DEFAULT_PASSWORD_FOR_TESTS)) {
        other.command("sql", "INSERT INTO Account SET id = 1");
      }

      assertThatThrownBy(database::commit).isInstanceOf(DuplicatedKeyException.class);
      assertThat(activeSessions()).as("a failed commit must end its session on the server").isZero();
      assertThat(database.rollbacks.get()).as("the server said the session is closed: no rollback to send").isZero();
    }
  }

  private static void assertRetryAfterPauses(final List<Long> pauses, final int expected) {
    final long retryAfterMs = RetryLaterException.SNAPSHOT_INSTALL_RETRY_AFTER_SECONDS * 1_000L;
    assertThat(pauses).hasSize(expected);
    for (final long pause : pauses)
      assertThat(pause).isBetween(retryAfterMs, retryAfterMs + retryAfterMs / 10);
  }

  private int activeSessions() {
    return getServer(0).getHttpServer().getSessionManager().getActiveSessions();
  }

  private DeflectingDatabase newDatabase(final String route, final int deflections) {
    return new DeflectingDatabase(getServer(0), "127.0.0.1", getServerHttpPort(), DATABASE_NAME, route, deflections);
  }

  /**
   * Opens the server's snapshot-install window for the first {@code deflections} requests to one transaction route, so
   * the server itself answers them 503, and records the pauses of the retry loop instead of sleeping them.
   */
  private static class DeflectingDatabase extends RemoteDatabase {
    final         AtomicInteger  deflected        = new AtomicInteger();
    final         AtomicInteger  rollbacks        = new AtomicInteger();
    final         List<Long>     pauses           = new ArrayList<>();
    private final ArcadeDBServer server;
    private final String         route;
    private final AtomicInteger  remaining;
    boolean                      releaseSessions  = true;

    DeflectingDatabase(final ArcadeDBServer server, final String host, final int port, final String databaseName,
        final String route, final int deflections) {
      super(host, port, databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
      this.server = server;
      this.route = "/api/v1/" + route + "/";
      this.remaining = new AtomicInteger(deflections);
    }

    @Override
    public void rollback() {
      rollbacks.incrementAndGet();
      if (releaseSessions) {
        super.rollback();
        return;
      }
      setSessionId(null);
    }

    @Override
    protected void sleepBeforeRetry(final long delayMs) {
      pauses.add(delayMs);
    }

    @Override
    HttpResponse<String> sendWithWatchdog(final HttpRequest request) throws IOException, InterruptedException {
      if (!request.uri().getPath().contains(route) || remaining.getAndDecrement() <= 0)
        return super.sendWithWatchdog(request);

      server.setSnapshotInstallInProgress(true);
      try {
        final HttpResponse<String> response = super.sendWithWatchdog(request);
        assertThat(response.statusCode()).as("the server must have deflected the request").isEqualTo(503);
        deflected.incrementAndGet();
        return response;
      } finally {
        server.setSnapshotInstallInProgress(false);
      }
    }
  }
}
