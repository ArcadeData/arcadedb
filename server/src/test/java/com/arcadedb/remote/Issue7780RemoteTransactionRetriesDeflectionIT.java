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

import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7780: {@link RemoteDatabase#begin()} and {@link RemoteDatabase#commit()} wrapped a retryable
 * {@link NeedRetryException} (the 503 a server answers while it installs a snapshot, a quorum it cannot reach yet, a
 * leader change) in a {@link TransactionException}, which {@link RemoteDatabase#transaction} does not retry. The
 * configured retry budget was therefore never spent, although the server had refused the request before running it.
 * <p>
 * The deflection is the real one: the test opens the server's snapshot-install window around exactly the request it
 * wants refused, so the answer comes from {@code AbstractServerHttpHandler}'s gate and is decoded by the client's own
 * {@code manageException}.
 */
class Issue7780RemoteTransactionRetriesDeflectionIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-tx-7780";

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

  /**
   * The unambiguous half of the issue: a refused {@code /begin} created no transaction and ran nothing, so the retry
   * loop must spend its budget on it instead of failing on the first attempt.
   */
  @Test
  void aDeflectedBeginIsRetriedByTransaction() {
    try (final DeflectingDatabase database = newDatabase("begin", 1)) {
      database.command("sql", "CREATE DOCUMENT TYPE Person");

      final AtomicInteger executions = new AtomicInteger();
      final boolean createdNewTx = database.transaction(() -> {
        executions.incrementAndGet();
        database.command("sql", "INSERT INTO Person SET name = 'Alice'");
      }, false, 3);

      assertThat(createdNewTx).isTrue();
      assertThat(database.deflected.get()).as("the first /begin must have been refused by the server").isEqualTo(1);
      assertThat(executions.get()).as("the block must not run for the refused begin, and must run once after it")
          .isEqualTo(1);
      assertThat(database.countType("Person", false)).isEqualTo(1);
    }
  }

  /**
   * A refused {@code /commit} is answered before the handler runs, so the transaction never committed: the retry must
   * run the block again and leave exactly one record, not zero and not two.
   */
  @Test
  void aDeflectedCommitIsRetriedByTransaction() {
    try (final DeflectingDatabase database = newDatabase("commit", 1)) {
      database.command("sql", "CREATE DOCUMENT TYPE Person");

      final AtomicInteger executions = new AtomicInteger();
      database.transaction(() -> {
        executions.incrementAndGet();
        database.command("sql", "INSERT INTO Person SET name = 'Alice'");
      }, false, 3);

      assertThat(database.deflected.get()).as("the first /commit must have been refused by the server").isEqualTo(1);
      assertThat(executions.get()).isEqualTo(2);
      assertThat(database.countType("Person", false)).as("only the attempt that committed may leave a record")
          .isEqualTo(1);
    }
  }

  /**
   * The raw calls hand the retryable type to their own caller too, instead of burying it as the cause of a
   * {@link TransactionException}.
   */
  @Test
  void aDeflectedBeginSurfacesAsNeedRetryException() {
    try (final DeflectingDatabase database = newDatabase("begin", 1)) {
      assertThatThrownBy(database::begin).isInstanceOf(NeedRetryException.class)
          .isNotInstanceOf(TransactionException.class);
      assertThat(database.isTransactionActive()).isFalse();

      // The window is closed again: the next begin goes through.
      database.begin();
      assertThat(database.isTransactionActive()).isTrue();
      database.rollback();
    }
  }

  /**
   * The budget is still a budget: a server that keeps refusing is given up on after the configured attempts, with the
   * retryable exception rather than a wrapped one.
   */
  @Test
  void aServerThatKeepsRefusingExhaustsTheBudget() {
    try (final DeflectingDatabase database = newDatabase("begin", Integer.MAX_VALUE)) {
      final AtomicInteger executions = new AtomicInteger();
      assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, false, 3))
          .isInstanceOf(NeedRetryException.class);

      assertThat(database.deflected.get()).isEqualTo(3);
      assertThat(executions.get()).isZero();
    }
  }

  private DeflectingDatabase newDatabase(final String route, final int deflections) {
    return new DeflectingDatabase(getServer(0), "127.0.0.1", getServerHttpPort(), DATABASE_NAME, route, deflections);
  }

  /**
   * Opens the server's snapshot-install window for the first {@code deflections} requests to one transaction route,
   * so the server itself answers them 503, and counts them.
   */
  private static class DeflectingDatabase extends RemoteDatabase {
    final         AtomicInteger  deflected = new AtomicInteger();
    private final ArcadeDBServer server;
    private final String         route;
    private final AtomicInteger  remaining;

    DeflectingDatabase(final ArcadeDBServer server, final String host, final int port, final String databaseName,
        final String route, final int deflections) {
      super(host, port, databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
      this.server = server;
      this.route = "/api/v1/" + route + "/";
      this.remaining = new AtomicInteger(deflections);
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
