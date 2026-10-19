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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10388: #10234 made an HTTP request open a database that is on disk and not open, and gave the operator's
 * {@code close database} a mark so a request does not reopen it mid-close. The two operations that DELETE the files - a
 * {@code drop database}, and the drop an overwrite restore performs on its target - had no such mark. {@code drop()} closes the
 * instance and only then runs the recursive delete, and for as long as that delete has not reached {@code schema.json} the
 * directory still holds a database: a request arriving in that window loaded the directory that was being deleted, and the
 * outcome was a torn-database error that reads as corruption, a live instance whose files were removed under it, or an open
 * instance deregistered behind its back that refused the name when it was created again.
 * <p>
 * The window is held open deterministically: the embedded instance is replaced by a JDK proxy whose {@code drop()} does what
 * {@code LocalDatabase.drop()} does - close, then delete - but waits between the two for the test.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10388RequestsDoNotReopenADatabaseBeingTakenAwayTest extends BaseGraphServerTest {
  private static final String NAME = "Issue10388Dropped";

  @Test
  void aRequestDuringTheDeleteDoesNotReopenTheDatabaseBeingDropped() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final ServerDatabase created = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    created.getSchema().createDocumentType("Doc");
    final File directory = new File(created.getDatabasePath());

    final CountDownLatch closedNotDeleted = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final DatabaseInternal blocking = blockingDrop(created.getEmbedded(), closedNotDeleted, release, false);
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      final Future<?> drop = pool.submit(() -> server.dropLocalDatabase(blocking, NAME));

      assertThat(closedNotDeleted.await(30, TimeUnit.SECONDS)).as("the drop reached the window").isTrue();
      assertThat(ArcadeDBServer.holdsDatabase(directory)).as("in the window the directory still holds a database").isTrue();

      // Every way a request resolves the name: none of them may bring the directory being deleted back
      assertThatThrownBy(() -> server.getDatabaseForRequest(NAME)).isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("being dropped");
      assertThatThrownBy(() -> server.getDatabase(NAME)).as("the binary protocols resolve through getDatabase")
          .isInstanceOf(DatabaseNotAvailableException.class);
      final HttpResponse<String> http = postQuery(NAME, "select from Doc");
      assertThat(http.statusCode()).as("HTTP answers 'not found', not a database reopened under the delete").isEqualTo(404);
      assertThat(server.getDatabaseForRequest(getDatabaseName()).isOpen()).as("any other database is unaffected").isTrue();

      // A create of the same name is refused too: the name is still taken until the files are gone
      assertThatThrownBy(() -> server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE))
          .isInstanceOf(ServerControlPlane.OperationInProgressException.class).hasMessageContaining("being dropped");

      // A second drop of the name is refused, and never ends the hold of the one running
      assertThatThrownBy(() -> server.dropLocalDatabase(created.getEmbedded(), NAME))
          .isInstanceOf(ServerControlPlane.OperationInProgressException.class).hasMessageContaining("already being dropped");
      assertThatThrownBy(() -> server.getDatabase(NAME)).as("the refused second drop left the first one's hold in place")
          .isInstanceOf(DatabaseNotAvailableException.class);

      release.countDown();
      drop.get(30, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      pool.shutdownNow();
    }

    assertThat(directory).as("the files are gone").doesNotExist();
    assertThat(server.existsDatabase(NAME)).as("and so is the registry entry").isFalse();
    assertThatThrownBy(() -> server.getDatabaseForRequest(NAME)).as("nothing to open once it is dropped")
        .isInstanceOf(DatabaseNotAvailableException.class);

    // The mark ended with the drop: the name can be created again, which the leaked open instance of the defect refused
    final ServerDatabase again = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    try {
      assertThat(server.getDatabaseForRequest(NAME).isOpen()).isTrue();
      assertThat(again.isOpen()).isTrue();
    } finally {
      dropIfPresent(server, NAME);
    }
  }

  @Test
  void aDropThatFailsReleasesTheName() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final ServerDatabase created = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    final DatabaseInternal failing = blockingDrop(created.getEmbedded(), new CountDownLatch(1), new CountDownLatch(0), true);
    try {
      assertThatThrownBy(() -> server.dropLocalDatabase(failing, NAME)).isInstanceOf(DatabaseOperationException.class);

      // The files are still there and the name is not held: a request opens the database as it would have before the drop
      assertThat(server.getDatabaseForRequest(NAME).isOpen()).isTrue();
    } finally {
      dropIfPresent(server, NAME);
    }
  }

  @Test
  void aRequestDuringARestoreDoesNotOpenTheTargetItIsAboutToReplace() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final ServerDatabase created = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    // ON DISK, NOT OPEN, NOT CLOSED BY ANYBODY: what an overwrite restore finds when it meets a database nobody has opened yet
    created.getEmbedded().close();
    server.removeDatabase(NAME);
    assertThat(server.existsDatabase(NAME)).isFalse();

    server.reserveDatabaseNameForRestore(NAME);
    try {
      assertThatThrownBy(() -> server.getDatabaseForRequest(NAME)).isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("restore");
      assertThat(postQuery(NAME, "select 1").statusCode()).isEqualTo(404);
      assertThat(server.existsDatabase(NAME)).as("the request opened nothing").isFalse();

      // The restore itself is not a request: it resolves the target it replaces through getDatabase, and is not refused
      assertThat(server.getDatabase(NAME).isOpen()).isTrue();
      // Once it is open it is served as before: the reservation takes no live database away from its readers
      assertThat(server.getDatabaseForRequest(NAME).isOpen()).isTrue();
    } finally {
      server.releaseDatabaseNameReservedForRestore(NAME);
      dropIfPresent(server, NAME);
    }
  }

  @Test
  void aRequestOpensAClosedDatabaseOnceTheRestoreHasEnded() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final ServerDatabase created = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    created.getEmbedded().close();
    server.removeDatabase(NAME);

    server.reserveDatabaseNameForRestore(NAME);
    server.releaseDatabaseNameReservedForRestore(NAME);
    try {
      assertThat(server.getDatabaseForRequest(NAME).isOpen()).as("a released reservation holds nothing").isTrue();
    } finally {
      dropIfPresent(server, NAME);
    }
  }

  @Test
  void theDropDatabaseCommandStillDropsAndTheNameCanBeCreatedAgain() throws Exception {
    final ArcadeDBServer server = getServer(0);
    server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);

    new ServerControlPlane(server).dropDatabase(NAME);

    assertThat(server.existsDatabase(NAME)).isFalse();
    assertThat(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME))
        .doesNotExist();
    try {
      assertThat(server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE).isOpen()).isTrue();
    } finally {
      dropIfPresent(server, NAME);
    }
  }

  /**
   * An embedded instance whose {@code drop()} closes the database, tells the test, waits for it, and only then deletes the files:
   * {@code LocalDatabase.drop()}, with the window between its two steps held open. Everything else is the real instance's.
   */
  private static DatabaseInternal blockingDrop(final DatabaseInternal real, final CountDownLatch closedNotDeleted,
      final CountDownLatch release, final boolean failInsteadOfDeleting) {
    return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(), new Class<?>[] { DatabaseInternal.class },
        (proxy, method, args) -> {
          if (method.getName().equals("drop") && method.getParameterCount() == 0) {
            real.closeForDrop();
            closedNotDeleted.countDown();
            assertThat(release.await(30, TimeUnit.SECONDS)).isTrue();
            if (failInsteadOfDeleting)
              throw new DatabaseOperationException("The recursive delete failed");
            FileUtils.deleteRecursively(new File(real.getDatabasePath()));
            return null;
          }
          try {
            return method.invoke(real, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  private static void dropIfPresent(final ArcadeDBServer server, final String name) {
    if (server.existsDatabase(name)) {
      final DatabaseInternal embedded = server.getDatabase(name).getEmbedded();
      server.dropLocalDatabase(embedded, name);
    }
    FileUtils.deleteRecursively(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), name));
  }

  private HttpResponse<String> postQuery(final String database, final String query) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/query/" + database)))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", query).toString()))
        .build();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }
  }
}
