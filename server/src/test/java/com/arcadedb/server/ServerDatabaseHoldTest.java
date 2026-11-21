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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MaintenanceCoordinator;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A database an operator holds - closed by {@code close database}, reserved by a restore, being dropped - is neither served
 * to a client nor reported as a missing name: requests do not reopen it, writes are refused, and it still exists for the
 * control plane.
 */
class ServerDatabaseHoldTest {
  /*
   * Issue #10553: {@link ArcadeDBServer#isDatabaseHeld} says a name an operator holds is a database although it is not registered,
   * which is what a protocol that guesses whether a key prefix names a database (Redis) needs: {@code existsDatabase} cannot tell a
   * held database from a name that is no database at all.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #10553 isDatabaseHeld answers true for a name an operator holds although it is not registered
  @Test
  void aNameNobodyHoldsIsNotHeld() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    assertThat(server.isDatabaseHeld("nobody")).isFalse();
    assertThat(server.existsDatabase("nobody")).isFalse();
  }

  // Regression: #10553 isDatabaseHeld answers true for a name an operator holds although it is not registered
  @Test
  void aRestoreHoldsItsTargetUntilItIsReleased() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    server.reserveDatabaseNameForRestore("restored");
    try {
      assertThat(server.isDatabaseHeld("restored")).isTrue();
      assertThat(server.existsDatabase("restored")).as("it is not registered").isFalse();
      assertThat(server.isDatabaseHeld("other")).as("only the name it claimed").isFalse();
    } finally {
      server.releaseDatabaseNameReservedForRestore("restored");
    }
    assertThat(server.isDatabaseHeld("restored")).isFalse();
  }

  // Regression: #10553 isDatabaseHeld answers true for a name an operator holds although it is not registered
  @Test
  void aCloseCommandHoldsTheDatabaseItClosedUntilItIsUnmarked() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    server.markClosedByCommand("closed");
    try {
      assertThat(server.isDatabaseHeld("closed")).isTrue();
    } finally {
      server.unmarkClosedByCommand("closed");
    }
    assertThat(server.isDatabaseHeld("closed")).isFalse();
  }

  // Regression: #10388 a request must not reopen a database a drop or an overwrite restore is deleting
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
  @Nested
  class DropAndRestoreMarks extends BaseGraphServerTest {
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
        assertThatThrownBy(() -> server.getDatabaseForOperator(NAME)).as("the binary protocols resolve through getDatabase")
            .isInstanceOf(DatabaseNotAvailableException.class);
        final HttpResponse<String> http = postQuery(NAME, "select from Doc");
        // 503 'being dropped' rather than the 404 of a name that is no database: the files are still there (issue #10872)
        assertThat(http.statusCode()).as("HTTP answers 'being dropped', not a database reopened under the delete").isEqualTo(503);
        assertThat(server.getDatabaseForRequest(getDatabaseName()).isOpen()).as("any other database is unaffected").isTrue();

        // A create of the same name is refused too: the name is still taken until the files are gone
        assertThatThrownBy(() -> server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE))
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class).hasMessageContaining("being dropped");

        // A second drop of the name is refused, and never ends the hold of the one running
        assertThatThrownBy(() -> server.dropLocalDatabase(created.getEmbedded(), NAME))
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class).hasMessageContaining("already being dropped");
        assertThatThrownBy(() -> server.getDatabaseForOperator(NAME)).as("the refused second drop left the first one's hold in place")
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
        // the database is there and a restore holds it: 503, not the 404 of a name that is no database (issue #10872)
        assertThat(postQuery(NAME, "select 1").statusCode()).isEqualTo(503);
        assertThat(server.existsDatabase(NAME)).as("the request opened nothing").isFalse();

        // The restore itself is not a request: it resolves the target it replaces through getDatabase, and is not refused
        assertThat(server.getDatabaseForOperator(NAME).isOpen()).isTrue();
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
        final DatabaseInternal embedded = server.getDatabaseForOperator(name).getEmbedded();
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

  // Regression: #10689 the target of an overwrite restore refuses writes for the whole download
  /**
   * Issue #10689: a database an {@code overwrite} restore has claimed stayed open and registered for the whole download, and every
   * resolver served it from its lock-free fast path, above the check of the claim. A client writing in that window was acknowledged,
   * and the swap then deleted the directory its rows were in.
   * <p>
   * The claim now refuses every WRITE to the database under its name, in the database itself: reads go on (the readers of a live
   * database keep it until the swap, as #10533 decided), and every commit carrying changes and every schema change is answered "not
   * available", whichever way the client reached the database - a resolution made now, a handle a session resolved before the
   * restore began (a Postgres connection keeps one for its life), or a transaction begun before the claim and committed inside it.
   * The resolution alone could not have done that: a refusal at resolution is not seen by a handle resolved earlier.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RestoreTargetRefusesWrites extends BaseGraphServerTest {
    private static final String NAME = "Issue10689Target";

    private ArcadeDBServer server;

    @BeforeEach
    void createTarget() {
      server = getServer(0);
      final ServerDatabase created = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
      created.getSchema().createDocumentType("T");
      created.transaction(() -> created.command("sql", "INSERT INTO T SET v = 1"));
    }

    @AfterEach
    void dropTarget() {
      server.releaseDatabaseNameReservedForRestore(NAME);
      if (server.existsDatabase(NAME))
        server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
      FileUtils.deleteRecursively(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
    }

    /** The report's own sequence: claimed while open and registered, resolved by a client, written to. */
    @Test
    void aClientWriteToTheOpenTargetOfARestoreIsRefused() {
      server.reserveDatabaseNameForRestore(NAME);

      for (final ServerDatabase resolved : new ServerDatabase[] { server.getDatabaseForClient(NAME), server.getDatabaseForRequest(NAME) }) {
        assertThat(count(resolved)).as("its readers keep it until the swap").isEqualTo(1L);

        assertThatThrownBy(() -> resolved.transaction(() -> resolved.command("sql", "INSERT INTO T SET v = 2")))
            .isInstanceOf(DatabaseNotAvailableException.class).hasMessageContaining("restore");
        assertThatThrownBy(() -> resolved.getSchema().createDocumentType("Other")).as("a schema change is a write too")
            .isInstanceOf(DatabaseNotAvailableException.class);
      }
      assertThat(count(server.getDatabaseForClient(NAME))).as("nothing was written").isEqualTo(1L);
    }

    /**
     * A handle resolved BEFORE the claim, as a Postgres session holds one for its life and a Bolt connection across its requests: no
     * check at resolution can reach it, the refusal in the database does.
     */
    @Test
    void aHandleResolvedBeforeTheRestoreBeganIsRefusedToo() {
      final ServerDatabase session = server.getDatabaseForClient(NAME);

      server.reserveDatabaseNameForRestore(NAME);

      assertThatThrownBy(() -> session.transaction(() -> session.command("sql", "INSERT INTO T SET v = 2")))
          .isInstanceOf(DatabaseNotAvailableException.class);
      assertThat(ErrorCategory.of(catchWrite(session))).as("what every wire protocol maps to its own 'not available'")
          .isEqualTo(ErrorCategory.UNAVAILABLE);
    }

    /** A transaction begun before the claim and committed inside it would be acknowledged and then destroyed just the same. */
    @Test
    void aTransactionBegunBeforeTheRestoreCannotCommitInsideIt() {
      final ServerDatabase session = server.getDatabaseForClient(NAME);
      session.begin();
      try {
        session.command("sql", "INSERT INTO T SET v = 2");

        server.reserveDatabaseNameForRestore(NAME);

        assertThatThrownBy(session::commit).isInstanceOf(DatabaseNotAvailableException.class);
      } finally {
        if (session.isTransactionActive())
          session.rollback();
      }
      assertThat(count(session)).isEqualTo(1L);
    }

    /**
     * A commit that passed the refusal's check just before the claim put it up is still writing: the restore waits for it before it
     * goes on, so it cannot complete during the download and be destroyed by the swap. A commit holds the database's shared lock
     * from its check to its last page, which is what the commit in flight stands for here.
     */
    @Test
    void theRestoreWaitsForACommitAlreadyPastTheCheck() throws Exception {
      final LocalDatabase local = (LocalDatabase) server.getDatabaseForOperator(NAME).getEmbedded();
      final CountDownLatch inFlight = new CountDownLatch(1);
      final CountDownLatch finish = new CountDownLatch(1);
      final ExecutorService pool = Executors.newFixedThreadPool(2);
      try {
        final Future<?> commit = pool.submit(() -> local.executeInReadLock(() -> {
          inFlight.countDown();
          assertThat(finish.await(30, TimeUnit.SECONDS)).isTrue();
          return null;
        }));
        assertThat(inFlight.await(30, TimeUnit.SECONDS)).isTrue();

        server.reserveDatabaseNameForRestore(NAME);
        final Future<?> drain = pool.submit(() -> server.awaitCommitsInFlightOn(NAME));

        assertThatThrownBy(() -> drain.get(300, TimeUnit.MILLISECONDS)).as("the commit in flight is waited for")
            .isInstanceOf(TimeoutException.class);

        finish.countDown();
        commit.get(30, TimeUnit.SECONDS);
        drain.get(30, TimeUnit.SECONDS);
      } finally {
        finish.countDown();
        pool.shutdownNow();
      }
    }

    /** A database setting saved into the target is a write too: the swap deletes the configuration.json it went to. */
    @Test
    void aSettingChangeIsRefusedWhileTheRestoreHoldsTheTarget() throws Exception {
      final String key = GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH.getKey();
      final int before = server.getDatabaseForOperator(NAME).getConfiguration().getValueAsInteger(GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH);

      server.reserveDatabaseNameForRestore(NAME);
      assertThatThrownBy(() -> new ServerControlPlane(server).setDatabaseSetting(NAME, key, "3"))
          .isInstanceOf(DatabaseNotAvailableException.class);
      assertThat(server.getDatabaseForOperator(NAME).getConfiguration().getValueAsInteger(GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH))
          .as("the setting was not applied").isEqualTo(before);

      server.releaseDatabaseNameReservedForRestore(NAME);
      new ServerControlPlane(server).setDatabaseSetting(NAME, key, "3");
      assertThat(server.getDatabaseForOperator(NAME).getConfiguration().getValueAsInteger(GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH))
          .isEqualTo(3);
    }

    /** The operator's own step may open the target while the claim stands: what it opens refuses writes as well. */
    @Test
    void aTargetOpenedWhileTheRestoreHoldsItRefusesWrites() {
      server.getDatabaseForOperator(NAME).getEmbedded().close();
      server.removeDatabase(NAME);

      server.reserveDatabaseNameForRestore(NAME);
      assertThatThrownBy(() -> server.getDatabaseForClient(NAME)).as("a client still cannot open it")
          .isInstanceOf(DatabaseNotAvailableException.class);

      final ServerDatabase opened = server.getDatabaseForOperator(NAME);
      assertThat(count(opened)).isEqualTo(1L);
      assertThatThrownBy(() -> opened.transaction(() -> opened.command("sql", "INSERT INTO T SET v = 2")))
          .isInstanceOf(DatabaseNotAvailableException.class);
    }

    /** A restore that ends - failed, so the target is still the one it found - gives the database its writes back. */
    @Test
    void theEndOfTheRestoreGivesTheWritesBack() {
      server.reserveDatabaseNameForRestore(NAME);
      server.releaseDatabaseNameReservedForRestore(NAME);

      final ServerDatabase resolved = server.getDatabaseForClient(NAME);
      resolved.transaction(() -> resolved.command("sql", "INSERT INTO T SET v = 2"));
      assertThat(count(resolved)).isEqualTo(2L);
    }

    /**
     * The restore's refusal and the one an HA server puts on a database before its plugin wraps it (issue #8270) are lifted each by
     * its own owner: ending one must not end the other, in either order.
     */
    @Test
    void theRestoreRefusalAndAnotherOneAreLiftedIndependently() {
      final LocalDatabase local = (LocalDatabase) server.getDatabaseForOperator(NAME).getEmbedded();
      final String other = "another operation refuses writes";
      try {
        local.refuseWrites(other);
        server.reserveDatabaseNameForRestore(NAME);
        assertThatThrownBy(local::checkWritesAccepted).as("the restore's 'not available' wins over a retryable refusal put first")
            .isInstanceOf(DatabaseNotAvailableException.class);

        local.acceptWrites(other);
        assertThat(local.getWriteRefusal()).as("the restore's refusal outlives the other one").isEqualTo(ArcadeDBServer.RESTORE_WRITE_REFUSAL);
        assertThatThrownBy(local::checkWritesAccepted).isInstanceOf(DatabaseNotAvailableException.class);

        local.refuseWrites(other);
        server.releaseDatabaseNameReservedForRestore(NAME);
        assertThat(local.getWriteRefusal()).as("the end of the restore leaves the other refusal in place").isEqualTo(other);
        assertThatThrownBy(local::checkWritesAccepted).isInstanceOf(NeedRetryException.class)
            .isNotInstanceOf(DatabaseNotAvailableException.class);
      } finally {
        local.acceptWrites(other);
      }
      assertThat(local.getWriteRefusal()).isNull();
    }

    private static long count(final ServerDatabase database) {
      return database.query("sql", "SELECT count(@rid) AS c FROM T").next().<Number>getProperty("c").longValue();
    }

    private static RuntimeException catchWrite(final ServerDatabase database) {
      try {
        database.transaction(() -> database.command("sql", "INSERT INTO T SET v = 3"));
      } catch (final RuntimeException e) {
        return e;
      }
      throw new AssertionError("the write was accepted");
    }
  }

  // Regression: #10872 a held database answers 'unavailable', not the 404 of a name that is no database
  /**
   * Issue #10872: HTTP answered "the database exists and cannot serve you now" - closed by {@code close database}, held by a
   * restore, being dropped - with the {@code 404 Database not found} of a name that is no database, with the same exception
   * class, so in production mode, which conceals the detail, the two bodies were identical. The resolver now raises
   * {@link DatabaseNotFoundException} for a name that is no database and keeps {@link DatabaseNotAvailableException}, with the
   * state as its reason, for one that is there; HTTP answers the first 404 and the second 503 under a label naming the state,
   * and the remote client rebuilds both types.
   * <p>
   * Run in production mode on purpose: the free-form detail is concealed there, so what tells the two apart has to be the
   * status, the label, the exception class and its arguments.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UnavailableIsNotNotFound extends BaseGraphServerTest {
    private static final String NAME    = "Issue10872Held";
    private static final String MISSING = "Issue10872NoSuchDatabase";

    private ArcadeDBServer server;

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      config.setValue(GlobalConfiguration.SERVER_MODE, "production");
    }

    @BeforeEach
    void createHeldDatabase() {
      server = getServer(0);
      final ServerDatabase created = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
      created.getSchema().createDocumentType("T");
      created.transaction(() -> created.command("sql", "INSERT INTO T SET k = 1"));
    }

    @AfterEach
    void dropHeldDatabase() {
      server.releaseDatabaseNameReservedForRestore(NAME);
      server.unmarkClosedByCommand(NAME);
      if (server.existsDatabase(NAME))
        server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
      FileUtils.deleteRecursively(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
    }

    @Test
    void aNameThatIsNoDatabaseIsNotFound() throws Exception {
      for (final HttpResponse<String> response : new HttpResponse[] { post("query", MISSING, "select 1"),
          post("command", MISSING, "insert into T set k = 2") }) {
        assertThat(response.statusCode()).isEqualTo(404);
        final JSONObject body = new JSONObject(response.body());
        assertThat(body.getString("error")).isEqualTo("Database not found");
        assertThat(body.getString("exception")).isEqualTo(DatabaseNotFoundException.class.getName());
        assertThat(body.has("detail")).as("production mode conceals the detail").isFalse();
      }

      // the resolver itself: a name with no database on disk reaches the allowLoad=false refusal
      assertThatThrownBy(() -> server.getDatabaseForRequest(MISSING)).isInstanceOf(DatabaseNotFoundException.class);
      assertThatThrownBy(() -> server.getDatabaseForRequest("..")).as("a name that leaves the database directory")
          .isInstanceOf(DatabaseNotFoundException.class);
    }

    /** The report's run 1: a database a {@code close database} command closed is there, and says so. */
    @Test
    void aDatabaseClosedByCommandIsUnavailableNotMissing() throws Exception {
      assertThat(serverCommand("close database " + NAME).statusCode()).isEqualTo(200);

      final HttpResponse<String> closed = post("query", NAME, "select from T");
      final HttpResponse<String> missing = post("query", MISSING, "select from T");
      assertThat(closed.statusCode()).isEqualTo(503);
      assertThat(missing.statusCode()).isEqualTo(404);

      final JSONObject body = new JSONObject(closed.body());
      assertThat(body.getString("error")).isEqualTo("Database is closed");
      assertThat(body.getString("exception")).isEqualTo(DatabaseNotAvailableException.class.getName());
      assertThat(body.getString("exceptionArgs")).isEqualTo("CLOSED");
      assertThat(body.has("detail")).as("production mode conceals the detail").isFalse();
      assertThat(closed.headers().firstValue("Retry-After")).as("how long an operator keeps it closed is unknown").isEmpty();

      assertThat(post("command", NAME, "insert into T set k = 2").statusCode()).isEqualTo(503);
      assertThatThrownBy(() -> server.getDatabaseForRequest(NAME)).isInstanceOf(DatabaseNotAvailableException.class)
          .isNotInstanceOf(DatabaseNotFoundException.class)
          .satisfies(e -> assertThat(((DatabaseNotAvailableException) e).getReason()).isEqualTo(DatabaseNotAvailableException.Reason.CLOSED));

      // the remote client is told the state, typed, and does not spin retrying a database closed on purpose
      try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), NAME, "root",
          DEFAULT_PASSWORD_FOR_TESTS)) {
        assertThatThrownBy(() -> remote.query("sql", "select from T").close()).isInstanceOf(DatabaseNotAvailableException.class)
            .isNotInstanceOf(DatabaseNotFoundException.class)
            .satisfies(e -> assertThat(((DatabaseNotAvailableException) e).getReason()).isEqualTo(DatabaseNotAvailableException.Reason.CLOSED));
      }
      try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), MISSING, "root",
          DEFAULT_PASSWORD_FOR_TESTS)) {
        assertThatThrownBy(() -> remote.query("sql", "select 1").close()).isInstanceOf(DatabaseNotFoundException.class);
      }

      assertThat(serverCommand("open database " + NAME).statusCode()).isEqualTo(200);
      assertThat(post("query", NAME, "select from T").statusCode()).isEqualTo(200);
    }

    /** The report's run 2: in the same second a read of a database a restore holds answers 200 and a write 503, never 404. */
    @Test
    void aWriteToADatabaseARestoreHoldsIsUnavailableNotMissing() throws Exception {
      server.reserveDatabaseNameForRestore(NAME);
      try {
        assertThat(post("query", NAME, "select count(*) as c from T").statusCode()).isEqualTo(200);

        final HttpResponse<String> write = post("command", NAME, "insert into T set k = 2");
        assertThat(write.statusCode()).isEqualTo(503);
        final JSONObject body = new JSONObject(write.body());
        assertThat(body.getString("error")).isEqualTo("Database does not accept writes");
        assertThat(body.getString("exceptionArgs")).isEqualTo("WRITES_REFUSED");

        assertThat(post("command", MISSING, "insert into T set k = 2").statusCode()).isEqualTo(404);
      } finally {
        server.releaseDatabaseNameReservedForRestore(NAME);
      }
      assertThat(post("command", NAME, "insert into T set k = 3").statusCode()).isEqualTo(200);
    }

    /** A restore holding a name that is not open: no client opens it, and the refusal names the restore. */
    @Test
    void aDatabaseARestoreHoldsClosedIsBeingRestored() throws Exception {
      server.getDatabaseForOperator(NAME).getEmbedded().close();
      server.removeDatabase(NAME);
      server.reserveDatabaseNameForRestore(NAME);
      try {
        final HttpResponse<String> response = post("query", NAME, "select 1");
        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(new JSONObject(response.body()).getString("error")).isEqualTo("Database is being restored");
      } finally {
        server.releaseDatabaseNameReservedForRestore(NAME);
      }
    }

    private HttpResponse<String> post(final String endpoint, final String database, final String command) throws Exception {
      return send(HttpRequest.newBuilder().uri(URI.create(getServerHttpUrl(0, "/api/v1/" + endpoint + "/" + database)))
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", command).toString())));
    }

    private HttpResponse<String> serverCommand(final String command) throws Exception {
      return send(HttpRequest.newBuilder().uri(URI.create(getServerHttpUrl(0, "/api/v1/server")))
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString())));
    }

    private static HttpResponse<String> send(final HttpRequest.Builder builder) throws Exception {
      final HttpRequest request = builder.header("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
          .header("Content-Type", "application/json").build();
      try (final HttpClient client = HttpClient.newHttpClient()) {
        return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      }
    }
  }

  // Regression: #11310 the control plane reports a held database as existing
  /**
   * Issue #11310: the control plane asked whether a database exists with the question of the data path, "may a client's first
   * request open it", which is false for a database an operator holds - closed by {@code close database}, reserved by a restore,
   * being dropped. Such a database exists: its name is refused by a create, and an operator acts on it. So {@code GET /exists}
   * answered false for a database a create of the same name then refused with 409, and {@code drop database} answered "does not
   * exist" for a database on disk that {@code open database} brought straight back. The invariant pinned here, for each state: the
   * name exists exactly when a create refuses it, and a drop either drops it or refuses with the reason it is held.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class HeldDatabaseExists extends BaseGraphServerTest {
    private static final String HELD = "held11310";

    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void dropTheHeldDatabase() {
      final ArcadeDBServer server = getServer(0);
      if (server == null || !server.isStarted())
        return;
      if (server.isDatabaseOfServer(HELD))
        new ServerControlPlane(server).dropDatabase(HELD);
    }

    @Test
    void anOpenDatabaseExistsAndItsNameIsTaken() throws Exception {
      assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(200);

      assertThat(exists(HELD)).isTrue();
      assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(409);
    }

    @Test
    void aDatabaseClosedByCommandExistsAndIsDropped() throws Exception {
      assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(200);
      assertThat(serverCommand("close database " + HELD).statusCode()).isEqualTo(200);

      // IT USED TO ANSWER false, AND THE CREATE THAT FOLLOWED 409
      assertThat(exists(HELD)).isTrue();
      assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(409);

      // IT USED TO ANSWER "does not exist" AND LEAVE THE DATABASE ON DISK
      final HttpResponse<String> drop = serverCommand("drop database " + HELD);
      assertThat(drop.statusCode()).as(drop.body()).isEqualTo(200);
      assertThat(databaseDirectory(HELD)).doesNotExist();
      assertThat(exists(HELD)).isFalse();
      assertThat(getServer(0).isDatabaseHeld(HELD)).as("the close mark goes with the database").isFalse();
    }

    @Test
    void aDatabaseBeingRestoredExistsAndIsNotDroppedFromUnderTheRestore() throws Exception {
      assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(200);
      beginRestore();
      try {
        assertThat(exists(HELD)).isTrue();
        assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(409);

        // REFUSED WITH THE REASON IT IS HELD, NEVER "does not exist"
        final HttpResponse<String> drop = serverCommand("drop database " + HELD);
        assertThat(drop.statusCode()).isEqualTo(409);
        assertThat(drop.body()).contains("restore").doesNotContain("does not exist");
        assertThat(databaseDirectory(HELD)).exists();
      } finally {
        endRestore();
      }

      assertThat(serverCommand("drop database " + HELD).statusCode()).isEqualTo(200);
      assertThat(exists(HELD)).isFalse();
    }

    @Test
    void aNameReservedForTheRestoreOfANewDatabaseExistsWhileTheRestoreRuns() throws Exception {
      beginRestore();
      try {
        assertThat(exists(HELD)).isTrue();
        assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(409);
      } finally {
        endRestore();
      }
      assertThat(exists(HELD)).isFalse();
    }

    /** What a restore takes, in its order: the maintenance slot of the database, then the claim on its name. */
    private void beginRestore() {
      assertThat(getServer(0).getBackupCoordinator().begin(HELD, MaintenanceCoordinator.Operation.RESTORE)).isNull();
      getServer(0).reserveDatabaseNameForRestore(HELD);
    }

    private void endRestore() {
      getServer(0).releaseDatabaseNameReservedForRestore(HELD);
      getServer(0).getBackupCoordinator().end(HELD, MaintenanceCoordinator.Operation.RESTORE);
    }

    @Test
    void aNameThatIsNoDatabaseDoesNotExist() throws Exception {
      assertThat(exists(HELD)).isFalse();
      final HttpResponse<String> drop = serverCommand("drop database " + HELD);
      assertThat(drop.statusCode()).isEqualTo(400);
    }

    private File databaseDirectory(final String name) {
      return new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), name);
    }

    private boolean exists(final String name) throws Exception {
      final HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/exists/" + name)))
          .header("Authorization", authorization()).GET().build(), HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return new JSONObject(response.body()).getBoolean("result");
    }

    private HttpResponse<String> serverCommand(final String command) throws Exception {
      return http.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/server")))
          .header("Authorization", authorization()).header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString(), StandardCharsets.UTF_8))
          .build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String authorization() {
      return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    }
  }
}
