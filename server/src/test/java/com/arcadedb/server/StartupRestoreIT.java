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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import com.arcadedb.engine.MaintenanceCoordinator;
import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code restore:} startup command of {@code arcadedb.server.defaultDatabases}: its published progress, the maintenance
 * slot it takes, and its local-URL policy.
 */
class StartupRestoreIT {
  // Regression: #7440 the startup restore publishes its progress
  /**
   * Issue #7440: #7385 gave every restore that reaches {@code ServerControlPlane.performRestore} a published
   * {@code OperationProgress} - HTTP {@code restore database}, HTTP {@code restore backup}, gRPC
   * {@code RestoreDatabase} and gRPC {@code RestoreBackup}. The {@code restore:} startup command of
   * {@code arcadedb.server.defaultDatabases} does not go through the control plane and stayed silent, so a
   * container booting against a large archive answered "nothing running" on the very database it was building.
   * <p>
   * The observability claim the issue rests on is testable rather than theoretical:
   * {@code ArcadeDBServer.start()} calls {@code httpServer.startService()} before
   * {@code loadDefaultDatabases()}, and {@code GetProgressHandler} reads only the lock-free registry snapshot -
   * no database access - so a poll that lands during a startup restore is served.
   * {@link #theProgressEndpointReportsAStartupRestoreWhileItRuns} is exactly that poll, against a real server
   * boot whose archive is trickled out by a local HTTP server so the window is one the test controls rather
   * than one it hopes for.
   */
  @Nested
  class Progress extends BaseGraphServerTest {
    private static final String RESTORED_DB    = "graph";
    private static final String SOURCE_DB      = "source7440";
    private static final String ARCHIVE_NAME   = "backup-7440.zip";
    private static final int    DOCUMENT_COUNT = 200;
    private static final int    TYPE_COUNT     = 6;
    private static final int    ARCHIVE_SLICES = 30;
    /**
     * The IPv4 loopback LITERAL, used for both ends: the host the server is told to bind and the host the poller
     * dials. The name {@code localhost} resolves to {@code ::1} or {@code 127.0.0.1} depending on the machine and
     * the moment, so a server bound by name and a client dialling the same name can still end up on different
     * stacks - and on {@code ::1} the request is answered by whatever else holds the wildcard address, which
     * replies with somebody else's HTTP status rather than with a connection error that would name the problem.
     */
    private static final String LOOPBACK       = "127.0.0.1";
    private static final long   SLICE_PAUSE_MS = 50;

    /** Samples of the progress endpoint's payload taken while the server was booting. */
    private final List<JSONObject> endpointSamples = Collections.synchronizedList(new ArrayList<>());
    /** Samples taken straight off the registry, which needs no listener and so covers the pre-HTTP window too. */
    private final List<JSONObject> registrySamples = Collections.synchronizedList(new ArrayList<>());

    private final HttpClient client = HttpClient.newHttpClient();
    /**
     * Distinct {@code status@port} outcomes of the polls, reported when the endpoint assertion fails. A poll that
     * reaches somebody else's server reads as an authorization failure rather than as a port conflict, so name
     * the status and the port instead of leaving the next reader to guess at a bare "reported nothing".
     */
    private final Set<String> pollOutcomes = ConcurrentHashMap.newKeySet();

    private          HttpServer archiveServer;
    private          Thread     sampler;
    private volatile boolean    sampling;
    private          File       archive;

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      super.onServerConfiguration(config);

      archive = produceArchive();
      archiveServer = serveArchiveSlowly(archive);

      // The archive is served from loopback, which the SSRF guard blocks by default. Since issue #7468 the startup
      // restore resolves this against the server's own ContextConfiguration, which - with no overlay for the key,
      // and this test sets none - falls through to the static global, so setting it there is still what this test
      // needs. beginTest() has already run resetAll(), so this is not leaking into another class.
      GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);

      // A dedicated port, not the shared 2480-2489 range this suite defaults to. This test polls an HTTP endpoint
      // several hundred times while the server boots and asserts on what comes back, so a neighbouring server -
      // another test class mid-shutdown, an IDE, a locally installed ArcadeDB - answering even once would be read
      // as the server under test saying something it never said.
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, LOOPBACK);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(allocateFreePorts(1)[0]));

      config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES,
          RESTORED_DB + "[albert:einstein:admin]{restore:http://" + LOOPBACK + ":"
              + archiveServer.getAddress().getPort() + "/" + ARCHIVE_NAME + "}");
    }

    @Override
    protected void onBeforeStarting(final ArcadeDBServer server) {
      // Started BEFORE server.start(): loadDefaultDatabases() runs inside start(), so a sampler that only began
      // afterwards would have nothing left to observe.
      sampling = true;
      sampler = new Thread(() -> {
        while (sampling) {
          for (final OperationProgress op : OperationProgressRegistry.instance().getOperations(RESTORED_DB))
            registrySamples.add(op.toJSON());
          pollTheProgressEndpoint(server);
          try {
            Thread.sleep(5);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }, "issue7440-sampler");
      sampler.setDaemon(true);
      sampler.start();
    }

    @AfterEach
    @Override
    public void endTest() {
      stopSampling();
      if (archiveServer != null)
        archiveServer.stop(0);
      try {
        super.endTest();
      } finally {
        FileUtils.deleteRecursively(new File("./target/backups"));
      }
    }

    /**
     * The whole point of the issue: boot a server whose {@code defaultDatabases} carries a {@code restore:}
     * command, and poll {@code GET /api/v1/progress/{database}} while it boots.
     */
    @Test
    @Timeout(180)
    void theProgressEndpointReportsAStartupRestoreWhileItRuns() {
      stopSampling();

      assertThat(registrySamples)
          .as("the startup restore published nothing to the operation progress registry").isNotEmpty();
      assertThat(registrySamples).allSatisfy(op -> {
        assertThat(op.getString("database")).isEqualTo(RESTORED_DB);
        assertThat(op.getString("operation")).isEqualTo("restore database");
      });

      assertThat(endpointSamples)
          .as("GET /api/v1/progress/%s reported nothing for the whole startup restore; poll outcomes (status@port) were %s",
              RESTORED_DB, pollOutcomes).isNotEmpty();
      assertThat(endpointSamples).allSatisfy(op -> {
        assertThat(op.getString("database")).isEqualTo(RESTORED_DB);
        assertThat(op.getString("operation")).isEqualTo("restore database");
        assertThat(op.getString("stepName")).isNotBlank();
        assertThat(op.getInt("totalSteps")).isEqualTo(2);
      });

      // The archive is trickled out over ARCHIVE_SLICES * SLICE_PAUSE_MS, so the extraction is spread across a
      // window wide enough to observe the per-entry counter ADVANCING rather than only being published. Nothing
      // else here would notice a callback that is installed but never fed, since the two coarse step markers
      // are written by the server itself.
      assertThat(registrySamples).anySatisfy(op -> {
        assertThat(op.getInt("stepIndex")).as("the extraction step was never observed").isEqualTo(1);
        assertThat(op.getLong("done")).as("no per-entry count ever reached the registry").isGreaterThan(0L);
      });

      // The restore actually happened, and the operation did not outlive it.
      final Database database = getServer(0).getDatabaseForOperator(RESTORED_DB);
      long restored = 0;
      for (int t = 0; t < TYPE_COUNT; t++)
        restored += database.countType(typeName(t), true);
      assertThat(restored).isEqualTo(DOCUMENT_COUNT);
      assertThat(OperationProgressRegistry.instance().getOperations(RESTORED_DB))
          .as("the operation must be retired when the startup restore returns").isEmpty();
    }

    /**
     * A local archive takes the parallel extractor, which knows the archive's entry count up front, so a real
     * denominator has to reach the registry. This is the only assertion that can catch the progress callback
     * never being installed on the restorer: that failure is swallowed by design, so the coarse step markers
     * would still be published and every other assertion here would still pass.
     */
    @Test
    @Timeout(180)
    void aStartupRestorePublishesTheArchiveCounters() {
      stopSampling();

      final String target = "counters7440";
      final List<JSONObject> observed = Collections.synchronizedList(new ArrayList<>());
      final Thread watcher = watch(target, observed);

      try {
        getServer(0).restoreDatabaseFromStartupCommand(target, "file://" + archive.getAbsolutePath(),
            databaseDirectory() + File.separator + target);
      } finally {
        stop(watcher);
      }

      assertThat(observed).as("nothing was published while the restore ran").isNotEmpty();
      // Only the denominator is asserted here. A local archive restores in a couple of milliseconds, so which
      // numerator a sampler happens to catch is a race; the DENOMINATOR is not, and it is the one that can only
      // be there if the callback was installed. `done` advancing is asserted instead by
      // theProgressEndpointReportsAStartupRestoreWhileItRuns, whose archive is trickled out over seconds.
      assertThat(observed).anySatisfy(op ->
          assertThat(op.getLong("total")).as("the archive entry count never reached the registry").isGreaterThan(0L));
      assertThat(observed).allSatisfy(op -> {
        assertThat(op.getString("operation")).isEqualTo("restore database");
        assertThat(op.getInt("totalSteps")).isEqualTo(2);
        assertThat(op.getInt("stepIndex")).isBetween(1, 2);
      });

      assertThat(getServer(0).existsDatabase(target)).isTrue();
      assertThat(OperationProgressRegistry.instance().getOperations(target)).isEmpty();
    }

    /**
     * The half that matters more than the publication itself: a startup restore that fails must not leave a
     * phantom operation behind for the rest of the life of the process.
     */
    @Test
    @Timeout(180)
    void aFailedStartupRestoreRetiresTheOperation() {
      stopSampling();

      final String target = "failed7440";
      assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(target,
          "file://" + new File("./target/does-not-exist-7440.zip").getAbsolutePath(),
          databaseDirectory() + File.separator + target)).isInstanceOf(RuntimeException.class);

      assertThat(OperationProgressRegistry.instance().getOperations(target))
          .as("a failed startup restore must retire its operation too").isEmpty();
      assertThat(getServer(0).existsDatabase(target)).isFalse();
    }

    // ------------------------------------------------------------------------------------------------- HELPERS

    /** Creates a throwaway database, backs it up, drops it, and returns the archive. */
    private File produceArchive() {
      final String databaseDirectory = GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + "0";
      FileUtils.deleteRecursively(new File("./target/backups"));

      try (final DatabaseFactory factory = new DatabaseFactory(databaseDirectory + File.separator + SOURCE_DB)) {
        try (final Database database = factory.create()) {
          // Several types, not one: every bucket is an archive entry, and the per-entry counter this test
          // watches only advances once an entry is fully extracted. One type would put almost all the bytes in
          // a single entry, so the counter would stay at 0 until the very end of the window.
          for (int t = 0; t < TYPE_COUNT; t++)
            database.getSchema().createDocumentType(typeName(t));
          database.transaction(() -> {
            for (int i = 0; i < DOCUMENT_COUNT; i++)
              database.newDocument(typeName(i % TYPE_COUNT)).set("i", i).set("payload", "x".repeat(512)).save();
          });
          database.command("sql", "backup database file://" + ARCHIVE_NAME).close();
          database.drop();
        }
      }

      final File produced = new File("./target/backups/" + SOURCE_DB + "/" + ARCHIVE_NAME);
      assertThat(produced).exists();
      return produced;
    }

    /**
     * Serves the archive in {@link #ARCHIVE_SLICES} slices {@link #SLICE_PAUSE_MS} apart: a second and a half of
     * restore out of a few milliseconds of real work, so the window in which the restore is running is one the
     * test controls rather than one it hopes for. It also forces the SEQUENTIAL restore path, since an http(s)
     * body cannot be opened for random access.
     */
    private HttpServer serveArchiveSlowly(final File file) {
      try {
        final byte[] bytes = Files.readAllBytes(file.toPath());
        final HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(LOOPBACK), 0), 0);
        server.createContext("/" + ARCHIVE_NAME, exchange -> {
          exchange.sendResponseHeaders(200, bytes.length);
          try (final OutputStream out = exchange.getResponseBody()) {
            final int slice = Math.max(1, bytes.length / ARCHIVE_SLICES);
            for (int offset = 0; offset < bytes.length; offset += slice) {
              out.write(bytes, offset, Math.min(slice, bytes.length - offset));
              out.flush();
              try {
                Thread.sleep(SLICE_PAUSE_MS);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
            }
          }
        });
        server.start();
        return server;
      } catch (final Exception e) {
        throw new RuntimeException("Cannot serve the test archive", e);
      }
    }

    private void pollTheProgressEndpoint(final ArcadeDBServer server) {
      try {
        // Deliberately read the port off the live listener rather than off the configuration: the server scans
        // upward when the configured port is taken, and getPort() is only meaningful once startService() ran.
        final int port = port(server);
        if (port <= 0)
          return;
        final HttpRequest request = HttpRequest.newBuilder()
            .uri(new URI("http://" + LOOPBACK + ":" + port + "/api/v1/progress/" + RESTORED_DB))
            .header("Authorization", "Basic " + Base64.getEncoder()
                .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
            .GET().build();
        final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        pollOutcomes.add(response.statusCode() + "@" + port);
        if (response.statusCode() != 200)
          return;
        final JSONArray operations = new JSONObject(response.body()).getJSONArray("result");
        for (int i = 0; i < operations.length(); i++)
          endpointSamples.add(operations.getJSONObject(i));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (final Exception e) {
        // The listener is not up yet, or is refusing while the server boots: keep sampling.
        pollOutcomes.add(e.getClass().getSimpleName() + "@" + port(server));
      }
    }

    private static int port(final ArcadeDBServer server) {
      return server.getHttpServer() == null ? -1 : server.getHttpServer().getPort();
    }

    /** Samples the registry for {@code databaseName} until stopped, taking a JSON snapshot of each operation. */
    private static Thread watch(final String databaseName, final List<JSONObject> into) {
      final Thread thread = new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          for (final OperationProgress op : OperationProgressRegistry.instance().getOperations(databaseName))
            into.add(op.toJSON());
          try {
            Thread.sleep(1);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }, "issue7440-watch-" + databaseName);
      thread.setDaemon(true);
      thread.start();
      return thread;
    }

    private static void stop(final Thread thread) {
      thread.interrupt();
      try {
        thread.join(5_000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private void stopSampling() {
      sampling = false;
      if (sampler != null) {
        stop(sampler);
        sampler = null;
      }
    }

    private static String typeName(final int index) {
      return "Doc7440_" + index;
    }

    private static String databaseDirectory() {
      return GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + "0";
    }
  }

  // Regression: #7454 the startup restore takes the per-database maintenance slot
  /**
   * Issue #7454: the {@code restore:} startup command of {@code arcadedb.server.defaultDatabases} takes the
   * per-database maintenance slot issue #7384 gave every backup, restore and import that goes through
   * {@code ServerControlPlane}.
   * <p>
   * The window the issue describes is real rather than theoretical: {@code ArcadeDBServer.startInternal()} calls
   * {@code httpServer.startService()} before {@code loadDefaultDatabases()}, and no HTTP command handler gates on
   * server status, so a client that authenticates while a container is extracting a large archive at boot could
   * have its own {@code restore database}, {@code trigger backup} or {@code import database} of that database
   * admitted - the slot was free because the startup command took nothing. Two writers then shared one database
   * directory.
   * <p>
   * The tests drive {@code restoreDatabaseFromStartupCommand} directly, which is what it is package-private for:
   * racing a full server boot observes nothing reliably, and the reservation is the whole subject here.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class MaintenanceSlot extends BaseGraphServerTest {
    private static final String SOURCE_DB      = "source7454";
    private static final String ARCHIVE_NAME   = "backup-7454.zip";
    private static final String RESTORED_TYPE  = "Doc7454";
    private static final int    DOCUMENT_COUNT = 40;
    private static final String LOOPBACK       = "127.0.0.1";
    private static final int    ARCHIVE_SLICES = 20;
    private static final long   SLICE_PAUSE_MS = 50;

    private final List<String> databasesToDrop = new ArrayList<>();

    private File       archive;
    private HttpServer archiveServer;

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      super.onServerConfiguration(config);
      // The archives this fixture restores from live on disk and on loopback, so the server has to be willing to
      // fetch both. The startup restore resolves this from the static global, not from the ContextConfiguration.
      GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
      config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
    }

    @AfterEach
    @Override
    public void endTest() {
      if (archiveServer != null) {
        archiveServer.stop(0);
        archiveServer = null;
      }
      try {
        for (final String databaseName : databasesToDrop)
          if (getServer(0) != null && getServer(0).existsDatabase(databaseName))
            getServer(0).getDatabaseForOperator(databaseName).getEmbedded().drop();
      } finally {
        databasesToDrop.clear();
        try {
          super.endTest();
        } finally {
          FileUtils.deleteRecursively(new File("./target/backups"));
        }
      }
    }

    /**
     * The invariant, through every operation that can hold the slot. {@code BACKUP} is the one that matters most -
     * it is the only one of the three that reads the directory this command is about to DELETE - but a restore and
     * an import refuse it equally, and the wording is the one every other entry point uses.
     */
    @ParameterizedTest
    @EnumSource(Operation.class)
    @Timeout(180)
    void aStartupRestoreIsRefusedWhileAConflictingOperationHoldsTheSlot(final Operation holder) {
      final String target = "refused7454" + holder.name().toLowerCase();
      databasesToDrop.add(target);
      // Issue #7652: 0 keeps the immediate refusal this test was written against reachable, and unchanged.
      setStartupRestoreSlotWaitMs(0);
      final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

      assertThat(coordinator.begin(target, holder))
          .as("the fixture must own the slot before the startup restore asks for it").isNull();
      try {
        assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(target, localArchiveUrl(),
            databaseDirectory() + File.separator + target))
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
            .isInstanceOf(DatabaseOperationInProgressException.class)
            .hasMessage(MaintenanceCoordinator.refusal(Operation.RESTORE, target, holder));
      } finally {
        coordinator.end(target, holder);
      }

      // A refusal must publish nothing and leave nothing behind: the slot the fixture released is the only one
      // that was ever taken.
      assertThat(OperationProgressRegistry.instance().getOperations(target))
          .as("a refused startup restore must not publish an operation").isEmpty();
      assertThat(getServer(0).existsDatabase(target))
          .as("a refused startup restore must not have restored anything").isFalse();
      assertThat(coordinator.begin(target, Operation.RESTORE))
          .as("the refused startup restore must not have leaked a reservation").isNull();
      coordinator.end(target, Operation.RESTORE);
    }

    /**
     * The half of the command the issue's suggested fix did not reach. The {@code restore:} command DROPS the
     * database it is replacing, and that drop used to run outside any reservation - so a backup of that database,
     * holding the slot, had the directory it was reading deleted underneath it. The drop now happens under the
     * same reservation as the extraction, which means a refusal has to leave the existing database intact.
     */
    @Test
    @Timeout(180)
    void aRefusedStartupRestoreDoesNotDropTheDatabaseItWouldHaveReplaced() {
      final String target = "notdropped7454";
      databasesToDrop.add(target);
      createDatabaseWithMarker(target);
      setStartupRestoreSlotWaitMs(0);

      final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
      assertThat(coordinator.begin(target, Operation.BACKUP)).isNull();
      try {
        assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(target, localArchiveUrl(),
            databaseDirectory() + File.separator + target))
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class);
      } finally {
        coordinator.end(target, Operation.BACKUP);
      }

      assertThat(getServer(0).existsDatabase(target))
          .as("the database a refused startup restore would have replaced must still exist").isTrue();
      final Database database = getServer(0).getDatabaseForOperator(target);
      assertThat(database.getSchema().existsType("Marker7454"))
          .as("the database must be the untouched original, not a restored one").isTrue();
      assertThat(database.countType("Marker7454", false)).isEqualTo(1);
    }

    /**
     * The reservation has to be HELD for the duration, not merely taken and dropped. The archive is trickled out
     * by a local HTTP server so the restore spans a window the test controls, and a watcher samples the
     * coordinator throughout it.
     */
    @Test
    @Timeout(180)
    void aStartupRestoreHoldsTheSlotWhileItRuns() {
      final String target = "held7454";
      databasesToDrop.add(target);
      final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

      final List<Boolean> samples = Collections.synchronizedList(new ArrayList<>());
      final AtomicBoolean sampling = new AtomicBoolean(true);
      final Thread watcher = new Thread(() -> {
        while (sampling.get()) {
          samples.add(coordinator.isInProgress(target, Operation.RESTORE));
          try {
            Thread.sleep(5);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }, "issue7454-watcher");
      watcher.setDaemon(true);
      watcher.start();

      try {
        getServer(0).restoreDatabaseFromStartupCommand(target, slowArchiveUrl(),
            databaseDirectory() + File.separator + target);
      } finally {
        sampling.set(false);
        stop(watcher);
      }

      assertThat(samples).as("the watcher never sampled the coordinator").isNotEmpty();
      assertThat(samples).as("the startup restore never held the RESTORE slot while it ran").contains(true);
      assertThat(getServer(0).existsDatabase(target)).isTrue();
    }

    /**
     * The maintenance slot is not the only protection a control-plane restore takes, and it cannot stand in for the
     * other one: {@code create database} is not a participant in the slot at all, so only the #7441 name claim
     * refuses a client creating this name while the archive is being extracted into its directory. The window opens
     * the moment the drop removes the database being replaced, which is why the claim is taken before the drop.
     * <p>
     * The wait is on the claim itself rather than on a sleep, so what the test asserts is what the restore
     * published about its own state.
     */
    @Test
    @Timeout(180)
    void aStartupRestoreClaimsTheDatabaseNameAgainstAConcurrentCreate() throws Exception {
      final String target = "claimed7454";
      databasesToDrop.add(target);

      final AtomicReference<Throwable> restoreFailure = new AtomicReference<>();
      final Thread restorer = new Thread(() -> {
        try {
          getServer(0).restoreDatabaseFromStartupCommand(target, slowArchiveUrl(),
              databaseDirectory() + File.separator + target);
        } catch (final Throwable t) {
          restoreFailure.set(t);
        }
      }, "issue7454-restorer");
      restorer.setDaemon(true);
      restorer.start();

      try {
        final boolean claimed = awaitNameClaim(target);
        assertThat(claimed).as("the startup restore never claimed the database name").isTrue();

        assertThatThrownBy(() -> getServer(0).createDatabase(target, ComponentFile.MODE.READ_WRITE))
            .as("a create of the name a startup restore is extracting into must be refused, not admitted")
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
            .hasMessageContaining(target);
      } finally {
        restorer.join(120_000);
      }

      assertThat(restoreFailure.get()).as("the startup restore itself must still have succeeded").isNull();
      assertThat(getServer(0).isDatabaseNameReservedForRestore(target))
          .as("the name claim must be released when the startup restore returns").isFalse();
      assertThat(getServer(0).existsDatabase(target)).isTrue();
    }

    /**
     * A reservation that outlives its operation blocks every later backup, restore and import of that database
     * until the server restarts, so both exits are asserted: the successful one here, the failing one below.
     */
    @Test
    @Timeout(180)
    void aSuccessfulStartupRestoreReleasesTheSlot() {
      final String target = "released7454";
      databasesToDrop.add(target);

      getServer(0).restoreDatabaseFromStartupCommand(target, localArchiveUrl(),
          databaseDirectory() + File.separator + target);

      assertReservable(target);
      assertThat(getServer(0).getDatabaseForOperator(target).countType(RESTORED_TYPE, false)).isEqualTo(DOCUMENT_COUNT);
    }

    @Test
    @Timeout(180)
    void aFailedStartupRestoreReleasesTheSlot() {
      final String target = "failed7454";
      databasesToDrop.add(target);

      assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(target,
          "file://" + new File("./target/does-not-exist-7454.zip").getAbsolutePath(),
          databaseDirectory() + File.separator + target)).isInstanceOf(RuntimeException.class);

      assertReservable(target);
      assertThat(getServer(0).existsDatabase(target)).isFalse();
    }

    /**
     * The drop moved inside the reservation; it still has to happen. Without it the restore would extract into a
     * directory that already holds a database and the marker type would survive.
     */
    @Test
    @Timeout(180)
    void aStartupRestoreOverAnExistingDatabaseStillReplacesIt() {
      final String target = "replaced7454";
      databasesToDrop.add(target);
      createDatabaseWithMarker(target);

      getServer(0).restoreDatabaseFromStartupCommand(target, localArchiveUrl(),
          databaseDirectory() + File.separator + target);

      final Database database = getServer(0).getDatabaseForOperator(target);
      assertThat(database.getSchema().existsType("Marker7454"))
          .as("the database the startup restore replaced was not dropped").isFalse();
      assertThat(database.countType(RESTORED_TYPE, false)).isEqualTo(DOCUMENT_COUNT);
      assertReservable(target);
    }

    /**
     * Issue #7652: a conflicting holder is waited out rather than refused. A backup is the case that matters - a
     * refusal fails the boot and the backup dies with the JVM half way into its archive - so the holder here is a
     * backup that finishes while the startup restore waits, and the restore must then run to completion.
     * <p>
     * The release is gated on the restore being observably PARKED in the wait, not on a sleep: while it waits as a
     * {@code RESTORE} waiter, an {@code EXPORT} - which the held {@code BACKUP} alone would admit - is refused by the
     * waiter guard and named as a restore. A refused startup restore registers no waiter, so this cannot pass by the
     * restore having failed fast and the release landing afterwards.
     */
    @Test
    @Timeout(180)
    void aStartupRestoreWaitsOutAConflictingBackupThatFinishesInTime() throws Exception {
      final String target = "waited7652";
      databasesToDrop.add(target);
      setStartupRestoreSlotWaitMs(120_000);
      final String url = localArchiveUrl();
      final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

      assertThat(coordinator.begin(target, Operation.BACKUP))
          .as("the fixture must own the slot before the startup restore asks for it").isNull();

      final AtomicReference<Throwable> restoreFailure = new AtomicReference<>();
      final Thread restorer = new Thread(() -> {
        try {
          getServer(0).restoreDatabaseFromStartupCommand(target, url, databaseDirectory() + File.separator + target);
        } catch (final Throwable t) {
          restoreFailure.set(t);
        }
      }, "issue7652-restorer");
      restorer.setDaemon(true);
      restorer.start();

      final boolean waited;
      try {
        waited = awaitRestoreWaiter(target, restorer);
      } finally {
        coordinator.end(target, Operation.BACKUP);
      }
      restorer.join(120_000);

      assertThat(restorer.isAlive()).as("the startup restore did not return after the backup released").isFalse();
      assertThat(restoreFailure.get()).as("the startup restore must succeed once the backup releases the slot").isNull();
      assertThat(waited).as("the startup restore never waited for the backup holding the slot").isTrue();
      assertThat(getServer(0).getDatabaseForOperator(target).countType(RESTORED_TYPE, false)).isEqualTo(DOCUMENT_COUNT);
      assertReservable(target);
    }

    /**
     * Issue #7652: the wait is bounded, and the refusal #7454 introduced is its fallback - a holder that never lets go
     * still fails the command with the same exception and the same message, and leaves the existing database alone.
     * <p>
     * The stopwatch is a hang tripwire, not a latency claim: it separates a wait that gave up from one that did not.
     */
    @Test
    @Timeout(180)
    void aStartupRestoreStillRefusesWhenTheHolderOutlastsTheWait() {
      final String target = "timedout7652";
      databasesToDrop.add(target);
      createDatabaseWithMarker(target);
      setStartupRestoreSlotWaitMs(300);
      final String url = localArchiveUrl();
      final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

      assertThat(coordinator.begin(target, Operation.BACKUP)).isNull();
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      try {
        assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(target, url,
            databaseDirectory() + File.separator + target))
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
            .hasMessage(MaintenanceCoordinator.refusal(Operation.RESTORE, target, Operation.BACKUP));
      } finally {
        coordinator.end(target, Operation.BACKUP);
      }
      stopwatch.assertGaveUpWithin(60_000, "a bounded 300ms slot wait from one that never gives up");

      assertThat(getServer(0).existsDatabase(target)).isTrue();
      assertThat(getServer(0).getDatabaseForOperator(target).getSchema().existsType("Marker7454"))
          .as("a startup restore that timed out waiting must leave the original database untouched").isTrue();
      assertThat(OperationProgressRegistry.instance().getOperations(target))
          .as("a startup restore that timed out waiting must not publish an operation").isEmpty();
      assertReservable(target);
    }

    /**
     * Issue #7652: a boot thread interrupted while it waits takes nothing - it refuses exactly as a timeout does, keeps
     * its interrupt flag, leaves the existing database alone and leaks neither the slot nor its waiter registration.
     */
    @Test
    @Timeout(180)
    void anInterruptedStartupRestoreWaitRefusesAndLeaksNothing() throws Exception {
      final String target = "interrupted7652";
      databasesToDrop.add(target);
      createDatabaseWithMarker(target);
      setStartupRestoreSlotWaitMs(120_000);
      final String url = localArchiveUrl();
      final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();

      assertThat(coordinator.begin(target, Operation.BACKUP)).isNull();

      final AtomicReference<Throwable> restoreFailure = new AtomicReference<>();
      final AtomicBoolean interruptFlagKept = new AtomicBoolean();
      final Thread restorer = new Thread(() -> {
        try {
          getServer(0).restoreDatabaseFromStartupCommand(target, url, databaseDirectory() + File.separator + target);
        } catch (final Throwable t) {
          restoreFailure.set(t);
          interruptFlagKept.set(Thread.currentThread().isInterrupted());
        }
      }, "issue7652-interrupted-restorer");
      restorer.setDaemon(true);
      restorer.start();

      final boolean waited;
      try {
        waited = awaitRestoreWaiter(target, restorer);
        restorer.interrupt();
        restorer.join(120_000);
      } finally {
        coordinator.end(target, Operation.BACKUP);
      }

      assertThat(waited).as("the startup restore never waited for the backup holding the slot").isTrue();
      assertThat(restorer.isAlive()).as("the interrupted startup restore did not return").isFalse();
      assertThat(restoreFailure.get())
          .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
          .hasMessage(MaintenanceCoordinator.refusal(Operation.RESTORE, target, Operation.BACKUP));
      assertThat(interruptFlagKept.get()).as("the interrupt must not be swallowed").isTrue();
      assertThat(getServer(0).getDatabaseForOperator(target).getSchema().existsType("Marker7454"))
          .as("an interrupted startup restore must leave the original database untouched").isTrue();
      // The waiter registration must be gone too: with it leaked, the guard would still refuse this export.
      assertThat(coordinator.begin(target, Operation.EXPORT))
          .as("the interrupted startup restore leaked its waiter registration").isNull();
      coordinator.end(target, Operation.EXPORT);
      assertReservable(target);
    }

    // ------------------------------------------------------------------------------------------------- HELPERS

    private void setStartupRestoreSlotWaitMs(final long waitMs) {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_STARTUP_RESTORE_SLOT_WAIT_MS, waitMs);
    }

    /**
     * Waits until the startup restore is registered as a waiter on {@code databaseName}, probing with an
     * {@code EXPORT} the waiter guard refuses. Returns {@code false} if the restore thread ends first or the hang
     * guard runs out - never a latency assertion about how fast the restore reaches its wait.
     */
    private boolean awaitRestoreWaiter(final String databaseName, final Thread restorer) throws InterruptedException {
      final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
      for (int i = 0; i < 4_000 && restorer.isAlive(); i++) {
        final Operation refusedBy = coordinator.begin(databaseName, Operation.EXPORT);
        if (refusedBy == Operation.RESTORE)
          return true;
        if (refusedBy == null)
          coordinator.end(databaseName, Operation.EXPORT);
        Thread.sleep(5);
      }
      return false;
    }

    private void assertReservable(final String databaseName) {
      final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
      assertThat(coordinator.begin(databaseName, Operation.RESTORE))
          .as("the startup restore leaked its reservation on '%s'", databaseName).isNull();
      coordinator.end(databaseName, Operation.RESTORE);
      assertThat(getServer(0).isDatabaseNameReservedForRestore(databaseName))
          .as("the startup restore leaked its name claim on '%s'", databaseName).isFalse();
      assertThat(OperationProgressRegistry.instance().getOperations(databaseName)).isEmpty();
    }

    /**
     * Waits for the restore thread to publish its name claim. The claim is taken on the first statement of the
     * method, so what this actually waits for is the thread to start - the twenty seconds is a hang guard that
     * lets a missing claim FAIL rather than hang, never a latency assertion about how fast a restore starts.
     */
    private boolean awaitNameClaim(final String databaseName) throws InterruptedException {
      for (int i = 0; i < 4_000; i++) {
        if (getServer(0).isDatabaseNameReservedForRestore(databaseName))
          return true;
        Thread.sleep(5);
      }
      return false;
    }

    private void createDatabaseWithMarker(final String databaseName) {
      final Database database = getServer(0).createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
      database.getSchema().createDocumentType("Marker7454");
      database.transaction(() -> database.newDocument("Marker7454").set("k", 1).save());
    }

    private String localArchiveUrl() {
      return "file://" + archive().getAbsolutePath();
    }

    private String slowArchiveUrl() {
      if (archiveServer == null)
        archiveServer = serveArchiveSlowly(archive());
      return "http://" + LOOPBACK + ":" + archiveServer.getAddress().getPort() + "/" + ARCHIVE_NAME;
    }

    /** Creates a throwaway database, backs it up, drops it, and returns the archive. Produced once per test. */
    private File archive() {
      if (archive != null)
        return archive;

      final String databaseDirectory = databaseDirectory();
      FileUtils.deleteRecursively(new File("./target/backups"));

      try (final DatabaseFactory factory = new DatabaseFactory(databaseDirectory + File.separator + SOURCE_DB)) {
        try (final Database database = factory.create()) {
          database.getSchema().createDocumentType(RESTORED_TYPE);
          database.transaction(() -> {
            for (int i = 0; i < DOCUMENT_COUNT; i++)
              database.newDocument(RESTORED_TYPE).set("i", i).set("payload", "x".repeat(512)).save();
          });
          database.command("sql", "backup database file://" + ARCHIVE_NAME).close();
          database.drop();
        }
      }

      final File produced = new File("./target/backups/" + SOURCE_DB + "/" + ARCHIVE_NAME);
      assertThat(produced).exists();
      archive = produced;
      return archive;
    }

    /**
     * Serves the archive in {@link #ARCHIVE_SLICES} slices {@link #SLICE_PAUSE_MS} apart, so the window in which
     * the restore is running is one the test controls rather than one it hopes for.
     */
    private HttpServer serveArchiveSlowly(final File file) {
      try {
        final byte[] bytes = Files.readAllBytes(file.toPath());
        final HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(LOOPBACK), 0), 0);
        server.createContext("/" + ARCHIVE_NAME, exchange -> {
          exchange.sendResponseHeaders(200, bytes.length);
          try (final OutputStream out = exchange.getResponseBody()) {
            final int slice = Math.max(1, bytes.length / ARCHIVE_SLICES);
            for (int offset = 0; offset < bytes.length; offset += slice) {
              out.write(bytes, offset, Math.min(slice, bytes.length - offset));
              out.flush();
              try {
                Thread.sleep(SLICE_PAUSE_MS);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
            }
          }
        });
        server.start();
        return server;
      } catch (final Exception e) {
        throw new RuntimeException("Cannot serve the test archive", e);
      }
    }

    private static void stop(final Thread thread) {
      thread.interrupt();
      try {
        thread.join(5_000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private static String databaseDirectory() {
      return GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + "0";
    }
  }

  // Regression: #7468 the startup restore applies the local-URL policy
  /**
   * Issue #7468: the {@code restore:} startup command of {@code arcadedb.server.defaultDatabases} never called
   * {@code Restore.setAllowLocalUrls(...)}, so {@code RestoreSettings.allowLocalUrls} stayed {@code null} and
   * {@code FullRestoreFormat.openInputFile()} fell back to the <b>static</b>
   * {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS} value. Every other server-side
   * restore/import path resolves that flag through {@code ServerControlPlane.isRestoreImportLocalUrlsAllowed()},
   * which reads the server's own {@link ContextConfiguration}, so an operator who overrode the setting for one
   * server instance got one answer from that server's {@code restore database} verb and a different one from the
   * same server's boot-time restore.
   * <p>
   * Each test here sets the flag on the server's {@code ContextConfiguration} and the static global to
   * <b>opposite</b> values, so the assertion can only be satisfied by the per-server answer - a fix that kept
   * reading the global would fail every one of them in the direction the override went. The third test pins the
   * other half of the contract: with no per-server override the global is still what decides, because
   * {@link ContextConfiguration#getValueAsBoolean} falls through to it. That is what keeps this change invisible
   * to every deployment that never overrode the setting.
   * <p>
   * The archive is served over {@code http://127.0.0.1}, not handed over as a {@code file://} path, on purpose:
   * {@code FullRestoreFormat.openInputFile()} gates only its {@code http(s)} branch on {@code allowLocalUrls}, so
   * a {@code file://} URL would exercise no policy at all and pass against the unfixed code.
   */
  @Nested
  class LocalUrlPolicy extends BaseGraphServerTest {
    private static final String SOURCE_DB      = "source7468";
    private static final String ARCHIVE_NAME   = "backup-7468.zip";
    private static final int    DOCUMENT_COUNT = 40;
    private static final String TYPE_NAME      = "Doc7468";
    /**
     * The IPv4 loopback LITERAL for both ends. {@code localhost} resolves to {@code ::1} or {@code 127.0.0.1}
     * depending on the machine and the moment, and a restore dialling a name that lands on the other stack reads
     * as a connection failure rather than as the policy decision this test is about.
     */
    private static final String LOOPBACK       = "127.0.0.1";

    private HttpServer archiveServer;
    private File       archive;
    private boolean    globalDefault;

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      super.onServerConfiguration(config);

      globalDefault = GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.getValueAsBoolean();
      archive = produceArchive();
      archiveServer = serveArchive(archive);

      // A dedicated port rather than the shared 2480-2489 range: this class needs its own server to be the one
      // answering, and a neighbouring listener would turn a policy assertion into an unrelated wall of red.
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, LOOPBACK);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(allocateFreePorts(1)[0]));
    }

    @AfterEach
    @Override
    public void endTest() {
      if (archiveServer != null)
        archiveServer.stop(0);
      try {
        super.endTest();
      } finally {
        GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(globalDefault);
        FileUtils.deleteRecursively(new File("./target/backups"));
      }
    }

    /**
     * Override <b>on</b>, global <b>off</b>: the operator deliberately allowed local URLs for this server, so its
     * own boot-time restore must fetch one. Against the unfixed code the restore read the global, saw {@code false}
     * and refused a URL the operator had allowed.
     */
    @Test
    @Timeout(180)
    void aStartupRestoreFollowsAPerServerOverrideThatAllowsLocalUrls() {
      GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(false);
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);

      final String target = "allowed7468";
      getServer(0).restoreDatabaseFromStartupCommand(target, archiveUrl(), targetPath(target));

      assertThat(getServer(0).existsDatabase(target))
          .as("the per-server override allows local URLs, so the startup restore must have run").isTrue();
      assertThat(getServer(0).getDatabaseForOperator(target).countType(TYPE_NAME, true)).isEqualTo(DOCUMENT_COUNT);
    }

    /**
     * Override <b>off</b>, global <b>on</b>: the operator disabled local URLs for this server, so its own boot-time
     * restore must refuse one even though the process-wide default permits it. This is the direction that matters
     * for the SSRF control - against the unfixed code the restore read the global, saw {@code true} and fetched
     * from loopback anyway.
     */
    @Test
    @Timeout(180)
    void aStartupRestoreFollowsAPerServerOverrideThatForbidsLocalUrls() {
      GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, false);

      final String target = "forbidden7468";
      assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(target, archiveUrl(), targetPath(target)))
          .isInstanceOf(CommandExecutionException.class)
          .rootCause()
          .isInstanceOf(SecurityException.class)
          .hasMessageContaining("blocked request to a non-public or restricted address");

      assertThat(getServer(0).existsDatabase(target))
          .as("a refused startup restore must leave no database behind").isFalse();
    }

    /**
     * No per-server override at all: the static global is still what decides, so nothing changes for a deployment
     * that never overrode the setting. Without this the fix could have hard-coded an answer and the two tests above
     * would not have noticed.
     */
    @Test
    @Timeout(180)
    void aStartupRestoreWithNoOverrideStillFollowsTheGlobal() {
      assertThat(getServer(0).getConfiguration()
          .hasValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.getKey()))
          .as("this test is only meaningful while the server carries no overlay for the setting").isFalse();

      GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(false);
      final String refused = "globaloff7468";
      assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(refused, archiveUrl(), targetPath(refused)))
          .isInstanceOf(CommandExecutionException.class)
          .rootCause()
          .isInstanceOf(SecurityException.class);
      assertThat(getServer(0).existsDatabase(refused)).isFalse();

      GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
      final String allowed = "globalon7468";
      getServer(0).restoreDatabaseFromStartupCommand(allowed, archiveUrl(), targetPath(allowed));
      assertThat(getServer(0).existsDatabase(allowed)).isTrue();
      assertThat(getServer(0).getDatabaseForOperator(allowed).countType(TYPE_NAME, true)).isEqualTo(DOCUMENT_COUNT);
    }

    // ------------------------------------------------------------------------------------------------- HELPERS

    private String archiveUrl() {
      return "http://" + LOOPBACK + ":" + archiveServer.getAddress().getPort() + "/" + ARCHIVE_NAME;
    }

    private static String targetPath(final String databaseName) {
      return GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + "0" + File.separator + databaseName;
    }

    /** Creates a throwaway database, backs it up, drops it, and returns the archive. */
    private File produceArchive() {
      final String databaseDirectory = GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + "0";
      FileUtils.deleteRecursively(new File("./target/backups"));

      try (final DatabaseFactory factory = new DatabaseFactory(databaseDirectory + File.separator + SOURCE_DB)) {
        try (final Database database = factory.create()) {
          database.getSchema().createDocumentType(TYPE_NAME);
          database.transaction(() -> {
            for (int i = 0; i < DOCUMENT_COUNT; i++)
              database.newDocument(TYPE_NAME).set("i", i).set("payload", "x".repeat(256)).save();
          });
          database.command("sql", "backup database file://" + ARCHIVE_NAME).close();
          database.drop();
        }
      }

      final File produced = new File("./target/backups/" + SOURCE_DB + "/" + ARCHIVE_NAME);
      assertThat(produced).exists();
      return produced;
    }

    /** Serves the archive whole, from the IPv4 loopback literal, on an OS-assigned port. */
    private HttpServer serveArchive(final File file) {
      try {
        final byte[] bytes = Files.readAllBytes(file.toPath());
        final HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(LOOPBACK), 0), 0);
        server.createContext("/" + ARCHIVE_NAME, exchange -> {
          exchange.sendResponseHeaders(200, bytes.length);
          try (final OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
          }
        });
        server.start();
        return server;
      } catch (final IOException e) {
        throw new RuntimeException("Cannot serve the test archive", e);
      }
    }
  }
}
