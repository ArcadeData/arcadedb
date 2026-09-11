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
import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue7440StartupRestoreProgressIT extends BaseGraphServerTest {
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

    // The archive is served from loopback, which the SSRF guard blocks by default. The startup restore reads
    // this from the static global (it has no ContextConfiguration of its own to resolve against), so set it
    // there - beginTest() has already run resetAll(), so this is not leaking into another class.
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);

    // A dedicated port, not the shared 2480-2489 range this suite defaults to. This test polls an HTTP endpoint
    // several hundred times while the server boots and asserts on what comes back, so a neighbouring server -
    // another test class mid-shutdown, an IDE, a locally installed ArcadeDB - answering even once would be read
    // as the server under test saying something it never said.
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, LOOPBACK);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(freePort()));

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
    final Database database = getServer(0).getDatabase(RESTORED_DB);
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

  /** An ephemeral port the OS has just handed out, released again immediately so the server can take it. */
  private static int freePort() {
    try (final ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName(LOOPBACK))) {
      return socket.getLocalPort();
    } catch (final IOException e) {
      throw new RuntimeException("Cannot reserve a free port for the test server", e);
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
