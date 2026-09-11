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
package com.arcadedb.server.backup;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.server.ServerControlPlane.ProgressListener;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7385: {@code restore backup} and {@code restore database} can run for minutes, and for that whole time
 * {@code GET /api/v1/progress/{database}} used to say the database had nothing running - so Studio showed an
 * idle database that was in fact being replaced. Every restore that goes through
 * {@code ServerControlPlane.performRestore} now publishes an {@code OperationProgress} for its target database,
 * retired in a {@code finally} on success and on failure alike.
 * <p>
 * The control-plane tests below are the deterministic ones: {@code performRestore} calls the caller's
 * {@code ProgressListener} while the operation is live, so a listener that reads the registry at that moment
 * observes the registration without racing it. They also stand in for the gRPC {@code RestoreDatabase} and
 * {@code RestoreBackup} RPCs, which call exactly these two methods
 * ({@code ArcadeDbGrpcAdminService.java:797} and {@code :821}), as HTTP does
 * ({@code PostServerCommandHandler.java:423} and {@code :457}).
 * <p>
 * {@link #theProgressEndpointReportsARunningRestore} then closes the loop end to end over HTTP - the endpoint
 * the issue is named after - against an archive served by a deliberately slow local HTTP server, so the window
 * in which the restore is running is one the test controls rather than one it hopes for.
 * <p>
 * The startup {@code SERVER_DEFAULT_DATABASES} {@code restore:} command does not go through the control plane
 * and is still silent: that is #7440.
 */
class Issue7385RestoreProgressIT extends BaseGraphServerTest {
  private static final String BACKUP_DIR_NAME = "test-backups-7385-progress";
  private static final String BACKUP_CONFIG   = """
      {
        "version": 1,
        "enabled": true,
        "backupDirectory": "%s",
        "defaults": {
          "enabled": true,
          "runOnServer": "*",
          "schedule": { "type": "frequency", "frequencyMinutes": 9999 },
          "retention": { "maxFiles": 50 }
        }
      }
      """.formatted(BACKUP_DIR_NAME);

  private File         backupConfigFile;
  private File         backupDir;
  private final List<String> restoredDatabases = new ArrayList<>();

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    try {
      final File configDir = new File("./target/config");
      configDir.mkdirs();

      backupConfigFile = new File(configDir, "backup.json");
      try (final FileWriter writer = new FileWriter(backupConfigFile)) {
        writer.write(BACKUP_CONFIG);
      }

      backupDir = new File("./target/" + BACKUP_DIR_NAME);
      if (backupDir.exists())
        FileUtils.deleteRecursively(backupDir);
      backupDir.mkdirs();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to set up backup config", e);
    }

    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "auto-backup:" + AutoBackupSchedulerPlugin.class.getName());
    // The archives this test restores are local files and a loopback HTTP server, both of which the SSRF guard
    // blocks by default. Opt in the way an operator restoring from their own filesystem would (issue #6381).
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @AfterEach
  void cleanUp() {
    for (final String name : restoredDatabases) {
      try {
        final ArcadeDBServer server = getServer(0);
        if (server.existsDatabase(name))
          server.getDatabase(name).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort
      }
    }
    restoredDatabases.clear();
    if (backupConfigFile != null && backupConfigFile.exists())
      backupConfigFile.delete();
    if (backupDir != null && backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
  }

  /**
   * {@code restore database <name> <url>} - the HTTP verb and the gRPC {@code RestoreDatabase} RPC both land
   * here.
   */
  @Test
  void restoreDatabasePublishesProgressAtTheControlPlane() throws Exception {
    final String target = register("progress7385_db");
    final File archive = takeABackup();

    final List<JSONObject> observed = Collections.synchronizedList(new ArrayList<>());
    new ServerControlPlane(getServer(0)).restoreDatabase(target, "file://" + archive.getAbsolutePath(),
        sampler(target, observed));

    assertThat(observed).as("nothing was published while the restore ran").isNotEmpty();
    assertThat(observed).allSatisfy(op -> {
      assertThat(op.getString("database")).isEqualTo(target);
      assertThat(op.getString("operation")).isEqualTo("restore database");
      assertThat(op.getInt("totalSteps")).isEqualTo(3);
    });

    // A LOCAL ARCHIVE TAKES THE PARALLEL PATH, WHICH KNOWS ITS ENTRY COUNT, SO A REAL DENOMINATOR HAS TO REACH
    // THE REGISTRY. THIS IS THE ONLY ASSERTION THAT CAN CATCH installRestoreProgressCallback FAILING TO FIND THE
    // SETTER: IT SWALLOWS THE ReflectiveOperationException BY DESIGN, SO THE COARSE STEP MARKERS ABOVE WOULD
    // STILL BE PUBLISHED AND EVERYTHING ELSE HERE WOULD STILL PASS
    assertThat(observed).anySatisfy(op -> {
      assertThat(op.getLong("total")).as("the archive entry count never reached the registry").isGreaterThan(0L);
      assertThat(op.getLong("done")).isGreaterThan(0L);
      assertThat(op.getInt("percentage")).isBetween(0, 100);
    });

    assertThat(getServer(0).existsDatabase(target)).isTrue();
    assertThat(OperationProgressRegistry.instance().getOperations(target))
        .as("the operation must be retired when the restore returns").isEmpty();
  }

  /**
   * {@code restore backup <db> <file> as <target>} - the HTTP verb and the gRPC {@code RestoreBackup} RPC both
   * land here. It carries its own operation label, because that is the command the operator typed.
   */
  @Test
  void restoreBackupPublishesProgressAtTheControlPlane() throws Exception {
    final String target = register("progress7385_backup");
    final String fileName = takeABackup().getName();

    final List<JSONObject> observed = Collections.synchronizedList(new ArrayList<>());
    new ServerControlPlane(getServer(0)).restoreBackup(getDatabaseName(), fileName, target, false,
        sampler(target, observed));

    assertThat(observed).isNotEmpty();
    assertThat(observed).allSatisfy(op -> {
      assertThat(op.getString("database")).isEqualTo(target);
      assertThat(op.getString("operation")).isEqualTo("restore backup");
    });

    assertThat(getServer(0).existsDatabase(target)).isTrue();
    assertThat(OperationProgressRegistry.instance().getOperations(target)).isEmpty();
  }

  /**
   * The half that matters more than the publication itself: a restore that fails must not leave a phantom
   * operation behind for the rest of the life of the process.
   */
  @Test
  void aFailedRestoreRetiresTheOperation() throws Exception {
    final String target = register("progress7385_failed");
    final File archive = takeABackup();
    Files.write(archive.toPath(), "not a valid zip archive".getBytes(StandardCharsets.UTF_8));

    final List<JSONObject> observed = Collections.synchronizedList(new ArrayList<>());
    final ServerControlPlane controlPlane = new ServerControlPlane(getServer(0));

    assertThatThrownBy(() -> controlPlane.restoreDatabase(target, "file://" + archive.getAbsolutePath(),
        sampler(target, observed))).isInstanceOf(RuntimeException.class);

    assertThat(observed).as("the operation must be published before the failure, not after it").isNotEmpty();
    assertThat(OperationProgressRegistry.instance().getOperations(target))
        .as("a failed restore must retire its operation too").isEmpty();
  }

  /**
   * End to end over HTTP, on the endpoint the issue is named after. The archive is served by a local HTTP server
   * that trickles it out, which does two things: it forces the sequential restore path (an http(s) body cannot be
   * opened for random access) and it makes the window in which the restore is running long enough to poll without
   * racing it.
   */
  @Test
  @Timeout(120)
  void theProgressEndpointReportsARunningRestore() throws Exception {
    final String target = register("progress7385_http");
    final byte[] archive = Files.readAllBytes(takeABackup().toPath());

    final HttpServer archiveServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    archiveServer.createContext("/backup.zip", exchange -> {
      exchange.sendResponseHeaders(200, archive.length);
      try (final OutputStream out = exchange.getResponseBody()) {
        // 20 slices, 50 ms apart: about a second of restore, from a handful of milliseconds of real work.
        final int slice = Math.max(1, archive.length / 20);
        for (int offset = 0; offset < archive.length; offset += slice) {
          out.write(archive, offset, Math.min(slice, archive.length - offset));
          out.flush();
          try {
            Thread.sleep(50);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
    });
    archiveServer.start();

    try {
      final String url = "http://localhost:" + archiveServer.getAddress().getPort() + "/backup.zip";
      final AtomicReference<HttpResponse<String>> restoreResponse = new AtomicReference<>();
      final AtomicReference<Exception> restoreFailure = new AtomicReference<>();

      final Thread restore = new Thread(() -> {
        try {
          restoreResponse.set(postCommand("restore database " + target + " " + url));
        } catch (final Exception e) {
          restoreFailure.set(e);
        }
      }, "issue7385-restore");
      restore.setDaemon(true);
      restore.start();

      JSONObject seen = null;
      while (restore.isAlive() && seen == null) {
        final HttpResponse<String> poll = getProgress(target);
        assertThat(poll.statusCode()).as("progress poll: %s", poll.body()).isEqualTo(200);
        final JSONArray operations = new JSONObject(poll.body()).getJSONArray("result");
        if (!operations.isEmpty())
          seen = operations.getJSONObject(0);
        else
          Thread.sleep(5);
      }
      restore.join();

      assertThat(restoreFailure.get()).isNull();
      assertThat(restoreResponse.get().statusCode()).isEqualTo(200);

      assertThat(seen).as("the progress endpoint reported nothing for the whole restore").isNotNull();
      assertThat(seen.getString("operation")).isEqualTo("restore database");
      assertThat(seen.getString("database")).isEqualTo(target);
      assertThat(seen.getInt("totalSteps")).isEqualTo(3);

      assertThat(new JSONObject(getProgress(target).body()).getJSONArray("result").length())
          .as("the operation must be retired when the restore returns").isZero();
      assertThat(getServer(0).existsDatabase(target)).isTrue();
    } finally {
      archiveServer.stop(0);
    }
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  /**
   * A {@code ProgressListener} that takes a JSON SNAPSHOT of the database's running operations every time the
   * restore reports a line - which {@code performRestore} does while the operation is live, so the registration is
   * observed rather than raced.
   * <p>
   * The snapshot is the point. {@code getOperations} hands back the live {@link OperationProgress} objects the
   * producer is still writing to, so keeping the references would mean asserting, at the end, on whatever the last
   * write left behind: every sample would read alike and the counters would all show the final step. {@code
   * toJSON} is also exactly what the progress endpoint serializes, so what is asserted here is what a caller sees.
   */
  private static ProgressListener sampler(final String databaseName, final List<JSONObject> into) {
    return message -> {
      for (final OperationProgress op : OperationProgressRegistry.instance().getOperations(databaseName))
        into.add(op.toJSON());
    };
  }

  private String register(final String databaseName) {
    restoredDatabases.add(databaseName);
    return databaseName;
  }

  /** Produces a real backup of the seeded database and returns the archive on disk. */
  private File takeABackup() throws Exception {
    assertThat(postCommand("trigger backup " + getDatabaseName()).statusCode()).isEqualTo(200);

    final JSONArray backups = new JSONObject(postCommand("list backups " + getDatabaseName()).body())
        .getJSONArray("backups");
    assertThat(backups.length()).isEqualTo(1);

    final File archive = new File(new File(backupDir, getDatabaseName()),
        backups.getJSONObject(0).getString("fileName"));
    assertThat(archive).exists();
    return archive;
  }

  private HttpResponse<String> getProgress(final String databaseName) throws Exception {
    return send(HttpRequest.newBuilder()
        .uri(new URI("http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1/progress/" + databaseName))
        .GET());
  }

  private HttpResponse<String> postCommand(final String command) throws Exception {
    return send(HttpRequest.newBuilder()
        .uri(new URI("http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1/server"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString())));
  }

  private static HttpResponse<String> send(final HttpRequest.Builder builder) throws Exception {
    final HttpRequest request = builder.header("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes())).build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
