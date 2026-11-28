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
import com.arcadedb.database.Database;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * A SQL {@code BACKUP DATABASE} run over HTTP with a low read rate cap ends when the statement is terminated by its tag, and
 * the database is free for the next maintenance operation right after: the slot it held is released and no partial archive
 * is left in the backup directory.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class SqlBackupStopRequestTest extends BaseGraphServerTest {
  private static final String TAG_HEADER = "X-ArcadeDB-Query-Tag";
  private static final String TAG        = "capped-backup";

  private File backupDir;

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");

    backupDir = new File("./target/backups-stop-request");
    if (backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
    backupDir.mkdirs();
    config.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, backupDir.getAbsolutePath());
    GlobalConfiguration.SERVER_BACKUP_DIRECTORY.setValue(backupDir.getAbsolutePath());
  }

  @AfterEach
  void cleanup() {
    GlobalConfiguration.SERVER_BACKUP_DIRECTORY.reset();
    if (backupDir != null && backupDir.exists())
      FileUtils.deleteRecursively(backupDir);
  }

  // A HANG DETECTOR, NOT A LATENCY BOUND: LEFT ALONE THE CAPPED BACKUP BELOW RUNS FOR HALF A MINUTE
  @Test
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void aCappedBackupEndsWhenItsStatementIsTerminatedAndTheDatabaseIsFreeAgain() throws Exception {
    // ABOUT 24 MB OF PAGES: AT 1 MB/S THE BACKUP IS FAR FROM DONE WHEN IT IS TERMINATED
    final Database database = getServerDatabase(0, getDatabaseName());
    final String block = "x".repeat(80_000);
    database.getSchema().createDocumentType("StopRequestPayload");
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("StopRequestPayload").set("payload", block + i).save();
    });

    final CompletableFuture<HttpResponse<String>> running = CompletableFuture.supplyAsync(() -> {
      try {
        return send("/api/v1/command/" + getDatabaseName(), new JSONObject().put("language", "sql")
            .put("command", "BACKUP DATABASE file://capped.zip WITH maxMBPerSecond = 1, compressionThreads = 2"), TAG);
      } catch (final Exception e) {
        throw new IllegalStateException(e);
      }
    });

    final RunningQuery entry = awaitRunning();
    awaitPartialArchive();

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final HttpResponse<String> terminate = send("/api/v1/server",
        new JSONObject().put("command", "terminate query " + entry.getId()), null);
    assertThat(terminate.statusCode()).isEqualTo(200);
    assertThat(new JSONObject(terminate.body()).getJSONObject("result").getString("status")).isEqualTo("terminated");

    final HttpResponse<String> response = running.get(60, TimeUnit.SECONDS);
    watch.assertGaveUpWithin(12_000, "a terminated backup stopping at its next chunk, against one that runs to the end of its cap");
    assertThat(response.statusCode()).isEqualTo(409);
    assertThat(response.body()).contains("QueryTerminatedException");

    assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).as("the slot is released").isFalse();
    assertThat(partialArchives()).as("no partial archive is left behind").isEmpty();

    // THE SLOT IS GENUINELY FREE, NOT MERELY REPORTED FREE: THE NEXT MAINTENANCE OPERATION IS ADMITTED
    assertThat(send("/api/v1/server", new JSONObject().put("command", "trigger backup " + getDatabaseName()), null).statusCode())
        .isEqualTo(200);
  }

  private RunningQuery awaitRunning() throws InterruptedException {
    final long giveUp = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (System.nanoTime() < giveUp) {
      for (final RunningQuery query : getServer(0).getRunningQueries().getRunning())
        if (TAG.equals(query.getTag()))
          return query;
      Thread.sleep(10);
    }
    return fail("the backup statement never showed in the running statements");
  }

  /** The archive is claimed by its partial name before the first byte is read, and the cap keeps it open for long. */
  private void awaitPartialArchive() throws Exception {
    final long giveUp = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (System.nanoTime() < giveUp) {
      if (!partialArchives().isEmpty()) {
        // LET THE BACKUP SETTLE INTO ITS CAPPED READ LOOP
        Thread.sleep(300);
        return;
      }
      Thread.sleep(10);
    }
    fail("the backup never claimed its archive");
  }

  private List<Path> partialArchives() throws IOException {
    if (!backupDir.exists())
      return List.of();
    try (final Stream<Path> files = Files.walk(backupDir.toPath())) {
      return files.filter(path -> path.getFileName().toString().endsWith(".partial")).toList();
    }
  }

  private HttpResponse<String> send(final String path, final JSONObject payload, final String tag) throws Exception {
    final HttpRequest.Builder request = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl(0, path)))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()));
    if (tag != null)
      request.header(TAG_HEADER, tag);
    return HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofString());
  }
}
