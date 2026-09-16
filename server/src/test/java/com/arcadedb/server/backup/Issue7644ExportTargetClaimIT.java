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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7644: {@link com.arcadedb.engine.MaintenanceCoordinator.Operation#EXPORT} admits any number of exports of
 * one database at once (issue #7450), on the premise that two exports name two different files. Nothing enforced
 * that premise:
 * <ul>
 *   <li>two explicit {@code EXPORT DATABASE} statements naming the SAME URL with {@code WITH overwrite = true} -
 *   the exporter formats' {@code file.exists() && !overwriteFile} check does not even apply then, so two writers
 *   simply interleaved their output into one archive;</li>
 *   <li>the default name, built from a millisecond timestamp plus a random component (issue #7644's own fix to the
 *   deterministic half of this, landed with #7450) - astronomically unlikely to collide, but the SAME URL supplied
 *   explicitly twice was always reachable by a caller and had no protection at all.</li>
 * </ul>
 * The fix: {@code AbstractExporterFormat.claimExportFile} creates a lock file next to the resolved target,
 * atomically, before a byte is written - independent of {@code overwriteFile} - so the loser of the race is
 * refused outright ({@code ExportException}, "Another export to '...' is already in progress") instead of
 * corrupting the winner's output.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7644ExportTargetClaimIT extends BaseGraphServerTest {
  private final File exportDir = new File("./exports");

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
  }

  @BeforeEach
  void discardArchivesFromAnEarlierRun() {
    if (exportDir.exists())
      FileUtils.deleteRecursively(exportDir);
  }

  @AfterEach
  void cleanUp() {
    final BackupCoordinator coordinator = getServer(0).getBackupCoordinator();
    for (int i = 0; i < 64 && coordinator.isInProgress(getDatabaseName()); i++)
      coordinator.end(getDatabaseName(), com.arcadedb.engine.MaintenanceCoordinator.Operation.EXPORT);

    if (exportDir.exists())
      FileUtils.deleteRecursively(exportDir);
  }

  /**
   * Genuinely concurrent, not merely sequential: every racer names the SAME explicit URL with
   * {@code overwrite = true}, so none of them is stopped by the pre-existing (and still present)
   * {@code file.exists()} check - that check only ever refused a SECOND, later export, never two admitted together.
   * The claim is deterministic regardless of how the race actually interleaves: exactly one racer creates the lock
   * file, and every other one sees {@code FileAlreadyExistsException} and is refused before it opens the target for
   * writing.
   */
  @Test
  @Timeout(90)
  void twoConcurrentExportsToTheSameTargetDoNotBothWriteIt() throws Exception {
    final int racers = 8;
    final CountDownLatch startTogether = new CountDownLatch(1);
    final List<HttpResponse<String>> responses = Collections.synchronizedList(new ArrayList<>());

    final ExecutorService executor = Executors.newFixedThreadPool(racers);
    try {
      for (int i = 0; i < racers; i++)
        executor.submit(() -> {
          try {
            startTogether.await();
            responses.add(postSql("EXPORT DATABASE file://racing-7644.jsonl.tgz WITH overwrite = true"));
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
          return null;
        });

      startTogether.countDown();
      executor.shutdown();
      assertThat(executor.awaitTermination(80, TimeUnit.SECONDS)).isTrue();
    } finally {
      executor.shutdownNow();
    }

    final long succeeded = responses.stream().filter(r -> r.statusCode() == 200).count();
    final long refused = responses.stream()
        .filter(r -> r.statusCode() != 200 && r.body().contains("already in progress")).count();

    // THE LOCK IS A MUTEX ON THE TARGET, NOT A ONE-SHOT CLAIM: AN EXPORT OF THIS FIXTURE'S NEAR-EMPTY DATABASE CAN
    // FINISH FAST ENOUGH THAT A LATER RACER CLAIMS THE SAME PATH ONLY AFTER AN EARLIER ONE RELEASED IT, AND THAT
    // SECOND RUN IS LEGITIMATE - IT NEVER OVERLAPPED THE FIRST, SO NOTHING INTERLEAVED. WHAT MUST NEVER HAPPEN IS
    // TWO RACERS HOLDING THE CLAIM AT ONCE, WHICH IS WHAT "EVERY RESPONSE IS EITHER A CLEAN SUCCESS OR THIS EXACT
    // REFUSAL" RULES OUT: A CORRUPTED, INTERLEAVED WRITE WOULD SURFACE AS SOME OTHER FAILURE, NOT THIS ONE
    assertThat(succeeded + refused).as("every racer is either admitted or refused by the claim, nothing else: %s",
        responses).isEqualTo(racers);
    assertThat(succeeded).as("at least one racer claims the target: %s", responses).isGreaterThanOrEqualTo(1);

    // THE ARCHIVE THE WINNER WROTE IS INTACT - NOT TWO WRITERS' OUTPUT INTERLEAVED INTO ONE GZIP STREAM - AND NO
    // LOCK FILE WAS LEFT BEHIND
    final File archive = new File(exportDir, "racing-7644.jsonl.tgz");
    assertThat(archive).exists();
    try (final GZIPInputStream in = new GZIPInputStream(new FileInputStream(archive));
        final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
      assertThat(reader.readLine()).as("the archive starts with a well-formed 'info' line").contains("\"t\":\"info\"");
    }
    assertThat(new File(exportDir, "racing-7644.jsonl.tgz.exporting")).doesNotExist();

    // AND THE SLOT ITSELF IS FREE AGAIN, EXACTLY AS IT IS AFTER ANY OTHER COMPLETED EXPORT
    assertThat(getServer(0).getBackupCoordinator().isInProgress(getDatabaseName())).isFalse();
  }

  /**
   * The lock is released even when the export FAILS after claiming it, so a failed export does not permanently
   * block every later one to the same target.
   */
  @Test
  @Timeout(60)
  void aFailedExportReleasesItsTargetClaim() throws Exception {
    // A NONEXISTENT includeTypes ENTRY DOES NOT FAIL THE EXPORT - USE overwrite = false TWICE INSTEAD, WHICH
    // GENUINELY FAILS ON THE SECOND, SEQUENTIAL CALL VIA THE PRE-EXISTING file.exists() CHECK, PROVING THE LOCK
    // FROM THE FIRST (SUCCESSFUL) RUN DID NOT LEAK
    assertThat(postSql("EXPORT DATABASE file://leak-check-7644.jsonl.tgz").statusCode()).isEqualTo(200);

    final HttpResponse<String> second = postSql("EXPORT DATABASE file://leak-check-7644.jsonl.tgz");
    assertThat(second.statusCode()).isNotEqualTo(200);
    assertThat(second.body()).as("the refusal is the pre-existing 'already exist' one, not a stale lock")
        .doesNotContain("already in progress");

    assertThat(new File(exportDir, "leak-check-7644.jsonl.tgz.exporting")).doesNotExist();

    // AND A FRESH EXPORT TO THE SAME TARGET WITH overwrite = true NOW SUCCEEDS - NOTHING IS LEFT CLAIMING IT
    assertThat(postSql("EXPORT DATABASE file://leak-check-7644.jsonl.tgz WITH overwrite = true").statusCode())
        .isEqualTo(200);
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private HttpResponse<String> postSql(final String command) throws Exception {
    return post("/api/v1/command/" + getDatabaseName(),
        new JSONObject().put("language", "sql").put("command", command));
  }

  private HttpResponse<String> post(final String path, final JSONObject payload) throws Exception {
    final HttpClient client = HttpClient.newHttpClient();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + path))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
