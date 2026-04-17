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
package com.arcadedb.server.ha.raft;

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link SnapshotHttpHandler} gates concurrent downloads with its semaphore and returns
 * 503 + a retry-friendly message when the configured maximum is exhausted. Also verifies that
 * the semaphore is fully released once the held permits are returned, so a follow-up request
 * succeeds (covers the "permit leak on timeout" failure mode the watchdog guards against).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotHttpHandlerConcurrencyIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void returns503WhenSemaphoreExhausted() throws Exception {
    final int leaderIndex = getLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final ArcadeDBServer server = getServer(leaderIndex);
    final Semaphore semaphore = extractSemaphoreFromHandler(server);
    final int maxConcurrent = semaphore.availablePermits();
    assertThat(maxConcurrent).isGreaterThan(0);

    // Drain every permit so the next tryAcquire returns false.
    assertThat(semaphore.tryAcquire(maxConcurrent)).isTrue();
    try {
      final HttpURLConnection conn = openSnapshotRequest(leaderIndex);
      try {
        assertThat(conn.getResponseCode()).isEqualTo(503);
        final String body = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(body).contains("Too many concurrent snapshot downloads");
      } finally {
        conn.disconnect();
      }
    } finally {
      semaphore.release(maxConcurrent);
    }
  }

  @Test
  void semaphoreReleaseRestoresCapacity() throws Exception {
    final int leaderIndex = getLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final ArcadeDBServer server = getServer(leaderIndex);
    final Semaphore semaphore = extractSemaphoreFromHandler(server);
    final int maxConcurrent = semaphore.availablePermits();

    // Drain + release cycle mirrors what happens when the handler's try/finally fires
    // (either on normal completion or after the watchdog closes the connection).
    assertThat(semaphore.tryAcquire(maxConcurrent)).isTrue();
    semaphore.release(maxConcurrent);

    // A subsequent request must succeed because every permit was returned.
    final HttpURLConnection conn = openSnapshotRequest(leaderIndex);
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
    } finally {
      conn.disconnect();
    }
  }

  private HttpURLConnection openSnapshotRequest(final int serverIndex) throws Exception {
    final int httpPort = 2480 + serverIndex;
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/ha/snapshot/" + getDatabaseName()).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    return conn;
  }

  /**
   * The {@link SnapshotHttpHandler} field and its {@code snapshotSemaphore} are private;
   * reflection is isolated to this test so production code does not expose an internal seam.
   */
  private static Semaphore extractSemaphoreFromHandler(final ArcadeDBServer server) throws Exception {
    final RaftHAPlugin plugin = (RaftHAPlugin) server.getHA();
    assertThat(plugin).as("Raft HA plugin must be installed on the server").isNotNull();

    final Field handlerField = RaftHAPlugin.class.getDeclaredField("snapshotHandler");
    handlerField.setAccessible(true);
    final SnapshotHttpHandler handler = (SnapshotHttpHandler) handlerField.get(plugin);
    assertThat(handler).as("SnapshotHttpHandler must be wired on the server").isNotNull();

    final Field semaphoreField = SnapshotHttpHandler.class.getDeclaredField("snapshotSemaphore");
    semaphoreField.setAccessible(true);
    return (Semaphore) semaphoreField.get(handler);
  }
}
