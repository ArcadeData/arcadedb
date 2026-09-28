/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8468: a node served a snapshot of a database it had quarantined.
 * <p>
 * A quarantine skips a committed entry of the database and lets every later entry advance the node's applied index
 * past it, so the applied index the #8454 header reports overstated what the copy held, and the installing follower's
 * check passed it. The leader now refuses (503) to serve a database it has quarantined, so the follower's install
 * fails and keeps its own copy instead of swapping in one that is short of a committed entry; once the quarantine is
 * cleared the same install succeeds.
 */
class Issue8468QuarantinedSnapshotSourceIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 1);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 100L);
  }

  @Test
  @Timeout(180)
  void aLeaderRefusesToServeADatabaseItHasQuarantined() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String dbName = getDatabaseName();
    final String type = "Issue8468Record";

    final Database leaderDb = getServerDatabase(leaderIndex, dbName);
    leaderDb.transaction(() -> {
      leaderDb.getSchema().createVertexType(type);
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex(type).set("id", i).save();
    });
    assertClusterConsistency();

    final ArcadeStateMachine leaderMachine = getRaftPlugin(leaderIndex).getRaftHAServer().getStateMachine();
    final ArcadeStateMachine followerMachine = getRaftPlugin(followerIndex).getRaftHAServer().getStateMachine();

    // What a WAL version gap on the leader's apply thread records: the entry is skipped, the index moves on.
    leaderMachine.markStateDiverged(dbName, DivergenceCause.WAL_VERSION_GAP);
    try {
      final int status = snapshotStatus(leaderIndex, dbName);
      assertThat(status).as("the leader must not serve a copy it has quarantined").isEqualTo(503);

      assertThatThrownBy(() -> followerMachine.resyncDatabaseFromLeader(dbName))
          .as("the follower's install must fail rather than swap in the quarantined copy")
          .isInstanceOf(ReplicationException.class)
          .rootCause().hasMessageContaining("HTTP 503");
      assertThat(countOn(followerIndex, type)).as("the follower keeps its own copy").isEqualTo(10L);
    } finally {
      leaderMachine.clearDivergedDatabase(dbName);
    }

    assertThat(snapshotStatus(leaderIndex, dbName)).as("a healed database is served again").isEqualTo(200);
    followerMachine.resyncDatabaseFromLeader(dbName);
    assertThat(countOn(followerIndex, type)).isEqualTo(10L);

    leaderDb.transaction(() -> leaderDb.newVertex(type).set("id", 10).save());
    assertThat(awaitCountOn(followerIndex, type, 11)).isEqualTo(11);
  }

  /**
   * A quarantine recorded between the handler's first check and the capture of the image (code review on PR #8484): the
   * copy can hold entries past the skipped one while the reported index is below it, so the handler checks again once
   * the image is captured and still answers 503.
   */
  @Test
  @Timeout(180)
  void aQuarantineRecordedAfterTheFirstCheckIsStillRefused() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final String dbName = getDatabaseName();

    final Database leaderDb = getServerDatabase(leaderIndex, dbName);
    leaderDb.transaction(() -> leaderDb.getSchema().createVertexType("Issue8468Late"));
    assertClusterConsistency();

    final ArcadeStateMachine leaderMachine = getRaftPlugin(leaderIndex).getRaftHAServer().getStateMachine();
    final AtomicBoolean quarantinedInTheWindow = new AtomicBoolean();
    SnapshotHttpHandler.afterQuarantineCheckForTesting = () -> {
      if (quarantinedInTheWindow.compareAndSet(false, true))
        leaderMachine.markStateDiverged(dbName, DivergenceCause.APPLY_ERROR);
    };
    try {
      assertThat(snapshotStatus(leaderIndex, dbName)).as("the quarantine recorded before the capture must be seen")
          .isEqualTo(503);
      assertThat(quarantinedInTheWindow.get()).as("the quarantine must have landed after the first check").isTrue();
    } finally {
      SnapshotHttpHandler.afterQuarantineCheckForTesting = null;
      leaderMachine.clearDivergedDatabase(dbName);
    }
    assertThat(snapshotStatus(leaderIndex, dbName)).isEqualTo(200);
  }

  @AfterEach
  void clearSeam() {
    SnapshotHttpHandler.afterQuarantineCheckForTesting = null;
  }

  private int snapshotStatus(final int serverIndex, final String dbName) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/ha/snapshot/" + dbName).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      final int status = conn.getResponseCode();
      // Drain the body (the whole zip on a 200) so the handler finishes before the next request.
      try (final InputStream in = status >= 400 ? conn.getErrorStream() : conn.getInputStream()) {
        if (in != null)
          in.readAllBytes();
      }
      return status;
    } finally {
      conn.disconnect();
    }
  }
}
