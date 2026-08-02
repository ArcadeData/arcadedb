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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.FileManager;
import com.arcadedb.exception.TimeoutException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5728: a schema change executed while ANOTHER thread holds the
 * file-manager recording session was applied on the leader alone and never proposed to Raft.
 * <p>
 * {@code FileManager.recordedChanges} is a single per-database slot with no owner, so
 * {@code RaftReplicatedDatabase.recordFileChanges} could not tell a safe re-entrant nesting (the same
 * thread already inside its own replicating session, whose outer frame ships everything) from a
 * genuinely contended one (a different thread's session, which ships nothing on our behalf). It treated
 * both as "someone else will replicate this" and ran the callback straight on the inner
 * {@code LocalDatabase}. Nothing threw; the followers simply stayed one type behind.
 * <p>
 * The window is real work, not an instant: the owning session releases the database write lock when its
 * callback returns but keeps the recording session until its {@code replicateSchema()} Raft round trip
 * completes. That is why the original symptom -
 * {@code Bolt5002RoutingTableIT.neo4jSchemeRoutesReadsAndWrites} losing its Cypher-created vertex type -
 * appeared only on loaded CI runners.
 * <p>
 * The state is planted rather than raced for: the test itself takes the recording session, the same
 * technique {@link RaftIndexCompactionReplicationIT#compactionDefersWhenRecordingSessionActive()} uses to
 * pin the sibling #4063 contract. Without the fix the DDL returns immediately down the local-only path
 * and the followers never see the type; with it, the DDL waits for the session and then replicates.
 */
class Issue5728SchemaChangeDuringRecordingSessionIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE      = "Issue5728Contended";
  private static final long   HOLD_SESSION_MS  = 1_000;
  private static final long   DDL_TIMEOUT_SECS = 60;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void schemaChangeReplicatesWhenAnotherThreadHoldsTheRecordingSession() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final DatabaseInternal leaderDb = (DatabaseInternal) getServerDatabase(leaderIndex, getDatabaseName());

    // Settle in-flight async work so the session we take is not immediately contended by the engine.
    leaderDb.async().waitCompletion();

    final FileManager fileManager = leaderDb.getEmbedded().getFileManager();
    assertThat(acquireRecordingSession(fileManager))
        .as("the test must own the recording session, otherwise it is not reproducing the contended case")
        .isTrue();

    final CountDownLatch ddlStarted = new CountDownLatch(1);
    final CountDownLatch ddlFinished = new CountDownLatch(1);
    final AtomicReference<Throwable> ddlFailure = new AtomicReference<>();

    final Thread ddl = new Thread(() -> {
      ddlStarted.countDown();
      try {
        leaderDb.getSchema().buildVertexType().withName(VERTEX_TYPE).withTotalBuckets(1).create();
      } catch (final Throwable t) {
        ddlFailure.set(t);
      } finally {
        ddlFinished.countDown();
      }
    }, "issue5728-ddl");
    ddl.start();

    assertThat(ddlStarted.await(10, TimeUnit.SECONDS)).isTrue();
    // Hold the session across the start of the DDL. The exact delay only has to be long enough that an
    // unfixed build has taken its local-only shortcut by the time we release; a fixed build is still
    // waiting and finishes whenever we let go.
    Thread.sleep(HOLD_SESSION_MS);
    fileManager.stopRecordingChanges();

    assertThat(ddlFinished.await(DDL_TIMEOUT_SECS, TimeUnit.SECONDS))
        .as("the schema change must not block indefinitely on the contending session")
        .isTrue();
    ddl.join(TimeUnit.SECONDS.toMillis(DDL_TIMEOUT_SECS));
    assertThat(ddlFailure.get()).isNull();

    assertThat(leaderDb.getSchema().existsType(VERTEX_TYPE)).as("leader must have the new type").isTrue();

    waitForAllServers();

    testEachServer(serverIndex -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(serverDb.getSchema().existsType(VERTEX_TYPE))
          .as("""
              server %d is missing type '%s': the schema change ran while another thread held the recording \
              session and was applied on the leader only, replicating nothing (#5728)""", serverIndex, VERTEX_TYPE)
          .isTrue();
    });
  }

  /**
   * The other half of the contract: when the contending session outlives the wait, the schema change must
   * fail loudly rather than fall back to the local-only path that diverged the cluster. Without this the
   * timeout branch is never executed, so a fix that silently swallowed the expiry would still look green.
   * <p>
   * The session must be held by a thread OTHER than the one running the DDL. A holder on the DDL's own
   * thread is the legitimate re-entrant nesting the fix deliberately still allows through, so the wait -
   * and therefore the timeout - would never be reached.
   */
  @Test
  void schemaChangeFailsRatherThanDivergingWhenTheSessionIsNeverReleased() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final DatabaseInternal leaderDb = (DatabaseInternal) getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.async().waitCompletion();

    final FileManager fileManager = leaderDb.getEmbedded().getFileManager();
    final String timeoutType = VERTEX_TYPE + "Timeout";
    final CountDownLatch sessionTaken = new CountDownLatch(1);
    final CountDownLatch releaseSession = new CountDownLatch(1);
    final AtomicBoolean sessionOwned = new AtomicBoolean();

    // Holds the session for the whole attempt, so the DDL exhausts HA_QUORUM_TIMEOUT and gives up.
    final Thread holder = new Thread(() -> {
      try {
        sessionOwned.set(acquireRecordingSession(fileManager));
        sessionTaken.countDown();
        releaseSession.await(DDL_TIMEOUT_SECS, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        if (sessionOwned.get())
          fileManager.stopRecordingChanges();
      }
    }, "issue5728-session-holder");
    holder.start();

    try {
      assertThat(sessionTaken.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(sessionOwned.get())
          .as("the holder thread must own the recording session, otherwise this is not the contended case")
          .isTrue();

      assertThatThrownBy(() ->
          leaderDb.getSchema().buildVertexType().withName(timeoutType).withTotalBuckets(1).create())
          .as("a schema change that cannot claim the recording session must throw, never apply leader-locally")
          .isInstanceOf(TimeoutException.class)
          .hasMessageContaining("waiting for the file recording session");
    } finally {
      releaseSession.countDown();
      holder.join(TimeUnit.SECONDS.toMillis(DDL_TIMEOUT_SECS));
    }

    assertThat(leaderDb.getSchema().existsType(timeoutType))
        .as("the refused schema change must not have been applied on the leader either")
        .isFalse();
  }

  /**
   * Polls for the recording session, which an engine-internal DDL or compaction may momentarily hold
   * even after {@code waitCompletion()}.
   */
  private static boolean acquireRecordingSession(final FileManager fileManager) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 5_000;
    while (System.currentTimeMillis() < deadline) {
      if (fileManager.startRecordingChanges())
        return true;
      Thread.sleep(50);
    }
    return false;
  }
}
