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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies primary + paired external bucket pages are replicated atomically when the leader crashes mid-commit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftLeaderCrashWithExternalPropertyIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE      = "ExtCrash";
  private static final String EXT_PROPERTY     = "blob";
  private static final String INJECTED_PAYLOAD = "injected-payload-the-quick-brown-fox-jumps-over-the-lazy-dog";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @AfterEach
  void clearPostReplicationHook() {
    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = null;
  }

  @Test
  void externalPropertyRecoversAtomicallyAfterLeaderCrashBetweenReplicationAndPhase2() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Phase 1: schema + baseline writes with no fault injection. Each save touches BOTH the primary bucket
    // and the paired external bucket in a single transaction.
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE)) {
        final VertexType type = leaderDb.getSchema().createVertexType(VERTEX_TYPE);
        type.createProperty(EXT_PROPERTY, Type.STRING).setExternal(true);
      }
    });
    final int baseline = 50;
    leaderDb.transaction(() -> {
      for (int i = 0; i < baseline; i++) {
        final MutableVertex v = leaderDb.newVertex(VERTEX_TYPE);
        v.set("name", "baseline-" + i);
        v.set(EXT_PROPERTY, "baseline-blob-" + i);
        v.save();
      }
    });
    assertClusterConsistency();
    assertExternalBucketsAlignWithPrimary(baseline);

    // Phase 2: arm the fault-injection hook. Single-shot: fires on the next successful Raft replication and
    // (a) stops the leader on a separate thread (stopping inline would deadlock the Ratis gRPC channel),
    // (b) throws so commit2ndPhase() never runs locally on the crashed leader.
    final AtomicBoolean hookFired = new AtomicBoolean(false);
    final CountDownLatch leaderStopped = new CountDownLatch(1);
    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = dbName -> {
      if (!hookFired.compareAndSet(false, true))
        return;
      LogManager.instance().log(RaftLeaderCrashWithExternalPropertyIT.class, Level.INFO,
          "TEST: fault-injection hook firing for db=%s, stopping leader %d asynchronously",
          dbName, leaderIndex);
      final Thread stopper = new Thread(() -> {
        try {
          getServer(leaderIndex).stop();
        } catch (final Throwable t) {
          LogManager.instance().log(RaftLeaderCrashWithExternalPropertyIT.class, Level.WARNING,
              "TEST: async leader stop failed: %s", t.getMessage());
        } finally {
          leaderStopped.countDown();
        }
      }, "TEST-fault-injection-stop");
      stopper.setDaemon(true);
      stopper.start();
      throw new RuntimeException(
          "TEST fault injection: simulated leader crash between Raft commit and commit2ndPhase");
    };

    // Phase 3: write a record whose EXTERNAL property has a recognisable payload. The transaction commits
    // BOTH primary and external bucket pages; the fault fires AFTER Raft has replicated both to followers.
    final RID[] injectedRid = new RID[1];
    try {
      leaderDb.begin();
      final MutableVertex v = leaderDb.newVertex(VERTEX_TYPE);
      v.set("name", "injected-0");
      v.set(EXT_PROPERTY, INJECTED_PAYLOAD);
      v.save();
      injectedRid[0] = v.getIdentity();
      leaderDb.commit();
    } catch (final Exception expected) {
      LogManager.instance().log(this, Level.INFO,
          "TEST: leader commit failed as expected: %s", expected.getMessage());
    }

    assertThat(hookFired.get()).as("Fault-injection hook must have fired").isTrue();
    assertThat(leaderStopped.await(30, TimeUnit.SECONDS))
        .as("Async leader stop must complete within 30s").isTrue();

    // Phase 4: a new leader must emerge from the 2 survivors.
    final int newLeaderIndex = waitForNewLeader(leaderIndex);
    assertThat(newLeaderIndex).as("A new leader must be elected").isGreaterThanOrEqualTo(0);
    assertThat(newLeaderIndex).as("New leader must differ from crashed leader").isNotEqualTo(leaderIndex);
    LogManager.instance().log(this, Level.INFO, "TEST: new leader elected: server %d", newLeaderIndex);

    // Phase 5: the injected record AND its EXTERNAL value must be visible on the new leader. This proves the
    // primary AND external bucket pages were replicated atomically before the leader crashed (and that
    // Raft followers applied them as a unit).
    waitForReplicationIsCompleted(newLeaderIndex);
    final Database newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    assertThat(newLeaderDb.countType(VERTEX_TYPE, true))
        .as("New leader must have baseline + injected record")
        .isEqualTo(baseline + 1L);
    final Vertex injectedOnNewLeader = newLeaderDb.lookupByRID(injectedRid[0], true).asVertex();
    assertThat(injectedOnNewLeader.getString(EXT_PROPERTY))
        .as("EXTERNAL value must be readable on the new leader (atomic primary+external replication)")
        .isEqualTo(INJECTED_PAYLOAD);

    // Phase 6: restart the crashed leader. Its Raft log has the committed entry (primary+external pages) but
    // commit2ndPhase() never ran, so neither half is on disk. Ratis replays the entry through the state
    // machine follower path, applying both halves in lock-step.
    Thread.sleep(2_000);
    LogManager.instance().log(this, Level.INFO, "TEST: restarting old leader %d", leaderIndex);
    getServer(leaderIndex).start();

    waitForReplicationIsCompleted(leaderIndex);

    // Phase 7: the recovered old leader must hold both halves. Specifically:
    //   - countType reflects the injected record (primary bucket recovered)
    //   - reading the EXTERNAL property returns the original payload (external bucket recovered)
    //   - the external bucket count matches the primary bucket count (no half-records, no orphans)
    final Database oldLeaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    assertThat(oldLeaderDb.countType(VERTEX_TYPE, true))
        .as("Recovered old leader must have baseline + injected record")
        .isEqualTo(baseline + 1L);
    final Vertex injectedOnOldLeader = oldLeaderDb.lookupByRID(injectedRid[0], true).asVertex();
    assertThat(injectedOnOldLeader.getString(EXT_PROPERTY))
        .as("EXTERNAL value must be readable on recovered old leader (atomic WAL replay of both buckets)")
        .isEqualTo(INJECTED_PAYLOAD);
    assertExternalBucketsAlignWithPrimary(baseline + 1);

    // Phase 8: cross-node page-level convergence (already counts external buckets - they are regular components).
    assertClusterConsistency();
  }

  /** Catches half-record corruption: each primary bucket and its paired ext bucket must hold the same row count. */
  private void assertExternalBucketsAlignWithPrimary(final int expectedRecordCount) {
    for (int i = 0; i < getServerCount(); i++) {
      if (getServer(i) == null || !getServer(i).isStarted())
        continue;
      final Database db = getServerDatabase(i, getDatabaseName());
      final LocalDocumentType type = (LocalDocumentType) db.getSchema().getType(VERTEX_TYPE);
      final LocalSchema schema = (LocalSchema) db.getSchema().getEmbedded();
      long primaryTotal = 0;
      long externalTotal = 0;
      for (final var b : type.getBuckets(false)) {
        primaryTotal += b.count();
        final Integer extId = type.getExternalBucketIdFor(b.getFileId());
        assertThat(extId).as("type '%s' on server %d primary bucket '%s' must have a paired external bucket",
            VERTEX_TYPE, i, b.getName()).isNotNull();
        final LocalBucket extBucket = schema.getBucketById(extId);
        externalTotal += extBucket.count();
      }
      assertThat(primaryTotal)
          .as("server %d: primary bucket count for '%s'", i, VERTEX_TYPE)
          .isEqualTo(expectedRecordCount);
      assertThat(externalTotal)
          .as("server %d: external bucket count must equal primary bucket count for '%s' "
              + "(half-record indicates WAL replay applied only one half of the transaction)", i, VERTEX_TYPE)
          .isEqualTo(expectedRecordCount);
    }
  }

  private int waitForNewLeader(final int excludeIndex) {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
        if (i == excludeIndex)
          continue;
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.isLeader())
          return i;
      }
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
    return -1;
  }
}
