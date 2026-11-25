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
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #11592 across a promotion: a transaction on a follower reads vertex b; the leader deletes b and inserts an identical vertex
 * that takes its RID; the follower applies the insert and is held after it published the page, and is promoted meanwhile. The
 * transaction then creates a relationship to b and commits on the new leader (a leader commit, which may merge an edge-list head)
 * while the apply is still held. The commit is refused with a retryable conflict and no relationship exists anywhere.
 */
class PromotionDuringReplicatedApplyIT extends BaseRaftHATest {
  private static final long   WAIT_S      = 60;
  private static final String VERTEX_TYPE = "Issue11592V";
  private static final String EDGE_TYPE   = "Issue11592E";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected int[] getServerToCheck() {
    return startedServers();
  }

  @AfterEach
  void clearHook() throws Exception {
    setApplyHook(null);
  }

  @Test
  @Timeout(300)
  void aCommitOnANodePromotedWhileItAppliesTheReplacementOfAVertexIsRefused() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final int third = 3 - leader - follower;
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    leaderDb.getSchema().buildVertexType().withName(VERTEX_TYPE).withTotalBuckets(1).create();
    leaderDb.getSchema().buildEdgeType().withName(EDGE_TYPE).withTotalBuckets(1).create();
    final AtomicReference<RID> a = new AtomicReference<>();
    final AtomicReference<RID> b = new AtomicReference<>();
    leaderDb.transaction(() -> {
      a.set(leaderDb.newVertex(VERTEX_TYPE).set("name", "a").save().getIdentity());
      b.set(leaderDb.newVertex(VERTEX_TYPE).set("name", "b").save().getIdentity());
    });
    await().atMost(WAIT_S, TimeUnit.SECONDS).until(() -> exists(followerDb, b.get()));

    // a known record counter, as in steady state: an apply locks a bucket whose counter is unknown (#8640), and that lock alone would
    // hold the commit off until its lock timeout
    assertThat(followerDb.countType(VERTEX_TYPE, false)).isEqualTo(2);
    final LocalBucket followerBucket = (LocalBucket) followerDb.getSchema().getBucketById(b.get().getBucketId());
    assertThat(followerBucket.getCachedRecordCount()).as("the follower's counter of the vertex bucket is known").isEqualTo(2);

    // only the follower applies the leader's entries through TransactionManager.applyChanges
    getServer(third).stop();

    // the transaction on the follower reads b alive
    final CountDownLatch read = new CountDownLatch(1);
    final CountDownLatch commitNow = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean leaderAtCommit = new AtomicBoolean();
    final AtomicBoolean committedWhileHeld = new AtomicBoolean();
    final AtomicBoolean applyHeld = new AtomicBoolean();
    final RaftHAServer followerRaft = getRaftPlugin(follower).getRaftHAServer();
    final Thread transaction = new Thread(() -> {
      try {
        followerDb.begin();
        final Vertex readB;
        try {
          readB = followerDb.lookupByRID(b.get(), true).asVertex();
        } finally {
          read.countDown();
        }
        if (!commitNow.await(WAIT_S, TimeUnit.SECONDS))
          throw new IllegalStateException("the follower was not promoted within " + WAIT_S + "s");
        followerDb.lookupByRID(a.get(), true).asVertex().newEdge(EDGE_TYPE, readB);
        leaderAtCommit.set(followerRaft.isLeader());
        followerDb.commit();
      } catch (final Throwable e) {
        failure.set(e);
        if (followerDb.isTransactionActive())
          followerDb.rollback();
      } finally {
        committedWhileHeld.set(applyHeld.get());
      }
    }, "issue11592-transaction");
    transaction.start();
    assertThat(read.await(WAIT_S, TimeUnit.SECONDS)).as("the transaction read b").isTrue();

    // the follower's apply of the second entry (the insert taking b's RID) is held once its first page is published
    final PageManager followerPages = ((DatabaseInternal) followerDb).getPageManager();
    final AtomicInteger followerApplies = new AtomicInteger();
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    setApplyHook(() -> {
      // the hook is JVM-wide: only the follower's applies, which publish on its page manager, are counted
      if ((followerPages.getPublicationSequence() & 1) == 0 || followerApplies.incrementAndGet() != 2)
        return;
      applyHeld.set(true);
      held.countDown();
      try {
        release.await(WAIT_S * 3, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        applyHeld.set(false);
      }
    });

    try {
      leaderDb.transaction(() -> leaderDb.lookupByRID(b.get(), true).asVertex().delete());
      final AtomicReference<RID> replacement = new AtomicReference<>();
      leaderDb.transaction(() -> replacement.set(leaderDb.newVertex(VERTEX_TYPE).set("name", "b").save().getIdentity()));
      assertThat(replacement.get()).as("the new vertex reuses the RID of the deleted one").isEqualTo(b.get());
      assertThat(held.await(WAIT_S, TimeUnit.SECONDS)).as("the follower's apply of the insert is held").isTrue();

      LogManager.instance().log(this, Level.INFO, "TEST: promoting server %d while its apply of the insert is held", follower);
      try {
        getRaftPlugin(leader).getRaftHAServer().transferLeadership(followerRaft.getLocalPeerId().toString(), 15_000);
      } catch (final RuntimeException e) {
        LogManager.instance().log(this, Level.INFO, "TEST: transfer reported %s, checking the role", null, e.toString());
      }
      await().atMost(WAIT_S, TimeUnit.SECONDS).until(followerRaft::isLeader);
      assertThat(applyHeld.get()).as("the apply is still held after the promotion").isTrue();

      commitNow.countDown();
      transaction.join(TimeUnit.SECONDS.toMillis(20));
    } finally {
      commitNow.countDown();
      release.countDown();
    }
    transaction.join(TimeUnit.SECONDS.toMillis(WAIT_S));
    assertThat(transaction.isAlive()).as("the transaction finished").isFalse();
    LogManager.instance().log(this, Level.INFO, "TEST: commit on the promoted node: leader=%s, finished while the apply was held=%s,"
        + " failure=%s", null, leaderAtCommit.get(), committedWhileHeld.get(), String.valueOf(failure.get()));

    assertThat(leaderAtCommit.get()).as("the commit ran on the promoted node as the leader").isTrue();
    // the #11592 refusal, from mergeEdgeListHeads, not a lock timeout: the apply holds no lock the commit needs
    assertThat(failure.get()).as("the commit is refused with a retryable conflict")
        .isInstanceOf(ConcurrentModificationException.class);
    assertThat(committedWhileHeld.get()).as("the commit was refused while the apply was still held").isTrue();

    await().atMost(WAIT_S, TimeUnit.SECONDS).until(() -> exists(followerDb, b.get()));
    for (final int server : startedServers()) {
      final Database db = getServerDatabase(server, getDatabaseName());
      try (final ResultSet result = db.query("sql", "SELECT count(@rid) AS c FROM " + EDGE_TYPE)) {
        assertThat(result.next().<Number>getProperty("c").longValue()).as("server %d holds no relationship", server).isZero();
      }
      db.transaction(() -> {
        assertThat(db.lookupByRID(a.get(), true).asVertex().getVertices(Vertex.DIRECTION.OUT, EDGE_TYPE)).as("server %d", server)
            .isEmpty();
        assertThat(db.lookupByRID(b.get(), true).asVertex().getVertices(Vertex.DIRECTION.IN, EDGE_TYPE)).as("server %d", server)
            .isEmpty();
      });
    }
  }

  private static boolean exists(final Database db, final RID rid) {
    return db.existsRecord(rid);
  }

  /** TransactionManager.applyChangesHookForTesting is package-private to the engine: this test sets it from another package. */
  private static void setApplyHook(final Runnable hook) throws Exception {
    final Field field = TransactionManager.class.getDeclaredField("applyChangesHookForTesting");
    field.setAccessible(true);
    field.set(null, hook);
  }
}
