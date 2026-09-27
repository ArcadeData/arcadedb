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
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HA shape of {@code ConcurrentTailChunkAllocationTest}: small records on full pages grown by a few bytes in
 * concurrent batched transactions issued on every server, so the tail chunks of all writers compete for the same
 * partly-filled page. No two committed chains may point at the same tail slot on any replica.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftConcurrentTailChunkAllocationIT extends BaseRaftHATest {
  private static final int RECORDS            = Integer.getInteger("repro.records", 4_000);
  private static final int THREADS_PER_SERVER = Integer.getInteger("repro.threads", 3);
  private static final int BATCH              = Integer.getInteger("repro.batch", 20);
  private static final int ROUNDS             = Integer.getInteger("repro.rounds", 3);
  private static final long LEADER_CHANGE_MS  = Long.getLong("repro.leaderChangeMs", 0L);
  private static final long DDL_MS            = Long.getLong("repro.ddlMs", 0L);

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void concurrentGrowthNeverCrossLinksTailChunksOnAnyReplica() throws Exception {
    final int leader = findLeaderIndex();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    final List<RID> rids = new ArrayList<>(RECORDS);
    leaderDb.transaction(() -> {
      final var type = leaderDb.getSchema().buildVertexType().withName("Entity").withTotalBuckets(1).create();
      type.createProperty("name", Type.STRING);
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS).setExternal(true);
      type.createTypeIndex(com.arcadedb.schema.Schema.INDEX_TYPE.LSM_TREE, false, "name");
      leaderDb.getSchema().buildEdgeType().withName("RELATES_TO").withTotalBuckets(1).create();
      for (int i = 0; i < RECORDS; i++)
        rids.add(leaderDb.newVertex("Entity").set("name", "entity-" + i).set("text", "t".repeat(200 + i % 60))
            .set("embedding", embedding(i)).save().getIdentity());
    });
    assertClusterConsistency();

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final AtomicLong conflicts = new AtomicLong();
    final java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);
    final Thread chaos = new Thread(() -> {
      while (running.get() && LEADER_CHANGE_MS > 0) {
        try {
          Thread.sleep(LEADER_CHANGE_MS);
          // Targeted: a transfer with no target only steps the leader down at the same term (#8480)
          final int current = findLeaderIndex();
          final RaftHAPlugin plugin = getRaftPlugin(current);
          if (plugin != null)
            plugin.getRaftHAServer().transferLeadership(peerIdForIndex((current + 1) % getServerCount()), 10_000);
        } catch (final InterruptedException e) {
          return;
        } catch (final Throwable e) {
          // keep going
        }
      }
    });
    chaos.start();
    final Thread ddl = new Thread(() -> {
      int n = 0;
      while (running.get() && DDL_MS > 0) {
        try {
          Thread.sleep(DDL_MS);
          final Database db = getServerDatabase(findLeaderIndex(), getDatabaseName());
          final String prop = "ddl" + (n++);
          db.command("sql", "CREATE PROPERTY Entity." + prop + " IF NOT EXISTS STRING").close();
          db.command("sql", "CREATE INDEX IF NOT EXISTS ON Entity (" + prop + ") NOTUNIQUE").close();
          db.command("sql", "DROP INDEX `Entity[" + prop + "]`").close();
        } catch (final InterruptedException e) {
          return;
        } catch (final Throwable e) {
          // keep going
        }
      }
    });
    ddl.start();
    final int writers = getServerCount() * THREADS_PER_SERVER;

    for (int round = 0; round < ROUNDS; round++) {
      final String property = "p" + round;
      final List<Thread> workers = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final int writer = w;
        final int server = w % getServerCount();
        final Thread worker = new Thread(() -> {
          for (int from = writer * BATCH; from < RECORDS && errors.isEmpty(); from += writers * BATCH) {
            final int start = from;
            Throwable last = null;
            for (int attempt = 0; ; attempt++) {
              if (attempt == 1000) {
                errors.add(new AssertionError("batch " + start + " on server " + server + " never committed", last));
                break;
              }
              try {
                // Resolved per attempt: a resync of the replica replaces the database instance
                final Database db = getServerDatabase(server, getDatabaseName());
                db.transaction(() -> {
                  for (int i = start; i < Math.min(RECORDS, start + BATCH); i++) {
                    final com.arcadedb.graph.MutableVertex doc = db.lookupByRID(rids.get(i), true).asVertex().modify();
                    doc.set(property, "v" + i + "-" + property);
                    if (i % 3 == 0)
                      doc.set("embedding", embedding(i + 1));
                    doc.save();
                  }
                }, false, 1);
                break;
              } catch (final NeedRetryException e) {
                last = e;
                conflicts.incrementAndGet();
              } catch (final com.arcadedb.exception.TransactionException e) {
                // a conflict on a command forwarded to the leader comes back wrapped
                if (e.getMessage() == null || !e.getMessage().contains("ReplicatedPageConflictException"))
                  throw e;
                conflicts.incrementAndGet();
              } catch (final Throwable e) {
                if (LEADER_CHANGE_MS == 0) {
                  errors.add(e);
                  break;
                }
                // leadership moves under the workload: anything may fail, retry it
                last = e;
                try {
                  Thread.sleep(50);
                } catch (final InterruptedException ie) {
                  return;
                }
              }
            }
          }
        });
        workers.add(worker);
        worker.start();
      }
      for (final Thread worker : workers)
        worker.join();
    }

    running.set(false);
    chaos.interrupt();
    chaos.join();
    ddl.interrupt();
    ddl.join();
    for (int s = 0; s < getServerCount(); s++)
      waitForReplicationIsCompleted(s);

    for (int s = 0; s < getServerCount(); s++) {
      try (final ResultSet rs = getServerDatabase(s, getDatabaseName()).command("SQL", "check database")) {
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("crossLinkedChunks")).longValue()).as("server " + s + ": " + row.toJSON()).isZero();
        assertThat(((Number) row.getProperty("totalErrors")).longValue()).as("server " + s + ": " + row.toJSON()).isZero();
      }
    }
    assertThat(errors).isEmpty();
    assertClusterConsistency();
  }

  private static float[] embedding(final int seed) {
    final float[] v = new float[768];
    for (int i = 0; i < v.length; i++)
      v[i] = (seed * 31 + i) % 97 / 97F;
    return v;
  }
}
