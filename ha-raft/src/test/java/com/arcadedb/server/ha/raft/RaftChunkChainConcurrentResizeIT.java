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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HA shape of {@code ChunkChainConcurrentResizeStressTest}: the same concurrent grow/shrink of multi-page Entity
 * vertices, issued on every server of a 3-node Raft cluster, then CHECK DATABASE on each replica: no broken, orphaned
 * or cross-linked chunk chain and no record whose content no longer decodes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftChunkChainConcurrentResizeIT extends BaseRaftHATest {
  private static final int     THREADS_PER_SERVER = Integer.getInteger("repro.threads", 4);
  private static final int     OPERATIONS         = Integer.getInteger("repro.ops", 1_500);
  private static final int     HOT_SET            = Integer.getInteger("repro.hot", 200);
  private static final boolean VECTOR_INDEX       = Boolean.getBoolean("repro.vector");
  private static final int     DIMENSIONS         = 768;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void concurrentResizeOfChunkedRecordsKeepsEveryChainIntactOnEveryReplica() throws Exception {
    final int leader = findLeaderIndex();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.transaction(() -> {
      final VertexType entity = leaderDb.getSchema().buildVertexType().withName("Entity").withTotalBuckets(1).create();
      entity.createProperty("name", Type.STRING);
      entity.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      entity.createProperty("description", Type.STRING);
      entity.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
      leaderDb.getSchema().buildEdgeType().withName("RELATES_TO").withTotalBuckets(1).create();
    });
    if (VECTOR_INDEX)
      leaderDb.command("sql", """
          CREATE INDEX ON Entity (embedding) LSM_VECTOR
          METADATA { "dimensions": %d, "similarity": "COSINE" }""".formatted(DIMENSIONS));
    assertClusterConsistency();

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final AtomicLong conflicts = new AtomicLong();
    final List<Thread> workers = new ArrayList<>();

    for (int s = 0; s < getServerCount(); s++) {
      final Database db = getServerDatabase(s, getDatabaseName());
      for (int t = 0; t < THREADS_PER_SERVER; t++) {
        final Thread worker = new Thread(() -> runWorkload(db, errors, conflicts));
        workers.add(worker);
        worker.start();
      }
    }
    for (final Thread worker : workers)
      worker.join();

    assertClusterConsistency();

    for (int s = 0; s < getServerCount(); s++) {
      try (final ResultSet rs = getServerDatabase(s, getDatabaseName()).command("SQL", "check database")) {
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("totalMultiPageRecords")).longValue()).isGreaterThan(0L);
        assertThat(((Number) row.getProperty("totalErrors")).longValue()).as("server " + s + ": " + row.toJSON()).isZero();
        assertThat(((Number) row.getProperty("orphanedChunks")).longValue()).as("server " + s + ": " + row.toJSON()).isZero();
        assertThat(((Number) row.getProperty("crossLinkedChunks")).longValue()).as("server " + s + ": " + row.toJSON()).isZero();
        assertThat(((Number) row.getProperty("totalUndecodableRecords")).longValue()).as("server " + s + ": " + row.toJSON()).isZero();
      }
    }
    assertThat(errors).as("no operation may fail other than on a conflict").isEmpty();
  }

  private void runWorkload(final Database db, final List<Throwable> errors, final AtomicLong conflicts) {
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < OPERATIONS && errors.isEmpty(); i++) {
      final String name = "entity-" + random.nextInt(HOT_SET);
      try {
        final int op = random.nextInt(100);
        if (op < 30) {
          db.transaction(() -> merge(db, name), false, 3);
          db.transaction(() -> {
            final MutableVertex v = merge(db, name).modify();
            v.set("embedding", embedding(random));
            v.save();
          }, false, 3);
        } else if (op < 50) {
          db.transaction(() -> {
            final MutableVertex v = merge(db, name).modify();
            final String d = v.getString("description");
            v.set("description", (d == null ? "" : d) + "g".repeat(random.nextInt(200, 2_500)));
            v.save();
          }, false, 3);
        } else if (op < 65) {
          db.transaction(() -> {
            final MutableVertex v = merge(db, name).modify();
            final String d = v.getString("description");
            if (d != null && !d.isEmpty())
              v.set("description", d.substring(0, random.nextInt(d.length())));
            if (random.nextInt(4) == 0)
              v.remove("embedding");
            v.save();
          }, false, 3);
        } else {
          final String other = "entity-" + random.nextInt(HOT_SET);
          db.transaction(() -> merge(db, name).newEdge("RELATES_TO", merge(db, other)), false, 3);
        }
      } catch (final NeedRetryException | DuplicatedKeyException | RecordNotFoundException e) {
        conflicts.incrementAndGet();
      } catch (final Throwable e) {
        errors.add(e);
      }
    }
  }

  private static Vertex merge(final Database db, final String name) {
    final IndexCursor cursor = db.lookupByKey("Entity", "name", name);
    if (cursor.hasNext())
      return cursor.next().asVertex(true);
    return db.newVertex("Entity").set("name", name).save();
  }

  private static float[] embedding(final ThreadLocalRandom random) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = random.nextFloat();
    return v;
  }
}
