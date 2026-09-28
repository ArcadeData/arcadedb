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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
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
 * Concurrent grow/shrink of multi-page records sharing pages, shaped on a production workload: entities are MERGEd by
 * a unique name from a hot set, grown past their page with a 768-float embedding and text in separate transactions,
 * shrunk back, and linked by edges, from several threads at once. A chunk slot freed while a live chain still points at
 * it surfaces as a broken chunk chain, on read or in CHECK DATABASE; a chunk two committed chains both point at (a
 * cross-linked chain, which reads as healthy until either record changes) and content that no longer decodes are
 * reported by CHECK DATABASE too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class ChunkChainConcurrentResizeStressTest extends TestHelper {
  private static final int     THREADS      = Integer.getInteger("repro.threads", 8);
  private static final int     OPERATIONS   = Integer.getInteger("repro.ops", 4_000);
  private static final int     HOT_SET      = Integer.getInteger("repro.hot", 400);
  private static final boolean VECTOR_INDEX = Boolean.getBoolean("repro.vector");
  private static final boolean DELETES      = Boolean.getBoolean("repro.deletes");
  private static final int     PREFILL      = Integer.getInteger("repro.prefill", 0);
  private static final int     DIMENSIONS   = 768;

  @Test
  void concurrentResizeOfChunkedRecordsKeepsEveryChainIntact() throws Exception {
    database.transaction(() -> {
      final VertexType entity = database.getSchema().buildVertexType().withName("Entity").withTotalBuckets(1).create();
      entity.createProperty("name", Type.STRING);
      entity.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      entity.createProperty("description", Type.STRING);
      entity.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
      database.getSchema().buildEdgeType().withName("RELATES_TO").withTotalBuckets(1).create();
      if (VECTOR_INDEX)
        database.command("sql", """
            CREATE INDEX ON Entity (embedding) LSM_VECTOR
            METADATA { "dimensions": %d, "similarity": "COSINE" }""".formatted(DIMENSIONS));
    });

    // A large bucket of small records, a share of them deleted, so later chunks land in holes of old pages
    for (int batch = 0; batch < PREFILL; batch += 5_000) {
      final int from = batch;
      database.transaction(() -> {
        for (int i = from; i < Math.min(PREFILL, from + 5_000); i++)
          database.newVertex("Entity").set("name", "entity-" + i).set("description", "p".repeat(i % 300)).save();
      });
    }
    database.transaction(() -> {
      for (int i = 0; i < PREFILL; i += 3) {
        final Vertex v = lookup("entity-" + i);
        if (v != null)
          v.delete();
      }
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final AtomicLong conflicts = new AtomicLong();

    final List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < THREADS; t++) {
      final Thread worker = new Thread(() -> {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < OPERATIONS && errors.isEmpty(); i++) {
          final String name = "entity-" + random.nextInt(Math.max(HOT_SET, PREFILL));
          try {
            final int op = random.nextInt(100);
            if (op < 30) {
              // MERGE, then grow the embedding in a SEPARATE transaction, like the reported application does
              database.transaction(() -> merge(name), false, 3);
              database.transaction(() -> {
                final MutableVertex v = merge(name).modify();
                v.set("embedding", embedding(random));
                v.save();
              }, false, 3);
            } else if (op < 50) {
              // grow gradually, so chains get extended chunk by chunk
              database.transaction(() -> {
                final MutableVertex v = merge(name).modify();
                final String d = v.getString("description");
                v.set("description", (d == null ? "" : d) + "g".repeat(random.nextInt(200, 2_500)));
                v.save();
              }, false, 3);
            } else if (op < 65) {
              // shrink
              database.transaction(() -> {
                final MutableVertex v = merge(name).modify();
                final String d = v.getString("description");
                if (d != null && !d.isEmpty())
                  v.set("description", d.substring(0, random.nextInt(d.length())));
                if (random.nextInt(4) == 0)
                  v.remove("embedding");
                v.save();
              }, false, 3);
            } else if (op < 97 || !DELETES) {
              final String other = "entity-" + random.nextInt(Math.max(HOT_SET, PREFILL));
              database.transaction(() -> merge(name).newEdge("RELATES_TO", merge(other)), false, 3);
            } else {
              database.transaction(() -> {
                final Vertex v = lookup(name);
                if (v != null)
                  v.delete();
              }, false, 3);
            }
          } catch (final ConcurrentModificationException | DuplicatedKeyException | RecordNotFoundException e) {
            conflicts.incrementAndGet();
          } catch (final Throwable e) {
            errors.add(e);
          }
        }
      });
      workers.add(worker);
      worker.start();
    }
    for (final Thread worker : workers)
      worker.join();

    try (final ResultSet rs = database.command("SQL", "check database")) {
      final Result row = rs.next();
      assertThat(errors).as("no operation may fail other than on a conflict").isEmpty();
      assertThat(((Number) row.getProperty("totalMultiPageRecords")).longValue()).as("the workload must build chunk chains")
          .isGreaterThan(0L);
      assertThat(((Number) row.getProperty("totalErrors")).longValue()).as("check database: " + row.toJSON()).isZero();
      assertThat(((Number) row.getProperty("orphanedChunks")).longValue()).as("check database: " + row.toJSON()).isZero();
      assertThat(((Number) row.getProperty("crossLinkedChunks")).longValue()).as("check database: " + row.toJSON()).isZero();
      assertThat(((Number) row.getProperty("totalUndecodableRecords")).longValue()).as("check database: " + row.toJSON()).isZero();
    }
  }

  private Vertex lookup(final String name) {
    final IndexCursor cursor = database.lookupByKey("Entity", "name", name);
    return cursor.hasNext() ? cursor.next().asVertex(true) : null;
  }

  private Vertex merge(final String name) {
    final Vertex existing = lookup(name);
    if (existing != null)
      return existing;
    return database.newVertex("Entity").set("name", name).save();
  }

  private static float[] embedding(final ThreadLocalRandom random) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = random.nextFloat();
    return v;
  }
}
