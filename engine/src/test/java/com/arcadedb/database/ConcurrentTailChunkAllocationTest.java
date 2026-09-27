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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Small records on full pages, grown by a few bytes at a time in concurrent batched transactions: every growth spills
 * a tiny tail chunk, and the tail chunks of all the writers compete for the same partly-filled page. Two transactions
 * must never both commit a chain pointing at the same tail slot (a cross-linked chunk), which reads as two healthy
 * records until either of them is updated or deleted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class ConcurrentTailChunkAllocationTest extends BucketPageLayoutTestSupport {
  private static final String TYPE    = "Entity";
  private static final int    RECORDS = Integer.getInteger("repro.records", 6_000);
  private static final int    THREADS = Integer.getInteger("repro.threads", 8);
  private static final int    BATCH   = Integer.getInteger("repro.batch", 40);
  private static final int    ROUNDS  = Integer.getInteger("repro.rounds", 4);
  private static final int    INSERTERS = Integer.getInteger("repro.inserters", 2);

  @Test
  void concurrentGrowthNeverCrossLinksTailChunks() throws Exception {
    final List<RID> rids = new ArrayList<>(RECORDS);
    database.transaction(() -> {
      final var type = database.getSchema().buildVertexType().withName(TYPE).withTotalBuckets(1).create();
      type.createProperty("name", Type.STRING);
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS).setExternal(true);
      type.createTypeIndex(com.arcadedb.schema.Schema.INDEX_TYPE.LSM_TREE, false, "name");
      database.getSchema().buildEdgeType().withName("RELATES_TO").withTotalBuckets(1).create();
      for (int i = 0; i < RECORDS; i++)
        rids.add(database.newVertex(TYPE).set("name", "entity-" + i).set("text", "t".repeat(200 + i % 60))
            .set("embedding", embedding(i)).save().getIdentity());
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final AtomicLong conflicts = new AtomicLong();

    for (int round = 0; round < ROUNDS; round++) {
      final String property = "p" + round;
      final List<Thread> workers = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        final int thread = t;
        final Thread worker = new Thread(() -> {
          // interleaved slices: every writer grows records spread over the whole bucket, so their tail chunks all
          // land on the same pages at the same time
          for (int from = thread * BATCH; from < RECORDS && errors.isEmpty(); from += THREADS * BATCH) {
            final int start = from;
            for (int attempt = 0; ; attempt++) {
              if (attempt == 500) {
                errors.add(new AssertionError("batch " + start + " never committed"));
                break;
              }
              try {
                database.transaction(() -> {
                  for (int i = start; i < Math.min(RECORDS, start + BATCH); i++) {
                    final com.arcadedb.graph.MutableVertex doc = rids.get(i).asVertex(true).modify();
                    doc.set(property, "v" + i + "-" + property);
                    if (i % 3 == 0)
                      doc.set("embedding", embedding(i + 1));
                    doc.save();
                    if (i % 4 == 0)
                      doc.newEdge("RELATES_TO", rids.get((i * 7 + 13) % RECORDS).asVertex(true));
                  }
                }, false, 1);
                break;
              } catch (final com.arcadedb.exception.NeedRetryException e) {
                conflicts.incrementAndGet();
              } catch (final Throwable e) {
                errors.add(e);
                break;
              }
            }
          }
        });
        workers.add(worker);
        worker.start();
      }
      final int[] inserted = new int[1];
      for (int t = 0; t < INSERTERS; t++) {
        final int thread = t;
        final Thread inserter = new Thread(() -> {
          for (int i = 0; i < RECORDS / 20 && errors.isEmpty(); i++) {
            final int n = i;
            try {
              database.transaction(() -> {
                for (int k = 0; k < 5; k++)
                  database.newVertex(TYPE).set("name", "new-" + property + "-" + thread + "-" + n + "-" + k)
                      .set("text", "n".repeat(220)).set("embedding", embedding(n)).save();
              }, false, 10);
            } catch (final ConcurrentModificationException e) {
              conflicts.incrementAndGet();
            } catch (final Throwable e) {
              errors.add(e);
            }
          }
        });
        workers.add(inserter);
        inserter.start();
      }
      for (final Thread worker : workers)
        worker.join();
    }

    assertThat(errors).isEmpty();

    final Result row = checkDatabaseRow(false);
    assertThat(numberProperty(row, "totalMultiPageRecords")).as("the workload must build chunk chains").isPositive();
    assertThat(numberProperty(row, "crossLinkedChunks")).as("conflicts=" + conflicts + " " + row.toJSON()).isZero();
    assertThat(numberProperty(row, "totalErrors")).as(row.toJSON().toString()).isZero();

    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        final Document doc = rids.get(i).asDocument(true);
        for (int round = 0; round < ROUNDS; round++)
          assertThat(doc.getString("p" + round)).isEqualTo("v" + i + "-p" + round);
      }
    });
  }

  private static float[] embedding(final int seed) {
    final float[] v = new float[768];
    for (int i = 0; i < v.length; i++)
      v[i] = (seed * 31 + i) % 97 / 97F;
    return v;
  }
}
