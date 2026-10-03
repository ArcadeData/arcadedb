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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8985: concurrent read-modify-write transactions on vertices that grow past one page lost updates with the
 * disjoint-slot merge on: two committed transactions wrote the same {@code n} after reading the same version.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue8985LostUpdateGrowingVerticesTest extends TestHelper {
  private static final int RECORDS            = 40;
  private static final int THREADS            = 12;
  private static final int UPDATES_PER_THREAD = 300;
  private static final int ROUNDS             = 5;

  /**
   * The window the stress below hits, made deterministic: a record taken for modification, another transaction commits a
   * change to it, and only then is the first one saved. A record that spans pages used to slip through: the pin took the
   * newer head page, the commit-time checks compared that page with itself, and the older value overwrote the change.
   */
  @Test
  void aMultiPageRecordModifiedBeforeAConcurrentCommitIsRefusedAtSave() {
    database.getSchema().createDocumentType("Big");
    final RID[] rid = new RID[1];
    database.transaction(
        () -> rid[0] = database.newDocument("Big").set("n", 0).set("s", "x".repeat(150_000)).save().getIdentity());

    database.begin();
    try {
      final MutableDocument mine = database.lookupByRID(rid[0], true).asDocument().modify();

      final Thread other = new Thread(() -> database.transaction(() -> {
        final MutableDocument d = database.lookupByRID(rid[0], true).asDocument().modify();
        d.set("n", 1);
        d.set("s", d.getString("s") + "y");
        d.save();
      }));
      other.start();
      try {
        other.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      mine.set("n", mine.getInteger("n") + 1);
      assertThatThrownBy(() -> {
        mine.save();
        database.commit();
      }).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(database.lookupByRID(rid[0], true).asDocument().getInteger("n")).isEqualTo(1);
  }

  @Test
  void noCommittedUpdateIsLost() throws Exception {
    database.getSchema().createVertexType("V");

    for (int round = 0; round < ROUNDS; round++) {
      final String type = "V";
      final String chunk = "y".repeat(2000);
      final RID[] rids = new RID[RECORDS];
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          rids[i] = database.newVertex(type).set("n", 0).set("s", "").save().getIdentity();
      });

      final AtomicIntegerArray committed = new AtomicIntegerArray(RECORDS);
      final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        final int seed = t + round * 100;
        futures.add(pool.submit(() -> {
          final Random random = new Random(seed);
          for (int u = 0; u < UPDATES_PER_THREAD; u++) {
            final int i = random.nextInt(RECORDS);
            database.transaction(() -> {
              final MutableVertex v = database.lookupByRID(rids[i], true).asVertex().modify();
              v.set("n", v.getInteger("n") + 1);
              v.set("s", v.getString("s") + chunk);
              v.save();
            }, false, 100_000);
            committed.incrementAndGet(i);
          }
        }));
      }
      for (final Future<?> f : futures)
        f.get();
      pool.shutdown();

      int lost = 0;
      for (int i = 0; i < RECORDS; i++) {
        final Vertex v = database.lookupByRID(rids[i], true).asVertex();
        final int n = v.getInteger("n");
        if (n != committed.get(i) || v.getString("s").length() != n * chunk.length())
          lost += committed.get(i) - n;
      }
      assertThat(lost).as("round " + round + ": updates committed but not reflected in the data").isZero();

      database.transaction(() -> database.command("sql", "DELETE FROM V"));
    }
  }
}
