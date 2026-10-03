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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9071: four threads insert, update and delete records of a type with a COSINE LSM_VECTOR index while a fifth one runs
 * COMPACT INDEX every 250 ms. A record committed between the moment the compaction read the live set from the pages and the
 * moment it swapped the rewritten file in was in neither: the old file was dropped and the new one never held it, so
 * vectorNeighbors() with the record's own vector did not return it, before and after a reopen, until REBUILD INDEX.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue9071CompactionDuringConcurrentWritesTest extends TestHelper {
  private static final int DIM     = 8;
  private static final int WRITERS = 4;

  @Test
  void everyCommittedRecordIsFoundAfterCompactionsRanBetweenCommits() throws Exception {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.command("sql", "CREATE PROPERTY P.pid LONG");
      database.command("sql", "CREATE PROPERTY P.emb ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON P (pid) UNIQUE");
      database.command("sql", "CREATE INDEX ON P (emb) LSM_VECTOR METADATA { \"dimensions\": " + DIM
          + ", \"similarity\": \"COSINE\", \"mutationsBeforeRebuild\": 150 }");
    });

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicInteger compactions = new AtomicInteger();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < WRITERS; t++) {
      final int writer = t;
      threads.add(new Thread(() -> {
        try {
          final RID[] ring = new RID[8];
          for (long s = 0; !stop.get(); s++) {
            final long fs = s;
            final RID[] mine = new RID[1];
            database.transaction(() -> {
              final MutableDocument d = database.newDocument("P").set("pid", writer * 1_000_000L + fs).set("emb", vector(writer, fs, 0));
              d.save();
              mine[0] = d.getIdentity();
              if (fs % 5 == 4)
                database.lookupByRID(ring[(int) ((fs - 3) % 8)], true).asDocument().modify().set("emb", vector(writer, fs - 3, 1)).save();
              if (fs % 7 == 6)
                database.lookupByRID(ring[(int) ((fs - 4) % 8)], true).delete();
            }, false, 100);
            ring[(int) (s % 8)] = mine[0];
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }));
    }
    threads.add(new Thread(() -> {
      try {
        while (!stop.get()) {
          try (final ResultSet rs = database.command("sql", "COMPACT INDEX `P[emb]`")) {
            while (rs.hasNext())
              rs.next();
            compactions.incrementAndGet();
          } catch (final TimeoutException | IndexException e) {
            // a compaction the writers did not leave room for is retried by the next round
          }
          Thread.sleep(250);
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }));

    threads.forEach(Thread::start);
    Thread.sleep(12_000);
    stop.set(true);
    for (final Thread t : threads)
      t.join();

    assertThat(failure.get()).as("a writer failed").isNull();
    assertThat(compactions.get()).as("compactions that ran while the writers were committing").isGreaterThan(0);
    assertThat(missing()).as("records missing before the close").isEmpty();

    reopenDatabase();
    assertThat(missing()).as("records missing after the reopen").isEmpty();
  }

  private List<String> missing() {
    final List<String> missing = new ArrayList<>();
    final Iterator<Record> it = database.iterateType("P", false);
    while (it.hasNext()) {
      final Document d = (Document) it.next();
      final Object e = d.get("emb");
      final float[] q;
      if (e instanceof float[] floats)
        q = floats;
      else {
        final List<?> l = (List<?>) e;
        q = new float[l.size()];
        for (int i = 0; i < q.length; i++)
          q[i] = ((Number) l.get(i)).floatValue();
      }
      boolean found = false;
      try (final ResultSet rs = database.query("sql", "SELECT expand(vectorNeighbors('P[emb]', ?, 10, 400))", q)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          if (r.getIdentity().isPresent() && r.getIdentity().get().equals(d.getIdentity()))
            found = true;
        }
      }
      if (!found)
        missing.add(d.getIdentity() + " pid=" + d.get("pid"));
    }
    return missing;
  }

  private static float[] vector(final long t, final long s, final int gen) {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      long z = (t * 1000003L + s) * 31 + gen * 7919L + i * 104729L + 12345;
      z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
      z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
      z ^= z >>> 31;
      v[i] = (float) ((z >>> 11) * (1.0 / (1L << 53)) * 2 - 1);
    }
    return v;
  }
}
