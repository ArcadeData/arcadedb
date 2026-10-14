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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10002: grouped {@code vector.neighbors} and {@code vector.sparseNeighbors} answered short while other
 * transactions committed, although the corpus always held far more live groups and members than the answer needed.
 * <p>
 * A vector index takes a commit's new rows before the commit publishes their records, so for a moment a search can rank
 * a record it cannot read yet. The group-key resolver filed such a record under the null group, where it took one of
 * the index's group slots and kept a real group out of the index's answer. By the time the function emitted the rows
 * the record was readable, under its real group, so it was not counted as stale and the #9697 top-up never ran: the
 * answer came back with a group made of that one record and the members of a real group missing.
 * <p>
 * The shape is the reporter's, scaled down: two buckets, where insertion alternates buckets so each bucket holds only
 * half of the groups, and a writer that keeps replacing the records nearest a hot query with new ones of random groups
 * around it, while two readers ask grouped probes there. Every group always holds about a hundred live members, so
 * every answer must be full. Timing dependent: before the fix about one probe in 250 came back short, so a run of this
 * length failed every time; after it, none did.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue10002GroupedNeighborsChurnTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int GROUPS     = 40;
  private static final int RECORDS    = 4_000;
  private static final int K          = 5;
  private static final int GROUP_SIZE = 3;
  private static final int COMMITS    = 300;
  private static final int REPLACED   = 30;

  @Test
  void denseGroupedProbesStayFullUnderChurn() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Dense BUCKETS 2");
    database.command("sql", "CREATE PROPERTY Dense.emb ARRAY_OF_FLOATS");
    database.command("sql", "CREATE PROPERTY Dense.cat INTEGER");
    final Random random = new Random(41);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Dense").set("emb", randomVector(random), "cat", i % GROUPS).save();
    });
    database.command("sql",
        "CREATE INDEX ON Dense (emb) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\" }");

    final float[] hot = randomVector(new Random(99));
    final String grouped = "SELECT expand(`vector.neighbors`('Dense[emb]', ?, " + K + ", { efSearch: 200, groupBy: 'cat', groupSize: "
        + GROUP_SIZE + " }))";

    assertNeverShortUnderChurn(grouped, seed -> new Object[] { jitter(hot, new Random(seed)) }, writer -> {
      final List<RID> nearest = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(`vector.neighbors`('Dense[emb]', ?, " + REPLACED + ", { efSearch: 200 }))", (Object) hot)) {
        rs.forEachRemaining(r -> nearest.add(r.getProperty("@rid")));
      }
      database.transaction(() -> {
        for (final RID rid : nearest)
          database.lookupByRID(rid, false).asDocument().delete();
        for (int i = 0; i < nearest.size(); i++)
          database.newDocument("Dense").set("emb", jitter(hot, writer), "cat", writer.nextInt(GROUPS)).save();
      });
    });
  }

  @Test
  void sparseGroupedProbesStayFullUnderChurn() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Sparse BUCKETS 2");
    database.command("sql", "CREATE PROPERTY Sparse.cat INTEGER");
    database.command("sql", "CREATE PROPERTY Sparse.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Sparse (tokens, weights) LSM_SPARSE_VECTOR METADATA { \"dimensions\": 64 }");
    final Random random = new Random(43);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        newSparse(random, i % GROUPS);
    });

    final String grouped = "SELECT expand(`vector.sparseNeighbors`('Sparse[tokens,weights]', ?, ?, " + K + ", { groupBy: 'cat', groupSize: "
        + GROUP_SIZE + " }))";

    final List<RID> live = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Sparse")) {
      rs.forEachRemaining(r -> live.add(r.getProperty("rid")));
    }
    assertNeverShortUnderChurn(grouped, seed -> new Object[] { new int[] { 0, 1, 2 }, new float[] { 1f, 1f, 1f } }, writer -> {
      final List<RID> victims = new ArrayList<>();
      for (int i = 0; i < REPLACED; i++)
        victims.add(live.remove(writer.nextInt(live.size())));
      final List<RID> added = new ArrayList<>();
      database.transaction(() -> {
        for (final RID rid : victims)
          database.lookupByRID(rid, false).asDocument().delete();
        for (int i = 0; i < victims.size(); i++)
          added.add(newSparse(writer, writer.nextInt(GROUPS)));
      });
      live.addAll(added);
    });
  }

  @FunctionalInterface
  private interface ProbeArgs {
    Object[] of(long seed);
  }

  @FunctionalInterface
  private interface WriterRound {
    void run(Random writer);
  }

  private void assertNeverShortUnderChurn(final String grouped, final ProbeArgs probeArgs, final WriterRound round)
      throws Exception {
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong probes = new AtomicLong();
    final AtomicLong shortAnswers = new AtomicLong();
    final AtomicReference<Throwable> readerError = new AtomicReference<>();
    final List<Thread> readers = new ArrayList<>();
    for (int t = 0; t < 2; t++) {
      final long seedBase = 1_000L * (t + 1);
      final Thread reader = new Thread(() -> {
        try {
          long seed = seedBase;
          while (!stop.get()) {
            int rows = 0;
            try (final ResultSet rs = database.query("sql", grouped, probeArgs.of(seed++))) {
              while (rs.hasNext()) {
                final Result ignored = rs.next();
                rows++;
              }
            }
            if (rows < K * GROUP_SIZE)
              shortAnswers.incrementAndGet();
            probes.incrementAndGet();
          }
        } catch (final Throwable e) {
          readerError.set(e);
        }
      });
      reader.setDaemon(true);
      readers.add(reader);
    }
    readers.forEach(Thread::start);
    try {
      final Random writer = new Random(77);
      for (int i = 0; i < COMMITS; i++)
        round.run(writer);
    } finally {
      stop.set(true);
      for (final Thread reader : readers)
        reader.join();
    }

    assertThat(readerError.get()).isNull();
    assertThat(probes.get()).isPositive();
    assertThat(shortAnswers.get()).as("grouped probes answering fewer than %d rows out of %d", K * GROUP_SIZE, probes.get())
        .isZero();
  }

  private RID newSparse(final Random random, final int cat) {
    final int[] tokens = new int[] { random.nextInt(3), 3 + random.nextInt(61) };
    final float[] weights = new float[] { 0.2f + random.nextFloat(), 0.2f + random.nextFloat() };
    return database.newDocument("Sparse").set("cat", cat, "tokens", tokens, "weights", weights).save().getIdentity();
  }

  private static float[] randomVector(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = random.nextFloat();
    return v;
  }

  private static float[] jitter(final float[] center, final Random random) {
    final float[] v = center.clone();
    for (int i = 0; i < v.length; i++)
      v[i] += (random.nextFloat() - 0.5f) * 0.05f;
    return v;
  }
}
