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
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9396: a BINARY quantized index answered an EMPTY list while another thread committed transactions that
 * deleted the records nearest to the query and inserted new ones near it. The search fetches an oversampled
 * candidate list and reranks it on the stored vectors, dropping the candidates whose record is gone: a commit landing
 * between the two steps deleted exactly those candidates, so every one of them was dropped although 3,000 records
 * were live at every instant.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9396BinaryRerankEmptyUnderCommitsTest extends TestHelper {
  private static final int DIM = 32;

  @Test
  void searchNeverAnswersFewerThanKWhileTheNearestRecordsAreReplaced() throws Exception {
    final Random r = new Random(500);
    database.command("sql", "CREATE DOCUMENT TYPE Product BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Product.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Product.embedding ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    final Map<Long, float[]> live = new HashMap<>();
    database.begin();
    for (int i = 0; i < 3000; i++) {
      final float[] v = unit(r);
      database.newDocument("Product").set("pid", (long) i, "embedding", v).save();
      live.put((long) i, v);
    }
    database.commit();
    database.command("sql", "CREATE INDEX ON Product (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIM
        + ", \"similarity\": \"EUCLIDEAN\", \"beamWidth\": 100, \"quantization\": \"BINARY\" }");

    final LSMVectorIndex lsm = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Product[embedding]")).getIndexesOnBuckets()[0];
    final float[] q = unit(new Random(4242));
    final int[] ks = { 1, 5, 10 };

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong searches = new AtomicLong();
    final AtomicLong shortAnswers = new AtomicLong();
    final Thread reader = new Thread(() -> {
      int step = 0;
      while (!stop.get()) {
        final int k = ks[step++ % ks.length];
        if (lsm.findNeighborsFromVector(q, k, 100).size() < k)
          shortAnswers.incrementAndGet();
        searches.incrementAndGet();
      }
    });
    reader.setDaemon(true);
    reader.start();
    try {
      long nextPid = 100000;
      for (int round = 0; round < 40; round++) {
        final List<Long> pids = new ArrayList<>(live.keySet());
        pids.sort(Comparator.comparingDouble(p -> dist(live.get(p), q)));
        final List<Long> victims = new ArrayList<>(pids.subList(0, 100));
        database.begin();
        database.command("sql", "DELETE FROM Product WHERE pid IN :ids", Map.of("ids", victims)).close();
        for (final long p : victims)
          live.remove(p);
        for (int i = 0; i < 100; i++) {
          final float[] v = near(q, r);
          database.newDocument("Product").set("pid", nextPid, "embedding", v).save();
          live.put(nextPid++, v);
        }
        database.commit();
      }
    } finally {
      stop.set(true);
      reader.join();
    }

    assertThat(searches.get()).isPositive();
    assertThat(shortAnswers.get()).as("searches with fewer than k RIDs out of " + searches.get()).isZero();
  }

  private static float[] unit(final Random r) {
    final float[] v = new float[DIM];
    double s = 0;
    for (int i = 0; i < DIM; i++) {
      v[i] = (float) r.nextGaussian();
      s += v[i] * v[i];
    }
    s = Math.sqrt(s);
    for (int i = 0; i < DIM; i++)
      v[i] /= (float) s;
    return v;
  }

  private static float[] near(final float[] q, final Random r) {
    final float[] v = new float[DIM];
    double s = 0;
    for (int i = 0; i < DIM; i++) {
      v[i] = (float) (q[i] + 0.05 * r.nextGaussian());
      s += v[i] * v[i];
    }
    s = Math.sqrt(s);
    for (int i = 0; i < DIM; i++)
      v[i] /= (float) s;
    return v;
  }

  private static double dist(final float[] a, final float[] b) {
    double s = 0;
    for (int i = 0; i < DIM; i++)
      s += (a[i] - b[i]) * (a[i] - b[i]);
    return s;
  }
}
