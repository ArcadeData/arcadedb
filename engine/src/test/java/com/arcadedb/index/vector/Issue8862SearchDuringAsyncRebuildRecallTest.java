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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #8862: a search that runs while the index rebuilds its graph in the background returned poor
 * neighbors (recall@10 0.14-0.31 against 0.99 before and after). The rebuild published the NEW ordinal-to-vector-id
 * map long before the new graph, so a search walking the still-published OLD graph resolved its ordinals through a
 * map that was compacted for the new one, and scored every node against some other record's vector.
 * <p>
 * The test deletes 20% of the vectors (which crosses the rebuild threshold), samples recall@10 against an exact
 * cosine ground truth for as long as the rebuild runs, and requires every sample to stay as good as before it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue8862SearchDuringAsyncRebuildRecallTest {
  private static final String DB_PATH    = "target/test-databases/Issue8862SearchDuringAsyncRebuildRecallTest";
  private static final int    DIMENSIONS = 32;
  private static final int    COUNT      = 20_000;
  private static final int    DELETES    = 4_000;
  private static final int    K          = 10;
  private static final int    EF_SEARCH  = 100;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void recallHoldsWhileTheAsyncRebuildRuns() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        final Random random = new Random(5);
        final Map<RID, float[]> live = new HashMap<>();
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        });
        final List<RID> rids = new ArrayList<>(COUNT);
        db.begin();
        for (int i = 0; i < COUNT; i++) {
          final float[] v = randomVector(random);
          final RID rid = db.newDocument("Doc").set("id", i).set("vector", v).save().getIdentity();
          rids.add(rid);
          live.put(rid, v);
          if (i % 5000 == 4999) {
            db.commit();
            db.begin();
          }
        }
        db.commit();
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\" }");
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc")
            .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];

        final double before = recall(index, live, new Random(11), 20);
        assertThat(before).as("precondition: recall before the deletes").isGreaterThan(0.9);

        final long rebuildsBefore = (Long) index.getStats().get("graphRebuildCount");
        final List<RID> shuffled = new ArrayList<>(rids);
        Collections.shuffle(shuffled, new Random(13));
        db.begin();
        for (final RID rid : shuffled.subList(0, DELETES)) {
          db.deleteRecord(db.lookupByRID(rid, false));
          live.remove(rid);
        }
        db.commit();

        // Sample unconditionally until the rebuild has finished: every sample, before or during the rebuild, must hold
        final Random sampler = new Random(31);
        double worst = 1.0;
        int samples = 0;
        final long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        boolean done = false;
        while (!done && System.nanoTime() < deadline) {
          worst = Math.min(worst, recall(index, live, sampler, 3));
          samples++;
          done = (Long) index.getStats().get("graphRebuildCount") > rebuildsBefore
              && (Long) index.getStats().get("asyncRebuildInProgress") == 0L;
        }

        assertThat(samples).as("sampled at least once").isPositive();
        assertThat(worst).as("worst recall@10 sampled during the async rebuild (before: %s)", before)
            .isGreaterThan(0.9);

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress")).isEqualTo(0L));
      } finally {
        if (db.isOpen())
          db.drop();
      }
    }
  }

  private static double recall(final LSMVectorIndex index, final Map<RID, float[]> live, final Random random,
      final int queries) {
    double sum = 0;
    for (int q = 0; q < queries; q++) {
      final float[] query = randomVector(random);
      final List<Pair<RID, Float>> got = index.findNeighborsFromVector(query, K, EF_SEARCH);
      final RID[] order = live.keySet().toArray(new RID[0]);
      final double[] sims = new double[order.length];
      final Integer[] idx = new Integer[order.length];
      for (int i = 0; i < order.length; i++) {
        sims[i] = cosine(query, live.get(order[i]));
        idx[i] = i;
      }
      Arrays.sort(idx, (a, b) -> Double.compare(sims[b], sims[a]));
      final Set<RID> truth = new HashSet<>();
      for (int i = 0; i < K; i++)
        truth.add(order[idx[i]]);
      int hit = 0;
      for (final Pair<RID, Float> p : got)
        if (truth.contains(p.getFirst()))
          hit++;
      sum += hit / (double) K;
    }
    return sum / queries;
  }

  private static float[] randomVector(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = random.nextFloat() * 2 - 1;
    return v;
  }

  private static double cosine(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    return dot / Math.sqrt(na * nb);
  }
}
