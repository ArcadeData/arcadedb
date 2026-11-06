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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Plan 3 Task 23 load-bearing item (final review A): vertices with an EXTERNAL body and edges, mutated while a rebuild of their projection
 * scans and while it is held once its scan is done, with apply passes forced on the test thread meanwhile. The first rounds are the create
 * rebuilds of the five COLUMNAR indexes over existing data, the others {@code check_fix} rebuilds; after every round the projection must
 * equal the GROUP BY of every indexed property.
 */
@Tag("slow")
class ColumnarDivergenceStressTest extends TestHelper {
  private static final List<String> PROPERTIES = List.of("brand", "tags", "fileSize", "publishDate", "author");
  private static final int          ROUNDS     = 30;
  private static final int          BATCH      = 150;
  private static final long         WAIT_MS    = 60_000;

  private final ExecutorService writer = Executors.newSingleThreadExecutor();

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
    writer.shutdownNow();
  }

  @Test
  void rebuildsAndPassesRacingWritesKeepEquivalence() throws Exception {
    // a new seed per run, reported by a failure: -Dcolumnar.stress.seed replays it
    final long seed = Long.getLong("columnar.stress.seed", System.nanoTime());
    final ColumnarTestData data = new ColumnarTestData(seed);
    data.asVertices = true;
    data.edgesPerVertex = 2;
    data.externalBody = true;
    data.deleteRatio = 0.05;
    data.columnarIndexes = false;
    data.createSchema(database);
    data.insertAssets(database, 300);

    final List<String> indexed = new ArrayList<>();
    for (int round = 0; round < ROUNDS; round++) {
      final CountDownLatch scanned = new CountDownLatch(1);
      final CountDownLatch releaseScan = new CountDownLatch(1);
      final AtomicBoolean fired = new AtomicBoolean();
      ColumnarTestHooks.killPoint = name -> {
        if (name.equals(ColumnarRebuilder.SCANNED) && fired.compareAndSet(false, true)) {
          scanned.countDown();
          try {
            releaseScan.await(WAIT_MS, TimeUnit.MILLISECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      };
      try {
        if (round < PROPERTIES.size()) {
          final String property = PROPERTIES.get(round);
          database.command("sql", "CREATE INDEX ON " + ColumnarTestData.TYPE + " (" + (property.equals("tags") ? "tags BY ITEM" : property)
              + ") COLUMNAR");
          indexed.add(property);
        } else
          ColumnarRebuilder.schedule(manager(), projection(), ColumnarRebuilder.CHECK_FIX);

        // one batch while the scan may run, the next one once it is held after the scan
        final Future<?> writes = writer.submit(() -> {
          data.mutate(database, BATCH);
          assertThat(scanned.await(WAIT_MS, TimeUnit.MILLISECONDS)).as("round's rebuild reached its scan end").isTrue();
          data.mutate(database, BATCH);
          return null;
        });
        while (!writes.isDone())
          ColumnarApplier.runPass(manager(), projection());
        writes.get();
      } finally {
        releaseScan.countDown();
      }
      try {
        FacetOracle.awaitCaughtUp(database);
        for (final String property : indexed)
          FacetOracle.assertEquivalent(database, ColumnarTestData.TYPE, property, null);
      } catch (final AssertionError e) {
        throw new AssertionError("Divergence in round " + round + " of seed " + seed, e);
      }
      ColumnarTestHooks.killPoint = null;
    }
    assertThat(indexed).containsExactlyElementsOf(PROPERTIES);
  }

  private ColumnarProjection projection() {
    return manager().projection(database.getSchema().getType(ColumnarTestData.TYPE).getBuckets(false).stream().mapToInt(Bucket::getFileId)
        .findFirst().orElseThrow());
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }
}
