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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.RID;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 3 verification: {@link Memtable} satisfies put/remove/iterate semantics under both
 * single-threaded and concurrent workloads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MemtableTest {

  @Test
  void putReadbackInRidOrder() {
    final Memtable m = new Memtable();
    m.put(0, new RID(0, 30), 0.3f);
    m.put(0, new RID(0, 10), 0.1f);
    m.put(0, new RID(0, 20), 0.2f);

    final List<MemtablePosting> got = drain(m.iterateDim(0));
    assertThat(got).hasSize(3);
    assertThat(got.get(0).rid()).isEqualTo(new RID(0, 10));
    assertThat(got.get(1).rid()).isEqualTo(new RID(0, 20));
    assertThat(got.get(2).rid()).isEqualTo(new RID(0, 30));
    assertThat(m.totalPostings()).isEqualTo(3L);
    assertThat(m.tombstoneCount()).isZero();
  }

  @Test
  void putOverridesPriorWeight() {
    final Memtable m = new Memtable();
    m.put(7, new RID(0, 100), 0.5f);
    m.put(7, new RID(0, 100), 0.9f);

    final List<MemtablePosting> got = drain(m.iterateDim(7));
    assertThat(got).hasSize(1);
    assertThat(got.getFirst().weight()).isEqualTo(0.9f);
    assertThat(m.totalPostings()).isEqualTo(1L);
  }

  @Test
  void removeStoresTombstoneAndPreservesRidOrder() {
    final Memtable m = new Memtable();
    m.put(0, new RID(0, 100), 0.5f);
    m.put(0, new RID(0, 200), 0.7f);
    m.remove(0, new RID(0, 150));   // tombstone for non-existent row
    m.remove(0, new RID(0, 100));   // tombstone replacing live entry

    final List<MemtablePosting> got = drain(m.iterateDim(0));
    assertThat(got).hasSize(3);
    assertThat(got.get(0).rid()).isEqualTo(new RID(0, 100));
    assertThat(got.get(0).tombstone()).isTrue();
    assertThat(got.get(1).rid()).isEqualTo(new RID(0, 150));
    assertThat(got.get(1).tombstone()).isTrue();
    assertThat(got.get(2).rid()).isEqualTo(new RID(0, 200));
    assertThat(got.get(2).tombstone()).isFalse();

    assertThat(m.totalPostings()).isEqualTo(3L);
    assertThat(m.tombstoneCount()).isEqualTo(2L);
  }

  @Test
  void putAfterTombstoneResurrects() {
    final Memtable m = new Memtable();
    m.put(0, new RID(0, 1), 0.5f);
    m.remove(0, new RID(0, 1));
    m.put(0, new RID(0, 1), 0.7f);

    final List<MemtablePosting> got = drain(m.iterateDim(0));
    assertThat(got).hasSize(1);
    assertThat(got.getFirst().tombstone()).isFalse();
    assertThat(got.getFirst().weight()).isEqualTo(0.7f);
    assertThat(m.tombstoneCount()).isZero();
  }

  @Test
  void sortedDimsAreReturnedAscending() {
    final Memtable m = new Memtable();
    m.put(5, new RID(0, 1), 0.1f);
    m.put(1, new RID(0, 1), 0.1f);
    m.put(20, new RID(0, 1), 0.1f);
    m.put(10, new RID(0, 1), 0.1f);

    assertThat(m.sortedDims()).containsExactly(1, 5, 10, 20);
  }

  @Test
  void clearResetsAllCounters() {
    final Memtable m = new Memtable();
    m.put(0, new RID(0, 1), 0.5f);
    m.remove(0, new RID(0, 2));
    assertThat(m.isEmpty()).isFalse();
    m.clear();
    assertThat(m.isEmpty()).isTrue();
    assertThat(m.totalPostings()).isZero();
    assertThat(m.tombstoneCount()).isZero();
    assertThat(m.dimCount()).isZero();
  }

  @Test
  void rejectsInvalidWeights() {
    final Memtable m = new Memtable();
    assertThatThrownBy(() -> m.put(0, new RID(0, 1), Float.NaN))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("finite");
    assertThatThrownBy(() -> m.put(0, new RID(0, 1), -0.1f))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
  }

  @Test
  void concurrentWritersDoNotLoseEntries() throws Exception {
    final Memtable m = new Memtable();
    final int writers = 8;
    final int perWriter = 5_000;
    final ExecutorService exec = Executors.newFixedThreadPool(writers);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicInteger errors = new AtomicInteger();
    try {
      for (int w = 0; w < writers; w++) {
        final int writerId = w;
        exec.submit(() -> {
          try {
            start.await();
            final Random rnd = new Random(writerId);
            for (int i = 0; i < perWriter; i++) {
              final int dim = rnd.nextInt(50);
              // Each writer uses a disjoint RID range so we don't lose entries to overwrite.
              final RID rid = new RID(writerId, (long) i);
              m.put(dim, rid, rnd.nextFloat());
            }
          } catch (final Throwable t) {
            errors.incrementAndGet();
          }
        });
      }
      start.countDown();
      exec.shutdown();
      assertThat(exec.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
    } finally {
      exec.shutdownNow();
    }
    assertThat(errors.get()).isZero();
    assertThat(m.totalPostings()).isEqualTo((long) writers * perWriter);

    // Every dim's postings must be sorted by RID ascending.
    int seenPostings = 0;
    for (final int dim : m.sortedDims()) {
      RID prev = null;
      final Iterator<MemtablePosting> it = m.iterateDim(dim);
      while (it.hasNext()) {
        final MemtablePosting p = it.next();
        if (prev != null)
          assertThat(SparseSegmentBuilder.compareRid(p.rid(), prev)).isPositive();
        prev = p.rid();
        seenPostings++;
      }
    }
    assertThat(seenPostings).isEqualTo(writers * perWriter);
  }

  // ---------- helpers ----------

  private static List<MemtablePosting> drain(final Iterator<MemtablePosting> it) {
    final List<MemtablePosting> out = new ArrayList<>();
    while (it.hasNext())
      out.add(it.next());
    return out;
  }
}
