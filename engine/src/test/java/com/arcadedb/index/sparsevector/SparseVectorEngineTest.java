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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 verification: end-to-end engine lifecycle. Exercises put / remove / topK / flush /
 * compact / reopen with the FP32-quantization variant so weight assertions can be exact.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseVectorEngineTest {

  private static final SegmentParameters TEST_PARAMS = SegmentParameters.builder()
      .weightQuantization(SegmentFormat.WeightQuantization.FP32)
      .build();

  @Test
  void writesAreVisibleInTopKBeforeFlush(@TempDir final Path tmp) throws IOException {
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      engine.put(0, new RID(0, 1), 0.5f);
      engine.put(0, new RID(0, 2), 0.7f);
      engine.put(0, new RID(0, 3), 0.3f);
      assertThat(engine.segmentCount()).isZero();

      final List<RidScore> got = engine.topK(new int[] { 0 }, new float[] { 1.0f }, 5);
      assertThat(got).hasSize(3);
      assertThat(got.get(0).rid()).isEqualTo(new RID(0, 2));
      assertThat(got.get(1).rid()).isEqualTo(new RID(0, 1));
      assertThat(got.get(2).rid()).isEqualTo(new RID(0, 3));
    }
  }

  @Test
  void flushPersistsToSegmentAndDrainsMemtable(@TempDir final Path tmp) throws IOException {
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      engine.put(0, new RID(0, 1), 0.5f);
      engine.put(0, new RID(0, 2), 0.7f);
      assertThat(engine.memtablePostings()).isEqualTo(2L);

      final long segmentId = engine.flush();
      assertThat(segmentId).isPositive();
      assertThat(engine.segmentCount()).isEqualTo(1);
      assertThat(engine.memtablePostings()).isZero();

      // Query still works after flush.
      final List<RidScore> got = engine.topK(new int[] { 0 }, new float[] { 1.0f }, 5);
      assertThat(got).hasSize(2);
    }
  }

  @Test
  void writesShadowedByMemtableAfterFlush(@TempDir final Path tmp) throws IOException {
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      engine.put(0, new RID(0, 1), 0.5f);
      engine.flush();
      // Same RID rewritten into the new memtable; merged cursor must take the memtable's value.
      engine.put(0, new RID(0, 1), 0.99f);
      final List<RidScore> got = engine.topK(new int[] { 0 }, new float[] { 1.0f }, 5);
      assertThat(got).hasSize(1);
      assertThat(got.getFirst().score()).isEqualTo(0.99f);
    }
  }

  @Test
  void tombstoneInMemtableMasksFlushedPosting(@TempDir final Path tmp) throws IOException {
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      engine.put(0, new RID(0, 1), 0.5f);
      engine.put(0, new RID(0, 2), 0.7f);
      engine.flush();
      engine.remove(0, new RID(0, 1));

      final List<RidScore> got = engine.topK(new int[] { 0 }, new float[] { 1.0f }, 5);
      assertThat(got).hasSize(1);
      assertThat(got.getFirst().rid()).isEqualTo(new RID(0, 2));
    }
  }

  @Test
  void multipleFlushesProduceMultipleSegments(@TempDir final Path tmp) throws IOException {
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      engine.put(0, new RID(0, 1), 0.1f);
      engine.flush();
      engine.put(0, new RID(0, 2), 0.2f);
      engine.flush();
      engine.put(0, new RID(0, 3), 0.3f);
      engine.flush();
      assertThat(engine.segmentCount()).isEqualTo(3);

      final List<RidScore> got = engine.topK(new int[] { 0 }, new float[] { 1.0f }, 5);
      assertThat(got).hasSize(3);
      assertThat(got.get(0).rid()).isEqualTo(new RID(0, 3));
    }
  }

  @Test
  void compactAllMergesIntoOneSegment(@TempDir final Path tmp) throws IOException {
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      engine.put(0, new RID(0, 1), 0.1f);
      engine.flush();
      engine.put(0, new RID(0, 2), 0.2f);
      engine.flush();
      engine.put(0, new RID(0, 3), 0.3f);
      engine.flush();
      assertThat(engine.segmentCount()).isEqualTo(3);

      final long compactedId = engine.compactAll();
      assertThat(compactedId).isPositive();
      assertThat(engine.segmentCount()).isEqualTo(1);

      final List<RidScore> got = engine.topK(new int[] { 0 }, new float[] { 1.0f }, 5);
      assertThat(got).hasSize(3);
    }
  }

  @Test
  void reopenLoadsExistingSegments(@TempDir final Path tmp) throws IOException {
    long firstSegmentId;
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      engine.put(0, new RID(0, 1), 0.1f);
      engine.put(1, new RID(0, 2), 0.2f);
      firstSegmentId = engine.flush();
    }

    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      assertThat(engine.segmentCount()).isEqualTo(1);
      assertThat(engine.segmentIds()).containsExactly(firstSegmentId);

      final List<RidScore> got = engine.topK(new int[] { 0, 1 }, new float[] { 1.0f, 1.0f }, 5);
      assertThat(got).hasSize(2);

      // New segment id must be strictly greater than any existing id.
      engine.put(2, new RID(0, 3), 0.3f);
      final long newId = engine.flush();
      assertThat(newId).isGreaterThan(firstSegmentId);
    }
  }

  @Test
  void closeFlushesPendingMemtable(@TempDir final Path tmp) throws IOException {
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      engine.put(0, new RID(0, 1), 0.5f);
    }
    // Reopen: the close above should have flushed the memtable.
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      assertThat(engine.segmentCount()).isEqualTo(1);
      final List<RidScore> got = engine.topK(new int[] { 0 }, new float[] { 1.0f }, 5);
      assertThat(got).hasSize(1);
    }
  }

  @Test
  void compactOldestRetainsRecentSegments(@TempDir final Path tmp) throws IOException {
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      for (int i = 1; i <= 5; i++) {
        engine.put(0, new RID(0, i), 0.1f * i);
        engine.flush();
      }
      assertThat(engine.segmentCount()).isEqualTo(5);

      // Compact the 3 oldest into 1; expect 1 (merged) + 2 (untouched) = 3 segments.
      final long mergedId = engine.compactOldest(3);
      assertThat(mergedId).isPositive();
      assertThat(engine.segmentCount()).isEqualTo(3);

      final List<RidScore> got = engine.topK(new int[] { 0 }, new float[] { 1.0f }, 10);
      assertThat(got).hasSize(5);
    }
  }

  @Test
  void largeRoundtripAfterFlushAndCompactionMatchesInput(@TempDir final Path tmp) throws IOException {
    final Set<RID> expected = new HashSet<>();
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      // 5 batches of 200 docs, flushed individually -> 5 segments
      for (int batch = 0; batch < 5; batch++) {
        for (int i = 0; i < 200; i++) {
          final RID rid = new RID(0, batch * 1000L + i);
          engine.put(0, rid, 0.1f + (i % 50) / 100.0f);
          engine.put(1, rid, 0.2f);
          expected.add(rid);
        }
        engine.flush();
      }
      assertThat(engine.segmentCount()).isEqualTo(5);
      engine.compactAll();
      assertThat(engine.segmentCount()).isEqualTo(1);

      final List<RidScore> got = engine.topK(new int[] { 0, 1 }, new float[] { 1.0f, 1.0f }, expected.size());
      final Set<RID> seen = new HashSet<>();
      for (final RidScore r : got)
        seen.add(r.rid());
      assertThat(seen).isEqualTo(expected);
    }
  }
}
