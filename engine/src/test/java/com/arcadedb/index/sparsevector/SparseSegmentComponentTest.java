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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Step 1 verification for the page-component-backed sparse segment format. Builds a sealed
 * segment via {@link SparseSegmentBuilder} inside a real ArcadeDB transaction, then re-opens it
 * through {@link PaginatedSegmentReader} and checks that the posting set roundtrips correctly.
 * Confirms the page WAL captures every byte of the new segment so durability flows through
 * ArcadeDB's normal commit pipeline.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseSegmentComponentTest extends TestHelper {

  /** FP32 quantization so weight equality is exact for the basic correctness test. */
  private static final SegmentParameters EXACT_PARAMS = SegmentParameters.builder()
      .weightQuantization(WeightQuantization.FP32)
      .build();

  @Test
  void buildAndReadSingleDimRoundtrip() throws Exception {
    final List<RID>   rids = ridSequence(0, 0, 64);
    final float[] weights = randomWeights(rids.size(), 0xCAFEL);

    final AtomicReference<SparseSegmentComponent> componentHolder = new AtomicReference<>();

    inTx(() -> {
      final SparseSegmentComponent c = newComponent("seg-rt-1");
      componentHolder.set(c);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, EXACT_PARAMS)) {
        b.setSegmentId(11L);
        b.startDim(7);
        for (int i = 0; i < rids.size(); i++)
          b.appendPosting(rids.get(i), weights[i]);
        b.endDim();
        b.finish();
      }
    });

    inTx(() -> {
      final PaginatedSegmentReader r = new PaginatedSegmentReader(componentHolder.get());
      assertThat(r.totalDims()).isEqualTo(1);
      assertThat(r.totalPostings()).isEqualTo(rids.size());
      assertThat(r.segmentId()).isEqualTo(11L);
      assertThat(r.hasDim(7)).isTrue();
      assertThat(r.hasDim(8)).isFalse();

      try (final PaginatedSegmentDimCursor c = r.openCursor(7)) {
        for (int i = 0; i < rids.size(); i++) {
          assertThat(c.advance()).isTrue();
          assertThat(c.currentRid()).isEqualTo(rids.get(i));
          assertThat(c.isTombstone()).isFalse();
          assertThat(c.currentWeight()).isEqualTo(weights[i]);
        }
        assertThat(c.advance()).isFalse();
      }
    });
  }

  @Test
  void multipleDimsAndTombstonesRoundtrip() throws Exception {
    final TreeMap<Integer, TreeMap<RID, Float>> truth = new TreeMap<>();
    truth.put(0, new TreeMap<>());
    truth.get(0).put(new RID(0, 100), 0.5f);
    truth.get(0).put(new RID(0, 200), 0.7f);
    truth.put(2, new TreeMap<>());
    truth.get(2).put(new RID(0, 5),  0.1f);
    truth.get(2).put(new RID(0, 15), 0.2f);
    truth.get(2).put(new RID(0, 25), 0.3f);

    final AtomicReference<SparseSegmentComponent> componentHolder = new AtomicReference<>();

    inTx(() -> {
      final SparseSegmentComponent c = newComponent("seg-rt-2");
      componentHolder.set(c);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, EXACT_PARAMS)) {
        b.setSegmentId(22L);
        b.startDim(0);
        b.appendPosting(new RID(0, 100), 0.5f);
        b.appendTombstone(new RID(0, 150));
        b.appendPosting(new RID(0, 200), 0.7f);
        b.endDim();
        b.startDim(2);
        for (final var e : truth.get(2).entrySet())
          b.appendPosting(e.getKey(), e.getValue());
        b.endDim();
        b.finish();
      }
    });

    inTx(() -> {
      final PaginatedSegmentReader r = new PaginatedSegmentReader(componentHolder.get());
      assertThat(r.totalDims()).isEqualTo(2);
      try (final PaginatedSegmentDimCursor c = r.openCursor(0)) {
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 100));
        assertThat(c.isTombstone()).isFalse();
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 150));
        assertThat(c.isTombstone()).isTrue();
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 200));
        assertThat(c.isTombstone()).isFalse();
        assertThat(c.advance()).isFalse();
      }
      try (final PaginatedSegmentDimCursor c = r.openCursor(2)) {
        for (final var e : truth.get(2).entrySet()) {
          assertThat(c.advance()).isTrue();
          assertThat(c.currentRid()).isEqualTo(e.getKey());
          assertThat(c.currentWeight()).isEqualTo(e.getValue());
        }
        assertThat(c.advance()).isFalse();
      }
    });
  }

  @Test
  void multiBlockDimAcrossPagesRoundtrip() throws Exception {
    // Force tiny blocks so we span many of them; verifies block locators / skip list addressing.
    final SegmentParameters tinyBlocks = SegmentParameters.builder()
        .weightQuantization(WeightQuantization.FP32)
        .blockSize(16)
        .skipStride(4)
        .build();

    final List<RID> rids = ridSequence(0, 0, 500);
    final float[] weights = randomWeights(rids.size(), 0xBEEFL);
    final AtomicReference<SparseSegmentComponent> componentHolder = new AtomicReference<>();

    inTx(() -> {
      final SparseSegmentComponent c = newComponent("seg-rt-3");
      componentHolder.set(c);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, tinyBlocks)) {
        b.setSegmentId(33L);
        b.startDim(42);
        for (int i = 0; i < rids.size(); i++)
          b.appendPosting(rids.get(i), weights[i]);
        b.endDim();
        b.finish();
      }
    });

    inTx(() -> {
      final PaginatedSegmentReader r = new PaginatedSegmentReader(componentHolder.get());
      final PaginatedDimMetadata md = r.dimMetadata(42);
      assertThat(md).isNotNull();
      assertThat(md.blockCount()).isEqualTo(32);              // 500 / 16 = 31.25 -> 32
      assertThat(md.skipList().length).isEqualTo(8);          // 32 / 4 = 8
      try (final PaginatedSegmentDimCursor c = r.openCursor(42)) {
        for (int i = 0; i < rids.size(); i++) {
          assertThat(c.advance()).isTrue();
          assertThat(c.currentRid()).isEqualTo(rids.get(i));
          assertThat(c.currentWeight()).isEqualTo(weights[i]);
        }
        assertThat(c.advance()).isFalse();
      }
    });
  }

  @Test
  void seekToCrossesBlockBoundariesCorrectly() throws Exception {
    final SegmentParameters tinyBlocks = SegmentParameters.builder()
        .weightQuantization(WeightQuantization.FP32)
        .blockSize(16)
        .skipStride(2)
        .build();
    final List<RID> rids = ridSequence(0, 0, 200);
    final AtomicReference<SparseSegmentComponent> componentHolder = new AtomicReference<>();

    inTx(() -> {
      final SparseSegmentComponent c = newComponent("seg-seek");
      componentHolder.set(c);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, tinyBlocks)) {
        b.setSegmentId(44L);
        b.startDim(0);
        for (final RID rid : rids)
          b.appendPosting(rid, 0.5f);
        b.endDim();
        b.finish();
      }
    });

    inTx(() -> {
      final PaginatedSegmentReader r = new PaginatedSegmentReader(componentHolder.get());
      try (final PaginatedSegmentDimCursor c = r.openCursor(0)) {
        // Forward-only seeks: each strictly past the previous landing point.
        assertThat(c.seekTo(rids.get(50))).isTrue();
        assertThat(c.currentRid()).isEqualTo(rids.get(50));
        assertThat(c.seekTo(rids.get(100))).isTrue();
        assertThat(c.currentRid()).isEqualTo(rids.get(100));
        assertThat(c.seekTo(rids.get(180))).isTrue();
        assertThat(c.currentRid()).isEqualTo(rids.get(180));

        // Past the last RID: cursor exhausts.
        assertThat(c.seekTo(new RID(99, 0))).isFalse();
      }
    });
  }

  @Test
  void persistsAcrossDatabaseReopen() throws Exception {
    // Build a segment, close the database, reopen, and verify the component's pages survived
    // through ArcadeDB's normal page-WAL recovery without any sparse-vector-specific code.
    final List<RID> rids = ridSequence(0, 1000, 32);
    final float[] weights = randomWeights(rids.size(), 0xFEEDL);

    final long expectedSegmentId = 99L;
    final String componentName = "seg-persist";

    inTx(() -> {
      final SparseSegmentComponent c = newComponent(componentName);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, EXACT_PARAMS)) {
        b.setSegmentId(expectedSegmentId);
        b.startDim(0);
        for (int i = 0; i < rids.size(); i++)
          b.appendPosting(rids.get(i), weights[i]);
        b.endDim();
        b.finish();
      }
    });

    database.close();
    database = factory.open();

    inTx(() -> {
      final SparseSegmentComponent reopened = lookupComponent(componentName);
      assertThat(reopened).as("component should survive reopen").isNotNull();
      final PaginatedSegmentReader r = new PaginatedSegmentReader(reopened);
      assertThat(r.segmentId()).isEqualTo(expectedSegmentId);
      assertThat(r.totalPostings()).isEqualTo(rids.size());
      try (final PaginatedSegmentDimCursor c = r.openCursor(0)) {
        for (int i = 0; i < rids.size(); i++) {
          assertThat(c.advance()).isTrue();
          assertThat(c.currentRid()).isEqualTo(rids.get(i));
          assertThat(c.currentWeight()).isEqualTo(weights[i]);
        }
      }
    });
  }

  // --- helpers --------------------------------------------------------------

  /** Wraps a checked-throwing block so it can run inside a {@code TransactionScope.execute()}. */
  @FunctionalInterface
  private interface CheckedRunnable {
    void run() throws Exception;
  }

  private void inTx(final CheckedRunnable r) {
    database.transaction(() -> {
      try {
        r.run();
      } catch (final RuntimeException e) {
        throw e;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private SparseSegmentComponent newComponent(final String name) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final String filePath = db.getDatabasePath() + "/" + name;
    try {
      final SparseSegmentComponent c = new SparseSegmentComponent(db, name, filePath, ComponentFile.MODE.READ_WRITE,
          SparseSegmentComponent.DEFAULT_PAGE_SIZE);
      ((com.arcadedb.schema.LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
      return c;
    } catch (final java.io.IOException e) {
      throw new RuntimeException("failed to create sparse segment component '" + name + "'", e);
    }
  }

  private SparseSegmentComponent lookupComponent(final String name) {
    final DatabaseInternal db = (DatabaseInternal) database;
    return (SparseSegmentComponent) ((com.arcadedb.schema.LocalSchema) db.getSchema().getEmbedded()).getFileByName(name);
  }

  private static List<RID> ridSequence(final int bucket, final long startOffset, final int n) {
    final List<RID> out = new ArrayList<>(n);
    long pos = startOffset;
    for (int i = 0; i < n; i++) {
      out.add(new RID(bucket, pos));
      pos += 1 + (i % 7);
    }
    return out;
  }

  private static float[] randomWeights(final int n, final long seed) {
    final Random rnd = new Random(seed);
    final float[] out = new float[n];
    for (int i = 0; i < n; i++)
      out[i] = rnd.nextFloat();
    return out;
  }
}
