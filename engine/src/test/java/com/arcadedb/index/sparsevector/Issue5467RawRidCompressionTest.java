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
import com.arcadedb.engine.MutablePage;
import com.arcadedb.index.sparsevector.SegmentFormat.RidCompression;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.schema.LocalSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RidCompression#RAW}, which had no test anywhere in the repository.
 * <p>
 * It is not a dead branch: the compression mode is read straight off the segment header, and
 * {@link SegmentParameters.Builder#ridCompression} is public, so a segment written with it decodes
 * through {@link PaginatedSegmentDimCursor} like any other. It happens not to be what
 * {@link LSMSparseVectorIndex} selects, which is presumably why it went uncovered - and why issue
 * #5467 could rewrite its decode branch from relative to absolute page reads, and add two corruption
 * guards to it, with nothing to catch a mistake.
 * <p>
 * This gives the RAW path the same treatment the varint path has: a round trip that pins what the
 * cursor returns, a seek walk, and the corruption guards exercised by mutating real on-disk bytes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5467RawRidCompressionTest extends TestHelper {

  private static final SegmentParameters RAW_PARAMS = SegmentParameters.builder()
      .weightQuantization(WeightQuantization.FP32)
      .ridCompression(RidCompression.RAW)
      .blockSize(SegmentFormat.MIN_BLOCK_SIZE)
      .build();

  private static final int POSTINGS = 500;

  /**
   * Every posting the builder wrote comes back, in order, with its weight - across many blocks, so
   * the block-to-block transitions are covered rather than just the first decode.
   */
  @Test
  void aRawSegmentRoundTripsEveryPosting() throws Exception {
    final List<RID> rids = new ArrayList<>();
    final List<Float> weights = new ArrayList<>();
    for (int i = 0; i < POSTINGS; i++) {
      // Two buckets, so the decoder's bucket component is exercised and not just the position.
      rids.add(new RID(i < POSTINGS / 2 ? 0 : 3, 1L + i));
      weights.add(0.125f + (i % 11) * 0.0625f);
    }

    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildSegment("seg-5467-raw-roundtrip", rids, weights)));

    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      assertThat(reader.parameters().ridCompression())
          .as("the segment must really be RAW, or this test is covering the varint path again")
          .isEqualTo(RidCompression.RAW);

      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        assertThat(c.metadata().blockCount()).as("the walk must span many blocks").isGreaterThan(20);
        c.start();
        for (int i = 0; i < POSTINGS; i++) {
          assertThat(c.currentRid()).as("posting %d", i).isEqualTo(rids.get(i));
          assertThat(c.currentWeight()).as("weight of posting %d", i).isEqualTo(weights.get(i));
          assertThat(c.isTombstone()).isFalse();
          if (i < POSTINGS - 1)
            assertThat(c.advance()).as("advance off posting %d", i).isTrue();
        }
        assertThat(c.advance()).isFalse();
        assertThat(c.isExhausted()).isTrue();
      }
    });
  }

  /**
   * Seeking lands on the first posting at or after the target, including targets that fall in the
   * gaps the fixture leaves and targets in the second bucket. A seek is the operation the
   * non-essential probe of a MaxScore traversal runs, and on the RAW path it scans the decoded
   * positions rather than replaying deltas.
   */
  @Test
  void seekingIntoARawSegmentLandsOnTheFirstPostingAtOrAfterTheTarget() throws Exception {
    final List<RID> rids = new ArrayList<>();
    final List<Float> weights = new ArrayList<>();
    for (int i = 0; i < POSTINGS; i++) {
      rids.add(new RID(i < POSTINGS / 2 ? 0 : 3, 10L * (i + 1)));  // gaps of 10 between positions
      weights.add(0.5f);
    }

    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildSegment("seg-5467-raw-seek", rids, weights)));

    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      for (int i = 0; i < POSTINGS; i += 37) {
        final RID target = rids.get(i);
        try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
          c.start();
          // A position between two postings must land on the later one, never before the target.
          assertThat(c.seekTo(target.getBucketId(), target.getPosition() - 3)).isTrue();
          assertThat(c.currentRid()).as("seek to just before posting %d", i).isEqualTo(target);
          // Seeking to exactly where we already are must not move.
          assertThat(c.seekTo(target.getBucketId(), target.getPosition())).isTrue();
          assertThat(c.currentRid()).isEqualTo(target);
        }
      }

      // Past the end: the cursor exhausts rather than reporting a stale position.
      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        c.start();
        assertThat(c.seekTo(99, 1L)).isFalse();
        assertThat(c.isExhausted()).isTrue();
        assertThat(c.currentRid()).isNull();
      }
    });
  }

  /**
   * The RAW branch got the same strictly-ascending guard as the varint branch in issue #5467, and it
   * is the branch where nothing else would catch a scrambled payload: absolute RIDs decode to
   * whatever bytes are there, with no delta arithmetic to overflow first.
   */
  @Test
  void aNonAscendingRawPayloadReadsAsACorruptSegment() throws Exception {
    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    final int[] blockLocator = new int[2];
    inTx(() -> component.set(buildAscendingSegment("seg-5467-raw-order")));

    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        blockLocator[0] = c.metadata().blockPageNum(1);
        blockLocator[1] = c.metadata().blockOffset(1);
      }
    });

    inTx(() -> {
      // The block header holds posting 0; the payload starts at posting 1, as (int bucket, long
      // position). Rewriting that position to 0 puts it at or before the block's first RID.
      final MutablePage page = component.get().modifyPage(blockLocator[0]);
      final int payloadStart = blockLocator[1] + SegmentFormat.BLOCK_HEADER_SIZE;
      page.writeInt(payloadStart, 0);
      page.writeLong(payloadStart + 4, 0L);
    });

    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        assertThatThrownBy(() -> {
          c.start();
          while (c.advance())
            ;
        }).isInstanceOf(IOException.class)
            .hasMessageContaining("segment is corrupt")
            .hasMessageContaining("not in strictly ascending RID order");
      }
    });
  }

  /** The posting-count guard is shared, but it had never run against a RAW block. */
  @Test
  void aRawBlockClaimingTooManyPostingsReadsAsACorruptSegment() throws Exception {
    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    final int[] blockLocator = new int[2];
    inTx(() -> component.set(buildAscendingSegment("seg-5467-raw-count")));

    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        blockLocator[0] = c.metadata().blockPageNum(1);
        blockLocator[1] = c.metadata().blockOffset(1);
      }
    });

    inTx(() -> {
      final MutablePage page = component.get().modifyPage(blockLocator[0]);
      page.writeShort(blockLocator[1] + 2 * SegmentFormat.RID_SIZE_BYTES, (short) (RAW_PARAMS.blockSize() + 1));
    });

    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        assertThatThrownBy(() -> {
          c.start();
          while (c.advance())
            ;
        }).isInstanceOf(IOException.class)
            .hasMessageContaining("segment is corrupt")
            .hasMessageContaining("is not in 1..");
      }
    });
  }

  // ---------- helpers ----------

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

  private SparseSegmentComponent buildAscendingSegment(final String name) throws IOException {
    final List<RID> rids = new ArrayList<>();
    final List<Float> weights = new ArrayList<>();
    for (int i = 0; i < POSTINGS; i++) {
      rids.add(new RID(0, 1L + i));
      weights.add(0.5f);
    }
    return buildSegment(name, rids, weights);
  }

  private SparseSegmentComponent buildSegment(final String name, final List<RID> rids, final List<Float> weights)
      throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
        ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
    ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);

    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, RAW_PARAMS)) {
      b.setSegmentId(5467L);
      b.startDim(0);
      for (int i = 0; i < rids.size(); i++)
        b.appendPosting(rids.get(i), weights.get(i));
      b.endDim();
      b.finish();
    }
    return c;
  }
}
