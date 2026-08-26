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
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.schema.LocalSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two guards issue #5467 introduced alongside the in-place block decode, pinned as tests rather
 * than as prose.
 * <p>
 * <b>Corrupt block headers.</b> Decoding used to copy a bounded slice of the page into a scratch
 * buffer, so a header describing more postings than the block really holds ran out of that slice and
 * surfaced as a {@code BufferUnderflowException} from the decode loop. Reading the page in
 * place has no slice to run out of, so the decode carries its own bounds checks and reports a corrupt
 * segment by name. The header is untrusted input in the same sense as issue #6566's segment surface:
 * it is whatever is on disk.
 * <p>
 * <b>The block-bounds memo contract.</b> {@link DimCursor#lastProbedBlockEnd} reads the memo without
 * re-validating that it covers the probe, which is only safe because the block-max skip calls it
 * immediately after {@link DimCursor#blockMaxAt} for the same probe. That is enforced by a Java
 * {@code assert}, so the guard exists only while assertions are enabled - which this test also pins,
 * because a Surefire configuration that turned them off would remove the check silently rather than
 * visibly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5467UntrustedBlockAndMemoContractTest extends TestHelper {

  private static final SegmentParameters PARAMS = SegmentParameters.builder()
      .weightQuantization(WeightQuantization.FP32)
      .blockSize(SegmentFormat.MIN_BLOCK_SIZE)
      .build();

  /**
   * A posting count larger than the segment's own block size cannot be decoded into arrays sized for
   * that block size. Without the guard it is an {@code ArrayIndexOutOfBoundsException} from the
   * middle of the decode loop; with it, a named {@link IOException}.
   */
  @Test
  void aPostingCountPastTheBlockSizeReadsAsACorruptSegment() throws Exception {
    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildSegment("seg-5467-corrupt-count", 640)));

    final int[] blockLocator = new int[2];
    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        assertThat(c.metadata().blockCount()).as("the dim must span several blocks").isGreaterThan(2);
        blockLocator[0] = c.metadata().blockPageNum(1);
        blockLocator[1] = c.metadata().blockOffset(1);
      }
    });

    // posting_count sits after the block header's two RIDs.
    inTx(() -> {
      final MutablePage page = component.get().modifyPage(blockLocator[0]);
      page.writeShort(blockLocator[1] + 2 * SegmentFormat.RID_SIZE_BYTES, (short) (PARAMS.blockSize() + 1));
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

  /**
   * A header claiming zero postings is corruption too - {@link SparseSegmentBuilder} never writes an
   * empty block - and it is the more dangerous of the two, because nothing runs off the end of
   * anything: the block would simply report the header's first RID as a posting that is not there.
   */
  @Test
  void aZeroPostingCountReadsAsACorruptSegmentRatherThanAPhantomPosting() throws Exception {
    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildSegment("seg-5467-corrupt-zero", 640)));

    final int[] blockLocator = new int[2];
    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        blockLocator[0] = c.metadata().blockPageNum(1);
        blockLocator[1] = c.metadata().blockOffset(1);
      }
    });

    inTx(() -> {
      final MutablePage page = component.get().modifyPage(blockLocator[0]);
      page.writeShort(blockLocator[1] + 2 * SegmentFormat.RID_SIZE_BYTES, (short) 0);
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
            .hasMessageContaining("posting count 0");
      }
    });
  }

  /**
   * RIDs within a block are strictly ascending by construction: {@code appendInternal} rejects a
   * non-increasing RID and {@code flushBlock} fails loud on a negative delta, on the stated grounds
   * that "a negative delta would silently encode as a huge unsigned VarInt and decode to a different
   * RID on read". The read side has to say the same thing, or a corrupt payload decodes to a
   * descending or wrapped RID sequence and the merge quietly runs out of order.
   * <p>
   * A zero delta pair is the smallest expression of that: it encodes "the same RID again", which no
   * writer can produce.
   */
  @Test
  void aNonAscendingPostingSequenceReadsAsACorruptSegment() throws Exception {
    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildSegment("seg-5467-corrupt-order", 640)));

    final int[] blockLocator = new int[2];
    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        blockLocator[0] = c.metadata().blockPageNum(1);
        blockLocator[1] = c.metadata().blockOffset(1);
      }
    });

    inTx(() -> {
      final MutablePage page = component.get().modifyPage(blockLocator[0]);
      final int payloadStart = blockLocator[1] + SegmentFormat.BLOCK_HEADER_SIZE;
      // (bucketDelta 0, positionDelta 0): a repeat of the previous RID.
      page.writeByte(payloadStart, (byte) 0x00);
      page.writeByte(payloadStart + 1, (byte) 0x00);
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

  /**
   * A payload overwritten with continuation bytes never terminates a VarLong. The decode has to stop
   * on its own rather than shifting past the width of a {@code long} - Java masks shift amounts by 63,
   * so an unguarded loop would silently wrap and decode garbage instead of failing.
   */
  @Test
  void aPayloadOfContinuationBytesReadsAsACorruptSegmentRatherThanWrappingTheShift() throws Exception {
    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildSegment("seg-5467-corrupt-varint", 640)));

    final int[] blockLocator = new int[2];
    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        blockLocator[0] = c.metadata().blockPageNum(1);
        blockLocator[1] = c.metadata().blockOffset(1);
      }
    });

    inTx(() -> {
      final MutablePage page = component.get().modifyPage(blockLocator[0]);
      final int payloadStart = blockLocator[1] + SegmentFormat.BLOCK_HEADER_SIZE;
      // Enough bytes to blow past the 64-bit shift budget, all with the continuation bit set.
      for (int i = 0; i < 4 * VarInt.MAX_VARLONG_BYTES; i++)
        page.writeByte(payloadStart + i, (byte) 0xFF);
    });

    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
        assertThatThrownBy(() -> {
          c.start();
          while (c.advance())
            ;
        }).isInstanceOf(IOException.class).hasMessageContaining("segment is corrupt");
      }
    });
  }

  /**
   * {@link DimCursor#lastProbedBlockEnd} may only be called for the probe the preceding
   * {@link DimCursor#blockMaxAt} refreshed the memo with. Calling it for any other probe must fail,
   * and it must fail <i>here</i>, in the test suite, rather than in production where assertions are
   * off and the caller would silently get a bound for the wrong block.
   */
  @Test
  void theBlockBoundsMemoContractIsEnforcedWhileTheSuiteRunsWithAssertions() throws Exception {
    boolean assertionsEnabled = false;
    assert assertionsEnabled = true;
    assertThat(assertionsEnabled)
        .as("lastProbedBlockEnd's contract is enforced by a Java assert, so the suite must run with -ea "
            + "(Surefire enables assertions by default); without it this guard is not present at all")
        .isTrue();

    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildSegment("seg-5467-memo-contract", 640)));

    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      try (final DimCursor c = new DimCursor(0, List.of(reader.openCursor(0)))) {
        c.start();
        final int probeBucketId = c.currentBucketId();
        final long probePosition = c.currentPosition();

        // The sanctioned pairing: refresh the memo, then read the block end it goes with.
        assertThat(c.blockMaxAt(probeBucketId, probePosition)).isGreaterThan(0.0f);
        assertThat(c.lastProbedBlockEnd(probeBucketId, probePosition)).isNotNull();

        // Any other probe is a misuse, and the assert has to say so.
        assertThatThrownBy(() -> c.lastProbedBlockEnd(probeBucketId, probePosition - 1_000_000L))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("the block-bounds memo does not cover this probe");
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

  /** One dim of {@code postings} sequential RIDs, which at the minimum block size spans many blocks. */
  private SparseSegmentComponent buildSegment(final String name, final int postings) throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
        ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
    ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);

    final TreeMap<RID, Float> byRid = new TreeMap<>();
    for (int i = 0; i < postings; i++)
      byRid.put(new RID(0, 1L + i), 0.25f + (i % 7) * 0.1f);

    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, PARAMS)) {
      b.setSegmentId(5467L);
      b.startDim(0);
      for (final var e : byRid.entrySet())
        b.appendPosting(e.getKey(), e.getValue());
      b.endDim();
      b.finish();
    }
    return c;
  }
}
