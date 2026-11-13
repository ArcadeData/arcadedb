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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.SegmentFormat.RidCompression;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for segment encoding, block bounds and untrusted on-disk bytes. Each nested class keeps the fixture of the issue it covers.
 */
class SparseSegmentIntegrityTest {

  /**
   * Regression for #5467: {@link RidCompression#RAW}, which had no test anywhere in the repository.
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
  @Nested
  class RawRidCompression extends TestHelper {

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

  /**
   * Regression for #5467: what the in-place block decode has to prove for itself, pinned as tests rather than
   * as prose.
   * <p>
   * <b>Untrusted block payloads.</b> Decoding used to copy a bounded slice of the page into a scratch
   * buffer, so a header describing more postings than the block really holds ran out of that slice and
   * surfaced as a {@code BufferUnderflowException} from the decode loop - protection that came for free
   * with the copy. Reading the page in place has no slice to run out of, so the decode carries its own
   * checks and reports a corrupt segment by name. A block header is untrusted input in the same sense
   * as issue #6566's segment surface: it is whatever is on disk. The cases below are the ones each
   * successive check was added for, and each fails if its check is removed:
   * <ul>
   *   <li>a posting count past the segment's block size, or of zero;</li>
   *   <li>a VarLong carrying payload bits past the width of a {@code long};</li>
   *   <li>a decoded RID component too large for a bucket id or a position;</li>
   *   <li>a posting sequence that is not strictly ascending;</li>
   *   <li>a payload that is well-formed and well-ordered throughout and still ends somewhere other
   *       than where the block header says it does.</li>
   * </ul>
   * <p>
   * <b>The fused block bounds.</b> The block-max skip needs both edges of one probe, and
   * {@link DimCursor#blockBoundsAt} returns them together so the memo is consulted once instead of
   * twice. This pins that the fused reader agrees with the two single-edge readers it replaced, across
   * a walk long enough to cross block boundaries repeatedly - which is where a memo answering for the
   * wrong probe would start to disagree.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UntrustedBlockAndMemoContract extends TestHelper {

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
     * The maximum-width VarLong case, which the ascending-RID check cannot see: nine continuation bytes
     * followed by a terminal {@code 0x02}. A tenth byte contributes only bit 63, so Java's shift
     * discards the {@code 2}, and the delta decodes as a perfectly ordered zero. No encoder can produce
     * that byte sequence - {@link VarInt#writeUnsignedVarLong} emits a tenth byte only for bit 63, with
     * payload 0 or 1 - so the width is checked rather than the resulting value.
     */
    @Test
    void aVarLongWiderThanALongReadsAsACorruptSegment() throws Exception {
      final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
      inTx(() -> component.set(buildSegment("seg-5467-corrupt-varwidth", 640)));

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
        for (int i = 0; i < VarInt.MAX_VARLONG_BYTES - 1; i++)
          page.writeByte(payloadStart + i, (byte) 0x80);
        // Payload 2 at shift 63: the bits Java throws away, leaving a delta of 0.
        page.writeByte(payloadStart + VarInt.MAX_VARLONG_BYTES - 1, (byte) 0x02);
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
              .hasMessageContaining("payload bits past the width of a long");
        }
      });
    }

    /**
     * A VarLong is unsigned, so a value at or above 2^63 arrives as a negative long - and neither
     * ordering check can see what is wrong with it.
     * <p>
     * A bucket delta of {@code 2^63 + 1} narrows to {@code 1}, which reads as an ordinary step to the
     * next bucket. A position of {@code 2^63} rides in on a bucket that genuinely did increase, so the
     * RID compares as greater however negative the position is. Both are rejected on range before
     * anything narrows or accumulates.
     */
    @Test
    void aDecodedRidComponentTooLargeForItsTypeReadsAsACorruptSegment() throws Exception {
      // (bucketDelta = 2^63 + 1): bit 0 in the first byte, bit 63 in the tenth.
      final byte[] hugeBucketDelta = new byte[VarInt.MAX_VARLONG_BYTES];
      hugeBucketDelta[0] = (byte) 0x81;
      for (int i = 1; i < VarInt.MAX_VARLONG_BYTES - 1; i++)
        hugeBucketDelta[i] = (byte) 0x80;
      hugeBucketDelta[VarInt.MAX_VARLONG_BYTES - 1] = (byte) 0x01;
      assertCorruptPayload("seg-5467-huge-bucket", hugeBucketDelta);

      // (bucketDelta = 1, position = 2^63): a real bucket step carrying a negative absolute position.
      final byte[] negativePosition = new byte[1 + VarInt.MAX_VARLONG_BYTES];
      negativePosition[0] = (byte) 0x01;
      for (int i = 1; i < negativePosition.length - 1; i++)
        negativePosition[i] = (byte) 0x80;
      negativePosition[negativePosition.length - 1] = (byte) 0x01;
      assertCorruptPayload("seg-5467-negative-position", negativePosition);
    }

    /**
     * The payload has to land where the block header says it ends. This is the check that does not
     * depend on spotting anything wrong with the bytes themselves: a payload can decode to a perfectly
     * well-ordered sequence of in-range RIDs and still be the wrong sequence, and the header's last RID
     * is an independent witness of what the builder actually wrote.
     * <p>
     * Bumping the first posting's delta by one shifts every RID after it, so the block ends one past
     * where the header says - ordered, in range, and wrong.
     */
    @Test
    void aPayloadThatDoesNotEndWhereTheHeaderSaysReadsAsACorruptSegment() throws Exception {
      final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
      inTx(() -> component.set(buildSegment("seg-5467-header-mismatch", 640)));

      final int[] blockLocator = new int[2];
      inTx(() -> {
        final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
        try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
          blockLocator[0] = c.metadata().blockPageNum(1);
          blockLocator[1] = c.metadata().blockOffset(1);
        }
      });

      inTx(() -> {
        // The fixture's RIDs are consecutive, so the first delta pair is (bucket 0, position 1).
        // Raising the position delta to 2 keeps everything ordered and in range, and moves the end.
        final MutablePage page = component.get().modifyPage(blockLocator[0]);
        final int payloadStart = blockLocator[1] + SegmentFormat.BLOCK_HEADER_SIZE;
        assertThat(page.readByte(payloadStart)).as("first bucket delta").isEqualTo((byte) 0x00);
        assertThat(page.readByte(payloadStart + 1)).as("first position delta").isEqualTo((byte) 0x01);
        page.writeByte(payloadStart + 1, (byte) 0x02);
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
              .hasMessageContaining("but the block header says");
        }
      });
    }

    /**
     * {@link DimCursor#blockBoundsAt} must hand back exactly what the two single-edge readers would for
     * the same probe. Fusing them is what lets the block-max skip consult the memo once instead of
     * twice, so it is worth pinning that the fusion is faithful and not merely fast - and that the pair
     * it returns belongs to one probe, which is the property an earlier version of this fix left to a
     * temporal contract nothing could enforce outside the test suite.
     */
    @Test
    void theFusedBlockBoundsMatchBothSingleEdgeReadersForTheSameProbe() throws Exception {
      final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
      inTx(() -> component.set(buildSegment("seg-5467-fused-bounds", 640)));

      inTx(() -> {
        final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
        try (final DimCursor probe = new DimCursor(0, List.of(reader.openCursor(0)));
            final DimCursor viaMax = new DimCursor(0, List.of(reader.openCursor(0)));
            final DimCursor viaEnd = new DimCursor(0, List.of(reader.openCursor(0)))) {
          probe.start();
          viaMax.start();
          viaEnd.start();

          final RID[] endOut = new RID[1];
          int checked = 0;
          // Walk far enough to cross several block boundaries, which is where a memo that answered for
          // the wrong probe would start disagreeing.
          while (!probe.isExhausted() && checked < 400) {
            final int bucketId = probe.currentBucketId();
            final long position = probe.currentPosition();

            final float fusedMax = probe.blockBoundsAt(bucketId, position, endOut);
            assertThat(fusedMax).isEqualTo(viaMax.blockMaxAt(bucketId, position));
            assertThat(endOut[0]).isEqualTo(viaEnd.blockEndAt(bucketId, position));
            assertThat(endOut[0]).as("a sealed segment always bounds a finite block end").isNotNull();
            checked++;

            if (!probe.advance())
              break;
          }
          assertThat(checked).as("the walk must actually cross block boundaries").isGreaterThan(3 * PARAMS.blockSize());
        }
      });
    }

    // ---------- helpers ----------

    /** Overwrite the start of a block payload with {@code bytes} and assert the decode reports corruption. */
    private void assertCorruptPayload(final String name, final byte[] bytes) throws Exception {
      final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
      inTx(() -> component.set(buildSegment(name, 640)));

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
        for (int i = 0; i < bytes.length; i++)
          page.writeByte(payloadStart + i, bytes[i]);
      });

      inTx(() -> {
        final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
        try (final PaginatedSegmentDimCursor c = reader.openCursor(0)) {
          assertThatThrownBy(() -> {
            c.start();
            while (c.advance())
              ;
          }).as("payload %s must not decode", name)
              .isInstanceOf(IOException.class)
              .hasMessageContaining("segment is corrupt")
              .hasMessageContaining("outside the range a bucket id or a position can hold");
        }
      });
    }

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

  /**
   * Regression for #6566: whether a sparse-vector index still holds a segment whose
   * precedence predates the recency-epoch fix (issue #6379) used to be observable only as a
   * once-per-instance {@code WARNING} log line - nothing could alert on it, and once a node had been
   * up for a while there was no way to ask "is this index still affected" without restarting it, nor
   * to confirm a {@code REBUILD INDEX} actually cleared it.
   * <p>
   * {@link PaginatedSparseVectorEngine#untrustedSegmentCount()} re-derives the same condition
   * {@code warnOnPreRecencyEpochSegments} logs, but from the live segment snapshot on every call
   * instead of latching it once - so it is what {@link LSMSparseVectorIndex#getStats()},
   * {@link LSMSparseVectorIndex#getUpgradeWarning()} (the same surface {@code schema:indexes},
   * {@code schema:index:<name>}, Studio and the HTTP admin API already read for every other index
   * upgrade warning) and {@link LSMSparseVectorIndexMetrics#buildJSON} now expose.
   * <p>
   * "Legacy-shaped" segments are synthesised the way a pre-#6379 build actually left them on disk:
   * parents recorded, recency epoch left at 0 - the same fixture
   * {@code PaginatedSparseVectorEngineCompactionRecencyTest#preRecencyEpochMergedSegmentIsReportedAtOpen}
   * uses to pin the WARNING log line itself.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UntrustedSegmentSurface extends TestHelper {

    private static final int DIM = 0;

    @Test
    void untrustedSegmentCountReportsALegacyShapedSegmentAndConfirmsARebuildClearsIt() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      // A legacy merge: parents recorded, epoch never set - the on-disk shape a pre-#6379 build left behind.
      database.transaction(() -> {
        final SparseSegmentComponent component = newComponent(db, "UntrustedSurfaceTest_seg99");
        try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, SegmentParameters.defaults())) {
          b.setSegmentId(99L);
          b.setRecencyEpoch(0L); // what a pre-fix builder left in the slot
          b.setParentSegments(new long[] { 90L, 91L });
          b.startDim(DIM);
          b.appendPosting(new RID(0, 500L), 0.5f);
          b.endDim();
          b.finish();
        }
      });

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine(db, "UntrustedSurfaceTest",
          SegmentParameters.defaults(), /* memtableFlushThreshold */ 100_000L, /* tierFanout */ 1_000_000,
          /* tierBasePostings */ 1L)) {
        assertThat(engine.segmentCount()).isEqualTo(1);
        assertThat(engine.untrustedSegmentCount())
            .as("the legacy-shaped segment must be counted, not only logged").isEqualTo(1);
      }

      // The remedy: drop the legacy segment and rebuild the index content from scratch, exactly what
      // REBUILD INDEX does at the storage level - a fresh segment with no parents.
      final var legacyComponent = ((LocalSchema) db.getSchema().getEmbedded()).getFileByName("UntrustedSurfaceTest_seg99");
      assertThat(legacyComponent).isNotNull();
      db.getFileManager().dropFile(legacyComponent.getFileId());

      try (final PaginatedSparseVectorEngine rebuilt = new PaginatedSparseVectorEngine(db, "UntrustedSurfaceTest",
          SegmentParameters.defaults(), /* memtableFlushThreshold */ 100_000L, /* tierFanout */ 1_000_000,
          /* tierBasePostings */ 1L)) {
        assertThat(rebuilt.segmentCount())
            .as("the legacy segment must be gone, not merely outnumbered").isZero();
        rebuilt.put(DIM, new RID(0, 501L), 0.7f);
        rebuilt.flush();
        assertThat(rebuilt.untrustedSegmentCount())
            .as("a queryable stat, not only a latched log line: it must be able to go back to zero and say so, "
                + "confirming the remedy actually worked").isZero();
      }
    }

    /**
     * End-to-end through the {@link LSMSparseVectorIndex} an ordinary {@code CREATE INDEX ... LSM_SPARSE_VECTOR}
     * produces: the surface a monitoring system or Studio would actually read, not only the engine underneath it.
     */
    @Test
    void upgradeWarningAndStatsSurfaceTheUntrustedSegmentThroughTheRealIndex() throws Exception {
      final String typeName = "UntrustedSurfaceDoc";
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType(typeName);
        type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
        type.createProperty("weights", Type.ARRAY_OF_FLOATS);
        database.getSchema().buildTypeIndex(typeName, new String[] { "tokens", "weights" })
            .withSparseVectorType().withDimensions(100).create();

        final MutableDocument doc = database.newDocument(typeName);
        doc.set("tokens", new int[] { 1, 5 });
        doc.set("weights", new float[] { 0.3f, 0.7f });
        doc.save();
      });

      final LSMSparseVectorIndex sparse = sparseSubIndex(typeName);
      sparse.getEngine().flush();
      final String engineName = sparse.getName(); // the per-bucket physical index name the engine's segments are keyed under

      assertThat(sparse.getUpgradeWarning())
          .as("nothing has been merged from a pre-#6379 segment yet").isNull();
      assertThat(sparse.getStats().get("untrustedSegments")).isZero();

      final DatabaseInternal db = (DatabaseInternal) database;
      database.transaction(() -> {
        final SparseSegmentComponent component = newComponent(db, engineName + "_seg999");
        try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, SegmentParameters.defaults())) {
          b.setSegmentId(999L);
          b.setRecencyEpoch(0L);
          b.setParentSegments(new long[] { 900L, 901L });
          b.startDim(DIM);
          b.appendPosting(new RID(0, 500L), 0.5f);
          b.endDim();
          b.finish();
        }
      });

      // Capture what the schema-load warning actually says on reopen (PR #6720 review): getUpgradeWarning() must
      // describe the problem only, and LocalSchema.reportUpgradeWarning() - the sole caller that knows the LOGICAL
      // TypeIndex name - must be the one and only place "REBUILD INDEX" is appended, naming that logical index and
      // not the physical per-bucket sub-index name this test's own engineName variable holds.
      final List<String> warnings = Collections.synchronizedList(new ArrayList<>());
      final Logger originalLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(new CapturingLogger(warnings, originalLogger));
      try {
        reopenDatabase();
      } finally {
        LogManager.instance().setLogger(originalLogger);
      }

      final LSMSparseVectorIndex reopened = sparseSubIndex(typeName);
      assertThat(reopened.getStats().get("untrustedSegments"))
          .as("the reopened index must count the legacy segment now on disk").isEqualTo(1L);
      assertThat(reopened.getUpgradeWarning())
          .as("getUpgradeWarning() itself must say what is lost, not what to type - the physical sub-index name "
              + "must never appear in a 'REBUILD INDEX' clause it assembles")
          .contains("#6379").doesNotContain("REBUILD INDEX");

      final String logicalName = typeName + "[tokens,weights]";
      final String remedyLine = "REBUILD INDEX `" + logicalName + "`";
      assertThat(warnings)
          .as("the schema-load WARNING must name the LOGICAL index for REBUILD INDEX exactly once, never the "
              + "physical per-bucket sub-index name (%s) this fix used to embed a second time", engineName)
          .anyMatch(w -> w.contains("#6379") && w.contains(remedyLine) && countOccurrences(w, "REBUILD INDEX") == 1
              && !w.contains(engineName));

      // Exhaustive, not anyMatch: a second, wrongly-targeted WARNING could coexist with the correct one above and
      // an anyMatch-only assertion would not notice (PR #6720 review, round 2 - PaginatedSparseVectorEngine's own
      // warnOnPreRecencyEpochSegments() log line had exactly this bug, undetected by this test's first version).
      // Every captured line is checked, not just the one this test is looking for.
      assertThat(warnings)
          .as("no captured WARNING may pair 'REBUILD INDEX' with the physical per-bucket sub-index name (%s)",
              engineName)
          .noneMatch(w -> w.contains("REBUILD INDEX") && w.contains(engineName));
    }

    private LSMSparseVectorIndex sparseSubIndex(final String typeName) {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(typeName + "[tokens,weights]");
      return (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    }

    private static SparseSegmentComponent newComponent(final DatabaseInternal db, final String name) {
      try {
        final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
            ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
        ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
        return c;
      } catch (final IOException e) {
        throw new RuntimeException("failed to create sparse segment component '" + name + "'", e);
      }
    }

    private static int countOccurrences(final String haystack, final String needle) {
      int count = 0;
      for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length()))
        count++;
      return count;
    }

    /**
     * Captures WARNING-and-above messages while forwarding everything to the real logger, matching the pattern
     * {@code PaginatedSparseVectorEngineCompactionRecencyTest} uses for the same purpose. The fixed-arity overload
     * must NOT delegate to the varargs one: the fixed arity is the exact match, so it would recurse until the stack
     * dies.
     */
    private static final class CapturingLogger implements Logger {
      private final List<String> warnings;
      private final Logger       delegate;

      CapturingLogger(final List<String> warnings, final Logger delegate) {
        this.warnings = warnings;
        this.delegate = delegate;
      }

      private void capture(final Level level, final String message, final Object... args) {
        if (message == null || level.intValue() < Level.WARNING.intValue())
          return;
        String formatted = message;
        if (args != null && args.length > 0) {
          try {
            formatted = message.formatted(args);
          } catch (final Exception ignored) {
            // Raw template is good enough for the substring matching the assertions do.
          }
        }
        warnings.add(formatted);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
            arg15, arg16, arg17);
        delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
            arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        capture(level, message, args);
        delegate.log(requester, level, message, exception, context, args);
      }

      @Override
      public void flush() {
        delegate.flush();
      }
    }
  }

  /**
   * Regression for #8157: the FP16 weight codec round-trip.
   * <p>
   * {@code WeightCodec}'s fp16 arm documents itself as IEEE 754 half-precision, "round-to-nearest-even,
   * subnormal-preserving". Its decoder normalised the subnormal significand counting the shifts in a variable
   * incremented at the TOP of the loop, so the exponent it rebuilt was one binade too high and EVERY one of the 2046
   * subnormal patterns decoded to exactly TWICE the value it denotes. Half subnormals cover |w| &lt; 2^-14 (about
   * 6.1e-5), which for a sparse-vector posting list is an ordinary small term weight, so a document's contribution for
   * such a dimension was scored at double its stored weight: wrong similarity scores and wrong top-K order, silently,
   * in the quantization mode that exists precisely to be MORE accurate than the INT8 default.
   * <p>
   * The encoder agreed with the JDK on magnitude but rounded half-AWAY-from-zero rather than to even, worth 1 ulp on
   * an exact tie.
   * <p>
   * The reference is the JDK's own implementation of the same format ({@code Float.float16ToFloat} /
   * {@code Float.floatToFloat16}), and the sweep is EXHAUSTIVE over the whole 16-bit domain: the defect lived entirely
   * inside a range that the in-tree test's eight sampled magnitudes straddled without ever landing in (its smallest,
   * 1e-4, is just above the subnormal threshold).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WeightCodecFp16 {

    /** The number of half-precision patterns whose exponent field is zero and significand is not: 2 signs x 1023. */
    private static final int SUBNORMAL_PATTERNS = 2046;

    @Test
    void everyHalfPatternDecodesExactlyAsTheJdkDoes() {
      int subnormalsChecked = 0;
      for (int h = 0; h <= 0xFFFF; h++) {
        final short pattern = (short) h;
        final float decoded = WeightCodec.fromFp16(pattern);
        final float reference = Float.float16ToFloat(pattern);

        if (Float.isNaN(reference)) {
          assertThat(Float.isNaN(decoded)).as("pattern 0x%04X must stay NaN", h).isTrue();
          continue;
        }
        assertThat(Float.floatToRawIntBits(decoded))
            .as("pattern 0x%04X decoded to %s, the format says %s", h, decoded, reference)
            .isEqualTo(Float.floatToRawIntBits(reference));

        if (((h >>> 10) & 0x1F) == 0 && (h & 0x3FF) != 0)
          ++subnormalsChecked;
      }
      // The sweep has to have actually visited the broken range, not merely covered the normals around it.
      assertThat(subnormalsChecked).isEqualTo(SUBNORMAL_PATTERNS);
    }

    @Test
    void subnormalWeightsRoundTripAtTheirOwnMagnitudeAndNotAtTwiceIt() {
      // Every one of these is below the 2^-14 subnormal threshold and used to come back doubled.
      final float[] subnormalWeights = { 6.0e-5f, 5.0e-5f, 3.0e-5f, 1.0e-5f, 1.0e-6f, 1.0e-7f, -5.0e-5f, -1.0e-6f };
      for (final float w : subnormalWeights) {
        final float back = WeightCodec.fromFp16(WeightCodec.toFp16(w));
        assertThat(back).as("weight %s", w).isNotZero();
        // What the stored pattern denotes, exactly.
        assertThat(back).as("weight %s", w).isEqualTo(Float.float16ToFloat(Float.floatToFloat16(w)));
        // And the defect itself: a subnormal's quantization error grows towards the bottom of the range (2^-24 is the
        // step there, so 1e-7 legitimately comes back ~19% high), but it was never a FACTOR OF TWO. 1.5 separates the
        // two readings for every magnitude in the range and is not a precision claim.
        assertThat(Math.abs(back)).as("weight %s came back as %s", w, back).isLessThan(Math.abs(w) * 1.5f);
      }
    }

    @Test
    void theSmallestSubnormalIsTwoToTheMinusTwentyFour() {
      // The single pattern the issue opens with: significand 1, exponent 0. It denotes 2^-24, and answered 2^-23.
      assertThat(WeightCodec.fromFp16((short) 0x0001)).isEqualTo(Math.scalb(1.0f, -24));
      assertThat(WeightCodec.fromFp16((short) 0x8001)).isEqualTo(-Math.scalb(1.0f, -24));
    }

    @Test
    void encodingMatchesTheJdkForEveryFiniteInRangeValue() {
      final Random random = new Random(8157);
      int checked = 0;
      for (int i = 0; i < 2_000_000; i++) {
        final float f = i < 0x10000 ? Float.float16ToFloat((short) i) : Float.intBitsToFloat(random.nextInt());
        if (Float.isNaN(f) || Math.abs(f) > 65504.0f)
          // Both are deliberate departures, asserted on their own below.
          continue;
        assertThat(WeightCodec.toFp16(f)).as("value %s", f).isEqualTo(Float.floatToFloat16(f));
        ++checked;
      }
      assertThat(checked).isGreaterThan(1_000_000);
    }

    @Test
    void finiteOverflowSaturatesInsteadOfBecomingInfinity() {
      // The segment format's own rule, kept: a weight has to stay a number that can be summed into a score.
      assertThat(WeightCodec.fromFp16(WeightCodec.toFp16(1.0e6f))).isEqualTo(65504.0f);
      assertThat(WeightCodec.fromFp16(WeightCodec.toFp16(-1.0e6f))).isEqualTo(-65504.0f);
      assertThat(WeightCodec.fromFp16(WeightCodec.toFp16(Float.MAX_VALUE))).isEqualTo(65504.0f);
      // An input that already IS infinite stays infinite, which is what the previous implementation did too.
      assertThat(WeightCodec.fromFp16(WeightCodec.toFp16(Float.POSITIVE_INFINITY))).isEqualTo(Float.POSITIVE_INFINITY);
      assertThat(WeightCodec.fromFp16(WeightCodec.toFp16(Float.NEGATIVE_INFINITY))).isEqualTo(Float.NEGATIVE_INFINITY);
    }

    /**
     * A NaN weight's fp16 image can land exactly on {@link SegmentFormat#FP16_TOMBSTONE_SENTINEL} - a negative NaN
     * whose high significand bit is the only one set encodes to 0xFE00 - and would then read back as a DELETED
     * posting. INT8 and fp32 already refuse a NaN weight for the same reason; fp16 was the one that did not.
     */
    @Test
    void nanIsRefusedRatherThanCollidingWithTheTombstoneSentinel() {
      assertThatThrownBy(() -> WeightCodec.toFp16(Float.NaN))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("NaN");
      assertThatThrownBy(() -> WeightCodec.toFp16(Float.intBitsToFloat(0xFFC00000)))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void noFiniteWeightEncodesToTheTombstoneSentinel() {
      final Random random = new Random(20260922);
      for (int i = 0; i < 500_000; i++) {
        final float f = Float.intBitsToFloat(random.nextInt());
        if (Float.isNaN(f))
          continue;
        assertThat(WeightCodec.toFp16(f)).isNotEqualTo(SegmentFormat.FP16_TOMBSTONE_SENTINEL);
      }
    }
  }

  /**
   * Regression for #8553: the Block-Max MaxScore traversal orders its posting cursors by one packed {@code long} per RID instead of
   * comparing {@code (bucketId, position)} pairs. That only holds if the packing preserves {@link SparseSegmentBuilder#compareRid}
   * exactly - including at the edges of the packed ranges, where a wrong shift or a sign bit would reorder postings
   * silently - and if a RID the packing cannot hold is neither refused nor misordered: file ids only grow, so a long-lived
   * database can legitimately own bucket ids of any size. This pins the order itself, then the answers of traversals on
   * the packed path, relative to a large base bucket id, and forced onto the RID-comparison fallback.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PackedRidKey extends TestHelper {

    private static final long MAX_POSITION   = SparseSegmentBuilder.MAX_PACKED_POSITION;
    private static final int  MAX_OFFSET     = SparseSegmentBuilder.MAX_PACKED_BUCKET_OFFSET;
    private static final int  BIG_BUCKET_ID  = 5_000_000;

    @Test
    void packedKeysOrderExactlyLikeCompareRid() {
      final Random rnd = new Random(8553L);
      for (final int base : new int[] { 0, 7, BIG_BUCKET_ID }) {
        final int[] offsets = { 0, 1, 2, 17, MAX_OFFSET - 1, MAX_OFFSET };
        final long[] positions = { 0L, 1L, 2L, 1L << 31, (1L << 41) + 7, MAX_POSITION - 1, MAX_POSITION };
        final List<Integer> bucketSamples = new ArrayList<>();
        final List<Long> positionSamples = new ArrayList<>();
        for (final int o : offsets)
          for (final long p : positions) {
            bucketSamples.add(base + o);
            positionSamples.add(p);
          }
        for (int i = 0; i < 1_000; i++) {
          bucketSamples.add(base + rnd.nextInt(MAX_OFFSET + 1));
          positionSamples.add(rnd.nextLong() & MAX_POSITION);
        }

        for (int i = 0; i < bucketSamples.size(); i++)
          for (int j = 0; j < bucketSamples.size(); j += 1 + (i % 7)) {
            final int b1 = bucketSamples.get(i);
            final long p1 = positionSamples.get(i);
            final int b2 = bucketSamples.get(j);
            final long p2 = positionSamples.get(j);
            final long k1 = SparseSegmentBuilder.packRid(b1, p1, base);
            final long k2 = SparseSegmentBuilder.packRid(b2, p2, base);
            assertThat(Integer.signum(Long.compare(k1, k2)))
                .as("(%d,%d) vs (%d,%d) on base %d", b1, p1, b2, p2, base)
                .isEqualTo(Integer.signum(SparseSegmentBuilder.compareRid(b1, p1, b2, p2)));
            assertThat(k1).isGreaterThanOrEqualTo(0L).isLessThan(SparseSegmentBuilder.UNPACKABLE);
            assertThat(SparseSegmentBuilder.unpackBucketId(k1, base)).isEqualTo(b1);
            assertThat(SparseSegmentBuilder.unpackPosition(k1)).isEqualTo(p1);
          }
      }

      // An exhausted cursor sorts first, as compareRid puts bucket -1 first.
      assertThat(SparseSegmentBuilder.packRid(-1, -1L, 0)).isEqualTo(-1L);
      // What does not fit is marked, never truncated into a wrong order.
      assertThat(SparseSegmentBuilder.packRid(MAX_OFFSET + 1, 0L, 0)).isEqualTo(SparseSegmentBuilder.UNPACKABLE);
      assertThat(SparseSegmentBuilder.packRid(3, MAX_POSITION + 1, 0)).isEqualTo(SparseSegmentBuilder.UNPACKABLE);
      assertThat(SparseSegmentBuilder.packRid(Integer.MAX_VALUE, 0L, 0)).isEqualTo(SparseSegmentBuilder.UNPACKABLE);
    }

    @Test
    void aBoundBeyondThePackedRangeMapsToTheFirstKeyNotBeforeIt() {
      assertThat(SparseSegmentBuilder.packRidCeiling(5, MAX_POSITION + 1, 0)).isEqualTo(SparseSegmentBuilder.packRid(6, 0L, 0));
      assertThat(SparseSegmentBuilder.packRidCeiling(5, Long.MAX_VALUE, 0)).isEqualTo(SparseSegmentBuilder.packRid(6, 0L, 0));
      assertThat(SparseSegmentBuilder.packRidCeiling(MAX_OFFSET + 1, 0L, 0)).isEqualTo(SparseSegmentBuilder.UNPACKABLE);
      assertThat(SparseSegmentBuilder.packRidCeiling(MAX_OFFSET, MAX_POSITION + 1, 0))
          .isGreaterThan(SparseSegmentBuilder.packRid(MAX_OFFSET, MAX_POSITION, 0))
          .isLessThan(SparseSegmentBuilder.UNPACKABLE);
      // A bound below the base is before every key the traversal can see.
      assertThat(SparseSegmentBuilder.packRidCeiling(3, 99L, 10)).isEqualTo(0L);
      assertThat(SparseSegmentBuilder.packRidCeiling(9, 12L, 0)).isEqualTo(SparseSegmentBuilder.packRid(9, 12L, 0));
    }

    /** Several buckets, positions at the very top of the range: the packed path, checked against an exact scan. */
    @Test
    void topKAcrossBucketsAndHighPositionsMatchesAnExactScan() throws Exception {
      final long fallbacksBefore = BmwScorer.WIDE_FALLBACKS.get();
      checkAgainstExactScan("Issue8553Packed", new int[] { 0, 3, 11, MAX_OFFSET }, false);
      assertThat(BmwScorer.WIDE_FALLBACKS.get())
          .as("RIDs inside the packed range must not leave the packed path")
          .isEqualTo(fallbacksBefore);
    }

    /** A bucket id far above 2^20 is not special: keys are packed relative to the traversal's smallest bucket id. */
    @Test
    void aLargeBucketIdIsAcceptedAndStaysOnThePackedPath() throws Exception {
      final long fallbacksBefore = BmwScorer.WIDE_FALLBACKS.get();
      checkAgainstExactScan("Issue8553BigBucket", new int[] { BIG_BUCKET_ID, BIG_BUCKET_ID + 2 }, false);
      assertThat(BmwScorer.WIDE_FALLBACKS.get()).isEqualTo(fallbacksBefore);
    }

    /**
     * Buckets further apart than the packed offset can span, and positions past 2^42: nothing is refused, and the
     * traversal finishes on RID comparisons with the same answers.
     */
    @Test
    void ridsThePackingCannotHoldFallBackToAnExactTraversal() throws Exception {
      final long fallbacksBefore = BmwScorer.WIDE_FALLBACKS.get();
      checkAgainstExactScan("Issue8553Wide", new int[] { 0, 2, MAX_OFFSET + 1, BIG_BUCKET_ID }, true);
      assertThat(BmwScorer.WIDE_FALLBACKS.get())
          .as("the corpus must actually force the fallback, or this test proves nothing about it")
          .isGreaterThan(fallbacksBefore);
    }

    private void checkAgainstExactScan(final String indexName, final int[] buckets, final boolean hugePositions)
        throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final SegmentParameters exact = SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build();
      final Random rnd = new Random(0x8553L);
      final int dims = 24;
      final Map<RID, Map<Integer, Float>> docs = new HashMap<>();
      for (final int bucket : buckets)
        for (int i = 0; i < 150; i++) {
          // Half the documents crowd the top of the position range (or pass it), half sit low: both edges of the packing
          // are exercised in the same traversal, and the bucket boundaries between them too.
          final long high = hugePositions && i % 4 == 0 ? Long.MAX_VALUE / 2 - i : MAX_POSITION - i;
          final long position = i % 2 == 0 ? high : (long) i * 1_000_003L;
          final Map<Integer, Float> doc = new TreeMap<>();
          while (doc.size() < 6)
            doc.put(rnd.nextInt(dims), 0.05f + rnd.nextFloat());
          docs.put(new RID(bucket, position), doc);
        }

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine(db, indexName, exact, 100_000L)) {
        int written = 0;
        for (final Map.Entry<RID, Map<Integer, Float>> doc : docs.entrySet()) {
          for (final Map.Entry<Integer, Float> dw : doc.getValue().entrySet())
            engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          // Flush halfway, so half the corpus is answered from a sealed segment and half from the memtable.
          if (++written == docs.size() / 2)
            engine.flush();
        }

        for (int q = 0; q < 40; q++) {
          final int qDims = 2 + rnd.nextInt(8);
          final int[] queryDims = new int[qDims];
          final float[] queryWeights = new float[qDims];
          final Set<Integer> seen = new HashSet<>();
          for (int filled = 0; filled < qDims; ) {
            final int d = rnd.nextInt(dims);
            if (seen.add(d)) {
              queryDims[filled] = d;
              queryWeights[filled++] = 0.1f + rnd.nextFloat();
            }
          }
          final int k = 1 + rnd.nextInt(25);

          final List<RidScore> got = engine.topK(queryDims, queryWeights, k);
          final List<RidScore> expected = exactTopK(docs, queryDims, queryWeights, k);
          assertThat(got).hasSize(expected.size());
          for (int i = 0; i < expected.size(); i++) {
            assertThat(got.get(i).rid()).as("query %d rank %d", q, i).isEqualTo(expected.get(i).rid());
            assertThat(got.get(i).score()).isCloseTo(expected.get(i).score(), Offset.offset(1e-4f));
          }
        }
      }
    }

    private static List<RidScore> exactTopK(final Map<RID, Map<Integer, Float>> docs, final int[] queryDims,
        final float[] queryWeights, final int k) {
      final List<RidScore> all = new ArrayList<>();
      for (final Map.Entry<RID, Map<Integer, Float>> doc : docs.entrySet()) {
        float score = 0f;
        boolean matched = false;
        for (int i = 0; i < queryDims.length; i++) {
          final Float w = doc.getValue().get(queryDims[i]);
          if (w != null) {
            score += queryWeights[i] * w;
            matched = true;
          }
        }
        if (matched)
          all.add(new RidScore(doc.getKey(), score));
      }
      all.sort(BmwScorer.BY_SCORE_DESC);
      return all.size() <= k ? all : new ArrayList<>(all.subList(0, k));
    }
  }

  /**
   * Regression for #9834: every bound a sparse segment stores for pruning must bound the value the reader will
   * actually DECODE from that segment. The builder took the block-max, the dim's global max and the skip list's
   * max-to-end from the raw float32 weight, while an FP16 segment reads back {@code fromFp16(toFp16(w))}, which is
   * round-to-nearest and so lands above the raw weight about half the time. A Block-Max WAND / MaxScore traversal whose
   * threshold sits on such an under-stated bound declares the term non-essential and never scores the document that
   * belongs in the top-K.
   * <p>
   * The tests pin the RULE (decoded weight never above any stored bound, for every quantization and segment shape), the
   * end-to-end symptom (the top-1 document is returned), and the read side for FP16 segments written before the fix,
   * which carry the raw bound on disk.
   */
  @Nested
  class Fp16BlockMaxBound extends TestHelper {

    private static final int[]   BLOCK_SIZES  = { SegmentFormat.MIN_BLOCK_SIZE, SegmentFormat.MIN_BLOCK_SIZE + 1, 128 };
    private static final int[]   SKIP_STRIDES = { 1, 3, 8 };
    private static final float   TARGET_RAW   = 0.999f;
    private static final RID     TARGET_RID   = new RID(0, 500L);

    /** The extracted "what will the reader see" helper must agree with the reader's own decode, bit for bit. */
    @Test
    void decodedValueOfMatchesTheReaderDecode() {
      final Random rnd = new Random(9834L);
      for (int i = 0; i < 100_000; i++) {
        final float a = rnd.nextFloat() * 100f;
        final float b = rnd.nextFloat() * 100f;
        final float min = Math.min(a, b);
        final float max = Math.max(a, b);
        final float w = min + rnd.nextFloat() * (max - min);
        assertThat(WeightCodec.decodedValueOf(w, WeightQuantization.FP32, min, max)).isEqualTo(w);
        assertThat(WeightCodec.decodedValueOf(w, WeightQuantization.FP16, min, max)).isEqualTo(
            WeightCodec.fromFp16(WeightCodec.toFp16(w)));
        assertThat(WeightCodec.decodedValueOf(w, WeightQuantization.INT8, min, max)).isEqualTo(
            WeightCodec.dequantizeInt8(WeightCodec.quantizeInt8(w, min, max), min, max));
      }
      // The case quoted in the issue: raw 16.007818 decodes to 16.015625 in fp16.
      assertThat(WeightCodec.decodedValueOf(16.007818f, WeightQuantization.FP16, 0f, 16.007818f)).isEqualTo(16.015625f);
    }

    /**
     * The stored-bound invariant across every quantization and a spread of block sizes and skip strides, with
     * tombstones mixed in: for each live posting the reader returns, the decoded weight is at most the block max the
     * cursor reports for it, at most {@code upperBoundRemaining()} at that position, and at most the dim's global max.
     */
    @Test
    void everyStoredBoundCoversTheDecodedWeight() throws Exception {
      int segment = 0;
      for (final WeightQuantization q : WeightQuantization.values())
        for (final int blockSize : BLOCK_SIZES)
          for (final int skipStride : SKIP_STRIDES) {
            final SegmentParameters params = SegmentParameters.builder().weightQuantization(q).blockSize(blockSize)
                .skipStride(skipStride).build();
            final String name = "seg-9834-inv-" + segment++;
            final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
            inTx(() -> component.set(buildRandomSegment(name, params, blockSize, skipStride)));
            inTx(() -> assertBoundsCoverDecodedWeights(new PaginatedSegmentReader(component.get()),
                q + "/blockSize=" + blockSize + "/skipStride=" + skipStride));
          }
    }

    /**
     * The end-to-end case from the issue: the 0.999f document's stored ceiling was 0.999 while it decodes to
     * 0.99902344, the threshold rises to exactly 0.999 on the first dim-0 document, dim 1 is proven non-essential on the
     * under-stated ceiling and the true top-1 is never scored.
     */
    @Test
    void fp16TopOneIsNotPrunedOnTheStoredBound() throws Exception {
      final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
      inTx(() -> component.set(buildIssueSegment("seg-9834-fp16-e2e", WeightQuantization.FP16)));
      inTx(() -> assertTopOneIsTheTarget(new PaginatedSegmentReader(component.get()), true));
    }

    /**
     * The WRITE side on its own: the bytes the builder persists in the target's block header are already the decoded
     * ceiling, so a new segment does not depend on the read-side lift. The lift would hide a builder regression from
     * every reader-level assertion, which is why this one reads the page directly.
     */
    @Test
    void builderPersistsTheDecodedCeilingInTheBlockHeader() throws Exception {
      final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
      inTx(() -> component.set(buildIssueSegment("seg-9834-fp16-bytes", WeightQuantization.FP16)));
      inTx(() -> {
        final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
        final PaginatedDimMetadata md = reader.dimMetadata(1);
        final int lastBlock = md.blockCount() - 1;
        // bmw_upper_bound sits after the block header's two RIDs and its 2-byte posting count.
        final float stored = component.get().readPage(md.blockPageNum(lastBlock))
            .readFloat(md.blockOffset(lastBlock) + 2 * SegmentFormat.RID_SIZE_BYTES + 2);
        assertThat(stored).isEqualTo(WeightCodec.fromFp16(WeightCodec.toFp16(TARGET_RAW)));
      });
    }

    /** Control: on FP32 the same data scores the target exactly at the threshold, so it correctly does not win. */
    @Test
    void fp32ControlKeepsTheTieBreakResult() throws Exception {
      final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
      inTx(() -> component.set(buildIssueSegment("seg-9834-fp32-e2e", WeightQuantization.FP32)));
      inTx(() -> assertTopOneIsTheTarget(new PaginatedSegmentReader(component.get()), false));
    }

    /**
     * An FP16 segment written before the fix carries the RAW bound on disk. Simulate one by rewriting every stored
     * bound back to the raw 0.999f (block headers, the dim trailer's global max and the skip list's max-to-end all hold
     * the same float), then check the reader still hands the scorer a bound that covers the decoded weight, and the
     * top-1 still comes back, without a rebuild.
     */
    @Test
    void legacyFp16SegmentWithRawBoundsIsLiftedOnRead() throws Exception {
      final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
      inTx(() -> component.set(buildIssueSegment("seg-9834-fp16-legacy", WeightQuantization.FP16)));

      final float decoded = WeightCodec.fromFp16(WeightCodec.toFp16(TARGET_RAW));
      assertThat(decoded).isGreaterThan(TARGET_RAW);

      // Exactly the three bound slots: the target's block header, the dim's global max and the dim's single skip entry
      // (4 blocks at the default stride of 8).
      rewriteBoundsToRaw(component.get(), decoded, TARGET_RAW, 3);

      inTx(() -> {
        final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
        assertBoundsCoverDecodedWeights(reader, "legacy FP16");
        assertTopOneIsTheTarget(reader, true);
      });
    }

    /**
     * INT8 is affected too, which the issue ruled out: its top level dequantizes to {@code min + 1.0f * (max - min)},
     * and float rounding puts that one ulp above {@code max} for bands like this one. A fixed builder stores the decoded
     * ceiling; a segment written before the fix stored the raw maximum and is lifted on read from the block's own band.
     */
    @Test
    void int8TopLevelDecodingAboveTheBandMaxIsCovered() throws Exception {
      final float min = 18.354017f;
      final float max = 50.413555f;
      final float decoded = WeightCodec.decodedValueOf(max, WeightQuantization.INT8, min, max);
      assertThat(decoded).as("this band's top INT8 level decodes above the raw maximum").isGreaterThan(max);

      final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
      inTx(() -> component.set(buildTwoPostingSegment("seg-9834-int8", WeightQuantization.INT8, min, max)));
      inTx(() -> assertBoundsCoverDecodedWeights(new PaginatedSegmentReader(component.get()), "INT8"));

      // The same segment as a pre-fix builder would have written it: block header, global max and skip entry raw.
      rewriteBoundsToRaw(component.get(), decoded, max, 3);
      inTx(() -> assertBoundsCoverDecodedWeights(new PaginatedSegmentReader(component.get()), "legacy INT8"));
    }

    // ---------- helpers ----------

    private void assertTopOneIsTheTarget(final PaginatedSegmentReader reader, final boolean expected) throws IOException {
      final int[] queryDims = { 0, 1 };
      final float[] queryWeights = { 1.998f, 1.0f };
      final DimCursor[] cursors = new DimCursor[queryDims.length];
      for (int i = 0; i < queryDims.length; i++)
        cursors[i] = new DimCursor(queryDims[i], List.of(reader.openCursor(queryDims[i])));
      try {
        final List<RidScore> top = BmwScorer.topK(queryDims, queryWeights, cursors, 1);
        assertThat(top).hasSize(1);
        if (expected)
          assertThat(top.getFirst().rid()).as("the document decoding above the threshold must be top-1").isEqualTo(TARGET_RID);
        else
          assertThat(top.getFirst().rid()).isNotEqualTo(TARGET_RID);
      } finally {
        for (final DimCursor c : cursors)
          c.close();
      }
    }

    private static void assertBoundsCoverDecodedWeights(final PaginatedSegmentReader reader, final String label)
        throws IOException {
      for (final int dim : reader.dims()) {
        final float globalMax = reader.dimMetadata(dim).globalMaxWeight();
        try (final PaginatedSegmentDimCursor c = reader.openCursor(dim)) {
          final float beforeStart = c.upperBoundRemaining();
          c.start();
          while (!c.isExhausted()) {
            if (!c.isTombstone()) {
              final float w = c.currentWeight();
              final RID rid = c.currentRid();
              assertThat(c.blockMaxAt(rid)).as("%s dim %d %s: blockMaxAt vs decoded weight", label, dim, rid)
                  .isGreaterThanOrEqualTo(w);
              assertThat(c.upperBoundRemaining()).as("%s dim %d %s: upperBoundRemaining vs decoded weight", label, dim, rid)
                  .isGreaterThanOrEqualTo(w);
              assertThat(beforeStart).as("%s dim %d %s: bound before start vs decoded weight", label, dim, rid)
                  .isGreaterThanOrEqualTo(w);
              assertThat(globalMax).as("%s dim %d %s: global max vs decoded weight", label, dim, rid)
                  .isGreaterThanOrEqualTo(w);
            }
            if (!c.advance())
              break;
          }
        }
      }
    }

    /** Dims with posting counts at the block / stride edges, random weights spread over several fp16 binades. */
    private SparseSegmentComponent buildRandomSegment(final String name, final SegmentParameters params, final int blockSize,
        final int skipStride) throws IOException {
      final Random rnd = new Random(name.hashCode());
      final int[] counts = { 1, blockSize, blockSize + 1, blockSize * skipStride, blockSize * skipStride + 7 };
      final SparseSegmentComponent c = newComponent(name);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, params)) {
        b.setSegmentId(9834L);
        for (int dim = 0; dim < counts.length; dim++) {
          b.startDim(dim);
          long position = 1L;
          int bucket = 0;
          for (int i = 0; i < counts[dim]; i++) {
            if (rnd.nextInt(50) == 0) {
              bucket++;
              position = 1L;
            }
            final RID rid = new RID(bucket, position);
            position += 1 + rnd.nextInt(3);
            if (rnd.nextInt(10) == 0)
              b.appendTombstone(rid);
            else
              b.appendPosting(rid, rnd.nextFloat() * 100f);
          }
          b.endDim();
        }
        b.finish();
      }
      return c;
    }

    /**
     * dim 0: 40 documents at 0.5 (exact in fp16), so with query weight 1.998 each scores 0.999. dim 1: 399 filler
     * postings at 0.001 (so the term sorts non-essential first on the tie) plus the target at 0.999f.
     */
    private SparseSegmentComponent buildIssueSegment(final String name, final WeightQuantization q) throws IOException {
      final Map<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
      final TreeMap<RID, Float> dim0 = new TreeMap<>();
      for (int i = 0; i < 40; i++)
        dim0.put(new RID(0, 1L + i), 0.5f);
      final TreeMap<RID, Float> dim1 = new TreeMap<>();
      for (int i = 0; i < 399; i++)
        dim1.put(new RID(0, 100L + i), 0.001f);
      dim1.put(TARGET_RID, TARGET_RAW);
      byDim.put(0, dim0);
      byDim.put(1, dim1);

      final SparseSegmentComponent c = newComponent(name);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, SegmentParameters.builder().weightQuantization(q).build())) {
        b.setSegmentId(9834L);
        for (final var dim : byDim.entrySet()) {
          b.startDim(dim.getKey());
          for (final var p : dim.getValue().entrySet())
            b.appendPosting(p.getKey(), p.getValue());
          b.endDim();
        }
        b.finish();
      }
      return c;
    }

    /**
     * Simulates a segment written before the fix by rewriting every 4-byte slot holding the decoded ceiling back to the
     * raw maximum. The count is exact so an accidental match inside a posting payload, which would corrupt the segment
     * instead of simulating an old one, fails here rather than somewhere less obvious.
     */
    private void rewriteBoundsToRaw(final SparseSegmentComponent c, final float decoded, final float raw, final int expectedSlots) {
      final int decodedBits = Float.floatToRawIntBits(decoded);
      final int rawBits = Float.floatToRawIntBits(raw);
      final int[] patched = { 0 };
      inTx(() -> {
        final int contentSize = c.pageContentSize();
        for (int pageNum = 0; pageNum < c.getTotalPages(); pageNum++) {
          final BasePage page = c.readPage(pageNum);
          for (int off = 0; off + 4 <= contentSize; off++) {
            if (page.readInt(off) == decodedBits) {
              final MutablePage mp = c.modifyPage(pageNum);
              mp.writeInt(off, rawBits);
              patched[0]++;
            }
          }
        }
      });
      assertThat(patched[0]).as("bound slots holding %s rewritten to %s; a different count means the segment layout changed"
          + " (block count, skip stride) or a posting payload happens to contain the same bytes", decoded, raw)
          .isEqualTo(expectedSlots);
    }

    private SparseSegmentComponent buildTwoPostingSegment(final String name, final WeightQuantization q, final float w1,
        final float w2) throws IOException {
      final SparseSegmentComponent c = newComponent(name);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, SegmentParameters.builder().weightQuantization(q).build())) {
        b.setSegmentId(9834L);
        b.startDim(0);
        b.appendPosting(new RID(0, 1L), w1);
        b.appendPosting(new RID(0, 2L), w2);
        b.endDim();
        b.finish();
      }
      return c;
    }

    private SparseSegmentComponent newComponent(final String name) throws IOException {
      final DatabaseInternal db = (DatabaseInternal) database;
      final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
          ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
      ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
      return c;
    }

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
  }
}
