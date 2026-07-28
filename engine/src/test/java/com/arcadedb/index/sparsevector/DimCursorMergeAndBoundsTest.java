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
import com.arcadedb.schema.LocalSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the merged block-bound memo introduced in the second round of issue #5467.
 * <p>
 * {@link DimCursor#blockMaxAt(int, long)} and {@link DimCursor#blockEndAt(int, long)} are consulted
 * once per candidate by {@link BmwScorer}'s Block-Max skip, but their value is constant over the
 * whole block range the bound covers - a block holds {@code blockSize} postings, 128 by default - so
 * recomputing them per candidate walked every live source and, inside each, the segment's block
 * headers. The reporter's profile put the merged walk at 8.6% of query self time on a 1M SPLADE
 * corpus, the second-largest line after the essential-term heap.
 * <p>
 * Two properties have to hold for the memo to be safe, and both are asserted here:
 * <ul>
 *   <li><b>Soundness.</b> The value handed back must remain an upper bound on the merged weight of
 *       <i>every</i> posting between the probe and the reported block end - that is the whole
 *       contract the scorer prunes against, so a memo that is one block stale would silently drop
 *       documents from the result set rather than fail loudly.</li>
 *   <li><b>Out-of-order probes bypass the memo.</b> The memo is only valid forward of the position
 *       it was computed at; a probe before it must recompute, or a caller walking backwards would
 *       get a bound from a block that does not cover it.</li>
 * </ul>
 * The third test asserts the memo actually fires, which is the point of the change: the underlying
 * sources must be consulted once per block range rather than once per probe.
 * <p>
 * The last test covers the other half of the same round: a {@link DimCursor} over a lone source
 * skips the merge machinery entirely, so it has to be proven to iterate and seek exactly as the
 * general N-source path does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DimCursorMergeAndBoundsTest extends TestHelper {

  /** Several hundred postings per segment, so the dim spans dozens of blocks at the default size. */
  private static final int POSTINGS = 4_000;

  @Test
  void mergedBlockMaxBoundsEveryPostingUpToTheReportedBlockEnd() throws Exception {
    // Two overlapping segments so the merge exercises the newest-wins tie path: the bound has to
    // cover whichever source ends up supplying the weight.
    final TreeMap<RID, Float> older = weightedRids(0, POSTINGS, 2L, 0.10f, 11L);
    final TreeMap<RID, Float> newer = weightedRids(0, POSTINGS, 3L, 0.20f, 22L);
    final TreeMap<RID, Float> merged = new TreeMap<>(SparseSegmentBuilder::compareRid);
    merged.putAll(older);
    merged.putAll(newer);  // newest source wins on conflict

    inTx(() -> {
      final PaginatedSegmentReader oldReader = buildSegment("seg-5467-bounds-old", 1L, older);
      final PaginatedSegmentReader newReader = buildSegment("seg-5467-bounds-new", 2L, newer);

      try (final DimCursor c = new DimCursor(0, List.of(oldReader.openCursor(0), newReader.openCursor(0)))) {
        c.start();
        int probes = 0;
        while (!c.isExhausted()) {
          final int bucketId = c.currentBucketId();
          final long position = c.currentPosition();
          final float blockMax = c.blockMaxAt(bucketId, position);
          final RID blockEnd = c.blockEndAt(bucketId, position);
          final RID probe = new RID(bucketId, position);

          assertThat(blockMax).isGreaterThanOrEqualTo(merged.get(probe));
          if (blockEnd != null) {
            assertThat(SparseSegmentBuilder.compareRid(blockEnd, probe)).isGreaterThanOrEqualTo(0);
            // The bound is what the scorer skips a whole range against, so it has to hold for every
            // posting in [probe, blockEnd], not merely for the probe itself.
            for (final Float w : merged.subMap(probe, true, blockEnd, true).values())
              assertThat(w).isLessThanOrEqualTo(blockMax);
          }
          probes++;
          c.advance();
        }
        assertThat(probes).isEqualTo(merged.size());
      }
    });
  }

  @Test
  void aProbeBehindTheMemoisedRangeRecomputesInsteadOfReusingIt() throws Exception {
    // Heavy head, negligible tail: a memo taken at the tail and reused for a head probe would
    // return a bound far below the head weight, which is exactly the failure mode to exclude.
    final TreeMap<RID, Float> postings = new TreeMap<>(SparseSegmentBuilder::compareRid);
    for (int i = 0; i < POSTINGS; i++)
      postings.put(new RID(0, 1L + i), i < 128 ? 5.0f : 0.01f);

    inTx(() -> {
      final PaginatedSegmentReader reader = buildSegment("seg-5467-bounds-backward", 1L, postings);

      try (final DimCursor c = new DimCursor(0, List.of(reader.openCursor(0)))) {
        c.start();
        final RID head = c.currentRid();

        // Probe far ahead first, which is what fills the memo with a tail block...
        final RID tail = new RID(0, (long) POSTINGS);
        final float tailMax = c.blockMaxAt(tail.getBucketId(), tail.getPosition());
        assertThat(tailMax).isLessThan(1.0f);

        // ...then back at the head, where the memo must not be reused.
        final float headMax = c.blockMaxAt(head.getBucketId(), head.getPosition());
        assertThat(headMax).isGreaterThanOrEqualTo(5.0f);
      }
    });
  }

  @Test
  void blockBoundsAreResolvedOncePerBlockRangeRatherThanOncePerProbe() throws Exception {
    final TreeMap<RID, Float> postings = weightedRids(0, POSTINGS, 1L, 0.10f, 33L);

    inTx(() -> {
      final PaginatedSegmentReader reader = buildSegment("seg-5467-bounds-memo", 1L, postings);
      final PaginatedSegmentDimCursor source = reader.openCursor(0);
      final long blockCount = source.metadata().blockCount();
      final CountingSourceCursor counting = new CountingSourceCursor(source);

      try (final DimCursor c = new DimCursor(0, List.of(counting))) {
        c.start();
        int probes = 0;
        RID previousEnd = null;
        while (!c.isExhausted()) {
          final int bucketId = c.currentBucketId();
          final long position = c.currentPosition();
          c.blockMaxAt(bucketId, position);
          final RID end = c.blockEndAt(bucketId, position);
          // A repeated probe inside the same range must hand back the very same object, i.e. the
          // block-max probe of a Block-Max skip allocates nothing per candidate.
          assertThat(c.blockEndAt(bucketId, position)).isSameAs(end);
          previousEnd = end;
          probes++;
          c.advance();
        }
        assertThat(previousEnd).isNotNull();
        assertThat(probes).isEqualTo(POSTINGS);
        // A block holds blockSize postings, so the source must be consulted once per block, not
        // once per posting. Before the memo both counters were equal to the probe count.
        assertThat(blockCount).isLessThan(probes / 10L);
        assertThat(counting.blockMaxCalls).isLessThanOrEqualTo(blockCount + 1);
        assertThat(counting.blockEndCalls).isLessThanOrEqualTo(blockCount + 1);
      }
    });
  }

  @Test
  void aLoneSourceIteratesAndSeeksExactlyAsTheMergedPathDoes() throws Exception {
    // The same posting set twice: once whole in a single segment, once split across two disjoint
    // segments. The lone-source cursor takes the fast path, the split one the general merge, and
    // every observable of the two must agree posting for posting.
    final TreeMap<RID, Float> whole = weightedRids(0, POSTINGS, 2L, 0.10f, 44L);
    final TreeMap<RID, Float> even = new TreeMap<>(SparseSegmentBuilder::compareRid);
    final TreeMap<RID, Float> odd = new TreeMap<>(SparseSegmentBuilder::compareRid);
    int i = 0;
    for (final Map.Entry<RID, Float> e : whole.entrySet())
      (i++ % 2 == 0 ? even : odd).put(e.getKey(), e.getValue());

    inTx(() -> {
      final PaginatedSegmentReader all = buildSegment("seg-5467-single-all", 1L, whole);
      final PaginatedSegmentReader evenReader = buildSegment("seg-5467-single-even", 1L, even);
      final PaginatedSegmentReader oddReader = buildSegment("seg-5467-single-odd", 2L, odd);

      try (final DimCursor lone = new DimCursor(0, List.of(all.openCursor(0)));
           final DimCursor split = new DimCursor(0, List.of(evenReader.openCursor(0), oddReader.openCursor(0)))) {
        lone.start();
        split.start();
        int seen = 0;
        while (!lone.isExhausted() || !split.isExhausted()) {
          assertThat(lone.isExhausted()).isEqualTo(split.isExhausted());
          assertThat(lone.currentBucketId()).isEqualTo(split.currentBucketId());
          assertThat(lone.currentPosition()).isEqualTo(split.currentPosition());
          assertThat(lone.currentRid()).isEqualTo(split.currentRid());
          assertThat(lone.currentWeight()).isEqualTo(split.currentWeight());
          assertThat(lone.isTombstone()).isEqualTo(split.isTombstone());
          seen++;
          assertThat(lone.advance()).isEqualTo(split.advance());
        }
        assertThat(seen).isEqualTo(whole.size());
        // Both must report the same exhausted state, with no stale position left behind.
        assertThat(lone.currentBucketId()).isEqualTo(-1);
        assertThat(split.currentBucketId()).isEqualTo(-1);
      }

      // Seeks, including one past the end, from a fresh pair each time so every jump starts cold.
      final List<RID> targets = new ArrayList<>(whole.keySet());
      for (int t = 0; t < targets.size(); t += 331) {
        try (final DimCursor lone = new DimCursor(0, List.of(all.openCursor(0)));
             final DimCursor split = new DimCursor(0, List.of(evenReader.openCursor(0), oddReader.openCursor(0)))) {
          lone.start();
          split.start();
          final RID target = targets.get(t);
          assertThat(lone.seekTo(target)).isEqualTo(split.seekTo(target));
          assertThat(lone.currentRid()).isEqualTo(split.currentRid());
          assertThat(lone.currentWeight()).isEqualTo(split.currentWeight());
        }
      }
      try (final DimCursor lone = new DimCursor(0, List.of(all.openCursor(0)));
           final DimCursor split = new DimCursor(0, List.of(evenReader.openCursor(0), oddReader.openCursor(0)))) {
        lone.start();
        split.start();
        final RID pastTheEnd = new RID(9, Long.MAX_VALUE);
        assertThat(lone.seekTo(pastTheEnd)).isFalse();
        assertThat(split.seekTo(pastTheEnd)).isFalse();
        assertThat(lone.isExhausted()).isTrue();
        assertThat(split.isExhausted()).isTrue();
        assertThat(lone.currentBucketId()).isEqualTo(-1);
        assertThat(split.currentBucketId()).isEqualTo(-1);
      }
    });
  }

  // ---------- helpers ----------

  /** Counts how often the merged cursor reaches through to the underlying source's block bounds. */
  private static final class CountingSourceCursor implements SourceCursor {
    private final SourceCursor delegate;
    private long               blockMaxCalls;
    private long               blockEndCalls;

    private CountingSourceCursor(final SourceCursor delegate) {
      this.delegate = delegate;
    }

    @Override
    public void start() throws IOException {
      delegate.start();
    }

    @Override
    public boolean advance() throws IOException {
      return delegate.advance();
    }

    @Override
    public boolean seekTo(final RID target) throws IOException {
      return delegate.seekTo(target);
    }

    @Override
    public boolean seekTo(final int bucketId, final long position) throws IOException {
      return delegate.seekTo(bucketId, position);
    }

    @Override
    public RID currentRid() {
      return delegate.currentRid();
    }

    @Override
    public int currentBucketId() {
      return delegate.currentBucketId();
    }

    @Override
    public long currentPosition() {
      return delegate.currentPosition();
    }

    @Override
    public float currentWeight() {
      return delegate.currentWeight();
    }

    @Override
    public boolean isTombstone() {
      return delegate.isTombstone();
    }

    @Override
    public boolean isExhausted() {
      return delegate.isExhausted();
    }

    @Override
    public float upperBoundRemaining() {
      return delegate.upperBoundRemaining();
    }

    @Override
    public long documentFrequency() {
      return delegate.documentFrequency();
    }

    @Override
    public float blockMaxAt(final RID rid) {
      blockMaxCalls++;
      return delegate.blockMaxAt(rid);
    }

    @Override
    public float blockMaxAt(final int bucketId, final long position) {
      blockMaxCalls++;
      return delegate.blockMaxAt(bucketId, position);
    }

    @Override
    public RID blockEndAt(final RID rid) {
      blockEndCalls++;
      return delegate.blockEndAt(rid);
    }

    @Override
    public RID blockEndAt(final int bucketId, final long position) {
      blockEndCalls++;
      return delegate.blockEndAt(bucketId, position);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  private static TreeMap<RID, Float> weightedRids(final int bucketId, final int count, final long stride,
      final float base, final long seed) {
    final Random rnd = new Random(seed);
    final TreeMap<RID, Float> out = new TreeMap<>(SparseSegmentBuilder::compareRid);
    for (int i = 0; i < count; i++)
      out.put(new RID(bucketId, 1L + i * stride), base + rnd.nextFloat());
    return out;
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

  /** FP32 so weights round-trip exactly and the bound assertions carry no quantization slack. */
  private PaginatedSegmentReader buildSegment(final String name, final long segmentId, final Map<RID, Float> postings)
      throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
        ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
    ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);

    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c,
        SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
      b.setSegmentId(segmentId);
      b.startDim(0);
      for (final var e : postings.entrySet())
        b.appendPosting(e.getKey(), e.getValue());
      b.endDim();
      b.finish();
    }
    return new PaginatedSegmentReader(c);
  }
}
