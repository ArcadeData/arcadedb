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

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.index.sparsevector.SegmentFormat.RidCompression;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Forward cursor over the postings of a single dim within a page-backed sealed segment. Implements
 * a {@code block_header} / {@code posting} state machine and reads block payloads via
 * {@link PaginatedSegmentReader#readBlockPayloadInto} (page-cache-backed). The skip path uses the
 * per-segment skip list to avoid decompressing blocks that cannot beat the current threshold.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PaginatedSegmentDimCursor implements SourceCursor {

  private final PaginatedSegmentReader reader;
  private final PaginatedDimMetadata   meta;
  private final SegmentParameters      params;

  private int     currentBlock = -1;
  private int     currentInBlock;
  // Position of the posting the cursor sits on, held as components. The {@link RID} object is
  // materialised lazily by {@link #currentRid()} and cached in {@code currentRidObj}: a DAAT
  // traversal compares positions millions of times per query but reads the RID only for the handful
  // of documents that reach the result set, so allocating one per posting was both the bulk of the
  // cursor's garbage and an extra dereference on every comparison (issue #5467).
  private int     currentBucketId = -1;
  private long    currentPosition = -1L;
  private RID     currentRidObj;
  private float   currentWeight;
  private boolean currentTombstone;

  // Decoded postings of the current block, held as parallel primitive arrays. An earlier version
  // materialised a RID per posting at decode time; on a learned-sparse query that is millions of
  // short-lived objects per query, and it dominated the traversal cost (issue #5388). Only the
  // posting the cursor is actually positioned on ever becomes a RID.
  private final int[]   blockBuckets;
  private final long[]  blockPositions;
  private final float[] blockWeights;
  private final boolean[] blockTombstones;
  private int           blockSize;
  private boolean       blockDecoded;
  private boolean       exhausted;

  // Lazy-decode state for Block-Max WAND skips (issue #5388). When {@code pendingDecode} is true
  // the cursor is parked at the FIRST posting of {@code currentBlock} ({@code currentInBlock == 0},
  // position == the current block's first RID) without the block payload having
  // been read. {@link #currentWeight()} / {@link #isTombstone()} and any navigation resolve it on
  // demand. This lets a block-max skip hop from block boundary to block boundary consulting only
  // in-memory headers, decoding a payload solely when a document is actually scored - which is
  // what turns the pruning from "skips scoring" into "skips decoding", the cost the reporter sees.
  private boolean       pendingDecode;

  // Memo for {@link #blockContaining}: the block index returned for {@code shallowRid}. Block-max
  // probes arrive in non-decreasing RID order during a DAAT traversal, so resuming the header walk
  // from here makes it amortised O(blocks) per query instead of O(blocks) per probe.
  private int           shallowBlock = -1;
  private int           shallowBucketId = -1;
  private long          shallowPosition;

  // Per-cursor count of block payloads actually decoded (page reads). Bumped once per
  // {@link #decodeBlockIfNeeded} that hits the wire. This is the real cost signal BMW pruning
  // drives down: block-max checks read only in-memory headers and never touch this counter.
  // Per-cursor (per-query) state, so it is contention-free even under concurrent queries.
  private long          decodedBlockCount;

  // Per-cursor count of payload bytes copied out of the page cache by those decodes. Tracked
  // separately from {@link #decodedBlockCount} because the two are not proportional: the reader used
  // to hand back every byte from the block payload to the end of the page regardless of how long the
  // block actually was, so a ~500-byte block cost a ~32 KB copy on a 64 KB page. Asserting on this
  // counter is what keeps that from coming back (issue #5388).
  private long          decodedPayloadBytes;

  // Required scratch capacity (one full page of payload). The byte[] / ByteBuffer pair is
  // borrowed from {@link com.arcadedb.database.DatabaseContext.DatabaseContextTL#getTemporaryBuffer1()}
  // inside {@link #decodeBlockIfNeeded} - the same per-thread Binary that BinarySerializer and
  // friends already share. Decoded values land in this cursor's
  // {@code blockBuckets} / {@code blockPositions} / {@code blockWeights} / {@code blockTombstones} arrays before the call returns, so the
  // buffer can be clobbered by any subsequent caller on the same thread without affecting us
  // (issue #4086).
  private final int scratchSize;

  PaginatedSegmentDimCursor(final PaginatedSegmentReader reader, final PaginatedDimMetadata meta) {
    this.reader = reader;
    this.meta = meta;
    this.params = reader.parameters();
    this.blockBuckets = new int[params.blockSize()];
    this.blockPositions = new long[params.blockSize()];
    this.blockWeights = new float[params.blockSize()];
    this.blockTombstones = new boolean[params.blockSize()];
    this.scratchSize = reader.component().pageContentSize();
  }

  public int dimId() {
    return meta.dimId();
  }

  public PaginatedDimMetadata metadata() {
    return meta;
  }

  @Override
  public boolean isExhausted() {
    return exhausted;
  }

  @Override
  public RID currentRid() {
    if (currentRidObj == null && currentBucketId >= 0)
      currentRidObj = new RID(currentBucketId, currentPosition);
    return currentRidObj;
  }

  @Override
  public int currentBucketId() {
    return currentBucketId;
  }

  @Override
  public long currentPosition() {
    return currentPosition;
  }

  /** Set the position from components, dropping any cached {@link RID} for the previous one. */
  private void setPosition(final int bucketId, final long position) {
    currentBucketId = bucketId;
    currentPosition = position;
    currentRidObj = null;
  }

  /** Set the position from an existing {@link RID}, reusing it as the cached object. */
  private void setPosition(final RID rid) {
    if (rid == null) {
      clearPosition();
      return;
    }
    currentBucketId = rid.getBucketId();
    currentPosition = rid.getPosition();
    currentRidObj = rid;
  }

  private void clearPosition() {
    currentBucketId = -1;
    currentPosition = -1L;
    currentRidObj = null;
  }

  @Override
  public float currentWeight() {
    resolvePendingDecode();
    return currentWeight;
  }

  @Override
  public boolean isTombstone() {
    resolvePendingDecode();
    return currentTombstone;
  }

  @Override
  public float upperBoundRemaining() {
    if (exhausted)
      return 0.0f;
    if (currentBlock < 0)
      return meta.globalMaxWeight();
    final int skipEntries = meta.skipCount();
    if (skipEntries == 0)
      return meta.blockMaxWeight(currentBlock);
    int idx = currentBlock / params.skipStride();
    if (idx >= skipEntries)
      idx = skipEntries - 1;
    return meta.skipMaxWeightToEnd(idx);
  }

  @Override
  public long documentFrequency() {
    return meta.df();
  }

  @Override
  public float blockMaxAt(final RID rid) {
    return blockMaxAt(rid.getBucketId(), rid.getPosition());
  }

  @Override
  public float blockMaxAt(final int bucketId, final long position) {
    if (exhausted)
      return 0.0f;
    final int b = blockContaining(bucketId, position);
    if (b >= meta.blockCount())
      return 0.0f;
    return meta.blockMaxWeight(b);
  }

  @Override
  public RID blockEndAt(final RID rid) {
    return blockEndAt(rid.getBucketId(), rid.getPosition());
  }

  @Override
  public RID blockEndAt(final int bucketId, final long position) {
    if (exhausted)
      return null;
    final int b = blockContaining(bucketId, position);
    if (b >= meta.blockCount())
      return null;
    return new RID(meta.blockLastBucketId(b), meta.blockLastPosition(b));
  }

  /**
   * Index of the first block at or after the current position whose {@code lastRid} is &gt;=
   * {@code rid}, i.e. the block that would contain {@code rid} (or the block immediately after a
   * gap that swallows it). Reads only in-memory block headers - no page decode. Returns
   * {@link PaginatedDimMetadata#blockCount()} when every remaining block ends before {@code rid}.
   * <p>
   * The scan starts from {@code currentBlock} and only moves forward. The result of the previous
   * call is memoised in {@code shallowBlock} and reused whenever the new {@code rid} is at or after
   * the previous one, which is the access pattern of a DAAT traversal (candidates only move
   * forward). Without that memo the scan restarts from {@code currentBlock} on every probe and, for
   * a cursor that is being skipped over rather than advanced, degrades into a repeated walk of the
   * whole header array - the per-query overhead reported on the memtable/unsettled path of issue
   * #5388. The memo is bypassed for an out-of-order (smaller) {@code rid} so an over-tight bound can
   * never be returned.
   */
  private int blockContaining(final int bucketId, final long position) {
    int b = currentBlock < 0 ? 0 : currentBlock;
    if (shallowBucketId >= 0 && shallowBlock > b
        && SparseSegmentBuilder.compareRid(bucketId, position, shallowBucketId, shallowPosition) >= 0)
      b = shallowBlock;
    final int total = meta.blockCount();
    while (b < total
        && SparseSegmentBuilder.compareRid(meta.blockLastBucketId(b), meta.blockLastPosition(b), bucketId, position) < 0)
      b++;
    shallowBlock = b;
    shallowBucketId = bucketId;
    shallowPosition = position;
    return b;
  }

  /** Test/observability hook: total block payloads decoded by this cursor since construction. */
  public long decodedBlockCount() {
    return decodedBlockCount;
  }

  /** Test/observability hook: total payload bytes copied out of the page cache by those decodes. */
  public long decodedPayloadBytes() {
    return decodedPayloadBytes;
  }

  @Override
  public void start() throws IOException {
    if (exhausted)
      return;
    if (meta.blockCount() == 0) {
      exhausted = true;
      clearPosition();
      return;
    }
    positionAtBlock(0);
    decodeBlockIfNeeded();
    materializePosting(0);
  }

  @Override
  public boolean advance() throws IOException {
    if (exhausted)
      return false;
    if (currentBlock < 0) {
      start();
      return !exhausted;
    }
    // A lazily-parked block start represents the posting at index 0; resolve it so we advance off
    // the real posting rather than re-parking, and so blockSize is known.
    resolvePendingDecode();
    if (currentInBlock + 1 < blockSize) {
      materializePosting(currentInBlock + 1);
      return true;
    }
    if (currentBlock + 1 >= meta.blockCount()) {
      exhausted = true;
      // Drop the position too: an exhausted cursor reports none. The traversal navigates on the
      // primitive accessors, so leaving the last posting behind here would let a stale key survive
      // in a comparison (issue #5467).
      clearPosition();
      return false;
    }
    positionAtBlock(currentBlock + 1);
    decodeBlockIfNeeded();
    materializePosting(0);
    return true;
  }

  @Override
  public boolean seekTo(final RID target) throws IOException {
    return seekTo(target.getBucketId(), target.getPosition());
  }

  @Override
  public boolean seekTo(final int targetBucketId, final long targetPosition) throws IOException {
    if (exhausted)
      return false;
    if (currentBucketId >= 0
        && SparseSegmentBuilder.compareRid(currentBucketId, currentPosition, targetBucketId, targetPosition) >= 0)
      return true;

    final int oldBlock = currentBlock;
    final int oldInBlock = currentInBlock;
    int targetBlock = currentBlock < 0 ? 0 : currentBlock;
    final int total = meta.blockCount();
    // Use the per-dim skip list to jump close to {@code target} before the linear refinement,
    // dropping the cost from O(blocks) to O(log skip_entries + skipStride). At default settings
    // (blockSize=128, skipStride=8) this turns a 1M-posting cursor's seek from a ~7,800-block
    // walk into a ~10-comparison binary search plus an &lt;= 8-block linear scan.
    // Fast path: probes on a dense posting list arrive close to where the cursor already sits, so
    // check the current block before paying for the skip-list search. Without this, every
    // non-essential probe of a MaxScore traversal ran a binary search over the whole skip list to
    // discover it had not moved (issue #5467).
    final boolean beyondCurrentBlock = targetBlock >= total || SparseSegmentBuilder.compareRid(
        meta.blockLastBucketId(targetBlock), meta.blockLastPosition(targetBlock), targetBucketId, targetPosition) < 0;
    final int skipEntries = meta.skipCount();
    if (beyondCurrentBlock && skipEntries > 0) {
      // Binary search for the largest skip entry whose firstRid &lt;= target. Its blockIndex is
      // the start of the stride that contains (or immediately precedes) target; the linear loop
      // below then refines within that stride.
      int lo = 0;
      int hi = skipEntries - 1;
      int found = -1;
      while (lo <= hi) {
        final int mid = (lo + hi) >>> 1;
        if (SparseSegmentBuilder.compareRid(meta.skipFirstBucketId(mid), meta.skipFirstPosition(mid), targetBucketId,
            targetPosition) <= 0) {
          found = mid;
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      if (found >= 0 && meta.skipBlockIndex(found) > targetBlock)
        targetBlock = meta.skipBlockIndex(found);
    }
    while (targetBlock < total && SparseSegmentBuilder.compareRid(meta.blockLastBucketId(targetBlock),
        meta.blockLastPosition(targetBlock), targetBucketId, targetPosition) < 0)
      targetBlock++;
    if (targetBlock >= total) {
      exhausted = true;
      clearPosition();
      return false;
    }

    // Block-Max WAND fast path (issue #5388): when the target falls at or before this block's first
    // posting the seek lands exactly on a block boundary, so park there without reading the payload.
    // A subsequent skip re-parks off headers alone; the block is decoded only if it is scored.
    final int firstBucketId = meta.blockFirstBucketId(targetBlock);
    final long firstPosition = meta.blockFirstPosition(targetBlock);
    if (SparseSegmentBuilder.compareRid(firstBucketId, firstPosition, targetBucketId, targetPosition) >= 0) {
      positionAtBlock(targetBlock);
      currentInBlock = 0;
      setPosition(firstBucketId, firstPosition);
      pendingDecode = true;
      return true;
    }

    if (targetBlock != currentBlock) {
      positionAtBlock(targetBlock);
      decodeBlockIfNeeded();
    } else if (!blockDecoded) {
      decodeBlockIfNeeded();
    }

    final int startIdx = oldBlock == targetBlock ? Math.max(0, oldInBlock) : 0;
    for (int i = startIdx; i < blockSize; i++) {
      if (SparseSegmentBuilder.compareRid(blockBuckets[i], blockPositions[i], targetBucketId, targetPosition) >= 0) {
        materializePosting(i);
        return true;
      }
    }

    if (targetBlock + 1 >= total) {
      exhausted = true;
      clearPosition();
      return false;
    }
    positionAtBlock(targetBlock + 1);
    decodeBlockIfNeeded();
    materializePosting(0);
    return true;
  }

  @Override
  public void close() {
    exhausted = true;
    clearPosition();
    pendingDecode = false;
  }

  // --- internals ------------------------------------------------------------

  /**
   * Materialise a block start that was parked without decoding (see {@code pendingDecode}). Decodes
   * the payload now and re-materialises posting 0. Called on the first read of a weight/tombstone or
   * on any navigation off a parked block. Wraps the checked {@link IOException} because the
   * {@link SourceCursor} value getters it backs are not declared to throw.
   */
  private void resolvePendingDecode() {
    if (!pendingDecode)
      return;
    pendingDecode = false;
    try {
      decodeBlockIfNeeded();
    } catch (final IOException e) {
      throw new RuntimeException("failed to decode sparse-vector block " + currentBlock + " of dim " + meta.dimId(), e);
    }
    materializePosting(0);
  }

  private void positionAtBlock(final int block) {
    if (block != currentBlock) {
      currentBlock = block;
      blockDecoded = false;
      blockSize = 0;
    }
  }

  private void decodeBlockIfNeeded() throws IOException {
    if (blockDecoded)
      return;
    final int pageNum = meta.blockPageNum(currentBlock);
    final int offsetInPage = meta.blockOffset(currentBlock);
    final int blockPostingCount = meta.blockPostingCount(currentBlock);
    // {@code getTemporaryBuffer1} returns a cleared per-thread {@link Binary}; {@code size(int)}
    // grows the underlying byte[] to {@code scratchSize} on first sparse-vector decode and is a
    // no-op on subsequent calls. The default Binary uses big-endian byte order, matching the
    // segment format.
    final Binary scratch = reader.component().getDatabase().getContext().getTemporaryBuffer1();
    scratch.size(scratchSize);
    final ByteBuffer buf = reader.readBlockPayloadInto(pageNum, offsetInPage, payloadLengthBound(currentBlock, blockPostingCount),
        scratch.getContent(), scratch.getByteBuffer());
    decodedBlockCount++;
    decodedPayloadBytes += buf.limit();

    final int n = blockPostingCount;
    blockSize = n;
    blockBuckets[0] = meta.blockFirstBucketId(currentBlock);
    blockPositions[0] = meta.blockFirstPosition(currentBlock);

    if (params.ridCompression() == RidCompression.VARINT_DELTA) {
      int prevBucket = blockBuckets[0];
      long prevPosition = blockPositions[0];
      for (int i = 1; i < n; i++) {
        final long bucketDelta = VarInt.readUnsignedVarLong(buf);
        final long secondField = VarInt.readUnsignedVarLong(buf);
        if (bucketDelta == 0L) {
          prevPosition += secondField;
        } else {
          prevBucket += (int) bucketDelta;
          prevPosition = secondField;
        }
        blockBuckets[i] = prevBucket;
        blockPositions[i] = prevPosition;
      }
    } else {
      for (int i = 1; i < n; i++) {
        blockBuckets[i] = buf.getInt();
        blockPositions[i] = buf.getLong();
      }
    }

    final WeightQuantization wq = params.weightQuantization();
    final float weightMin = meta.blockWeightMin(currentBlock);
    final float weightMax = meta.blockWeightMax(currentBlock);
    for (int i = 0; i < n; i++) {
      switch (wq) {
        case INT8 -> {
          final byte b = buf.get();
          if (b == SegmentFormat.INT8_TOMBSTONE_SENTINEL) {
            blockTombstones[i] = true;
            blockWeights[i] = Float.NaN;
          } else {
            blockTombstones[i] = false;
            blockWeights[i] = WeightCodec.dequantizeInt8(b, weightMin, weightMax);
          }
        }
        case FP16 -> {
          final short s = buf.getShort();
          if (s == SegmentFormat.FP16_TOMBSTONE_SENTINEL) {
            blockTombstones[i] = true;
            blockWeights[i] = Float.NaN;
          } else {
            blockTombstones[i] = false;
            blockWeights[i] = WeightCodec.fromFp16(s);
          }
        }
        case FP32 -> {
          final int bits = buf.getInt();
          if (WeightCodec.isFp32Tombstone(bits)) {
            blockTombstones[i] = true;
            blockWeights[i] = Float.NaN;
          } else {
            blockTombstones[i] = false;
            blockWeights[i] = Float.intBitsToFloat(bits);
          }
        }
      }
    }

    blockDecoded = true;
  }

  /**
   * Upper bound on the payload bytes of {@code block}, used to keep the page-to-scratch copy down to
   * the block instead of the rest of the page (issue #5388).
   * <p>
   * Two independent bounds, whichever is tighter:
   * <ul>
   *   <li>the format's worst case for {@code postingCount} postings at this segment's quantization,
   *       which always holds;</li>
   *   <li>the gap to the next block of this dim when that block sits on the same page. The builder
   *       writes a dim's blocks back to back (header immediately followed by payload, then the next
   *       block), so on the common path this gap <i>is</i> the exact payload length. It is only
   *       taken when positive, so a build path that ever lays blocks out non-contiguously falls back
   *       to the worst case rather than truncating a payload.</li>
   * </ul>
   * The last block of a dim on a page has no next-block gap and rides on the worst case alone; the
   * reader clamps whatever comes back to the bytes actually left on the page.
   */
  private int payloadLengthBound(final int block, final int postingCount) {
    int bound = SegmentFormat.maxBlockPayloadSize(postingCount, params.weightQuantization());
    if (block + 1 < meta.blockCount() && meta.blockPageNum(block + 1) == meta.blockPageNum(block)) {
      final int gap = meta.blockOffset(block + 1) - meta.blockOffset(block) - SegmentFormat.BLOCK_HEADER_SIZE;
      if (gap > 0 && gap < bound)
        bound = gap;
    }
    return bound;
  }

  private void materializePosting(final int idxInBlock) {
    pendingDecode = false;
    currentInBlock = idxInBlock;
    setPosition(blockBuckets[idxInBlock], blockPositions[idxInBlock]);
    currentWeight = blockWeights[idxInBlock];
    currentTombstone = blockTombstones[idxInBlock];
  }
}
