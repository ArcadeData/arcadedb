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
  private RID     currentRid;
  private float   currentWeight;
  private boolean currentTombstone;

  private final RID[]   blockRids;
  private final float[] blockWeights;
  private final boolean[] blockTombstones;
  private int           blockSize;
  private boolean       blockDecoded;
  private boolean       exhausted;

  // Lazy-decode state for Block-Max WAND skips (issue #5388). When {@code pendingDecode} is true
  // the cursor is parked at the FIRST posting of {@code currentBlock} ({@code currentInBlock == 0},
  // {@code currentRid == blockHeader(currentBlock).firstRid()}) without the block payload having
  // been read. {@link #currentWeight()} / {@link #isTombstone()} and any navigation resolve it on
  // demand. This lets a block-max skip hop from block boundary to block boundary consulting only
  // in-memory headers, decoding a payload solely when a document is actually scored - which is
  // what turns the pruning from "skips scoring" into "skips decoding", the cost the reporter sees.
  private boolean       pendingDecode;

  // Per-cursor count of block payloads actually decoded (page reads). Bumped once per
  // {@link #decodeBlockIfNeeded} that hits the wire. This is the real cost signal BMW pruning
  // drives down: block-max checks read only in-memory headers and never touch this counter.
  // Per-cursor (per-query) state, so it is contention-free even under concurrent queries.
  private long          decodedBlockCount;

  // Required scratch capacity (one full page of payload). The byte[] / ByteBuffer pair is
  // borrowed from {@link com.arcadedb.database.DatabaseContext.DatabaseContextTL#getTemporaryBuffer1()}
  // inside {@link #decodeBlockIfNeeded} - the same per-thread Binary that BinarySerializer and
  // friends already share. Decoded values land in this cursor's {@code blockRids} /
  // {@code blockWeights} / {@code blockTombstones} arrays before the call returns, so the
  // buffer can be clobbered by any subsequent caller on the same thread without affecting us
  // (issue #4086).
  private final int scratchSize;

  PaginatedSegmentDimCursor(final PaginatedSegmentReader reader, final PaginatedDimMetadata meta) {
    this.reader = reader;
    this.meta = meta;
    this.params = reader.parameters();
    this.blockRids = new RID[params.blockSize()];
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
    return currentRid;
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
    final SkipEntry[] sl = meta.skipList();
    if (sl.length == 0)
      return meta.blockHeader(currentBlock).bmwUpperBound();
    int idx = currentBlock / params.skipStride();
    if (idx >= sl.length)
      idx = sl.length - 1;
    return sl[idx].maxWeightToEnd();
  }

  @Override
  public float blockMaxAt(final RID rid) {
    if (exhausted)
      return 0.0f;
    final int b = blockContaining(rid);
    if (b >= meta.blockCount())
      return 0.0f;
    return meta.blockHeader(b).bmwUpperBound();
  }

  @Override
  public RID blockEndAt(final RID rid) {
    if (exhausted)
      return null;
    final int b = blockContaining(rid);
    if (b >= meta.blockCount())
      return null;
    return meta.blockHeader(b).lastRid();
  }

  /**
   * Index of the first block at or after the current position whose {@code lastRid} is &gt;=
   * {@code rid}, i.e. the block that would contain {@code rid} (or the block immediately after a
   * gap that swallows it). Reads only in-memory block headers - no page decode. Returns
   * {@link PaginatedDimMetadata#blockCount()} when every remaining block ends before {@code rid}.
   * <p>
   * The scan starts from {@code currentBlock} and only moves forward, so across a query it is
   * amortised against the cursor's own forward progress; the common case (the pivot RID sits in
   * the current or next block) is O(1).
   */
  private int blockContaining(final RID rid) {
    int b = currentBlock < 0 ? 0 : currentBlock;
    final int total = meta.blockCount();
    while (b < total && SparseSegmentBuilder.compareRid(meta.blockHeader(b).lastRid(), rid) < 0)
      b++;
    return b;
  }

  /** Test/observability hook: total block payloads decoded by this cursor since construction. */
  public long decodedBlockCount() {
    return decodedBlockCount;
  }

  @Override
  public void start() throws IOException {
    if (exhausted)
      return;
    if (meta.blockCount() == 0) {
      exhausted = true;
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
      return false;
    }
    positionAtBlock(currentBlock + 1);
    decodeBlockIfNeeded();
    materializePosting(0);
    return true;
  }

  @Override
  public boolean seekTo(final RID target) throws IOException {
    if (exhausted)
      return false;
    if (currentRid != null && SparseSegmentBuilder.compareRid(currentRid, target) >= 0)
      return true;

    final int oldBlock = currentBlock;
    final int oldInBlock = currentInBlock;
    int targetBlock = currentBlock < 0 ? 0 : currentBlock;
    final int total = meta.blockCount();
    // Use the per-dim skip list to jump close to {@code target} before the linear refinement,
    // dropping the cost from O(blocks) to O(log skip_entries + skipStride). At default settings
    // (blockSize=128, skipStride=8) this turns a 1M-posting cursor's seek from a ~7,800-block
    // walk into a ~10-comparison binary search plus an &lt;= 8-block linear scan.
    final SkipEntry[] sl = meta.skipList();
    if (sl.length > 0) {
      // Binary search for the largest skip entry whose firstRid &lt;= target. Its blockIndex is
      // the start of the stride that contains (or immediately precedes) target; the linear loop
      // below then refines within that stride.
      int lo = 0;
      int hi = sl.length - 1;
      int found = -1;
      while (lo <= hi) {
        final int mid = (lo + hi) >>> 1;
        if (SparseSegmentBuilder.compareRid(sl[mid].firstRid(), target) <= 0) {
          found = mid;
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      if (found >= 0 && sl[found].blockIndex() > targetBlock)
        targetBlock = sl[found].blockIndex();
    }
    while (targetBlock < total
        && SparseSegmentBuilder.compareRid(meta.blockHeader(targetBlock).lastRid(), target) < 0)
      targetBlock++;
    if (targetBlock >= total) {
      exhausted = true;
      currentRid = null;
      return false;
    }

    // Block-Max WAND fast path (issue #5388): when the target falls at or before this block's first
    // posting the seek lands exactly on a block boundary, so park there without reading the payload.
    // A subsequent skip re-parks off headers alone; the block is decoded only if it is scored.
    final RID targetFirstRid = meta.blockHeader(targetBlock).firstRid();
    if (SparseSegmentBuilder.compareRid(targetFirstRid, target) >= 0) {
      positionAtBlock(targetBlock);
      currentInBlock = 0;
      currentRid = targetFirstRid;
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
      if (SparseSegmentBuilder.compareRid(blockRids[i], target) >= 0) {
        materializePosting(i);
        return true;
      }
    }

    if (targetBlock + 1 >= total) {
      exhausted = true;
      currentRid = null;
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
    currentRid = null;
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
    final BlockHeader bh = meta.blockHeader(currentBlock);
    // {@code getTemporaryBuffer1} returns a cleared per-thread {@link Binary}; {@code size(int)}
    // grows the underlying byte[] to {@code scratchSize} on first sparse-vector decode and is a
    // no-op on subsequent calls. The default Binary uses big-endian byte order, matching the
    // segment format.
    final Binary scratch = reader.component().getDatabase().getContext().getTemporaryBuffer1();
    scratch.size(scratchSize);
    final ByteBuffer buf = reader.readBlockPayloadInto(pageNum, offsetInPage, scratch.getContent(), scratch.getByteBuffer());
    decodedBlockCount++;

    final int n = bh.postingCount();
    blockSize = n;
    blockRids[0] = bh.firstRid();

    if (params.ridCompression() == RidCompression.VARINT_DELTA) {
      RID prev = bh.firstRid();
      for (int i = 1; i < n; i++) {
        final long bucketDelta = VarInt.readUnsignedVarLong(buf);
        final long secondField = VarInt.readUnsignedVarLong(buf);
        final RID curr;
        if (bucketDelta == 0L) {
          curr = new RID(prev.getBucketId(), prev.getPosition() + secondField);
        } else {
          curr = new RID(prev.getBucketId() + (int) bucketDelta, secondField);
        }
        blockRids[i] = curr;
        prev = curr;
      }
    } else {
      for (int i = 1; i < n; i++) {
        final int bucket = buf.getInt();
        final long pos = buf.getLong();
        blockRids[i] = new RID(bucket, pos);
      }
    }

    final WeightQuantization wq = params.weightQuantization();
    for (int i = 0; i < n; i++) {
      switch (wq) {
        case INT8 -> {
          final byte b = buf.get();
          if (b == SegmentFormat.INT8_TOMBSTONE_SENTINEL) {
            blockTombstones[i] = true;
            blockWeights[i] = Float.NaN;
          } else {
            blockTombstones[i] = false;
            blockWeights[i] = WeightCodec.dequantizeInt8(b, bh.weightMin(), bh.weightMax());
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

  private void materializePosting(final int idxInBlock) {
    pendingDecode = false;
    currentInBlock = idxInBlock;
    currentRid = blockRids[idxInBlock];
    currentWeight = blockWeights[idxInBlock];
    currentTombstone = blockTombstones[idxInBlock];
  }
}
