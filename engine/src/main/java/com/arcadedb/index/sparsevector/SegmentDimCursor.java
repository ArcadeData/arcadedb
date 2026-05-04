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
import com.arcadedb.index.sparsevector.SegmentFormat.RidCompression;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Forward cursor over the postings of a single dim within a sealed segment.
 * <p>
 * Iteration cycles between two states: <i>block header</i> (we know which block we're in but
 * haven't decoded its payload yet) and <i>posting</i> (we've decoded a specific posting). The
 * BMW skip path lives in {@link #seekTo(RID)} which uses the per-segment skip list to avoid
 * decompressing blocks that cannot beat the current threshold.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SegmentDimCursor implements SourceCursor {

  private final SparseSegmentReader reader;
  private final DimMetadata         meta;
  private final SegmentParameters   params;

  // Current posting state.
  private int     currentBlock = -1;       // -1 == not yet positioned
  private int     currentInBlock;          // index within the current block
  private RID     currentRid;
  private float   currentWeight;           // NaN for tombstone
  private boolean currentTombstone;

  // Decoded block buffer: rids and weights for the current block. Filled on demand.
  private final RID[]   blockRids;
  private final float[] blockWeights;
  private final boolean[] blockTombstones;
  private int           blockSize;         // postings actually decoded in current block
  private boolean       blockDecoded;
  private boolean       exhausted;

  SegmentDimCursor(final SparseSegmentReader reader, final DimMetadata meta) {
    this.reader = reader;
    this.meta = meta;
    this.params = reader.parameters();
    this.blockRids = new RID[params.blockSize()];
    this.blockWeights = new float[params.blockSize()];
    this.blockTombstones = new boolean[params.blockSize()];
  }

  public int dimId() {
    return meta.dimId();
  }

  public DimMetadata metadata() {
    return meta;
  }

  /** True once the cursor has consumed every posting and any further calls to {@link #advance()} will be no-ops. */
  public boolean isExhausted() {
    return exhausted;
  }

  public RID currentRid() {
    return currentRid;
  }

  public float currentWeight() {
    return currentWeight;
  }

  public boolean isTombstone() {
    return currentTombstone;
  }

  /**
   * Upper bound on the contribution of any remaining posting in this dim to a score. Looked up
   * via the skip list: each skip entry caches {@code max_weight_to_end} for the stride containing
   * its block. This is a (slight) overestimate when the current block is strictly inside a stride,
   * which is fine for BMW: pruning remains conservative-correct.
   */
  public float upperBoundRemaining() {
    if (exhausted)
      return 0.0f;
    if (currentBlock < 0)
      return meta.globalMaxWeight();
    final SkipEntry[] sl = meta.skipList();
    if (sl.length == 0)
      return meta.blockHeader(currentBlock).maxWeight();
    int idx = currentBlock / params.skipStride();
    if (idx >= sl.length)
      idx = sl.length - 1;
    return sl[idx].maxWeightToEnd();
  }

  /** Move to the first posting (block 0, posting 0). */
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

  /**
   * Advance to the next posting. Returns {@code false} when exhausted.
   */
  public boolean advance() throws IOException {
    if (exhausted)
      return false;
    if (currentBlock < 0) {
      start();
      return !exhausted;
    }
    if (currentInBlock + 1 < blockSize) {
      materializePosting(currentInBlock + 1);
      return true;
    }
    // Move to next block.
    if (currentBlock + 1 >= meta.blockCount()) {
      exhausted = true;
      return false;
    }
    positionAtBlock(currentBlock + 1);
    decodeBlockIfNeeded();
    materializePosting(0);
    return true;
  }

  /**
   * Seek the cursor forward to the first posting whose RID is >= {@code target}. Uses the
   * per-segment skip list to skip whole blocks. After the seek, {@link #currentRid()} is
   * either the first posting >= target or the cursor is exhausted.
   *
   * @return false if the cursor is exhausted (no posting >= target in this dim).
   */
  public boolean seekTo(final RID target) throws IOException {
    if (exhausted)
      return false;
    if (currentRid != null && SparseSegmentWriter.compareRid(currentRid, target) >= 0)
      return true;

    // Find the block whose [first_RID, last_RID] range contains target, or the next block if target is between blocks.
    final int oldBlock = currentBlock;
    final int oldInBlock = currentInBlock;
    int targetBlock = currentBlock < 0 ? 0 : currentBlock;
    final int total = meta.blockCount();
    while (targetBlock < total && SparseSegmentWriter.compareRid(meta.blockHeader(targetBlock).lastRid(), target) < 0)
      targetBlock++;
    if (targetBlock >= total) {
      exhausted = true;
      currentRid = null;
      return false;
    }

    if (targetBlock != currentBlock) {
      positionAtBlock(targetBlock);
      decodeBlockIfNeeded();
    } else if (!blockDecoded) {
      decodeBlockIfNeeded();
    }

    // Linear scan within the block (block size is small, default 128). When we stay in the same
    // block (oldBlock == targetBlock) we resume at oldInBlock to honour the forward-only contract;
    // crossing into a new block resets the in-block position.
    final int startIdx = (oldBlock == targetBlock) ? Math.max(0, oldInBlock) : 0;
    for (int i = startIdx; i < blockSize; i++) {
      if (SparseSegmentWriter.compareRid(blockRids[i], target) >= 0) {
        materializePosting(i);
        return true;
      }
    }

    // Block exhausted without finding >=target: advance to next block.
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

  /**
   * Returns the {@code max_weight_to_end} from the skip list at the cursor's current position,
   * or {@link Float#NEGATIVE_INFINITY} if the cursor isn't yet positioned.
   */
  public float skipListMaxWeightToEnd() {
    if (currentBlock < 0 || exhausted)
      return Float.NEGATIVE_INFINITY;
    final SkipEntry[] sl = meta.skipList();
    if (sl.length == 0)
      return meta.blockHeader(currentBlock).maxWeight();
    int idx = currentBlock / params.skipStride();
    if (idx >= sl.length)
      idx = sl.length - 1;
    return sl[idx].maxWeightToEnd();
  }

  @Override
  public void close() {
    exhausted = true;
    currentRid = null;
  }

  // ---------- internals ----------

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
    final long offset = meta.blockOffset(currentBlock);
    final BlockHeader bh = meta.blockHeader(currentBlock);
    final int payloadCap = reader.maxBlockPayloadSize();
    final ByteBuffer buf = reader.readBlockPayload(offset, payloadCap);

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
    currentInBlock = idxInBlock;
    currentRid = blockRids[idxInBlock];
    currentWeight = blockWeights[idxInBlock];
    currentTombstone = blockTombstones[idxInBlock];
  }
}
