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
    return currentWeight;
  }

  @Override
  public boolean isTombstone() {
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

    if (targetBlock != currentBlock) {
      positionAtBlock(targetBlock);
      decodeBlockIfNeeded();
    } else if (!blockDecoded) {
      decodeBlockIfNeeded();
    }

    final int startIdx = (oldBlock == targetBlock) ? Math.max(0, oldInBlock) : 0;
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
  }

  // --- internals ------------------------------------------------------------

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
