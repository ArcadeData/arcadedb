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
import java.nio.ByteOrder;

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

  // Reusable decode scratch space, sized once to a full page payload. Decoding a block consumes
  // the buffer linearly (RIDs first, then weights), so we can reuse it across blocks and across
  // queries on this cursor without allocating per-block byte[] / ByteBuffer pairs. Cuts out the
  // dominant allocation in BMW-DAAT under high-fanout queries.
  // <p>
  // FUTURE: at the default 64 KiB page this is a 64 KiB allocation per cursor instance. A query
  // over D query dims fanning out across S segments allocates D * S of these (D=30, S=15 -&gt; ~28
  // MiB per query, reclaimed when the query finishes). Under concurrent load this is real heap
  // pressure. The natural fix is a thread-local or {@link java.util.concurrent.ConcurrentLinkedDeque}-backed
  // pool of {@code byte[pageContentSize]} buffers handed to cursors at {@link #start} and
  // returned at {@link #close}. Deferred until #4085 (parallel scoring) lands - parallel
  // dispatch multiplies the allocation rate by the parallelism factor, so the right time to
  // tackle this is alongside the dispatch wiring.
  private final byte[]     decodeScratch;
  private final ByteBuffer decodeView;

  PaginatedSegmentDimCursor(final PaginatedSegmentReader reader, final PaginatedDimMetadata meta) {
    this.reader = reader;
    this.meta = meta;
    this.params = reader.parameters();
    this.blockRids = new RID[params.blockSize()];
    this.blockWeights = new float[params.blockSize()];
    this.blockTombstones = new boolean[params.blockSize()];
    final int maxPayload = reader.component().pageContentSize();
    this.decodeScratch = new byte[maxPayload];
    this.decodeView = ByteBuffer.wrap(decodeScratch).order(ByteOrder.BIG_ENDIAN);
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
    final ByteBuffer buf = reader.readBlockPayloadInto(pageNum, offsetInPage, decodeScratch, decodeView);

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
