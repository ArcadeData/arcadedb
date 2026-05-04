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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Streaming writer for {@code .sparseseg} sealed segment files.
 * <p>
 * Postings must arrive in {@code (dim_id asc, RID asc)} order. The writer:
 * <ol>
 *   <li>Buffers up to {@link SegmentParameters#blockSize()} postings per block in memory.</li>
 *   <li>On block fill or {@link #endDim()}, computes block stats, compresses, and flushes
 *       the block to disk; remembers the block file offset and header for the dim trailer.</li>
 *   <li>On {@link #endDim()}, writes the dim trailer (header + block_offsets + skip_list)
 *       and records the dim_index entry.</li>
 *   <li>On {@link #finish()}, writes the dim_index, the manifest, then back-patches the file
 *       header with manifest_offset / total_postings / total_dims and an updated CRC.</li>
 * </ol>
 * All numeric fields are little-endian. RIDs use VarInt-delta within a block; the first RID
 * of a block is stored raw (in the block header) so that block-skip can land at the start of
 * any block without decoding a prior block.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SparseSegmentWriter implements AutoCloseable {

  private static final int BLOCK_PAYLOAD_INITIAL_CAPACITY = 4096;

  private final Path              file;
  private final SegmentParameters params;
  private final FileChannel       channel;

  // File header back-patched at finish(). Kept in memory so total_postings and crc can be set.
  private final ByteBuffer headerBuffer;

  // Posting region cursor.
  private long fileOffset;

  // Currently-open dim state. -1 == no dim open.
  private int            currentDimId      = -1;
  private long           currentDimPostingCount;
  private float          currentDimMaxWeight;
  private boolean        currentDimHasTombstones;
  private final List<Long>        currentDimBlockOffsets = new ArrayList<>();
  private final List<BlockHeader> currentDimBlockHeaders = new ArrayList<>();

  // Block buffering.
  private final RID[]   blockRids;
  private final float[] blockWeights;
  private final boolean[] blockTombstones;
  private int           blockCursor;

  // dim_index entries: (dim_id, dim_header_offset).
  private final List<long[]> dimIndex = new ArrayList<>();

  // Aggregate counters.
  private long totalPostings;

  // Manifest fields.
  private long   segmentId            = 0L;
  private long[] parentSegments       = new long[0];
  private long   tombstoneFloorSegment = 0L;

  private boolean finished;
  private boolean closed;

  public SparseSegmentWriter(final Path file, final SegmentParameters params) throws IOException {
    this.file = file;
    this.params = params;
    this.channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
    this.headerBuffer = ByteBuffer.allocate(SegmentFormat.HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    this.blockRids = new RID[params.blockSize()];
    this.blockWeights = new float[params.blockSize()];
    this.blockTombstones = new boolean[params.blockSize()];
    writeFileHeaderPlaceholder();
    this.fileOffset = pageAlign(SegmentFormat.HEADER_SIZE);
    channel.position(this.fileOffset);
  }

  public void setSegmentId(final long segmentId) {
    if (currentDimId != -1 || !dimIndex.isEmpty())
      throw new IllegalStateException("setSegmentId must be called before any dim is written");
    this.segmentId = segmentId;
  }

  public void setParentSegments(final long[] parents) {
    if (currentDimId != -1 || !dimIndex.isEmpty())
      throw new IllegalStateException("setParentSegments must be called before any dim is written");
    this.parentSegments = parents.clone();
  }

  public void setTombstoneFloorSegment(final long floor) {
    if (currentDimId != -1 || !dimIndex.isEmpty())
      throw new IllegalStateException("setTombstoneFloorSegment must be called before any dim is written");
    this.tombstoneFloorSegment = floor;
  }

  public void startDim(final int dimId) {
    if (closed)
      throw new IllegalStateException("writer is closed");
    if (finished)
      throw new IllegalStateException("writer is finished");
    if (currentDimId != -1)
      throw new IllegalStateException("dim " + currentDimId + " is open; call endDim() first");
    if (!dimIndex.isEmpty()) {
      final int lastDim = (int) dimIndex.getLast()[0];
      if (dimId <= lastDim)
        throw new IllegalArgumentException("dims must arrive in strictly ascending order: got " + dimId + " after " + lastDim);
    }
    this.currentDimId = dimId;
    this.currentDimPostingCount = 0L;
    this.currentDimMaxWeight = Float.NEGATIVE_INFINITY;
    this.currentDimHasTombstones = false;
    this.currentDimBlockOffsets.clear();
    this.currentDimBlockHeaders.clear();
    this.blockCursor = 0;
  }

  public void appendPosting(final RID rid, final float weight) throws IOException {
    if (currentDimId == -1)
      throw new IllegalStateException("call startDim before appendPosting");
    if (Float.isNaN(weight) || Float.isInfinite(weight))
      throw new IllegalArgumentException("weight must be a finite number: " + weight);
    if (weight < 0.0f)
      throw new IllegalArgumentException("weight must be non-negative: " + weight);
    appendInternal(rid, weight, false);
  }

  public void appendTombstone(final RID rid) throws IOException {
    if (currentDimId == -1)
      throw new IllegalStateException("call startDim before appendTombstone");
    appendInternal(rid, Float.NaN, true);
  }

  private void appendInternal(final RID rid, final float weight, final boolean tombstone) throws IOException {
    if (rid == null)
      throw new IllegalArgumentException("rid must not be null");
    if (blockCursor > 0) {
      final RID prev = blockRids[blockCursor - 1];
      if (compareRid(rid, prev) <= 0)
        throw new IllegalArgumentException(
            "RIDs must arrive in strictly ascending order within a dim: got " + rid + " after " + prev);
    } else if (!currentDimBlockHeaders.isEmpty()) {
      final RID prev = currentDimBlockHeaders.getLast().lastRid();
      if (compareRid(rid, prev) <= 0)
        throw new IllegalArgumentException(
            "RIDs must arrive in strictly ascending order within a dim: got " + rid + " after " + prev);
    }
    blockRids[blockCursor] = rid;
    blockWeights[blockCursor] = weight;
    blockTombstones[blockCursor] = tombstone;
    blockCursor++;
    if (blockCursor == params.blockSize())
      flushBlock();
  }

  public void endDim() throws IOException {
    if (currentDimId == -1)
      throw new IllegalStateException("no dim open");
    if (blockCursor > 0)
      flushBlock();
    if (currentDimBlockHeaders.isEmpty()) {
      // Empty dim - nothing to write. Drop it silently.
      currentDimId = -1;
      return;
    }
    final long dimHeaderOffset = fileOffset;
    writeDimTrailer();
    dimIndex.add(new long[] { currentDimId, dimHeaderOffset });
    totalPostings += currentDimPostingCount;
    currentDimId = -1;
  }

  public void finish() throws IOException {
    if (finished)
      return;
    if (currentDimId != -1)
      throw new IllegalStateException("call endDim before finish");

    final long dimIndexOffset = fileOffset;
    writeDimIndex();
    final long manifestOffset = fileOffset;
    writeManifest(dimIndexOffset);

    // Back-patch the file header.
    headerBuffer.putLong(SegmentFormat.HEADER_OFFSET_MANIFEST_OFFSET, manifestOffset);
    headerBuffer.putLong(SegmentFormat.HEADER_OFFSET_TOTAL_POSTINGS, totalPostings);
    headerBuffer.putInt(SegmentFormat.HEADER_OFFSET_TOTAL_DIMS, dimIndex.size());
    headerBuffer.putLong(SegmentFormat.HEADER_OFFSET_CREATED_AT, System.currentTimeMillis());
    final int crc = computeHeaderCrc(headerBuffer);
    headerBuffer.putInt(SegmentFormat.HEADER_OFFSET_CRC32, crc);

    headerBuffer.position(0);
    headerBuffer.limit(SegmentFormat.HEADER_SIZE);
    channel.write(headerBuffer.duplicate(), 0L);

    channel.force(true);
    finished = true;
  }

  @Override
  public void close() throws IOException {
    if (closed)
      return;
    try {
      // Only auto-finish a clean writer state. If a dim is open (mid-stream or after an error)
      // we leave the file incomplete - any subsequent attempt to open it will fail header validation.
      if (!finished && currentDimId == -1)
        finish();
    } finally {
      channel.close();
      closed = true;
    }
  }

  public Path file() {
    return file;
  }

  // ---------- internals ----------

  private void writeFileHeaderPlaceholder() throws IOException {
    headerBuffer.clear();
    headerBuffer.putLong(SegmentFormat.HEADER_OFFSET_MAGIC, SegmentFormat.MAGIC);
    headerBuffer.putInt(SegmentFormat.HEADER_OFFSET_FORMAT_VERSION, SegmentFormat.FORMAT_VERSION);
    headerBuffer.putInt(SegmentFormat.HEADER_OFFSET_PAGE_SIZE, params.pageSize());
    headerBuffer.putInt(SegmentFormat.HEADER_OFFSET_BLOCK_SIZE, params.blockSize());
    headerBuffer.putInt(SegmentFormat.HEADER_OFFSET_SKIP_STRIDE, params.skipStride());
    headerBuffer.put(SegmentFormat.HEADER_OFFSET_WEIGHT_QUANTIZATION, params.weightQuantization().code());
    headerBuffer.put(SegmentFormat.HEADER_OFFSET_RID_COMPRESSION, params.ridCompression().code());
    // reserved (14B) left zero; manifest_offset / total_postings / total_dims / created_at / crc back-patched in finish().

    headerBuffer.position(0);
    headerBuffer.limit(SegmentFormat.HEADER_SIZE);
    channel.write(headerBuffer.duplicate(), 0L);

    // Pad page 0 with zeros to the page boundary so subsequent block writes start on a new page.
    final int padding = params.pageSize() - SegmentFormat.HEADER_SIZE;
    if (padding > 0) {
      final ByteBuffer zeros = ByteBuffer.allocate(padding);
      channel.write(zeros, SegmentFormat.HEADER_SIZE);
    }
  }

  private long pageAlign(final long offset) {
    final int ps = params.pageSize();
    final long rem = offset % ps;
    return rem == 0 ? offset : offset + (ps - rem);
  }

  private void flushBlock() throws IOException {
    if (blockCursor == 0)
      return;

    // Compute weight min/max excluding tombstones; if all-tombstone block, both are 0.
    float weightMin = Float.POSITIVE_INFINITY;
    float weightMax = Float.NEGATIVE_INFINITY;
    boolean hasTombstones = false;
    for (int i = 0; i < blockCursor; i++) {
      if (blockTombstones[i]) {
        hasTombstones = true;
        continue;
      }
      final float w = blockWeights[i];
      if (w < weightMin)
        weightMin = w;
      if (w > weightMax)
        weightMax = w;
    }
    if (weightMin == Float.POSITIVE_INFINITY) {
      weightMin = 0.0f;
      weightMax = 0.0f;
    }
    final float blockMaxForBmw = (weightMax == Float.NEGATIVE_INFINITY) ? 0.0f : weightMax;

    final ByteBuffer payload = ByteBuffer.allocate(estimateBlockPayloadSize()).order(ByteOrder.LITTLE_ENDIAN);

    // Header.
    putRid(payload, blockRids[0]);
    putRid(payload, blockRids[blockCursor - 1]);
    payload.putShort((short) blockCursor);
    payload.putFloat(blockMaxForBmw);
    payload.putFloat(weightMin);
    payload.putFloat(weightMax);
    payload.put((byte) (hasTombstones ? 1 : 0));
    payload.put((byte) 0); // reserved

    // Compressed RIDs: skip first (already in header), encode subsequent as VarInt deltas.
    // Block RIDs are sorted ascending by (bucketId, position), so bucketDelta >= 0; within a bucket
    // position deltas are positive; on a bucket change we store the new position raw.
    if (params.ridCompression() == RidCompression.VARINT_DELTA) {
      RID prev = blockRids[0];
      for (int i = 1; i < blockCursor; i++) {
        final RID curr = blockRids[i];
        final int bucketDelta = curr.getBucketId() - prev.getBucketId();
        VarInt.writeUnsignedVarLong(payload, bucketDelta);
        if (bucketDelta == 0) {
          VarInt.writeUnsignedVarLong(payload, curr.getPosition() - prev.getPosition());
        } else {
          VarInt.writeUnsignedVarLong(payload, curr.getPosition());
        }
        prev = curr;
      }
    } else {
      for (int i = 1; i < blockCursor; i++)
        putRid(payload, blockRids[i]);
    }

    // Compressed weights.
    switch (params.weightQuantization()) {
      case INT8 -> {
        for (int i = 0; i < blockCursor; i++) {
          if (blockTombstones[i]) {
            payload.put(SegmentFormat.INT8_TOMBSTONE_SENTINEL);
          } else {
            payload.put(WeightCodec.quantizeInt8(blockWeights[i], weightMin, weightMax));
          }
        }
      }
      case FP16 -> {
        for (int i = 0; i < blockCursor; i++) {
          if (blockTombstones[i]) {
            payload.putShort(SegmentFormat.FP16_TOMBSTONE_SENTINEL);
          } else {
            payload.putShort(WeightCodec.toFp16(blockWeights[i]));
          }
        }
      }
      case FP32 -> {
        for (int i = 0; i < blockCursor; i++) {
          if (blockTombstones[i]) {
            payload.putInt(WeightCodec.FP32_TOMBSTONE_BITS);
          } else {
            payload.putInt(WeightCodec.floatToTombstoneAwareBits(blockWeights[i]));
          }
        }
      }
    }

    payload.flip();

    // Block boundaries don't span pages: round up the start position if the block won't fit.
    final int payloadLen = payload.remaining();
    final long blockStart = ensureBlockFitsOnePage(payloadLen);
    channel.write(payload, blockStart);
    fileOffset = blockStart + payloadLen;

    final BlockHeader header = new BlockHeader(blockRids[0], blockRids[blockCursor - 1], blockCursor, blockMaxForBmw, weightMin,
        weightMax, hasTombstones);
    currentDimBlockOffsets.add(blockStart);
    currentDimBlockHeaders.add(header);
    currentDimPostingCount += blockCursor;
    if (blockMaxForBmw > currentDimMaxWeight)
      currentDimMaxWeight = blockMaxForBmw;
    if (hasTombstones)
      currentDimHasTombstones = true;
    blockCursor = 0;
  }

  private int estimateBlockPayloadSize() {
    final int base = SegmentFormat.BLOCK_HEADER_SIZE + params.blockSize() * VarInt.MAX_VARLONG_BYTES * 2;
    final int weightBytes = switch (params.weightQuantization()) {
      case INT8 -> params.blockSize();
      case FP16 -> params.blockSize() * 2;
      case FP32 -> params.blockSize() * 4;
    };
    return Math.max(BLOCK_PAYLOAD_INITIAL_CAPACITY, base + weightBytes);
  }

  /**
   * Returns the file offset at which a block of {@code payloadLen} bytes can start without spanning a page boundary.
   * If the current {@link #fileOffset} would split the block across pages, advance to the next page.
   */
  private long ensureBlockFitsOnePage(final int payloadLen) throws IOException {
    if (payloadLen > params.pageSize())
      throw new IOException("block payload " + payloadLen + " bytes exceeds page size " + params.pageSize()
          + "; reduce blockSize or increase pageSize");
    final int ps = params.pageSize();
    final long pageStart = (fileOffset / ps) * ps;
    final long pageEnd = pageStart + ps;
    if (fileOffset + payloadLen <= pageEnd)
      return fileOffset;
    // Pad current page with zeros to the next page boundary.
    final int padding = (int) (pageEnd - fileOffset);
    if (padding > 0) {
      final ByteBuffer zeros = ByteBuffer.allocate(padding);
      channel.write(zeros, fileOffset);
    }
    return pageEnd;
  }

  private void writeDimTrailer() throws IOException {
    final int blockCount = currentDimBlockHeaders.size();
    final int skipEntries = (blockCount + params.skipStride() - 1) / params.skipStride();

    // Compute max_weight_to_end values from the back.
    final float[] maxWeightToEnd = new float[skipEntries];
    float runningMax = Float.NEGATIVE_INFINITY;
    for (int b = blockCount - 1; b >= 0; b--) {
      final float bm = currentDimBlockHeaders.get(b).maxWeight();
      if (bm > runningMax)
        runningMax = bm;
      // If b is the start of a skip stride, record the running max.
      if (b % params.skipStride() == 0) {
        maxWeightToEnd[b / params.skipStride()] = runningMax;
      }
    }

    final int trailerSize =
        // dim_header
        4 + 4 + 4 + 4 + 4
            // block_offsets
            + blockCount * 8
            // skip_list
            + skipEntries * SegmentFormat.SKIP_ENTRY_SIZE;

    final ByteBuffer buf = ByteBuffer.allocate(trailerSize).order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(currentDimId);
    buf.putInt(blockCount);
    buf.putInt((int) currentDimPostingCount);
    buf.putInt((int) currentDimPostingCount); // df == posting_count for v2 segment-local df
    buf.putFloat(currentDimMaxWeight == Float.NEGATIVE_INFINITY ? 0.0f : currentDimMaxWeight);

    for (int b = 0; b < blockCount; b++)
      buf.putLong(currentDimBlockOffsets.get(b));

    for (int s = 0; s < skipEntries; s++) {
      final int firstBlock = s * params.skipStride();
      final BlockHeader bh = currentDimBlockHeaders.get(firstBlock);
      putRid(buf, bh.firstRid());
      buf.putFloat(maxWeightToEnd[s]);
      buf.putInt(firstBlock);
    }

    buf.flip();
    channel.write(buf, fileOffset);
    fileOffset += trailerSize;
  }

  private void writeDimIndex() throws IOException {
    // dim_index format: int count, then count * (int dim_id, long dim_header_offset).
    final int size = 4 + dimIndex.size() * (4 + 8);
    final ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(dimIndex.size());
    for (final long[] entry : dimIndex) {
      buf.putInt((int) entry[0]);
      buf.putLong(entry[1]);
    }
    buf.flip();
    channel.write(buf, fileOffset);
    fileOffset += size;
  }

  private void writeManifest(final long dimIndexOffset) throws IOException {
    // Manifest layout: dim_index_offset(8) segment_id(8) parent_count(4) parent_segments(parent_count * 8) tombstone_floor(8) reserved(8) crc32(4).
    final int parentCount = parentSegments.length;
    final int size = 8 + 8 + 4 + parentCount * 8 + 8 + 8 + 4;
    final ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    buf.putLong(dimIndexOffset);
    buf.putLong(segmentId);
    buf.putInt(parentCount);
    for (final long p : parentSegments)
      buf.putLong(p);
    buf.putLong(tombstoneFloorSegment);
    buf.putLong(0L); // reserved
    final CRC32 crc = new CRC32();
    crc.update(buf.array(), 0, size - 4);
    buf.putInt((int) crc.getValue());
    buf.flip();
    channel.write(buf, fileOffset);
    fileOffset += size;
  }

  static void putRid(final ByteBuffer buf, final RID rid) {
    buf.putInt(rid.getBucketId());
    buf.putLong(rid.getPosition());
  }

  static int compareRid(final RID a, final RID b) {
    final int b1 = a.getBucketId();
    final int b2 = b.getBucketId();
    if (b1 != b2)
      return Integer.compare(b1, b2);
    return Long.compare(a.getPosition(), b.getPosition());
  }

  static int computeHeaderCrc(final ByteBuffer header) {
    final CRC32 crc = new CRC32();
    crc.update(header.array(), 0, SegmentFormat.HEADER_OFFSET_CRC32);
    return (int) crc.getValue();
  }
}
