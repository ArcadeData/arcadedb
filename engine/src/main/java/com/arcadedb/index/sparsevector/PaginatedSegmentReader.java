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
import com.arcadedb.engine.BasePage;
import com.arcadedb.index.sparsevector.SegmentFormat.RidCompression;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * Reads sealed sparse segments stored as a {@link SparseSegmentComponent}. All disk I/O goes
 * through ArcadeDB's page cache via {@link SparseSegmentComponent#readPage}; cold reads incur a
 * single page-load, hot reads are memory-resident.
 * <p>
 * Per-segment metadata (segment id, parameters, dim_index, per-dim block locators, skip lists)
 * is parsed eagerly at open time and held in primitive parallel arrays. Cursor opens are
 * O(log n) via binary search with no further page reads on the metadata path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PaginatedSegmentReader implements AutoCloseable {

  private final SparseSegmentComponent component;
  private final SegmentParameters      params;
  private final long                   segmentId;
  private final long                   totalPostings;
  private final int                    totalDims;
  private final long[]                 parentSegments;
  private final long                   tombstoneFloorSegment;

  // Sorted parallel arrays: dimIds[i] -> trailer at (trailerPageNums[i], trailerOffsets[i] & 0xFFFF).
  private final int[]                  dimIds;
  private final int[]                  trailerPageNums;
  private final short[]                trailerOffsets;
  private final PaginatedDimMetadata[] dimCacheByPos;

  public PaginatedSegmentReader(final SparseSegmentComponent component) throws IOException {
    this.component = component;

    // ---- Page 0: header ----
    final BasePage headerPage = component.readPage(0);
    final byte[] headerBytes = new byte[PaginatedSegmentFormat.HEADER_SIZE];
    headerPage.readByteArray(0, headerBytes);
    final ByteBuffer header = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN);

    final long magic = header.getLong(PaginatedSegmentFormat.HEADER_OFFSET_MAGIC);
    if (magic != SegmentFormat.MAGIC)
      throw new IOException("Bad magic in segment '" + component.getName() + "': expected ASPV0001, got "
          + Long.toHexString(magic));

    final int formatVersion = header.getInt(PaginatedSegmentFormat.HEADER_OFFSET_FORMAT_VERSION);
    if (formatVersion != SegmentFormat.FORMAT_VERSION)
      throw new IOException("Unsupported format version in segment '" + component.getName() + "': " + formatVersion);

    final int storedCrc = header.getInt(PaginatedSegmentFormat.HEADER_OFFSET_CRC32);
    final CRC32 crc = new CRC32();
    crc.update(headerBytes, 0, PaginatedSegmentFormat.HEADER_OFFSET_CRC32);
    if ((int) crc.getValue() != storedCrc)
      throw new IOException("Header CRC mismatch in segment '" + component.getName() + "' (file truncated or corrupt)");

    final int blockSize = header.getInt(PaginatedSegmentFormat.HEADER_OFFSET_BLOCK_SIZE);
    final int skipStride = header.getInt(PaginatedSegmentFormat.HEADER_OFFSET_SKIP_STRIDE);
    final WeightQuantization wq = WeightQuantization
        .fromCode(header.get(PaginatedSegmentFormat.HEADER_OFFSET_WEIGHT_QUANTIZATION));
    final RidCompression rc = RidCompression
        .fromCode(header.get(PaginatedSegmentFormat.HEADER_OFFSET_RID_COMPRESSION));

    this.params = SegmentParameters.builder()
        .pageSize(component.getPageSize())
        .blockSize(blockSize)
        .skipStride(skipStride)
        .weightQuantization(wq)
        .ridCompression(rc)
        .build();
    this.segmentId = header.getLong(PaginatedSegmentFormat.HEADER_OFFSET_SEGMENT_ID);
    this.totalPostings = header.getLong(PaginatedSegmentFormat.HEADER_OFFSET_TOTAL_POSTINGS);
    this.totalDims = header.getInt(PaginatedSegmentFormat.HEADER_OFFSET_TOTAL_DIMS);
    final int manifestPageNum = header.getInt(PaginatedSegmentFormat.HEADER_OFFSET_MANIFEST_PAGE_NUM);
    final int dimIndexPageNum = header.getInt(PaginatedSegmentFormat.HEADER_OFFSET_DIM_INDEX_PAGE_NUM);

    // ---- Manifest page ----
    final BasePage manifestPage = component.readPage(manifestPageNum);
    final long manifestSegId = manifestPage.readLong(0);
    if (manifestSegId != segmentId)
      throw new IOException("Manifest segment_id (" + manifestSegId + ") does not match header (" + segmentId + ")");
    final int parentCount = manifestPage.readInt(8);
    if (parentCount < 0 || parentCount > 1_000_000)
      throw new IOException("Implausible parent_segments count in manifest of '" + component.getName() + "': " + parentCount);
    this.parentSegments = new long[parentCount];
    int cursor = 12;
    for (int i = 0; i < parentCount; i++) {
      this.parentSegments[i] = manifestPage.readLong(cursor);
      cursor += 8;
    }
    this.tombstoneFloorSegment = manifestPage.readLong(cursor);
    // reserved(8) + crc32(4) follow; not validated on read for now (CRC could be added if profiling shows it's worthwhile).

    // ---- Dim index page ----
    final BasePage dimIndexPage = component.readPage(dimIndexPageNum);
    final int n = dimIndexPage.readInt(0);
    if (n != totalDims)
      throw new IOException("Inconsistent dim count in '" + component.getName() + "': header says " + totalDims
          + ", dim_index says " + n);
    this.dimIds = new int[n];
    this.trailerPageNums = new int[n];
    this.trailerOffsets = new short[n];
    this.dimCacheByPos = new PaginatedDimMetadata[n];
    int prevDim = Integer.MIN_VALUE;
    int p = 4;
    for (int i = 0; i < n; i++) {
      final int dimId = dimIndexPage.readInt(p);
      p += 4;
      final int trailerPage = dimIndexPage.readInt(p);
      p += 4;
      final short trailerOffset = dimIndexPage.readShort(p);
      p += 2;
      if (dimId <= prevDim)
        throw new IOException("dim_index not sorted in '" + component.getName() + "': " + dimId + " after " + prevDim);
      prevDim = dimId;
      this.dimIds[i] = dimId;
      this.trailerPageNums[i] = trailerPage;
      this.trailerOffsets[i] = trailerOffset;
    }
  }

  public SegmentParameters parameters() {
    return params;
  }

  public long totalPostings() {
    return totalPostings;
  }

  public int totalDims() {
    return totalDims;
  }

  public long segmentId() {
    return segmentId;
  }

  public long[] parentSegments() {
    return parentSegments.clone();
  }

  public long tombstoneFloorSegment() {
    return tombstoneFloorSegment;
  }

  public boolean hasDim(final int dimId) {
    return Arrays.binarySearch(dimIds, dimId) >= 0;
  }

  public int[] dims() {
    return dimIds.clone();
  }

  /** Returns the metadata for {@code dimId}, loading on demand. {@code null} if the dim is absent. */
  public PaginatedDimMetadata dimMetadata(final int dimId) throws IOException {
    final int p = Arrays.binarySearch(dimIds, dimId);
    if (p < 0)
      return null;
    PaginatedDimMetadata m = dimCacheByPos[p];
    if (m != null)
      return m;
    m = loadDimMetadata(dimId, trailerPageNums[p], trailerOffsets[p] & 0xFFFF);
    dimCacheByPos[p] = m;
    return m;
  }

  /** Opens a forward cursor over the postings of {@code dimId} in ascending RID order. */
  public PaginatedSegmentDimCursor openCursor(final int dimId) throws IOException {
    final PaginatedDimMetadata md = dimMetadata(dimId);
    if (md == null)
      return null;
    return new PaginatedSegmentDimCursor(this, md);
  }

  @Override
  public void close() {
    // Nothing to release: the component is owned by the engine + FileManager.
  }

  // --- internals ------------------------------------------------------------

  SparseSegmentComponent component() {
    return component;
  }

  private PaginatedDimMetadata loadDimMetadata(final int dimId, final int pageNum, final int offsetInPage) throws IOException {
    final BasePage trailerPage = component.readPage(pageNum);
    int p = offsetInPage;

    final int storedDimId = trailerPage.readInt(p);            p += 4;
    if (storedDimId != dimId)
      throw new IOException("dim trailer dim_id mismatch: expected " + dimId + " got " + storedDimId);
    final int blockCount = trailerPage.readInt(p);             p += 4;
    final int postingCount = trailerPage.readInt(p);           p += 4;
    final int df = trailerPage.readInt(p);                     p += 4;
    final float globalMaxWeight = trailerPage.readFloat(p);    p += 4;

    if (blockCount <= 0 || blockCount > 1_000_000_000)
      throw new IOException("Implausible block_count for dim " + dimId + ": " + blockCount);

    final int[]   blockPageNums = new int[blockCount];
    final short[] blockOffsets  = new short[blockCount];
    for (int b = 0; b < blockCount; b++) {
      blockPageNums[b] = trailerPage.readInt(p);               p += 4;
      blockOffsets[b]  = trailerPage.readShort(p);             p += 2;
    }

    final int skipEntries = (blockCount + params.skipStride() - 1) / params.skipStride();
    final SkipEntry[] skipList = new SkipEntry[skipEntries];
    for (int s = 0; s < skipEntries; s++) {
      final int bucketId = trailerPage.readInt(p);             p += 4;
      final long ridPos  = trailerPage.readLong(p);            p += 8;
      final float maxToEnd = trailerPage.readFloat(p);         p += 4;
      final int blockIndex = trailerPage.readInt(p);           p += 4;
      skipList[s] = new SkipEntry(new RID(bucketId, ridPos), maxToEnd, blockIndex);
    }

    final BlockHeader[] blockHeaders = new BlockHeader[blockCount];
    for (int b = 0; b < blockCount; b++)
      blockHeaders[b] = readBlockHeader(blockPageNums[b], blockOffsets[b] & 0xFFFF);

    return new PaginatedDimMetadata(dimId, postingCount, df, globalMaxWeight, blockPageNums, blockOffsets, blockHeaders,
        skipList);
  }

  BlockHeader readBlockHeader(final int pageNum, final int offsetInPage) throws IOException {
    final BasePage page = component.readPage(pageNum);
    int p = offsetInPage;
    final int firstBucket = page.readInt(p);                   p += 4;
    final long firstPos   = page.readLong(p);                  p += 8;
    final int lastBucket  = page.readInt(p);                   p += 4;
    final long lastPos    = page.readLong(p);                  p += 8;
    final int postingCount = page.readShort(p) & 0xFFFF;       p += 2;
    final float maxWeight = page.readFloat(p);                 p += 4;
    final float weightMin = page.readFloat(p);                 p += 4;
    final float weightMax = page.readFloat(p);                 p += 4;
    final boolean hasTombstones = page.readByte(p) != 0;
    return new BlockHeader(new RID(firstBucket, firstPos), new RID(lastBucket, lastPos), postingCount, maxWeight,
        weightMin, weightMax, hasTombstones);
  }

  /**
   * Fetch the block payload bytes that immediately follow a block header at
   * {@code (pageNum, offsetInPage)}. Returns a defensively-copied {@link ByteBuffer} positioned
   * at the first compressed RID byte. The caller knows the logical block length via the block
   * header's posting count, so we read up to the page's content end and let the cursor stop
   * decoding once it has the right number of postings.
   */
  ByteBuffer readBlockPayload(final int pageNum, final int offsetInPage) throws IOException {
    final BasePage page = component.readPage(pageNum);
    final int payloadOffset = offsetInPage + SegmentFormat.BLOCK_HEADER_SIZE;
    final int len = page.getMaxContentSize() - payloadOffset;
    if (len <= 0)
      return ByteBuffer.allocate(0).order(ByteOrder.BIG_ENDIAN);
    final byte[] copy = new byte[len];
    page.readByteArray(payloadOffset, copy);
    return ByteBuffer.wrap(copy).order(ByteOrder.BIG_ENDIAN);
  }
}
