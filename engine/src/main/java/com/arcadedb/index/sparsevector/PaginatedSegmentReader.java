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
import java.util.concurrent.atomic.AtomicReferenceArray;
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
  private final long                   tombstoneCount;

  // Sorted parallel arrays: dimIds[i] -> trailer at (trailerPageNums[i], trailerOffsets[i] & 0xFFFF).
  private final int[]                                 dimIds;
  private final int[]                                 trailerPageNums;
  private final short[]                               trailerOffsets;
  // Per-position lazy cache. AtomicReferenceArray gives us volatile-like read/write semantics on
  // each slot so the dim_metadata produced under a concurrent {@link #dimMetadata} call is safely
  // published to readers on other threads. Without this, BMW DAAT scoring on the query thread
  // could see a torn metadata reference written by a parallel cursor opener.
  private final AtomicReferenceArray<PaginatedDimMetadata> dimCacheByPos;

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
    // Slot 0 (8 bytes): total tombstones in this segment, written by SparseSegmentBuilder
    // (Tier 2 follow-up to #4068). Older segments built before this field was populated read 0L
    // here, which is a safe under-report - the engine's tombstone-ratio compaction trigger just
    // skips those segments. Slot 1 stays reserved.
    this.tombstoneCount = manifestPage.readLong(cursor);
    cursor += 8;
    cursor += 8; // skip reserved slot 1
    // Manifest CRC validation. Layout written by SparseSegmentBuilder.writeManifest covers
    // segmentId + parentCount + parents[] + reserved(16), with the CRC of all of those in the
    // last 4 bytes. A bit-flipped manifest could otherwise return wrong {@code parentSegments}
    // silently; we surface that as a SEVERE log here so the issue is observable without
    // failing the segment open.
    final int manifestSize = 8 + 4 + parentCount * 8 + 8 + 8 + 4; // segmentId + parentCount + parents + 2 reserved + crc
    final byte[] manifestBytes = new byte[manifestSize];
    manifestPage.readByteArray(0, manifestBytes);
    final int storedManifestCrc = manifestPage.readInt(cursor);
    final CRC32 manifestCrc = new CRC32();
    manifestCrc.update(manifestBytes, 0, manifestSize - 4);
    if ((int) manifestCrc.getValue() != storedManifestCrc) {
      com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.SEVERE,
          "Manifest CRC mismatch in segment '%s' (expected %d, got %d). The segment is being opened anyway, but parentSegments may be corrupt.",
          component.getName(), storedManifestCrc, (int) manifestCrc.getValue());
    }

    // ---- Dim index pages ----
    // The dim_index can span multiple consecutive pages when the corpus has more unique dims
    // than fit in one page (~6552 entries per 64 KiB page). Page 0 of the dim_index begins with
    // a 4-byte count header; entries pack densely across that page and any pages that follow,
    // never spanning a page boundary. The reader walks page by page until it has read all
    // {@code totalDims} entries.
    final int entrySize = PaginatedSegmentFormat.DIM_INDEX_ENTRY_SIZE;
    final int pageContentSize = component.pageContentSize();
    BasePage dimIndexPage = component.readPage(dimIndexPageNum);
    final int n = dimIndexPage.readInt(0);
    if (n != totalDims)
      throw new IOException("Inconsistent dim count in '" + component.getName() + "': header says " + totalDims
          + ", dim_index says " + n);
    this.dimIds = new int[n];
    this.trailerPageNums = new int[n];
    this.trailerOffsets = new short[n];
    this.dimCacheByPos = new AtomicReferenceArray<>(n);
    int prevDim = Integer.MIN_VALUE;
    int p = 4;                  // skip the 4-byte count on page 0
    int currentDimIndexPage = dimIndexPageNum;
    for (int i = 0; i < n; i++) {
      // Roll to the next contiguous dim_index page if the next entry won't fit on the current one.
      if (p + entrySize > pageContentSize) {
        currentDimIndexPage++;
        dimIndexPage = component.readPage(currentDimIndexPage);
        p = 0;
      }
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

  /**
   * Number of tombstones inside this segment, parsed from the manifest's tombstone slot. Used by
   * the engine's tombstone-ratio compaction trigger (Tier 2). Returns 0 for segments built before
   * this field was populated - those segments are simply skipped by the trigger, never falsely
   * flagged.
   */
  public long tombstoneCount() {
    return tombstoneCount;
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

  public boolean hasDim(final int dimId) {
    return Arrays.binarySearch(dimIds, dimId) >= 0;
  }

  public int[] dims() {
    return dimIds.clone();
  }

  /**
   * Returns the metadata for {@code dimId}, loading on demand. {@code null} if the dim is absent.
   * Thread-safe: the lazy cache is an {@link AtomicReferenceArray} so a metadata reference loaded
   * by one thread is safely published to readers on other threads. Two threads racing on the
   * same dim load the metadata twice (the loader is idempotent and cheap), but only one of the
   * results is kept in the slot - which is fine because the metadata is value-equivalent.
   */
  public PaginatedDimMetadata dimMetadata(final int dimId) throws IOException {
    final int p = Arrays.binarySearch(dimIds, dimId);
    if (p < 0)
      return null;
    PaginatedDimMetadata m = dimCacheByPos.get(p);
    if (m != null)
      return m;
    m = loadDimMetadata(dimId, trailerPageNums[p], trailerOffsets[p] & 0xFFFF);
    if (!dimCacheByPos.compareAndSet(p, null, m)) {
      // Another thread populated the slot first; return its value to keep callers consistent.
      return dimCacheByPos.get(p);
    }
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
    final int pageContentSize = component.pageContentSize();
    // Defensive page-boundary check. {@link SparseSegmentBuilder#endDim} guarantees a trailer
    // never straddles a page (it allocates a fresh page when {@code currentWritePageFree <
    // trailerSize}), so a well-formed segment will always have the full trailer reachable from
    // {@code offsetInPage}. A corrupt or hand-crafted segment whose dim_index pointed at a
    // straddling offset would otherwise cause silent reads of zero-padded tail bytes followed by
    // garbage from the next page; surface that as an {@link IOException} here. Use the minimum
    // trailer size (24 bytes: 5 ints + 1 float for the fixed-shape header) as the guard since
    // the variable parts (block locators + skip entries) need {@code blockCount} which we have
    // not read yet.
    final int minTrailerHeaderSize = 4 + 4 + 4 + 4 + 4;
    if (offsetInPage < 0 || offsetInPage + minTrailerHeaderSize > pageContentSize)
      throw new IOException(
          "dim trailer for dim " + dimId + " on page " + pageNum + " offset " + offsetInPage
              + " would straddle the page boundary (page content size " + pageContentSize + "); segment is corrupt");
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
    if (postingCount < 0 || postingCount > 1_000_000_000)
      throw new IOException("Implausible posting_count for dim " + dimId + ": " + postingCount);
    if (df < 0 || df > postingCount)
      throw new IOException(
          "Implausible df for dim " + dimId + ": df=" + df + " posting_count=" + postingCount + " (df must be in [0, posting_count])");
    // Now that {@code blockCount} is known, validate the full trailer fits on this page.
    final int skipStride = params.skipStride();
    final int skipEntries = (blockCount + skipStride - 1) / skipStride;
    final int fullTrailerSize = minTrailerHeaderSize + blockCount * (4 + 2) + skipEntries * (12 + 4 + 4);
    if (offsetInPage + fullTrailerSize > pageContentSize)
      throw new IOException(
          "dim trailer for dim " + dimId + " (full size " + fullTrailerSize + " B) would straddle the page boundary at offset "
              + offsetInPage + " on page " + pageNum + " (page content size " + pageContentSize + "); segment is corrupt");

    final int[]   blockPageNums = new int[blockCount];
    final short[] blockOffsets  = new short[blockCount];
    for (int b = 0; b < blockCount; b++) {
      blockPageNums[b] = trailerPage.readInt(p);               p += 4;
      blockOffsets[b]  = trailerPage.readShort(p);             p += 2;
    }

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
    final float bmwUpperBound = page.readFloat(p);             p += 4;
    final float weightMin = page.readFloat(p);                 p += 4;
    final float weightMax = page.readFloat(p);                 p += 4;
    final boolean hasTombstones = page.readByte(p) != 0;
    return new BlockHeader(new RID(firstBucket, firstPos), new RID(lastBucket, lastPos), postingCount, bmwUpperBound,
        weightMin, weightMax, hasTombstones);
  }

  /**
   * Fetch the block payload that immediately follows a block header at
   * {@code (pageNum, offsetInPage)} into a caller-owned scratch buffer. Returns the same
   * {@code reusableView} (which must wrap {@code dest}) positioned at the first compressed RID
   * byte and limited to the bytes-read window, so the caller can decode without allocating.
   * <p>
   * The caller knows the logical block length via the block header's posting count, so we read
   * up to the page's content end and let the cursor stop decoding once it has the right number
   * of postings; the limit is set conservatively to the bytes that were actually read.
   *
   * @param dest         scratch byte array sized at {@code &gt;= component.pageContentSize()}; the
   *                     reader fills it in place starting at offset 0
   * @param reusableView a {@link ByteBuffer} that wraps {@code dest}; the reader rewinds it,
   *                     applies the limit, and returns it
   */
  ByteBuffer readBlockPayloadInto(final int pageNum, final int offsetInPage, final byte[] dest,
      final ByteBuffer reusableView) throws IOException {
    final BasePage page = component.readPage(pageNum);
    final int payloadOffset = offsetInPage + SegmentFormat.BLOCK_HEADER_SIZE;
    final int len = page.getMaxContentSize() - payloadOffset;
    reusableView.clear();
    if (len <= 0) {
      reusableView.limit(0);
      return reusableView;
    }
    page.readByteArray(payloadOffset, dest, 0, len);
    reusableView.limit(len);
    return reusableView;
  }

}
