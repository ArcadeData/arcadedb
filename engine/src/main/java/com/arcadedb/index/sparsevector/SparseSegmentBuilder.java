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
import com.arcadedb.engine.MutablePage;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.sparsevector.SegmentFormat.RidCompression;
import com.arcadedb.log.LogManager;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.zip.CRC32;

/**
 * Builds a sealed sparse segment by allocating pages on a {@link SparseSegmentComponent}. Pages
 * are written through ArcadeDB's transaction context, so the resulting segment is fully durable
 * via the page WAL the moment its enclosing transaction commits - no separate fsync, no
 * flush-on-commit, no sparse-vector-specific recovery code.
 * <p>
 * <b>On-page layout</b> (in order of pages produced):
 * <pre>
 * Page 0: segment header (see {@link PaginatedSegmentFormat#HEADER_SIZE}).
 * Pages 1..N: block payloads (each block fits in one page; multiple blocks pack into one page).
 * Pages N+1..M: per-dim trailers (dim header + block_locators + skip_list), packed contiguously
 *               across pages (one trailer fits in one page; trailers do not span pages).
 * Page M+1: dim_index page (count + sorted (dim_id, trailer_page_num, trailer_offset) entries).
 * Page M+2: manifest page (segment_id, parent_segments, tombstone_floor, crc).
 * </pre>
 * The header, dim_index, and manifest each occupy their own dedicated page; the block stream and
 * trailer stream pack densely.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SparseSegmentBuilder implements AutoCloseable {

  private final SparseSegmentComponent component;
  private final SegmentParameters       params;
  private final int                     pageContentSize;

  // Manifest fields, fixed once before any dim is started.
  private long   segmentId             = 0L;
  private long[] parentSegments        = new long[0];

  // Currently open dim state.
  private int                     currentDimId = -1;
  private long                    currentDimPostingCount;     // includes tombstones
  private long                    currentDimLivePostingCount; // df: live (non-tombstone) only
  private float                   currentDimMaxWeight;
  // Per-block locator storage: parallel primitive arrays grown via {@code Arrays.copyOf} instead
  // of {@code List<long[]>}. At the default {@code blockSize=128} a 1M-posting dim has ~7,800
  // blocks, which would otherwise be ~7,800 separate {@code long[2]} heap allocations per dim
  // during flush - directly visible as GC pressure on bulk-load profiles. The two arrays
  // together cost 6 bytes per block (int + short) plus negligible header overhead, vs ~64 bytes
  // per {@code long[2]} heap object.
  private int[]                   currentDimBlockPageNums = new int[16];
  private short[]                 currentDimBlockOffsets  = new short[16];
  private int                     currentDimBlockLocatorCount;
  private final List<BlockHeader> currentDimBlockHeaders  = new ArrayList<>();

  // Block buffering.
  private final RID[]     blockRids;
  private final float[]   blockWeights;
  private final boolean[] blockTombstones;
  private int             blockCursor;

  // Allocated pages, in order. Held in memory until the enclosing transaction commits.
  private final List<MutablePage> pages = new ArrayList<>();
  private int                     currentWritePage      = -1;
  private int                     currentWritePageFree;

  // Index entries for the dim_index page: {dim_id, trailer_page_num, trailer_offset_in_page}.
  private final List<int[]> dimIndex = new ArrayList<>();

  private long    totalPostings;
  private boolean finished;

  // Reusable payload scratch space, sized once at construction. Used by:
  // - {@link #flushBlock} to assemble a block header + RID/weight payload, then copy the
  //   populated prefix into the active page;
  // - {@link #writeDimIndex} to pack a full page of dim_index entries in one writeByteArray call;
  // - {@link #writeDimTrailer} to assemble a per-dim trailer (block locators + skip list) for
  //   one writeByteArray call, instead of {@code ByteBuffer.allocate} per dim.
  // Sized to {@code max(estimateBlockPayloadSize, pageContentSize)}: the per-block worst case
  // is normally smaller than a full page, but the dim_index packer fills up to a full page in
  // one go and the trailer can grow up to a full page for very wide dims, so the scratch must
  // cover both. On a default 64 KiB page that is one ~64 KiB allocation per builder instead of
  // a fresh ByteBuffer per block + per dim_index page + per dim trailer.
  private final byte[]     payloadScratch;
  private final ByteBuffer payloadBuf;

  public SparseSegmentBuilder(final SparseSegmentComponent component, final SegmentParameters params) {
    if (component.getPageSize() != params.pageSize())
      throw new IllegalArgumentException(
          "component page size (" + component.getPageSize() + ") does not match params (" + params.pageSize() + ")");
    this.component = component;
    this.params = params;
    this.pageContentSize = component.pageContentSize();
    this.blockRids = new RID[params.blockSize()];
    this.blockWeights = new float[params.blockSize()];
    this.blockTombstones = new boolean[params.blockSize()];
    this.payloadScratch = new byte[Math.max(estimateBlockPayloadSize(params), pageContentSize)];
    this.payloadBuf = ByteBuffer.wrap(payloadScratch).order(ByteOrder.BIG_ENDIAN);
    // Allocate page 0 right away; back-patched at finish().
    allocateNewPage();
    // Reserve all of page 0 for the header so subsequent pages don't intrude on it.
    currentWritePageFree = 0;
  }

  private static int estimateBlockPayloadSize(final SegmentParameters params) {
    final int base = SegmentFormat.BLOCK_HEADER_SIZE + params.blockSize() * VarInt.MAX_VARLONG_BYTES * 2;
    final int weightBytes = switch (params.weightQuantization()) {
      case INT8 -> params.blockSize();
      case FP16 -> params.blockSize() * 2;
      case FP32 -> params.blockSize() * 4;
    };
    return Math.max(4096, base + weightBytes);
  }

  public void setSegmentId(final long segmentId) {
    requireFresh();
    this.segmentId = segmentId;
  }

  public void setParentSegments(final long[] parents) {
    requireFresh();
    this.parentSegments = parents.clone();
  }

  public void startDim(final int dimId) {
    if (finished)
      throw new IllegalStateException("builder is finished");
    if (currentDimId != -1)
      throw new IllegalStateException("dim " + currentDimId + " is open; call endDim() first");
    if (!dimIndex.isEmpty()) {
      final int lastDim = dimIndex.getLast()[0];
      if (dimId <= lastDim)
        throw new IllegalArgumentException("dims must arrive in strictly ascending order: got " + dimId + " after " + lastDim);
    }
    this.currentDimId = dimId;
    this.currentDimPostingCount = 0L;
    this.currentDimLivePostingCount = 0L;
    this.currentDimMaxWeight = Float.NEGATIVE_INFINITY;
    this.currentDimBlockLocatorCount = 0;  // arrays kept allocated; reused for the next dim
    this.currentDimBlockHeaders.clear();
    this.blockCursor = 0;
  }

  public void appendPosting(final RID rid, final float weight) {
    if (currentDimId == -1)
      throw new IllegalStateException("call startDim before appendPosting");
    if (Float.isNaN(weight) || Float.isInfinite(weight))
      throw new IllegalArgumentException("weight must be a finite number: " + weight);
    if (weight < 0.0f)
      throw new IllegalArgumentException("weight must be non-negative: " + weight);
    appendInternal(rid, weight, false);
  }

  public void appendTombstone(final RID rid) {
    if (currentDimId == -1)
      throw new IllegalStateException("call startDim before appendTombstone");
    appendInternal(rid, Float.NaN, true);
  }

  private void appendInternal(final RID rid, final float weight, final boolean tombstone) {
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

  public void endDim() {
    if (currentDimId == -1)
      throw new IllegalStateException("no dim open");
    if (blockCursor > 0)
      flushBlock();
    if (currentDimBlockHeaders.isEmpty()) {
      // Empty dim - drop silently, matches the file-based writer's contract.
      currentDimId = -1;
      return;
    }
    final int trailerSize = computeDimTrailerSize();
    if (currentWritePageFree < trailerSize)
      allocateNewPage();
    final int trailerPage = currentWritePage;
    final int trailerOffset = pageContentSize - currentWritePageFree;
    writeDimTrailer(trailerPage, trailerOffset);
    dimIndex.add(new int[] { currentDimId, trailerPage, trailerOffset });
    totalPostings += currentDimPostingCount;
    currentDimId = -1;
  }

  /**
   * Finalize the segment: write the dim_index, the manifest, then back-patch page 0's header.
   * Caller is responsible for the surrounding transaction (commit makes the segment durable).
   */
  public void finish() {
    if (finished)
      return;
    if (currentDimId != -1)
      throw new IllegalStateException("call endDim before finish");

    // Dim index always starts on a fresh page so its locator from the manifest can address it cleanly.
    allocateNewPage();
    final int dimIndexPage = currentWritePage;
    writeDimIndex(dimIndexPage);

    // Manifest gets its own (final) page.
    allocateNewPage();
    final int manifestPage = currentWritePage;
    writeManifest(manifestPage);

    // Back-patch page 0 header now that all pointers are known.
    writeHeader(pages.getFirst(), manifestPage, dimIndexPage);
    finished = true;
  }

  @Override
  public void close() {
    // Builder owns no I/O resources outside the transaction's MutablePage list. The only thing
    // close() can usefully do is loud-fail an obvious misuse: if a caller wrote one or more
    // dims (so dimIndex is non-empty) but never called finish(), the segment file is registered
    // with the schema yet missing its manifest, dim_index and back-patched header - a future
    // reader would throw a magic / CRC mismatch much later, far from the bug. Throwing here
    // is safe under try-with-resources: when the body has already thrown, Java's resource
    // cleanup attaches this exception via {@link Throwable#addSuppressed} rather than masking
    // the body's primary throwable. The engine's flush() and compactInputs() catch the throw
    // and drop the partial component (see PaginatedSparseVectorEngine), so the orphan file
    // never escapes a single transaction.
    if (!finished && !dimIndex.isEmpty())
      throw new IllegalStateException(
          "SparseSegmentBuilder for component '" + component.getName() + "' was closed with " + dimIndex.size()
              + " dim(s) written but no finish() call; the segment file is incomplete. Either call finish() or"
              + " drop the component before closing the builder.");
  }

  // --- internals ------------------------------------------------------------

  private void requireFresh() {
    if (currentDimId != -1 || !dimIndex.isEmpty())
      throw new IllegalStateException("must be called before any dim is written");
  }

  private void allocateNewPage() {
    final int nextPageNum = pages.size();
    final MutablePage page = component.allocatePage(nextPageNum);
    pages.add(page);
    currentWritePage = nextPageNum;
    currentWritePageFree = pageContentSize;
  }

  /** Write {@code length} bytes into the active page starting at the current free offset; advance. */
  private int writeBytesAtPageCursor(final byte[] source, final int length) {
    final int offset = pageContentSize - currentWritePageFree;
    pages.get(currentWritePage).writeByteArray(offset, source, 0, length);
    currentWritePageFree -= length;
    return offset;
  }

  private void flushBlock() {
    if (blockCursor == 0)
      return;

    float weightMin = Float.POSITIVE_INFINITY;
    float weightMax = Float.NEGATIVE_INFINITY;
    boolean hasTombstones = false;
    int liveInBlock = 0;
    for (int i = 0; i < blockCursor; i++) {
      if (blockTombstones[i]) {
        hasTombstones = true;
        continue;
      }
      liveInBlock++;
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

    // Reuse the per-builder scratch buffer (sized at construction to the worst-case block
    // payload size). Saves an allocation per block - on a default-shaped 1M-posting flush that
    // is ~7.8k ByteBuffer allocations dropped from the flush path.
    payloadBuf.clear();

    // Block header.
    putRid(payloadBuf, blockRids[0]);
    putRid(payloadBuf, blockRids[blockCursor - 1]);
    payloadBuf.putShort((short) blockCursor);
    payloadBuf.putFloat(blockMaxForBmw);
    payloadBuf.putFloat(weightMin);
    payloadBuf.putFloat(weightMax);
    payloadBuf.put((byte) (hasTombstones ? 1 : 0));
    payloadBuf.put((byte) 0);

    // Compressed RIDs (skip first; it's already in the header).
    if (params.ridCompression() == RidCompression.VARINT_DELTA) {
      RID prev = blockRids[0];
      for (int i = 1; i < blockCursor; i++) {
        final RID curr = blockRids[i];
        final int bucketDelta = curr.getBucketId() - prev.getBucketId();
        // {@link #appendInternal} already enforces ascending RID order, so by construction
        // {@code bucketDelta >= 0} and (when {@code bucketDelta == 0}) the position delta is
        // also non-negative. A negative delta would silently encode as a huge unsigned VarInt
        // and decode to a different RID on read - a corruption far from the cause - so we fail
        // loud here in case some future writer path bypasses the order check.
        if (bucketDelta < 0)
          throw new IndexException("Non-monotonic RID bucket on segment write: prev=" + prev + " curr=" + curr);
        VarInt.writeUnsignedVarLong(payloadBuf, bucketDelta);
        if (bucketDelta == 0) {
          final long positionDelta = curr.getPosition() - prev.getPosition();
          if (positionDelta < 0)
            throw new IndexException("Non-monotonic RID position on segment write: prev=" + prev + " curr=" + curr);
          VarInt.writeUnsignedVarLong(payloadBuf, positionDelta);
        } else {
          VarInt.writeUnsignedVarLong(payloadBuf, curr.getPosition());
        }
        prev = curr;
      }
    } else {
      for (int i = 1; i < blockCursor; i++)
        putRid(payloadBuf, blockRids[i]);
    }

    // Compressed weights.
    switch (params.weightQuantization()) {
      case INT8 -> {
        for (int i = 0; i < blockCursor; i++) {
          if (blockTombstones[i])
            payloadBuf.put(SegmentFormat.INT8_TOMBSTONE_SENTINEL);
          else
            payloadBuf.put(WeightCodec.quantizeInt8(blockWeights[i], weightMin, weightMax));
        }
      }
      case FP16 -> {
        for (int i = 0; i < blockCursor; i++) {
          if (blockTombstones[i])
            payloadBuf.putShort(SegmentFormat.FP16_TOMBSTONE_SENTINEL);
          else
            payloadBuf.putShort(WeightCodec.toFp16(blockWeights[i]));
        }
      }
      case FP32 -> {
        for (int i = 0; i < blockCursor; i++) {
          if (blockTombstones[i])
            payloadBuf.putInt(WeightCodec.FP32_TOMBSTONE_BITS);
          else
            payloadBuf.putInt(WeightCodec.floatToTombstoneAwareBits(blockWeights[i]));
        }
      }
    }

    final int payloadLen = payloadBuf.position();
    if (payloadLen > pageContentSize)
      throw new IndexException("block payload " + payloadLen + " bytes exceeds page content size "
          + pageContentSize + "; reduce blockSize or increase pageSize");

    if (currentWritePageFree < payloadLen)
      allocateNewPage();

    final int offsetInPage = writeBytesAtPageCursor(payloadScratch, payloadLen);
    if (currentDimBlockLocatorCount == currentDimBlockPageNums.length) {
      final int newCap = currentDimBlockPageNums.length * 2;
      currentDimBlockPageNums = Arrays.copyOf(currentDimBlockPageNums, newCap);
      currentDimBlockOffsets  = Arrays.copyOf(currentDimBlockOffsets, newCap);
    }
    currentDimBlockPageNums[currentDimBlockLocatorCount] = currentWritePage;
    currentDimBlockOffsets[currentDimBlockLocatorCount] = (short) offsetInPage;
    currentDimBlockLocatorCount++;
    currentDimBlockHeaders.add(new BlockHeader(blockRids[0], blockRids[blockCursor - 1], blockCursor, blockMaxForBmw,
        weightMin, weightMax, hasTombstones));
    currentDimPostingCount += blockCursor;
    currentDimLivePostingCount += liveInBlock;
    if (blockMaxForBmw > currentDimMaxWeight)
      currentDimMaxWeight = blockMaxForBmw;
    blockCursor = 0;
  }


  private int computeDimTrailerSize() {
    final int blockCount = currentDimBlockHeaders.size();
    final int skipEntries = (blockCount + params.skipStride() - 1) / params.skipStride();
    return PaginatedSegmentFormat.DIM_TRAILER_HEADER_SIZE
        + blockCount * PaginatedSegmentFormat.BLOCK_LOCATOR_SIZE
        + skipEntries * SegmentFormat.SKIP_ENTRY_SIZE;
  }

  private void writeDimTrailer(final int pageNum, final int offsetInPage) {
    final int blockCount = currentDimBlockHeaders.size();
    final int skipEntries = (blockCount + params.skipStride() - 1) / params.skipStride();

    // Compute max_weight_to_end backwards from the tail.
    final float[] maxWeightToEnd = new float[skipEntries];
    float runningMax = Float.NEGATIVE_INFINITY;
    for (int b = blockCount - 1; b >= 0; b--) {
      final float bm = currentDimBlockHeaders.get(b).bmwUpperBound();
      if (bm > runningMax)
        runningMax = bm;
      if (b % params.skipStride() == 0)
        maxWeightToEnd[b / params.skipStride()] = runningMax;
    }

    final int trailerSize = computeDimTrailerSize();
    payloadBuf.clear();
    payloadBuf.putInt(currentDimId);
    payloadBuf.putInt(blockCount);
    payloadBuf.putInt((int) currentDimPostingCount);
    // df is the segment-local document frequency: number of LIVE postings only. Tombstones do
    // not contribute - keeping them in df would inflate IDF in proportion to the
    // uncompacted-tombstone load, which is exactly the case where IDF should be most accurate.
    // posting_count above stays as the total entry count (live + tombstones) so on-disk
    // bookkeeping (dim trailer scans, debugging) sees what is actually written.
    payloadBuf.putInt((int) currentDimLivePostingCount);
    payloadBuf.putFloat(currentDimMaxWeight == Float.NEGATIVE_INFINITY ? 0.0f : currentDimMaxWeight);

    for (int b = 0; b < blockCount; b++) {
      payloadBuf.putInt(currentDimBlockPageNums[b]);
      payloadBuf.putShort(currentDimBlockOffsets[b]);
    }

    for (int s = 0; s < skipEntries; s++) {
      final int firstBlock = s * params.skipStride();
      final BlockHeader bh = currentDimBlockHeaders.get(firstBlock);
      putRid(payloadBuf, bh.firstRid());
      payloadBuf.putFloat(maxWeightToEnd[s]);
      payloadBuf.putInt(firstBlock);
    }

    pages.get(pageNum).writeByteArray(offsetInPage, payloadScratch, 0, trailerSize);
    currentWritePageFree -= trailerSize;
  }

  /**
   * Writes the dim_index across one or more contiguous pages starting at {@code firstPageNum}.
   * <p>
   * Layout:
   * <ul>
   *   <li>Page 0 of the dim_index: int32 entry count, then entries packed densely.</li>
   *   <li>Pages 1..k of the dim_index: entries packed densely (no per-page header).</li>
   * </ul>
   * Entries do not span page boundaries; if the next entry wouldn't fit on the current page, we
   * roll to a freshly-allocated page. The entry count + entry size is enough for the reader to
   * compute which page each entry lives on, so no per-page metadata is needed.
   */
  private void writeDimIndex(final int firstPageNum) {
    final int entrySize = PaginatedSegmentFormat.DIM_INDEX_ENTRY_SIZE;
    final int total = dimIndex.size();
    int writtenEntries = 0;
    int pageNum = firstPageNum;
    int offsetInPage = 0;

    // Reuse {@link #payloadScratch} (sized at construction to {@code >= pageContentSize}, so a
    // full page of dim_index entries always fits) instead of allocating a fresh ByteBuffer per
    // page. Multi-page dim_index for high-vocab corpora (>6552 dims at the default 64 KiB page)
    // drops one allocation per page from the flush path.
    payloadBuf.clear();

    // Page 0: write the count header first.
    payloadBuf.putInt(total);
    pages.get(pageNum).writeByteArray(0, payloadScratch, 0, 4);
    offsetInPage = 4;

    while (writtenEntries < total) {
      // Roll to a new page if the next entry can't fit in the remaining space on this page.
      if (offsetInPage + entrySize > pageContentSize) {
        allocateNewPage();
        pageNum = currentWritePage;
        offsetInPage = 0;
      }
      // Pack as many entries as fit on the current page in one writeByteArray call.
      final int remainingEntries = total - writtenEntries;
      final int spaceLeft = pageContentSize - offsetInPage;
      final int entriesOnThisPage = Math.min(remainingEntries, spaceLeft / entrySize);
      payloadBuf.clear();
      for (int i = 0; i < entriesOnThisPage; i++) {
        final int[] entry = dimIndex.get(writtenEntries + i);
        payloadBuf.putInt(entry[0]);              // dim_id
        payloadBuf.putInt(entry[1]);              // trailer page_num
        payloadBuf.putShort((short) entry[2]);    // trailer offset_in_page
      }
      pages.get(pageNum).writeByteArray(offsetInPage, payloadScratch, 0, entriesOnThisPage * entrySize);
      offsetInPage += entriesOnThisPage * entrySize;
      writtenEntries += entriesOnThisPage;
    }

    currentWritePage = pageNum;
    currentWritePageFree = pageContentSize - offsetInPage;
  }

  private void writeManifest(final int pageNum) {
    final int parentCount = parentSegments.length;
    final int size = 8 + 4 + parentCount * 8 + 8 + 8 + 4;
    if (size > pageContentSize)
      throw new IndexException("manifest size " + size + " exceeds page content size " + pageContentSize);
    // Reuse {@link #payloadBuf} (sized at construction to {@code >= pageContentSize}, so the
    // manifest always fits) for consistency with the rest of the builder's buffer-reuse strategy.
    // Not a hot path - one call per segment - but allocating a fresh ByteBuffer here was the only
    // remaining one-off allocation in the build pipeline.
    payloadBuf.clear();
    payloadBuf.putLong(segmentId);
    payloadBuf.putInt(parentCount);
    for (final long p : parentSegments)
      payloadBuf.putLong(p);
    // Two reserved 8-byte slots. The first slot was originally reserved for a
    // {@code tombstoneFloorSegment} - the oldest segment id whose tombstones can be physically
    // dropped during compaction - but the current compaction path uses a binary
    // {@code dropAllTombstones} flag set by {@link #compactAll}, so the floor is dead weight.
    // Kept as reserved bytes so manifest layout stays at the same size; revisit if a tiered
    // tombstone-eviction policy lands.
    payloadBuf.putLong(0L);                     // reserved (formerly tombstoneFloorSegment)
    payloadBuf.putLong(0L);                     // reserved
    final CRC32 crc = new CRC32();
    crc.update(payloadScratch, 0, size - 4);
    payloadBuf.putInt((int) crc.getValue());
    pages.get(pageNum).writeByteArray(0, payloadScratch, 0, size);
    currentWritePageFree = pageContentSize - size;
  }

  private void writeHeader(final MutablePage page0, final int manifestPage, final int dimIndexPage) {
    final ByteBuffer buf = ByteBuffer.allocate(PaginatedSegmentFormat.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
    buf.putLong(SegmentFormat.MAGIC);
    buf.putInt(SegmentFormat.FORMAT_VERSION);
    buf.putInt(params.blockSize());
    buf.putInt(params.skipStride());
    buf.put(params.weightQuantization().code());
    buf.put(params.ridCompression().code());
    buf.putShort((short) 0);             // reserved
    buf.putLong(segmentId);
    buf.putLong(totalPostings);
    buf.putInt(dimIndex.size());
    buf.putLong(System.currentTimeMillis());
    buf.putInt(manifestPage);
    buf.putInt(dimIndexPage);
    final CRC32 crc = new CRC32();
    crc.update(buf.array(), 0, PaginatedSegmentFormat.HEADER_SIZE - 4);
    buf.putInt((int) crc.getValue());
    page0.writeByteArray(0, buf.array(), 0, PaginatedSegmentFormat.HEADER_SIZE);
  }

  static void putRid(final ByteBuffer buf, final RID rid) {
    buf.putInt(rid.getBucketId());
    buf.putLong(rid.getPosition());
  }

  /**
   * Lexicographic compare of two RIDs by (bucketId, position). Public so cursor + scorer code can
   * share one canonical ordering helper without each having to re-implement it.
   */
  public static int compareRid(final RID a, final RID b) {
    final int b1 = a.getBucketId();
    final int b2 = b.getBucketId();
    if (b1 != b2)
      return Integer.compare(b1, b2);
    return Long.compare(a.getPosition(), b.getPosition());
  }
}
