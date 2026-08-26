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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Forward cursor over the postings of a single dim within a page-backed sealed segment. Implements
 * a {@code block_header} / {@code posting} state machine and decodes block payloads straight out of
 * the page-cache page they live on. The skip path uses the per-segment skip list to avoid
 * decompressing blocks that cannot beat the current threshold.
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
  // Weight and tombstone of the posting the cursor sits on, decoded from the page on first read and
  // held until the cursor moves. The traversal walks far more postings than it scores - a block-max
  // skip reads none of them, and a non-essential probe that is abandoned early reads none either -
  // so decoding the whole block's weights up front was work the query mostly threw away
  // (issue #5467).
  private float   currentWeight;
  private boolean currentTombstone;
  private boolean weightResolved;

  // Decoded positions of the current block, held as parallel primitive arrays. An earlier version
  // materialised a RID per posting at decode time; on a learned-sparse query that is millions of
  // short-lived objects per query, and it dominated the traversal cost (issue #5388). Only the
  // posting the cursor is actually positioned on ever becomes a RID.
  private final int[]   blockBuckets;
  private final long[]  blockPositions;
  private int           blockSize;
  private boolean       blockDecoded;
  private boolean       exhausted;

  // The page that holds {@code currentBlock}, kept between blocks. The builder packs a dim's blocks
  // back to back, so well over a hundred default-sized blocks share one 64 KiB page and a sequential
  // walk used to re-resolve the same page once per block. Each resolution allocated a {@code PageId}
  // and an {@code ImmutablePage}, hashed into the page-cache map, and bumped the cache's global hit
  // counter - an atomic that every parallel-range worker of issue #5518 contends on. Holding the page
  // collapses all of that to once per page (issue #5467).
  // <p>
  // Only ABSOLUTE reads are made through {@code pageBuf}: a REPEATABLE_READ transaction hands the
  // same {@code ImmutablePage} - and therefore the same {@link ByteBuffer} - to every cursor that
  // asks for the page, so touching its position would be a cross-cursor data race. Sealed segments
  // are never written again, so the bytes behind it cannot change under us, and a page evicted from
  // the cache stays valid here because eviction only drops the map entry.
  // <p>
  // <b>The footprint this costs, deliberately.</b> The reference keeps one page image reachable per
  // open cursor until the cursor walks onto another page or is closed, so a wide learned-sparse query
  // (up to ~120 terms) can hold ~120 distinct 64 KiB pages, and the parallel-range fan-out of issue
  // #5518 multiplies that by the worker count. The pages are ones the query is actively reading and
  // are already resident in the shared page cache; what the reference adds is only that an eviction
  // racing the query cannot drop them from under it. It is bounded by open cursors rather than by
  // blocks decoded, {@link #close()} releases it, and a REPEATABLE_READ transaction already pins
  // every page it reads for its whole lifetime, which is strictly more.
  private ByteBuffer    pageBuf;
  private int           pageNum = -1;
  private int           pageLimit;
  private long          pageFetchCount;

  // Where the current block's weight section starts in {@code pageBuf}, and the block's
  // dequantization band. Weights are fixed-stride, so posting {@code i}'s weight is one indexed read
  // at {@code weightsOffset + i * weightStride}; the offset itself is wherever the variable-length
  // RID section ended.
  private int           weightsOffset;
  private float         blockWeightMin;
  private float         blockWeightMax;
  private final int     weightStride;

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

  // Per-cursor count of payload bytes those decodes read. Tracked separately from
  // {@link #decodedBlockCount} because the two are not proportional: the reader used to hand back
  // every byte from the block payload to the end of the page regardless of how long the block
  // actually was, so a ~500-byte block cost a ~32 KB copy on a 64 KB page. Asserting on this counter
  // is what keeps that from coming back (issue #5388). Since issue #5467 the decode works in place
  // on the page and copies nothing at all, so this is the exact payload length of every block
  // touched.
  private long          decodedPayloadBytes;

  // Per-cursor count of postings whose weight was actually decoded. Compared against
  // {@link #decodedBlockCount} times the block size, this is how much of the weight work the query
  // used to do up front it now never does at all.
  private long          resolvedWeightCount;

  PaginatedSegmentDimCursor(final PaginatedSegmentReader reader, final PaginatedDimMetadata meta) {
    this.reader = reader;
    this.meta = meta;
    this.params = reader.parameters();
    this.blockBuckets = new int[params.blockSize()];
    this.blockPositions = new long[params.blockSize()];
    this.weightStride = switch (params.weightQuantization()) {
      case INT8 -> 1;
      case FP16 -> 2;
      case FP32 -> 4;
    };
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

  private void clearPosition() {
    currentBucketId = -1;
    currentPosition = -1L;
    currentRidObj = null;
  }

  @Override
  public float currentWeight() {
    resolvePendingDecode();
    resolveWeight();
    return currentWeight;
  }

  @Override
  public boolean isTombstone() {
    resolvePendingDecode();
    resolveWeight();
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

  /** Test/observability hook: total payload bytes read out of the page cache by those decodes. */
  public long decodedPayloadBytes() {
    return decodedPayloadBytes;
  }

  /**
   * Test/observability hook: how many times this cursor asked the page cache for a page. One per
   * distinct page it walked onto, not one per block decoded - see the {@code pageBuf} field.
   */
  public long pageFetchCount() {
    return pageFetchCount;
  }

  /** Test/observability hook: postings whose weight this cursor actually decoded. */
  public long resolvedWeightCount() {
    return resolvedWeightCount;
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
    // Release the page: a closed cursor must not keep a 64 KiB page image reachable after the query
    // that opened it has finished with it.
    pageBuf = null;
    pageNum = -1;
    blockDecoded = false;
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

  /**
   * Decode the current block's RID section in place on the page it lives on, and record where its
   * weight section starts.
   * <p>
   * Nothing is copied. Before issue #5467 the payload was copied into a per-thread scratch buffer
   * first, which is why {@link SegmentFormat#maxBlockPayloadSize} and the "gap to the next block"
   * estimate had to exist at all - the format does not store a block's byte length, so a copy needed
   * an upper bound on how much to take. Decoding against the page needs no bound: the RID loop
   * consumes exactly the bytes the block's posting count describes and ends on the first weight
   * byte, which is the block's exact RID-section length rather than an estimate of it.
   * <p>
   * Reads are bounds-checked against the page's content limit so a corrupt posting count surfaces as
   * an {@link IOException} naming the segment rather than as a buffer exception from the decode
   * loop.
   */
  private void decodeBlockIfNeeded() throws IOException {
    if (blockDecoded)
      return;
    final int blockPage = meta.blockPageNum(currentBlock);
    if (blockPage != pageNum || pageBuf == null) {
      final BasePage page = reader.readPage(blockPage);
      pageBuf = page.getContent();
      pageLimit = BasePage.PAGE_HEADER_SIZE + page.getMaxContentSize();
      pageNum = blockPage;
      pageFetchCount++;
    }
    final ByteBuffer buf = pageBuf;
    final int limit = pageLimit;
    final int base = BasePage.PAGE_HEADER_SIZE + meta.blockOffset(currentBlock) + SegmentFormat.BLOCK_HEADER_SIZE;
    final int n = meta.blockPostingCount(currentBlock);
    // The header's posting count is read off the segment file, so it is untrusted input (issue
    // #6566). Above the segment's declared block size it runs off the end of the decode arrays; at
    // zero it makes the block report the header's first RID as a posting that is not there.
    // {@link SparseSegmentBuilder#flushBlock} never writes an empty block, so both are corruption,
    // and both should read as a corrupt segment rather than as an ArrayIndexOutOfBoundsException
    // from the middle of the decode loop or as a phantom posting in the result set.
    if (n <= 0 || n > blockBuckets.length)
      throw new IOException(corruptBlockMessage() + " (posting count " + n + " is not in 1.." + blockBuckets.length
          + ", the segment's declared block size)");
    blockSize = n;
    blockBuckets[0] = meta.blockFirstBucketId(currentBlock);
    blockPositions[0] = meta.blockFirstPosition(currentBlock);

    int p = base;
    if (params.ridCompression() == RidCompression.VARINT_DELTA) {
      // The VarLong reader is inlined rather than called through {@link VarInt}: it has to advance a
      // local offset instead of a ByteBuffer position (absolute reads only - see {@code pageBuf}),
      // and a method cannot return both the value and the bytes it consumed without either a second
      // pass or an allocation. This is the traversal's innermost loop.
      // <p>
      // {@code shift == 70} means a tenth byte was read, and a tenth byte contributes only bit 63, so
      // its payload can be 0 or 1. Above that, the bits Java's shift silently discards would let a
      // nine-continuation-bytes-then-{@code 0x02} encoding decode as a plausible small value that the
      // ascending-order check below cannot see anything wrong with - so the width is checked once per
      // VarLong, on a branch that is always false on anything a writer produced. This is the same
      // condition {@link VarInt#readUnsignedVarLong} tests as {@code shift == 63}: that one checks
      // before its {@code shift += 7}, this one after the loop has already applied it.
      int prevBucket = blockBuckets[0];
      long prevPosition = blockPositions[0];
      for (int i = 1; i < n; i++) {
        long bucketDelta = 0L;
        int shift = 0;
        byte b;
        do {
          if (p >= limit || shift >= 64)
            throw new IOException(corruptBlockMessage());
          b = buf.get(p++);
          bucketDelta |= ((long) (b & 0x7F)) << shift;
          shift += 7;
        } while ((b & 0x80) != 0);
        if (shift == 70 && (b & 0x7F) > 1)
          throw new IOException(corruptBlockMessage() + OVERWIDE_VARLONG);
        long secondField = 0L;
        shift = 0;
        do {
          if (p >= limit || shift >= 64)
            throw new IOException(corruptBlockMessage());
          b = buf.get(p++);
          secondField |= ((long) (b & 0x7F)) << shift;
          shift += 7;
        } while ((b & 0x80) != 0);
        if (shift == 70 && (b & 0x7F) > 1)
          throw new IOException(corruptBlockMessage() + OVERWIDE_VARLONG);
        // A VarLong is unsigned, so a decoded value at or above 2^63 arrives here as a NEGATIVE long.
        // Neither RID component can legitimately be that large - a bucket id is an int and a position
        // is a non-negative long - and the ordering checks below cannot see it: a negative bucket
        // delta narrows to a small positive int (0x8000_0000_0000_0001 casts to 1, which looks like a
        // perfectly ordinary step forward), and a negative absolute position rides in on a bucket
        // that did increase. So the range is checked before anything narrows or accumulates.
        if (bucketDelta < 0L || secondField < 0L)
          throw new IOException(corruptBlockMessage() + OUT_OF_RANGE_RID);
        // Strictly ascending RIDs are a write-side invariant that the read side never checked:
        // SparseSegmentBuilder.appendInternal rejects a non-increasing RID and flushBlock fails loud
        // on a negative delta, precisely because "a negative delta would silently encode as a huge
        // unsigned VarInt and decode to a different RID on read". Checking the same thing here is
        // what turns that into a named error instead of a traversal quietly merging out of order.
        if (bucketDelta == 0L) {
          final long nextPosition = prevPosition + secondField;
          if (nextPosition <= prevPosition)
            throw new IOException(corruptBlockMessage() + notAscending(prevBucket, prevPosition));
          prevPosition = nextPosition;
        } else {
          if (bucketDelta > Integer.MAX_VALUE)
            throw new IOException(corruptBlockMessage() + OUT_OF_RANGE_RID);
          final int nextBucket = prevBucket + (int) bucketDelta;
          if (nextBucket <= prevBucket)
            throw new IOException(corruptBlockMessage() + notAscending(prevBucket, prevPosition));
          prevBucket = nextBucket;
          prevPosition = secondField;
        }
        blockBuckets[i] = prevBucket;
        blockPositions[i] = prevPosition;
      }
    } else {
      // The first posting's RID lives in the block header, so the payload holds n-1 of them at the
      // format's fixed width.
      if (p + (n - 1) * SegmentFormat.RID_SIZE_BYTES > limit)
        throw new IOException(corruptBlockMessage());
      for (int i = 1; i < n; i++) {
        blockBuckets[i] = buf.getInt(p);
        p += 4;
        blockPositions[i] = buf.getLong(p);
        p += 8;
        if (SparseSegmentBuilder.compareRid(blockBuckets[i], blockPositions[i], blockBuckets[i - 1],
            blockPositions[i - 1]) <= 0)
          throw new IOException(corruptBlockMessage() + notAscending(blockBuckets[i - 1], blockPositions[i - 1]));
      }
    }

    // The header names the block's last RID, and the builder writes it from the same posting the
    // payload ends on. Checking that they agree costs one comparison per block - not per posting -
    // and cross-validates the payload against the header rather than only checking each against
    // itself: a truncated, scrambled or mis-counted payload almost never lands on the right last RID,
    // however well-ordered the sequence it decoded to happened to be.
    if (SparseSegmentBuilder.compareRid(blockBuckets[n - 1], blockPositions[n - 1], meta.blockLastBucketId(currentBlock),
        meta.blockLastPosition(currentBlock)) != 0)
      throw new IOException(corruptBlockMessage() + " (the payload ends on #" + blockBuckets[n - 1] + ":"
          + blockPositions[n - 1] + ", but the block header says #" + meta.blockLastBucketId(currentBlock) + ":"
          + meta.blockLastPosition(currentBlock) + ")");

    if (p + n * weightStride > limit)
      throw new IOException(corruptBlockMessage());
    weightsOffset = p;
    blockWeightMin = meta.blockWeightMin(currentBlock);
    blockWeightMax = meta.blockWeightMax(currentBlock);

    decodedBlockCount++;
    decodedPayloadBytes += (long) (p - base) + (long) n * weightStride;
    blockDecoded = true;
    weightResolved = false;
  }

  private static final String OUT_OF_RANGE_RID =
      " (a decoded RID component is outside the range a bucket id or a position can hold)";

  private static final String OVERWIDE_VARLONG =
      " (a VarLong carries payload bits past the width of a long, which no encoder can produce)";

  private String notAscending(final int previousBucketId, final long previousPosition) {
    return " (postings are not in strictly ascending RID order after #" + previousBucketId + ":" + previousPosition + ")";
  }

  private String corruptBlockMessage() {
    return "sparse-vector block " + currentBlock + " of dim " + meta.dimId() + " in segment '"
        + reader.component().getName() + "' runs past the end of page " + pageNum + "; segment is corrupt";
  }

  /**
   * Decode the weight and tombstone flag of the posting the cursor sits on, once per posting.
   * <p>
   * The weight section is fixed-stride whatever the quantization, so this is a single indexed read
   * plus the dequantization. Doing it here rather than in {@link #decodeBlockIfNeeded} is what makes
   * the block-max skip and the early-abandoned non-essential probe free of weight work: neither ever
   * asks for a weight, and before issue #5467 both paid for a full block of them.
   */
  private void resolveWeight() {
    if (weightResolved)
      return;
    weightResolved = true;
    resolvedWeightCount++;
    // Branching on the stride rather than on the quantization enum keeps this to an int compare on the
    // hot path; the stride IS the quantization here, mapped once in the constructor, so a new width
    // has to be added in both places.
    final int at = weightsOffset + currentInBlock * weightStride;
    if (weightStride == 1) {
      final byte b = pageBuf.get(at);
      if (b == SegmentFormat.INT8_TOMBSTONE_SENTINEL) {
        currentTombstone = true;
        currentWeight = Float.NaN;
      } else {
        currentTombstone = false;
        currentWeight = WeightCodec.dequantizeInt8(b, blockWeightMin, blockWeightMax);
      }
    } else if (weightStride == 2) {
      final short s = pageBuf.getShort(at);
      if (s == SegmentFormat.FP16_TOMBSTONE_SENTINEL) {
        currentTombstone = true;
        currentWeight = Float.NaN;
      } else {
        currentTombstone = false;
        currentWeight = WeightCodec.fromFp16(s);
      }
    } else {
      final int bits = pageBuf.getInt(at);
      if (WeightCodec.isFp32Tombstone(bits)) {
        currentTombstone = true;
        currentWeight = Float.NaN;
      } else {
        currentTombstone = false;
        currentWeight = Float.intBitsToFloat(bits);
      }
    }
  }

  private void materializePosting(final int idxInBlock) {
    pendingDecode = false;
    currentInBlock = idxInBlock;
    setPosition(blockBuckets[idxInBlock], blockPositions[idxInBlock]);
    weightResolved = false;
  }
}
