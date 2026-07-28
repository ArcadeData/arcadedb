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

/**
 * Per-dim metadata for a {@link PaginatedSegmentReader} (page-component-backed segment). Block
 * locations are stored as parallel {@code int[] blockPageNums} + {@code short[] blockOffsets}
 * (both unsigned in concept; offsets are within-page) instead of file offsets, since pages are
 * the addressable unit in this format.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PaginatedDimMetadata {
  private final int           dimId;
  private final int           postingCount;
  private final int           df;
  private final float         globalMaxWeight;
  private final int[]         blockPageNums;
  private final short[]       blockOffsets;
  private final SkipEntry[]   skipList;

  // Per-block header fields, exploded into parallel primitive arrays instead of a BlockHeader[] of
  // records. The DAAT hot paths - locating the block containing a probe target, seeking, reading a
  // block max - walk these arrays and nothing else, so what used to be an array load plus a record
  // dereference plus a RID dereference per block is now a pair of array loads (issue #5467). It also
  // drops the per-block object graph: a BlockHeader plus its two RIDs cost ~80 bytes per block, i.e.
  // tens of MB of live metadata on a large learned-sparse index.
  private final int[]     blockFirstBucketIds;
  private final long[]    blockFirstPositions;
  private final int[]     blockLastBucketIds;
  private final long[]    blockLastPositions;
  private final short[]   blockPostingCounts;
  private final float[]   blockMaxWeights;
  private final float[]   blockWeightMins;
  private final float[]   blockWeightMaxs;
  private final boolean[] blockHasTombstones;

  // Skip list, likewise exploded. The seek path binary-searches it on every jump.
  private final int[]   skipFirstBucketIds;
  private final long[]  skipFirstPositions;
  private final float[] skipMaxWeightsToEnd;
  private final int[]   skipBlockIndexes;

  public PaginatedDimMetadata(final int dimId, final int postingCount, final int df, final float globalMaxWeight,
      final int[] blockPageNums, final short[] blockOffsets, final BlockHeader[] blockHeaders, final SkipEntry[] skipList) {
    if (blockPageNums.length != blockOffsets.length || blockOffsets.length != blockHeaders.length)
      throw new IllegalArgumentException("blockPageNums, blockOffsets, blockHeaders must have matching length: "
          + blockPageNums.length + ", " + blockOffsets.length + ", " + blockHeaders.length);
    this.dimId = dimId;
    this.postingCount = postingCount;
    this.df = df;
    this.globalMaxWeight = globalMaxWeight;
    this.blockPageNums = blockPageNums;
    this.blockOffsets = blockOffsets;
    this.skipList = skipList;

    final int n = blockHeaders.length;
    this.blockFirstBucketIds = new int[n];
    this.blockFirstPositions = new long[n];
    this.blockLastBucketIds = new int[n];
    this.blockLastPositions = new long[n];
    this.blockPostingCounts = new short[n];
    this.blockMaxWeights = new float[n];
    this.blockWeightMins = new float[n];
    this.blockWeightMaxs = new float[n];
    this.blockHasTombstones = new boolean[n];
    for (int i = 0; i < n; i++) {
      final BlockHeader h = blockHeaders[i];
      this.blockFirstBucketIds[i] = h.firstRid().getBucketId();
      this.blockFirstPositions[i] = h.firstRid().getPosition();
      this.blockLastBucketIds[i] = h.lastRid().getBucketId();
      this.blockLastPositions[i] = h.lastRid().getPosition();
      this.blockPostingCounts[i] = (short) h.postingCount();
      this.blockMaxWeights[i] = h.bmwUpperBound();
      this.blockWeightMins[i] = h.weightMin();
      this.blockWeightMaxs[i] = h.weightMax();
      this.blockHasTombstones[i] = h.hasTombstones();
    }

    final int m = skipList.length;
    this.skipFirstBucketIds = new int[m];
    this.skipFirstPositions = new long[m];
    this.skipMaxWeightsToEnd = new float[m];
    this.skipBlockIndexes = new int[m];
    for (int i = 0; i < m; i++) {
      final SkipEntry e = skipList[i];
      this.skipFirstBucketIds[i] = e.firstRid().getBucketId();
      this.skipFirstPositions[i] = e.firstRid().getPosition();
      this.skipMaxWeightsToEnd[i] = e.maxWeightToEnd();
      this.skipBlockIndexes[i] = e.blockIndex();
    }
  }

  public int dimId() {
    return dimId;
  }

  public int blockCount() {
    return blockPageNums.length;
  }

  public int postingCount() {
    return postingCount;
  }

  public int df() {
    return df;
  }

  public float globalMaxWeight() {
    return globalMaxWeight;
  }

  public int blockPageNum(final int blockIndex) {
    return blockPageNums[blockIndex];
  }

  public int blockOffset(final int blockIndex) {
    // {@code blockOffsets} is a {@code short[]} so each entry fits in 2 bytes (page content sizes
    // up to 64 KiB). The {@code & 0xFFFF} mask converts the signed-short read to an unsigned
    // 0..65535 int - without it, a block whose offset is &gt;= 32768 would sign-extend to a
    // negative value and the page-read would land at the wrong byte.
    return blockOffsets[blockIndex] & 0xFFFF;
  }

  public int blockFirstBucketId(final int blockIndex) {
    return blockFirstBucketIds[blockIndex];
  }

  public long blockFirstPosition(final int blockIndex) {
    return blockFirstPositions[blockIndex];
  }

  public int blockLastBucketId(final int blockIndex) {
    return blockLastBucketIds[blockIndex];
  }

  public long blockLastPosition(final int blockIndex) {
    return blockLastPositions[blockIndex];
  }

  /** Postings in the block. Stored as a short (blockSize is bounded by the format) and read unsigned. */
  public int blockPostingCount(final int blockIndex) {
    return blockPostingCounts[blockIndex] & 0xFFFF;
  }

  public float blockMaxWeight(final int blockIndex) {
    return blockMaxWeights[blockIndex];
  }

  public float blockWeightMin(final int blockIndex) {
    return blockWeightMins[blockIndex];
  }

  public float blockWeightMax(final int blockIndex) {
    return blockWeightMaxs[blockIndex];
  }

  public boolean blockHasTombstones(final int blockIndex) {
    return blockHasTombstones[blockIndex];
  }

  public int skipCount() {
    return skipBlockIndexes.length;
  }

  public int skipFirstBucketId(final int i) {
    return skipFirstBucketIds[i];
  }

  public long skipFirstPosition(final int i) {
    return skipFirstPositions[i];
  }

  public float skipMaxWeightToEnd(final int i) {
    return skipMaxWeightsToEnd[i];
  }

  public int skipBlockIndex(final int i) {
    return skipBlockIndexes[i];
  }

  public SkipEntry[] skipList() {
    return skipList;
  }
}
