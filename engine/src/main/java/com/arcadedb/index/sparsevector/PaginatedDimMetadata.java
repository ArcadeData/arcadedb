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
  private final BlockHeader[] blockHeaders;
  private final SkipEntry[]   skipList;

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
    this.blockHeaders = blockHeaders;
    this.skipList = skipList;
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

  public BlockHeader blockHeader(final int blockIndex) {
    return blockHeaders[blockIndex];
  }

  public SkipEntry[] skipList() {
    return skipList;
  }
}
