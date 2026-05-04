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
 * In-memory representation of one dim's posting-list metadata, loaded from the on-disk
 * {@code dim_header} + {@code block_offsets} + {@code skip_list} arrays.
 * <p>
 * {@code blockOffsets} is parallel to {@code blockHeaders}: index {@code i} of either
 * refers to block {@code i} of this posting list. {@code skipList} is shorter, one entry
 * per {@code skipStride} blocks.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class DimMetadata {
  private final int           dimId;
  private final int           postingCount;
  private final int           df;
  private final float         globalMaxWeight;
  private final long[]        blockOffsets;
  private final BlockHeader[] blockHeaders;
  private final SkipEntry[]   skipList;

  public DimMetadata(final int dimId, final int postingCount, final int df, final float globalMaxWeight,
      final long[] blockOffsets, final BlockHeader[] blockHeaders, final SkipEntry[] skipList) {
    if (blockOffsets.length != blockHeaders.length)
      throw new IllegalArgumentException(
          "blockOffsets and blockHeaders must have the same length: " + blockOffsets.length + " vs " + blockHeaders.length);
    this.dimId = dimId;
    this.postingCount = postingCount;
    this.df = df;
    this.globalMaxWeight = globalMaxWeight;
    this.blockOffsets = blockOffsets;
    this.blockHeaders = blockHeaders;
    this.skipList = skipList;
  }

  public int dimId() {
    return dimId;
  }

  public int blockCount() {
    return blockOffsets.length;
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

  public long blockOffset(final int blockIndex) {
    return blockOffsets[blockIndex];
  }

  public BlockHeader blockHeader(final int blockIndex) {
    return blockHeaders[blockIndex];
  }

  public SkipEntry[] skipList() {
    return skipList;
  }
}
