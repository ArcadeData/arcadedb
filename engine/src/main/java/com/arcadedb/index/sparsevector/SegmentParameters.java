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

import com.arcadedb.index.sparsevector.SegmentFormat.RidCompression;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;

/**
 * Per-segment encoding parameters. Frozen at segment creation time and persisted in the file
 * header so the reader knows how to decode without external metadata.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SegmentParameters {
  private final int                pageSize;
  private final int                blockSize;
  private final int                skipStride;
  private final WeightQuantization weightQuantization;
  private final RidCompression     ridCompression;

  private SegmentParameters(final Builder b) {
    if (b.pageSize <= 0 || (b.pageSize & (b.pageSize - 1)) != 0)
      throw new IllegalArgumentException("pageSize must be a positive power of two: " + b.pageSize);
    if (b.blockSize < SegmentFormat.MIN_BLOCK_SIZE || b.blockSize > SegmentFormat.MAX_BLOCK_SIZE)
      throw new IllegalArgumentException(
          "blockSize must be within [" + SegmentFormat.MIN_BLOCK_SIZE + ", " + SegmentFormat.MAX_BLOCK_SIZE + "]: " + b.blockSize);
    if (b.skipStride < SegmentFormat.MIN_SKIP_STRIDE || b.skipStride > SegmentFormat.MAX_SKIP_STRIDE)
      throw new IllegalArgumentException(
          "skipStride must be within [" + SegmentFormat.MIN_SKIP_STRIDE + ", " + SegmentFormat.MAX_SKIP_STRIDE + "]: "
              + b.skipStride);

    this.pageSize = b.pageSize;
    this.blockSize = b.blockSize;
    this.skipStride = b.skipStride;
    this.weightQuantization = b.weightQuantization;
    this.ridCompression = b.ridCompression;
  }

  public int pageSize() {
    return pageSize;
  }

  public int blockSize() {
    return blockSize;
  }

  public int skipStride() {
    return skipStride;
  }

  public WeightQuantization weightQuantization() {
    return weightQuantization;
  }

  public RidCompression ridCompression() {
    return ridCompression;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static SegmentParameters defaults() {
    return builder().build();
  }

  public static final class Builder {
    private int                pageSize           = SparseSegmentComponent.DEFAULT_PAGE_SIZE;
    private int                blockSize          = SegmentFormat.DEFAULT_BLOCK_SIZE;
    private int                skipStride         = SegmentFormat.DEFAULT_SKIP_STRIDE;
    private WeightQuantization weightQuantization = WeightQuantization.INT8;
    private RidCompression     ridCompression     = RidCompression.VARINT_DELTA;

    public Builder pageSize(final int v) {
      this.pageSize = v;
      return this;
    }

    public Builder blockSize(final int v) {
      this.blockSize = v;
      return this;
    }

    public Builder skipStride(final int v) {
      this.skipStride = v;
      return this;
    }

    public Builder weightQuantization(final WeightQuantization v) {
      this.weightQuantization = v;
      return this;
    }

    public Builder ridCompression(final RidCompression v) {
      this.ridCompression = v;
      return this;
    }

    public SegmentParameters build() {
      return new SegmentParameters(this);
    }
  }
}
