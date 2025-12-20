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
package com.arcadedb.index.vector;

/**
 * Base class for quantization metadata stored with each vector.
 * This metadata is required for accurate dequantization when retrieving vectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class VectorQuantizationMetadata {
  /**
   * Returns the quantization type for this metadata.
   *
   * @return the quantization type
   */
  public abstract VectorQuantizationType getType();

  /**
   * Metadata for INT8 quantization.
   * Stores quantized bytes and min/max values used for min-max scaling during quantization.
   */
  public static class Int8QuantizationMetadata extends VectorQuantizationMetadata {
    public final byte[] quantized;
    public final float min;
    public final float max;

    /**
     * Creates INT8 quantization metadata with quantized bytes and min/max values.
     *
     * @param quantized the quantized byte array
     * @param min       the minimum value in the original vector
     * @param max       the maximum value in the original vector
     */
    public Int8QuantizationMetadata(final byte[] quantized, final float min, final float max) {
      this.quantized = quantized;
      this.min = min;
      this.max = max;
    }

    @Override
    public VectorQuantizationType getType() {
      return VectorQuantizationType.INT8;
    }
  }

  /**
   * Metadata for BINARY quantization.
   * Stores packed bits, median value used as threshold, and the original vector length.
   */
  public static class BinaryQuantizationMetadata extends VectorQuantizationMetadata {
    public final byte[] packed;
    public final float median;
    public final int originalLength;

    /**
     * Creates BINARY quantization metadata with packed bits, median and original length.
     *
     * @param packed         the packed binary data (bit array as bytes)
     * @param median         the median value used as quantization threshold
     * @param originalLength the original vector length before packing bits
     */
    public BinaryQuantizationMetadata(final byte[] packed, final float median, final int originalLength) {
      this.packed = packed;
      this.median = median;
      this.originalLength = originalLength;
    }

    @Override
    public VectorQuantizationType getType() {
      return VectorQuantizationType.BINARY;
    }
  }
}
