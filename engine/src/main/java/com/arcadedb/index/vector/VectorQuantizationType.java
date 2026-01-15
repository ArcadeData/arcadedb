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
 * Enumeration of supported vector quantization types for LSM vector indexes.
 * Quantization reduces memory usage by compressing float vectors at the cost of some accuracy.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum VectorQuantizationType {
  /**
   * No quantization applied. Vectors stored as float32 (4 bytes per dimension).
   * This is the default and maintains full accuracy.
   */
  NONE,

  /**
   * INT8 quantization using min-max scaling to [-128, 127] range.
   * Provides 4x memory compression (4 bytes/value → 1 byte/value).
   * Accuracy: typically &lt;2% distance error for most similarity search use cases.
   */
  INT8,

  /**
   * Binary quantization using median threshold to 1 bit per dimension.
   * Provides 32x memory compression (4 bytes/value → 1 bit/value).
   * Uses Hamming distance instead of cosine/euclidean.
   * Suitable for approximate search with reranking.
   */
  BINARY,

  /**
   * Product Quantization (PQ) using JVector's native implementation.
   * Divides vectors into M subspaces with K clusters each.
   * Provides configurable compression (typically 16x-64x) via pqSubspaces parameter.
   * Enables zero-disk-I/O approximate search by keeping compressed vectors in memory.
   * Best for RAG systems where microsecond latency is required.
   */
  PRODUCT
}
