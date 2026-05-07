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
 * The wire / storage encoding for vectors flowing into and out of an {@code LSM_VECTOR} index.
 * Distinct from {@link VectorQuantizationType} (the index-internal compression scheme):
 * encoding is what the application hands over and what gets persisted in the document property
 * column; quantization is what the index does <i>to</i> that data internally.
 *
 * <ul>
 *   <li>{@link #FLOAT32} (default) - the document's vector property is {@code ARRAY_OF_FLOATS}
 *       (4 bytes per dim). Backwards-compatible behavior; nothing changes vs. pre-#4132.</li>
 *   <li>{@link #INT8} - the document's vector property is {@code BINARY} (the ArcadeDB type that
 *       maps to a Java {@code byte[]}; one signed byte per dim). Callers using providers that emit
 *       int8 directly (Cohere `int8` endpoints, OpenAI `text-embedding-3-large` reduced precision,
 *       Sentence Transformers with int8 quantization) skip a precision-losing client-side
 *       {@code int8 -> float32 -> server} round trip. The HTTP payload and document-bucket storage
 *       shrink 4x. JVector 4.0.0-rc.8 still requires {@code float32} for HNSW build/search
 *       internally, so the engine dequantizes on the read path; native int8 HNSW is tracked at
 *       <a href="https://github.com/datastax/jvector/issues/665">datastax/jvector#665</a>.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum VectorEncoding {
  /** Float32 vector property; 4 bytes per dimension. */
  FLOAT32,
  /**
   * Signed int8 vector property; 1 byte per dimension. Bytes are dequantized to float using
   * {@code value / 127.0f} (Cohere / OpenAI int8 calibration convention) before the HNSW graph
   * sees them - lossless within the int8 source's own resolution.
   * <p>
   * <b>Calibration note:</b> Java's {@code byte} range is {@code [-128, 127]} but the
   * Cohere/OpenAI calibration only emits {@code [-127, 127]}. A raw {@code -128} would dequantize
   * to {@code -1.0079f}, breaking the unit-norm assumption COSINE similarity relies on. The
   * dequantizer in {@link com.arcadedb.index.vector.VectorUtils#dequantizeInt8ToFloat(byte[])}
   * therefore clamps {@code -128} up to {@code -127}; callers feeding non-Cohere/OpenAI byte
   * sources should be aware that this is a silent numeric correction for the {@code -128} edge
   * case (no impact on values in {@code [-127, 127]}). The dequantizer also emits a one-time
   * {@code WARNING} the first time a {@code -128} byte is seen so operators can investigate.
   */
  INT8;

  /**
   * Resolves a string into a {@link VectorEncoding} with a uniform error message; shared by
   * {@code TypeLSMVectorIndexBuilder.withEncoding(String)} and the bucket-level builder so the
   * accepted-values list cannot drift between the two entry points.
   *
   * @param name the encoding name (case-insensitive)
   *
   * @return the matching enum constant
   *
   * @throws IllegalArgumentException if {@code name} is not a recognized encoding
   */
  public static VectorEncoding fromString(final String name) {
    try {
      return VectorEncoding.valueOf(name.toUpperCase());
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid vector encoding: " + name + ". Supported values: FLOAT32, INT8", e);
    }
  }
}
