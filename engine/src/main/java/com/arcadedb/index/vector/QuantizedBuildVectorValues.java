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

import com.arcadedb.utility.VectorApiSupport;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.util.Arrays;
import java.util.concurrent.CancellationException;

/**
 * What jvector's graph builder reads vectors through when the corpus of an INT8 or BINARY index outgrows the float
 * cache (issue #10543): a row of {@link QuantizedBuildVectors} is dequantized into a scratch vector the reading
 * thread owns, and a row the store does not hold is read from the index pages into a scratch row, so a read allocates
 * nothing in either case.
 * <p>
 * <b>The returned vector is shared</b>: it is only valid until the next {@link #getVector} on the same instance. That
 * is the contract {@link #isValueShared()} announces and the one jvector's own builder is written against: it asks the
 * reader for {@link #threadLocalSupplier()}, which hands every worker a {@link #copy()} of its own, and keeps two of
 * them per worker so the two operands of a distance never collide. Everything this index does with the reader goes
 * through that supplier. Nothing may keep the vector it got, which is why the persistence and the unreachable-node
 * scan keep using {@link ArcadePageVectorValues}, whose vectors are theirs to keep.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class QuantizedBuildVectorValues implements RandomAccessVectorValues {
  private static final VectorTypeSupport VTS = VectorApiSupport.jvectorProvider().getVectorTypeSupport();

  private final QuantizedBuildVectors  store;
  private final NonResidentRows        nonResident;
  private final VectorQuantizationType type;
  private final int                    size;
  private final int                    dimensions;
  private final VectorFloat<?>         sentinel;

  // The reader's own: a copy shares everything above it and none of this
  private final float[]                scratchArray;
  private final VectorFloat<?>         scratch;
  private final boolean                scratchWrapsArray;
  private final byte[]                 row;
  private final float[]                scales = new float[2];

  private QuantizedBuildVectorValues(final QuantizedBuildVectors store, final NonResidentRows nonResident,
      final VectorQuantizationType type, final int size, final int dimensions, final VectorFloat<?> sentinel) {
    this.store = store;
    this.nonResident = nonResident;
    this.type = type;
    this.size = size;
    this.dimensions = dimensions;
    this.sentinel = sentinel;

    this.scratchArray = new float[dimensions];
    this.scratch = VTS.createFloatVector(scratchArray);
    // The provider decides whether a vector wraps the array it is given or copies it; the fast path needs the former
    this.scratchWrapsArray = scratch.get() == scratchArray;
    this.row = new byte[QuantizedBuildVectors.rowBytes(type, dimensions)];
  }

  /**
   * @param store       the resident rows, ordinals {@code [0, store.size())}
   * @param type        the index's quantization
   * @param size        the number of ordinals of the whole corpus, resident or not
   * @param dimensions  the width of a vector
   * @param nonResident how to read the rest, or null when the store holds everything
   */
  static QuantizedBuildVectorValues create(final QuantizedBuildVectors store, final VectorQuantizationType type,
      final int size, final int dimensions, final NonResidentRows nonResident) {
    return new QuantizedBuildVectorValues(store, nonResident, type, size, dimensions, createSentinel(dimensions));
  }

  /** For tests: an INT8 reader over a store that holds the whole corpus, with nothing to fall back on. */
  static QuantizedBuildVectorValues ofStoreOnly(final QuantizedBuildVectors store, final int size, final int dimensions) {
    return create(store, VectorQuantizationType.INT8, size, dimensions, null);
  }

  /**
   * Whether this is the placeholder handed back for an ordinal that could not be read, which a caller that scores
   * what it reads has to be able to tell from a vector. By reference, and one instance is shared by every copy.
   */
  boolean isDeletedSentinel(final VectorFloat<?> vector) {
    return vector == sentinel;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int dimension() {
    return dimensions;
  }

  @Override
  public VectorFloat<?> getVector(final int ordinal) {
    // A graph build is cancelled by interrupting its workers. A read served from memory has no I/O to notice that on,
    // and a build whose corpus is resident would otherwise run to the end of the insertion after a close (issue #7432).
    if (Thread.currentThread().isInterrupted())
      throw new CancellationException("Vector read interrupted (ordinal=" + ordinal + ")");

    if (store.isResident(ordinal)) {
      store.dequantizeInto(ordinal, scratchArray);
      return published();
    }

    if (nonResident != null && ordinal >= 0 && ordinal < size && nonResident.read(ordinal, row, scales)) {
      if (type == VectorQuantizationType.INT8)
        QuantizedBuildVectors.dequantizeInt8(row, 0, dimensions, scales[0], scales[1], scratchArray);
      else
        QuantizedBuildVectors.dequantizeBinary(row, 0, dimensions, scales[0], scratchArray);
      return published();
    }

    // Never null: a deleted or unreadable ordinal must not make jvector's traversal dereference one (issue #3715)
    return sentinel;
  }

  @Override
  public boolean isValueShared() {
    return true;
  }

  @Override
  public RandomAccessVectorValues copy() {
    return new QuantizedBuildVectorValues(store, nonResident, type, size, dimensions, sentinel);
  }

  private VectorFloat<?> published() {
    if (!scratchWrapsArray)
      for (int i = 0; i < dimensions; i++)
        scratch.set(i, scratchArray[i]);
    return scratch;
  }

  /** The same placeholder {@link ArcadePageVectorValues} hands back: the right shape, and nothing a score can be read from. */
  private static VectorFloat<?> createSentinel(final int dimensions) {
    final float[] sentinel = new float[dimensions];
    Arrays.fill(sentinel, 1.0f);
    return VTS.createFloatVector(sentinel);
  }

  /** Reads the quantized row of an ordinal the store does not hold, from wherever the index keeps it. */
  @FunctionalInterface
  interface NonResidentRows {
    /**
     * @param ordinal the ordinal to read
     * @param row     receives the row: {@link QuantizedBuildVectors#rowBytes} bytes
     * @param scales  receives the floats that scale it: the minimum and the maximum, or the median
     *
     * @return false when the row cannot be read
     */
    boolean read(int ordinal, byte[] row, float[] scales);
  }
}
