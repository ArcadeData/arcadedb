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
package com.arcadedb.function.sql.vector;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a sparse vector using a map of non-zero indices to values.
 * Efficient for vectors with many zero values (typical in NLP and sparse embeddings like SPLADE).
 * <p>
 * Storage: Map<Integer, Float> where key=index, value=non-zero value
 * Supports dynamic dimensionality (dimension inferred from max index or explicitly set).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SparseVector {
  private final Map<Integer, Float> values;
  private final int                 dimensions;

  /**
   * Create a sparse vector with explicit dimensions.
   */
  public SparseVector(final int dimensions) {
    this.dimensions = dimensions;
    this.values = new HashMap<>();
  }

  /**
   * Create a sparse vector from indices and values arrays.
   */
  public SparseVector(final int[] indices, final float[] values) {
    if (indices.length != values.length)
      throw new IllegalArgumentException("Indices and values must have same length");

    this.values = new HashMap<>(indices.length);
    int maxIndex = 0;

    for (int i = 0; i < indices.length; i++) {
      if (indices[i] < 0)
        throw new IllegalArgumentException("Index must be non-negative, found: " + indices[i]);
      this.values.put(indices[i], values[i]);
      if (indices[i] > maxIndex)
        maxIndex = indices[i];
    }

    this.dimensions = maxIndex + 1;
  }

  /**
   * Create a sparse vector from indices and values arrays with explicit dimensions.
   */
  public SparseVector(final int[] indices, final float[] values, final int dimensions) {
    if (indices.length != values.length)
      throw new IllegalArgumentException("Indices and values must have same length");

    this.dimensions = dimensions;
    this.values = new HashMap<>(indices.length);

    for (int i = 0; i < indices.length; i++) {
      if (indices[i] < 0 || indices[i] >= dimensions)
        throw new IllegalArgumentException("Index " + indices[i] + " out of bounds for dimension " + dimensions);
      this.values.put(indices[i], values[i]);
    }
  }

  /**
   * Set a value at the given index.
   */
  public void set(final int index, final float value) {
    if (index < 0 || index >= dimensions)
      throw new IndexOutOfBoundsException("Index " + index + " out of bounds for dimension " + dimensions);

    if (value != 0.0f) {
      values.put(index, value);
    } else {
      values.remove(index);
    }
  }

  /**
   * Get a value at the given index (returns 0.0 for missing values).
   */
  public float get(final int index) {
    if (index < 0 || index >= dimensions)
      throw new IndexOutOfBoundsException("Index " + index + " out of bounds for dimension " + dimensions);

    return values.getOrDefault(index, 0.0f);
  }

  /**
   * Get the number of non-zero elements.
   */
  public int getNonZeroCount() {
    return values.size();
  }

  /**
   * Get the dimensionality of the vector.
   */
  public int getDimensions() {
    return dimensions;
  }

  /**
   * Get the underlying map of indices to values.
   */
  public Map<Integer, Float> getValues() {
    return values;
  }

  /**
   * Convert to dense float array.
   */
  public float[] toDense() {
    final float[] dense = new float[dimensions];
    for (final Map.Entry<Integer, Float> entry : values.entrySet()) {
      dense[entry.getKey()] = entry.getValue();
    }
    return dense;
  }

  /**
   * Calculate dot product with another sparse vector. Dimensions are only used to size a dense
   * expansion ({@link #toDense()}); the dot product itself only needs the indices the two vectors
   * have in common (it is implicitly 0 over every non-overlapping index), so vectors whose inferred
   * dimensions differ are still valid operands.
   */
  public float dotProduct(final SparseVector other) {
    // Iterate over the smaller vector to optimize
    final Map<Integer, Float> smallerMap = this.getNonZeroCount() <= other.getNonZeroCount() ? this.values : other.values;
    final Map<Integer, Float> largerMap = smallerMap == this.values ? other.values : this.values;

    // Widen before multiplying and sum in double, as the dense VectorUtils.dotProduct does (issue #12023)
    double result = 0.0;
    for (final Map.Entry<Integer, Float> entry : smallerMap.entrySet()) {
      final Float otherValue = largerMap.get(entry.getKey());
      if (otherValue != null)
        result += (double) entry.getValue() * otherValue;
    }

    return (float) result;
  }

  /**
   * Calculate magnitude (L2 norm).
   */
  public float magnitude() {
    return (float) Math.sqrt(sumOfSquares());
  }

  private double sumOfSquares() {
    double sumSquares = 0.0;
    for (final float value : values.values())
      sumSquares += (double) value * value;
    return sumSquares;
  }

  /**
   * Normalize to unit length.
   */
  public SparseVector normalize() {
    // The magnitude stays in double (issue #12853): narrowed to a float, a magnitude beyond Float.MAX_VALUE turned every value into 0
    final double mag = Math.sqrt(sumOfSquares());
    if (mag == 0.0)
      return new SparseVector(dimensions); // Return zero vector

    final int[] indices = new int[values.size()];
    final float[] normalized = new float[values.size()];
    int idx = 0;
    for (final Map.Entry<Integer, Float> entry : values.entrySet()) {
      indices[idx] = entry.getKey();
      normalized[idx] = (float) (entry.getValue() / mag);
      idx++;
    }

    return new SparseVector(indices, normalized, dimensions);
  }

  @Override
  public String toString() {
    return String.format("SparseVector(dim=%d, nnz=%d, values=%s)", dimensions, values.size(), values);
  }
}
