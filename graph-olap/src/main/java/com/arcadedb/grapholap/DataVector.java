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
package com.arcadedb.grapholap;

/**
 * A typed batch container for vectorized processing. Holds up to {@link #VECTOR_SIZE} values
 * of a single primitive type, stored in contiguous arrays for SIMD-friendly access.
 * <p>
 * Design principles:
 * <ul>
 *   <li>Fixed-size batches (2048) aligned to typical L1/L2 cache sizes</li>
 *   <li>Primitive arrays only — zero boxing, zero GC pressure</li>
 *   <li>Supports "flat" mode (single value broadcast) for factorized joins</li>
 *   <li>Null tracking via boolean mask (1 byte per entry, not a bitset, for SIMD compatibility)</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DataVector {
  public static final int VECTOR_SIZE = 2048;

  public enum Type {
    INT, LONG, DOUBLE, STRING
  }

  private final Type type;
  private int        size;

  // Typed backing arrays — only one is active based on type
  private int[]     intData;
  private long[]    longData;
  private double[]  doubleData;
  private String[]  stringData;
  private boolean[] nullMask;

  // Flat mode: when true, only element at flatIndex is valid,
  // logically broadcast to all positions
  private boolean flat;
  private int     flatIndex;

  public DataVector(final Type type) {
    this.type = type;
    this.size = 0;
    this.flat = false;
    this.flatIndex = 0;

    switch (type) {
    case INT:
      intData = new int[VECTOR_SIZE];
      break;
    case LONG:
      longData = new long[VECTOR_SIZE];
      break;
    case DOUBLE:
      doubleData = new double[VECTOR_SIZE];
      break;
    case STRING:
      stringData = new String[VECTOR_SIZE];
      break;
    }
    nullMask = new boolean[VECTOR_SIZE];
  }

  public void reset() {
    size = 0;
    flat = false;
    flatIndex = 0;
  }

  public Type getType() {
    return type;
  }

  public int getSize() {
    return size;
  }

  public void setSize(final int size) {
    this.size = size;
  }

  public boolean isFlat() {
    return flat;
  }

  public void setFlat(final boolean flat, final int flatIndex) {
    this.flat = flat;
    this.flatIndex = flatIndex;
  }

  public int getFlatIndex() {
    return flatIndex;
  }

  // --- Int accessors ---

  public int getInt(final int index) {
    return intData[flat ? flatIndex : index];
  }

  public void setInt(final int index, final int value) {
    intData[index] = value;
    nullMask[index] = false;
  }

  public int[] getIntData() {
    return intData;
  }

  // --- Long accessors ---

  public long getLong(final int index) {
    return longData[flat ? flatIndex : index];
  }

  public void setLong(final int index, final long value) {
    longData[index] = value;
    nullMask[index] = false;
  }

  public long[] getLongData() {
    return longData;
  }

  // --- Double accessors ---

  public double getDouble(final int index) {
    return doubleData[flat ? flatIndex : index];
  }

  public void setDouble(final int index, final double value) {
    doubleData[index] = value;
    nullMask[index] = false;
  }

  public double[] getDoubleData() {
    return doubleData;
  }

  // --- String accessors ---

  public String getString(final int index) {
    return stringData[flat ? flatIndex : index];
  }

  public void setString(final int index, final String value) {
    stringData[index] = value;
    nullMask[index] = false;
  }

  public String[] getStringData() {
    return stringData;
  }

  // --- Null handling ---

  public boolean isNull(final int index) {
    return nullMask[flat ? flatIndex : index];
  }

  public void setNull(final int index) {
    nullMask[index] = true;
  }

  public boolean[] getNullMask() {
    return nullMask;
  }
}
