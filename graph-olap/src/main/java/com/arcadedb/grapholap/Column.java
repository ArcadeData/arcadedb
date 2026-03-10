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
 * A single typed column in the columnar property store, indexed by dense node ID.
 * <p>
 * Stores values in flat primitive arrays for maximum cache efficiency and
 * SIMD-friendly auto-vectorization by the JVM:
 * <ul>
 *   <li>{@code INT}: {@code int[nodeCount]}</li>
 *   <li>{@code LONG}: {@code long[nodeCount]}</li>
 *   <li>{@code DOUBLE}: {@code double[nodeCount]}</li>
 *   <li>{@code STRING}: dictionary-encoded as {@code int[nodeCount]} codes + {@link DictionaryEncoding}</li>
 * </ul>
 * <p>
 * Null values are tracked via a compact bitset: {@code long[ceil(nodeCount/64)]},
 * using 1 bit per node (64x more compact than {@code boolean[]}).
 * All values start as null; setting a value clears the null bit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Column {

  public enum Type {
    INT, LONG, DOUBLE, STRING
  }

  private final String name;
  private final Type   type;
  private final int    nodeCount;

  // Typed backing arrays — only one is active based on type
  private int[]    intData;
  private long[]   longData;
  private double[] doubleData;

  // For STRING type: dictionary-encoded codes (index into dictionary)
  private int[]               stringCodes;
  private DictionaryEncoding  dictionary;

  // Null tracking: bit i is set if node i IS null (all start as null)
  private final long[] nullBitset;

  Column(final String name, final Type type, final int nodeCount) {
    this.name = name;
    this.type = type;
    this.nodeCount = nodeCount;

    switch (type) {
    case INT:
      intData = new int[nodeCount];
      break;
    case LONG:
      longData = new long[nodeCount];
      break;
    case DOUBLE:
      doubleData = new double[nodeCount];
      break;
    case STRING:
      stringCodes = new int[nodeCount];
      dictionary = new DictionaryEncoding();
      break;
    }

    // All values start as null
    nullBitset = new long[(nodeCount + 63) >>> 6];
    for (int i = 0; i < nullBitset.length; i++)
      nullBitset[i] = ~0L;
    // Clear bits beyond nodeCount in the last word
    final int extraBits = nodeCount & 63;
    if (extraBits > 0 && nullBitset.length > 0)
      nullBitset[nullBitset.length - 1] = (1L << extraBits) - 1;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public int getNodeCount() {
    return nodeCount;
  }

  // --- Null handling ---

  public boolean isNull(final int nodeId) {
    return (nullBitset[nodeId >>> 6] & (1L << (nodeId & 63))) != 0;
  }

  public void setNull(final int nodeId) {
    nullBitset[nodeId >>> 6] |= (1L << (nodeId & 63));
  }

  private void clearNull(final int nodeId) {
    nullBitset[nodeId >>> 6] &= ~(1L << (nodeId & 63));
  }

  /**
   * Returns the null bitset for vectorized null-checking.
   * Bit i is set if node i is null.
   */
  public long[] getNullBitset() {
    return nullBitset;
  }

  /**
   * Counts the number of non-null values in this column.
   */
  public int countNonNull() {
    int nullCount = 0;
    for (final long word : nullBitset)
      nullCount += Long.bitCount(word);
    return nodeCount - nullCount;
  }

  // --- Int accessors ---

  public int getInt(final int nodeId) {
    return intData[nodeId];
  }

  public void setInt(final int nodeId, final int value) {
    intData[nodeId] = value;
    clearNull(nodeId);
  }

  public int[] getIntData() {
    return intData;
  }

  // --- Long accessors ---

  public long getLong(final int nodeId) {
    return longData[nodeId];
  }

  public void setLong(final int nodeId, final long value) {
    longData[nodeId] = value;
    clearNull(nodeId);
  }

  public long[] getLongData() {
    return longData;
  }

  // --- Double accessors ---

  public double getDouble(final int nodeId) {
    return doubleData[nodeId];
  }

  public void setDouble(final int nodeId, final double value) {
    doubleData[nodeId] = value;
    clearNull(nodeId);
  }

  public double[] getDoubleData() {
    return doubleData;
  }

  // --- String accessors (dictionary-encoded) ---

  public String getString(final int nodeId) {
    return dictionary.decode(stringCodes[nodeId]);
  }

  public void setString(final int nodeId, final String value) {
    stringCodes[nodeId] = dictionary.encode(value);
    clearNull(nodeId);
  }

  /**
   * Returns the raw dictionary codes array for vectorized processing.
   * Each entry is an index into the dictionary.
   */
  public int[] getStringCodes() {
    return stringCodes;
  }

  /**
   * Returns the dictionary encoding for this string column.
   */
  public DictionaryEncoding getDictionary() {
    return dictionary;
  }

  /**
   * Returns approximate memory usage in bytes.
   */
  public long getMemoryUsageBytes() {
    long bytes = (long) nullBitset.length * Long.BYTES;
    switch (type) {
    case INT:
      bytes += (long) intData.length * Integer.BYTES;
      break;
    case LONG:
      bytes += (long) longData.length * Long.BYTES;
      break;
    case DOUBLE:
      bytes += (long) doubleData.length * Double.BYTES;
      break;
    case STRING:
      bytes += (long) stringCodes.length * Integer.BYTES;
      if (dictionary != null)
        bytes += dictionary.getMemoryUsageBytes();
      break;
    }
    return bytes;
  }
}
