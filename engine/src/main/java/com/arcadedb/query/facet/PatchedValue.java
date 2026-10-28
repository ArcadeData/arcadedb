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
package com.arcadedb.query.facet;

import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.index.columnar.ValueSink;

import java.util.Arrays;

/**
 * The value of one property of one record read from the record itself, filled once by {@code ColumnarValueExtractor.extract} (it is
 * the {@link ValueSink}; {@link #reset()} makes the instance reusable for the next record) and read back by the collectors through the getters. Ordinals are in the generation of the snapshot's
 * dictionary, ascending; a value the dictionary does not hold yet is an unknown.
 */
public final class PatchedValue implements ValueSink {
  private static final int[]    NO_ORDINALS = new int[0];
  private static final Object[] NO_UNKNOWNS = new Object[0];

  private int      ordinalCount;
  private int[]    ordinals     = NO_ORDINALS;
  private int      unknownCount;
  private Object[] unknowns     = NO_UNKNOWNS;
  private boolean  missing;
  private boolean  hasLong;
  private long     longValue;
  private boolean  hasDouble;
  private double   doubleValue;

  /**
   * Test factory: the values the dictionary holds become ordinals (ascending, deduplicated), the others unknowns; no value at all is
   * a missing value.
   */
  public static PatchedValue ofValues(final GlobalDictionary dictionary, final Object... values) {
    final PatchedValue result = new PatchedValue();
    if (values.length == 0) {
      result.missing();
      return result;
    }
    final int[] known = new int[values.length];
    int knownCount = 0;
    for (final Object value : values) {
      final int ordinal = dictionary.ordinalOf(value);
      if (ordinal >= 0)
        known[knownCount++] = ordinal;
      else
        result.unknownValue(value);
    }
    Arrays.sort(known, 0, knownCount);
    int distinct = 0;
    for (int i = 0; i < knownCount; i++)
      if (i == 0 || known[i] != known[i - 1])
        known[distinct++] = known[i];
    if (distinct > 0)
      result.ordinals(known, distinct);
    return result;
  }

  @Override
  public void ordinal(final int o) {
    if (ordinals.length < 1)
      ordinals = new int[4];
    ordinals[0] = o;
    ordinalCount = 1;
  }

  @Override
  public void ordinals(final int[] sorted, final int count) {
    if (ordinals.length < count)
      ordinals = new int[Math.max(4, count)];
    System.arraycopy(sorted, 0, ordinals, 0, count);
    ordinalCount = count;
  }

  @Override
  public void longValue(final long v) {
    hasLong = true;
    longValue = v;
  }

  @Override
  public void doubleValue(final double v) {
    hasDouble = true;
    doubleValue = v;
  }

  @Override
  public void missing() {
    missing = true;
  }

  @Override
  public void unknownValue(final Object normalized) {
    if (unknownCount == unknowns.length)
      unknowns = Arrays.copyOf(unknowns, Math.max(4, unknownCount * 2));
    unknowns[unknownCount++] = normalized;
  }

  /** Clears the value so the instance can be filled for the next record; the buffers are kept. */
  public void reset() {
    ordinalCount = 0;
    Arrays.fill(unknowns, 0, unknownCount, null);
    unknownCount = 0;
    missing = false;
    hasLong = false;
    hasDouble = false;
  }

  public boolean isMissing() {
    return missing;
  }

  public int ordinalCount() {
    return ordinalCount;
  }

  /** The i-th ordinal, ascending. Not named {@code ordinal}: {@link #ordinal(int)} is the sink method that sets the single ordinal. */
  public int ordinalAt(final int i) {
    return ordinals[i];
  }

  public int unknownCount() {
    return unknownCount;
  }

  public Object unknown(final int i) {
    return unknowns[i];
  }

  public boolean hasLong() {
    return hasLong;
  }

  public long longValue() {
    return longValue;
  }

  public boolean hasDouble() {
    return hasDouble;
  }

  public double doubleValue() {
    return doubleValue;
  }
}
