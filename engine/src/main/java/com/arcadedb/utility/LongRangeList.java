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
package com.arcadedb.utility;

import java.math.BigInteger;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/**
 * Immutable, lazily evaluated list of longs described by a start value, a step and the number of elements.
 * The i-th element is computed as {@code start + i * step}, so the list occupies a constant amount of heap
 * regardless of its length: no backing array is ever allocated.
 * <p>
 * It is the representation used by the {@code range()} function (see {@code com.arcadedb.function.coll.RangeFunction}),
 * which used to materialise every element into an {@code ArrayList} - a single query could then exhaust the
 * whole JVM heap (advisory GHSA-xmjm-8q85-g778). Streaming consumers (UNWIND, IN, indexing, size()) now pay
 * nothing, while the very few operations that really need a copy (sort, serialization to JSON, ...) materialise
 * at most the number of elements allowed by {@code arcadedb.queryMaxRangeSize}.
 * <p>
 * All the mutating methods of {@link List} throw {@link UnsupportedOperationException}, inherited from
 * {@link AbstractList}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LongRangeList extends AbstractList<Long> implements RandomAccess {
  private static final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);

  private final long start;
  private final long step;
  private final int  size;

  /**
   * @param start first element of the range
   * @param step  distance between two consecutive elements, cannot be zero
   * @param size  number of elements, cannot be negative
   */
  public LongRangeList(final long start, final long step, final int size) {
    if (step == 0)
      throw new IllegalArgumentException("Range step cannot be zero");
    if (size < 0)
      throw new IllegalArgumentException("Range size cannot be negative");
    this.start = start;
    this.step = step;
    this.size = size;
  }

  /**
   * Returns how many elements the range [start, end] with the given step contains, without building it.
   * The count is computed in arbitrary precision because {@code end - start} can overflow a long, and it is
   * clamped to {@link Long#MAX_VALUE}: the caller only needs to compare it against a much smaller limit.
   */
  public static long cardinality(final long start, final long end, final long step) {
    if (step == 0)
      throw new IllegalArgumentException("Range step cannot be zero");
    if (step > 0 ? start > end : start < end)
      return 0L;

    // (end - start) and step always share the same sign here, so the quotient is positive and the
    // truncation applied by divide() is the floor required to count the steps that fit in the interval.
    final BigInteger count = BigInteger.valueOf(end)
        .subtract(BigInteger.valueOf(start))
        .divide(BigInteger.valueOf(step))
        .add(BigInteger.ONE);

    return count.compareTo(MAX_LONG) > 0 ? Long.MAX_VALUE : count.longValue();
  }

  @Override
  public Long get(final int index) {
    if (index < 0 || index >= size)
      throw new IndexOutOfBoundsException("Index " + index + " out of bounds for range of size " + size);
    // By construction start + (size - 1) * step never exceeds the range end, so no overflow is possible here.
    return start + index * step;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public boolean contains(final Object o) {
    return indexOf(o) > -1;
  }

  /**
   * O(1) lookup: an element belongs to the range when its distance from the start is an exact multiple of the
   * step and the resulting index falls inside the range.
   */
  @Override
  public int indexOf(final Object o) {
    if (!(o instanceof Number number) || o instanceof Double || o instanceof Float)
      return -1;

    final long value = number.longValue();
    final long distance;
    try {
      distance = Math.subtractExact(value, start);
    } catch (final ArithmeticException e) {
      // The distance does not fit in a long: fall back to arbitrary precision instead of wrapping around.
      final BigInteger bigDistance = BigInteger.valueOf(value).subtract(BigInteger.valueOf(start));
      final BigInteger[] divMod = bigDistance.divideAndRemainder(BigInteger.valueOf(step));
      if (divMod[1].signum() != 0)
        return -1;
      return divMod[0].compareTo(BigInteger.valueOf(size)) < 0 && divMod[0].signum() >= 0 ? divMod[0].intValue() : -1;
    }

    if (distance % step != 0)
      return -1;
    final long index = distance / step;
    return index >= 0 && index < size ? (int) index : -1;
  }

  @Override
  public int lastIndexOf(final Object o) {
    // Every element of a range is unique (the step is never zero), so the last occurrence is the first one.
    return indexOf(o);
  }

  @Override
  public Iterator<Long> iterator() {
    return new Iterator<>() {
      private int next = 0;

      @Override
      public boolean hasNext() {
        return next < size;
      }

      @Override
      public Long next() {
        if (next >= size)
          throw new NoSuchElementException();
        return start + (next++) * step;
      }
    };
  }

  @Override
  public List<Long> subList(final int fromIndex, final int toIndex) {
    if (fromIndex < 0 || toIndex > size || fromIndex > toIndex)
      throw new IndexOutOfBoundsException("Invalid sub-list range [" + fromIndex + ", " + toIndex + ") for size " + size);
    // A slice of a range is still a range: keep it lazy instead of returning a view that walks the parent.
    return new LongRangeList(start + fromIndex * step, step, toIndex - fromIndex);
  }

  public long getStart() {
    return start;
  }

  public long getStep() {
    return step;
  }
}
