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

/**
 * Counts per dictionary ordinal for one facet. {@link #choose} picks the representation: a dense array when the match set is large
 * against the cardinality, a hash map otherwise, so the memory is proportional to what is counted and not to the dictionary.
 * <p>
 * Not thread safe: counting is sequential on the query thread.
 */
public interface FacetCounter {
  /** Largest cardinality counted in a dense array. */
  int MAX_DENSE_CARDINALITY = 100_000;

  /** Receives one non-zero count. */
  @FunctionalInterface
  interface IntLongConsumer {
    void accept(int ordinal, long count);
  }

  void increment(int ordinal);

  void add(int ordinal, long n);

  long get(int ordinal);

  void forEachNonZero(IntLongConsumer consumer);

  /** The heap held by the counter, for the memory budget. */
  long estimatedBytes();

  /** Dense when {@code cardinality <= 100_000 && matchCount * 8 >= cardinality}, hash otherwise. */
  static FacetCounter choose(final long matchCount, final int cardinality) {
    if (cardinality <= MAX_DENSE_CARDINALITY && matchCount >= (cardinality + 7L) / 8)
      return new DenseFacetCounter(cardinality);
    return new HashFacetCounter((int) Math.min(matchCount, cardinality));
  }
}
