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

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Wrapper around RandomAccessVectorValues that tracks progress during graph building.
 * JVector calls getVector() multiple times per node during construction, so we track
 * unique ordinals accessed to estimate progress.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ProgressTrackingVectorValues implements RandomAccessVectorValues {
  private final RandomAccessVectorValues  delegate;
  private final AtomicLong                totalAccesses;
  private final AtomicInteger             uniqueOrdinalsAccessed;
  private final BiConsumer<Integer, Long> progressCallback; // (uniqueOrdinals, totalAccesses)
  private final long                      progressInterval; // Report every N accesses
  private final Set<Integer>              accessedOrdinals;

  /**
   * Create a progress tracking wrapper.
   *
   * @param delegate         The underlying vector values
   * @param progressCallback Callback to report progress (uniqueOrdinals, totalAccesses)
   * @param progressInterval Report progress every N vector accesses
   */
  public ProgressTrackingVectorValues(final RandomAccessVectorValues delegate,
      final BiConsumer<Integer, Long> progressCallback, final long progressInterval) {
    this.delegate = delegate;
    this.totalAccesses = new AtomicLong(0);
    this.uniqueOrdinalsAccessed = new AtomicInteger(0);
    this.progressCallback = progressCallback;
    this.progressInterval = progressInterval;
    this.accessedOrdinals = Collections.newSetFromMap(new ConcurrentHashMap<>());
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public int dimension() {
    return delegate.dimension();
  }

  @Override
  public VectorFloat<?> getVector(final int ordinal) {
    final VectorFloat<?> result = delegate.getVector(ordinal);

    // Track progress
    final long accesses = totalAccesses.incrementAndGet();

    // Track unique ordinals
    if (accessedOrdinals.add(ordinal)) {
      uniqueOrdinalsAccessed.incrementAndGet();
    }

    // Report progress at intervals
    if (progressCallback != null && accesses % progressInterval == 0) {
      progressCallback.accept(uniqueOrdinalsAccessed.get(), accesses);
    }

    return result;
  }

  @Override
  public boolean isValueShared() {
    return delegate.isValueShared();
  }

  @Override
  public RandomAccessVectorValues copy() {
    // Return a new wrapper with the same tracking state
    return new ProgressTrackingVectorValues(delegate.copy(), progressCallback, progressInterval);
  }

  /**
   * Get total number of vector accesses (getVector calls).
   */
  public long getTotalAccesses() {
    return totalAccesses.get();
  }

  /**
   * Get number of unique ordinals accessed during graph building.
   */
  public int getUniqueOrdinalsAccessed() {
    return uniqueOrdinalsAccessed.get();
  }
}
