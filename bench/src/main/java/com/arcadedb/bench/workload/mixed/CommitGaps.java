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
package com.arcadedb.bench.workload.mixed;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the longest stretch of a measured window in which nothing committed, across every thread that reports a commit. The
 * stretch before the first commit and the one after the last commit count too. Thread safe and lock free.
 */
final class CommitGaps {
  private final AtomicLong last;
  private final AtomicLong longest = new AtomicLong();

  CommitGaps(final long startNanos) {
    last = new AtomicLong(startNanos);
  }

  /**
   * Records a commit at {@code nanos}. Calls may arrive out of order: an older time never moves the last commit back.
   */
  void committed(final long nanos) {
    final long previous = last.getAndAccumulate(nanos, Math::max);
    if (nanos > previous)
      longest.accumulateAndGet(nanos - previous, Math::max);
  }

  long longestGapNanos(final long endNanos) {
    return Math.max(longest.get(), endNanos - last.get());
  }
}
