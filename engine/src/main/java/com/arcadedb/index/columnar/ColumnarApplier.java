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
package com.arcadedb.index.columnar;

import com.arcadedb.utility.IntHashSet;

/**
 * One apply pass of one projection: takes the dirty pages out of its trackers and brings the projection up to date with them.
 * <p>
 * This version only DRAINS: under {@link ColumnarProjection#applyLock()} it moves every page marked dirty (primary and external
 * tracker, R7) through in flight to unflushed, without re-reading it, so the trackers empty and the caught-up state of the manager
 * is reachable. The re-read of the pages into the overlay replaces it (Task 15).
 */
final class ColumnarApplier {
  private ColumnarApplier() {
  }

  /** Runs one pass on the calling thread (an apply pool thread, or a closing one) and returns the number of pages taken. */
  static int runPass(final ColumnarIndexManager manager, final ColumnarProjection projection) {
    projection.applyLock().lock();
    try {
      final int[] pages = drain(projection.tracker());
      projection.tracker().completed(pages, pages.length);
      projection.pagesApplied(pages, pages.length);
      int taken = pages.length;

      final DirtyPageTracker external = projection.externalTracker();
      if (external != null) {
        final int[] externalPages = drain(external);
        external.completed(externalPages, externalPages.length);
        projection.externalPagesApplied(externalPages, externalPages.length);
        taken += externalPages.length;
      }
      return taken;
    } finally {
      projection.applyLock().unlock();
    }
  }

  private static int[] drain(final DirtyPageTracker tracker) {
    final IntHashSet pages = new IntHashSet();
    tracker.drainInto(pages);
    return pages.toArray();
  }
}
