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
package com.arcadedb.index.lsm;

import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;

import java.util.Arrays;

/**
 * The {@link PageId} of each page of one component, built once and handed out again (#10654).
 * <p>
 * An index lookup names every page it visits by a {@link PageId}, and a lookup runs millions of times on a bulk load, so
 * building one per page per lookup was a steady stream of short-lived objects, with a hash code computed for each. A
 * {@code PageId} is immutable and equal by value, so one instance per page number is indistinguishable from a new one to
 * every map that takes it as a key.
 * <p>
 * Lock-free: a reader sees the array it read, whole. Two threads that build the same entry store equal ids, and a grow that
 * races another only costs the loser a rebuild of the entries it did not copy. The array never grows past the component's
 * page count, so a page number the component does not have (a bad entry read from disk) gets a plain new {@code PageId} and
 * cannot make the cache allocate by its value.
 */
final class PageIdCache {
  private final PaginatedComponent component;
  private volatile PageId[]        ids = new PageId[0];

  PageIdCache(final PaginatedComponent component) {
    this.component = component;
  }

  PageId get(final int pageNumber) {
    PageId[] cached = ids;
    if (pageNumber >= 0 && pageNumber < cached.length) {
      final PageId id = cached[pageNumber];
      if (id != null)
        return id;
    } else {
      final int totalPages = pageNumber >= 0 ? component.getTotalPages() : 0;
      if (pageNumber < 0 || pageNumber >= totalPages)
        return new PageId(component.getDatabase(), component.getFileId(), pageNumber);
      // Growing to the page count copies the whole array for each new page. That is on purpose: the copy is negligible next to
      // creating the page, and sizing the array to the page count keeps it from ever growing past it. Do not "optimize" it.
      cached = Arrays.copyOf(cached, totalPages);
      ids = cached;
    }
    final PageId id = new PageId(component.getDatabase(), component.getFileId(), pageNumber);
    cached[pageNumber] = id;
    return id;
  }
}
