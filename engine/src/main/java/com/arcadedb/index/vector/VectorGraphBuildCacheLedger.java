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

import java.util.concurrent.atomic.AtomicLong;

/**
 * The heap the caches of the graph builds running on the vector indexes of ONE database plan on (issue #12544).
 * <p>
 * A build's cache is local to the build and preloaded as soon as it is sized, and the available-heap reading shows it only
 * after a collection of the old generation. The search caches, the other builds and the delta payload budget of the
 * database are charged this total instead.
 * <p>
 * The total lives on the database, not on a walk of the schema's indexes: a schema reload publishes a successor for an
 * index while that index's build may still be running and holding its cache, and a walk would no longer find it. Each
 * build charges its cache once it is sized and gives exactly that back when it ends, whether it succeeded or failed, so the
 * charge lasts as long as the cache and no longer.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public final class VectorGraphBuildCacheLedger {
  private final AtomicLong bytes = new AtomicLong();

  /** @return the bytes the running builds of the database plan on together, never negative */
  public long bytes() {
    return Math.max(0L, bytes.get());
  }

  /** Charges a build cache that was just sized. */
  void charge(final long plannedBytes) {
    if (plannedBytes > 0L)
      bytes.addAndGet(plannedBytes);
  }

  /** Gives back what a build charged, when the build ends. */
  void release(final long chargedBytes) {
    if (chargedBytes > 0L)
      bytes.addAndGet(-chargedBytes);
  }
}
