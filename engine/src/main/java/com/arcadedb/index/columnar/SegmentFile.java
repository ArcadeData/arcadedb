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

import com.arcadedb.engine.block.BlockCache;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Identity of one segment file on disk, shared by every {@link SegmentReader} over it ({@code withDead} copies included). It owns the
 * file's {@link BlockCache} file id. Segment files are immutable, so the id never needs invalidating while the file exists: the cache
 * is invalidated exactly once, by {@link #deleted()}, never on a reader close or a dead-set swap (an invalidation bumps a JVM-wide
 * epoch and sweeps every entry).
 */
final class SegmentFile {
  private final Path          path;
  private final long          cacheFileId;
  private final AtomicInteger references = new AtomicInteger(1);
  private final AtomicBoolean deleted    = new AtomicBoolean();

  private SegmentFile(final Path path) {
    this.path = path;
    this.cacheFileId = BlockCache.instance().newFileId();
  }

  /** A new identity with a fresh cache file id and one reference, owned by the caller. */
  static SegmentFile open(final Path file) {
    return new SegmentFile(file);
  }

  Path path() {
    return path;
  }

  long cacheFileId() {
    return cacheFileId;
  }

  /** Adds a reference. */
  SegmentFile retain() {
    while (true) {
      final int current = references.get();
      if (current <= 0)
        throw new IllegalStateException("Segment file " + path + " is already released");
      if (references.compareAndSet(current, current + 1))
        return this;
    }
  }

  /** Drops a reference. Reaching 0 neither deletes the file nor invalidates the cache: the owner decides when the file goes. */
  void release() {
    while (true) {
      final int current = references.get();
      if (current <= 0)
        throw new IllegalStateException("Segment file " + path + " is released more often than retained");
      if (references.compareAndSet(current, current - 1))
        return;
    }
  }

  int referencesForTesting() {
    return references.get();
  }

  /**
   * Called by the owner after the last release and after the file was deleted from disk: drops the file's cached blocks, once.
   *
   * @throws IllegalStateException when a reader still holds a reference
   */
  void deleted() {
    final int held = references.get();
    if (held > 0)
      throw new IllegalStateException("Segment file " + path + " is deleted while " + held + " readers still hold it");
    if (deleted.compareAndSet(false, true))
      BlockCache.instance().invalidateFile(cacheFileId);
  }
}
