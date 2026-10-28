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

/**
 * The package-private test hooks of {@link ColumnarIndexManager} and {@link ColumnarRebuilder}, for tests of other modules (the HA
 * integration tests of plan 3, Task 23) that cannot live in this package.
 */
public final class ColumnarTestAccess {
  private ColumnarTestAccess() {
  }

  /** Runs the flush and merge job of the bucket's projection on the calling thread, whatever its schedule. */
  public static void maintainNow(final ColumnarIndexManager manager, final int bucketId) {
    manager.maintainForTesting(bucketId);
  }

  /** A rebuild of the bucket's projection is queued or running. */
  public static boolean isRebuildQueuedOrRunning(final ColumnarIndexManager manager, final int bucketId) {
    return ColumnarRebuilder.isQueuedOrRunning(projection(manager, bucketId));
  }

  /** The generation of the manifest the bucket's projection publishes, -1 when it publishes none. */
  public static long publishedGeneration(final ColumnarIndexManager manager, final int bucketId) {
    final ColumnarManifest manifest = projection(manager, bucketId).manifest();
    return manifest == null ? -1 : manifest.generation();
  }

  /** The segments of the manifest the bucket's projection publishes, 0 when it publishes none. */
  public static int publishedSegments(final ColumnarIndexManager manager, final int bucketId) {
    final ColumnarManifest manifest = projection(manager, bucketId).manifest();
    return manifest == null ? 0 : manifest.segments().size();
  }

  /**
   * The references held on the bucket's current projection state, the projection's own one and one per open snapshot included. Only
   * comparable between two reads while nothing publishes a new state (caught up, no flush or merge running).
   */
  public static int stateReferences(final ColumnarIndexManager manager, final int bucketId) {
    final ProjectionState state = projection(manager, bucketId).retainState();
    try {
      return state.referencesForTesting() - 1;
    } finally {
      state.release();
    }
  }

  private static ColumnarProjection projection(final ColumnarIndexManager manager, final int bucketId) {
    final ColumnarProjection projection = manager.projection(bucketId);
    if (projection == null)
      throw new IllegalArgumentException("Bucket " + bucketId + " has no columnar projection");
    return projection;
  }
}
