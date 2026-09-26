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

import java.util.Map;

/**
 * What a test means when it asserts "nothing is pending in the delta buffer" of an {@link LSMVectorIndex} after a graph
 * build (issue #8200).
 * <p>
 * {@code deltaVectorsCount} alone does not answer it. A Vamana build occasionally leaves a node unreachable, and the
 * index re-queues that node's vector into the delta buffer on purpose, so the delta scan can still return it (issue
 * #7190). That entry is already IN the graph: it is not a write waiting for a rebuild, no rebuild owes anything for it,
 * and it does not drain until the next real mutation - a fresh build orphans a fresh set. A flat
 * {@code deltaVectorsCount == 0} after a build therefore goes red at a rate that depends on the corpus, and inside an
 * {@code Awaitility.untilAsserted} it does not even retry past it: it times out (issues #7742, #8115, #8200).
 * <p>
 * Only meaningful once this session has resolved a graph: before the first search of a reopened index,
 * {@code unreachableGraphNodes} is read from the manifest while the buffer holds none of those nodes yet, so a
 * precondition taken at that point must keep asserting {@code deltaVectorsCount} itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PendingDeltaVectors {
  private PendingDeltaVectors() {
  }

  /**
   * The buffered vectors that are genuinely waiting for a rebuild: {@code deltaVectorsCount} less the nodes the build
   * that produced the current graph left unreachable and re-queued.
   */
  static long of(final Map<String, Long> stats) {
    return stats.get("deltaVectorsCount") - stats.get("unreachableGraphNodes");
  }

  static long of(final LSMVectorIndex index) {
    return of(index.getStats());
  }
}
