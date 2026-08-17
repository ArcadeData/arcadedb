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
package com.arcadedb.graph.olap;

/**
 * Cooperative abort hook that an iterative {@link GraphAlgorithms} kernel calls once per iteration.
 * <p>
 * The iteration count of a kernel such as {@link GraphAlgorithms#pageRank} comes straight from a caller-supplied
 * knob ({@code algo.pageRank({maxIterations: ...})}), and for time there is no honest ceiling to pick: how long a
 * run may legitimately take is a property of the graph, the hardware and the caller's patience, not of the
 * parameter. So a large value is not forbidden, it is made abortable - and the kernel, which knows nothing about
 * queries, timeouts or threads, asks this hook whether it should still be running.
 * </p>
 * <p>
 * The hook exists rather than a direct dependency on the query layer's guard because {@code com.arcadedb.graph.olap}
 * sits <em>below</em> {@code com.arcadedb.query}: the OpenCypher procedures pass their own guard as a method
 * reference, and the kernels stay free of any knowledge of it.
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@FunctionalInterface
public interface WorkCheckpoint {
  /** Checkpoint for a caller with nothing to cancel: never aborts, and the JIT folds it away. */
  WorkCheckpoint NONE = () -> {
  };

  /**
   * Throws to abort the algorithm, or returns to let it continue. Called from the calling thread between two
   * iterations, never from inside a parallel chunk, so an implementation does not have to be thread-safe and the
   * exception it throws propagates straight to the caller of the kernel.
   */
  void check();
}
