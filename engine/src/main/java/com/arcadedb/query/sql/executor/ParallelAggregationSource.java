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
package com.arcadedb.query.sql.executor;

/**
 * A step whose rows an aggregation downstream of it can consume in the workers of a parallel scan, rather than one at
 * a time on the thread consuming the query (issue #8523): a type scan, filtered or not, and an index fetch that serves
 * its range by a scan or in physical order (issue #8333).
 * <p>
 * A source hands its rows over in one round or, when they cannot be held at once, in several consecutive ones: the
 * aggregation runs every round to the end before it asks for the next, so a round may reuse what the previous one held.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
interface ParallelAggregationSource {
  /**
   * Plans the parallel scan of this execution's rows, or of their first round, for an aggregation that consumes them
   * in the scan's workers. Takes this execution's decision: a source that answers {@code null} serves its rows
   * sequentially, the way it would have without the call.
   *
   * @return the scan, or {@code null} when this execution cannot run in parallel or has already started
   */
  ParallelTypeScan planParallelAggregation(CommandContext context);

  /**
   * The parallel scan of the next round, asked once every row of the previous one has been aggregated.
   *
   * @return the scan, or {@code null} when no row is left
   */
  default ParallelTypeScan nextParallelAggregationRound(final CommandContext context) {
    return null;
  }

  /** Whether an execution starting now would scan in parallel: what an EXPLAIN shows. */
  boolean wouldRunInParallel(CommandContext context);
}
