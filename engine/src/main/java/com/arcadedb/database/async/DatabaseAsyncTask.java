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
package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/***
 * Interface for asynchronous tasks.
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface DatabaseAsyncTask {

  void execute(DatabaseAsyncExecutorImpl.AsyncThread async, DatabaseInternal database);

  default void completed() {
    // DEFAULT IMPLEMENTATION
  }

  default boolean requiresActiveTx() {
    return true;
  }

  /**
   * Whether this task's write, if it has one, lands in the worker's shared batch transaction that
   * {@code AsyncThread#commitBatch} tracks and can replay after a rolled-back periodic-boundary commit
   * (issue #7615 review). {@code requiresActiveTx()} answers a different question - whether this task
   * needs {@code begin()} called before it runs - so a pure-read task (a bucket scan, a browse iterator)
   * or one that writes somewhere entirely separate from {@code database}'s transaction (an append to a
   * time-series shard) still defaults to {@code requiresActiveTx() == true} while having nothing here
   * that a rolled-back commit could actually lose. Counting such a task as unreplayable anyway would
   * needlessly disable the retry-by-replay for every command buffered ahead of it on the same worker,
   * falling back to abandon-and-notify for a conflict that a replay could otherwise have resolved
   * transparently. Default {@code true} (conservative): a task must affirmatively know it does not write
   * to the shared batch before opting out.
   */
  default boolean writesToSharedBatch() {
    return true;
  }

  /**
   * Called when this task's write was applied to a worker's shared batch transaction that was then
   * abandoned instead of committed - a boundary commit conflict {@code AsyncThread#commitBatch} could not
   * safely retry because this task's write cannot be replayed (issue #7615). {@code cause} is the failure
   * that closed the batch. Default is a no-op, for tasks with no per-task error callback to invoke (e.g.
   * the graph edge-creation tasks, whose only callback reports a new edge, not a failure); a task that has
   * one (e.g. {@link DatabaseAsyncCreateRecord}) overrides this to call it, so its submitter is not left
   * having seen only a success callback for a write that never actually landed.
   */
  default void notifyBatchAbandoned(final Throwable cause) {
    // DEFAULT IMPLEMENTATION: nothing to notify
  }
}
