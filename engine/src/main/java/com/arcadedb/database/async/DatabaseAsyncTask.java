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
