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

/**
 * Base of the engine's own async tasks: carries the {@link DatabaseAsyncTask#getRootEpoch() root epoch} the executor stamps on
 * a task when it is scheduled, so {@link DatabaseAsyncExecutorImpl#waitCompletion(long)} can tell the cascades it owes its
 * caller from the ones another thread's work keeps scheduling (issue #11201).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class DatabaseAsyncAbstractTask implements DatabaseAsyncTask {
  // Written by the scheduling thread before the task reaches a queue, read by the worker after it polled it: the queue hand-off
  // orders the two, so a plain field is enough
  private long rootEpoch;

  @Override
  public long getRootEpoch() {
    return rootEpoch;
  }

  @Override
  public void setRootEpoch(final long rootEpoch) {
    this.rootEpoch = rootEpoch;
  }
}
