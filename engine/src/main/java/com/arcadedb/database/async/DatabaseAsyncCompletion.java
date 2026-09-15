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
import com.arcadedb.database.TransactionContext;

import java.util.concurrent.CountDownLatch;

public class DatabaseAsyncCompletion extends DatabaseAsyncAbstractCallbackTask {
  public DatabaseAsyncCompletion() {
    super(new CountDownLatch(1));
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    try {
      if (database.isTransactionActive()) {
        // #7615: this commit flushes out whatever DatabaseAsyncCommand tasks ran since the last
        // commitEvery boundary - db.async().waitCompletion() is exactly the call the original report
        // used to observe the loss, so this dangling tail batch needs the same retry-by-replay and
        // per-command notification as the periodic boundary itself, not a bare commit().
        final TransactionContext activeTx = database.getTransaction();
        async.commitBatch(activeTx.isUseWAL(), activeTx.getWALFlush(), false);
      } else
        async.onOk();
    } catch (final Exception e) {
      async.onError(e);
    }
  }

  @Override
  public boolean requiresActiveTx() {
    return false;
  }

  @Override
  public String toString() {
    return "Completion";
  }
}
