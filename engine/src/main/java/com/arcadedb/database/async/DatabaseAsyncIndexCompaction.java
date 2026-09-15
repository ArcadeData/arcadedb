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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;

import java.util.function.IntConsumer;
import java.util.logging.Level;

public class DatabaseAsyncIndexCompaction implements DatabaseAsyncTask {
  public final IndexInternal index;

  public DatabaseAsyncIndexCompaction(final IndexInternal index) {
    this.index = index;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    if (database.isTransactionActive()) {
      try {
        // #7615: same test-only fault-injection hook commitBatch() fires - lets a test reproduce a
        // conflict on THIS out-of-band commit deterministically too, without a real compaction race.
        final IntConsumer hook = DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK;
        if (hook != null)
          hook.accept(1);

        database.commit();
        // #7615: those commands are durably committed now - drop the stale references so a LATER periodic
        // boundary commit that fails and retries by replay (commitBatch()) cannot replay them a second
        // time (this task runs with requiresActiveTx() == false, so it can land mid-batch on a worker that
        // has commands from pendingBatchCommands still open in this very transaction).
        async.clearPendingBatchCommands();
      } catch (final Throwable e) {
        // This commit closes out whatever DatabaseAsyncCommand tasks this worker had already run against
        // its shared batch transaction before this compaction task ran - none of them are this task's own
        // doing, so their failure must not vanish silently. Told individually instead; propagates
        // unchanged otherwise, exactly as an uncaught commit failure here always has.
        async.notifyPendingBatchCommandsAndAbandon(e);
        throw e;
      }
    }

    try {
      ((LocalDatabase) database.getEmbedded()).indexCompactions.incrementAndGet();
      index.compact();
    } catch (final Exception e) {
      if (e instanceof IllegalArgumentException && e.getMessage().contains("File with id ") && e.getMessage().contains("was not found"))
        LogManager.instance().log(this, Level.SEVERE, "Error on executing compaction of index '%s' (%s)", index.getName(), e.getMessage());
      else if (e instanceof IndexException && e.getMessage().contains("not valid"))
        LogManager.instance().log(this, Level.SEVERE, "Error on executing compaction of index '%s' (%s)", index.getName(), e.getMessage());
      else
        LogManager.instance().log(this, Level.SEVERE, "Error on executing compaction of index '%s'", e, index.getName());
    }
  }

  @Override
  public boolean requiresActiveTx() {
    return false;
  }

  @Override
  public String toString() {
    return "IndexCompaction(" + index.getName() + ")";
  }

}
