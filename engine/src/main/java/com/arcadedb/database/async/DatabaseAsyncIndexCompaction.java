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
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public class DatabaseAsyncIndexCompaction implements DatabaseAsyncTask {
  public final IndexInternal index;

  public DatabaseAsyncIndexCompaction(final IndexInternal index) {
    this.index = index;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    if (database.isTransactionActive())
      database.commit();

    try {
      ((EmbeddedDatabase) database.getEmbedded()).indexCompactions.incrementAndGet();
      index.compact();
    } catch (Exception e) {
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
