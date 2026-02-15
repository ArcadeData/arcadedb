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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public class MaterializedViewChangeListener
    implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private final MaterializedViewImpl view;
  private final Database database;
  private final String callbackKey;

  public MaterializedViewChangeListener(final Database database, final MaterializedViewImpl view) {
    this.database = database;
    this.view = view;
    this.callbackKey = "mv-refresh:" + view.getName();
  }

  @Override
  public void onAfterCreate(final Record record) {
    schedulePostCommitRefresh();
  }

  @Override
  public void onAfterUpdate(final Record record) {
    schedulePostCommitRefresh();
  }

  @Override
  public void onAfterDelete(final Record record) {
    schedulePostCommitRefresh();
  }

  private void schedulePostCommitRefresh() {
    final DatabaseInternal db = (DatabaseInternal) database;
    if (!db.isTransactionActive())
      return;

    final TransactionContext tx = db.getTransaction();
    tx.addAfterCommitCallbackIfAbsent(callbackKey, () -> {
      try {
        MaterializedViewRefresher.fullRefresh(database, view);
      } catch (final Exception e) {
        view.setStatus("STALE");
        LogManager.instance().log(this, Level.WARNING,
            "Error in incremental refresh for view '%s', marking as STALE: %s",
            e, view.getName(), e.getMessage());
      }
    });
  }

  public MaterializedViewImpl getView() {
    return view;
  }
}
