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
package com.arcadedb.engine.timeseries;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;

/**
 * The transaction a TimeSeries write path begins for itself, and the only one it is ever allowed to roll back
 * (issue #7732).
 * <p>
 * Every such path - a shard append, a header-page initialisation, the engine init inside a {@code CREATE
 * TIMESERIES TYPE} - opens its own {@code begin()}/{@code commit()} pair whatever the caller has open, because an
 * ArcadeDB nested transaction is an independent transaction rather than a savepoint (the contract
 * {@code TimeSeriesShard.appendSamples} states at length). Undoing one on failure therefore has to name WHICH
 * transaction, and {@code db.isTransactionActive()} cannot: it answers for whatever context is on top of the
 * thread's stack at that moment.
 * <p>
 * <b>The failure that motivates this.</b> After {@code commit()} THROWS, the transaction it failed to commit is
 * already gone: {@code LocalDatabase.commit()} pops it in a {@code finally}, and
 * {@code DatabaseContext.popIfNotLastTransaction()} removes the nested context whenever the stack holds more than
 * one. So on a caller that had a transaction open, {@code isTransactionActive()} then answers for the CALLER's
 * transaction, and the {@code rollback()} guarded by it discards the caller's own uncommitted work - silently, and
 * on the retry arm the append then succeeds and returns normally, so the caller is never told. Comparing the
 * context identity instead is exact in every arm: before the commit the context is ours and active, after a failed
 * commit it is either gone (nested) or already inactive (single), and after a successful one it is gone too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class OwnTransaction {

  private final DatabaseInternal   database;
  private final TransactionContext context;

  private OwnTransaction(final DatabaseInternal database, final TransactionContext context) {
    this.database = database;
    this.context = context;
  }

  /**
   * Begins a transaction and remembers which context it is.
   * <p>
   * {@code begin()} pushes a new context only when one is already active on the thread; with an inactive context
   * on the stack it re-begins that one instead. Both are "ours" in the sense that matters here - the caller had
   * nothing pending in an inactive context - and both are identified the same way, by the context {@code begin()}
   * left on top.
   */
  public static OwnTransaction begin(final DatabaseInternal database) {
    database.begin();
    return new OwnTransaction(database, database.getTransaction());
  }

  /**
   * Commits it. The context is gone from the thread's stack afterwards whether this returns or throws, which is
   * exactly what {@link #rollbackIfMine()} relies on.
   */
  public void commit() {
    database.commit();
  }

  /**
   * Rolls back the transaction {@link #begin} started, and only it: a no-op once the commit has popped it, and a
   * no-op when what sits on top of the stack now is somebody else's. Safe to call more than once.
   */
  public void rollbackIfMine() {
    final TransactionContext current = database.getTransactionIfExists();
    if (current == context && current != null && current.isActive())
      database.rollback();
  }
}
