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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;

/**
 * The protocol a row loop of an importer follows to resolve the transaction it pushed, and only that one: a flag that says
 * whether the loop still holds it, plus the transaction itself, to prove the live one is still it. Expressed once so that no
 * loop has to remember its halves (issue #11046): the clear-before-commit and its restore-on-failure are one protocol, and a
 * loop that wrote the first half by hand and forgot the second stranded its transaction on the caller's thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ImporterTransactions {
  private ImporterTransactions() {
  }

  /**
   * The transaction currently on top of this thread's stack, or {@code null} when there is none - and also when the
   * {@link Database} implementation does not expose its stack at all (a remote database), which {@link #isOwnTransactionLive}
   * treats as "cannot tell" rather than as "not mine".
   */
  public static TransactionContext currentTransaction(final Database database) {
    return database instanceof final DatabaseInternal internal ? internal.getTransactionIfExists() : null;
  }

  /**
   * Whether a commit or a rollback issued right now would resolve the loop's OWN transaction, and not somebody else's.
   * <p>
   * The flag is not enough on its own. {@code begin()} nests, so the stack can hold a caller's transaction under the
   * import's, and everything that runs inside the row loop - a parsing callback above all, which belongs to the caller - can
   * resolve the import's transaction without the flag ever hearing about it. A commit issued on that stale flag pops and
   * commits the caller's work instead, silently (issues #7272, #7328).
   * <p>
   * Identity plus liveness is what settles it: a resolved nested transaction is off the stack, so the top is a different
   * object; a resolved bottom transaction is still there but inactive; and a fresh {@code begin()} by the callback pushes a
   * new object rather than reviving this one. The only case identity cannot separate is a bottom transaction resolved and
   * re-begun, and a bottom transaction means there is no caller transaction underneath to protect.
   * <p>
   * The contract this puts on a parsing callback, the one thing running in a row loop that the caller writes: it must not
   * return with an extra transaction of its own left open. One that pushes a transaction on top of the import's and never
   * resolves it leaves the import's buried, and this then correctly refuses to touch what is on top rather than resolving the
   * wrong one. Nothing here unwinds a stack somebody else grew.
   *
   * @param open           the loop's own flag: false once the loop has resolved its transaction itself
   * @param ownTransaction the transaction the loop's last {@code begin()} left current, or {@code null} when the
   *                       implementation does not expose it - in which case the flag is all there is
   */
  public static boolean isOwnTransactionLive(final Database database, final boolean open,
      final TransactionContext ownTransaction) {
    if (!open)
      return false;
    if (ownTransaction == null)
      return database.isTransactionActive();
    return ownTransaction.isActive() && currentTransaction(database) == ownTransaction;
  }

  /**
   * Begins the loop's own transaction and records it: the flag set, and the transaction the {@code begin()} left current, which
   * is what {@link #isOwnTransactionLive} later proves the live one against.
   *
   * @param open           the loop's flag, a one-element array so a lambda can share it
   * @param ownTransaction the loop's transaction, a one-element array for the same reason
   */
  public static void beginOwnTransaction(final Database database, final boolean[] open, final TransactionContext[] ownTransaction) {
    database.begin();
    open[0] = true;
    ownTransaction[0] = currentTransaction(database);
  }

  /**
   * Rolls the loop's own transaction back when {@link #isOwnTransactionLive it is the live one}, and does nothing otherwise: a
   * failure that left it on the stack still discards it, and one that did not - a commit that popped it - never reaches past it
   * for whatever is underneath.
   */
  public static void rollbackOwnTransaction(final Database database, final boolean[] open,
      final TransactionContext[] ownTransaction) {
    if (isOwnTransactionLive(database, open[0], ownTransaction[0])) {
      open[0] = false;
      database.rollback();
    }
  }

  /**
   * Commits the loop's own transaction when {@link #isOwnTransactionLive it is the live one}, and does nothing otherwise.
   * <p>
   * The flag is cleared BEFORE the commit: a commit that throws skips everything after it, and a flag still set would hand
   * the loop's rollback a transaction the commit popped - on a {@link Database} that does not expose its stack, the CALLER's
   * (issue #7272). And it is restored when the commit throws (issue #11046): a refusal raised before the commit starts - a
   * rollback-only transaction, a terminated statement (#9680), a server that stopped accepting writes (#8270, #10689) - leaves
   * the transaction active, and {@code DatabaseContext#popIfNotLastTransaction()} does not pop the outermost one, so a flag
   * left cleared made the loop's rollback skip it and stranded it on the thread, never committed and never rolled back. It
   * then survived the caller's own {@code begin()}/{@code commit()} pair, which nests inside it.
   * <p>
   * Restored only when identity proves the live transaction is still the loop's own. A commit that popped it leaves the
   * caller's on top, which liveness alone cannot tell from it: on a {@link Database} that does not expose its stack (a remote
   * one), restoring from {@code isTransactionActive()} handed the loop's rollback the caller's transaction, discarding its
   * pending work. There the flag stays cleared, as it was before.
   *
   * @param open the loop's flag, a one-element array so a lambda can share it
   */
  public static void commitOwnTransaction(final Database database, final boolean[] open,
      final TransactionContext ownTransaction) {
    if (!isOwnTransactionLive(database, open[0], ownTransaction))
      return;
    open[0] = false;
    try {
      database.commit();
    } catch (final RuntimeException | Error commitFailure) {
      open[0] = ownTransaction != null && isOwnTransactionLive(database, true, ownTransaction);
      throw commitFailure;
    }
  }
}
