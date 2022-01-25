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
package com.arcadedb.database;

import com.arcadedb.exception.TransactionException;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Thread local to store transaction data.
 */
public class DatabaseContext extends ThreadLocal<Map<String, DatabaseContext.DatabaseContextTL>> {
  public DatabaseContextTL init(final DatabaseInternal database) {
    return init(database, null);
  }

  public DatabaseContextTL init(final DatabaseInternal database, final TransactionContext firstTransaction) {
    Map<String, DatabaseContextTL> map = get();

    final String key = database.getDatabasePath();

    DatabaseContextTL current;

    if (map == null) {
      map = new HashMap<>();
      set(map);
      current = new DatabaseContextTL();
      map.put(key, current);
    } else {
      current = map.get(key);
      if (current == null) {
        current = new DatabaseContextTL();
        map.put(key, current);
      } else {
        if (!current.transactions.isEmpty()) {
          // ROLLBACK PREVIOUS TXS
          while (!current.transactions.isEmpty()) {
            final Transaction tx = current.transactions.remove(current.transactions.size() - 1);
            try {
              tx.rollback();
            } catch (Exception e) {
              // IGNORE ANY ERROR DURING ROLLBACK
            }
          }
        }
      }
    }

    if (current.transactions.isEmpty())
      current.transactions.add(firstTransaction != null ? firstTransaction : new TransactionContext(database.getWrappedDatabaseInstance()));

    return current;
  }

  public DatabaseContextTL getContext(final String name) {
    final Map<String, DatabaseContextTL> map = get();
    return map != null ? map.get(name) : null;
  }

  public DatabaseContextTL removeContext(final String name) {
    final Map<String, DatabaseContextTL> map = get();
    if (map != null)
      return map.remove(name);
    return null;
  }

  /**
   * This method is used by Gremlin for IO only. Using the TL to retrieve the current database is not recommended.
   */
  public Database getActiveDatabase() {
    final Map<String, DatabaseContextTL> map = get();
    if (map != null && map.size() == 1) {
      final DatabaseContextTL tl = map.values().iterator().next();
      if (tl != null) {
        final TransactionContext tx = tl.getLastTransaction();
        if (tx != null)
          return tx.getDatabase();
      }
    }
    return null;
  }

  public static class DatabaseContextTL {
    public final List<TransactionContext> transactions = new ArrayList<>(3);
    public       boolean                  asyncMode    = false;
    private      Binary                   temporaryBuffer1;
    private      Binary                   temporaryBuffer2;
    private      int                      maxNested    = 3;
    private      SecurityDatabaseUser     currentUser  = null;

    public SecurityDatabaseUser getCurrentUser() {
      return currentUser;
    }

    public void setCurrentUser(final SecurityDatabaseUser currentUser) {
      this.currentUser = currentUser;
    }

    public Binary getTemporaryBuffer1() {
      if (temporaryBuffer1 == null) {
        temporaryBuffer1 = new Binary(8192);
        temporaryBuffer1.setAllocationChunkSize(1024);
      }
      temporaryBuffer1.clear();
      return temporaryBuffer1;
    }

    public Binary getTemporaryBuffer2() {
      if (temporaryBuffer2 == null) {
        temporaryBuffer2 = new Binary(8192);
        temporaryBuffer2.setAllocationChunkSize(1024);
      }
      temporaryBuffer2.clear();
      return temporaryBuffer2;
    }

    public TransactionContext getLastTransaction() {
      if (transactions.isEmpty())
        return null;
      return transactions.get(transactions.size() - 1);
    }

    public void pushTransaction(final TransactionContext tx) {
      if (transactions.size() + 1 > maxNested)
        throw new TransactionException("Exceeded number of " + transactions.size()
            + " nested transactions. Check your code if you are beginning new transactions without closing the previous one by mistake. Otherwise change this limit with setMaxNested()");

      transactions.add(tx);
    }

    public TransactionContext popIfNotLastTransaction() {
      if (transactions.isEmpty())
        return null;

      if (transactions.size() > 1)
        return transactions.remove(transactions.size() - 1);

      return transactions.get(0);
    }

    public int getMaxNested() {
      return maxNested;
    }

    public void setMaxNested(final int maxNested) {
      this.maxNested = maxNested;
    }
  }

  public static volatile DatabaseContext INSTANCE = new DatabaseContext();
}
