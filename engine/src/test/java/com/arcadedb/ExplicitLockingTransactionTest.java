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
package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class ExplicitLockingTransactionTest extends TestHelper {
  @Test
  public void testExplicitLock() {
    final Database db = database;

    final int TOT = 1000;

    database.getSchema().getOrCreateVertexType("Node");

    final AtomicInteger committed = new AtomicInteger(0);
    final AtomicInteger caughtExceptions = new AtomicInteger(0);

    final RID[] rid = new RID[1];

    database.transaction(() -> {
      final MutableVertex v = db.newVertex("Node");
      v.set("id", 0);
      v.set("name", "Exception(al)");
      v.set("surname", "Test");
      v.save();
      rid[0] = v.getIdentity();
    });

    final int CONCURRENT_THREADS = 16;

    // SPAWN ALL THE THREADS AND INCREMENT ONE BY ONE THE ID OF THE VERTEX
    final Thread[] threads = new Thread[CONCURRENT_THREADS];
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      threads[i] = new Thread(() -> {
        for (int k = 0; k < TOT; ++k) {
          try {
            database.transaction(() -> {
              database.acquireLock().type("Node").lock();

              MutableVertex v = rid[0].asVertex().modify();
              v.set("id", v.getInteger("id") + 1);
              v.save();
            });

            committed.incrementAndGet();

          } catch (Exception e) {
            caughtExceptions.incrementAndGet();
          }
        }

      });
      threads[i].start();
    }

    // WAIT FOR ALL THE THREADS
    for (int i = 0; i < CONCURRENT_THREADS; i++)
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        // IGNORE IT
      }

    assertThat(database.countType("Node", true)).isEqualTo(1);

    assertThat(rid[0].asVertex().getInteger("id")).isEqualTo(CONCURRENT_THREADS * TOT);
    assertThat(committed.get()).isEqualTo(CONCURRENT_THREADS * TOT);
    assertThat(caughtExceptions.get()).isEqualTo(0);
    assertThat(committed.get() + caughtExceptions.get()).isEqualTo(TOT * CONCURRENT_THREADS);
  }

  @Test
  public void testErrorOnExplicitLock() {
    final Database db = database;

    database.getSchema().getOrCreateVertexType("Node");

    final RID[] rid = new RID[1];

    database.transaction(() -> {
      final MutableVertex v = db.newVertex("Node");
      v.set("id", 0);
      v.set("name", "Exception(al)");
      v.set("surname", "Test");
      v.save();
      rid[0] = v.getIdentity();
    });

    try {
      database.acquireLock().type("Node").lock();
      fail("This must fail because the tx was not started yet");
    } catch (DatabaseOperationException e) {
      // EXPECTED
    }

    try {
      database.begin();
      database.acquireLock().type("Node").lock();
      database.acquireLock().type("Node").lock(); // THIS MUST FAIL
    } catch (TransactionException e) {
      // EXPECTED
    }

    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      database.query("sql", "select from Node").close();
      database.acquireLock().type("Node").lock();
      fail("This must fail because the tx is not empty");
    } catch (TransactionException e) {
      // EXPECTED
    }

    database.getSchema().getOrCreateVertexType("Node2");

    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      database.acquireLock().type("Node").lock();

      final MutableVertex v = db.newVertex("Node2");
      v.set("id", 777);
      v.save();

      database.commit();

      fail("This must fail because a record of unlocked type was created in the tx");
    } catch (TransactionException e) {
      // EXPECTED
    }
  }
}
