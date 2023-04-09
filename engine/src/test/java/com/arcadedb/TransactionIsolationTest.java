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
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

/**
 * Tests the isolation of transactions based on the configured settings.
 */
public class TransactionIsolationTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> {
      if (database.getSchema().existsType("Node"))
        database.getSchema().dropType("Node");
      database.getSchema().createVertexType("Node");
    });
  }

  @Test
  public void testNoDirtyReads() throws InterruptedException {
    final CountDownLatch sem1 = new CountDownLatch(1);
    final CountDownLatch sem2 = new CountDownLatch(1);

    final Thread thread1 = new Thread(() -> {
      database.transaction(() -> {
        try {
          Assertions.assertEquals(0, database.countType("Node", true));

          final MutableVertex v = database.newVertex("Node");
          v.set("id", 0);
          v.set("origin", "thread1");
          v.save();

          Assertions.assertEquals(1, database.countType("Node", true));

          sem1.countDown();

          sem2.await();

          Assertions.assertEquals(1, database.countType("Node", true));

        } catch (InterruptedException e) {
          Assertions.fail();
          throw new RuntimeException(e);
        }
      });
    });

    final Thread thread2 = new Thread(() -> {
      database.transaction(() -> {
        try {
          sem1.await();

          Assertions.assertEquals(0, database.countType("Node", true));

          final MutableVertex v = database.newVertex("Node");
          v.set("id", 1);
          v.set("origin", "thread2");
          v.save();

          Assertions.assertEquals(1, database.countType("Node", true));

          sem2.countDown();

        } catch (InterruptedException e) {
          Assertions.fail();
          throw new RuntimeException(e);
        }
      });
    });

    thread1.setDaemon(true);
    thread2.setDaemon(true);

    thread1.start();
    thread2.start();

    thread1.join(3000);
    thread2.join(3000);
  }

  @Test
  public void testReadCommitted() throws InterruptedException {
    final CountDownLatch sem1 = new CountDownLatch(1);
    final CountDownLatch sem2 = new CountDownLatch(1);
    final CountDownLatch sem3 = new CountDownLatch(1);

    final Thread thread1 = new Thread(() -> {
      database.transaction(() -> {
        database.newVertex("Node").set("id", 0, "origin", "thread1").save();
        Assertions.assertEquals(1, database.countType("Node", true));
      });

      sem1.countDown();

      database.transaction(() -> {
        try {
          sem2.await();
          // CHECK THE NEW RECORD (PHANTOM READ) IS VISIBLE
          Assertions.assertEquals(2, database.countType("Node", true));

          database.newVertex("Node").set("id", 3, "origin", "thread1").save();

          Assertions.assertEquals(3, database.countType("Node", true));

          // MODIFY A RECORD
          database.query("sql", "select from Node where id = 0").nextIfAvailable().getRecord().get().asVertex().modify().set("modified", true).save();

        } catch (InterruptedException e) {
          Assertions.fail();
          throw new RuntimeException(e);
        }
      });

      sem3.countDown();
    });

    final Thread thread2 = new Thread(() -> {
      database.transaction(() -> {
        try {
          sem1.await();

          // CHECK THE NEW RECORD (PHANTOM READ) IS VISIBLE
          Assertions.assertEquals(1, database.countType("Node", true));

          database.newVertex("Node").set("id", 1, "origin", "thread2").save();
          Assertions.assertEquals(2, database.countType("Node", true));

        } catch (InterruptedException e) {
          Assertions.fail();
          throw new RuntimeException(e);
        }
      });

      sem2.countDown();

      database.transaction(() -> {
        try {
          sem3.await();

          Assertions.assertEquals(3, database.countType("Node", true));

          // CHECK THE NEW RECORD WAS MODIFIED
          Assertions.assertTrue((Boolean) database.query("sql", "select from Node where id = 0").nextIfAvailable().getProperty("modified"));

        } catch (InterruptedException e) {
          Assertions.fail();
          throw new RuntimeException(e);
        }
      });
    });

    thread1.setDaemon(true);
    thread2.setDaemon(true);

    thread1.start();
    thread2.start();

    thread1.join(3000);
    thread2.join(3000);
  }

  @Test
  public void testRepeatableRead() throws InterruptedException {
    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {

      final CountDownLatch sem1 = new CountDownLatch(1);
      final CountDownLatch sem2 = new CountDownLatch(1);
      final CountDownLatch sem3 = new CountDownLatch(1);

      final Thread thread1 = new Thread(() -> {
        database.transaction(() -> {
          database.newVertex("Node").set("id", 0, "origin", "thread1").save();
          Assertions.assertEquals(1, database.countType("Node", true));
        });

        sem1.countDown();

        database.transaction(() -> {
          try {
            sem2.await();
            // CHECK THE NEW RECORD (PHANTOM READ) IS VISIBLE
            Assertions.assertEquals(2, database.countType("Node", true));

            database.newVertex("Node").set("id", 3, "origin", "thread1").save();

            Assertions.assertEquals(3, database.countType("Node", true));

            // MODIFY A RECORD
            database.query("sql", "select from Node where id = 0").nextIfAvailable().getRecord().get().asVertex().modify().set("modified", true).save();

          } catch (InterruptedException e) {
            Assertions.fail();
            throw new RuntimeException(e);
          }
        });

        sem3.countDown();
      });

      final Thread thread2 = new Thread(() -> {
        database.transaction(() -> {
          try {
            sem1.await();

            // CHECK THE NEW RECORD (PHANTOM READ) IS VISIBLE
            Assertions.assertEquals(1, database.countType("Node", true));

            database.newVertex("Node").set("id", 1, "origin", "thread2").save();
            Assertions.assertEquals(2, database.countType("Node", true));

          } catch (InterruptedException e) {
            Assertions.fail();
            throw new RuntimeException(e);
          }
        });

        sem2.countDown();

        database.transaction(() -> {
          try {
            Assertions.assertEquals(2, database.countType("Node", true));

            Assertions.assertNull(database.query("sql", "select from Node where id = 0").nextIfAvailable().getProperty("modified"));

            sem3.await();

            // CHECK THE NEW RECORD WAS MODIFIED
            Assertions.assertNull(database.query("sql", "select from Node where id = 0").nextIfAvailable().getProperty("modified"));

          } catch (InterruptedException e) {
            Assertions.fail();
            throw new RuntimeException(e);
          }
        });
      });

      thread1.setDaemon(true);
      thread2.setDaemon(true);

      thread1.start();
      thread2.start();

      thread1.join(3000);
      thread2.join(3000);
    } finally {
      database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    }
  }
}
