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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class TransactionIsolationTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> {
      if (database.getSchema().existsType("Node"))
        database.getSchema().dropType("Node");
      database.getSchema().createVertexType("Node", 8);
    });
  }

  @Test
  void noDirtyReads() throws Exception {
    final CountDownLatch sem1 = new CountDownLatch(1);
    final CountDownLatch sem2 = new CountDownLatch(1);

    final Thread thread1 = new Thread(() -> {
      database.transaction(() -> {
        try {
          assertThat(database.countType("Node", true)).isEqualTo(0);

          final MutableVertex v = database.newVertex("Node");
          v.set("id", 0);
          v.set("origin", "thread1");
          v.save();

          assertThat(database.countType("Node", true)).isEqualTo(1);

          sem1.countDown();

          sem2.await();

          assertThat(database.countType("Node", true)).isEqualTo(1);

        } catch (InterruptedException e) {
          fail("InterruptedException occurred");
          throw new RuntimeException(e);
        }
      });
    });

    final Thread thread2 = new Thread(() -> {
      database.transaction(() -> {
        try {
          sem1.await();

          assertThat(database.countType("Node", true)).isEqualTo(0);

          final MutableVertex v = database.newVertex("Node");
          v.set("id", 1);
          v.set("origin", "thread2");
          v.save();

          assertThat(database.countType("Node", true)).isEqualTo(1);

          sem2.countDown();

        } catch (InterruptedException e) {
          fail("InterruptedException occurred");
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
  void readCommitted() throws Exception {
    final CountDownLatch sem1 = new CountDownLatch(1);
    final CountDownLatch sem2 = new CountDownLatch(1);
    final CountDownLatch sem3 = new CountDownLatch(1);

    final Thread thread1 = new Thread(() -> {
      database.transaction(() -> {
        database.newVertex("Node").set("id", 0, "origin", "thread1").save();
        assertThat(database.countType("Node", true)).isEqualTo(1);
      });

      sem1.countDown();

      database.transaction(() -> {
        try {
          sem2.await();
          // CHECK THE NEW RECORD (PHANTOM READ) IS VISIBLE
          assertThat(database.countType("Node", true)).isEqualTo(2);

          database.newVertex("Node").set("id", 3, "origin", "thread1").save();

          assertThat(database.countType("Node", true)).isEqualTo(3);

          // MODIFY A RECORD
          database.query("sql", "select from Node where id = 0").nextIfAvailable().getRecord().get().asVertex().modify().set("modified", true).save();

        } catch (InterruptedException e) {
          fail("InterruptedException occurred");
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
          assertThat(database.countType("Node", true)).isEqualTo(1);

          database.newVertex("Node").set("id", 1, "origin", "thread2").save();
          assertThat(database.countType("Node", true)).isEqualTo(2);

        } catch (InterruptedException e) {
          fail("InterruptedException occurred");
          throw new RuntimeException(e);
        }
      });

      sem2.countDown();

      database.transaction(() -> {
        try {
          sem3.await();

          assertThat(database.countType("Node", true)).isEqualTo(3);

          // CHECK THE NEW RECORD WAS MODIFIED
          assertThat((Boolean) database.query("sql", "select from Node where id = 0").nextIfAvailable().getProperty("modified")).isTrue();

        } catch (InterruptedException e) {
          fail("InterruptedException occurred");
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
  void repeatableRead() throws Exception {
    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {

      final CountDownLatch sem1 = new CountDownLatch(1);
      final CountDownLatch sem2 = new CountDownLatch(1);
      final CountDownLatch sem3 = new CountDownLatch(1);

      final Thread thread1 = new Thread(() -> {
        database.transaction(() -> {
          database.newVertex("Node").set("id", 0, "origin", "thread1").save();
          assertThat(database.countType("Node", true)).isEqualTo(1);
        });

        sem1.countDown();

        database.transaction(() -> {
          try {
            sem2.await();
            // CHECK THE NEW RECORD (PHANTOM READ) IS VISIBLE
            assertThat(database.countType("Node", true)).isEqualTo(2);

            database.newVertex("Node").set("id", 3, "origin", "thread1").save();

            assertThat(database.countType("Node", true)).isEqualTo(3);

            // MODIFY A RECORD
            database.query("sql", "select from Node where id = 0").nextIfAvailable().getRecord().get().asVertex().modify().set("modified", true).save();

          } catch (InterruptedException e) {
            fail("InterruptedException occurred");
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
            assertThat(database.countType("Node", true)).isEqualTo(1);

            database.newVertex("Node").set("id", 1, "origin", "thread2").save();
            assertThat(database.countType("Node", true)).isEqualTo(2);

          } catch (InterruptedException e) {
            fail("InterruptedException occurred");
            throw new RuntimeException(e);
          }
        });

        database.transaction(() -> {
          sem2.countDown();

          try {
            assertThat(database.countType("Node", true)).isEqualTo(2);

              assertThat(
                      database.query("sql", "select from Node where id = 0")
                              .nextIfAvailable()
                              .<Boolean>getProperty("modified")
              ).isNull();

            sem3.await();

            // CHECK THE NEW RECORD WAS MODIFIED
            assertThat(
                    database.query("sql", "select from Node where id = 0")
                            .nextIfAvailable()
                            .<Boolean>getProperty("modified")
            ).isNull();

          } catch (InterruptedException e) {
            fail("InterruptedException occurred");
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
