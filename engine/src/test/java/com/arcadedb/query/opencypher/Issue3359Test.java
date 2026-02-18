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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3359.
 * Tests that concurrent read-only Cypher queries do not throw
 * ConcurrentModificationException when schema is being modified concurrently.
 * <p>
 * The root cause is that LocalSchema.getTypes() and LocalDocumentType.getSuperTypes()
 * return views over internal mutable collections (HashMap.values() and ArrayList).
 * When a writer thread creates new types while a reader thread iterates over types
 * (e.g., via MATCH (n) which scans all vertex types), the iteration fails with
 * ConcurrentModificationException.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/3359">Issue 3359</a>
 */
class Issue3359Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue3359-test").create();

    // Create initial types and data
    database.getSchema().createVertexType("PIPELINE_CONFIG");
    database.getSchema().createVertexType("USER_RIGHTS");
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Document");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.command("opencypher", "CREATE (n:Person {name: 'Person" + i + "'})");
      for (int i = 0; i < 10; i++)
        database.command("opencypher", "CREATE (n:Document {title: 'Doc" + i + "'})");
      database.command("opencypher", "CREATE (n:PIPELINE_CONFIG {key: 'config1'})");
      database.command("opencypher", "CREATE (n:USER_RIGHTS {key: 'rights1'})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Reproduces ConcurrentModificationException when multiple threads run the same
   * read-only Cypher query that scans all vertex types (MATCH (n)) while another
   * thread creates new vertex types.
   */
  @Test
  void concurrentReadQueriesWithSchemaModification() throws Exception {
    final int readerThreads = 4;
    final int iterations = 50;
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final AtomicBoolean running = new AtomicBoolean(true);
    final CountDownLatch startLatch = new CountDownLatch(1);

    final ExecutorService executor = Executors.newFixedThreadPool(readerThreads + 1);
    final List<Future<?>> futures = new ArrayList<>();

    // Writer thread: continuously creates new vertex types to modify the schema
    futures.add(executor.submit(() -> {
      try {
        startLatch.await();
        for (int i = 0; i < iterations && error.get() == null; i++) {
          try {
            database.getSchema().getOrCreateVertexType("DynamicType" + i);
            Thread.sleep(1);
          } catch (final Exception e) {
            if (e instanceof ConcurrentModificationException) {
              error.compareAndSet(null, e);
              break;
            }
          }
        }
      } catch (final InterruptedException ignored) {
      } finally {
        running.set(false);
      }
    }));

    // Reader threads: run the query from the issue report
    for (int t = 0; t < readerThreads; t++) {
      futures.add(executor.submit(() -> {
        try {
          startLatch.await();
          while (running.get() && error.get() == null) {
            try (final ResultSet rs = database.query("opencypher",
                """
                MATCH (n) \
                WHERE NOT (n:PIPELINE_CONFIG OR n:USER_RIGHTS) \
                RETURN labels(n)[0] AS NodeType, COUNT(n) AS count \
                ORDER BY count DESC""")) {
              while (rs.hasNext())
                rs.next();
            } catch (final ConcurrentModificationException e) {
              error.compareAndSet(null, e);
              break;
            } catch (final Exception e) {
              // Check if the root cause is ConcurrentModificationException
              Throwable cause = e;
              while (cause != null) {
                if (cause instanceof ConcurrentModificationException) {
                  error.compareAndSet(null, cause);
                  break;
                }
                cause = cause.getCause();
              }
              if (error.get() != null)
                break;
            }
          }
        } catch (final InterruptedException ignored) {
        }
      }));
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for all threads to complete
    for (final Future<?> f : futures)
      f.get(30, TimeUnit.SECONDS);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    // Verify no ConcurrentModificationException occurred
    assertThat(error.get())
        .as("Should not throw ConcurrentModificationException during concurrent read queries with schema changes")
        .isNull();
  }
}
