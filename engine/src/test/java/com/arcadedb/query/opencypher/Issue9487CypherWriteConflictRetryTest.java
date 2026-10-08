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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9487: a write conflict raised while an openCypher {@code SET} was executing escaped
 * {@code Database.transaction(tx, true, retries)} as a {@code CommandExecutionException} wrapping the
 * {@code ConcurrentModificationException}, which is not a {@code NeedRetryException}, so the retry loop never saw it.
 * The same SQL {@code UPDATE} commits every call.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9487CypherWriteConflictRetryTest extends TestHelper {
  private static final int THREADS = 4;
  private static final int CALLS   = 300;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Product");
    database.command("sql", "CREATE PROPERTY Product.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Product.views INTEGER");
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.command("sql", "INSERT INTO Product SET pid = " + i + ", views = 0");
    });
  }

  @Test
  void sqlUpdateRetriesEveryConflict() throws InterruptedException {
    contendOnTheSameProducts("sql", "UPDATE Product SET views = views + 1 WHERE pid IN [0, 1, 2]");
  }

  @Test
  void cypherSetRetriesEveryConflict() throws InterruptedException {
    contendOnTheSameProducts("opencypher", "MATCH (n:Product) WHERE n.pid IN [0, 1, 2] SET n.views = n.views + 1");
  }

  @Test
  void cypherMergeWithSetRetriesEveryConflict() throws InterruptedException {
    contendOnTheSameProducts("opencypher",
        "MERGE (n:Product {pid: 0}) ON MATCH SET n.views = n.views + 1");
  }

  private void contendOnTheSameProducts(final String language, final String update) throws InterruptedException {
    final Map<String, Integer> escaped = new TreeMap<>();
    final AtomicInteger ok = new AtomicInteger();
    final List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < THREADS; t++) {
      final Thread thread = new Thread(() -> {
        for (int i = 0; i < CALLS; i++) {
          try {
            database.transaction(() -> database.command(language, update), true, 50);
            ok.incrementAndGet();
          } catch (final Exception e) {
            final String key = e.getClass().getName() + (e.getCause() == null ? "" : " <- " + e.getCause().getClass().getName());
            synchronized (escaped) {
              escaped.merge(key, 1, Integer::sum);
            }
          }
        }
      });
      threads.add(thread);
      thread.start();
    }
    for (final Thread thread : threads)
      thread.join();

    assertThat(escaped).as("exceptions that escaped the retry loop").isEmpty();
    assertThat(ok.get()).isEqualTo(THREADS * CALLS);
    try (final ResultSet rs = database.query("sql", "SELECT views AS v FROM Product WHERE pid = 0")) {
      assertThat(((Number) rs.next().getProperty("v")).intValue()).isEqualTo(THREADS * CALLS);
    }
  }
}
