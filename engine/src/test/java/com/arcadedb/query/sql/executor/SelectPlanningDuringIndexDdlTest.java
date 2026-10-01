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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8855: a query planned while another thread creates or drops an index on the same type must not fail, even when
 * the query does not use that index. The planner used to walk the type's index list while the DDL changed it, and read
 * the type and null strategy of an index not yet populated (or already invalidated).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SelectPlanningDuringIndexDdlTest extends TestHelper {
  private static final int READERS = 6;
  private static final int ROUNDS  = 120;

  // WHERE with an index, ORDER BY served by an index, min/max served by an index, and a CONTAINSTEXT that may meet a full-text index
  private static final String[] QUERIES = {
      "SELECT count(*) AS c FROM Person WHERE id < ?",
      "SELECT count(*) AS c FROM Person WHERE id = ?",
      "SELECT id FROM Person WHERE id > ? ORDER BY id LIMIT 5",
      "SELECT id FROM Person WHERE id >= ? ORDER BY id DESC LIMIT 5",
      "SELECT min(id) AS m, max(id) AS x FROM Person WHERE id >= ?",
      "SELECT count(*) AS c FROM Person WHERE id < ? AND name CONTAINSTEXT 'x'" };

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void queriesDoNotFailWhileAnotherThreadCreatesAndDropsIndexes() throws Exception {
    runRace("NOTUNIQUE");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void queriesDoNotFailWhileAnotherThreadCreatesAndDropsFullTextIndexes() throws Exception {
    runRace("FULL_TEXT");
  }

  @Test
  void typeIndexWithoutSubIndexesIsNeverPlannedOn() {
    final DocumentType type = database.getSchema().createDocumentType("Empty");
    final TypeIndex index = new TypeIndex("Empty[x]", type);

    assertThat(index.getType()).isNull();
    assertThatThrownBy(index::getNullStrategy).isInstanceOf(IndexException.class);
    assertThatThrownBy(index::getPropertyNames).isInstanceOf(IndexException.class);
  }

  private void runRace(final String indexKind) throws Exception {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.command("sql", "CREATE PROPERTY Person.name STRING");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.transaction(() -> {
      for (long i = 0; i < 2_000; i++)
        database.newVertex("Person").set("id", i).save();
    });

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong answered = new AtomicLong();
    final Map<String, AtomicLong> failures = new ConcurrentHashMap<>();
    final List<Thread> readers = new ArrayList<>();
    for (int t = 0; t < READERS; t++) {
      final Random random = new Random(t);
      final Thread thread = new Thread(() -> {
        while (!stop.get()) {
          try (final ResultSet rs = database.query("sql", QUERIES[random.nextInt(QUERIES.length)], (long) random.nextInt(2_000))) {
            while (rs.hasNext())
              rs.next();
            answered.incrementAndGet();
          } catch (final Exception e) {
            failures.computeIfAbsent(e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).split("\n")[0],
                k -> new AtomicLong()).incrementAndGet();
          }
        }
      });
      readers.add(thread);
      thread.start();
    }

    try {
      for (int i = 0; i < ROUNDS; i++) {
        database.command("sql", "CREATE PROPERTY Person.p" + i + (indexKind.equals("FULL_TEXT") ? " STRING" : " LONG"));
        database.command("sql", "CREATE INDEX ON Person (p" + i + ") " + indexKind);
        // a composite index sharing the permanent one's first property ties with it when ranking WHERE id = ?
        if (!indexKind.equals("FULL_TEXT")) {
          database.command("sql", "CREATE INDEX ON Person (id, p" + i + ") NOTUNIQUE");
          database.command("sql", "DROP INDEX `Person[id,p" + i + "]`");
        }
        database.command("sql", "DROP INDEX `Person[p" + i + "]`");
      }
    } finally {
      stop.set(true);
      for (final Thread thread : readers)
        thread.join();
    }

    assertThat(answered.get()).isGreaterThan(0);
    assertThat(failures).as("queries answered: " + answered.get()).isEmpty();
  }
}
