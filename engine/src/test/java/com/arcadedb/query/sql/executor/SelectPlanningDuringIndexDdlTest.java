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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
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
  private static final int READERS          = 6;
  private static final int ROUNDS           = 120;
  private static final int COMPOSITE_ROUNDS = 300;

  // MATCH estimates the filter against the indexes of the type once the type has more than its threshold of records
  private static final String MATCH = "MATCH {type: Person, as: p, where: (id = ?)} RETURN p.id AS id";

  // WHERE with an index, ORDER BY served by an index, min/max served by an index, a CONTAINSTEXT that may meet a full-text index, MATCH
  private static final String[] QUERIES = {
      "SELECT count(*) AS c FROM Person WHERE id < ?",
      "SELECT count(*) AS c FROM Person WHERE id = ?",
      "SELECT id FROM Person WHERE id > ? ORDER BY id LIMIT 5",
      "SELECT id FROM Person WHERE id >= ? ORDER BY id DESC LIMIT 5",
      "SELECT min(id) AS m, max(id) AS x FROM Person WHERE id >= ?",
      "SELECT count(*) AS c FROM Person WHERE id < ? AND name CONTAINSTEXT 'x'",
      MATCH };

  /** What the DDL thread creates and drops in each round. */
  private enum Ddl {
    // an index on a new property, and a composite one sharing the permanent index's first property
    KEY,
    FULL_TEXT,
    // the composite one only
    COMPOSITE
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void queriesDoNotFailWhileAnotherThreadCreatesAndDropsIndexes() throws Exception {
    runRace(QUERIES, Ddl.KEY, ROUNDS);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void queriesDoNotFailWhileAnotherThreadCreatesAndDropsFullTextIndexes() throws Exception {
    runRace(QUERIES, Ddl.FULL_TEXT, ROUNDS);
  }

  /**
   * The race behind the rare failures of {@link #queriesDoNotFailWhileAnotherThreadCreatesAndDropsIndexes()}, isolated so that a
   * run meets it far more often: every reader plans the MATCH, whose root estimate reads the entries of the composite index under
   * {@code id = ?} with a range, and a drop that starts right after the estimate found the index ready deletes the sub-index file
   * under that range. The page manager's "File with id n was not found" reached the caller of the query, as the estimate took
   * only an {@link IndexException} for an index that went away while being read.
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void matchPlanningDoesNotFailWhileAnotherThreadDropsAnIndexItEstimatesFrom() throws Exception {
    runRace(new String[] { MATCH }, Ddl.COMPOSITE, COMPOSITE_ROUNDS);
  }

  @Test
  void typeIndexWithoutSubIndexesHasNoTypeAndThrows() {
    final DocumentType type = database.getSchema().createDocumentType("Empty");
    final TypeIndex index = new TypeIndex("Empty[x]", type);

    assertThat(index.getType()).isNull();
    assertThatThrownBy(index::getNullStrategy).isInstanceOf(IndexException.class);
    assertThatThrownBy(index::getPropertyNames).isInstanceOf(IndexException.class);
  }

  @Test
  void typeIndexIsReadyForQueriesOnlyWhilePopulatedAndNotDropped() {
    final DocumentType type = database.getSchema().createDocumentType("Ready");
    type.createProperty("x", Type.LONG);

    assertThat(new TypeIndex("Ready[none]", type).isReadyForQueries()).isFalse();

    final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "x");
    assertThat(index.isReadyForQueries()).isTrue();
    assertThat(type.getAllIndexes(true)).containsExactly(index);

    index.drop();
    assertThat(index.isReadyForQueries()).isFalse();
    assertThat(type.getAllIndexes(true)).isEmpty();
  }

  private void runRace(final String[] queries, final Ddl ddl, final int rounds) throws Exception {
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
    final boolean checkCounts = queries == QUERIES;
    for (int t = 0; t < READERS; t++) {
      final Random random = new Random(t);
      final Thread thread = new Thread(() -> {
        while (!stop.get()) {
          final int q = random.nextInt(queries.length);
          final long bound = random.nextInt(2_000);
          try (final ResultSet rs = database.query("sql", queries[q], bound)) {
            // ids 0..1999 are static: the count queries have a known answer, a plan on a half-built index must not change it
            Long count = null;
            while (rs.hasNext()) {
              final Result row = rs.next();
              if (checkCounts && q < 2)
                count = row.getProperty("c");
            }
            if (checkCounts && q == 0 && (count == null || count != bound))
              failures.computeIfAbsent("wrong count for id < " + bound, k -> new AtomicLong()).incrementAndGet();
            else if (checkCounts && q == 1 && (count == null || count != 1))
              failures.computeIfAbsent("wrong count for id = " + bound, k -> new AtomicLong()).incrementAndGet();
            answered.incrementAndGet();
          } catch (final Throwable e) {
            failures.computeIfAbsent(e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).split("\n")[0],
                k -> new AtomicLong()).incrementAndGet();
          }
        }
      });
      readers.add(thread);
      thread.start();
    }

    try {
      final boolean fullText = ddl == Ddl.FULL_TEXT;
      for (int i = 0; i < rounds; i++) {
        database.command("sql", "CREATE PROPERTY Person.p" + i + (fullText ? " STRING" : " LONG"));
        if (ddl != Ddl.COMPOSITE)
          database.command("sql", "CREATE INDEX ON Person (p" + i + ") " + (fullText ? "FULL_TEXT" : "NOTUNIQUE"));
        // a composite index sharing the permanent one's first property ties with it when ranking WHERE id = ?
        if (!fullText) {
          database.command("sql", "CREATE INDEX ON Person (id, p" + i + ") NOTUNIQUE");
          database.command("sql", "DROP INDEX `Person[id,p" + i + "]`");
        }
        if (ddl != Ddl.COMPOSITE)
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
