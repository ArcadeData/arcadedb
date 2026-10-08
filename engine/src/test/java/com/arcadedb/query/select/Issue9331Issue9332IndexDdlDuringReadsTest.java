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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SelectExecutionPlanner;
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

/**
 * Issues #9331 and #9332: a SQL {@code SELECT}, the native {@code database.select()} and {@code lookupByKey()} must keep
 * answering while another thread creates and drops the very index they query. The previous coverage churned indexes on
 * properties the readers never used, so no reader ever resolved the index being dropped.
 * <ul>
 * <li>#9331: {@code FetchFromIndexStep} reads the index when the step starts, after the planner validated it, and
 * the native executor read the properties and the type of candidates with no guard;</li>
 * <li>#9332: {@code holdsFoldedKeys} read the metadata of an index twice, and a drop between the two reads turned the
 * null check into a NullPointerException.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9331Issue9332IndexDdlDuringReadsTest extends TestHelper {
  private static final int READERS = 8;
  private static final int ROUNDS  = 150;
  private static final int ROWS    = 200;

  @Test
  void holdsFoldedKeysAnswersFalseForAnIndexWithoutMetadataInsteadOfFailing() {
    final DocumentType type = database.getSchema().createDocumentType("Folded");
    type.createProperty("x", Type.LONG);
    final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "x");
    assertThat(SelectExecutionPlanner.holdsFoldedKeys(index)).isFalse();

    index.drop();

    // The index went away with its sub-indexes: its metadata is null, which used to be read twice
    assertThat(((IndexInternal) index).getMetadata()).isNull();
    assertThat(SelectExecutionPlanner.holdsFoldedKeys(index)).isFalse();
    assertThat(SelectExecutionPlanner.isIndexCaseInsensitive(index, 0)).isFalse();
  }

  @Test
  void anIndexStillBeingBuiltIsNotUsedByAnyQuerySurface() {
    database.command("sql", "CREATE DOCUMENT TYPE Person BUCKETS 4");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.transaction(() -> {
      for (long i = 0; i < ROWS; i++)
        database.newDocument("Person").set("id", i).save();
    });

    final long[] found = new long[3];
    final AtomicBoolean probed = new AtomicBoolean();
    // Called for every record the build indexes: the index is registered by then, bucket by bucket, and holds only part of the
    // records. Nothing may answer from it until the build is over, so every id is found through the other path
    database.getSchema().getType("Person").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, new String[] { "id" }, 262_144,
        (document, totalIndexed) -> {
          if (totalIndexed < 10 || !probed.compareAndSet(false, true))
            return;
          for (long id = 0; id < ROWS; id++) {
            found[0] += countRows(database.query("sql", "SELECT FROM Person WHERE id = ?", id));
            found[1] += database.select().fromType("Person").where().property("id").eq().value(id).documents().stream().count();
            try {
              final var cursor = database.lookupByKey("Person", new String[] { "id" }, new Object[] { id });
              while (cursor.hasNext()) {
                cursor.next();
                found[2]++;
              }
            } catch (final IllegalArgumentException e) {
              // no index yet: the documented answer, the caller scans
              found[2]++;
            }
          }
        });

    assertThat(probed.get()).as("the build callback must have run").isTrue();
    assertThat(found).as("every id found through SQL, select() and lookupByKey while the index was half built")
        .containsExactly(ROWS, ROWS, ROWS);
    assertThat(countRows(database.query("sql", "SELECT FROM Person WHERE id = 7"))).isEqualTo(1);
    assertThat(database.lookupByKey("Person", new String[] { "id" }, new Object[] { 7L }).hasNext()).isTrue();
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void sqlSelectNativeSelectAndLookupByKeyKeepAnsweringWhileTheQueriedIndexIsDroppedAndRecreated() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.transaction(() -> {
      for (long i = 0; i < ROWS; i++)
        database.newDocument("Person").set("id", i).save();
    });

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong answered = new AtomicLong();
    final AtomicLong wrongRowCounts = new AtomicLong();
    final Map<String, AtomicLong> failures = new ConcurrentHashMap<>();
    final List<Thread> readers = new ArrayList<>();
    for (int t = 0; t < READERS; t++) {
      final Random random = new Random(t);
      final int flavour = t % 3;
      final Thread thread = new Thread(() -> {
        while (!stop.get()) {
          final long id = random.nextInt(ROWS);
          try {
            final int rows = switch (flavour) {
              case 0 -> countRows(database.query("sql", "SELECT FROM Person WHERE id = ?", id));
              case 1 -> {
                int n = 0;
                try (final var it = database.select().fromType("Person").where().property("id").eq().value(id).documents()) {
                  while (it.hasNext()) {
                    final Document ignored = it.next();
                    n++;
                  }
                }
                yield n;
              }
              default -> {
                // lookupByKey answers "no index" with an IllegalArgumentException while the index is not there: expected
                int n = 0;
                try (final var cursor = database.lookupByKey("Person", new String[] { "id" }, new Object[] { id })) {
                  while (cursor.hasNext()) {
                    cursor.next();
                    n++;
                  }
                } catch (final IllegalArgumentException e) {
                  if (!e.getMessage().startsWith("No index has been created"))
                    throw e;
                  n = 1;
                }
                yield n;
              }
            };
            if (rows != 1)
              wrongRowCounts.incrementAndGet();
            answered.incrementAndGet();
          } catch (final Throwable e) {
            failures.computeIfAbsent(
                "flavour " + flavour + " " + e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).split("\n")[0],
                k -> new AtomicLong()).incrementAndGet();
          }
        }
      });
      readers.add(thread);
      thread.start();
    }

    try {
      for (int i = 0; i < ROUNDS; i++) {
        database.command("sql", "CREATE INDEX ON Person (id) NOTUNIQUE");
        database.command("sql", "DROP INDEX `Person[id]`");
      }
    } finally {
      stop.set(true);
      for (final Thread thread : readers)
        thread.join();
    }

    assertThat(answered.get()).isGreaterThan(0);
    assertThat(failures).as("statements answered: " + answered.get()).isEmpty();
    // A query that was already reading the index in the instant it was dropped can miss the rows of the sub-indexes gone by then:
    // it was planned before the drop completed (see TypeIndex#drop). That is a handful in tens of thousands, never the half-built
    // index of the other test, which answered most of them wrongly
    assertThat(wrongRowCounts.get()).as("answers that missed their row, of " + answered.get())
        .isLessThan(Math.max(5, answered.get() / 200));
  }

  private static int countRows(final ResultSet rs) {
    try (rs) {
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      return rows;
    }
  }
}
