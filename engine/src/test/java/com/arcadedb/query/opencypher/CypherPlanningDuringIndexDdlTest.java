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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
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
 * Issue #8918: a Cypher MATCH, MERGE or planner statistics walk run while another thread creates or drops an index on the same
 * type must not fail, even when the statement does not use that index. The openCypher walks of the type's indexes read the type
 * of an index not yet populated (null) or already invalidated, as the SQL planner's did before issue #8855.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherPlanningDuringIndexDdlTest extends TestHelper {
  private static final int READERS = 6;
  private static final int ROUNDS  = 120;

  @Test
  void typeIndexWithoutSubIndexesIsNotAnExactKeyLookup() {
    final DocumentType type = database.getSchema().createDocumentType("Empty");
    final TypeIndex index = new TypeIndex("Empty[x]", type);

    assertThat(index.getPropertyNamesIfExactKeyLookup()).isNull();
    assertThat(TypeIndex.filterReadyForQueries(List.of(index))).isEmpty();
  }

  @Test
  void exactKeyLookupPropertiesOnlyForKeyIndexes() {
    final DocumentType type = database.getSchema().createDocumentType("Keys");
    type.createProperty("x", Type.LONG);
    type.createProperty("t", Type.STRING);
    final TypeIndex key = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "x");
    final TypeIndex fullText = type.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "t");

    assertThat(key.getPropertyNamesIfExactKeyLookup()).containsExactly("x");
    assertThat(fullText.getPropertyNamesIfExactKeyLookup()).isNull();

    key.drop();
    assertThat(key.getPropertyNamesIfExactKeyLookup()).isNull();
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void matchAndMergeDoNotFailWhileAnotherThreadCreatesAndDropsIndexes() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.transaction(() -> {
      for (long i = 0; i < 200; i++)
        database.newVertex("Person").set("id", i).save();
    });

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong answered = new AtomicLong();
    final Map<String, AtomicLong> failures = new ConcurrentHashMap<>();
    final List<Thread> readers = new ArrayList<>();
    for (int t = 0; t < READERS; t++) {
      final Random random = new Random(t);
      final boolean merge = t % 3 == 0;
      final Thread thread = new Thread(() -> {
        while (!stop.get()) {
          final long id = random.nextInt(200);
          try {
            if (merge)
              database.transaction(() -> {
                try (final ResultSet rs = database.command("opencypher", "MERGE (p:Person {id: $id}) RETURN p.id AS id", Map.of("id", id))) {
                  while (rs.hasNext())
                    rs.next();
                }
              });
            else
              try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE p.id = " + id + " RETURN p.id AS id")) {
                int rows = 0;
                while (rs.hasNext()) {
                  rs.next();
                  rows++;
                }
                if (rows != 1)
                  failures.computeIfAbsent("wrong row count " + rows + " for id = " + id, k -> new AtomicLong()).incrementAndGet();
              }
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
      for (int i = 0; i < ROUNDS; i++) {
        database.command("sql", "CREATE PROPERTY Person.p" + i + " LONG");
        database.command("sql", "CREATE INDEX ON Person (p" + i + ") NOTUNIQUE");
        database.command("sql", "DROP INDEX `Person[p" + i + "]`");
      }
    } finally {
      stop.set(true);
      for (final Thread thread : readers)
        thread.join();
    }

    assertThat(answered.get()).isGreaterThan(0);
    assertThat(failures).as("statements answered: " + answered.get()).isEmpty();
  }
}
