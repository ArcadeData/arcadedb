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
package com.arcadedb.query;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8865: a TypeIndex is registered on its type before the build has scanned every bucket, so the planners could choose it and
 * answer from a partially populated index. {@code TypeIndex.isReadyForQueries()} (#9331) is the "population complete" state, and
 * every reader of a type's indexes has to honour it: here SQL and OpenCypher, by equality, range, {@code IN} and an ordering that
 * only an index can give, probed from inside the build while the index holds part of the records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8865PartiallyBuiltIndexTest extends TestHelper {
  private static final int ROWS = 400;

  @Override
  public void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person BUCKETS 4");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.transaction(() -> {
      for (long i = 0; i < ROWS; i++)
        database.newVertex("Person").set("id", i).save();
    });
  }

  @Test
  void noQuerySurfaceAnswersFromAnIndexWhileItIsBeingBuilt() {
    assertProbedWhileBuilding(false);
  }

  @Test
  void noQuerySurfaceAnswersFromAUniqueIndexWhileItIsBeingBuilt() {
    assertProbedWhileBuilding(true);
  }

  private void assertProbedWhileBuilding(final boolean unique) {
    final AtomicBoolean probed = new AtomicBoolean();
    final AtomicLong wrong = new AtomicLong();
    final StringBuilder detail = new StringBuilder();

    database.getSchema().getType("Person").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, unique, new String[] { "id" }, 262_144,
        (document, totalIndexed) -> {
          // the index is registered bucket by bucket and holds part of the records here
          if (totalIndexed < 10 || !probed.compareAndSet(false, true))
            return;
          check(wrong, detail, "sql =", 1, count(database.query("sql", "SELECT FROM Person WHERE id = 7")));
          check(wrong, detail, "sql >=", ROWS - 50, count(database.query("sql", "SELECT FROM Person WHERE id >= 50")));
          check(wrong, detail, "sql between", 21, count(database.query("sql", "SELECT FROM Person WHERE id BETWEEN 100 AND 120")));
          check(wrong, detail, "sql in", 3, count(database.query("sql", "SELECT FROM Person WHERE id IN [1, 200, 399]")));
          check(wrong, detail, "sql order by", ROWS, count(database.query("sql", "SELECT FROM Person ORDER BY id")));
          check(wrong, detail, "sql count", ROWS, database.query("sql", "SELECT count(*) AS c FROM Person WHERE id >= 0").next()
              .<Number>getProperty("c").longValue());
          check(wrong, detail, "cypher =", 1, count(database.query("opencypher", "MATCH (n:Person {id: 7}) RETURN n.id")));
          check(wrong, detail, "cypher where", 1, count(database.query("opencypher", "MATCH (n:Person) WHERE n.id = 399 RETURN n.id")));
          check(wrong, detail, "cypher range", ROWS - 50,
              count(database.query("opencypher", "MATCH (n:Person) WHERE n.id >= 50 RETURN n.id")));
          check(wrong, detail, "cypher param", 1, count(database.query("opencypher", "MATCH (n:Person {id: $id}) RETURN n.id", Map.of("id", 123L))));
        });

    assertThat(probed.get()).as("the build callback must have run").isTrue();
    assertThat(wrong.get()).as("answers that came from the half built index: " + detail).isZero();
    assertThat(count(database.query("sql", "SELECT FROM Person WHERE id = 7"))).isEqualTo(1);
    assertThat(count(database.query("opencypher", "MATCH (n:Person {id: 7}) RETURN n.id"))).isEqualTo(1);
  }

  private static void check(final AtomicLong wrong, final StringBuilder detail, final String surface, final long expected, final long actual) {
    if (expected != actual) {
      wrong.incrementAndGet();
      detail.append(surface).append(" expected ").append(expected).append(" got ").append(actual).append("; ");
    }
  }

  private static long count(final ResultSet rs) {
    try (rs) {
      long rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      return rows;
    }
  }
}
