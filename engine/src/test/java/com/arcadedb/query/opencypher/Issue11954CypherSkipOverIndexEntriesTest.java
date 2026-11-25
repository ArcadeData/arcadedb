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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #11954: the SKIP of a MATCH answered by an index scan in key order passes over the skipped rows without loading their vertices,
 * when the RETURN reads nothing that can fail on a row and nothing else stands between. The rows must stay the ones the statement
 * returns without that: every statement here is run with {@code SKIP s LIMIT l} and compared with the same statement run with
 * {@code LIMIT s + l} minus its first {@code s} rows, over random offsets, both directions, ranges, null keys (from the label and
 * from an index that holds them), duplicate keys of a NOTUNIQUE index, an index declared on a super type, records deleted, updated
 * and created in the same transaction, a read listener (which must still see every vertex it saw) and a user who may not read some
 * of the buckets.
 */
class Issue11954CypherSkipOverIndexEntriesTest extends TestHelper {
  private static final int          RECORDS = 600;
  private static final List<String> LABELS  = List.of("Base", "Item");

  @Override
  protected void beginTest() {
    createSchemaAndRecords(database);
  }

  @Test
  void skippedRowsAreNotLoaded() {
    // Ascending the vertices with no key come after the index entries, descending with IS NOT NULL there are none; on a label
    // whose index is declared on the super type, the entries of the other types are dropped from their bucket
    assertLoadsAtMost(database, "MATCH (n:Base) RETURN n.u AS u ORDER BY n.u SKIP 400 LIMIT 5", 30);
    assertLoadsAtMost(database, "MATCH (n:Base) WHERE n.u IS NOT NULL RETURN n ORDER BY n.u DESC SKIP 400 LIMIT 5", 30);
    assertLoadsAtMost(database, "MATCH (n:Item) RETURN n, n.k ORDER BY n.u SKIP 100 LIMIT 5", 30);
    assertLoadsAtMost(database, "MATCH (n:Base) RETURN n.n ORDER BY n.n SKIP 400 LIMIT 5", 30);

    try (final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (n:Base) RETURN n.u AS u ORDER BY n.u SKIP 400 LIMIT 5")) {
      assertThat(result.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("SKIP 400 (passed over in the index scan)");
    }
    // A RETURN item that can fail on a row keeps the rows it skips
    try (final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (n:Base) RETURN n.u + 1 AS u ORDER BY n.u SKIP 400 LIMIT 5")) {
      assertThat(result.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("SKIP 400").doesNotContain("passed over");
    }
  }

  @Test
  void rowsAreTheOnesWithoutTheSkip() {
    assertSameRowsAsWithoutSkip(database, new Random(11954));
  }

  @Test
  void rowsAreTheOnesWithoutTheSkipInATransactionThatChangedTheRecords() {
    final Random random = new Random(119541);
    database.begin();
    try {
      final List<RID> rids = new ArrayList<>();
      database.query("sql", "SELECT FROM Base").stream().forEach(row -> rids.add(row.getIdentity().orElseThrow()));
      for (final RID rid : rids) {
        final int dice = random.nextInt(10);
        if (dice < 2)
          rid.asVertex().delete();
        else if (dice < 4) {
          final MutableVertex vertex = rid.asVertex().modify();
          vertex.set("k", random.nextInt(10) == 0 ? null : random.nextInt(40));
          vertex.set("n", random.nextInt(25));
          vertex.save();
        }
      }
      for (int i = 0; i < 60; i++)
        database.newVertex(LABELS.get(i % 2)).set("k", random.nextInt(40)).set("u", RECORDS + i).save();
      assertSameRowsAsWithoutSkip(database, random);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aReadListenerStillSeesEveryVertexItSaw() {
    // Only the read fires the listener, which can rewrite or hide the vertex: the vertices of a type that has one are loaded,
    // skipped or not
    final AtomicInteger reads = new AtomicInteger();
    final AfterRecordReadListener listener = record -> {
      reads.incrementAndGet();
      return record;
    };
    database.getSchema().getType("Item").getEvents().registerListener(listener);
    try {
      assertSameRowsAsWithoutSkip(database, new Random(119542));
      reads.set(0);
      assertThat(rows(database, "MATCH (n:Item) RETURN n ORDER BY n.u SKIP 100 LIMIT 5")).hasSize(5);
      assertThat(reads.get()).isGreaterThanOrEqualTo(105);
    } finally {
      database.getSchema().getType("Item").getEvents().unregisterListener(listener);
    }
  }

  @Test
  void aUserWhoMayNotReadABucketGetsWhatHeGotBefore() {
    final DatabaseFactory secured = new DatabaseFactory(getDatabasePath() + "-secured").setSecurity(db -> {
    });
    if (secured.exists())
      secured.open().drop();
    final Database restricted = secured.create();
    try {
      createSchemaAndRecords(restricted);
      final Set<Integer> hidden = new HashSet<>(restricted.getSchema().getType("Other").getBucketIds(false));
      final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContext(restricted.getDatabasePath());
      context.setCurrentUser(userWithoutReadOn(hidden));
      try {
        // The index on Base names vertices of Other, which the load refuses: a SKIP past them, or past every row, refuses too
        final String refused = outcome(restricted, "MATCH (n:Base) RETURN n.u AS u ORDER BY n.u LIMIT 5", 0);
        assertThat(refused).doesNotStartWith("[");
        assertThat(outcome(restricted, "MATCH (n:Base) RETURN n.u AS u ORDER BY n.u SKIP 150 LIMIT 5", 0)).isEqualTo(refused);
        assertThat(outcome(restricted, "MATCH (n:Base) RETURN n.u AS u ORDER BY n.u SKIP 100000 LIMIT 5", 0)).isEqualTo(refused);
        assertSameRowsAsWithoutSkip(restricted, new Random(119543));
      } finally {
        context.setCurrentUser(null);
      }
    } finally {
      restricted.drop();
      secured.close();
    }
  }

  private static void createSchemaAndRecords(final Database database) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Base");
      database.command("sql", "CREATE PROPERTY Base.k INTEGER");
      database.command("sql", "CREATE PROPERTY Base.u INTEGER");
      database.command("sql", "CREATE PROPERTY Base.n INTEGER");
      database.command("sql", "CREATE INDEX ON Base (k) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Base (u) UNIQUE");
      database.command("sql", "CREATE INDEX ON Base (n) NOTUNIQUE NULL_STRATEGY INDEX");
      database.command("sql", "CREATE VERTEX TYPE Item EXTENDS Base");
      database.command("sql", "CREATE VERTEX TYPE Other EXTENDS Base");
    });
    final Random random = new Random(42);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        final MutableVertex vertex = database.newVertex(i % 3 == 0 ? "Base" : i % 3 == 1 ? "Item" : "Other");
        // duplicate keys, about one null in ten
        vertex.set("k", random.nextInt(10) == 0 ? null : random.nextInt(40));
        vertex.set("u", i % 17 == 0 ? null : i);
        vertex.set("n", random.nextInt(7) == 0 ? null : random.nextInt(25));
        vertex.save();
      }
    });
  }

  private static List<String> statements(final String label) {
    final String match = "MATCH (n:" + label + ") ";
    return List.of(//
        match + "RETURN n ORDER BY n.k", //
        match + "RETURN n ORDER BY n.k DESC", //
        match + "RETURN n.k AS k, n.u AS u, 7 AS seven ORDER BY n.k", //
        match + "RETURN n.k AS k, n.u AS u ORDER BY k DESC", //
        match + "WHERE n.k IS NOT NULL RETURN n ORDER BY n.k DESC", //
        match + "WHERE n.k > 10 RETURN n ORDER BY n.k", //
        match + "WHERE n.k >= 10 AND n.k < 30 RETURN n ORDER BY n.k DESC", //
        match + "WHERE n.k <= 20 RETURN n ORDER BY n.k", //
        match + "RETURN n.k + 1 AS k1 ORDER BY n.k", //
        match + "RETURN DISTINCT n.k AS k ORDER BY k", //
        match + "RETURN n ORDER BY n.u", //
        match + "RETURN n.u AS u ORDER BY n.u DESC", //
        match + "WHERE n.u IS NOT NULL RETURN n.u AS u ORDER BY n.u", //
        match + "RETURN n ORDER BY n.n", //
        match + "RETURN n.n AS v, n ORDER BY n.n DESC", //
        match + "WHERE n.n IS NOT NULL RETURN n ORDER BY n.n");
  }

  private static void assertSameRowsAsWithoutSkip(final Database database, final Random random) {
    for (final String label : LABELS)
      for (final String statement : statements(label))
        for (int round = 0; round < 8; round++) {
          final int skip = switch (round) {
            case 0 -> 1;
            case 1 -> RECORDS + 50;
            default -> random.nextInt(RECORDS);
          };
          final int limit = 1 + random.nextInt(round == 2 ? 2 : 40);
          final String expected = outcome(database, statement + " LIMIT " + (skip + limit), skip);
          final String actual = outcome(database, statement + " SKIP " + skip + " LIMIT " + limit, 0);
          assertThat(actual).as("%s SKIP %d LIMIT %d", statement, skip, limit).isEqualTo(expected);
        }
  }

  private static List<String> rows(final Database database, final String statement) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet result = database.query("opencypher", statement)) {
      result.stream().forEach(row -> rows.add(row(row)));
    }
    return rows;
  }

  private static String row(final Result row) {
    final Map<String, Object> values = new TreeMap<>();
    for (final String name : row.getPropertyNames()) {
      final Object value = row.getProperty(name);
      values.put(name, value instanceof Document document ? document.getIdentity() : value);
    }
    return values.toString();
  }

  /** The rows of the statement past the first {@code drop}, or the simple name of the exception it fails with. */
  private static String outcome(final Database database, final String statement, final int drop) {
    try {
      final List<String> rows = rows(database, statement);
      return rows.subList(Math.min(drop, rows.size()), rows.size()).toString();
    } catch (final RuntimeException e) {
      return e.getClass().getSimpleName();
    }
  }

  private static void assertLoadsAtMost(final Database database, final String statement, final long loads) {
    final long before = (long) database.getStats().get("readRecord");
    final List<String> served = rows(database, statement);
    assertThat(served).as(statement).hasSize(5);
    assertThat((long) database.getStats().get("readRecord") - before).as("records loaded by %s", statement)
        .isLessThanOrEqualTo(loads);
  }

  /** A user who may do everything but read the records of the {@code hidden} buckets. */
  private static SecurityDatabaseUser userWithoutReadOn(final Set<Integer> hidden) {
    return new SecurityDatabaseUser() {
      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != ACCESS.READ_RECORD || !hidden.contains(fileId);
      }

      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }
}
