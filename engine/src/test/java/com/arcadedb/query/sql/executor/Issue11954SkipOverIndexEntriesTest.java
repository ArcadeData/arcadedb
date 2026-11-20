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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #11954: a SKIP over a scan that reads an index in order passes over the skipped rows without loading their records, when nothing
 * between the scan and the SKIP needs the record. The rows must stay the ones the statement returns without that: every statement
 * here is run with {@code SKIP s LIMIT l} and compared with the same statement run with {@code LIMIT s + l} minus its first
 * {@code s} rows, which loads every record it passes, over random offsets, both directions, inclusive and exclusive bounds, null
 * keys, duplicate keys of a NOTUNIQUE index, a composite index, an index declared on a super type, records deleted, updated and
 * created in the same transaction, a read listener that hides records and a user who may not read some of the buckets.
 */
class Issue11954SkipOverIndexEntriesTest extends TestHelper {
  private static final int          RECORDS = 600;
  private static final List<String> TYPES   = List.of("Base", "Item");

  @Override
  protected void beginTest() {
    createSchemaAndRecords(database);
  }

  @Test
  void skippedRowsAreNotLoaded() {
    // On a range, descending from the index, and on a subtype whose index is declared on the super type: the entries of the
    // other types are dropped from their bucket
    assertLoadsAtMost(database, "SELECT FROM Base WHERE u >= 0 ORDER BY u SKIP 400 LIMIT 5", 30);
    assertLoadsAtMost(database, "SELECT u FROM Base ORDER BY u DESC SKIP 400 LIMIT 5", 30);
    assertLoadsAtMost(database, "SELECT FROM Item WHERE u >= 0 ORDER BY u SKIP 100 LIMIT 5", 30);
    assertLoadsAtMost(database, "SELECT FROM Base WHERE k > 3 AND k <= 35 ORDER BY k DESC SKIP 300 LIMIT 5", 30);

    final ResultSet profiled = database.query("sql", "PROFILE SELECT FROM Base WHERE u >= 0 ORDER BY u SKIP 400 LIMIT 5");
    assertThat(profiled.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains(
        "400 rows passed over in the index, records not loaded");
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
        database.newVertex(TYPES.get(i % 2)).set("k", random.nextInt(40)).set("u", RECORDS + i).set("a", "a" + random.nextInt(4))
            .set("b", random.nextInt(10)).save();
      assertSameRowsAsWithoutSkip(database, random);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aReadListenerStillHidesTheRecordsItHides() {
    // Only the read applies the listener: the records of a type that has one are loaded, skipped or not
    final List<Integer> hidden = new ArrayList<>();
    database.query("sql", "SELECT u FROM Item WHERE u >= 0").stream().forEach(row -> {
      final int u = row.getProperty("u");
      if (u % 2 == 0)
        hidden.add(u);
    });
    assertThat(hidden).isNotEmpty();
    final List<String> withKey = rows(database, "SELECT u FROM Base WHERE u >= 0");

    final AfterRecordReadListener listener = record -> record.asDocument().get("u") instanceof Integer u && u % 2 == 0 ? null : record;
    database.getSchema().getType("Item").getEvents().registerListener(listener);
    try {
      assertSameRowsAsWithoutSkip(database, new Random(119542));
      final List<Integer> served = new ArrayList<>();
      database.query("sql", "SELECT u FROM Base WHERE u >= 0 ORDER BY u SKIP 10 LIMIT 1000").stream()
          .forEach(row -> served.add(row.getProperty("u")));
      assertThat(served).hasSize(withKey.size() - hidden.size() - 10).doesNotContainAnyElementsOf(hidden);
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
        // The index on Base names records of Other, which the load refuses: a SKIP past them, or past every row, refuses too
        assertThat(outcome(restricted, "SELECT FROM Base WHERE u >= 0 ORDER BY u SKIP 150 LIMIT 5", 0)).isEqualTo(
            "SecurityException");
        assertThat(outcome(restricted, "SELECT FROM Base WHERE u >= 0 ORDER BY u SKIP 100000 LIMIT 5", 0)).isEqualTo(
            "SecurityException");
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
      database.command("sql", "CREATE PROPERTY Base.a STRING");
      database.command("sql", "CREATE PROPERTY Base.b INTEGER");
      database.command("sql", "CREATE PROPERTY Base.n INTEGER");
      database.command("sql", "CREATE INDEX ON Base (k) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Base (u) UNIQUE");
      database.command("sql", "CREATE INDEX ON Base (a, b) NOTUNIQUE");
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
        vertex.set("a", "a" + random.nextInt(4));
        vertex.set("b", random.nextInt(8) == 0 ? null : random.nextInt(10));
        vertex.set("n", random.nextInt(7) == 0 ? null : random.nextInt(25));
        vertex.save();
      }
    });
  }

  private static List<String> statements(final String type) {
    return List.of(//
        "SELECT FROM " + type + " ORDER BY k", //
        "SELECT FROM " + type + " ORDER BY k DESC", //
        "SELECT k, u FROM " + type + " ORDER BY k", //
        "SELECT FROM " + type + " WHERE k > 10 ORDER BY k", //
        "SELECT FROM " + type + " WHERE k >= 10 AND k < 30 ORDER BY k DESC", //
        "SELECT FROM " + type + " WHERE k > 10 AND k <= 30 ORDER BY k", //
        "SELECT FROM " + type + " WHERE k < 20 ORDER BY k", //
        "SELECT FROM " + type + " WHERE k <= 20 ORDER BY k DESC", //
        "SELECT FROM " + type + " WHERE k BETWEEN 5 AND 25 ORDER BY k", //
        "SELECT FROM " + type + " WHERE k > 5", //
        "SELECT FROM " + type + " WHERE k >= 0 AND b > 4 ORDER BY k", //
        "SELECT DISTINCT k FROM " + type + " WHERE k >= 0 ORDER BY k", //
        "SELECT FROM " + type + " ORDER BY u", //
        "SELECT u FROM " + type + " ORDER BY u DESC", //
        "SELECT FROM " + type + " WHERE u >= 100 ORDER BY u", //
        "SELECT FROM " + type + " ORDER BY a, b", //
        "SELECT FROM " + type + " ORDER BY a DESC, b DESC", //
        "SELECT FROM " + type + " WHERE a > 'a1' ORDER BY a, b", //
        "SELECT FROM " + type + " ORDER BY n", //
        "SELECT FROM " + type + " ORDER BY n DESC", //
        "SELECT FROM " + type + " WHERE n > 3 ORDER BY n");
  }

  private static void assertSameRowsAsWithoutSkip(final Database database, final Random random) {
    for (final String type : TYPES)
      for (final String statement : statements(type))
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
    try (final ResultSet result = database.query("sql", statement)) {
      result.stream().forEach(
          row -> rows.add(row.isElement() ? row.getIdentity().orElseThrow().toString() : new TreeMap<>(row.toMap()).toString()));
    }
    return rows;
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
