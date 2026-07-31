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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5636 item 2: {@link Profiler} summed the per-database counters of the CURRENTLY OPEN
 * databases only, so closing or dropping one made the JVM-wide totals go BACKWARDS.
 * <p>
 * That is not just cosmetic. Those totals are exported to Micrometer, and a Prometheus counter that decreases is read
 * as a counter <i>reset</i>: every database close fabricated a rate() spike on the next scrape. Studio showed the same
 * thing more directly - the query and transaction counters visibly dropped when a database was dropped.
 * <p>
 * Assertions are all {@code >=} deltas rather than exact values: {@code Profiler.INSTANCE} is a JVM singleton, so
 * anything else running in the same fork contributes too. The regression is a DECREASE, which {@code >=} catches.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5636ProfilerMonotonicTest {

  private static final String DB_PATH  = "target/databases/issue5636-profiler-monotonic";
  private static final int    QUERIES  = 25;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void closingADatabaseDoesNotRewindTheJvmWideCounters() {
    final long queriesBefore = profilerCount("queries");
    final long writeTxBefore = profilerCount("writeTx");

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("k", 1).save());
      for (int i = 0; i < QUERIES; i++)
        db.query("sql", "select from Doc").close();

      final long queriesOpen = profilerCount("queries");
      final long writeTxOpen = profilerCount("writeTx");
      assertThat(queriesOpen).as("the open database's queries must be counted")
          .isGreaterThanOrEqualTo(queriesBefore + QUERIES);
      assertThat(writeTxOpen).as("the open database's write transactions must be counted")
          .isGreaterThan(writeTxBefore);

      db.close();

      assertThat(profilerCount("queries")).as("closing a database must not rewind the JVM-wide query counter")
          .isGreaterThanOrEqualTo(queriesOpen);
      assertThat(profilerCount("writeTx")).as("closing a database must not rewind the JVM-wide write-tx counter")
          .isGreaterThanOrEqualTo(writeTxOpen);
    }

    // Reopen and drop: the drop path unregisters too, and must not rewind either.
    final long queriesAfterClose = profilerCount("queries");
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      db.query("sql", "select from Doc").close();
      final long queriesReopened = profilerCount("queries");
      assertThat(queriesReopened).isGreaterThan(queriesAfterClose);

      db.drop();

      assertThat(profilerCount("queries")).as("dropping a database must not rewind the JVM-wide query counter")
          .isGreaterThanOrEqualTo(queriesReopened);
    }
  }

  /**
   * {@code statOf} degrades a missing or non-numeric key to 0 so a metrics scrape survives it. That is the right
   * trade at runtime, but it also means a typo in {@code DB_STAT_KEYS} - or a source key someone renames later -
   * silently produces a wrong total instead of failing. Nothing else would catch it: every other assertion here
   * reads through the same defaulting. So the key list is checked against a real stats map.
   */
  @Test
  void everyAccumulatedStatKeyExistsInARealStatsMap() throws Exception {
    // Read Profiler's OWN key list, not a copy of it: a second copy here would pass happily while the production
    // list carried the typo. This is the one assertion that has to reach into the class under test.
    final Field keysField = Profiler.class.getDeclaredField("DB_STAT_KEYS");
    keysField.setAccessible(true);
    final String[] dbStatKeys = (String[]) keysField.get(null);
    assertThat(dbStatKeys).isNotEmpty();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      try {
        final Map<String, Object> dbStats = db.getStats();
        for (final String key : dbStatKeys)
          assertThat(dbStats.get(key)).as("Profiler.DB_STAT_KEYS entry '%s' is not in DatabaseInternal.getStats()", key)
              .isInstanceOf(Number.class);

        final Map<String, Object> walStats = db.getTransactionManager().getStats();
        for (final String key : new String[] { "pagesWritten", "bytesWritten", "logFiles" })
          assertThat(walStats.get(key)).as("Profiler reads WAL stat '%s', which TransactionManager does not emit", key)
              .isInstanceOf(Number.class);
      } finally {
        db.drop();
      }
    }
  }

  /**
   * A database that is never registered - or is unregistered twice - must not double-count. Guards the
   * {@code if (!databases.contains(database)) return;} short-circuit that makes the fold idempotent.
   */
  @Test
  void unregisteringTwiceFoldsTheCountersOnlyOnce() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      db.getSchema().createDocumentType("Doc");
      for (int i = 0; i < QUERIES; i++)
        db.query("sql", "select from Doc").close();

      final long beforeClose = profilerCount("queries");
      db.close();
      final long afterFirst = profilerCount("queries");

      // The database is already gone from the registry; a second unregister must be a no-op, not a second fold.
      Profiler.INSTANCE.unregisterDatabase((DatabaseInternal) db);
      // Bounded rather than exact: a second fold would add at least this database's own QUERIES again, while
      // anything else sharing the reused surefire fork can only add a few. Exact equality would go intermittent
      // the day the suite runs test classes concurrently.
      assertThat(profilerCount("queries")).as("a repeated unregister must not fold the same counters in again")
          .isGreaterThanOrEqualTo(afterFirst).isLessThan(afterFirst + QUERIES);
      assertThat(afterFirst).isGreaterThanOrEqualTo(beforeClose);
    }

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      factory.open().drop();
    }
  }

  /**
   * {@code LocalDatabase.equals}/{@code hashCode} are derived from the database PATH, so an equals-based registry
   * cannot tell a closed instance from a freshly reopened one on the same path. The profiler tracks INSTANCES, and
   * with a path-keyed set a stale {@code unregisterDatabase(closedInstance)} would match the live reopened one:
   * it would fold the closed instance's counters a second time AND evict the live database from the registry, so
   * everything that database went on to do would stop being counted.
   */
  @Test
  void aStaleUnregisterOfAClosedInstanceDoesNotDisturbTheReopenedOne() {
    final Database closed;
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      closed = factory.create();
      closed.getSchema().createDocumentType("Doc");
      for (int i = 0; i < QUERIES; i++)
        closed.query("sql", "select from Doc").close();
      closed.close();
    }

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database reopened = factory.open();
      try {
        reopened.query("sql", "select from Doc").close();
        final long beforeStaleUnregister = profilerCount("queries");

        // The closed instance is equal-by-path to the reopened one, but is NOT the reopened one.
        Profiler.INSTANCE.unregisterDatabase((DatabaseInternal) closed);

        assertThat(profilerCount("queries"))
            .as("a stale unregister must not fold the closed instance's counters in a second time")
            .isLessThan(beforeStaleUnregister + QUERIES);

        // The live database must still be registered: if it had been evicted, its own counters would stop being
        // summed and this query would not move the total.
        final long beforeOneMoreQuery = profilerCount("queries");
        reopened.query("sql", "select from Doc").close();
        assertThat(profilerCount("queries")).as("the reopened database must still be registered")
            .isGreaterThan(beforeOneMoreQuery);
      } finally {
        reopened.drop();
      }
    }
  }

  private static long profilerCount(final String key) {
    final JSONObject json = Profiler.INSTANCE.toJSON();
    return json.getJSONObject(key).getLong("count", 0L);
  }
}
