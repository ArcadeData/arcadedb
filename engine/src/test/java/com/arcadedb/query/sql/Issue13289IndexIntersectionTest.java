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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An AND of conditions answered by two ordinary indexes loads only the records BOTH indexes name (issue #13289): the index the
 * planner fetches from is filtered by the record ids of the second index before a record is loaded. Before, every record of the
 * first index was loaded and the second condition was evaluated on it, 256 records for the 5 rows of the issue.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue13289IndexIntersectionTest extends TestHelper {
  private static final int  RECORDS = 4096;
  private static final long BASE    = 1704067200000L;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Tick");
    database.command("sql", "CREATE PROPERTY Tick.id STRING");
    database.command("sql", "CREATE PROPERTY Tick.updated_at DATETIME");
    database.command("sql", "CREATE PROPERTY Tick.grp INTEGER");
    database.command("sql", "CREATE PROPERTY Tick.n INTEGER");
    database.command("sql", "CREATE INDEX ON Tick (updated_at) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Tick (id) UNIQUE");
    database.command("sql", "CREATE INDEX ON Tick (grp) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Tick (n) NOTUNIQUE");

    // seeded as the issue does: 16 timestamps one second apart, 256 records each
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.command("sql", "INSERT INTO Tick SET id = ?, updated_at = date(?), ts = ?, grp = ?, n = ?", String.valueOf(i),
            BASE + (i % 16) * 1000L, BASE + (i % 16) * 1000L, i % 8, i);
    });
  }

  @Test
  void theKeysetPageOfTheIssueLoadsOnlyTheRecordsBothIndexesName() {
    final String sql = "SELECT id FROM Tick WHERE updated_at = date(" + BASE + ") AND id > '94' ORDER BY id LIMIT 20";
    final long before = readRecords();
    final List<String> ids;
    final String plan;
    try (final ResultSet rs = database.query("sql", sql)) {
      ids = rs.stream().map(r -> r.<String>getProperty("id")).toList();
      plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
    final long loaded = readRecords() - before;

    assertThat(ids).containsExactly("944", "96", "960", "976", "992");
    assertThat(plan).as(plan).contains("FETCH FROM INDEX Tick[updated_at]").contains("intersected with index Tick[id]");
    // 5 records answer, the equality alone matches 256 of them
    assertThat(loaded).as("records loaded, plan:%n%s", plan).isLessThan(20);
  }

  @Test
  void profileShowsTheIntersection() {
    try (final ResultSet rs = database.query("sql",
        "PROFILE SELECT id FROM Tick WHERE updated_at = date(" + BASE + ") AND id > '94' ORDER BY id LIMIT 20")) {
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as(plan).contains("intersected with index Tick[id]").contains("65 entries");
    }
  }

  @Test
  void withoutOrderByOrLimit() {
    assertSameAsScan("SELECT FROM Tick WHERE updated_at = date(" + BASE + ") AND id > '94'", Map.of(),
        d -> d.getLong("ts") == BASE && d.getString("id").compareTo("94") > 0, true);
    // the condition of the second index written first
    assertSameAsScan("SELECT FROM Tick WHERE id > '94' AND updated_at = date(" + BASE + ")", Map.of(),
        d -> d.getLong("ts") == BASE && d.getString("id").compareTo("94") > 0, true);
  }

  @Test
  void anAggregationReadsOnlyTheIntersection() {
    final long before = readRecords();
    try (final ResultSet rs = database.query("sql",
        "SELECT count(*) AS c FROM Tick WHERE updated_at = date(" + BASE + ") AND id > '94'")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(5L);
    }
    assertThat(readRecords() - before).isLessThan(20);
  }

  @Test
  void aPointLookupTooLargeToHoldIsFilteredToo() {
    // grp = 3 matches 512 records, past what a point lookup loads at once: the streaming path is filtered as well
    final long loaded = assertSameAsScan("SELECT FROM Tick WHERE grp = 3 AND n < 40", Map.of(),
        d -> d.getInteger("grp") == 3 && d.getInteger("n") < 40, true);
    // 5 rows, 512 records for the equality alone
    assertThat(loaded).as("records loaded").isLessThan(50);
  }

  @Test
  void aRangeIsFilteredByAnEquality() {
    // the planner fetches from n (range) or grp (equality): whichever it picks, the other one filters it
    assertSameAsScan("SELECT FROM Tick WHERE n BETWEEN 100 AND 400 AND updated_at = date(" + (BASE + 3000) + ") ORDER BY n", Map.of(),
        d -> d.getInteger("n") >= 100 && d.getInteger("n") <= 400 && d.getLong("ts") == BASE + 3000, true);
  }

  @Test
  void threeIndexesIntersect() {
    final long loaded = assertSameAsScan("SELECT FROM Tick WHERE grp = 1 AND n < 1000 AND id > '9' ORDER BY n", Map.of(),
        d -> d.getInteger("grp") == 1 && d.getInteger("n") < 1000 && d.getString("id").compareTo("9") > 0, true);
    // a handful of rows, 512 records for the equality alone and 125 for the equality and the first range
    assertThat(loaded).as("records loaded").isLessThan(50);
  }

  @Test
  void aSecondIndexLargerThanTheFirstIsGivenUp() {
    // id > '1' matches nearly every record: reading its index costs more than loading the 256 records of the equality
    final String sql = "SELECT FROM Tick WHERE updated_at = date(" + BASE + ") AND id > '1'";
    assertSameAsScan(sql, Map.of(), d -> d.getLong("ts") == BASE && d.getString("id").compareTo("1") > 0, true);
    try (final ResultSet rs = database.query("sql", "PROFILE " + sql)) {
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as(plan).contains("given up");
    }
  }

  @Test
  void aLimitThatStopsTheSearchEarlyKeepsThePlainSearch() {
    // no ORDER BY to re-sort and no aggregation: the first rows end the search, which reading a second index whole would not
    try (final ResultSet rs = database.query("sql", "SELECT FROM Tick WHERE grp = 1 AND n < 1000 LIMIT 3")) {
      final List<Result> rows = rs.stream().toList();
      assertThat(rows).hasSize(3);
      assertThat(rows).allMatch(r -> r.<Integer>getProperty("grp") == 1 && r.<Integer>getProperty("n") < 1000);
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).doesNotContain("intersected");
    }
  }

  @Test
  void parametersBoundToOtherValuesAreAnsweredByTheCachedPlan() {
    final String sql = "SELECT FROM Tick WHERE updated_at = :t AND id > :after ORDER BY id";
    for (final String after : List.of("94", "1", "999", "0", "95")) {
      for (final long t : new long[] { BASE, BASE + 5000, BASE + 15000 }) {
        final String a = after;
        final long tt = t;
        assertSameAsScan(sql, Map.of("t", date(t), "after", after), d -> d.getLong("ts") == tt && d.getString("id").compareTo(a) > 0,
            false);
      }
    }
  }

  @Test
  void uncommittedChangesAreSeen() {
    database.begin();
    try {
      // moved into the answer, moved out of it, created in it and deleted from it, all in this transaction
      database.command("sql", "UPDATE Tick SET id = '9500' WHERE id = '16'");
      database.command("sql", "UPDATE Tick SET id = '0944' WHERE id = '944'");
      database.newDocument("Tick").set("id", "99999").set("updated_at", date(BASE)).set("ts", BASE).set("grp", 0).set("n", -1).save();
      database.command("sql", "DELETE FROM Tick WHERE id = '960'");

      final String sql = "SELECT FROM Tick WHERE updated_at = date(" + BASE + ") AND id > '94' ORDER BY id";
      try (final ResultSet rs = database.query("sql", sql)) {
        assertThat(rs.stream().map(r -> r.<String>getProperty("id")).toList()).containsExactly("9500", "96", "976", "992", "99999");
      }
    } finally {
      database.rollback();
    }
  }

  @Test
  void updateAndDeleteReachTheSameRecords() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql",
          "UPDATE Tick SET grp = -1 RETURN AFTER WHERE updated_at = date(" + BASE + ") AND id > '94'")) {
        assertThat(rs.stream().map(r -> r.<String>getProperty("id")).toList()).containsExactlyInAnyOrder("944", "96", "960", "976",
            "992");
      }
    });
    assertThat(expected(d -> d.getInteger("grp") == -1)).containsExactlyInAnyOrder("944", "96", "960", "976", "992");

    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM Tick WHERE updated_at = date(" + BASE + ") AND id > '94'")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(5L);
      }
    });
    assertThat(expected(d -> d.getInteger("grp") == -1)).isEmpty();
    assertThat(expected(d -> true)).hasSize(RECORDS - 5);
  }

  @Test
  void aStatementRunAgainReadsTheIndexesAgain() {
    // a script runs the same statement on every iteration, each one after a commit that changed what the indexes name
    final List<Long> counts = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      try (final ResultSet rs = database.query("sql",
          "SELECT count(*) AS c FROM Tick WHERE updated_at = date(" + BASE + ") AND id > '94'")) {
        counts.add(rs.next().<Long>getProperty("c"));
      }
      final int n = i;
      database.transaction(() -> database.command("sql", "UPDATE Tick SET id = '99" + n + "x' WHERE id = '" + (16 * (n + 1)) + "'"));
    }
    assertThat(counts).containsExactly(5L, 6L, 7L);
  }

  @Test
  void aRecordACommitMakesMatchAfterTheSecondIndexWasReadIsNotDropped() {
    // grp = 3 holds 512 entries, read and loaded as the rows are pulled; n < 40 names 5 of them when its index is read, on the first
    // row. A commit then gives n = 1 to a record of grp 3 read near the end: the set does not name it, but it matches now, as it
    // does for the plain search, which loads it after the commit and keeps it
    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM Tick WHERE grp = 3 AND n < 40")) {
      assertThat(rs.hasNext()).isTrue();
      ids.add(rs.next().getProperty("id"));
      database.transaction(() -> database.command("sql", "UPDATE Tick SET n = 1 WHERE id = '4091'"));
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("intersected with index Tick[n]");
    }
    assertThat(ids).containsExactlyInAnyOrder("3", "11", "19", "27", "35", "4091");
  }

  @Test
  void aSubTypeIsReadThroughTheInheritedIndexes() {
    database.command("sql", "CREATE DOCUMENT TYPE SubTick EXTENDS Tick");
    database.transaction(() -> {
      for (int i = 0; i < 64; i++)
        database.newDocument("SubTick").set("id", "s" + i).set("updated_at", date(BASE)).set("ts", BASE).set("grp", 0).set("n", 10_000 + i).save();
    });
    assertSameAsScan("SELECT FROM Tick WHERE updated_at = date(" + BASE + ") AND n >= 4090 ORDER BY n", Map.of(),
        d -> d.getLong("ts") == BASE && d.getInteger("n") >= 4090, true);
  }

  /**
   * Runs {@code sql} and checks it returns the rows a scan of the type filtered by {@code filter} returns, in the order of its ORDER
   * BY when it has one.
   *
   * @return the records the query loaded
   */
  private long assertSameAsScan(final String sql, final Map<String, Object> params, final Predicate<Document> filter,
      final boolean expectIntersection) {
    final long before = readRecords();
    final List<Result> rows;
    final String plan;
    try (final ResultSet rs = database.query("sql", sql, params)) {
      rows = rs.stream().toList();
      plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
    final long loaded = readRecords() - before;

    final List<String> ids = rows.stream().map(r -> r.<String>getProperty("id")).toList();
    assertThat(ids).as("%s %s%n%s", sql, params, plan).containsExactlyInAnyOrderElementsOf(expected(filter));
    if (sql.contains("ORDER BY")) {
      final String key = sql.substring(sql.indexOf("ORDER BY") + 9).trim().split(" ")[0];
      final List<Result> sorted = new ArrayList<>(rows);
      if (key.equals("n"))
        sorted.sort(Comparator.comparingInt(r -> r.<Integer>getProperty("n")));
      else
        sorted.sort(Comparator.comparing(r -> r.<String>getProperty("id")));
      assertThat(ids).as("order of %s", sql).isEqualTo(sorted.stream().map(r -> r.<String>getProperty("id")).toList());
    }
    if (expectIntersection)
      assertThat(plan).as(plan).contains("intersected with index");
    return loaded;
  }

  /** The answer of a scan of the type, filtered here: what any plan must return. */
  private List<String> expected(final Predicate<Document> filter) {
    final List<String> ids = new ArrayList<>();
    final Iterator<?> it = database.iterateType("Tick", true);
    while (it.hasNext()) {
      final Document document = ((Record) it.next()).asDocument();
      if (filter.test(document))
        ids.add(document.getString("id"));
    }
    return ids;
  }

  /** The value `date(millis)` gives in SQL, as the records were seeded with. */
  private Object date(final long millis) {
    try (final ResultSet rs = database.query("sql", "SELECT date(?) AS d", millis)) {
      return rs.next().getProperty("d");
    }
  }

  private long readRecords() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }
}
