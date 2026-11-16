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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A subquery's {@code key = $parent.$current.<property>} is served by the index on key (issue #12286). $parent is set before the
 * subquery is planned and does not change during one execution of its plan, so the key is resolved when the fetch starts, as an
 * input parameter is. It used to be a full scan of the type for every outer row.
 */
class Issue12286ParentKeySubqueryIndexTest extends TestHelper {
  private static final int    RECORDS = 500;
  private static final String INNER   = "SELECT name FROM T WHERE id = $parent.$current.id + 1";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 4");
    database.command("sql", "CREATE PROPERTY T.id INTEGER");
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE S EXTENDS T");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(i % 5 == 4 ? "S" : "T").set("id", i).set("name", "n" + i).save();
      // an outer row with no id: its key is null and finds nothing
      database.newDocument("T").set("name", "no-id").save();
    });
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE id = 11"));
  }

  @Test
  void theSubqueryIsPlannedOnTheIndex() {
    final Map<String, Object> outer = new HashMap<>();
    outer.put("id", 41);
    assertThat(planAndRun(INNER, outer)).as("planned under a parent").startsWith("FETCH FROM INDEX T[id]").endsWith("|n42");

    outer.put("id", 499);
    assertThat(planAndRun(INNER, outer)).as("no next record").endsWith("|");
    outer.put("id", null);
    assertThat(planAndRun(INNER, outer)).as("a null key finds nothing").endsWith("|");
  }

  @Test
  void aProjectionSubqueryReadsTheRecordOfEachOuterRow() {
    final String sql = "SELECT id, (" + INNER + ") AS nxt FROM T WHERE id < 40 OR name = 'no-id'";
    assertThat(rows(sql)).containsExactlyInAnyOrderElementsOf(expected(40, true));
  }

  @Test
  void aLetSubqueryReadsTheRecordOfEachOuterRow() {
    // the outer filter is kept off the index: a LET after an index fetch reads the index entry as $current (issue #12301)
    final String sql = "SELECT id, $n AS nxt FROM T LET $n = (" + INNER + ") WHERE id + 0 < 40 OR name = 'no-id'";
    assertThat(rows(sql)).containsExactlyInAnyOrderElementsOf(expected(40, true));
  }

  @Test
  void uncommittedChangesAreSeen() {
    database.begin();
    try {
      database.command("sql", "UPDATE T SET name = 'renamed' WHERE id = 6");
      database.newDocument("T").set("id", 11).set("name", "fresh").save();
      final List<String> rows = rows("SELECT id, (" + INNER + ") AS nxt FROM T WHERE id IN [5, 10]");
      assertThat(rows).containsExactlyInAnyOrder("5->renamed", "10->fresh");
    } finally {
      database.rollback();
    }
  }

  /** Plans {@code sql} the way a subquery is planned, under a context whose parent holds the outer row, and runs it. */
  private String planAndRun(final String sql, final Map<String, Object> outerRow) {
    final BasicCommandContext parent = new BasicCommandContext();
    parent.setDatabase(database);
    parent.setVariable("current", new ResultInternal(outerRow));
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setParent(parent);

    final Statement statement = ((DatabaseInternal) database).getStatementCache().get(sql);
    final InternalExecutionPlan plan = statement.createExecutionPlan(context);
    final String firstStep = plan.prettyPrint(0, 2).lines().filter(l -> l.contains("FETCH FROM")).findFirst().orElse("").trim()
        .replaceFirst("^\\+ ", "");
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = new LocalResultSet(plan)) {
      rs.forEachRemaining(r -> names.add(r.getProperty("name")));
    }
    return firstStep + "|" + String.join(",", names);
  }

  private List<String> rows(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.stream().map(r -> r.getProperty("id") + "->" + nameOf(r.getProperty("nxt"))).toList();
    }
  }

  private static String nameOf(final Object nxt) {
    if (!(nxt instanceof List<?> list) || list.isEmpty())
      return "";
    return ((Result) list.getFirst()).getProperty("name");
  }

  private static List<String> expected(final int below, final boolean withNoId) {
    final List<String> rows = new ArrayList<>();
    for (int i = 0; i < below; i++)
      if (i != 11)
        rows.add(i + "->" + (i + 1 == 11 ? "" : "n" + (i + 1)));
    if (withNoId)
      rows.add("null->");
    return rows;
  }
}
