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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12518: a composite index whose trailing column is {@code attrs BY KEY} holds no entry for a record whose map is empty
 * (or null, or absent), so a query that filters only on the leading scalar column and was answered by a prefix scan of that
 * index lost those records, and {@code count(*)} undercounted them. The index must never change the answer: every statement
 * that reads, counts, orders, groups, updates or deletes through the leading column answers the same on a type holding the index
 * as on an identical type without it. The fix is the guard of issue #12444 (a column left unbound that expands a collection
 * keeps the prefix scan off); this is the reporter's scenario and the grid of statements that go through the same planner step.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12518PrefixScanOnMapKeyCompositeIndexTest extends TestHelper {
  private static final String PLAIN   = "MapPrefixPlain";
  private static final String INDEXED = "MapPrefixExample";

  @Override
  protected void beginTest() {
    for (final String type : new String[] { PLAIN, INDEXED }) {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".scope STRING");
        database.command("sql", "CREATE PROPERTY " + type + ".attrs MAP");
        database.command("sql", "INSERT INTO " + type + " CONTENT {\"scope\":\"p\",\"attrs\":{}}");
        // two keys: the index holds this record twice, so an ordered walk of it would repeat the record
        database.command("sql", "INSERT INTO " + type + " CONTENT {\"scope\":\"p\",\"attrs\":{\"x\":1,\"z\":3}}");
        // a record with no map at all, and one with a null map
        database.command("sql", "INSERT INTO " + type + " CONTENT {\"scope\":\"p\"}");
        database.command("sql", "INSERT INTO " + type + " CONTENT {\"scope\":\"p\",\"attrs\":null}");
        database.command("sql", "INSERT INTO " + type + " CONTENT {\"scope\":\"q\",\"attrs\":{\"y\":2}}");
        database.command("sql", "INSERT INTO " + type + " CONTENT {\"scope\":\"q\",\"attrs\":{}}");
      });
    }
  }

  @Test
  void reporterScenarioAnswersTheSameBeforeAndAfterTheIndex() {
    final long before = count("scope = 'p'");
    assertThat(before).isEqualTo(4L);
    assertThat(selectAttrs("scope = 'p'")).hasSize(4);

    database.command("sql", "CREATE INDEX ON " + INDEXED + " (scope, attrs BY KEY) NOTUNIQUE");

    assertThat(count("scope = 'p'")).as("count(*) after the index").isEqualTo(before);
    assertThat(selectAttrs("scope = 'p'")).as("rows after the index").hasSize(4);
    assertThat(countRecords("scope = 'p'")).as("count() after the index").isEqualTo(before);
  }

  @Test
  void everyStatementThroughTheLeadingColumnAnswersLikeAnUnindexedType() {
    database.command("sql", "CREATE INDEX ON " + INDEXED + " (scope, attrs BY KEY) NOTUNIQUE");

    for (final String statement : new String[] {//
        "SELECT scope, attrs FROM %s WHERE scope = 'p'",//
        "SELECT count(*) AS c FROM %s WHERE scope = 'p'",//
        "SELECT count(@rid) AS c FROM %s WHERE scope = 'p'",//
        "SELECT count(*) AS c FROM %s",//
        "SELECT scope, attrs FROM %s WHERE scope IN ['p', 'q']",//
        "SELECT scope, attrs FROM %s WHERE scope > 'a'",//
        "SELECT scope, attrs FROM %s WHERE scope >= 'p' AND scope < 'q'",//
        "SELECT scope, attrs FROM %s WHERE scope LIKE 'p%%'",//
        "SELECT scope, attrs FROM %s WHERE scope = 'p' AND attrs IS NULL",//
        "SELECT scope, attrs FROM %s ORDER BY scope",//
        "SELECT scope, attrs FROM %s WHERE scope = 'p' ORDER BY scope",//
        "SELECT scope, attrs FROM %s ORDER BY scope DESC",//
        "SELECT scope, count(*) AS c FROM %s GROUP BY scope",//
        "SELECT DISTINCT scope FROM %s",//
        "SELECT scope, attrs FROM %s WHERE scope = 'q' AND attrs CONTAINSKEY 'y'",//
        "SELECT scope, attrs FROM %s WHERE scope = 'q' AND attrs CONTAINSKEY 'nope'" }) {
      final List<String> expected = rows(String.format(statement, PLAIN));
      assertThat(rows(String.format(statement, INDEXED))).as(statement).containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  @Test
  void theIndexStillAnswersTheKeyedQuery() {
    database.command("sql", "CREATE INDEX ON " + INDEXED + " (scope, attrs BY KEY) NOTUNIQUE");

    assertThat(count("scope = 'p' AND attrs CONTAINSKEY 'x'")).isEqualTo(1L);
    assertThat(explain("scope = 'p' AND attrs CONTAINSKEY 'x'")).contains("FETCH FROM INDEX");
    assertThat(count("scope = 'q' AND attrs CONTAINSKEY 'y'")).isEqualTo(1L);
    assertThat(count("scope = 'q' AND attrs CONTAINSKEY 'x'")).isEqualTo(0L);
    // the prefix-only query is not answered from an index that cannot hold every record of it
    assertThat(explain("scope = 'p'")).doesNotContain("FETCH FROM INDEX");
  }

  @Test
  void updateAndDeleteThroughTheLeadingColumnReachEveryRecord() {
    database.command("sql", "CREATE INDEX ON " + INDEXED + " (scope, attrs BY KEY) NOTUNIQUE");

    database.transaction(() -> database.command("sql", "UPDATE " + INDEXED + " SET flag = true WHERE scope = 'p'"));
    assertThat(countRecords("scope = 'p' AND flag = true")).isEqualTo(4L);

    database.transaction(() -> database.command("sql", "DELETE FROM " + INDEXED + " WHERE scope = 'p'"));
    assertThat(countRecords("scope = 'p'")).isEqualTo(0L);
    assertThat(countRecords("scope = 'q'")).isEqualTo(2L);
  }

  @Test
  void mapKeyColumnFirstIsUnaffected() {
    // the report's workaround: the same two columns in the other order
    database.command("sql", "CREATE INDEX ON " + INDEXED + " (attrs BY KEY, scope) NOTUNIQUE");
    assertThat(count("scope = 'p'")).isEqualTo(4L);
    assertThat(selectAttrs("scope = 'p'")).hasSize(4);
  }

  private long count(final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM " + INDEXED + " WHERE " + where)) {
      return ((Number) rs.next().getProperty("total")).longValue();
    }
  }

  private long countRecords(final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS total FROM " + INDEXED + " WHERE " + where)) {
      return ((Number) rs.next().getProperty("total")).longValue();
    }
  }

  private List<Object> selectAttrs(final String where) {
    final List<Object> attrs = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT attrs FROM " + INDEXED + " WHERE " + where)) {
      while (rs.hasNext())
        attrs.add(rs.next().getProperty("attrs"));
    }
    return attrs;
  }

  private List<String> rows(final String sql) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.toJSON().toString());
      }
    }
    return rows;
  }

  private String explain(final String where) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT FROM " + INDEXED + " WHERE " + where)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
