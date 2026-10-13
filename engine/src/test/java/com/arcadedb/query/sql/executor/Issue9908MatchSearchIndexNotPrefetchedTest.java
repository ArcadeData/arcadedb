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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.WhereClause;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9908: {@code SEARCH_INDEX} cannot estimate how many records it returns and answers -1. {@code WhereClause.estimate()}
 * took that as a count, so a MATCH node filtered by it was estimated at -1 rows, below the prefetch threshold, and the whole
 * full-text result was drained into a list before the pattern started, whatever the LIMIT. An unknown estimate is now never
 * prefetched. The node is still preferred as a root over a large scan, but no longer over a RID or a set known to be small.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9908MatchSearchIndexNotPrefetchedTest extends TestHelper {
  private static final int NOTES     = 400;
  private static final int HOSPITALS = 4;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Note");
      database.command("sql", "CREATE PROPERTY Note.a STRING");
      database.command("sql", "CREATE PROPERTY Note.b STRING");
      database.command("sql", "CREATE INDEX ON Note (a, b) FULL_TEXT");
      database.command("sql", "CREATE PROPERTY Note.n INTEGER");
      database.command("sql", "CREATE INDEX ON Note (n) NOTUNIQUE");
      database.command("sql", "CREATE PROPERTY Note.parity INTEGER");
      database.command("sql", "CREATE INDEX ON Note (parity) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE Patient");
      database.command("sql", "CREATE VERTEX TYPE Hospital");
      database.command("sql", "CREATE EDGE TYPE R");
      database.command("sql", "CREATE EDGE TYPE H");

      for (int i = 0; i < HOSPITALS; i++)
        database.command("sql", "CREATE VERTEX Hospital SET n = " + i);
      for (int i = 0; i < NOTES; i++) {
        database.command("sql",
            "CREATE VERTEX Note SET n = " + i + ", parity = " + (i % 2) + ", a = '" + (i % 2 == 0 ? "fever and cough" : "rash") + "', b = 'note " + i + "'");
        database.command("sql", "CREATE VERTEX Patient SET n = " + i);
        database.command("sql", "CREATE EDGE R FROM (SELECT FROM Note WHERE n = " + i + ") TO (SELECT FROM Patient WHERE n = " + i + ")");
        database.command("sql",
            "CREATE EDGE H FROM (SELECT FROM Note WHERE n = " + i + ") TO (SELECT FROM Hospital WHERE n = " + (i % HOSPITALS) + ")");
      }
    });
  }

  @Test
  void edgePatternRootedAtSearchIndexIsStreamed() {
    final String query = "MATCH {type:Note, as:n, where:(SEARCH_INDEX('Note[a,b]', 'fever') = true)}.out('R'){as:p} "
        + "RETURN n.@rid AS nr LIMIT 5";
    final String plan = explain(query);
    assertThat(plan).doesNotContain("PREFETCH");
    assertThat(plan).contains("+ MATCH      ---->\n  {n}.out('R'){p}");
    assertThat(count(query)).isEqualTo(5);
  }

  @Test
  void singleNodeSearchIndexIsStreamed() {
    final String query = "MATCH {type:Note, as:n, where:(SEARCH_INDEX('Note[a,b]', 'fever') = true)} RETURN n.@rid AS nr LIMIT 5";
    assertThat(explain(query)).doesNotContain("PREFETCH");
    assertThat(count(query)).isEqualTo(5);
  }

  @Test
  void resultsAreUnchanged() {
    final Set<RID> expected = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS r FROM Note WHERE SEARCH_INDEX('Note[a,b]', 'fever') = true")) {
      while (rs.hasNext())
        expected.add(rs.next().getProperty("r"));
    }
    assertThat(expected).hasSize(NOTES / 2);

    final Set<RID> actual = new HashSet<>();
    try (final ResultSet rs = database.query("sql",
        "MATCH {type:Note, as:n, where:(SEARCH_INDEX('Note[a,b]', 'fever') = true)}.out('R'){as:p} RETURN n.@rid AS nr")) {
      while (rs.hasNext())
        assertThat(actual.add(rs.next().getProperty("nr"))).isTrue();
    }
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void searchIndexIsPreferredOverALargeScan() {
    // Patient HAS 400 RECORDS AND NO FILTER: THE FULL-TEXT NODE IS STILL THE BETTER ENTRY POINT
    final String query = "MATCH {type:Patient, as:p}<-R-{type:Note, as:n, where:(SEARCH_INDEX('Note[a,b]', 'fever') = true)} "
        + "RETURN count(*) AS c";
    final String plan = explain(query);
    assertThat(plan).doesNotContain("PREFETCH");
    assertThat(plan).contains("+ MATCH      <----\n  {p}.in('R'){n}");
    assertThat(count(query, "c")).isEqualTo(NOTES / 2);
  }

  @Test
  void aKnownSmallSetIsPreferredOverSearchIndex() {
    // Hospital HAS 4 RECORDS: IT IS PREFETCHED AND IS THE ROOT, THE FULL-TEXT NODE IS REACHED BY TRAVERSAL AND NOT PREFETCHED
    final String query = "MATCH {type:Note, as:n, where:(SEARCH_INDEX('Note[a,b]', 'fever') = true)}.out('H'){type:Hospital, as:h} "
        + "RETURN count(*) AS c";
    final String plan = explain(query);
    assertThat(plan).contains("PREFETCH h");
    assertThat(plan).doesNotContain("PREFETCH n");
    assertThat(plan).contains("+ MATCH      <----\n  {n}.out('H'){h}");
    assertThat(count(query, "c")).isEqualTo(NOTES / 2);
  }

  @Test
  void estimateTreatsAnIndexedFunctionWithoutABoundAsUnknown() {
    final String fever = "SEARCH_INDEX('Note[a,b]', 'fever') = true";
    assertThat(estimate(fever)).isEqualTo(WhereClause.UNKNOWN_ESTIMATE);
    // A KNOWN BOUND IN THE SAME AND-BLOCK BOUNDS IT: THE INDEX ON n USED TO BE IGNORED NEXT TO AN INDEXED FUNCTION
    assertThat(estimate(fever + " AND n = 4")).isEqualTo(1L);
    // ONE UNKNOWN BRANCH OF AN OR MAKES THE UNION UNKNOWN...
    assertThat(estimate(fever + " OR n = 4")).isEqualTo(WhereClause.UNKNOWN_ESTIMATE);
    // ...UNLESS ANOTHER BRANCH NEEDS A SCAN, WHICH ANSWERS THE TYPE COUNT (HALVED, AS FOR EVERY UNBOUNDED FILTER)
    assertThat(estimate(fever + " OR b = 'x'")).isEqualTo(NOTES / 2);
    assertThat(estimate("b = 'x' OR " + fever)).isEqualTo(NOTES / 2);
  }

  @Test
  void estimateCountsAnIndexedEqualityUpToTheThreshold() {
    // THE COUNT OF A FULL KEY WAS COMPUTED AND DROPPED, SO AN INDEXED EQUALITY NEVER BOUNDED THE ESTIMATE
    assertThat(estimate("n = 4")).isEqualTo(1L);
    assertThat(estimate("n = 4 OR n = 5")).isEqualTo(2L);
    // A KEY WITH 200 ENTRIES IS COUNTED ONLY UP TO THE THRESHOLD: "NOT SMALL" IS ALL THE PLANNER NEEDS, AND IT ANSWERS THE SCAN BOUND
    assertThat(estimate("parity = 0")).isEqualTo(NOTES / 2);
    // A VALUE THAT DOES NOT CONVERT TO THE INTEGER KEY GIVES NO BOUND, RATHER THAN A LOOKUP OF THE NULL KEY
    assertThat(estimate("n = 'not a number'")).isEqualTo(NOTES / 2);
    assertThat(explain("MATCH {type:Note, as:n, where:(n = 4)} RETURN n.n AS n")).contains("PREFETCH n");
  }

  @Test
  void aKnownBoundNextToSearchIndexIsPrefetched() {
    final String query = "MATCH {type:Note, as:n, where:(SEARCH_INDEX('Note[a,b]', 'fever') = true AND n = 4)}.out('R'){as:p} "
        + "RETURN p.n AS pn";
    assertThat(explain(query)).contains("PREFETCH n");
    assertThat(count(query)).isEqualTo(1);
  }

  @Test
  void bucketRootedSearchIndexIsStreamed() {
    final String bucket = database.getSchema().getType("Note").getBuckets(false).getFirst().getName();
    final String where = "SEARCH_INDEX('Note[a,b]', 'fever') = true";
    final String query = "MATCH {bucket:" + bucket + ", as:n, where:(" + where + ")} RETURN count(*) AS c";
    assertThat(explain(query)).doesNotContain("PREFETCH");
    assertThat(count(query, "c")).isEqualTo(count("SELECT count(*) AS c FROM bucket:" + bucket + " WHERE " + where, "c"));
  }

  private long estimate(final String where) {
    final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache()
        .get("SELECT FROM Note WHERE " + where);
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return statement.getWhereClause().estimate(database.getSchema().getType("Note"), 100, context);
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private long count(final String query) {
    long n = 0;
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
    }
    return n;
  }

  private long count(final String query, final String column) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.next().<Number>getProperty(column).longValue();
    }
  }
}
