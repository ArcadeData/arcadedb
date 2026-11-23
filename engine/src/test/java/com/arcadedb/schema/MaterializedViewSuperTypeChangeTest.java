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
package com.arcadedb.schema;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12031: a REFRESH INCREMENTAL view over a type whose set of subtypes changes with {@code ALTER TYPE ... SUPERTYPE +/-}
 * answers like its query afterwards. The rows of a detached subtype leave the view, the rows of an attached type join it, and
 * later writes follow the new hierarchy.
 */
class MaterializedViewSuperTypeChangeTest extends TestHelper {

  /** The steps of the issue, for a per-record view ({@code SELECT k FROM P}) and a whole-view one (ORDER BY). */
  @ParameterizedTest
  @ValueSource(strings = { "SELECT k FROM P", "SELECT k FROM P ORDER BY k" })
  void viewFollowsSubtypesDetachedAndAttached(final String query) {
    createHierarchy();
    database.command("sql", "CREATE MATERIALIZED VIEW M AS " + query + " REFRESH INCREMENTAL");
    assertViewMatchesQuery("M", "P", List.of(1, 2));

    database.command("sql", "ALTER TYPE C SUPERTYPE -P");
    assertViewMatchesQuery("M", "P", List.of(1));

    database.command("sql", "ALTER TYPE Q SUPERTYPE +P");
    assertViewMatchesQuery("M", "P", List.of(1, 3));

    write("INSERT INTO Q SET k = 30");
    assertViewMatchesQuery("M", "P", List.of(1, 3, 30));

    // the detached type no longer reaches the view
    write("UPDATE C SET k = 20");
    write("INSERT INTO C SET k = 21");
    assertViewMatchesQuery("M", "P", List.of(1, 3, 30));
  }

  /** A view over a type ABOVE the one whose subtype changes covers the change too: {@code SELECT FROM G} reaches C through P. */
  @Test
  void viewOverAnAncestorFollowsTheChange() {
    database.command("sql", "CREATE DOCUMENT TYPE G");
    createHierarchy();
    database.command("sql", "ALTER TYPE P SUPERTYPE +G");
    write("INSERT INTO G SET k = 0");
    database.command("sql", "CREATE MATERIALIZED VIEW MG AS SELECT k FROM G REFRESH INCREMENTAL");
    assertViewMatchesQuery("MG", "G", List.of(0, 1, 2));

    database.command("sql", "ALTER TYPE C SUPERTYPE -P");
    assertViewMatchesQuery("MG", "G", List.of(0, 1));

    database.command("sql", "ALTER TYPE Q SUPERTYPE +P");
    assertViewMatchesQuery("MG", "G", List.of(0, 1, 3));
  }

  /** The Java API changes the hierarchy without the SQL statement, also inside a caller's transaction: the view follows it. */
  @Test
  void viewFollowsTheSchemaApi() {
    createHierarchy();
    database.command("sql", "CREATE MATERIALIZED VIEW M AS SELECT k FROM P REFRESH INCREMENTAL");

    database.getSchema().getType("C").removeSuperType("P");
    assertViewMatchesQuery("M", "P", List.of(1));

    database.getSchema().getType("Q").addSuperType("P");
    assertViewMatchesQuery("M", "P", List.of(1, 3));

    database.getSchema().getType("C").setSuperTypes(List.of(database.getSchema().getType("P")));
    assertViewMatchesQuery("M", "P", List.of(1, 2, 3));

    database.transaction(() -> database.getSchema().getType("Q").removeSuperType("P"));
    assertViewMatchesQuery("M", "P", List.of(1, 2));
  }

  /** A view that does not read the changed type, nor one above it, is not rewritten by the change. */
  @Test
  void unrelatedViewIsNotRefreshed() {
    createHierarchy();
    database.command("sql", "CREATE MATERIALIZED VIEW MQ AS SELECT k FROM Q REFRESH INCREMENTAL");
    final long refreshes = database.getSchema().getMaterializedView("MQ").getRefreshCount();

    database.command("sql", "ALTER TYPE C SUPERTYPE -P");
    database.command("sql", "ALTER TYPE Q SUPERTYPE +P");

    assertThat(database.getSchema().getMaterializedView("MQ").getRefreshCount()).isEqualTo(refreshes);
    assertViewMatchesQuery("MQ", "Q", List.of(3));
  }

  /**
   * The pass the change owes runs inside the schema frame, before the frame's own flush of the plan caches: a plan of the view's
   * query that an earlier refresh cached would still read the buckets of the former hierarchy, and the view would keep the rows of
   * a detached type (or miss the ones of an attached type) while reporting VALID. A cap of 1 sends the per-record view through its
   * full pass, the one that reads the source type.
   */
  @ParameterizedTest
  @CsvSource({ "SELECT k FROM P,10000", "SELECT k FROM P,1", "SELECT k FROM P ORDER BY k,10000" })
  void aCachedPlanOfTheViewQueryDoesNotHideTheChange(final String query, final int cap) {
    createHierarchy();
    write("INSERT INTO C SET k = 22");
    database.command("sql", "CREATE MATERIALIZED VIEW M AS " + query + " REFRESH INCREMENTAL");

    withMaxDelta(cap, () -> {
      // the refresh of the commit below and the manual one run the view's query, so a plan of it is cached; a plain read of the same
      // text caches it for a statement of the user too
      write("INSERT INTO P SET k = 4");
      database.command("sql", "REFRESH MATERIALIZED VIEW M");
      sortedKeys(query);
      assertViewMatchesQuery("M", "P", List.of(1, 2, 4, 22));

      database.command("sql", "ALTER TYPE C SUPERTYPE -P");
      assertViewMatchesQuery("M", "P", List.of(1, 4));

      write("INSERT INTO P SET k = 5");
      database.command("sql", "REFRESH MATERIALIZED VIEW M");
      sortedKeys(query);
      database.command("sql", "ALTER TYPE Q SUPERTYPE +P");
      assertViewMatchesQuery("M", "P", List.of(1, 3, 4, 5));
    });
  }

  /** The records of the types below the one that changes move with it: the whole subtree leaves, and joins, the view. */
  @ParameterizedTest
  @ValueSource(strings = { "SELECT k FROM P", "SELECT k FROM P ORDER BY k" })
  void theWholeSubtreeBelowTheChangedTypeFollows(final String query) {
    createHierarchy();
    database.command("sql", "CREATE DOCUMENT TYPE D EXTENDS C");
    write("INSERT INTO D SET k = 4");
    database.command("sql", "CREATE MATERIALIZED VIEW M AS " + query + " REFRESH INCREMENTAL");
    assertViewMatchesQuery("M", "P", List.of(1, 2, 4));

    database.command("sql", "ALTER TYPE C SUPERTYPE -P");
    assertViewMatchesQuery("M", "P", List.of(1));

    database.command("sql", "ALTER TYPE C SUPERTYPE +P");
    assertViewMatchesQuery("M", "P", List.of(1, 2, 4));
  }

  /** A new type that extends the type a view reads holds no record, so nothing in any view changes: no pass is owed. */
  @ParameterizedTest
  @ValueSource(strings = { "SELECT k FROM P", "SELECT k FROM P ORDER BY k" })
  void creatingASubtypeDoesNotRefreshTheViews(final String query) {
    createHierarchy();
    database.command("sql", "CREATE MATERIALIZED VIEW M AS " + query + " REFRESH INCREMENTAL");
    final long refreshes = database.getSchema().getMaterializedView("M").getRefreshCount();

    database.command("sql", "CREATE DOCUMENT TYPE N EXTENDS P");

    assertThat(database.getSchema().getMaterializedView("M").getRefreshCount()).isEqualTo(refreshes);
    write("INSERT INTO N SET k = 7");
    assertViewMatchesQuery("M", "P", List.of(1, 2, 7));
  }

  /** Linking or unlinking a type that holds no record, below the types it covers, changes the rows of no view. */
  @ParameterizedTest
  @ValueSource(strings = { "SELECT k FROM P", "SELECT k FROM P ORDER BY k" })
  void aSubtreeWithoutRecordsDoesNotRefreshTheViews(final String query) {
    createHierarchy();
    database.command("sql", "CREATE DOCUMENT TYPE E");
    database.command("sql", "CREATE MATERIALIZED VIEW M AS " + query + " REFRESH INCREMENTAL");
    final long refreshes = database.getSchema().getMaterializedView("M").getRefreshCount();

    database.command("sql", "ALTER TYPE E SUPERTYPE +P");
    database.command("sql", "ALTER TYPE E SUPERTYPE -P");

    assertThat(database.getSchema().getMaterializedView("M").getRefreshCount()).isEqualTo(refreshes);
    assertViewMatchesQuery("M", "P", List.of(1, 2));
  }

  /**
   * A per-record view is brought in line for the records of the changed subtree only, as for any other change of its source: a
   * type of a few records detached from a source of many does not rewrite the rows of the others.
   */
  @Test
  void perRecordViewIsNotRewrittenForTheRecordsOutsideTheSubtree() {
    createHierarchy();
    database.transaction(() -> {
      for (int i = 100; i < 400; i++)
        database.command("sql", "INSERT INTO P SET k = " + i);
    });
    database.command("sql", "CREATE MATERIALIZED VIEW M AS SELECT k FROM P REFRESH INCREMENTAL");
    assertThat(((MaterializedViewImpl) database.getSchema().getMaterializedView("M")).isPerRecord()).isTrue();

    long before = writes();
    database.command("sql", "ALTER TYPE C SUPERTYPE -P");
    assertThat(writes() - before).as("the one row of the detached record goes, no other row is written").isLessThanOrEqualTo(4);
    assertViewMatchesQuery("M", "P", expectedKeys(1, 100, 400));

    before = writes();
    database.command("sql", "ALTER TYPE Q SUPERTYPE +P");
    assertThat(writes() - before).as("the one row of the attached record is created, no other row is written").isLessThanOrEqualTo(4);
    final List<Integer> expected = expectedKeys(1, 100, 400);
    expected.add(3);
    expected.sort(null);
    assertViewMatchesQuery("M", "P", expected);
  }

  /** A subtree larger than the cap a per-record pass re-evaluates one by one is handed to a full pass, as the commit of one is. */
  @Test
  void aSubtreeBeyondTheDeltaCapIsRewrittenByAFullPass() {
    createHierarchy();
    write("INSERT INTO C SET k = 22");
    write("INSERT INTO Q SET k = 33");
    database.command("sql", "CREATE MATERIALIZED VIEW M AS SELECT k FROM P REFRESH INCREMENTAL");

    withMaxDelta(1, () -> {
      database.command("sql", "ALTER TYPE C SUPERTYPE -P");
      assertViewMatchesQuery("M", "P", List.of(1));

      database.command("sql", "ALTER TYPE Q SUPERTYPE +P");
      assertViewMatchesQuery("M", "P", List.of(1, 3, 33));
    });
  }

  private void withMaxDelta(final int cap, final Runnable body) {
    final ContextConfiguration config = database.getConfiguration();
    final int previous = config.getValueAsInteger(GlobalConfiguration.MATERIALIZED_VIEW_PER_RECORD_MAX_DELTA);
    config.setValue(GlobalConfiguration.MATERIALIZED_VIEW_PER_RECORD_MAX_DELTA, cap);
    try {
      body.run();
    } finally {
      config.setValue(GlobalConfiguration.MATERIALIZED_VIEW_PER_RECORD_MAX_DELTA, previous);
    }
  }

  private void createHierarchy() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.command("sql", "CREATE DOCUMENT TYPE Q");
    write("INSERT INTO P SET k = 1");
    write("INSERT INTO C SET k = 2");
    write("INSERT INTO Q SET k = 3");
  }

  private void write(final String statement) {
    database.transaction(() -> database.command("sql", statement));
  }

  private long writes() {
    final Map<String, Object> stats = database.getStats();
    return ((Number) stats.get("updateRecord")).longValue() + ((Number) stats.get("createRecord")).longValue()
        + ((Number) stats.get("deleteRecord")).longValue();
  }

  /** {@code first}, then the keys from {@code from} (inclusive) to {@code to} (exclusive). */
  private List<Integer> expectedKeys(final int first, final int from, final int to) {
    final List<Integer> keys = new ArrayList<>();
    keys.add(first);
    for (int i = from; i < to; i++)
      keys.add(i);
    return keys;
  }

  private void assertViewMatchesQuery(final String view, final String source, final List<Integer> expected) {
    assertThat(sortedKeys("SELECT k FROM " + source)).isEqualTo(expected);
    assertThat(sortedKeys("SELECT k FROM " + view)).as("rows of view %s", view).isEqualTo(expected);
    assertThat(database.getSchema().getMaterializedView(view).getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
  }

  private List<Integer> sortedKeys(final String query) {
    final List<Integer> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.stream().forEach(r -> keys.add(r.<Number>getProperty("k").intValue()));
    }
    keys.sort(null);
    return keys;
  }
}
