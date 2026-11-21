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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

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
