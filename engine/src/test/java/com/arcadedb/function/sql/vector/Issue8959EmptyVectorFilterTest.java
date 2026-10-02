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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8959: a {@code filter} option of {@code vector.neighbors} that resolves to no RIDs was read as "no filter", so a
 * tenant subquery with no rows returned the records of every other tenant.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8959EmptyVectorFilterTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.name STRING");
    database.command("sql", "CREATE PROPERTY Doc.tenant STRING");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"COSINE\" }");
    database.transaction(() -> {
      database.newDocument("Doc").set("name", "a1", "tenant", "a", "vector", new float[] { 0.9f, 0.1f }).save();
      database.newDocument("Doc").set("name", "a2", "tenant", "a", "vector", new float[] { 0.1f, 0.9f }).save();
      database.newDocument("Doc").set("name", "b1", "tenant", "b", "vector", new float[] { 0.8f, 0.2f }).save();
      database.newDocument("Doc").set("name", "b2", "tenant", "b", "vector", new float[] { 0.2f, 0.8f }).save();
    });
  }

  private List<String> search(final String filter, final String tenant, final List<RID> ids) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(vectorNeighbors('Doc[vector]', :v, 3, { filter: " + filter + " })))",
        Map.of("v", new float[] { 1.0f, 0.0f }, "tenant", tenant, "ids", ids))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        names.add(r.getProperty("name"));
      }
    }
    return names;
  }

  private List<RID> idsOf(final String tenant) {
    final List<RID> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE tenant = ?", tenant)) {
      rs.forEachRemaining(r -> ids.add(r.getIdentity().get()));
    }
    return ids;
  }

  @Test
  void subqueryMatchingNothingReturnsNothing() {
    assertThat(search("(SELECT @rid FROM Doc WHERE tenant = :tenant)", "c", idsOf("c"))).isEmpty();
  }

  @Test
  void emptyRidListReturnsNothing() {
    assertThat(search(":ids", "c", idsOf("c"))).isEmpty();
  }

  @Test
  void badIndexSpecStaysLoudWithAnEmptyFilter() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(vectorNeighbors('Nope[vector]', [1.0, 0.0], 3, { filter: :ids }))", Map.of("ids", new ArrayList<RID>()))) {
        rs.hasNext();
      }
    }).isInstanceOf(SchemaException.class);
  }

  @Test
  void nestedEmptyListAndListOfNullsReturnNothing() {
    final List<Object> nested = new ArrayList<>();
    nested.add(new ArrayList<RID>());
    final List<Object> nulls = new ArrayList<>();
    nulls.add(null);
    for (final List<Object> ids : List.of(nested, nulls))
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(vectorNeighbors('Doc[vector]', [1.0, 0.0], 3, { filter: :ids }))", Map.of("ids", ids))) {
        assertThat(rs.hasNext()).isFalse();
      }
  }

  @Test
  void filterMatchingRecordsStillRestrictsTheSearch() {
    assertThat(search("(SELECT @rid FROM Doc WHERE tenant = :tenant)", "a", idsOf("a"))).containsExactlyInAnyOrder("a1", "a2");
    assertThat(search(":ids", "b", idsOf("b"))).containsExactlyInAnyOrder("b1", "b2");
  }

  @Test
  void absentFilterStillSearchesEverything() {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(vectorNeighbors('Doc[vector]', [1.0, 0.0], 3)))")) {
      rs.forEachRemaining(r -> names.add(r.getProperty("name")));
    }
    assertThat(names).hasSize(3);
  }
}
