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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10264: storing the result of a projection that holds a SELECT subquery failed with a {@code ClassCastException}
 * ({@code ResultInternal} to {@code Map}) in the record serializer, for {@code CREATE MATERIALIZED VIEW} and for
 * {@code INSERT ... FROM}, while the query itself answered fine. The subquery's value is a list of non-element results, which
 * the serializer types as maps but then cast without converting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10264ProjectedSubqueryStoreTest extends TestHelper {
  private static final String QUERY = "SELECT id, (SELECT count(*) FROM Owner) AS oc FROM Base WHERE n < 3";

  @BeforeEach
  void fill() {
    database.command("sql", "CREATE DOCUMENT TYPE Base").close();
    database.command("sql", "CREATE PROPERTY Base.id INTEGER").close();
    database.command("sql", "CREATE PROPERTY Base.n INTEGER").close();
    database.command("sql", "CREATE DOCUMENT TYPE Owner").close();
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newDocument("Base").set("id", i).set("n", i).save();
      for (int i = 0; i < 2; i++)
        database.newDocument("Owner").set("k", i).save();
    });
  }

  @SuppressWarnings("unchecked")
  private void assertCountOfOwners(final String source) {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT id, oc FROM " + source + " ORDER BY id")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(rows++);
        final List<Object> oc = row.getProperty("oc");
        assertThat(oc).hasSize(1);
        assertThat(((Map<String, Object>) oc.getFirst()).get("count(*)")).isEqualTo(2L);
      }
    }
    assertThat(rows).isEqualTo(3);
  }

  @Test
  void insertFromSelectStoresTheSubqueryAsAListOfMaps() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.transaction(() -> database.command("sql", "INSERT INTO T FROM " + QUERY).close());

    assertCountOfOwners("T");
  }

  @Test
  void materializedViewWithDefaultRefreshStoresTheSubquery() {
    database.command("sql", "CREATE MATERIALIZED VIEW vm AS " + QUERY).close();

    assertCountOfOwners("vm");
  }

  @Test
  void incrementalMaterializedViewStoresTheSubquery() {
    database.command("sql", "CREATE MATERIALIZED VIEW vi AS " + QUERY + " REFRESH INCREMENTAL").close();

    assertCountOfOwners("vi");
  }

  @Test
  void aMapHoldingAResultIsStoredToo() {
    // the same cast sits behind a Result nested in a map value, so it is pinned here and not only through the list path
    database.command("sql", "CREATE DOCUMENT TYPE M").close();
    database.transaction(() -> database.command("sql",
        "INSERT INTO M FROM SELECT id, { 'owners': (SELECT count(*) FROM Owner) } AS wrapped FROM Base WHERE n < 1").close());

    try (final ResultSet rs = database.query("sql", "SELECT wrapped FROM M")) {
      assertThat(rs.hasNext()).isTrue();
      final Map<String, Object> wrapped = rs.next().getProperty("wrapped");
      assertThat(wrapped.get("owners")).isEqualTo(List.of(Map.of("count(*)", 2L)));
    }
  }

  @Test
  void theControlWithoutASubqueryKeepsWorking() {
    database.command("sql", "CREATE MATERIALIZED VIEW vc AS SELECT id, n FROM Base WHERE n < 3 REFRESH INCREMENTAL").close();

    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM vc")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(3L);
    }
  }
}
