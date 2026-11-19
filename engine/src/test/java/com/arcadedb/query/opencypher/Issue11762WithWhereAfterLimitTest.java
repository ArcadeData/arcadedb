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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11762: the WHERE of a WITH filters the rows the WITH produces, so it runs after its ORDER BY, SKIP and LIMIT. It was
 * pushed below them and filtered the rows entering the limit, so {@code WITH x ORDER BY x LIMIT 1 WHERE x = 2} answered the row
 * the limit removed. Neo4j and Memgraph answer no row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11762WithWhereAfterLimitTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Node");
      database.command("opencypher", "CREATE (:Node {id: 1, v: 1, b: true})");
      database.command("opencypher", "CREATE (:Node {id: 2, v: 2, b: false})");
      database.command("opencypher", "CREATE (:Node {id: 3, v: 3, b: true})");
    });
  }

  @Test
  void whereAfterLimitFiltersTheLimitedRows() {
    assertThat(column("UNWIND [1,2,3] AS x WITH x ORDER BY x LIMIT 1 WHERE x = 2 RETURN x AS x", "x")).isEmpty();
    assertThat(column("UNWIND [1,2,3] AS x WITH x ORDER BY x LIMIT 1 WHERE x = 1 RETURN x AS x", "x")).containsExactly(1L);
    assertThat(column("UNWIND [1,2,3] AS x WITH x LIMIT 1 WHERE x = 2 RETURN x AS x", "x")).isEmpty();
  }

  @Test
  void whereAfterSkipFiltersTheSkippedRows() {
    assertThat(column("UNWIND [1,2,3] AS x WITH x ORDER BY x SKIP 2 WHERE x = 1 RETURN x AS x", "x")).isEmpty();
    assertThat(column("UNWIND [1,2,3] AS x WITH x ORDER BY x SKIP 2 WHERE x = 3 RETURN x AS x", "x")).containsExactly(3L);
    assertThat(column("UNWIND [1,2,3] AS x WITH x ORDER BY x SKIP 1 LIMIT 1 WHERE x = 3 RETURN x AS x", "x")).isEmpty();
    assertThat(column("UNWIND [1,2,3] AS x WITH x ORDER BY x DESC SKIP 1 LIMIT 1 WHERE x = 2 RETURN x AS x", "x")).containsExactly(2L);
  }

  @Test
  void graphWhereAfterLimit() {
    assertThat(column("MATCH (n:Node) WITH n ORDER BY n.v LIMIT 2 WHERE n.b = true RETURN n.id AS id ORDER BY id", "id"))
        .containsExactly(1);
    // a WHERE that keeps both limited rows still answers both
    assertThat(column("MATCH (n:Node) WITH n ORDER BY n.v LIMIT 2 WHERE n.id < 10 RETURN n.id AS id ORDER BY id", "id"))
        .containsExactly(1, 2);
    assertThat(column("MATCH (n:Node) WITH n ORDER BY n.v SKIP 1 WHERE n.b = true RETURN n.id AS id ORDER BY id", "id"))
        .containsExactly(3);
    assertThat(column("MATCH (n:Node) WITH n.id AS id, n.b AS b ORDER BY id LIMIT 2 WHERE b RETURN id ORDER BY id", "id"))
        .containsExactly(1);
  }

  @Test
  void whereBeforeTheWithPipelineIsUnchanged() {
    assertThat(column("MATCH (n:Node) WHERE n.b = true WITH n ORDER BY n.v LIMIT 2 RETURN n.id AS id ORDER BY id", "id"))
        .containsExactly(1, 3);
  }

  @Test
  void whereAfterLimitOnAnAggregatingAndDistinctWith() {
    assertThat(column("UNWIND [1,1,2,2,3] AS x WITH DISTINCT x ORDER BY x LIMIT 2 WHERE x = 3 RETURN x AS x", "x")).isEmpty();
    assertThat(column("UNWIND [1,1,2,2,3] AS x WITH DISTINCT x ORDER BY x LIMIT 2 WHERE x = 2 RETURN x AS x", "x"))
        .containsExactly(2L);
    assertThat(column("UNWIND [1,1,2,2,3] AS x WITH x, count(*) AS c ORDER BY x LIMIT 1 WHERE x = 2 RETURN x AS x", "x")).isEmpty();
    assertThat(column("UNWIND [1,1,2,2,3] AS x WITH x, count(*) AS c ORDER BY c DESC, x LIMIT 2 WHERE c = 2 RETURN x AS x ORDER BY x",
        "x")).containsExactly(1L, 2L);
  }

  private List<Object> column(final String query, final String name) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        values.add(resultSet.next().getProperty(name));
    }
    return values;
  }
}
