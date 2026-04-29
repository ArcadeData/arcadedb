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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for GitHub issue #4019.
 * <p>
 * A node variable carried through WITH must remain bound after a later anonymous
 * CREATE or MERGE writes an unrelated node. The anonymous write node has no
 * variable, so it must not overwrite any variable already present in the row.
 */
class Issue4019WithNodeVarNullAfterCreateMergeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-4019").create();
    database.getSchema().createVertexType("Person4019");
    database.getSchema().createVertexType("Temp4019");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Person4019 {name:'Alice'}), (:Person4019 {name:'Bob'})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void nodeVarCarriedThroughWithMustSurviveAnonymousCreate() {
    final ResultSet[] ref = new ResultSet[1];
    database.transaction(() -> ref[0] = database.command("opencypher",
        "MATCH (n:Person4019) WITH n CREATE (:Temp4019 {x:1}) RETURN n.name AS name ORDER BY name"));

    final List<Result> rows = collect(ref[0]);
    assertThat(rows).hasSize(2);
    assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
  }

  @Test
  void nodeVarCarriedThroughWithMustSurviveAnonymousMerge() {
    final ResultSet[] ref = new ResultSet[1];
    database.transaction(() -> ref[0] = database.command("opencypher",
        "MATCH (n:Person4019) WITH n MERGE (:Temp4019 {x:10}) RETURN n.name AS name ORDER BY name"));

    final List<Result> rows = collect(ref[0]);
    assertThat(rows).hasSize(2);
    assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
  }

  @Test
  void nodeVarAndScalarBothSurviveAnonymousCreate() {
    final ResultSet[] ref = new ResultSet[1];
    database.transaction(() -> ref[0] = database.command("opencypher",
        "MATCH (n:Person4019) WITH n, 1 AS x CREATE (:Temp4019 {x:3}) RETURN n.name AS name, x ORDER BY name"));

    final List<Result> rows = collect(ref[0]);
    assertThat(rows).hasSize(2);
    assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat(((Number) rows.get(0).getProperty("x")).intValue()).isEqualTo(1);
    assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
    assertThat(((Number) rows.get(1).getProperty("x")).intValue()).isEqualTo(1);
  }

  @Test
  void scalarAliasAloneStillWorksWithCreate() {
    final ResultSet[] ref = new ResultSet[1];
    database.transaction(() -> ref[0] = database.command("opencypher",
        "MATCH (n:Person4019) WITH n.name AS name CREATE (:Temp4019 {x:2}) RETURN name ORDER BY name"));

    final List<Result> rows = collect(ref[0]);
    assertThat(rows).hasSize(2);
    assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
  }

  @Test
  void withoutCreateNodeVarIsCorrect() {
    final List<Result> rows = collect(database.query("opencypher",
        "MATCH (n:Person4019) WITH n RETURN n.name AS name ORDER BY name"));

    assertThat(rows).hasSize(2);
    assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
