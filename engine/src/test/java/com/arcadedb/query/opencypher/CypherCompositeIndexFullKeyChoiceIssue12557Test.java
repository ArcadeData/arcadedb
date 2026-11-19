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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #12557: a node pattern that binds every column of a composite index must seek that index on the whole
 * key, whatever other composite index shares its leading column. The cost-based optimizer took the first index whose FIRST column
 * the pattern bound, then filtered the other columns row by row, so a lookup by {@code (tenantId, id)} read the whole tenant
 * whenever an index on {@code (tenantId, name)} sat in front of it. A label disjunction ({@code (n:A|B)}) must seek the index of
 * each type, with the same choice.
 * <p>
 * The types hold the same indexes in the opposite order, because which one used to win depended on the order of the schema.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCompositeIndexFullKeyChoiceIssue12557Test extends TestHelper {
  private static final int PER_TENANT = 300;

  @Override
  protected void beginTest() {
    // wholeKeyFirst: the (tenantId, id) index is created before the (tenantId, name) one; wholeKeyLast: after it
    createType("EA", true);
    createType("EB", false);
    createType("LA", true);
    createType("LB", false);

    // the whole key is the only index that leads with the tenant
    createTypeWithIndexes("EC", "(tenantId, id) NOTUNIQUE");
    // a UNIQUE whole key next to an index of the same leading column
    createTypeWithIndexes("EU", "(tenantId, name) NOTUNIQUE", "(tenantId, id) UNIQUE");
    // a single-column index on the second column does not hide the whole key either
    createTypeWithIndexes("EI", "(tenantId, name) NOTUNIQUE", "(id) NOTUNIQUE", "(tenantId, id) NOTUNIQUE");
    // a longer key that binds the same columns: the exact key is read as a lookup, the longer one as a prefix range
    createTypeWithIndexes("EL", "(tenantId, id, name) NOTUNIQUE", "(tenantId, id) NOTUNIQUE");
    // a UNIQUE whole key that binds fewer columns than a longer key over the same leading columns
    createTypeWithIndexes("EV", "(tenantId, id, name) NOTUNIQUE", "(tenantId, id) UNIQUE");
  }

  private void createType(final String type, final boolean wholeKeyFirst) {
    if (wholeKeyFirst)
      createTypeWithIndexes(type, "(tenantId, id) NOTUNIQUE", "(tenantId, name) NOTUNIQUE");
    else
      createTypeWithIndexes(type, "(tenantId, name) NOTUNIQUE", "(tenantId, id) NOTUNIQUE");
  }

  private void createTypeWithIndexes(final String type, final String... indexes) {
    database.command("sql", "CREATE VERTEX TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".tenantId STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".id STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
    for (final String index : indexes)
      database.command("sql", "CREATE INDEX ON " + type + " " + index);
    database.transaction(() -> {
      for (final String tenant : List.of("t1", "t2"))
        for (int i = 1; i <= PER_TENANT; i++)
          database.newVertex(type).set("tenantId", tenant).set("id", type + "-" + i).set("name", "n" + i).save();
    });
  }

  @Test
  void inlineEqualityOnEveryColumnSeeksTheWholeKey() {
    for (final String type : List.of("EA", "EB")) {
      final String plan = explain("MATCH (e:" + type + " {tenantId: $t, id: $v}) RETURN e.id AS id");
      assertThat(plan).as("plan of %s", type).contains("NodeIndexSeek").contains("[tenantId,id]");
      assertThat(plan).as("plan of %s", type).doesNotContain("NodeByLabelScan");
      assertThat(ids("MATCH (e:" + type + " {tenantId: $t, id: $v}) RETURN e.id AS id", type + "-150")).containsExactly(type + "-150");
    }
  }

  @Test
  void whereEqualityOnEveryColumnSeeksTheWholeKey() {
    for (final String type : List.of("EA", "EB")) {
      final String query = "MATCH (e:" + type + ") WHERE e.tenantId = $t AND e.id = $v RETURN e.id AS id";
      final String plan = explain(query);
      assertThat(plan).as("plan of %s", type).contains("NodeIndexSeek").contains("[tenantId,id]");
      assertThat(ids(query, type + "-7")).containsExactly(type + "-7");
    }
  }

  @Test
  void theColumnsOfTheKeyMayBeWrittenInAnyOrder() {
    for (final String type : List.of("EA", "EB")) {
      final String query = "MATCH (e:" + type + " {id: $v, tenantId: $t}) RETURN e.id AS id";
      assertThat(explain(query)).as("plan of %s", type).contains("NodeIndexSeek").contains("[tenantId,id]");
      assertThat(ids(query, type + "-299")).containsExactly(type + "-299");
    }
  }

  @Test
  void theWholeKeyIsSoughtWhateverOtherIndexesTheTypeHolds() {
    for (final String type : List.of("EC", "EU", "EI")) {
      final String query = "MATCH (e:" + type + " {tenantId: $t, id: $v}) RETURN e.id AS id";
      final String plan = explain(query);
      assertThat(plan).as("plan of %s", type).contains("NodeIndexSeek").contains("[tenantId,id]");
      assertThat(plan).as("plan of %s", type).doesNotContain("[tenantId,name]").doesNotContain("NodeByLabelScan");
      assertThat(ids(query, type + "-250")).as("rows of %s", type).containsExactly(type + "-250");
    }
  }

  @Test
  void theIndexThatBindsFewerColumnsOfAKeyIsNotChosenOverAnExactOne() {
    final String query = "MATCH (e:EL {tenantId: $t, id: $v}) RETURN e.id AS id";
    final String plan = explain(query);
    assertThat(plan).contains("NodeIndexSeek").contains("[tenantId,id]").doesNotContain("[tenantId,id,name]");
    assertThat(ids(query, "EL-5")).containsExactly("EL-5");
  }

  @Test
  void aUniqueWholeKeyIsChosenOverALongerKeyThatIsFullyBound() {
    final String query = "MATCH (e:EV {tenantId: $t, id: $v, name: 'n5'}) RETURN e.id AS id";
    final String plan = explain(query);
    assertThat(plan).contains("NodeIndexSeek").contains("[tenantId,id]").doesNotContain("[tenantId,id,name]");
    assertThat(ids(query, "EV-5")).containsExactly("EV-5");
  }

  @Test
  void theColumnsOfAKeyThePatternLeavesUnboundStayPrefixSeeks() {
    // only the tenant is bound: both indexes lead with it and tie, so the index name picks the same one for either creation order,
    // and the id is filtered above the prefix range
    for (final String type : List.of("EA", "EB")) {
      final String query = "MATCH (e:" + type + " {tenantId: $t}) RETURN e.id AS id";
      final String plan = explain(query);
      assertThat(plan).as("plan of %s", type).contains("NodeIndexSeek").contains("[tenantId,id]").doesNotContain("NodeByLabelScan");
      assertThat(ids(query, "x")).as("rows of %s", type).hasSize(PER_TENANT);
    }
  }

  @Test
  void theSameSchemaIsAlwaysPlannedTheSameWay() {
    final String first = explain("MATCH (e:EA {tenantId: $t}) RETURN e.id AS id");
    for (int i = 0; i < 5; i++)
      assertThat(explain("MATCH (e:EA {tenantId: $t}) RETURN e.id AS id")).isEqualTo(first);
  }

  @Test
  void aKeyWithNoMatchingTenantAnswersNothing() {
    for (final String type : List.of("EA", "EB"))
      assertThat(ids("MATCH (e:" + type + " {tenantId: 't3', id: $v}) RETURN e.id AS id", type + "-1")).isEmpty();
  }

  @Test
  void aLabelDisjunctionSeeksTheWholeKeyOfEveryType() {
    final String query = "MATCH (n:EA|LB {tenantId: $t, id: $v}) RETURN n.id AS id";
    final String plan = explain(query);
    assertThat(plan).contains("NodeByLabelDisjunctionIndexSeek").doesNotContain("NodeByLabelDisjunctionScan");
    assertThat(plan).contains("[tenantId,id]");
    assertThat(ids(query, "EA-42")).containsExactly("EA-42");
    assertThat(ids(query, "LB-42")).containsExactly("LB-42");
    assertThat(ids(query, "EB-42")).isEmpty();
  }

  @Test
  void aLabelDisjunctionAfterUnwindSeeksTheWholeKeyOfEveryType() {
    for (final String types : List.of("EA|LB", "EB|LA")) {
      final String query = "UNWIND [$v] AS v MATCH (n:" + types + " {tenantId: $t, id: v}) RETURN n.id AS id";
      final String first = types.substring(0, 2);
      final String second = types.substring(3, 5);
      assertThat(profile(query, first + "-9")).as("plan of %s", types).contains("[tenantId, id]").doesNotContain("[tenantId, name]");
      assertThat(ids(query, first + "-9")).containsExactly(first + "-9");
      assertThat(ids(query, second + "-9")).containsExactly(second + "-9");
    }
  }

  @Test
  void aNodeBoundAfterAWithSeeksTheWholeKey() {
    for (final String type : List.of("EA", "EB")) {
      final String query = "WITH $v AS v MATCH (e:" + type + " {tenantId: $t, id: v}) RETURN e.id AS id";
      assertThat(ids(query, type + "-33")).containsExactly(type + "-33");
      assertThat(profile(query, type + "-33")).as("plan of %s", type).contains("[tenantId, id]");
    }
  }

  private List<String> ids(final String query, final String value) {
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, Map.of("t", "t1", "v", value))) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("id"));
    }
    return got;
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, Map.of("t", "t1", "v", "x"))) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private String profile(final String query, final String value) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, Map.of("t", "t1", "v", value))) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
