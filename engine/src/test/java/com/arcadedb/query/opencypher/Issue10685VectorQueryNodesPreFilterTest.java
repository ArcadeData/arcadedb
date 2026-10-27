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
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10685: {@code db.index.vector.queryNodes(index, k, vector)} had no filter, so a tenant filter could only be applied
 * to its k results afterwards and a query whose nearest neighbours belong to another tenant answered fewer rows than asked,
 * or none. The optional fourth argument {@code {where: ..., filter: [...]}} restricts the candidates DURING the search, the
 * same pre-filter {@code vector.neighbors} takes.
 * <p>
 * The data reproduces the issue: 900 vectors of tenant b, all nearer the query than any of the 100 of tenant a.
 */
class Issue10685VectorQueryNodesPreFilterTest extends TestHelper {
  private static final int        K     = 10;
  private static final float[]    QUERY = { 1f, 0f };
  private static final Map<String, Object> PARAMS = Map.of("q", QUERY, "k", K, "tenant", "a");

  @Override
  public void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Fact");
    database.command("sql", "CREATE PROPERTY Fact.tenant STRING");
    database.command("sql", "CREATE PROPERTY Fact.embedding ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Fact (embedding) LSM_VECTOR METADATA { dimensions: 2, similarity: 'EUCLIDEAN' }");
    database.transaction(() -> {
      // tenant b: on the x axis near the query, every one nearer than any of tenant a
      for (int i = 0; i < 900; i++)
        database.newVertex("Fact").set("tenant", "b", "embedding", new float[] { 1f, i * 0.0001f }).save();
      // tenant a: far up the y axis
      for (int i = 0; i < 100; i++)
        database.newVertex("Fact").set("tenant", "a", "embedding", new float[] { 1f, 5f + i * 0.01f }).save();
    });
  }

  @Test
  void postFilterAfterTheTopKStillAnswersNothing() {
    // the documented three-argument form is unchanged: the filter applies after the top k
    assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q) YIELD node, score "
        + "WITH node WHERE node.tenant = $tenant RETURN node.tenant AS tenant")).isEmpty();
  }

  @Test
  void whereMapPreFiltersTheSearch() {
    final List<String> tenants = tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {where: {tenant: $tenant}}) "
        + "YIELD node, score RETURN node.tenant AS tenant");
    assertThat(tenants).hasSize(K);
    assertThat(tenants).containsOnly("a");
  }

  @Test
  void whereConditionWithNamedParameterPreFiltersTheSearch() {
    final List<String> tenants = tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {where: 'tenant = :tenant'}) "
        + "YIELD node, score RETURN node.tenant AS tenant");
    assertThat(tenants).hasSize(K);
    assertThat(tenants).containsOnly("a");
  }

  @Test
  void filterOfCollectedNodesPreFiltersTheSearch() {
    final List<String> tenants = tenants("MATCH (f:Fact) WHERE f.tenant = $tenant WITH collect(f) AS allowed "
        + "CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {filter: allowed}) YIELD node, score "
        + "RETURN node.tenant AS tenant");
    assertThat(tenants).hasSize(K);
    assertThat(tenants).containsOnly("a");
  }

  @Test
  void filterAndWhereAreBothApplied() {
    // the filter names one record of tenant a and one of tenant b; the where keeps only the tenant a one
    final String ridA;
    final String ridB;
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Fact WHERE tenant = 'a' LIMIT 1")) {
      ridA = rs.next().<Object>getProperty("rid").toString();
    }
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Fact WHERE tenant = 'b' LIMIT 1")) {
      ridB = rs.next().<Object>getProperty("rid").toString();
    }
    final List<String> tenants = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {filter: $allowed, where: {tenant: $tenant}}) YIELD node, score "
            + "RETURN node.tenant AS tenant, node",
        Map.of("q", QUERY, "k", K, "tenant", "a", "allowed", List.of(ridA, ridB)))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        tenants.add(row.getProperty("tenant"));
        assertThat(row.<Document>getProperty("node").getIdentity().toString()).isEqualTo(ridA);
      }
    }
    assertThat(tenants).containsExactly("a");
  }

  @Test
  void filterMatchingNothingAnswersNothing() {
    assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {filter: []}) YIELD node, score "
        + "RETURN node.tenant AS tenant")).isEmpty();
  }

  @Test
  void emptyOrNullOptionsBehaveAsTheThreeArgumentForm() {
    assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {}) YIELD node, score "
        + "RETURN node.tenant AS tenant")).hasSize(K).containsOnly("b");
    assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, null) YIELD node, score "
        + "RETURN node.tenant AS tenant")).hasSize(K).containsOnly("b");
  }

  @Test
  void unknownOptionIsRejectedByName() {
    assertThatThrownBy(() -> tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {tennant: 'a'}) YIELD node, score "
        + "RETURN node.tenant AS tenant"))
        .hasStackTraceContaining("tennant");
  }

  @Test
  void optionsThatAreNotAMapAreRejected() {
    assertThatThrownBy(() -> tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, 'tenant = :tenant') YIELD node, score "
        + "RETURN node.tenant AS tenant"))
        .hasStackTraceContaining("options");
  }

  private List<String> tenants(final String cypher) {
    final List<String> tenants = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher, PARAMS)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        tenants.add(row.getProperty("tenant"));
      }
    }
    return tenants;
  }
}
