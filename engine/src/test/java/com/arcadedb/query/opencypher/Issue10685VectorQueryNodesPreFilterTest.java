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
import com.arcadedb.exception.CommandSQLParsingException;
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
  private static final int                 K      = 10;
  private static final float[]             QUERY  = { 1f, 0f };
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
  void preFilterMatchingFewerThanKAnswersEveryMatch() {
    // k above the 100 records of tenant a: the search and its stale top-up answer every one of them and nothing else
    final List<String> tenants = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {where: {tenant: $tenant}}) YIELD node, score "
            + "RETURN node.tenant AS tenant", Map.of("q", QUERY, "k", 200, "tenant", "a"))) {
      while (rs.hasNext())
        tenants.add(rs.next().getProperty("tenant"));
    }
    assertThat(tenants).hasSize(100);
    assertThat(tenants).containsOnly("a");
  }

  @Test
  void preFilterAppliesToEverySubIndexOfAParentTypeSpec() {
    // a sub-type brings its own buckets, so 'Fact[embedding]' resolves several indexes: tenant b nearest in the sub-type
    // too, one tenant a record in it that the filter names and one the filter leaves out
    database.command("sql", "CREATE VERTEX TYPE SubFact EXTENDS Fact");
    final String[] subA = new String[2];
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newVertex("SubFact").set("tenant", "b", "embedding", new float[] { 1f, i * 0.00001f }).save();
      subA[0] = database.newVertex("SubFact").set("tenant", "a", "embedding", new float[] { 1f, 4f }).save().getIdentity().toString();
      subA[1] = database.newVertex("SubFact").set("tenant", "a", "embedding", new float[] { 1f, 4.1f }).save().getIdentity()
          .toString();
    });
    final List<String> allowed = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Fact WHERE @type = 'Fact'")) {
      while (rs.hasNext())
        allowed.add(rs.next().<Object>getProperty("rid").toString());
    }
    allowed.add(subA[0]);

    final List<String> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {filter: $allowed, where: {tenant: $tenant}}) YIELD node, score "
            + "RETURN node", Map.of("q", QUERY, "k", K, "tenant", "a", "allowed", allowed))) {
      while (rs.hasNext()) {
        final Document node = rs.next().getProperty("node");
        assertThat(node.getString("tenant")).isEqualTo("a");
        rids.add(node.getIdentity().toString());
      }
    }
    assertThat(rids).hasSize(K);
    // the sub-type's allowed tenant a record is the nearest of them all, the one the filter left out is not answered
    assertThat(rids.getFirst()).isEqualTo(subA[0]);
    assertThat(rids).doesNotContain(subA[1]);
  }

  @Test
  void filterMatchingNothingAnswersNothing() {
    assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {filter: []}) YIELD node, score "
        + "RETURN node.tenant AS tenant")).isEmpty();
  }

  @Test
  void whereMatchingNothingAnswersNothing() {
    assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {where: {tenant: 'nobody'}}) YIELD node, score "
        + "RETURN node.tenant AS tenant")).isEmpty();
    assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {where: \"tenant = 'nobody'\"}) YIELD node, score "
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
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Unknown option 'tennant'");
  }

  @Test
  void optionsThatAreNotAMapAreRejected() {
    assertThatThrownBy(() -> tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, 'tenant = :tenant') YIELD node, score "
        + "RETURN node.tenant AS tenant"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("options must be a map");
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
