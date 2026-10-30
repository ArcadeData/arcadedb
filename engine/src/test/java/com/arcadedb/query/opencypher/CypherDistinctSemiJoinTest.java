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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A read-only {@code RETURN DISTINCT} whose items and ORDER BY are bound before the last clauses runs those clauses as a
 * semi-join, stopping at the first row they produce for each input row. The results must stay those of the full
 * expansion.
 */
class CypherDistinctSemiJoinTest {
  private static final String RULE_QUERY = """
      MATCH (h:Host)
      OPTIONAL MATCH (h)-[:IN_RULE]-(r:Rule)
      WITH h, r WHERE r.direction = 'IN'
      OPTIONAL MATCH (r)-[:FROM]-(s:Source)
      WITH h, r, s
      OPTIONAL MATCH (r)-[:USES]-(svc:Service)
      WITH svc, h, r, s
      OPTIONAL MATCH (s)-[:MEMBER]-(m:Host)
      WITH svc, h, r, s, m WHERE $port IN svc.ports OR $port IN r.ports
      MATCH (m)-[:HAS]-(p:Port)
      WITH h, r, s, m, p WHERE p.public = true
      """;

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypherdistinctsemijoin").create();
    database.transaction(() -> {
      for (final String type : List.of("Host", "Rule", "Source", "Service", "Port"))
        database.getSchema().createVertexType(type);
      for (final String type : List.of("IN_RULE", "FROM", "USES", "MEMBER", "HAS"))
        database.getSchema().createEdgeType(type);

      final List<MutableVertex> hosts = new ArrayList<>();
      for (int i = 0; i < 20; i++) {
        final MutableVertex host = database.newVertex("Host").set("name", String.format("h%02d", i)).save();
        if (i % 3 != 0)
          host.newEdge("HAS", database.newVertex("Port").set("public", i % 2 == 0).save());
        hosts.add(host);
      }

      // Each rule applies to many hosts, so a host reaches the same answer through several rules and members
      for (int i = 0; i < 6; i++) {
        final MutableVertex rule = database.newVertex("Rule").set("direction", i % 4 == 3 ? "OUT" : "IN")
            .set("ports", i % 2 == 0 ? List.of(161) : List.of()).save();
        final MutableVertex source = database.newVertex("Source").save();
        rule.newEdge("FROM", source);
        if (i % 3 == 1)
          rule.newEdge("USES", database.newVertex("Service").set("ports", List.of(161, 162)).save());
        for (int h = i; h < hosts.size(); h += 2)
          hosts.get(h).newEdge("IN_RULE", rule);
        for (int m = 0; m < hosts.size(); m += i + 2)
          source.newEdge("MEMBER", hosts.get(m));
      }
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void keepsTheResultOfTheFullExpansion() {
    final List<String> expected = new ArrayList<>(
        new TreeSet<>(column(RULE_QUERY + "RETURN h.name AS name", Map.of("port", 161), "name")));
    assertThat(expected).isNotEmpty();

    final String query = RULE_QUERY + "RETURN DISTINCT h AS host ORDER BY host.name";
    assertThat(plan(query, Map.of("port", 161))).contains("SEMI JOIN");
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, Map.of("port", 161))) {
      rs.forEachRemaining(r -> names.add(r.<Vertex>getProperty("host").getString("name")));
    }
    assertThat(names).isEqualTo(expected);

    final String paged = RULE_QUERY + "RETURN DISTINCT h.name AS name ORDER BY name SKIP 1 LIMIT 3";
    assertThat(plan(paged, Map.of("port", 161))).contains("SEMI JOIN");
    assertThat(column(paged, Map.of("port", 161), "name")).isEqualTo(expected.subList(1, Math.min(4, expected.size())));

    assertThat(column(RULE_QUERY + "RETURN DISTINCT h.name AS name", Map.of("port", 7), "name")).isEmpty();
  }

  @Test
  void optionalMatchKeepsRowsWithoutAMatch() {
    final String query = "MATCH (h:Host) OPTIONAL MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT h.name AS name ORDER BY name";
    assertThat(plan(query, Map.of())).contains("SEMI JOIN");
    assertThat(column(query, Map.of(), "name")).hasSize(20);
  }

  @Test
  void followsAVariableRenamedBeforeTheChain() {
    final String query = "MATCH (h:Host) WITH h AS x MATCH (x)-[:HAS]-(p:Port) RETURN DISTINCT x.name AS name ORDER BY name";
    assertThat(plan(query, Map.of())).contains("SEMI JOIN");
    assertThat(column(query, Map.of(), "name")).isEqualTo(
        new ArrayList<>(new TreeSet<>(column("MATCH (h:Host)-[:HAS]-(:Port) RETURN h.name AS name", Map.of(), "name"))));
  }

  @Test
  void notUsedWhenTheResultNeedsTheLastClauses() {
    final String[] queries = {
        // RETURN reads a variable the last MATCH binds
        "MATCH (h:Host) MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT p.public AS v",
        // not DISTINCT: every row counts
        "MATCH (h:Host) MATCH (h)-[:HAS]-(p:Port) RETURN h.name AS v",
        // aggregation counts the rows
        "MATCH (h:Host) MATCH (h)-[:IN_RULE]-(r:Rule) RETURN DISTINCT h.name AS v, count(r) AS c",
        // a WITH that looks across rows
        "MATCH (h:Host) MATCH (h)-[:IN_RULE]-(r:Rule) WITH h, r LIMIT 3 RETURN DISTINCT h.name AS v",
        "MATCH (h:Host) MATCH (h)-[:IN_RULE]-(r:Rule) WITH DISTINCT h, r RETURN DISTINCT h.name AS v",
        "MATCH (h:Host) MATCH (h)-[:IN_RULE]-(r:Rule) WITH h, r ORDER BY r.direction RETURN DISTINCT h.name AS v",
        // a WITH that rebinds the returned name
        "MATCH (h:Host) MATCH (h)-[:HAS]-(p:Port) WITH p AS h RETURN DISTINCT h.public AS v",
        // a statement-level WHERE
        "MATCH (h:Host) MATCH (h)-[:HAS]-(p:Port) WHERE p.public = true RETURN DISTINCT h.name AS v" };
    for (final String query : queries)
      assertThat(plan(query, Map.of())).as(query).doesNotContain("SEMI JOIN");

    database.transaction(() -> {
      final String write = "MATCH (h:Host) MATCH (h)-[:HAS]-(p:Port) SET h.seen = true RETURN DISTINCT h.name AS v";
      assertThat(column(write, Map.of(), "v")).hasSize(13);
    });
    assertThat(column("MATCH (h:Host {seen: true}) RETURN h.name AS v", Map.of(), "v")).hasSize(13);
  }

  private String plan(final String query, final Map<String, Object> params) {
    final StringBuilder plan = new StringBuilder();
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
      rs.forEachRemaining(r -> plan.append(r.toJSON()));
    }
    return plan.toString();
  }

  private List<String> column(final String query, final Map<String, Object> params, final String name) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query, params)) {
      rs.forEachRemaining(r -> values.add(String.valueOf((Object) r.getProperty(name))));
    }
    return values;
  }
}
