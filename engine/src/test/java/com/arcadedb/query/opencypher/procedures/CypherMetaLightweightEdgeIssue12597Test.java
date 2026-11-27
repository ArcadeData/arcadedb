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
package com.arcadedb.query.opencypher.procedures;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12597: {@code meta.stats} and {@code meta.graph} counted and scanned the records of each edge type, and a LIGHTWEIGHT
 * edge type has none (its edges are entries of the two vertices' edge lists), so its relationships were missing from
 * {@code relCount}, {@code relTypeCount}, {@code relTypes} and {@code relTypesCount}, and {@code meta.graph} answered no
 * relationship for it. Neo4j counts every relationship, so a lightweight type is described like any other.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherMetaLightweightEdgeIssue12597Test extends TestHelper {

  private static final String STATS = """
      CALL apoc.meta.stats() YIELD nodeCount, relCount, labelCount, relTypeCount, propertyKeyCount, labels, relTypes, relTypesCount
      RETURN nodeCount, relCount, labelCount, relTypeCount, propertyKeyCount, labels, relTypes, relTypesCount""";
  private static final String GRAPH = """
      CALL apoc.meta.graph() YIELD nodes, relationships
      RETURN [n IN nodes | [n.name, n.count]] AS n,
             [r IN relationships | [type(r), r.out, r.in, r.count, labels(startNode(r)), labels(endNode(r))]] AS r""";

  @AfterEach
  void unbind() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  @Test
  void theRelationshipsOfALightweightTypeAreCountedAndDescribed() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE EDGE TYPE LW LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE R");
    database.command("sql", "CREATE EDGE TYPE EmptyLW LIGHTWEIGHT");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(1, 5) AS i CREATE (:A {i: i})-[:LW]->(:B {i: i})").close();
      database.command("opencypher", "UNWIND range(1, 2) AS i CREATE (:B {i: i})-[:LW]->(:A {i: i})").close();
      database.command("opencypher", "UNWIND range(1, 3) AS i CREATE (:A {i: i})-[:R {w: i}]->(:B {i: i})").close();
    });

    // the premise: the lightweight relationships exist and none of them is a record
    assertThat(database.countType("LW", false)).isZero();
    assertThat(countOf("MATCH ()-[r:LW]->() RETURN count(r) AS n")).isEqualTo(7L);

    final Result stats = single(STATS);
    assertThat(longOf(stats.getProperty("nodeCount"))).isEqualTo(20L);
    assertThat(longOf(stats.getProperty("relCount"))).isEqualTo(10L);
    assertThat(longOf(stats.getProperty("labelCount"))).isEqualTo(2L);
    // a lightweight type with no relationship is left out, as a regular type with none is
    assertThat(longOf(stats.getProperty("relTypeCount"))).isEqualTo(2L);
    // i on the nodes, w on the R relationships: a lightweight relationship carries no property
    assertThat(longOf(stats.getProperty("propertyKeyCount"))).isEqualTo(2L);
    assertThat((Map<?, ?>) stats.getProperty("relTypesCount")).isEqualTo(Map.of("LW", 7L, "R", 3L));
    assertThat((Map<?, ?>) stats.getProperty("relTypes")).isEqualTo(Map.of(
        "()-[:LW]->()", 7L, "(:A)-[:LW]->()", 5L, "(:B)-[:LW]->()", 2L, "()-[:LW]->(:A)", 2L, "()-[:LW]->(:B)", 5L,
        "()-[:R]->()", 3L, "(:A)-[:R]->()", 3L, "()-[:R]->(:B)", 3L));

    final Result graph = single(GRAPH);
    assertThat(list(graph.getProperty("n"))).containsExactly(List.of("A", 10L), List.of("B", 10L));
    assertThat(list(graph.getProperty("r"))).containsExactly(
        List.of("LW", 5L, 5L, 7L, List.of("A"), List.of("B")),
        List.of("LW", 2L, 2L, 7L, List.of("B"), List.of("A")),
        List.of("R", 3L, 3L, 3L, List.of("A"), List.of("B")));

    // the same answer from a database that was closed and opened again
    reopenDatabase();
    assertThat((Map<?, ?>) single(STATS).getProperty("relTypesCount")).isEqualTo(Map.of("LW", 7L, "R", 3L));
  }

  /**
   * A lightweight subtype of a regular type: each relationship is counted under its own type, as the records of a regular
   * subtype are.
   */
  @Test
  void aLightweightSubtypeIsCountedUnderItsOwnType() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE EDGE TYPE Base");
    database.command("sql", "CREATE EDGE TYPE Light EXTENDS Base LIGHTWEIGHT");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(1, 2) AS i CREATE (:A)-[:Base]->(:A)").close();
      database.command("opencypher", "UNWIND range(1, 3) AS i CREATE (:A)-[:Light]->(:A)").close();
    });

    final Result stats = single(STATS);
    assertThat(longOf(stats.getProperty("relCount"))).isEqualTo(5L);
    assertThat((Map<?, ?>) stats.getProperty("relTypesCount")).isEqualTo(Map.of("Base", 2L, "Light", 3L));
    assertThat(list(single(GRAPH).getProperty("r"))).containsExactly(
        List.of("Base", 2L, 2L, 2L, List.of("A"), List.of("A")),
        List.of("Light", 3L, 3L, 3L, List.of("A"), List.of("A")));
  }

  /**
   * A lightweight relationship is read from the node it leaves, so it is the caller's to count only when that node is: one
   * leaving a node of an unreadable type is not counted, one reaching such a node is counted without naming its label, and an
   * unreadable lightweight type is not described at all.
   */
  @Test
  void theWalkOfTheLightweightRelationshipsHonoursThePerTypeAcl() {
    database.command("sql", "CREATE VERTEX TYPE PublicNode");
    database.command("sql", "CREATE VERTEX TYPE SecretNode");
    database.command("sql", "CREATE EDGE TYPE LW LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE SECRET_LW LIGHTWEIGHT");
    database.transaction(() -> database.command("opencypher", """
        CREATE (:PublicNode)-[:LW]->(:SecretNode),
               (:SecretNode)-[:LW]->(:PublicNode),
               (a:PublicNode)-[:LW]->(b:PublicNode),
               (a)-[:SECRET_LW]->(b)""").close());

    // control: unbound, everything is counted
    Result stats = single(STATS);
    assertThat(longOf(stats.getProperty("relCount"))).isEqualTo(4L);
    assertThat((Map<?, ?>) stats.getProperty("relTypesCount")).isEqualTo(Map.of("LW", 3L, "SECRET_LW", 1L));

    bindUserDenying(Set.of("SecretNode", "SECRET_LW"));

    stats = single(STATS);
    assertThat(longOf(stats.getProperty("relCount"))).isEqualTo(2L);
    assertThat(longOf(stats.getProperty("relTypeCount"))).isEqualTo(1L);
    assertThat((Map<?, ?>) stats.getProperty("relTypesCount")).isEqualTo(Map.of("LW", 2L));
    assertThat((Map<?, ?>) stats.getProperty("relTypes")).isEqualTo(
        Map.of("()-[:LW]->()", 2L, "(:PublicNode)-[:LW]->()", 2L, "()-[:LW]->(:PublicNode)", 1L));

    final Result graph = single(GRAPH);
    assertThat(list(graph.getProperty("n"))).containsExactly(List.of("PublicNode", 4L));
    assertThat(list(graph.getProperty("r"))).containsExactly(
        List.of("LW", 2L, 1L, 2L, List.of("PublicNode"), List.of("PublicNode")));
  }

  /**
   * {@code meta.relTypeProperties} walks the relationships of the nodes it samples, so it has always seen a lightweight type.
   */
  @Test
  void relTypePropertiesDescribesALightweightType() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE EDGE TYPE LW LIGHTWEIGHT");
    database.transaction(() -> database.command("opencypher", "CREATE (:A)-[:LW]->(:A)").close());

    final StringBuilder out = new StringBuilder();
    try (final ResultSet rs = database.query("opencypher", "CALL apoc.meta.relTypeProperties()")) {
      while (rs.hasNext())
        out.append(rs.next().toJSON()).append('\n');
    }
    assertThat(out.toString()).contains("LW");
  }

  private Result single(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      assertThat(rs.hasNext()).as(cypher).isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).as(cypher).isFalse();
      return row;
    }
  }

  private long countOf(final String cypher) {
    return longOf(single(cypher).getProperty("n"));
  }

  private static long longOf(final Object value) {
    return ((Number) value).longValue();
  }

  @SuppressWarnings("unchecked")
  private static List<Object> list(final Object value) {
    return (List<Object>) value;
  }

  private void bindUserDenying(final Set<String> deniedTypes) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : deniedTypes)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "scoped";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return !deniedTypes.contains(typeName);
      }

      @Override
      public long getResultSetLimit() {
        return -1;
      }

      @Override
      public long getReadTimeout() {
        return -1;
      }
    });
  }
}
