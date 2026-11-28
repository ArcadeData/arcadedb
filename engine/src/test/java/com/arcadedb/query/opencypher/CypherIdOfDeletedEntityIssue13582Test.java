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
import com.arcadedb.exception.DeletedEntityException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * id() and elementId() of a node or relationship deleted earlier in the same statement answer the id the entity had, as
 * Neo4j does, while its labels and properties stay inaccessible (issue #13582).
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherIdOfDeletedEntityIssue13582Test extends TestHelper {

  @BeforeEach
  void createGraph() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {k: 1})-[:R {w: 1}]->(:B {k: 2})"));
  }

  private List<Result> run(final String cypher) {
    return run(cypher, Map.of());
  }

  private List<Result> run(final String cypher, final Map<String, Object> params) {
    final List<Result> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher, params)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
    });
    return rows;
  }

  @Test
  void idOfDeletedRelationshipIsTheIdItHad() {
    final List<Result> rows = run("""
        MATCH ()-[r:R]->() WITH r, id(r) AS before, elementId(r) AS eBefore DELETE r
        RETURN id(r) = before AS sameId, id(r) IS NULL AS idIsNull, elementId(r) = eBefore AS sameEid,
               elementId(r) IS NULL AS eidIsNull, type(r) AS t""");
    assertThat(rows).hasSize(1);
    final Result row = rows.getFirst();
    assertThat(row.<Boolean>getProperty("sameId")).isTrue();
    assertThat(row.<Boolean>getProperty("idIsNull")).isFalse();
    assertThat(row.<Boolean>getProperty("sameEid")).isTrue();
    assertThat(row.<Boolean>getProperty("eidIsNull")).isFalse();
    assertThat(row.<String>getProperty("t")).isEqualTo("R");
  }

  @Test
  void idOfDeletedNodeIsTheIdItHad() {
    final List<Result> rows = run("""
        MATCH (a:A) WITH a, id(a) AS before, elementId(a) AS eBefore DETACH DELETE a
        RETURN id(a) = before AS sameId, id(a) IS NULL AS idIsNull, elementId(a) = eBefore AS sameEid""");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Boolean>getProperty("sameId")).isTrue();
    assertThat(rows.getFirst().<Boolean>getProperty("idIsNull")).isFalse();
    assertThat(rows.getFirst().<Boolean>getProperty("sameEid")).isTrue();
  }

  @Test
  void eachDeletedNodeKeepsItsOwnId() {
    run("CREATE (:A {k: 10}), (:A {k: 11})");
    final List<Result> rows = run("MATCH (a:A) WITH a, id(a) AS before DETACH DELETE a RETURN before, id(a) AS after");
    assertThat(rows).hasSize(3);
    for (final Result row : rows)
      assertThat(row.<Object>getProperty("after")).isEqualTo(row.getProperty("before"));
    assertThat(rows.stream().map(r -> r.getProperty("after")).distinct().count()).isEqualTo(3);
  }

  /** The statement Neo4j-OGM sends to delete a relationship entity, which registers the ids it returns. */
  @Test
  void neo4jOgmRelationshipDeleteReturnsTheIds() {
    final long relId = ((Number) run("MATCH ()-[r:R]->() RETURN id(r) AS id").getFirst().getProperty("id")).longValue();
    final List<Result> rows = run("""
            UNWIND $rows AS row MATCH ()-[r]->() WHERE ID(r) = row.relId DELETE r RETURN ID(r) as ref, ID(r) as id, $type as type""",
        Map.of("rows", List.of(Map.of("relId", relId)), "type", "rel"));
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("ref")).longValue()).isEqualTo(relId);
    assertThat(((Number) rows.getFirst().getProperty("id")).longValue()).isEqualTo(relId);
  }

  /** The statement Neo4j-OGM sends to delete a node together with its relationships. */
  @Test
  void neo4jOgmNodeDeleteReturnsTheId() {
    final long nodeId = ((Number) run("MATCH (n:A) RETURN id(n) AS id").getFirst().getProperty("id")).longValue();
    final List<Result> rows = run("MATCH (n) WHERE id(n) = $id OPTIONAL MATCH (n)-[r0]-() DELETE r0, n RETURN ID(n)",
        Map.of("id", nodeId));
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("ID(n)")).longValue()).isEqualTo(nodeId);
  }

  @Test
  void deletedEntityHeldInAnotherAliasOrListKeepsItsId() {
    final List<Result> rows = run("""
        MATCH (a:A)-[r:R]->(b:B) WITH a, b, r, b AS other, [r] AS rels, id(b) AS bId, id(r) AS rId
        DELETE r, b
        RETURN id(other) = bId AS otherSame, id(rels[0]) = rId AS listSame, elementId(other) IS NULL AS otherEidNull""");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Boolean>getProperty("otherSame")).isTrue();
    assertThat(rows.getFirst().<Boolean>getProperty("listSame")).isTrue();
    assertThat(rows.getFirst().<Boolean>getProperty("otherEidNull")).isFalse();
  }

  @Test
  void deletedEntitiesOfAPathKeepTheirIds() {
    final List<Result> rows = run("""
        MATCH p = (a:A)-[r:R]->(b:B) WITH p, r, b, id(r) AS rId, id(b) AS bId
        DELETE r, b
        RETURN [x IN relationships(p) | id(x)] AS rels, [x IN nodes(p) | id(x)] AS nodes, rId, bId""");
    assertThat(rows).hasSize(1);
    final Result row = rows.getFirst();
    assertThat(row.<List<Object>>getProperty("rels")).containsExactly(row.<Object>getProperty("rId"));
    assertThat(row.<List<Object>>getProperty("nodes")).hasSize(2).last().isEqualTo(row.getProperty("bId"));
  }

  @Test
  void labelsAndPropertiesOfDeletedEntitiesStayInaccessible() {
    assertThatThrownBy(() -> run("MATCH (b:B) DETACH DELETE b RETURN labels(b)")).isInstanceOf(DeletedEntityException.class);
    assertThatThrownBy(() -> run("MATCH (b:B) DETACH DELETE b RETURN properties(b)")).isInstanceOf(DeletedEntityException.class);
    assertThatThrownBy(() -> run("MATCH ()-[r:R]->() DELETE r RETURN r.w")).isInstanceOf(DeletedEntityException.class);
    assertThatThrownBy(() -> run("MATCH ()-[r:R]->() DELETE r RETURN keys(r)")).isInstanceOf(DeletedEntityException.class);
  }

  @Test
  void namespacedIdFunctionsOfDeletedEntitiesAnswerTheRidTheyHad() {
    final List<Result> rows = run("""
        MATCH (a:A)-[r:R]->() WITH a, r, node.id(a) AS aBefore, rel.id(r) AS rBefore DETACH DELETE a
        RETURN node.id(a) = aBefore AS nodeSame, rel.id(r) = rBefore AS relSame""");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Boolean>getProperty("nodeSame")).isTrue();
    assertThat(rows.getFirst().<Boolean>getProperty("relSame")).isTrue();
  }
}
