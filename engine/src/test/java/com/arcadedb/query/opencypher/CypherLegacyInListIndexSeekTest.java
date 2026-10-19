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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code WHERE n.prop IN <list>} on an indexed property seeks the index on the legacy MATCH path too, which every query
 * with an OPTIONAL MATCH takes, instead of scanning the type and comparing each record with every element. Each query is
 * checked against the same predicate on {@code idCopy}, an unindexed copy of {@code id}, so the scan defines the answer.
 */
class CypherLegacyInListIndexSeekTest extends TestHelper {
  private static final String OPTIONAL_TAIL = " OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) RETURN p.id AS id";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var person = database.getSchema().createVertexType("Person");
      person.createProperty("id", Integer.class);
      person.createProperty("idCopy", Integer.class);
      person.createProperty("code", String.class);
      person.createProperty("codeCopy", String.class);
      person.createProperty("name", String.class);
      person.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      person.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "code");
      database.getSchema().createEdgeType("KNOWS");
    });

    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.command("opencypher",
            "CREATE (p:Person {id: $i, idCopy: $i, code: $c, codeCopy: $c, name: $n})",
            Map.of("i", i, "c", "c" + (i % 10), "n", "Person" + i));
      database.command("opencypher", "MATCH (a:Person), (b:Person) WHERE b.id = a.id + 1 CREATE (a)-[:KNOWS]->(b)");
    });
  }

  @Test
  void parameterListSeeksTheIndexAndAnswersAsTheScan() {
    final Map<String, Object> params = new HashMap<>();
    params.put("ids", Arrays.asList(3, 7, 7, 42, null, 999));

    final String seek = "MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL;
    final String scan = "MATCH (p:Person) WHERE p.idCopy IN $ids" + OPTIONAL_TAIL;

    assertThat(ids(seek, params)).containsExactly(3, 7, 42).isEqualTo(ids(scan, params));
    assertThat(plan(seek, params)).contains("[index: Person[id]]");
    assertThat(plan(scan, params)).doesNotContain("[index:");
  }

  @Test
  void listLiteralSeeksTheIndex() {
    final String seek = "MATCH (p:Person) WHERE p.id IN [1, 2, 3]" + OPTIONAL_TAIL;

    assertThat(ids(seek, null)).containsExactly(1, 2, 3);
    assertThat(plan(seek, null)).contains("[index: Person[id]]");
  }

  @Test
  void emptyOrAllNullListMatchesNothing() {
    final Map<String, Object> params = new HashMap<>();
    params.put("ids", Collections.singletonList(null));

    assertThat(ids("MATCH (p:Person) WHERE p.id IN []" + OPTIONAL_TAIL, null)).isEmpty();
    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL, params)).isEmpty();
  }

  @Test
  void otherConjunctsStillFilterTheSeek() {
    final Map<String, Object> params = Map.of("ids", List.of(2, 3, 4));

    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids AND p.name <> 'Person3'" + OPTIONAL_TAIL, params))
        .containsExactly(2, 4);
  }

  @Test
  void disjunctionIsNotSought() {
    final String query = "MATCH (p:Person) WHERE p.id IN [1, 2] OR p.name = 'Person5'" + OPTIONAL_TAIL;

    assertThat(ids(query, null)).containsExactly(1, 2, 5);
    assertThat(plan(query, null)).doesNotContain("[index:");
  }

  @Test
  void negatedInIsNotSought() {
    final String query = "MATCH (p:Person) WHERE NOT p.id IN [1, 2] AND p.id < 5" + OPTIONAL_TAIL;

    assertThat(ids(query, null)).containsExactly(0, 3, 4);
    assertThat(plan(query, null)).doesNotContain("[index:");
  }

  @Test
  void rowDependentListIsSoughtPerRow() {
    final String seek = "UNWIND [[1, 2], [3], [2, 4], []] AS ids MATCH (p:Person) WHERE p.id IN ids" + OPTIONAL_TAIL;
    final String scan = "UNWIND [[1, 2], [3], [2, 4], []] AS ids MATCH (p:Person) WHERE p.idCopy IN ids" + OPTIONAL_TAIL;

    assertThat(ids(seek, null)).containsExactly(1, 2, 2, 3, 4).isEqualTo(ids(scan, null));
  }

  @Test
  void textAgainstIntegerAnswersAsTheScan() {
    final Map<String, Object> params = Map.of("ids", List.of("5", 6, "x"));

    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL, params))
        .isEqualTo(ids("MATCH (p:Person) WHERE p.idCopy IN $ids" + OPTIONAL_TAIL, params));
  }

  @Test
  void nonUniqueIndexReturnsEveryRecordOfEveryKey() {
    final Map<String, Object> params = Map.of("codes", List.of("c1", "c2", "c1"));
    final String seek = "MATCH (p:Person) WHERE p.code IN $codes" + OPTIONAL_TAIL;

    assertThat(ids(seek, params)).hasSize(10)
        .isEqualTo(ids("MATCH (p:Person) WHERE p.codeCopy IN $codes" + OPTIONAL_TAIL, params));
    assertThat(plan(seek, params)).contains("[index: Person[code]]");
  }

  private List<Integer> ids(final String cypher, final Map<String, Object> params) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = params != null ? database.query("opencypher", cypher, params) : database.query("opencypher", cypher)) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    Collections.sort(ids);
    return ids;
  }

  private String plan(final String cypher, final Map<String, Object> params) {
    try (final ResultSet rs = params != null ?
        database.query("opencypher", "PROFILE " + cypher, params) :
        database.query("opencypher", "PROFILE " + cypher)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
