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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12743: a SQL MATCH whose root node has no {@code type:} (and no {@code rid:}) failed with a bare
 * {@link UnsupportedOperationException} from the root SELECT, which had no target. The root now walks every vertex, as an
 * unlabelled openCypher {@code MATCH (n)} does: each vertex once (a subtype, even one with two vertex supertypes, is not
 * visited twice), and no document or edge record.
 */
class Issue12743MatchRootWithoutTypeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("MA");
      database.getSchema().createVertexType("MB").addSuperType("MA");
      database.getSchema().createVertexType("MC");
      // TWO VERTEX SUPERTYPES: A POLYMORPHIC SCAN OF EACH WOULD VISIT ITS VERTICES TWICE
      database.getSchema().createVertexType("MD").addSuperType("MB").addSuperType("MC");
      database.getSchema().getType("MA").createProperty("k", Integer.class);
      database.getSchema().getType("MA").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");
      database.getSchema().createEdgeType("MR");
      database.getSchema().createEdgeType("MS");
      database.getSchema().createDocumentType("Doc");

      database.command("opencypher", """
          CREATE (a:MA {id: 1, k: 1}), (b:MB {id: 2, k: 1}), (c:MC {id: 3, k: 2}), (d:MD {id: 4, k: 1}), (:MC {id: 5})
          CREATE (a)-[:MR]->(b), (b)-[:MR]->(c), (c)-[:MS]->(a), (d)-[:MR]->(a)""");
      database.command("sql", "INSERT INTO Doc SET id = 6, k = 1");
    });
  }

  @Test
  void rootWithoutTypeCountsEveryVertex() {
    assertThat(count("MATCH {as: n0} RETURN count(*) AS c")).isEqualTo(5L).isEqualTo(cypherCount("MATCH (n) RETURN count(*) AS c"));
    assertThat(ids("MATCH {as: n0} RETURN n0.id AS id ORDER BY id")).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  void rootWithoutTypeFollowsEdges() {
    assertThat(pairs("MATCH {as: n0}-MR-{as: n1} RETURN n0.id AS a, n1.id AS b ORDER BY a, b"))
        .isEqualTo(cypherPairs("MATCH (n0)-[:MR]-(n1) RETURN n0.id AS a, n1.id AS b ORDER BY a, b"));
    assertThat(count("MATCH {as: n0}-->{as: n1} RETURN count(*) AS c")).isEqualTo(4L)
        .isEqualTo(cypherCount("MATCH (n0)-->(n1) RETURN count(*) AS c"));
    assertThat(count("MATCH {as: n0}.out('MR'){as: n1} RETURN count(*) AS c")).isEqualTo(3L)
        .isEqualTo(cypherCount("MATCH (n0)-[:MR]->(n1) RETURN count(*) AS c"));
  }

  @Test
  void rootWithoutTypeAppliesItsWhere() {
    // THE DOCUMENT HAS k = 1 TOO, AND MUST NOT BE A CANDIDATE
    assertThat(ids("MATCH {as: n0, where: (k = 1)} RETURN n0.id AS id ORDER BY id")).containsExactly(1, 2, 4);
    assertThat(count("MATCH {as: n0, where: (k = 1)}-MR->{as: n1} RETURN count(*) AS c")).isEqualTo(3L)
        .isEqualTo(cypherCount("MATCH (n0)-[:MR]->(n1) WHERE n0.k = 1 RETURN count(*) AS c"));
  }

  @Test
  void rootWithoutTypeAnswersNothingWithoutVertexTypes() {
    database.transaction(() -> {
      for (final String type : new String[] { "MR", "MS", "MD", "MB", "MA", "MC" })
        database.command("sql", "DROP TYPE " + type + " UNSAFE");
    });
    assertThat(ids("MATCH {as: n0} RETURN n0.id AS id")).isEmpty();
  }

  @Test
  void rootWithoutTypeSeesAVertexTypeCreatedAfterTheFirstRun() {
    assertThat(count("MATCH {as: n0} RETURN count(*) AS c")).isEqualTo(5L);
    database.transaction(() -> {
      database.getSchema().createVertexType("ME");
      database.command("sql", "CREATE VERTEX ME SET id = 7");
    });
    assertThat(count("MATCH {as: n0} RETURN count(*) AS c")).isEqualTo(6L);
  }

  private long count(final String match) {
    try (final ResultSet rs = database.query("sql", match)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long cypherCount(final String match) {
    try (final ResultSet rs = database.query("opencypher", match)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private List<Integer> ids(final String match) {
    final List<Integer> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", match)) {
      while (rs.hasNext())
        result.add(((Number) rs.next().getProperty("id")).intValue());
    }
    return result;
  }

  private List<String> pairs(final String match) {
    return pairs("sql", match);
  }

  private List<String> cypherPairs(final String match) {
    return pairs("opencypher", match);
  }

  private List<String> pairs(final String language, final String match) {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query(language, match)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.add(row.getProperty("a") + "-" + row.getProperty("b"));
      }
    }
    return result;
  }
}
