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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11585: when the first part of a MATCH is an anonymous relationship with an indexed inline property,
 * {@code MATCH EDGE BY INDEX} answered it without binding the relationship to anything, so a later part of the same MATCH
 * could bind the very same relationship again. Every relationship of one MATCH binds a different relationship, comma
 * separated parts included.
 */
class CypherEdgeIndexSeedRelationshipUniquenessIssue11585Test extends TestHelper {

  private static final String[] QUERIES = {
      "MATCH (s)-[:REL {k: 2}]->(o), (s)-[]->(o2) RETURN count(*) AS c",
      "MATCH (s:Entity)-[:REL {k: 2}]->(o:Entity), (s)-[:REL]->(o2:Entity) RETURN count(*) AS c",
      "MATCH ()-[:REL {k: 2}]->(o), ()-[:REL]->(o) RETURN count(*) AS c",
      "MATCH (s)-[r:REL {k: 2}]->(o), (s)-[]->(o2) RETURN count(*) AS c" };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Entity");
    database.command("sql", "CREATE EDGE TYPE REL");
    database.command("sql", "CREATE PROPERTY REL.k INTEGER");
    database.transaction(() -> database.command("opencypher", "CREATE (:Entity {id: 1})-[:REL {k: 2}]->(:Entity {id: 2})"));
  }

  @Test
  void theSeededRelationshipIsNotBoundAgainByALaterPart() {
    for (final String query : QUERIES)
      assertThat(count(query)).as("no index: %s", query).isZero();

    database.command("sql", "CREATE INDEX ON REL (k) NOTUNIQUE");

    for (final String query : QUERIES) {
      assertThat(plan(query)).as(query).contains("MATCH EDGE BY INDEX");
      assertThat(count(query)).as("index: %s", query).isZero();
    }
  }

  @Test
  void aSecondRelationshipStillMatches() {
    database.command("sql", "CREATE INDEX ON REL (k) NOTUNIQUE");
    database.transaction(() -> database.command("opencypher",
        "MATCH (s:Entity {id: 1}) CREATE (s)-[:REL {k: 7}]->(:Entity {id: 3})"));

    assertThat(count("MATCH (s)-[:REL {k: 2}]->(o), (s)-[]->(o2) RETURN count(*) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (s)-[:REL {k: 2}]->(o), (s)-[:REL]->(o2) RETURN o2.id AS c")).isEqualTo(3L);
  }

  @Test
  void aSeparateMatchClauseMayRebindTheSeededRelationship() {
    database.command("sql", "CREATE INDEX ON REL (k) NOTUNIQUE");
    final String query = "MATCH (s)-[:REL {k: 2}]->(o) MATCH (s)-[]->(o2) RETURN count(*) AS c";

    assertThat(plan(query)).contains("MATCH EDGE BY INDEX");
    assertThat(count(query)).isEqualTo(1L);
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
