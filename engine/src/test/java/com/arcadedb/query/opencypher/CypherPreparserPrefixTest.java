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
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #13556: Neo4j's {@code CYPHER <version> [option=value ...]} preparser prefix, which
 * {@code @neo4j/graphql} puts before every statement, was a syntax error. It selects nothing in ArcadeDB and is dropped.
 */
class CypherPreparserPrefixTest extends TestHelper {
  private static final String[] PREFIXES = { "CYPHER 5\n", "CYPHER 5 ", "cypher 25\n", "CYPHER 5 runtime=slotted\n",
      "CYPHER planner=cost runtime=interpreted\n", "  CYPHER 5\n\n", "// c\nCYPHER 5\n", "CYPHER 5\n// c\n", "CYPHER\n5\n",
      "CYPHER 5\nCYPHER runtime=interpreted\n", "CYPHER runtime=slotted CYPHER 25 " };

  @Test
  void prefixedQueryAndCommandRunAsTheStatementItPrefixes() {
    database.getSchema().createVertexType("Person");
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {name: 'a'})").close());

    for (final String prefix : PREFIXES) {
      try (final ResultSet rs = database.query("opencypher", prefix + "MATCH (p:Person) RETURN p.name AS name")) {
        assertThat(rs.next().<String>getProperty("name")).as(prefix).isEqualTo("a");
      }
      try (final ResultSet rs = database.command("opencypher", prefix + "UNWIND [1, 2] AS i RETURN i ORDER BY i")) {
        assertThat(rs.next().<Number>getProperty("i").intValue()).as(prefix).isEqualTo(1);
        assertThat(rs.next().<Number>getProperty("i").intValue()).as(prefix).isEqualTo(2);
      }
    }
  }

  @Test
  void prefixedWriteAndSchemaStatementsRun() {
    database.getSchema().createVertexType("Movie");
    database.transaction(() -> database.command("opencypher", "CYPHER 5\nCREATE (:Movie {id: 1})").close());
    try (final ResultSet rs = database.query("opencypher", "CYPHER 5 MATCH (m:Movie) RETURN count(m) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(1);
    }
    database.command("opencypher", "CYPHER 5\nCREATE INDEX IF NOT EXISTS FOR (m:Movie) ON (m.id)").close();
    assertThat(database.getSchema().getType("Movie").getAllIndexes(true)).isNotEmpty();
  }

  @Test
  void explainAndProfileFollowThePrefix() {
    database.getSchema().createVertexType("Person");
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {name: 'a'})").close());
    try (final ResultSet rs = database.command("opencypher", "CYPHER 5\nEXPLAIN MATCH (p:Person) RETURN p.name")) {
      assertThat(rs.next().<Object>getProperty("executionPlan")).isNotNull();
    }
    // @neo4j/graphql's TCK check sends EXPLAIN in front of the statement it generated, prefix and all
    try (final ResultSet rs = database.command("opencypher", "EXPLAIN CYPHER 5\nMATCH (p:Person) RETURN p.name")) {
      assertThat(rs.next().<Object>getProperty("executionPlan")).isNotNull();
    }
    try (final ResultSet rs = database.command("opencypher", "CYPHER 5\nPROFILE MATCH (p:Person) RETURN p.name AS name")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("a");
    }
  }

  @Test
  void prefixIsRecognisedOnlyAsAWholeLeadingWord() {
    final String plain = "MATCH (n) RETURN n";
    assertThat(OpenCypherQueryEngine.stripCypherPreparser(plain)).isSameAs(plain);
    assertThat(OpenCypherQueryEngine.stripCypherPreparser("CYPHERX 5 RETURN 1")).isEqualTo("CYPHERX 5 RETURN 1");
    assertThat(OpenCypherQueryEngine.stripCypherPreparser("CYPHER")).isEqualTo("CYPHER");
    assertThat(OpenCypherQueryEngine.stripCypherPreparser("CYPHER 5\nRETURN 1")).isEqualTo("RETURN 1");
    assertThat(OpenCypherQueryEngine.stripCypherPreparser("CYPHER 25 runtime=slotted RETURN 1")).isEqualTo("RETURN 1");
    assertThat(OpenCypherQueryEngine.stripCypherPreparser("CYPHER 5\nCYPHER runtime=interpreted\nRETURN 1")).isEqualTo("RETURN 1");
    assertThat(OpenCypherQueryEngine.stripCypherPreparser(null)).isNull();
  }
}
