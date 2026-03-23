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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3711:
 * Mandatory MATCH over existing relationship returns 0 rows,
 * while OPTIONAL MATCH and SQL can see the same edges.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherMandatoryMatchRelationshipTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Company");
    database.command("sql", "CREATE EDGE TYPE KNOWS UNIDIRECTIONAL");
    database.command("sql", "CREATE EDGE TYPE WORKS_FOR UNIDIRECTIONAL");

    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name: 'Alice', age: 30})" +
              "-[:KNOWS {since: 2020}]->" +
              "(b:Person {name: 'Bob', age: 35})" +
              "-[:KNOWS {since: 2021}]->" +
              "(c:Person {name: 'Charlie', age: 25})" +
              "-[:KNOWS {since: 2022}]->" +
              "(d:Person {name: 'David', age: 28})");

      database.command("opencypher", "CREATE (:Company {name: 'Acme'})");

      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Company {name: 'Acme'}) " +
              "CREATE (a)-[:WORKS_FOR]->(c), (b)-[:WORKS_FOR]->(c)");
    });
  }

  @Test
  void mandatoryMatchRelationshipShouldReturnResults() {
    // Issue #3711: mandatory MATCH with relationship should return results when edges exist
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) " +
            "RETURN p.name as person, c.name as company ORDER BY person")) {
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(2);
      assertThat(results.get(0).<String>getProperty("person")).isEqualTo("Alice");
      assertThat(results.get(0).<String>getProperty("company")).isEqualTo("Acme");
      assertThat(results.get(1).<String>getProperty("person")).isEqualTo("Bob");
      assertThat(results.get(1).<String>getProperty("company")).isEqualTo("Acme");
    }
  }

  @Test
  void sqlCanSeeEdges() {
    // Verify SQL can see the WORKS_FOR edges
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM WORKS_FOR")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(2);
    }
  }

  @Test
  void optionalMatchCanSeeEdges() {
    // Verify OPTIONAL MATCH can see the WORKS_FOR edges
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) " +
            "OPTIONAL MATCH (p)-[:WORKS_FOR]->(c:Company) " +
            "RETURN p.name as name, c.name as company ORDER BY name")) {
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(4);
      // Alice -> Acme
      assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Alice");
      assertThat(results.get(0).<String>getProperty("company")).isEqualTo("Acme");
      // Bob -> Acme
      assertThat(results.get(1).<String>getProperty("name")).isEqualTo("Bob");
      assertThat(results.get(1).<String>getProperty("company")).isEqualTo("Acme");
      // Charlie -> null
      assertThat(results.get(2).<String>getProperty("name")).isEqualTo("Charlie");
      assertThat((Object) results.get(2).getProperty("company")).isNull();
      // David -> null
      assertThat(results.get(3).<String>getProperty("name")).isEqualTo("David");
      assertThat((Object) results.get(3).getProperty("company")).isNull();
    }
  }
}
