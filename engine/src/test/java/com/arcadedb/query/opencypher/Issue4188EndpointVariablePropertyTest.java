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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4188.
 * <p>
 * A relationship pattern whose endpoint node carries a property map with a variable value
 * (e.g. {@code (:Person {name: fName})} where {@code fName} comes from a preceding WITH)
 * previously matched zero rows. The property map was compared structurally without resolving
 * the bound variable from the current row.
 */
class Issue4188EndpointVariablePropertyTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-4188-endpoint-variable-property").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FRIEND");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (a)-[:FRIEND]->(b)");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void endpointPropertyBoundFromWithVariable() {
    final ResultSet rs = database.query("opencypher",
        """
            WITH 'Bob' AS fName
            MATCH (p:Person)-[:FRIEND]->(:Person {name: fName})
            RETURN p.name AS person
            ORDER BY person""");

    final List<String> people = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      people.add(r.getProperty("person"));
    }
    assertThat(people).containsExactly("Alice");
  }

  /**
   * Control: same query but with the constraint moved into WHERE - already worked.
   */
  @Test
  void endpointPropertyInWhereStillWorks() {
    final ResultSet rs = database.query("opencypher",
        """
            WITH 'Bob' AS fName
            MATCH (p:Person)-[:FRIEND]->(f:Person)
            WHERE f.name = fName
            RETURN p.name AS person""");

    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    assertThat(r.<String>getProperty("person")).isEqualTo("Alice");
  }

  /**
   * Control: literal endpoint property keeps working after the fix.
   */
  @Test
  void endpointPropertyLiteralStillWorks() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[:FRIEND]->(:Person {name:'Bob'}) RETURN p.name AS person");

    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    assertThat(r.<String>getProperty("person")).isEqualTo("Alice");
  }

  /**
   * Variant: the bound value can also come from a query parameter ($name). This exercises
   * ParameterReference resolution adjacent to the new Expression evaluation path.
   */
  @Test
  void endpointPropertyBoundFromParameter() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[:FRIEND]->(:Person {name: $fName}) RETURN p.name AS person",
        java.util.Map.of("fName", "Bob"));

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("person")).isEqualTo("Alice");
  }

  /**
   * Variant: the bound variable can also come from an UNWIND.
   */
  @Test
  void endpointPropertyBoundFromUnwindVariable() {
    final ResultSet rs = database.query("opencypher",
        """
            UNWIND ['Bob','Alice'] AS fName
            MATCH (p:Person)-[:FRIEND]->(:Person {name: fName})
            RETURN p.name AS person""");

    final List<String> people = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      people.add(r.getProperty("person"));
    }
    assertThat(people).containsExactly("Alice");
  }
}
