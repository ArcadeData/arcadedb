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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6374: a pattern comprehension whose leading node reuses an
 * outer-bound variable ignored any label or inline-property constraint declared on that leading
 * node, expanding the bound vertex's edges regardless. The sibling {@code exists(...)} pattern
 * predicate already enforces the same constraint on a bound start vertex (issue #5095), so the two
 * constructs disagreed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherComprehensionBoundStartLabelIssue6374Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-comprehension-bound-start-6374");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> database.command("opencypher", "CREATE (p:Person {name:'P'})-[:KNOWS]->(:Person {name:'F'})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void aLabelConstraintOnACorrelatedBoundStartIsEnforced() {
    // p is a Person, not a Company: the :Company constraint on the bound p must fail the whole
    // comprehension, matching what exists(...) already answers for the same pattern.
    assertThat(listOf("MATCH (p:Person {name:'P'}) RETURN [(p:Company)-[:KNOWS]->(x) | x.name] AS r")).isEmpty();
    assertThat(booleanOf("MATCH (p:Person {name:'P'}) RETURN exists((p:Company)-[:KNOWS]->()) AS r")).isFalse();
  }

  @Test
  void anInlinePropertyConstraintOnACorrelatedBoundStartIsEnforced() {
    assertThat(listOf("MATCH (p:Person {name:'P'}) RETURN [(p {name:'WRONG'})-[:KNOWS]->(x) | x.name] AS r")).isEmpty();
    assertThat(booleanOf("MATCH (p:Person {name:'P'}) RETURN exists((p {name:'WRONG'})-[:KNOWS]->()) AS r")).isFalse();
  }

  @Test
  void aMatchingLabelOnACorrelatedBoundStartStillWorks() {
    assertThat(listOf("MATCH (p:Person {name:'P'}) RETURN [(p:Person)-[:KNOWS]->(x) | x.name] AS r"))
        .containsExactly("F");
  }

  @Test
  void anInlineWhereOnACorrelatedBoundStartIsStillEnforced() {
    // The inline-WHERE check was already correct before the fix; pinned here so the restructuring
    // that folds it together with the label/property checks does not regress it.
    assertThat(listOf("MATCH (p:Person {name:'P'}) RETURN [(p WHERE p.name = 'WRONG')-[:KNOWS]->(x) | x.name] AS r")).isEmpty();
    assertThat(listOf("MATCH (p:Person {name:'P'}) RETURN [(p WHERE p.name = 'P')-[:KNOWS]->(x) | x.name] AS r"))
        .containsExactly("F");
  }

  @SuppressWarnings("unchecked")
  private List<Object> listOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return (List<Object>) rs.next().getProperty("r");
    }
  }

  private boolean booleanOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return (Boolean) rs.next().getProperty("r");
    }
  }
}
