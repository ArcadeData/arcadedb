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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the two small items of issue #6363: a label disjunction anchor was costed from its first
 * alternative alone while the operator scanned every type any alternative accepted, and {@link Schema} had no
 * non-throwing type accessor, so every caller that could tolerate an absent type probed the same map twice.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDisjunctionCardinalityIssue6363Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-disjunction-cardinality-6363");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      // 2 :Author, 3 :Topic, and 4 :Reader vertices - three counts no pair of which sums to a third.
      for (int i = 0; i < 2; i++)
        database.command("opencypher", "CREATE (:Author {k:$k})", Map.of("k", "a" + i));
      for (int i = 0; i < 3; i++)
        database.command("opencypher", "CREATE (:Topic {k:$k})", Map.of("k", "t" + i));
      for (int i = 0; i < 4; i++)
        database.command("opencypher", "CREATE (:Reader {k:$k})", Map.of("k", "r" + i));
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void aDisjunctionAnchorIsCostedOverEveryTypeItScans() {
    // Was rows=2, the :Author count alone, for a scan that returns 5 rows.
    assertThat(explain("MATCH (n:Author|Topic) RETURN n.k AS k"))
        .contains("NodeByLabelDisjunctionScan(n:Author|Topic)")
        .contains("rows=5");
    // Order of the alternatives cannot change the estimate any more.
    assertThat(explain("MATCH (n:Topic|Author) RETURN n.k AS k")).contains("rows=5");
    assertThat(explain("MATCH (n:Author|Topic|Reader) RETURN n.k AS k")).contains("rows=9");
  }

  @Test
  void anAlternativeTheSchemaDoesNotHaveContributesNothingToTheEstimate() {
    assertThat(explain("MATCH (n:Author|NoSuchLabel) RETURN n.k AS k")).contains("rows=2");
    assertThat(explain("MATCH (n:NoSuchLabel|NorThisOne) RETURN n.k AS k")).contains("rows=0");
  }

  @Test
  void aSubtypeIsCountedOnceAndOnlyThroughTheAlternativesThatAcceptIt() {
    database.command("sql", "CREATE VERTEX TYPE Special EXTENDS Author, Topic");
    database.transaction(() -> database.command("sql", "INSERT INTO Special SET k = 'sp1'"));

    // Special answers to both alternatives; it must be added once, not once per ancestor that accepts it.
    assertThat(explain("MATCH (n:Author|Topic) RETURN n.k AS k")).contains("rows=6");
  }

  @Test
  void theEstimateFollowsTheDataItDoesNotStayAtAPlanningTimeGuess() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Topic {k:'t9'})"));
    assertThat(explain("MATCH (n:Author|Topic) RETURN n.k AS k")).contains("rows=6");
  }

  @Test
  void schemaAnswersForAnAbsentTypeWithoutThrowing() {
    final Schema schema = database.getSchema();

    final DocumentType author = schema.getTypeOrNull("Author");
    assertThat(author).isNotNull();
    assertThat(author.getName()).isEqualTo("Author");
    assertThat(author).isSameAs(schema.getType("Author"));

    assertThat(schema.getTypeOrNull("NoSuchLabel")).isNull();
    assertThatThrownBy(() -> schema.getType("NoSuchLabel")).hasMessageContaining("NoSuchLabel");
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
