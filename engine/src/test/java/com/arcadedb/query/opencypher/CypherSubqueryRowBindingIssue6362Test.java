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
 * Issue #6362: inserting an identity {@code CALL (*) { RETURN 0 AS barrier }} after a
 * {@code MERGE ... ON MATCH SET} changed the rows the query returned - {@code [{i:1, k4:10}, {i:2, k4:99}]}
 * became {@code [{i:1, k4:99}, {i:2, k4:99}]}, so a write performed for the second input row showed through the
 * binding the first row was carrying.
 * <p>
 * The cause is {@code SubqueryStep.refreshDocumentBindings}, added for issue #4182 so that a {@code SET} inside
 * the subquery is visible to the clauses after it. It re-reads every {@code Document} binding on the outer row
 * through {@code lookupByRID}, which answers from the transaction cache - so the snapshot the row was carrying
 * is replaced by the live, shared record instance, and every later mutation of that record is then visible
 * through a row that had already passed the subquery.
 * <p>
 * A read-only subquery cannot have invalidated any binding, so for it the refresh is pure damage and is now
 * skipped. The writing case it was added for keeps it, which is what
 * {@link #aWritingSubqueryStillRefreshesTheOuterBinding()} holds it to.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSubqueryRowBindingIssue6362Test {
  private Database database;

  /**
   * The reporter's query up to the {@code MERGE}: two input rows over one {@code n0}, whose {@code k4} the
   * {@code MERGE} sets on the second row only - the first row creates the pattern, so {@code ON MATCH} does not
   * fire for it.
   */
  private static final String HEAD = """
      UNWIND [1, 2] AS i \
      MATCH (n0 {id: 1})<-[:rt4]-(m {id: 114}), (n1 {id: 126})-[:rt2]->(x)-[:rt5]->(n0) \
      MERGE (n0)-[:rt1 {id: 42}]->(n2 {id: 2})<-[:rt11 {id: 43}]-(n0) \
      ON MATCH SET n0.k4 = 99\s""";

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-subquery-row-binding-6362");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    buildFixture();
  }

  /**
   * Rebuilds the reported four-node, three-relationship graph from nothing.
   * <p>
   * Called before every query rather than once per test: the {@code MERGE} under test is what creates the
   * pattern {@code ON MATCH} then fires on, so a second run over the same database would find it already there
   * and set {@code k4} on the very first input row - which is the answer this test exists to distinguish from.
   */
  private void buildFixture() {
    database.transaction(() -> {
      database.command("opencypher", "MATCH (n) DETACH DELETE n");
      database.command("opencypher", "CREATE (n0 {id: 1, k4: 10}), (m {id: 114}), (n1 {id: 126}), (x {id: 115})");
      database.command("opencypher", """
          MATCH (m {id: 114}), (n0 {id: 1}), (n1 {id: 126}), (x {id: 115}) \
          CREATE (m)-[:rt4]->(n0), (n1)-[:rt2]->(x)-[:rt5]->(n0)""");
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
  void anIdentitySubqueryDoesNotChangeTheRows() {
    final List<String> withoutSubquery = onFreshFixture(HEAD + "RETURN i, n0.k4 AS k4");
    final List<String> withSubquery = onFreshFixture(HEAD + """
        WITH i, n0, n2 \
        CALL (*) { RETURN 0 AS barrier } \
        WITH i, n0, n2 \
        RETURN i, n0.k4 AS k4""");

    assertThat(withoutSubquery).containsExactly("1|10", "2|99");
    assertThat(withSubquery)
        .as("an identity CALL (*) subquery must not change what the query returns")
        .isEqualTo(withoutSubquery);
  }

  @Test
  void theImplicitScopeSpellingBehavesTheSameWay() {
    assertThat(onFreshFixture(HEAD + """
        WITH i, n0, n2 \
        CALL { RETURN 0 AS barrier } \
        WITH i, n0, n2 \
        RETURN i, n0.k4 AS k4""")).containsExactly("1|10", "2|99");
  }

  @Test
  void thePlainControlsAreUnchanged() {
    // A plain WITH, and an UNWIND barrier, both already preserved the streaming answer; they are the reference
    // the subquery spelling has to match.
    assertThat(onFreshFixture(HEAD + "WITH i, n0, n2 RETURN i, n0.k4 AS k4")).containsExactly("1|10", "2|99");
    assertThat(onFreshFixture(HEAD + "WITH i, n0, n2 UNWIND [0] AS b WITH i, n0, n2 RETURN i, n0.k4 AS k4"))
        .containsExactly("1|10", "2|99");
  }

  @Test
  void theWriteItselfStillLands() {
    // The difference under test is a row-binding one, not a persistence one: after the query the record holds 99
    // either way.
    onFreshFixture(HEAD + "WITH i, n0, n2 CALL (*) { RETURN 0 AS barrier } WITH i, n0, n2 RETURN i, n0.k4 AS k4");
    assertThat(command("MATCH (n {id: 1}) RETURN n.id AS i, n.k4 AS k4")).containsExactly("1|99");
  }

  @Test
  void aWritingSubqueryStillRefreshesTheOuterBinding() {
    // Issue #4182, which is why the refresh exists: a SET performed inside the subquery must be visible to the
    // clauses after it through the imported variable.
    assertThat(command("""
        MATCH (n {id: 1}) \
        CALL (*) { WITH n SET n.k4 = 777 RETURN 0 AS ignored } \
        RETURN n.id AS i, n.k4 AS k4""")).containsExactly("1|777");
  }

  // ---------------------------------------------------------------------------------------------------------

  /** Rebuilds the fixture, then runs the query and flattens its rows. */
  private List<String> onFreshFixture(final String query) {
    buildFixture();
    return command(query);
  }

  private List<String> command(final String query) {
    final List<String> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet resultSet = database.command("opencypher", query)) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          final StringBuilder flattened = new StringBuilder();
          for (final String property : row.getPropertyNames()) {
            if (!flattened.isEmpty())
              flattened.append('|');
            flattened.append(String.valueOf((Object) row.getProperty(property)));
          }
          rows.add(flattened.toString());
        }
      }
    });
    return rows;
  }
}
