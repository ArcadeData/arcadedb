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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10440: a Cypher pattern naming a TIMESERIES type used to match nothing, so {@code MATCH (e:Ev) RETURN count(e)}
 * answered 0 over a series SQL counts 2 samples in. Time-series samples are not graph nodes or relationships, so the
 * pattern now fails with an error naming the type instead of reporting that there is no data.
 */
class CypherTimeSeriesLabelIssue10440Test {
  private static final String MESSAGE = "'Ev', a TIMESERIES type";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-timeseries-label-10440");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sqlscript", """
        CREATE TIMESERIES TYPE Ev TIMESTAMP ts PRECISION MILLISECOND TAGS (k STRING) FIELDS (v DOUBLE);
        INSERT INTO Ev SET ts = 1792396800000, k = 'a', v = 1.0;
        INSERT INTO Ev SET ts = 1792396860000, k = 'a', v = 2.0;
        """);
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> database.command("cypher", "CREATE (:P {name: 'x'})-[:R]->(:P {name: 'y'})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void sqlStillSeesTheSamples() {
    // the rows themselves, not count(*), whose cached counter is a separate path
    assertThat(drain(database.query("sql", "SELECT FROM Ev"))).hasSize(2);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // the queries of the issue: the count push-down and the plain scan
      "MATCH (e:Ev) RETURN count(e) AS n",
      "MATCH (e:Ev) RETURN e LIMIT 1",
      // inline properties and WHERE take the index/filter branches of the scan
      "MATCH (e:Ev {k: 'a'}) RETURN e",
      "MATCH (e:Ev) WHERE e.k = 'a' RETURN e",
      // hoisted into the pattern when parsed (LabelPredicateHoister), so planned as MATCH (e:Ev)
      "MATCH (e) WHERE e:Ev RETURN count(e) AS n",
      // the legacy (non-optimized) path
      "OPTIONAL MATCH (e:Ev) RETURN e",
      // a disjunction would silently drop the samples and answer the vertices only
      "MATCH (e:Ev|P) RETURN count(e) AS n",
      "MATCH (e:Ev:P) RETURN e",
      // the label is never scanned here, only checked on the expanded neighbor
      "MATCH (p:P)-[:R]->(e:Ev) RETURN count(*) AS n",
      // patterns inside expressions and subquery bodies
      "MATCH (p:P) WHERE (p)-->(:Ev) RETURN p",
      "MATCH (p:P) WHERE EXISTS { (p)-->(:Ev) } RETURN p",
      "MATCH (p:P) RETURN COUNT { (:Ev) } AS c",
      "MATCH (p:P) RETURN [(p)-->(e:Ev) | e] AS l",
      "CALL { MATCH (e:Ev) RETURN count(e) AS c } RETURN c",
      "MATCH (p:P) RETURN p.name AS n UNION MATCH (e:Ev) RETURN e.k AS n",
      // a relationship type naming the time-series type
      "MATCH ()-[r:Ev]->() RETURN count(r) AS n",
      // EXPLAIN plans the same statement
      "EXPLAIN MATCH (e:Ev) RETURN e"
  })
  void readPatternOnTimeSeriesTypeFails(final String query) {
    assertThatThrownBy(() -> drain(database.query("cypher", query)))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(MESSAGE);
    // the command entry point plans the same way
    assertThatThrownBy(() -> drain(database.command("cypher", query)))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(MESSAGE);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // the write clauses failed before too, but with an opaque message
      "MERGE (e:Ev {k: 'z'}) RETURN e",
      "CREATE (e:Ev {k: 'z'}) RETURN e",
      // patterns in a FOREACH body, and in a pattern predicate of a SET or a DELETE
      "MATCH (p:P) FOREACH (x IN [1] | MERGE (:Ev {k: 'z'}))",
      "MATCH (p:P) SET p.hasEv = EXISTS { (p)-->(:Ev) }",
      "MATCH (p:P) WITH p WHERE (p)-->(:Ev) DETACH DELETE p"
  })
  void writePatternOnTimeSeriesTypeFails(final String query) {
    assertThatThrownBy(() -> drain(database.command("cypher", query)))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(MESSAGE);
    // refused before anything ran: the vertices are untouched
    assertThat(drain(database.query("cypher", "MATCH (p:P) WHERE p.hasEv IS NULL RETURN p"))).hasSize(2);
  }

  @Test
  void dynamicLabelNamingTimeSeriesTypeFails() {
    // only known when the row is evaluated, so the scan itself refuses the type
    assertThatThrownBy(() -> drain(database.query("cypher", "WITH 'Ev' AS l MATCH (n:$(l)) RETURN n")))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(MESSAGE);
    assertThatThrownBy(() -> drain(database.query("cypher", "MATCH (n:$($l)) RETURN n", Map.of("l", "Ev"))))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(MESSAGE);
    assertThatThrownBy(() -> drain(database.query("cypher", "WITH ['P', 'Ev'] AS l MATCH (n:$(l)) RETURN n")))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(MESSAGE);
    assertThatThrownBy(() -> drain(database.command("cypher", "WITH ['Ev', 'P'] AS l MATCH (n:$(l)) RETURN n")))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(MESSAGE);
  }

  @Test
  void cachedPlanIsRecheckedAfterTheTypeIsCreated() {
    // planned and cached while 'Later' names nothing: no rows, as for any unknown label
    assertThat(drain(database.query("cypher", "MATCH (e:Later) RETURN e"))).isEmpty();
    assertThat(drain(database.query("cypher", "MATCH (e:Later) RETURN e"))).isEmpty();

    database.command("sql", "CREATE TIMESERIES TYPE Later TIMESTAMP ts FIELDS (v DOUBLE)");

    assertThatThrownBy(() -> drain(database.query("cypher", "MATCH (e:Later) RETURN e")))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("'Later', a TIMESERIES type");
  }

  @Test
  void graphQueriesAreUnaffected() {
    assertThat(drain(database.query("cypher", "MATCH (p:P) RETURN p.name AS n ORDER BY n"))).hasSize(2);
    assertThat(drain(database.query("cypher", "MATCH (p:P)-[:R]->(q:P) RETURN count(*) AS n")).get(0).<Long>getProperty("n"))
        .isEqualTo(1L);
    // an unknown label still matches nothing, without an error
    assertThat(drain(database.query("cypher", "MATCH (n:DoesNotExist) RETURN n"))).isEmpty();
    // labels are case-sensitive, as type names are: 'ev' names no type, so it is an unknown label, not the time-series type
    assertThat(drain(database.query("cypher", "MATCH (n:ev) RETURN n"))).isEmpty();
    // a label check that stays a predicate (a negation is never hoisted into the pattern) asks about a bound vertex, and
    // the answer - no vertex carries the label - is correct
    assertThat(drain(database.query("cypher", "MATCH (p:P) WHERE NOT p:Ev RETURN p"))).hasSize(2);
  }

  private static List<Result> drain(final ResultSet rs) {
    try (rs) {
      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());
      return rows;
    }
  }
}
