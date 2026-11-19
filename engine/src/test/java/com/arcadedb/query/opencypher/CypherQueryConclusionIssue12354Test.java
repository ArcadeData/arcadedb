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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12354: a query that concludes with a reading clause ({@code UNWIND}, and equally {@code MATCH}, {@code OPTIONAL MATCH},
 * {@code WITH}, {@code LOAD CSV} or a returning {@code CALL { }}) was accepted and answered the variables in scope. Neo4j and
 * Memgraph refuse it at compile time ("Query cannot conclude with UNWIND (must be a RETURN clause, a FINISH clause, an update
 * clause, a unit subquery call, or a procedure call with no YIELD)"), and so does ArcadeDB now.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherQueryConclusionIssue12354Test extends TestHelper {

  @BeforeEach
  void createGraph() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:A)-[:LOOP]->(a), ()-[:T]->()"));
  }

  @Test
  void theReportedQueryEndsWithReturnAndCountsTheLoop() {
    // The reproducer as written concludes with RETURN: it is valid, and the one self-loop is counted
    try (final ResultSet rs = database.query("opencypher", "MATCH (n)-[r]->(n) "
        + "WITH * WHERE CASE WHEN true THEN coalesce(true, false) ELSE false END "
        + "UNWIND [[794078]] AS x UNWIND x AS y WITH * RETURN count(r)")) {
      assertThat(rs.next().<Long>getProperty("count(r)")).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aQueryConcludingWithAReadingClauseIsRefused() {
    for (final Object[] query : new Object[][] {//
        { "MATCH (n)-[r]->(n) WITH * WHERE true UNWIND [[794078]] AS x UNWIND x AS y", "UNWIND" },//
        { "UNWIND [1, 2] AS x", "UNWIND" },//
        { "MATCH (n) WITH * WHERE true UNWIND [1] AS x", "UNWIND" },//
        { "MATCH (n)", "MATCH" },//
        { "MATCH (n) WHERE n.x = 1", "MATCH" },//
        { "OPTIONAL MATCH (n)", "OPTIONAL MATCH" },//
        { "WITH 1 AS x", "WITH" },//
        { "MATCH (n) WITH n", "WITH" },//
        { "MATCH (n) CALL { WITH n RETURN n.x AS x }", "CALL" },//
        { "CALL { RETURN 1 AS x }", "CALL" } }) {
      assertThatThrownBy(() -> database.query("opencypher", (String) query[0]).close()).as((String) query[0])//
          .isInstanceOf(CommandParsingException.class)//
          .hasMessageContaining("Query cannot conclude with " + query[1] + " (must be a RETURN clause");
    }
  }

  @Test
  void theBodyOfACallSubqueryConcludesLikeAQuery() {
    assertThatThrownBy(() -> database.query("opencypher", "CALL { MATCH (n) } RETURN 1 AS x").close())//
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("Query cannot conclude with MATCH");
    assertThatThrownBy(() -> database.query("opencypher", "CALL { MATCH (n) RETURN n UNION MATCH (n) } RETURN 1 AS x").close())//
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("Query cannot conclude with MATCH");
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (m) RETURN EXISTS { CALL { WITH m UNWIND [1] AS x } } AS r").close())//
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("Query cannot conclude with UNWIND");
  }

  @Test
  void aUnionBranchConcludesLikeAQuery() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN 1 AS x UNION MATCH (n)").close())//
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("Query cannot conclude with MATCH");
    // a CALL body two levels down in a UNION branch without RETURN, which the walk of the nested statements does not reach
    assertThatThrownBy(() -> database.command("opencypher",
        "CALL { CALL { MATCH (n) } CREATE (:B) } UNION CALL { CREATE (:B) }").close())//
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("Query cannot conclude with MATCH");
    assertThatThrownBy(() -> database.command("opencypher", "CALL { CALL { MATCH (n) } CREATE (:B) }").close())//
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("Query cannot conclude with MATCH");
  }

  @Test
  void everyValidConclusionIsStillAccepted() {
    for (final String query : List.of(//
        "MATCH (n) RETURN count(n) AS c",//
        "UNWIND [1, 2] AS x RETURN x",//
        "MATCH (n) FINISH",//
        "RETURN 1 AS x UNION RETURN 2 AS x",//
        "MATCH (n) CALL { WITH n RETURN n.x AS x } RETURN x",//
        // a procedure call with no YIELD, a standalone procedure call with YIELD
        "CALL db.labels()",//
        "CALL db.labels() YIELD label",//
        // EXISTS and COUNT bodies may end with a pattern
        "MATCH (n) WHERE EXISTS { MATCH (n)-->() } RETURN count(n) AS c",//
        // the RETURN of a nested CALL is not the body's: the body is still run as a query that concludes with a RETURN
        "MATCH (n) WHERE EXISTS { MATCH (n)-->(m) CALL { WITH m RETURN m.x AS x } } RETURN count(n) AS c",//
        "MATCH (n) RETURN COUNT { MATCH (n)-->(m) CALL { WITH m RETURN m.x AS x } } AS c",//
        "MATCH (n) RETURN COUNT { MATCH (n)-->() } AS c"))
      assertThatCode(() -> database.query("opencypher", query).close()).as(query).doesNotThrowAnyException();

    for (final String query : List.of(//
        "CREATE (:B)",//
        // a unit subquery
        "MATCH (n:A) CALL { WITH n SET n.seen = true }",//
        "MATCH (n:B) SET n.x = 1",//
        "MATCH (n:B) REMOVE n.x",//
        "MERGE (:C)",//
        "UNWIND [1] AS i FOREACH (j IN [i] | CREATE (:D {j: j}))",//
        "MATCH (n:B) DETACH DELETE n"))
      assertThatCode(() -> database.transaction(() -> database.command("opencypher", query).close())).as(query)
          .doesNotThrowAnyException();
  }
}
