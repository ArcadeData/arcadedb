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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/**
 * Issue #12657: #11888 refused a pattern expression that is not used as a boolean in a WHERE, and a RETURN, WITH, LET or
 * SET value already refused it, but every other clause that evaluates an expression still accepted it and evaluated it as
 * an existence boolean. {@code UNWIND [(p)-->()]} answered a list of one boolean (neither the list of paths of Cypher 4
 * nor the refusal of Neo4j 5), {@code ORDER BY (p)-->()} sorted by a boolean, and {@code size((p)-->())} failed with a type
 * mismatch that did not name {@code COUNT {}}. Like Neo4j 5, those clauses now refuse it, with the message of the WHERE.
 * A pattern expression in a boolean position, a pattern comprehension and the subquery expressions keep answering.
 * <p>
 * Graph: a -> b, a -> c, b -> c, so a has two outgoing relationships, b one and c none.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherPatternExpressionOtherClausesIssue12657Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:P {n: 'a'}), (b:P {n: 'b'}), (c:P {n: 'c'}), (a)-[:K]->(b), (a)-[:K]->(c), (b)-[:K]->(c)"));
  }

  /** The four statements of the issue. */
  @Test
  void statementsOfTheIssueAreRefused() {
    assertRefused("MATCH (p:P) UNWIND [(p)-->()] AS x RETURN x", "contexts that evaluate to a boolean");
    assertRefused("MATCH (p:P) RETURN p.n AS n ORDER BY (p)-->() DESC", "contexts that evaluate to a boolean");
    assertRefused("MATCH (p:P) WITH p ORDER BY size((p)-->()) DESC RETURN p.n AS n", "COUNT {}");
    assertRefused("MATCH (p:P) UNWIND range(0, size((p)-->())) AS i RETURN p.n AS n", "COUNT {}");
  }

  /** One statement per clause that evaluates an expression. */
  @Test
  void everyClauseThatEvaluatesAnExpressionIsChecked() {
    assertSoftly(softly -> {
      for (final String statement : List.of(//
          "MATCH (p:P) UNWIND (p)-->() AS x RETURN x",//
          "MATCH (p:P) FOR x IN [(p)-->()] RETURN x",//
          "MATCH (p:P) RETURN p.n AS n ORDER BY toString((p)-->())",//
          "MATCH (p:P) WITH p ORDER BY (p)-->() RETURN p.n AS n",//
          "MATCH (p:P) WITH p ORDER BY p.n, (p)<--() DESC RETURN p.n AS n",//
          "MATCH (p:P) RETURN p.n AS n SKIP toInteger((p)-->())",//
          "MATCH (p:P) RETURN p.n AS n LIMIT toInteger((p)-->())",//
          "MATCH (p:P) WITH p SKIP toInteger((p)-->()) RETURN p.n AS n",//
          "MATCH (p:P) WITH p LIMIT toInteger((p)-->()) RETURN p.n AS n",//
          "MATCH (p:P) FOREACH (x IN [(p)-->()] | SET p.v = x)",//
          "MATCH (p:P) DELETE (p)-->()",//
          "MATCH (p:P) CALL db.labels((p)-->()) YIELD label RETURN label",//
          "MATCH (p:P) LOAD CSV FROM toString((p)-->()) AS row RETURN row",//
          "MATCH (p:P) CREATE (:Q {v: (p)-->()})",//
          "MATCH (p:P) MERGE (:Q {v: (p)-->()})",//
          "MATCH (p:P) MATCH (q:P {n: (p)-->()}) RETURN q.n AS n",//
          "MATCH (p:P) INSERT (:Q {v: (p)-->()})",//
          "MATCH (p:P) SET p = {v: (p)-->()}",//
          "MATCH (p:P) SET p += {v: (p)-->()}",//
          "MATCH (p:P) SET p[toString((p)-->())] = 1",//
          "MATCH (p:P) REMOVE p[toString((p)-->())]",//
          "MATCH (p:P) SET p:$(toString((p)-->()))",//
          "MATCH (p:P) SET ((p)-->()).v = 1",//
          "MATCH (p:P) REMOVE ((p)-->()).v",//
          // a target that is neither a variable nor a parenthesis
          "MATCH (p:P) SET head([(p)-->()]).v = 1",//
          "MATCH (p:P) SET head([(p)-->()])['v'] = 1",//
          "MATCH (p:P) REMOVE head([(p)-->()]).v",//
          "MATCH (p:P) REMOVE head([(p)-->()])['v']",//
          "MATCH (p:P) CALL (p) { SET p.v = 1 } IN TRANSACTIONS OF toInteger((p)-->()) ROWS"))
        softly.assertThatThrownBy(() -> rows(statement)).as(statement).isInstanceOf(CommandParsingException.class)
            .hasMessageContaining("contexts that evaluate to a boolean");
    });
  }

  @Test
  void sizeOfAPatternNamesCountInEveryClause() {
    assertSoftly(softly -> {
      for (final String statement : List.of(//
          "MATCH (p:P) UNWIND [size((p)-->())] AS x RETURN x",//
          "MATCH (p:P) RETURN p.n AS n ORDER BY size((p)-->()) DESC",//
          "MATCH (p:P) RETURN p.n AS n LIMIT size((p)-->())",//
          "MATCH (p:P) FOREACH (i IN range(0, size((p)-->())) | SET p.v = i)",//
          "MATCH (p:P) CREATE (:Q {v: size((p)-->())})"))
        softly.assertThatThrownBy(() -> rows(statement)).as(statement).isInstanceOf(CommandParsingException.class)
            .hasMessageContaining("inside the function size()").hasMessageContaining("COUNT {}");
    });
  }

  /** The condition of a conditional query is a boolean position: a pattern used as a value in it is refused as in a WHERE. */
  @Test
  void conditionOfAConditionalQueryIsChecked() {
    assertRefused("WHEN size(()-->()) > 0 THEN RETURN 1 AS n ELSE RETURN 2 AS n", "COUNT {}");
    assertRefused("WHEN toString(()-->()) = 'true' THEN RETURN 1 AS n ELSE RETURN 2 AS n",
        "contexts that evaluate to a boolean");
  }

  /** A pattern expression in a boolean position of the clause expression keeps answering. */
  @Test
  void patternAsABooleanKeepsAnswering() {
    assertThat(rows("MATCH (p:P) RETURN p.n AS n ORDER BY exists((p)-->()), n")).containsExactly("c", "a", "b");
    assertThat(rows("MATCH (p:P) RETURN p.n AS n ORDER BY NOT (p)-->(), n")).containsExactly("a", "b", "c");
    assertThat(rows("MATCH (p:P) RETURN p.n AS n ORDER BY CASE WHEN (p)<--() THEN 0 ELSE 1 END, n")).containsExactly("b",
        "c", "a");
    assertThat(rows("MATCH (p:P) WITH p ORDER BY (p)-->() AND (p)<--() DESC, p.n RETURN p.n AS n")).containsExactly("b",
        "a", "c");
    assertThat(rows("MATCH (p:P) UNWIND [exists((p)-->())] AS x WITH p, x WHERE x RETURN p.n AS n ORDER BY n"))
        .containsExactly("a", "b");
    assertThat(rows("MATCH (p:P) UNWIND [x IN [p] WHERE (x)-->() | x.n] AS n RETURN n ORDER BY n")).containsExactly("a",
        "b");
  }

  /** The ways to count or collect the matches of a pattern stay accepted in every clause. */
  @Test
  void countAndComprehensionsStayTheWayToCount() {
    assertThat(rows("MATCH (p:P) RETURN p.n AS n ORDER BY COUNT { (p)-->() } DESC")).containsExactly("a", "b", "c");
    assertThat(rows("MATCH (p:P) WITH p ORDER BY size([(p)-->(q) | q]) DESC RETURN p.n AS n")).containsExactly("a", "b",
        "c");
    assertThat(rows("MATCH (p:P {n: 'a'}) UNWIND [(p)-->(q) | q.n] AS n RETURN n ORDER BY n")).containsExactly("b", "c");
    assertThat(rows("MATCH (p:P {n: 'a'}) UNWIND COLLECT { MATCH (p)-->(q) RETURN q.n } AS n RETURN n ORDER BY n"))
        .containsExactly("b", "c");
    assertThat(rows("MATCH (p:P {n: 'a'}) UNWIND range(1, COUNT { (p)-->() }) AS n RETURN n ORDER BY n")).containsExactly(
        1L, 2L);
  }

  private void assertRefused(final String statement, final String message) {
    assertSoftly(softly -> softly.assertThatThrownBy(() -> rows(statement)).as(statement)
        .isInstanceOf(CommandParsingException.class).hasMessageContaining(message));
  }

  private List<Object> rows(final String statement) {
    // A write that got through by mistake must not change the graph the other statements read
    database.begin();
    try (final ResultSet rs = database.command("opencypher", statement)) {
      return rs.stream().map(r -> r.<Object>getProperty("n")).toList();
    } finally {
      database.rollback();
    }
  }
}
