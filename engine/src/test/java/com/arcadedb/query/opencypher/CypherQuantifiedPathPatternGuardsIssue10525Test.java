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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10525: a quantified path pattern occupies ONE hop of the enclosing path pattern and its real relationship types,
 * variables and inner {@code WHERE} live on the inner pattern, which {@link CypherExpressionWalker} never descended into.
 * Every check that runs through the walker therefore read a placeholder {@code null} for the group: the guard of
 * issue #10440 let {@code MATCH ((a:P)-[:Ev]->(b:P)){1,2}} through to answer 0 rows over a series that has samples (and
 * {@code [:R|Ev]} answered the graph rows and dropped the series in silence), and the same blindness hid a repeated
 * relationship variable and an ill-typed function call inside a group.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherQuantifiedPathPatternGuardsIssue10525Test {
  private static final String              MESSAGE = "'Ev', a TIMESERIES type";
  private static final Cypher25AntlrParser PARSER  = new Cypher25AntlrParser();

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-qpp-guards-10525");
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
    // x -> y -> z
    database.transaction(() -> database.command("cypher",
        "CREATE (x:P {name: 'x'})-[:R]->(y:P {name: 'y'})-[:R]->(z:P {name: 'z'})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void theGroupOfAGraphTypeStillAnswers() {
    // the control of the issue: the feature itself works, so a 0 or a refusal below is about the time-series type
    assertThat(count("MATCH ((a:P)-[:R]->(b:P)){1,2} RETURN count(*) AS n")).isEqualTo(3L);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // the relationship type of the group
      "MATCH ((a:P)-[:Ev]->(b:P)){1,2} RETURN count(*) AS n",
      "MATCH p = ((a)-[:Ev]->(b)){1,3} RETURN count(p) AS n",
      "MATCH ((a:P)-[r:R]->(b:P)-[s:Ev]->(c:P)){1,2} RETURN count(*) AS n",
      // an alternation would answer the graph rows and drop the series without a word
      "MATCH ((a:P)-[:R|Ev]->(b:P)){1,2} RETURN count(*) AS n",
      // the label of an inner node
      "MATCH ((a:Ev)-[:R]->(b:P)){1,2} RETURN count(*) AS n",
      // the group written inside a boundary pair and inside the bodies that hold patterns
      "MATCH (s:P) ((a:P)-[:Ev]->(b:P)){1,2} (t:P) RETURN count(*) AS n",
      "MATCH (s:P) WHERE EXISTS { ((a:P)-[:Ev]->(b:P)){1,2} } RETURN s",
      "MATCH (s:P) RETURN COUNT { ((a:P)-[:Ev]->(b:P)){1,2} } AS c",
      "OPTIONAL MATCH ((a:P)-[:Ev]->(b:P)){1,2} RETURN count(*) AS n",
      "CALL { MATCH ((a:P)-[:Ev]->(b:P)){1,2} RETURN count(*) AS c } RETURN c",
      "EXPLAIN MATCH ((a:P)-[:Ev]->(b:P)){1,2} RETURN count(*)"
  })
  void aGroupNamingATimeSeriesTypeIsRefused(final String query) {
    assertThatThrownBy(() -> drain(database.query("cypher", query)))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(MESSAGE);
    assertThatThrownBy(() -> drain(database.command("cypher", query)))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(MESSAGE);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "CREATE ((a:P)-[:Ev]->(b:P)){1,2}",
      "MERGE ((a:P)-[:Ev]->(b:P)){1,2}"
  })
  void aGroupInAWriteClauseNeverReachesTheTimeSeriesTypeSilently(final String query) {
    // Neither clause takes a quantified path pattern; whatever refuses it, the series is not written to or answered as empty, and the
    // vertices are untouched
    assertThatThrownBy(() -> drain(database.command("cypher", query))).isInstanceOf(RuntimeException.class);

    assertThat(count("MATCH (p:P) RETURN count(p) AS n")).isEqualTo(3L);
  }

  @Test
  void aRelationshipVariableRepeatedInsideAGroupIsRefused() {
    assertThatThrownBy(() -> drain(database.query("cypher", "MATCH ((a)-[r:R]->(b)-[r:R]->(c)){1,2} RETURN count(*) AS n")))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("RelationshipUniquenessViolation")
        .hasMessageContaining("'r'");
  }

  @Test
  void aFunctionCallOfTheInnerWhereIsCheckedLikeAnyOther() {
    assertThatThrownBy(() -> drain(database.query("cypher", "MATCH ((a:P)-[:R]->(b:P) WHERE abs('x') > 0){1,2} RETURN count(*) AS n")))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()")
        .hasMessageContaining("STRING");
  }

  @Test
  void theWalkHandsTheInnerPatternOfAGroupToTheVisitor() {
    final CypherStatement statement = PARSER.parse("MATCH (s) ((a:P)-[r:R]->(b:P) WHERE b.name <> 'q'){1,2} (t) RETURN s");
    final List<PathPattern> patterns = new ArrayList<>();
    final List<Expression> expressions = new ArrayList<>();
    CypherExpressionWalker.walk(statement, new CypherExpressionWalker.Visitor() {
      @Override
      public void visit(final Expression expression) {
        expressions.add(expression);
      }

      @Override
      public void visitPattern(final PathPattern pattern) {
        patterns.add(pattern);
      }
    });

    // the enclosing pattern and the pattern the group repeats
    assertThat(patterns).hasSize(2);
    final PathPattern outer = patterns.getFirst();
    assertThat(outer.getRelationships()).hasSize(1).first().isInstanceOf(QuantifiedPathPattern.class);
    assertThat(patterns.get(1)).isSameAs(((QuantifiedPathPattern) outer.getRelationships().getFirst()).getInnerPattern());
    // the inner WHERE is walked: b.name is reached as an expression, and nothing else of the statement mentions it
    assertThat(expressions).anyMatch(expression -> expression instanceof PropertyAccessExpression property && "b".equals(property.getVariableName()));
  }

  @Test
  void theVariablesWrittenInsideAGroupAreReferenced() {
    // nothing outside the group mentions a, b or the property the inner WHERE reads
    final CypherStatement statement = PARSER.parse("MATCH (s) ((a:P)-[r:R]->(b:P) WHERE b.name <> 'q'){1,2} (t) RETURN s");
    assertThat(CypherReferencedVariables.of(statement).referencesAny(List.of("a"))).isTrue();
    assertThat(CypherReferencedVariables.of(statement).referencesAny(List.of("b"))).isTrue();
    assertThat(CypherReferencedVariables.of(statement).referencesAny(List.of("r"))).isTrue();
    assertThat(CypherReferencedVariables.of(statement).referencesAny(List.of("unrelated"))).isFalse();
  }

  private long count(final String query) {
    try (final ResultSet resultSet = database.query("cypher", query)) {
      final Result row = resultSet.next();
      return ((Number) row.getProperty("n")).longValue();
    }
  }

  private static List<Result> drain(final ResultSet resultSet) {
    final List<Result> rows = new ArrayList<>();
    try (resultSet) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }
}
