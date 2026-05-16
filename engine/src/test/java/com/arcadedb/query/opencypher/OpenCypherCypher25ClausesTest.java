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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * OpenCypher / GQL Cypher 25 clause and quantified-path-pattern regression tests (FINISH, FOR, INSERT, IS TYPED, QPP).
 */
class OpenCypherCypher25ClausesTest {
  private Database database;
  private String   databasePath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    databasePath = "./target/databases/testopencypher-cypher25-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("Person");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---------------------------------------------------------------------------
  // FINISH clause (issue #3365 section 1.3)
  // ---------------------------------------------------------------------------

  // Issue #3365: FINISH after CREATE persists the change and returns no rows.
  @Test
  void finishAfterCreatePersistsAndReturnsEmpty() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher", "CREATE (n:Person {name: 'Alice'}) FINISH");
      assertThat(result.hasNext()).isFalse();
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }

  // Issue #3365: FINISH after MATCH returns no rows even if MATCH produces some.
  @Test
  void finishAfterMatchReturnsEmptyWithNoRowLeak() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name: 'A'}), (:Person {name: 'B'}), (:Person {name: 'C'})");
    });

    final ResultSet result = database.query("opencypher", "MATCH (n:Person) FINISH");
    assertThat(result.hasNext()).isFalse();
  }

  // Issue #3365: FINISH after INSERT persists the change and returns no rows.
  @Test
  void finishAfterInsertPersistsAndReturnsEmpty() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher", "INSERT (n:Person {name: 'Bob'}) FINISH");
      assertThat(result.hasNext()).isFalse();
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Bob'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }

  // Issue #3365: RETURN followed by FINISH is a parse error.
  @Test
  void finishWithReturnIsParseError() {
    assertThatThrownBy(() ->
        database.query("opencypher", "MATCH (n:Person) RETURN n FINISH"))
        .isInstanceOf(CommandParsingException.class);
  }

  // ---------------------------------------------------------------------------
  // FOR ... IN ... clause (issue #3365 section 1.2)
  // ---------------------------------------------------------------------------

  // Issue #3365: FOR iterates over a literal list as a UNWIND synonym.
  @Test
  void forIterationOverLiteralList() {
    final ResultSet result = database.query("opencypher", "FOR x IN [1, 2, 3] RETURN x");
    final List<Integer> values = new ArrayList<>();
    while (result.hasNext())
      values.add(((Number) result.next().getProperty("x")).intValue());
    assertThat(values).containsExactly(1, 2, 3);
  }

  // Issue #3365: FOR iterates over a range expression.
  @Test
  void forIterationOverRange() {
    final ResultSet result = database.query("opencypher", "FOR n IN range(1, 5) RETURN n");
    final List<Integer> values = new ArrayList<>();
    while (result.hasNext())
      values.add(((Number) result.next().getProperty("n")).intValue());
    assertThat(values).containsExactly(1, 2, 3, 4, 5);
  }

  // Issue #3365: FOR feeds CREATE producing one vertex per iteration.
  @Test
  void forIterationFollowedByCreate() {
    database.transaction(() -> {
      database.command("opencypher", "FOR name IN ['Alice', 'Bob', 'Carol'] CREATE (:Person {name: name})");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN count(n) AS c");
    assertThat(((Number) verify.next().getProperty("c")).longValue()).isEqualTo(3L);
  }

  // Issue #3365: FOR produces the same row set as the equivalent UNWIND.
  @Test
  void forProducesSameResultsAsUnwind() {
    final ResultSet forResult = database.query("opencypher", "FOR x IN [10, 20, 30] RETURN x");
    final ResultSet unwindResult = database.query("opencypher", "UNWIND [10, 20, 30] AS x RETURN x");

    final List<Integer> forValues = new ArrayList<>();
    while (forResult.hasNext())
      forValues.add(((Number) forResult.next().getProperty("x")).intValue());

    final List<Integer> unwindValues = new ArrayList<>();
    while (unwindResult.hasNext())
      unwindValues.add(((Number) unwindResult.next().getProperty("x")).intValue());

    assertThat(forValues).isEqualTo(unwindValues).containsExactly(10, 20, 30);
  }

  // ---------------------------------------------------------------------------
  // INSERT clause (issue #3365 section 1.1)
  // ---------------------------------------------------------------------------

  // Issue #3365: INSERT creates a single labelled vertex with properties.
  @Test
  void insertSingleVertex() {
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("WORKS_AT");

    database.transaction(() -> {
      final ResultSet result = database.command("opencypher", "INSERT (n:Person {name: 'Alice', age: 30}) RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Result r = result.next();
      final Vertex v = (Vertex) r.toElement();
      assertThat(v.getTypeName()).isEqualTo("Person");
      assertThat((String) v.get("name")).isEqualTo("Alice");
      assertThat((Integer) v.get("age")).isEqualTo(30);
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }

  // Issue #3365: INSERT accepts multiple comma-separated patterns in one statement.
  @Test
  void insertMultiplePatternsInOneStatement() {
    database.transaction(() -> {
      database.command("opencypher", "INSERT (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  // Issue #3365: INSERT can create a relationship between two matched vertices.
  @Test
  void insertRelationship() {
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      database.command("opencypher", "INSERT (a:Person {name: 'Alice'})");
      database.command("opencypher", "INSERT (b:Person {name: 'Bob'})");
    });

    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) INSERT (a)-[r:KNOWS]->(b) RETURN r");
      assertThat(result.hasNext()).isTrue();
    });

    final ResultSet verify = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN r");
    assertThat(verify.hasNext()).isTrue();
  }

  // Issue #3365: INSERT and CREATE produce identical graphs.
  @Test
  void insertProducesIdenticalGraphToCreate() {
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      database.command("opencypher", "INSERT (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})");
    });
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'C'})-[:KNOWS]->(b:Person {name: 'D'})");
    });

    final ResultSet verifyVertices = database.query("opencypher", "MATCH (n:Person) RETURN count(n) AS c");
    assertThat(((Number) verifyVertices.next().getProperty("c")).longValue()).isEqualTo(4L);

    final ResultSet verifyEdges = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c");
    assertThat(((Number) verifyEdges.next().getProperty("c")).longValue()).isEqualTo(2L);
  }

  // Issue #3365: INSERT rejects variable-length relationships (stricter than CREATE).
  @Test
  void insertRejectsVariableLengthRelationship() {
    database.getSchema().createEdgeType("KNOWS");

    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("opencypher", "INSERT (a:Person)-[:KNOWS*1..3]->(b:Person)");
    }))
        .isInstanceOfAny(CommandParsingException.class, RuntimeException.class)
        .hasMessageContaining("");
  }

  // Issue #3365: INSERT accepts the IS-label syntax as an alternative to the colon.
  @Test
  void insertWithLabelIsSyntax() {
    database.transaction(() -> {
      database.command("opencypher", "INSERT (n IS Person {name: 'Eve'})");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Eve'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }

  // ---------------------------------------------------------------------------
  // IS TYPED value-type predicate (issue #3365 section 3.3)
  // ---------------------------------------------------------------------------

  // Issue #3365: integer literal IS TYPED INTEGER is true.
  @Test
  void integerIsTypedInteger() {
    final ResultSet rs = database.query("opencypher", "RETURN 42 IS TYPED INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  // Issue #3365: string literal IS TYPED INTEGER is false.
  @Test
  void stringIsNotTypedInteger() {
    final ResultSet rs = database.query("opencypher", "RETURN 'hello' IS TYPED INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isFalse();
  }

  // Issue #3365: string literal IS TYPED STRING is true.
  @Test
  void stringIsTypedString() {
    final ResultSet rs = database.query("opencypher", "RETURN 'hello' IS TYPED STRING AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  // Issue #3365: boolean literal IS TYPED BOOLEAN is true.
  @Test
  void booleanIsTypedBoolean() {
    final ResultSet rs = database.query("opencypher", "RETURN true IS TYPED BOOLEAN AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  // Issue #3365: float literal IS TYPED FLOAT is true.
  @Test
  void floatIsTypedFloat() {
    final ResultSet rs = database.query("opencypher", "RETURN 3.14 IS TYPED FLOAT AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  // Issue #3365: :: is the shorthand for IS TYPED.
  @Test
  void doubleColonShorthand() {
    final ResultSet rs = database.query("opencypher", "RETURN 7 :: INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  // Issue #3365: IS NOT TYPED negates the type predicate.
  @Test
  void isNotTypedNegation() {
    final ResultSet rs = database.query("opencypher", "RETURN 'hello' IS NOT TYPED INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  // Issue #3365: null conforms to a nullable declared type by default.
  @Test
  void nullIsTypedNullableType() {
    // GQL: NULL conforms to a nullable declared type (default).
    final ResultSet rs = database.query("opencypher", "RETURN null IS TYPED INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  // Issue #3365: null does not conform to a NOT NULL type.
  @Test
  void nullIsNotTypedNonNullableType() {
    // GQL: NULL does not conform to a NOT NULL type.
    final ResultSet rs = database.query("opencypher", "RETURN null IS TYPED INTEGER NOT NULL AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isFalse();
  }

  // Issue #3365: LIST<INTEGER> matches a homogeneous list of integers.
  @Test
  void listIsTypedList() {
    final ResultSet rs = database.query("opencypher", "RETURN [1, 2, 3] IS TYPED LIST<INTEGER> AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  // Issue #3365: LIST<INTEGER> does not match a list of strings.
  @Test
  void listOfStringsIsNotTypedListOfIntegers() {
    final ResultSet rs = database.query("opencypher", "RETURN ['a', 'b'] IS TYPED LIST<INTEGER> AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isFalse();
  }

  // Issue #3365: ANY accepts any non-null value.
  @Test
  void anyTypeAcceptsAnyNonNullValue() {
    final ResultSet rs1 = database.query("opencypher", "RETURN 1 IS TYPED ANY AS v");
    assertThat((Boolean) rs1.next().getProperty("v")).isTrue();
    final ResultSet rs2 = database.query("opencypher", "RETURN 'x' IS TYPED ANY AS v");
    assertThat((Boolean) rs2.next().getProperty("v")).isTrue();
  }

  // Issue #3365: schema-declared FLOAT/DOUBLE drive FLOAT32/FLOAT64 conformance.
  @Test
  void floatPropertyIsTypedFloat32ButNotDoublePropertyIsTyped() {
    // Schema-declared widths drive the runtime Java type ArcadeDB returns: Type.FLOAT yields
    // a java.lang.Float (FLOAT32) and Type.DOUBLE yields a java.lang.Double (FLOAT64).
    final VertexType measurement = database.getSchema().createVertexType("Measurement");
    measurement.createProperty("f32", Type.FLOAT);
    measurement.createProperty("f64", Type.DOUBLE);

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Measurement {f32: 1.5, f64: 2.5})");
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (m:Measurement) RETURN m.f32 IS TYPED FLOAT32 AS f32IsF32, m.f64 IS TYPED FLOAT32 AS f64IsF32, "
            + "m.f32 IS TYPED FLOAT64 AS f32IsF64, m.f64 IS TYPED FLOAT64 AS f64IsF64");
    final var row = rs.next();
    assertThat((Boolean) row.getProperty("f32IsF32")).isTrue();
    assertThat((Boolean) row.getProperty("f64IsF32")).isFalse();
    assertThat((Boolean) row.getProperty("f32IsF64")).isTrue();
    assertThat((Boolean) row.getProperty("f64IsF64")).isTrue();
  }

  // Issue #3365: integer width subtype hierarchy (INT8 <= INT16 <= INT32 <= INT64).
  @Test
  void integerWidthSubtypeHierarchy() {
    // GQL: a Byte conforms to INT8/INT16/INT32/INT64/INTEGER, but a Long does not conform to INT8.
    final VertexType m = database.getSchema().createVertexType("Sample");
    m.createProperty("b", Type.BYTE);
    m.createProperty("s", Type.SHORT);
    m.createProperty("i", Type.INTEGER);
    m.createProperty("l", Type.LONG);

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Sample {b: 1, s: 2, i: 3, l: 4})");
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Sample) RETURN n.b IS TYPED INT8 AS bIs8, n.l IS TYPED INT8 AS lIs8, "
            + "n.b IS TYPED INT64 AS bIs64, n.l IS TYPED INT64 AS lIs64");
    final var row = rs.next();
    assertThat((Boolean) row.getProperty("bIs8")).isTrue();
    assertThat((Boolean) row.getProperty("lIs8")).isFalse();
    assertThat((Boolean) row.getProperty("bIs64")).isTrue();
    assertThat((Boolean) row.getProperty("lIs64")).isTrue();
  }

  // Issue #3365: SIGNED INTEGER accepts all integer widths.
  @Test
  void signedIntegerAcceptsAllIntegerWidths() {
    final VertexType s = database.getSchema().createVertexType("Sig");
    s.createProperty("l", Type.LONG);
    database.transaction(() -> database.command("opencypher", "CREATE (:Sig {l: 99})"));

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Sig) RETURN n.l IS TYPED SIGNED INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  // Issue #3365: IS TYPED in WHERE filters out rows whose property type does not match.
  @Test
  void isTypedInWhereFiltersRows() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name: 'Alice', score: 10})");
      database.command("opencypher", "CREATE (:Person {name: 'Bob',   score: 'high'})");
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Person) WHERE n.score IS TYPED INTEGER RETURN n.name AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat((String) rs.next().getProperty("name")).isEqualTo("Alice");
    assertThat(rs.hasNext()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Quantified Path Patterns Phase A (issue #3365 sections 1.4 and 1.5)
  // ---------------------------------------------------------------------------

  // Issue #3365: a grouped + QPP is equivalent to a variable-length traversal.
  @Test
  void groupedPlusEquivalentToVarLength() {
    database.getSchema().createEdgeType("KNOWS");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})-[:KNOWS]->(c:Person {name:'C'})-[:KNOWS]->(d:Person {name:'D'})");
    });

    final ResultSet qpp = database.query("opencypher",
        "MATCH (a:Person {name:'A'})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person {name:'D'}) RETURN d.name AS name");
    assertThat(qpp.hasNext()).isTrue();

    final ResultSet grouped = database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(:Person))+(d:Person {name:'D'}) RETURN d.name AS name");
    assertThat(grouped.hasNext()).isTrue();
    assertThat((String) grouped.next().getProperty("name")).isEqualTo("D");
  }

  // Issue #3365: grouped {n,m} range quantifier produces the expected number of matches.
  @Test
  void groupedRangeQuantifier() {
    database.getSchema().createEdgeType("KNOWS");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})-[:KNOWS]->(c:Person {name:'C'})-[:KNOWS]->(d:Person {name:'D'})");
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(:Person)){1,2}(end:Person) RETURN end.name AS name ORDER BY name");
    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  // Issue #3365: grouped {n} exact quantifier matches paths of exactly n hops.
  @Test
  void groupedExactQuantifier() {
    database.getSchema().createEdgeType("KNOWS");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})-[:KNOWS]->(c:Person {name:'C'})-[:KNOWS]->(d:Person {name:'D'})");
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(:Person)){2}(end:Person) RETURN end.name AS name");
    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("name")).isEqualTo("C");
  }

  // Issue #3365: grouped {0} quantifier is rejected at parse time.
  @Test
  void groupedRejectsZeroQuantifier() {
    database.getSchema().createEdgeType("KNOWS");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})-[:KNOWS]->(c:Person {name:'C'})-[:KNOWS]->(d:Person {name:'D'})");
    });

    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(:Person)){0}(end:Person) RETURN end"))
        .isInstanceOf(CommandParsingException.class);
  }

  // Issue #3365: inner WHERE inside a grouped QPP is rejected (Phase B).
  @Test
  void groupedRejectsInnerWhere() {
    database.getSchema().createEdgeType("KNOWS");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})-[:KNOWS]->(c:Person {name:'C'})-[:KNOWS]->(d:Person {name:'D'})");
    });

    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(b:Person) WHERE b.name <> 'X')+(end:Person) RETURN end"))
        .isInstanceOf(CommandParsingException.class);
  }

  // Issue #3365: multi-relationship grouped patterns are rejected (Phase B).
  @Test
  void groupedRejectsMultiRelInner() {
    database.getSchema().createEdgeType("KNOWS");
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})-[:KNOWS]->(c:Person {name:'C'})-[:KNOWS]->(d:Person {name:'D'})");
    });

    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person))+(end:Person) RETURN end"))
        .isInstanceOf(CommandParsingException.class);
  }

  // ---------------------------------------------------------------------------
  // Cypher 25 Quantified Relationship syntax (issue #3924)
  // ---------------------------------------------------------------------------

  // Issue #3924: -[:NEXT]->+ is the "one or more" quantifier (equivalent to *1..).
  @Test
  void quantifiedPathPatternWithPlus() {
    database.getSchema().createVertexType("QuantPathTest");
    database.getSchema().createEdgeType("NEXT");
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:QuantPathTest {name: 'A'})-[:NEXT]->(b:QuantPathTest {name: 'B'})-[:NEXT]->(c:QuantPathTest {name: 'C'})"));

    // +  means "one or more" (equivalent to *1..)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (start:QuantPathTest {name: 'A'})-[:NEXT]->+(end:QuantPathTest) RETURN end.name AS result ORDER BY result");

    final List<Object> names = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result r = resultSet.next();
      names.add(r.getProperty("result"));
    }
    assertThat(names).containsExactly("B", "C");
  }

  // Issue #3924: -[:NEXT]->* is the "zero or more" quantifier (equivalent to *0..).
  @Test
  void quantifiedPathPatternWithStar() {
    database.getSchema().createVertexType("QuantPathTest");
    database.getSchema().createEdgeType("NEXT");
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:QuantPathTest {name: 'A'})-[:NEXT]->(b:QuantPathTest {name: 'B'})-[:NEXT]->(c:QuantPathTest {name: 'C'})"));

    // *  means "zero or more" (equivalent to *0..)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (start:QuantPathTest {name: 'A'})-[:NEXT]->*(end:QuantPathTest) RETURN end.name AS result ORDER BY result");

    final List<Object> names = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result r = resultSet.next();
      names.add(r.getProperty("result"));
    }
    // 0-length path returns the start node itself, then 1-hop (B) and 2-hop (C)
    assertThat(names).containsExactly("A", "B", "C");
  }

  // Issue #3924: -[:NEXT]->{2} is the exact-repetition quantifier.
  @Test
  void quantifiedPathPatternExactRepetition() {
    database.getSchema().createVertexType("QuantPathTest");
    database.getSchema().createEdgeType("NEXT");
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:QuantPathTest {name: 'A'})-[:NEXT]->(b:QuantPathTest {name: 'B'})-[:NEXT]->(c:QuantPathTest {name: 'C'})"));

    // {2} means exactly 2
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (start:QuantPathTest {name: 'A'})-[:NEXT]->{2}(end:QuantPathTest) RETURN end.name AS result ORDER BY result");

    final List<Object> names = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result r = resultSet.next();
      names.add(r.getProperty("result"));
    }
    assertThat(names).containsExactly("C");
  }

  // Issue #3924: -[:NEXT]->{1,2} is the range-repetition quantifier.
  @Test
  void quantifiedPathPatternRange() {
    database.getSchema().createVertexType("QuantPathTest");
    database.getSchema().createEdgeType("NEXT");
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:QuantPathTest {name: 'A'})-[:NEXT]->(b:QuantPathTest {name: 'B'})-[:NEXT]->(c:QuantPathTest {name: 'C'})"));

    // {1,2} means between 1 and 2
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (start:QuantPathTest {name: 'A'})-[:NEXT]->{1,2}(end:QuantPathTest) RETURN end.name AS result ORDER BY result");

    final List<Object> names = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result r = resultSet.next();
      names.add(r.getProperty("result"));
    }
    assertThat(names).containsExactly("B", "C");
  }
}
