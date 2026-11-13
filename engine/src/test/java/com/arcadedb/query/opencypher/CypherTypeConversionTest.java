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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Type functions and conversions: special floats, toFloat on booleans and strict numeric types.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherTypeConversionTest {
  /**
   * Issue #4141 (ISO/IEC 39075 GQL, section 3): strict numeric types - write side. A property declared with a
   * GQL width type ({@code INT8/16/32/64}, {@code FLOAT32/64}) must round-trip exactly: the schema property is
   * created at that width, a value written to it is stored and reloaded at that width, and it satisfies the
   * matching {@code IS TYPED} predicate (whose subtype hierarchy was already implemented for the read side).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue4141StrictNumericTypes {
    private Database database;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      final DatabaseFactory factory = new DatabaseFactory(
          "./target/databases/testIssue4141StrictNumeric-" + testInfo.getTestMethod().get().getName());
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Measure");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    // ---- Schema declaration: each GQL width maps to the matching ArcadeDB Type --------------------

    @ParameterizedTest
    @CsvSource({
        "INT8, BYTE", "INTEGER8, BYTE",
        "INT16, SHORT", "INTEGER16, SHORT",
        "INT32, INTEGER", "INTEGER32, INTEGER", "INT, INTEGER",
        "INT64, LONG", "INTEGER64, LONG", "INTEGER, LONG", "SIGNED INTEGER, LONG",
        "FLOAT32, FLOAT",
        "FLOAT64, DOUBLE", "FLOAT, DOUBLE" })
    void gqlNumericTypeMapsToArcadeWidth(final String gqlType, final Type expected) {
      database.command("opencypher", "CREATE CONSTRAINT FOR (m:Measure) REQUIRE m.v IS TYPED " + gqlType).close();
      assertThat(database.getSchema().getType("Measure").getProperty("v").getType()).isEqualTo(expected);
    }

    // ---- Persistence round-trip: a value keeps its declared width through store + reload ----------

    @ParameterizedTest
    @CsvSource({
        "INT8, 5, Byte",
        "INT16, 5, Short",
        "INT32, 5, Integer",
        "INT64, 5, Long",
        "FLOAT32, 1.5, Float",
        "FLOAT64, 1.5, Double" })
    void valueRoundTripsAtDeclaredWidth(final String gqlType, final String literal, final String javaType) {
      database.command("opencypher", "CREATE CONSTRAINT FOR (m:Measure) REQUIRE m.v IS TYPED " + gqlType).close();
      database.command("opencypher", "CREATE (m:Measure {v: " + literal + "})").close();

      // The reloaded record holds the value at the declared Java width (not widened to Long/Double).
      database.transaction(() -> {
        final Document doc = (Document) database.iterateType("Measure", true).next();
        assertThat(doc.get("v").getClass().getSimpleName()).isEqualTo(javaType);
      });

      // And the value still satisfies its declared IS TYPED predicate after the round-trip.
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (m:Measure) RETURN m.v IS TYPED " + gqlType + " AS ok")) {
        assertThat(rs.next().<Boolean>getProperty("ok")).isTrue();
      }
    }

    // ---- INT is the 32-bit alias (consistent with the IS TYPED read side) ------------------------

    @ParameterizedTest
    @CsvSource({
        "INT16, INT8",   // a Short is not an INT8
        "INT32, INT16",  // an Integer is not an INT16
        "FLOAT64, FLOAT32" }) // a Double is not a FLOAT32
    void declaredWidthIsNotMatchedByANarrowerType(final String declared, final String narrower) {
      database.command("opencypher", "CREATE CONSTRAINT FOR (m:Measure) REQUIRE m.v IS TYPED " + declared).close();
      database.command("opencypher", "CREATE (m:Measure {v: 1})").close();

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (m:Measure) RETURN m.v IS TYPED " + narrower + " AS ok")) {
        assertThat(rs.next().<Boolean>getProperty("ok")).isFalse();
      }
    }
  }

  /**
   * Behaviour tests for issue #5292, which reported that {@code type()} "crashes" on NaN and Infinity
   * FLOAT values while allegedly returning {@code FLOAT} for finite floats such as {@code type(1.5)}.
   * <p>
   * That premise does not hold. In openCypher (and in Neo4j, the reference implementation) {@code type()}
   * is the <i>relationship-type</i> function: it takes a RELATIONSHIP and returns its type name, e.g.
   * {@code KNOWS}. Passing any non-relationship value is a TypeError regardless of the value, so
   * {@code type(1.5)} is rejected exactly like {@code type(sqrt(-1))} - the reported "control query"
   * fails too, and no NaN/Infinity-specific behaviour exists. The function that returns {@code FLOAT}
   * for a value is {@code valueType()}, which ArcadeDB implements and which handles NaN and Infinity.
   * <p>
   * These tests pin that contract: {@code type()} is uniformly a client-side TypeError for every float
   * (finite or special), {@code valueType()} classifies NaN/Infinity as FLOAT, the special values
   * themselves evaluate to the correct Double, and {@code type()} still works on real relationships.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5292TypeOnSpecialFloat extends TestHelper {

    /** The reporter's queries, plus the finite-float "control" query they claimed returned FLOAT. */
    private static final String[] TYPE_ON_FLOAT_QUERIES = {
        "RETURN type(sqrt(-1)) AS result",
        "RETURN type(log(0)) AS result",
        "RETURN type(exp(10000)) AS result",
        "RETURN type(1.0 / 0.0) AS result",
        "RETURN type(1.5) AS result",
    };

    /**
     * type() rejects every float argument identically. The special values are not a special case: the
     * reporter's own control query, type(1.5), fails the same way, so there is no NaN/Infinity bug.
     */
    @Test
    void typeOnAnyFloatIsAUniformTypeError() {
      for (final String query : TYPE_ON_FLOAT_QUERIES)
        assertThatThrownBy(() -> database.query("cypher", query).next())
            .as("type() must reject the float argument in: %s", query)
            .isInstanceOf(CommandSemanticException.class)
            .hasMessageContaining("relationship");
    }

    /** The TypeError must point the user at valueType(), the function that answers what they asked. */
    @Test
    void typeErrorSuggestsValueType() {
      assertThatThrownBy(() -> database.query("cypher", "RETURN type(sqrt(-1)) AS result").next())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("valueType()");
    }

    /** valueType() - the correct function - classifies NaN and both infinities as FLOAT, like Neo4j. */
    @Test
    void valueTypeClassifiesSpecialFloatsAsFloat() {
      final String[] queries = {
          "RETURN valueType(sqrt(-1)) AS result",
          "RETURN valueType(log(0)) AS result",
          "RETURN valueType(exp(10000)) AS result",
          "RETURN valueType(1.0 / 0.0) AS result",
          "RETURN valueType(1.5) AS result",
      };
      for (final String query : queries) {
        final ResultSet rs = database.query("cypher", query);
        assertThat(rs.hasNext()).as("no row for: %s", query).isTrue();
        assertThat((String) rs.next().getProperty("result")).as("valueType for: %s", query).isEqualTo("FLOAT NOT NULL");
      }
    }

    /** The special values evaluate to the correct Double, matching Neo4j's NaN / -Infinity / Infinity. */
    @Test
    void specialFloatsEvaluateToTheCorrectDouble() {
      final ResultSet rs = database.query("cypher",
          "RETURN sqrt(-1) AS nan, log(0) AS negInf, exp(10000) AS posInf, 1.0 / 0.0 AS divInf");
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat((Double) row.getProperty("nan")).isNaN();
      assertThat((Double) row.getProperty("negInf")).isEqualTo(Double.NEGATIVE_INFINITY);
      assertThat((Double) row.getProperty("posInf")).isEqualTo(Double.POSITIVE_INFINITY);
      assertThat((Double) row.getProperty("divInf")).isEqualTo(Double.POSITIVE_INFINITY);
    }

    /** type() keeps doing its actual job: returning the type name of a relationship. */
    @Test
    void typeOnRelationshipReturnsTheRelationshipType() {
      database.command("cypher", "CREATE (:Person {name: 'a'})-[:KNOWS]->(:Person {name: 'b'})");
      final ResultSet rs = database.query("cypher", "MATCH ()-[r:KNOWS]->() RETURN type(r) AS result");
      assertThat(rs.hasNext()).isTrue();
      assertThat((String) rs.next().getProperty("result")).isEqualTo("KNOWS");
    }
  }

  /**
   * Regression test for issue #5294: passing an unsupported argument type to a Cypher scalar conversion
   * function ({@code toFloat}, {@code toInteger}, {@code toBoolean}) must fail as a client-side type error
   * ({@link CommandSemanticException}, mapped to HTTP 400), never as an unhandled {@code CommandExecutionException}
   * (mapped to HTTP 500). This mirrors the toString() fix from issue #5203.
   *
   * <p>Behaviour matches the Neo4j reference implementation:
   * <ul>
   *   <li>{@code toFloat()} accepts only STRING/INTEGER/FLOAT, so a BOOLEAN argument is a type error.</li>
   *   <li>{@code toInteger()} additionally accepts BOOLEAN (true -> 1, false -> 0).</li>
   *   <li>{@code toBoolean()} rejects FLOAT.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5294ToFloatBoolean extends TestHelper {

    // ===================== toFloat() =====================

    @Test
    void toFloatWithBooleanTrueThrowsSemanticError() {
      // Issue #5294: RETURN toFloat(true) used to crash with a 500 CommandExecutionException.
      assertThatThrownBy(() -> database.command("opencypher", "RETURN toFloat(true) AS r").stream().toList())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("toFloat")
          .hasMessageContaining("Boolean");
    }

    @Test
    void toFloatWithBooleanFalseThrowsSemanticError() {
      assertThatThrownBy(() -> database.command("opencypher", "RETURN toFloat(false) AS r").stream().toList())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("toFloat");
    }

    @Test
    void toFloatWithListThrowsSemanticError() {
      assertThatThrownBy(() -> database.command("opencypher", "RETURN toFloat([1, 2, 3]) AS r").stream().toList())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("toFloat");
    }

    @Test
    void toFloatStillConvertsNumbersAndStrings() {
      try (final ResultSet rs = database.command("opencypher", "RETURN toFloat(1) AS a, toFloat(0) AS b, toFloat('11.5') AS c")) {
        assertThat(rs.hasNext()).isTrue();
        final var row = rs.next();
        assertThat(((Number) row.getProperty("a")).doubleValue()).isEqualTo(1.0);
        assertThat(((Number) row.getProperty("b")).doubleValue()).isEqualTo(0.0);
        assertThat(((Number) row.getProperty("c")).doubleValue()).isEqualTo(11.5);
      }
    }

    @Test
    void toFloatOrNullStillReturnsNullForBoolean() {
      // The OrNull variant swallows the type error into null, matching Neo4j's toFloatOrNull(true) -> null.
      try (final ResultSet rs = database.command("opencypher", "RETURN toFloatOrNull(true) AS r")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Object>getProperty("r")).isNull();
      }
    }

    // ===================== toInteger() =====================

    @Test
    void toIntegerStillConvertsBoolean() {
      // Neo4j (and ArcadeDB) DO accept boolean for toInteger: true -> 1, false -> 0.
      try (final ResultSet rs = database.command("opencypher", "RETURN toInteger(true) AS t, toInteger(false) AS f")) {
        assertThat(rs.hasNext()).isTrue();
        final var row = rs.next();
        assertThat(((Number) row.getProperty("t")).longValue()).isEqualTo(1L);
        assertThat(((Number) row.getProperty("f")).longValue()).isEqualTo(0L);
      }
    }

    @Test
    void toIntegerWithListThrowsSemanticError() {
      assertThatThrownBy(() -> database.command("opencypher", "RETURN toInteger([1, 2, 3]) AS r").stream().toList())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("toInteger");
    }

    // ===================== toBoolean() =====================

    @Test
    void toBooleanWithFloatThrowsSemanticError() {
      assertThatThrownBy(() -> database.command("opencypher", "RETURN toBoolean(1.5) AS r").stream().toList())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("toBoolean");
    }

    @Test
    void toBooleanWithListThrowsSemanticError() {
      assertThatThrownBy(() -> database.command("opencypher", "RETURN toBoolean([1, 2, 3]) AS r").stream().toList())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("toBoolean");
    }
  }
}
