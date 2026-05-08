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
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the GQL {@code IS TYPED} value-type predicate (issue #3365 section 3.3).
 * Also covers the {@code ::} shorthand and the negated {@code IS NOT TYPED} form.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3365IsTypedTest {
  private Database database;
  private String   databasePath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    databasePath = "./target/databases/testopencypher-istyped-" + testInfo.getTestMethod().get().getName();
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

  @Test
  void integerIsTypedInteger() {
    final ResultSet rs = database.query("opencypher", "RETURN 42 IS TYPED INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  @Test
  void stringIsNotTypedInteger() {
    final ResultSet rs = database.query("opencypher", "RETURN 'hello' IS TYPED INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isFalse();
  }

  @Test
  void stringIsTypedString() {
    final ResultSet rs = database.query("opencypher", "RETURN 'hello' IS TYPED STRING AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  @Test
  void booleanIsTypedBoolean() {
    final ResultSet rs = database.query("opencypher", "RETURN true IS TYPED BOOLEAN AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  @Test
  void floatIsTypedFloat() {
    final ResultSet rs = database.query("opencypher", "RETURN 3.14 IS TYPED FLOAT AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  @Test
  void doubleColonShorthand() {
    final ResultSet rs = database.query("opencypher", "RETURN 7 :: INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  @Test
  void isNotTypedNegation() {
    final ResultSet rs = database.query("opencypher", "RETURN 'hello' IS NOT TYPED INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  @Test
  void nullIsTypedNullableType() {
    // GQL: NULL conforms to a nullable declared type (default).
    final ResultSet rs = database.query("opencypher", "RETURN null IS TYPED INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  @Test
  void nullIsNotTypedNonNullableType() {
    // GQL: NULL does not conform to a NOT NULL type.
    final ResultSet rs = database.query("opencypher", "RETURN null IS TYPED INTEGER NOT NULL AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isFalse();
  }

  @Test
  void listIsTypedList() {
    final ResultSet rs = database.query("opencypher", "RETURN [1, 2, 3] IS TYPED LIST<INTEGER> AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

  @Test
  void listOfStringsIsNotTypedListOfIntegers() {
    final ResultSet rs = database.query("opencypher", "RETURN ['a', 'b'] IS TYPED LIST<INTEGER> AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isFalse();
  }

  @Test
  void anyTypeAcceptsAnyNonNullValue() {
    final ResultSet rs1 = database.query("opencypher", "RETURN 1 IS TYPED ANY AS v");
    assertThat((Boolean) rs1.next().getProperty("v")).isTrue();
    final ResultSet rs2 = database.query("opencypher", "RETURN 'x' IS TYPED ANY AS v");
    assertThat((Boolean) rs2.next().getProperty("v")).isTrue();
  }

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

  @Test
  void signedIntegerAcceptsAllIntegerWidths() {
    final VertexType s = database.getSchema().createVertexType("Sig");
    s.createProperty("l", Type.LONG);
    database.transaction(() -> database.command("opencypher", "CREATE (:Sig {l: 99})"));

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Sig) RETURN n.l IS TYPED SIGNED INTEGER AS v");
    assertThat((Boolean) rs.next().getProperty("v")).isTrue();
  }

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
}
