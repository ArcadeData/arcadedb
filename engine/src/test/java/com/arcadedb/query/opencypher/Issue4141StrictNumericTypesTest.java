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
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4141 (ISO/IEC 39075 GQL, section 3): strict numeric types - write side. A property declared with a
 * GQL width type ({@code INT8/16/32/64}, {@code FLOAT32/64}) must round-trip exactly: the schema property is
 * created at that width, a value written to it is stored and reloaded at that width, and it satisfies the
 * matching {@code IS TYPED} predicate (whose subtype hierarchy was already implemented for the read side).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4141StrictNumericTypesTest {
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
