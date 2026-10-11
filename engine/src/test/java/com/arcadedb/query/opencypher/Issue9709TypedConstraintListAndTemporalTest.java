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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9709: a property type constraint could not declare a list ({@code IS :: LIST<T>} was refused),
 * and the correlated defects found with it - every temporal type constraint but DATE declared a type its own values did not
 * survive (DURATION refused every write, ZONED DATETIME lost its zone), and the {@code IS ::} predicate and {@code valueType()}
 * did not recognise a single temporal value or point.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9709TypedConstraintListAndTemporalTest extends TestHelper {

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {//
      "LIST<STRING>|STRING",//
      "LIST<STRING NOT NULL>|STRING",//
      "STRING LIST|STRING",//
      "STRING NOT NULL LIST|STRING",//
      "ARRAY<STRING>|STRING",//
      "LIST<INTEGER NOT NULL>|LONG",//
      "LIST<INT32>|INTEGER",//
      "LIST<FLOAT NOT NULL>|DOUBLE",//
      "LIST<BOOLEAN NOT NULL>|BOOLEAN",//
      "LIST<DATE NOT NULL>|DATE",//
      "LIST<ZONED DATETIME NOT NULL>|ZONED_DATETIME",//
      "LIST<DURATION NOT NULL>|DURATION" })
  void aListTypeDeclaresAListOfItsElementType(final String cypherType, final String ofType) {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: " + cypherType);

    final Property p = property("L", "p");
    assertThat(p.getType()).isEqualTo(Type.LIST);
    assertThat(p.getOfType()).isEqualTo(ofType);
  }

  @Test
  void aListOfAnyDeclaresAListWithoutElementType() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: LIST<ANY>");

    final Property p = property("L", "p");
    assertThat(p.getType()).isEqualTo(Type.LIST);
    assertThat(p.getOfType()).isNull();
  }

  @Test
  void aListOfStringsRoundTripsAndSatisfiesItsOwnType() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.tags IS :: LIST<STRING NOT NULL>");
    command("CREATE (:L {tags: ['a', 'b']})");

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:L) RETURN n.tags AS tags, n.tags IS :: LIST<STRING NOT NULL> AS typed, n.tags = ['a', 'b'] AS eq")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("tags")).isEqualTo(List.of("a", "b"));
      assertThat(r.<Boolean>getProperty("typed")).isTrue();
      assertThat(r.<Boolean>getProperty("eq")).isTrue();
    }
  }

  @Test
  void aListOfIntegersRefusesAnElementOfAnotherKind() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.v IS :: LIST<INTEGER NOT NULL>");
    command("CREATE (:L {v: [1, 2, 3]})");

    // the element is converted to the declared element type, and one that cannot be is refused
    assertThatThrownBy(() -> command("CREATE (:L {v: ['x']})")).isInstanceOf(RuntimeException.class);
    assertThat(count("MATCH (n:L) WHERE n.v IS :: LIST<INTEGER NOT NULL> RETURN count(n) AS c")).isEqualTo(1L);
  }

  @Test
  void theElementTypeOfAnExistingListIsReplaced() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.p LIST OF LONG");

    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: LIST<STRING>");
    assertThat(property("L", "p").getOfType()).isEqualTo("STRING");

    // a property has one type constraint: the other one is dropped first, which puts LIST OF LONG back, and LIST<ANY> then
    // lifts the element type
    command("DROP CONSTRAINT constraint_L_p_type");
    assertThat(property("L", "p").getOfType()).isEqualTo("LONG");
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: LIST<ANY>");
    assertThat(property("L", "p").getType()).isEqualTo(Type.LIST);
    assertThat(property("L", "p").getOfType()).isNull();
  }

  @Test
  void anExistingListOfTheSameElementTypeIsANoOp() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.p LIST OF STRING");

    try (final ResultSet rs = database.command("opencypher", "CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: STRING LIST")) {
      assertThat(rs.getStatistics().orElseThrow().getConstraintsAdded()).isZero();
    }
  }

  @Test
  void aNotNullOnTheTypeItselfIsRefused() {
    assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: STRING NOT NULL"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("IS NOT NULL");
    assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: LIST<STRING> NOT NULL"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void aListOfListsAndAUnionAreRefused() {
    assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: LIST<LIST<STRING>>"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("list of lists");
    assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: STRING LIST LIST"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("list of lists");
    assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: INTEGER | STRING"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("union");
  }

  /**
   * Each temporal type declares the native type that holds its values without loss, so a value written through the
   * constraint reads back equal to its literal and satisfies the constraint's own {@code IS ::}.
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {//
      "DATE|date('2020-01-01')|DATE",//
      "LOCAL DATETIME|localdatetime('2020-01-01T10:00:00.123456789')|DATETIME_NANOS",//
      "TIMESTAMP WITHOUT TIME ZONE|localdatetime('2020-01-01T10:00:00.123456789')|DATETIME_NANOS",//
      "ZONED DATETIME|datetime('2020-01-01T10:00:00.123456789+02:00')|ZONED_DATETIME",//
      "TIMESTAMP WITH TIME ZONE|datetime('2020-01-01T10:00:00.123456789+02:00')|ZONED_DATETIME",//
      "LOCAL TIME|localtime('10:00:00.123456789')|LOCAL_TIME",//
      "TIME WITHOUT TIME ZONE|localtime('10:00:00.123456789')|LOCAL_TIME",//
      "ZONED TIME|time('10:00:00.123456789+02:00')|OFFSET_TIME",//
      "TIME WITH TIME ZONE|time('10:00:00.123456789+02:00')|OFFSET_TIME",//
      "DURATION|duration('P1DT2H3.5S')|DURATION",//
      "POINT|point({x: 1, y: 2})|MAP" })
  void aTemporalTypeConstraintKeepsItsValues(final String cypherType, final String literal, final String declared) {
    command("CREATE CONSTRAINT FOR (n:T) REQUIRE n.p IS :: " + cypherType);
    assertThat(property("T", "p").getType()).isEqualTo(Type.valueOf(declared));

    command("CREATE (:T {p: " + literal + "})");

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:T) RETURN n.p IS :: " + cypherType + " AS typed, n.p = " + literal + " AS eq")) {
      final Result r = rs.next();
      assertThat(r.<Boolean>getProperty("typed")).isTrue();
      assertThat(r.<Boolean>getProperty("eq")).isTrue();
    }
  }

  /**
   * A property an earlier version declared LONG for {@code IS :: DURATION}: re-running the constraint retypes it to the native
   * DURATION when the stored records allow it, and refuses naming the property when one of them does not. Nothing is migrated
   * silently.
   */
  @Test
  void reRunningATemporalConstraintOverAnOldDeclarationRetypesItOrRefuses() {
    database.command("sql", "CREATE VERTEX TYPE Old");
    database.command("sql", "CREATE PROPERTY Old.d LONG");
    command("CREATE CONSTRAINT FOR (n:Old) REQUIRE n.d IS :: DURATION");
    assertThat(property("Old", "d").getType()).isEqualTo(Type.DURATION);

    database.command("sql", "CREATE VERTEX TYPE OldWithData");
    database.command("sql", "CREATE PROPERTY OldWithData.d LONG");
    database.transaction(() -> database.newVertex("OldWithData").set("d", 86_400L).save());
    // the stored 86400 converts to a DURATION, but it is not one: the record would violate the constraint created over it
    assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:OldWithData) REQUIRE n.d IS :: DURATION")).hasStackTraceContaining(
        "OldWithData.d IS :: DURATION");
    assertThat(property("OldWithData", "d").getType()).isEqualTo(Type.LONG);
    assertThat(names("SHOW CONSTRAINTS WHERE 'OldWithData' IN labelsOrTypes AND type = 'NODE_PROPERTY_TYPE' AND propertyType = 'DURATION'"))
        .isEmpty();
  }

  @Test
  void aTypeConstraintOverAValueOfAnotherKindIsRefusedAndDeclaresNothing() {
    command("CREATE (:Mixed {p: 'text'})");

    assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:Mixed) REQUIRE n.p IS :: INTEGER")).hasStackTraceContaining(
        "Mixed.p IS :: INTEGER");
    assertThat(database.getSchema().getType("Mixed").getPropertyIfExists("p")).isNull();
    assertThat(names("SHOW CONSTRAINTS WHERE 'Mixed' IN labelsOrTypes")).isEmpty();
  }

  @Test
  void aLocalDatetimeConstraintKeepsADatetimePropertyAtItsPrecision() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.p DATETIME");

    try (final ResultSet rs = database.command("opencypher", "CREATE CONSTRAINT FOR (n:T) REQUIRE n.p IS :: LOCAL DATETIME")) {
      assertThat(rs.getStatistics().orElseThrow().getConstraintsAdded()).isZero();
    }
    assertThat(property("T", "p").getType()).isEqualTo(Type.DATETIME);
  }

  /** The predicate and {@code valueType()} classify a value the same way, by Neo4j's type names. */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {//
      "date('2020-01-01')|DATE",//
      "localdatetime('2020-01-01T10:00')|LOCAL DATETIME",//
      "datetime('2020-01-01T10:00+02:00')|ZONED DATETIME",//
      "localtime('10:00')|LOCAL TIME",//
      "time('10:00+02:00')|ZONED TIME",//
      "duration('P1D')|DURATION",//
      "point({x: 1, y: 2})|POINT",//
      "{a: 1}|MAP" })
  void thePredicateAndValueTypeAgree(final String literal, final String type) {
    try (final ResultSet rs = database.query("opencypher",
        "RETURN " + literal + " IS :: " + type + " AS typed, valueType(" + literal + ") AS vt, " + literal + " IS :: STRING AS str")) {
      final Result r = rs.next();
      assertThat(r.<Boolean>getProperty("typed")).isTrue();
      assertThat(r.<String>getProperty("vt")).isEqualTo(type + " NOT NULL");
      assertThat(r.<Boolean>getProperty("str")).isFalse();
    }
  }

  @Test
  void aPointIsNotAMap() {
    try (final ResultSet rs = database.query("opencypher", "RETURN point({x: 1, y: 2}) IS :: MAP AS m")) {
      assertThat(rs.next().<Boolean>getProperty("m")).isFalse();
    }
  }

  @Test
  void theListElementNullabilityIsChecked() {
    try (final ResultSet rs = database.query("opencypher",
        "RETURN [1, null] IS :: LIST<INTEGER NOT NULL> AS strict, [1, null] IS :: LIST<INTEGER> AS lenient, "
            + "[1, 2] IS :: INTEGER LIST AS postfix")) {
      final Result r = rs.next();
      assertThat(r.<Boolean>getProperty("strict")).isFalse();
      assertThat(r.<Boolean>getProperty("lenient")).isTrue();
      assertThat(r.<Boolean>getProperty("postfix")).isTrue();
    }
  }

  private void command(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private Property property(final String typeName, final String propertyName) {
    final Property p = database.getSchema().getType(typeName).getPropertyIfExists(propertyName);
    assertThat(p).isNotNull();
    return p;
  }

  private List<Object> names(final String cypher) {
    final List<Object> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }
}
