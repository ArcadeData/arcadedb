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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10862: {@code CREATE CONSTRAINT ... REQUIRE n.e IS :: VECTOR<FLOAT32>(768)} was refused. It now declares the array
 * type the coordinate type maps to and enforces the dimension on every write through the property's MIN and MAX (an item
 * count), which the constraint owns and gives back on {@code DROP CONSTRAINT}. The {@code IS ::} predicate answers the same
 * vector type, so a constraint is refused when the stored data already violates it.
 */
class Issue10862VectorTypeConstraintTest extends TestHelper {

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {//
      "VECTOR<FLOAT32>(3)|ARRAY_OF_FLOATS",//
      "VECTOR(3, FLOAT32)|ARRAY_OF_FLOATS",//
      "VECTOR<FLOAT64>(3)|ARRAY_OF_DOUBLES",//
      "VECTOR<FLOAT>(3)|ARRAY_OF_DOUBLES",//
      "VECTOR<INT64>(3)|ARRAY_OF_LONGS",//
      "VECTOR<INTEGER>(3)|ARRAY_OF_LONGS",//
      "VECTOR<INT32>(3)|ARRAY_OF_INTEGERS",//
      "VECTOR<INT16>(3)|ARRAY_OF_SHORTS",//
      "VECTOR<INT8>(3)|BINARY" })
  void aVectorTypeDeclaresTheArrayOfItsCoordinateTypeBoundedByItsDimension(final String cypherType, final String arcadeType) {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.e IS :: " + cypherType);

    final Property p = property("L", "e");
    assertThat(p.getType()).isEqualTo(Type.valueOf(arcadeType));
    assertThat(p.getMin()).isEqualTo("3");
    assertThat(p.getMax()).isEqualTo("3");
  }

  @Test
  void theDimensionIsEnforcedOnEveryWrite() {
    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");

    command("CREATE (:L {e: [0.1, 0.2, 0.3]})");
    command("CREATE (:L {e: vector([1, 2, 3], 3, FLOAT32)})");
    assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2]})")).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2, 0.3, 0.4]})")).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> command("MATCH (n:L) SET n.e = [1.0]")).isInstanceOf(RuntimeException.class);

    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isEqualTo(2L);
    // what is stored satisfies the constraint's own type
    assertThat(count("MATCH (n:L) WHERE n.e IS :: VECTOR<FLOAT32>(3) RETURN count(n) AS c")).isEqualTo(2L);
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {//
      "VECTOR<FLOAT32>(3)", "VECTOR<FLOAT>(3)", "VECTOR<INTEGER>(3)", "VECTOR<INT32>(3)", "VECTOR<INT16>(3)" })
  void aWrittenListIsStoredAsAVectorOfTheConstraintsOwnType(final String cypherType) {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.e IS :: " + cypherType);

    command("CREATE (:L {e: [1, 2, 3]})");
    command("CREATE (:L {e: vector([1, 2, 3], 3, FLOAT32)})");
    assertThat(count("MATCH (n:L) WHERE n.e IS :: " + cypherType + " RETURN count(n) AS c")).isEqualTo(2L);
    assertThatThrownBy(() -> command("CREATE (:L {e: [1, 2]})")).isInstanceOf(RuntimeException.class);
  }

  @Test
  void anInt8VectorIsTheByteArrayOfABinary() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.e IS :: VECTOR<INT8>(3)");

    command("CREATE (:L {e: [1, 2, 3]})");
    assertThat(count("MATCH (n:L) WHERE n.e IS :: VECTOR<INT8>(3) RETURN count(n) AS c")).isEqualTo(1L);
    assertThatThrownBy(() -> command("CREATE (:L {e: [1, 2]})")).isInstanceOf(RuntimeException.class);
    // vector() builds a float[] whatever coordinate type it is given (#10917), which a BINARY refuses rather than truncates
    assertThatThrownBy(() -> command("CREATE (:L {e: vector([1, 2, 3], 3, INT8)})")).rootCause()
        .hasMessageContaining("cannot be converted to type byte[]");
  }

  @Test
  void thePredicateAnswersTheVectorType() {
    try (final ResultSet rs = database.query("opencypher", "WITH vector([1, 2, 3], 3, FLOAT32) AS v RETURN "
        + "v IS :: VECTOR<FLOAT32>(3) AS exact, v IS :: VECTOR<FLOAT32>(4) AS otherDimension, v IS :: VECTOR(3, FLOAT32) AS spelled, "
        + "v IS :: VECTOR<FLOAT>(3) AS wider, v IS :: VECTOR<INT32>(3) AS integers, v IS :: VECTOR AS bare, "
        + "[1.0, 2.0, 3.0] IS :: VECTOR<FLOAT32>(3) AS list, null IS :: VECTOR<FLOAT32>(3) AS nullable, "
        + "null IS :: VECTOR<FLOAT32>(3) NOT NULL AS notNull")) {
      final Result r = rs.next();
      assertThat(r.<Boolean>getProperty("exact")).isTrue();
      assertThat(r.<Boolean>getProperty("otherDimension")).isFalse();
      assertThat(r.<Boolean>getProperty("spelled")).isTrue();
      assertThat(r.<Boolean>getProperty("wider")).isTrue();
      assertThat(r.<Boolean>getProperty("integers")).isFalse();
      assertThat(r.<Boolean>getProperty("bare")).isTrue();
      // a list is not a vector
      assertThat(r.<Boolean>getProperty("list")).isFalse();
      assertThat(r.<Boolean>getProperty("nullable")).isTrue();
      assertThat(r.<Boolean>getProperty("notNull")).isFalse();
    }
  }

  @Test
  void aConstraintTheStoredDataViolatesIsRefusedAndLeavesNothingBehind() {
    database.transaction(() -> {
      database.getSchema().createVertexType("L");
      database.newVertex("L").set("e", new float[] { 1F, 2F }).save();
    });

    assertThatThrownBy(() -> command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)")).hasMessageContaining(
        "holds float[2]");
    assertThat(database.getSchema().getType("L").getPropertyIfExists("e")).isNull();
    assertThat(names("SHOW CONSTRAINTS YIELD name")).isEmpty();
  }

  @Test
  void aRefusalOverADeclaredPropertyTakesBackTheBoundsItSet() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.e ARRAY_OF_FLOATS");
    database.transaction(() -> database.newVertex("L").set("e", new float[] { 1F, 2F }).save());

    // the declaration was there before: the refusal keeps it, without the MIN and MAX the statement set
    assertThatThrownBy(() -> command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)")).hasMessageContaining(
        "holds float[2]");
    final Property p = property("L", "e");
    assertThat(p.getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
    assertThat(p.getMin()).isNull();
    assertThat(p.getMax()).isNull();
    assertThat(names("SHOW CONSTRAINTS YIELD name")).doesNotContain("t");
  }

  @Test
  void aStoredListIsNotAVectorSoTheConstraintIsRefused() {
    // a list written before any declaration stays a list: it answers false to IS :: VECTOR, as in Neo4j
    command("CREATE (:L {e: [1.0, 2.0, 3.0]})");

    assertThatThrownBy(() -> command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)")).hasMessageContaining(
        "(LIST<ANY>)");
    assertThat(database.getSchema().getType("L").getPropertyIfExists("e")).isNull();
  }

  @Test
  void aDropWhoseRestoreIsRefusedLeavesTheDimensionInPlace() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.e LONG");
    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
    command("CREATE (:L {e: [0.1, 0.2, 0.3]})");

    // the LONG declaration the constraint replaced cannot hold the stored vector: the drop is refused and changes nothing
    assertThatThrownBy(() -> command("DROP CONSTRAINT t")).isInstanceOf(RuntimeException.class);
    final Property p = property("L", "e");
    assertThat(p.getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
    assertThat(p.getMin()).isEqualTo("3");
    assertThat(p.getMax()).isEqualTo("3");
    assertThat(names("SHOW CONSTRAINTS YIELD name")).containsExactly("t");
    assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2]})")).isInstanceOf(RuntimeException.class);
  }

  @Test
  void dropConstraintGivesTheDeclarationBack() {
    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
    command("DROP CONSTRAINT t");

    assertThat(database.getSchema().getType("L").getPropertyIfExists("e")).isNull();
    command("CREATE (:L {e: [1.0]})");
  }

  @Test
  void aConstraintOverAVectorIndexPropertyOwnsOnlyTheDimension() {
    // CREATE VECTOR INDEX declares the property ARRAY_OF_FLOATS itself (#10678): the constraint adds the dimension only
    command("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3}}");
    assertThat(property("L", "e").getType()).isEqualTo(Type.ARRAY_OF_FLOATS);

    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
    assertThat(property("L", "e").getMin()).isEqualTo("3");
    assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2]})")).isInstanceOf(RuntimeException.class);

    command("DROP CONSTRAINT t");
    final Property p = property("L", "e");
    assertThat(p.getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
    assertThat(p.getMin()).isNull();
    assertThat(p.getMax()).isNull();
  }

  @Test
  void anotherDimensionOfTheSamePropertyConflicts() {
    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");

    assertThatThrownBy(() -> command("CREATE CONSTRAINT t2 FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(4)")).rootCause().hasMessageContaining(
        "already requires another type");
    // the same requirement again is a no-op
    command("CREATE CONSTRAINT IF NOT EXISTS FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
    assertThat(names("SHOW CONSTRAINTS YIELD name")).containsExactly("t");
  }

  @Test
  void dimensionBoundsPassToAnEquivalentConstraint() {
    command("CREATE CONSTRAINT a FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
    command("CREATE CONSTRAINT b FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");

    command("DROP CONSTRAINT a");
    assertThat(property("L", "e").getMax()).isEqualTo("3");
    assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2]})")).isInstanceOf(RuntimeException.class);

    command("DROP CONSTRAINT b");
    assertThat(database.getSchema().getType("L").getPropertyIfExists("e")).isNull();
  }

  @Test
  void theReplacedDeclarationWaitsForTheLastEquivalentConstraint() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.e LONG");
    command("CREATE CONSTRAINT a FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
    command("CREATE CONSTRAINT b FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");

    // b still requires the vector: dropping a neither restores LONG nor lifts the dimension
    command("DROP CONSTRAINT a");
    Property p = property("L", "e");
    assertThat(p.getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
    assertThat(p.getMax()).isEqualTo("3");
    assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2]})")).isInstanceOf(RuntimeException.class);

    // the last one puts the declaration a replaced back
    command("DROP CONSTRAINT b");
    p = property("L", "e");
    assertThat(p.getType()).isEqualTo(Type.LONG);
    assertThat(p.getMin()).isNull();
    assertThat(p.getMax()).isNull();
  }

  @Test
  void aScalarTypeConstraintAlsoHandsTheReplacedDeclarationOn() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.p LONG");
    command("CREATE CONSTRAINT a FOR (n:L) REQUIRE n.p IS :: STRING");
    command("CREATE CONSTRAINT b FOR (n:L) REQUIRE n.p IS :: STRING");

    command("DROP CONSTRAINT a");
    assertThat(property("L", "p").getType()).isEqualTo(Type.STRING);
    command("DROP CONSTRAINT b");
    assertThat(property("L", "p").getType()).isEqualTo(Type.LONG);
  }

  @Test
  void boundsAlreadyOnThePropertyThatSaySomethingElseAreRefused() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.e ARRAY_OF_FLOATS (MAX 10)");

    assertThatThrownBy(() -> command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)")).hasMessageContaining(
        "MAX 10");
    assertThat(property("L", "e").getMax()).isEqualTo("10");
  }

  @Test
  void aRetypedPropertyGetsItsDeclarationBackOnDrop() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.e LIST");

    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
    assertThat(property("L", "e").getType()).isEqualTo(Type.ARRAY_OF_FLOATS);

    command("DROP CONSTRAINT t");
    final Property p = property("L", "e");
    assertThat(p.getType()).isEqualTo(Type.LIST);
    assertThat(p.getMin()).isNull();
    assertThat(p.getMax()).isNull();
  }

  @Test
  void showConstraintsListsTheVectorType() {
    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR(3, FLOAT64)");

    try (final ResultSet rs = database.query("opencypher", "SHOW CONSTRAINTS YIELD name, propertyType")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("propertyType")).isEqualTo("VECTOR<FLOAT>(3)");
    }
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {//
      "VECTOR<FLOAT32>|coordinate type and its dimension",//
      "VECTOR(3)|coordinate type and its dimension",//
      "VECTOR|coordinate type and its dimension",//
      "VECTOR<FLOAT32>(3) NOT NULL|cannot require NOT NULL",//
      "VECTOR<FLOAT32>(0)|positive integer",//
      "LIST<VECTOR<FLOAT32>(3)>|list of vectors" })
  void anIncompleteOrUnsupportedVectorTypeIsRefused(final String cypherType, final String message) {
    assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.e IS :: " + cypherType)).isInstanceOf(
        CommandParsingException.class).hasMessageContaining(message);
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
