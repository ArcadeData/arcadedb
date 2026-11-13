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
import com.arcadedb.database.DocumentValidator;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cypher constraints: property type constraints, declared types and string patterns, constraint names and DROP CONSTRAINT.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherTypedConstraintTest {
  /**
   * Regression test for issue #9304: {@code REQUIRE n.x IS :: TYPE} dropped and recreated an existing property, losing every
   * other constraint of its declaration, and accepted a type the stored values do not satisfy.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9304TypedConstraintKeepsDeclaration extends TestHelper {

    @Test
    void typedConstraintKeepsTheOtherAttributesOfTheProperty() {
      database.command("sql", "CREATE VERTEX TYPE B");
      database.command("sql", "CREATE PROPERTY B.code STRING");
      final Property declared = database.getSchema().getType("B").getProperty("code");
      // REGEXP and MAX are written in STRING terms and are not carried onto a LONG (#9645): see
      // CypherTypedConstraintTest.Issue9645TypedConstraintTypeDependentAttributes for the attributes that depend on the type
      declared.setMandatory(true).setNotNull(true).setReadonly(true).setDefaultValue("111");

      database.command("opencypher", "CREATE CONSTRAINT FOR (n:B) REQUIRE n.code IS :: INTEGER").close();

      final Property p = database.getSchema().getType("B").getProperty("code");
      assertThat(p.getType()).isEqualTo(Type.LONG);
      assertThat(p.isMandatory()).isTrue();
      assertThat(p.isNotNull()).isTrue();
      assertThat(p.isReadonly()).isTrue();
      assertThat(p.getDefaultValueDefinition()).isEqualTo("111");
    }

    @Test
    void notNullThenTypedStillRefusesANodeWithoutTheProperty() {
      database.command("opencypher", "CREATE CONSTRAINT FOR (n:C) REQUIRE n.code IS NOT NULL").close();
      database.command("opencypher", "CREATE CONSTRAINT FOR (n:C) REQUIRE n.code IS :: INTEGER").close();

      assertThat(database.getSchema().getType("C").getProperty("code").isMandatory()).isTrue();
      assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "CREATE (a:C {other: 1})").close()))
          .isInstanceOf(Exception.class);
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:C) RETURN count(n) AS c")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
      }
    }

    @Test
    void typedConstraintOverNonConformingDataIsRefusedAndLeavesThePropertyUntouched() {
      database.command("sql", "CREATE VERTEX TYPE A");
      database.command("sql", "CREATE PROPERTY A.x STRING");
      database.getSchema().getType("A").getProperty("x").setMax("10");
      database.transaction(() -> database.command("sql", "CREATE VERTEX A SET x = 'hello'").close());

      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT cA FOR (n:A) REQUIRE n.x IS :: INTEGER"))
          .isInstanceOf(CommandExecutionException.class);

      final Property p = database.getSchema().getType("A").getProperty("x");
      assertThat(p.getType()).isEqualTo(Type.STRING);
      assertThat(p.getMax()).isEqualTo("10");
      try (final ResultSet rs = database.query("sql", "SELECT x FROM A")) {
        assertThat(rs.next().<String>getProperty("x")).isEqualTo("hello");
      }
    }

    @Test
    void retypingAnIndexedPropertyIsRefusedAndLeavesTheDeclarationAndIndexUntouched() {
      database.command("sql", "CREATE VERTEX TYPE D");
      database.command("sql", "CREATE PROPERTY D.k STRING");
      database.command("sql", "CREATE INDEX ON D (k) UNIQUE");
      database.getSchema().getType("D").getProperty("k").setMandatory(true);
      database.transaction(() -> database.command("sql", "CREATE VERTEX D SET k = 'abc'").close());

      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:D) REQUIRE n.k IS :: INTEGER"))
          .isInstanceOf(CommandExecutionException.class);
      Property p = database.getSchema().getType("D").getProperty("k");
      assertThat(p.getType()).isEqualTo(Type.STRING);
      assertThat(p.isMandatory()).isTrue();
      assertThat(database.getSchema().getType("D").getPolymorphicIndexByProperties("k")).isNotNull();

      // an indexed property cannot be retyped: the refusal is clear and leaves the declaration and the index as they were
      database.transaction(() -> database.command("sql", "DELETE FROM D").close());
      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:D) REQUIRE n.k IS :: INTEGER"))
          .hasRootCauseInstanceOf(SchemaException.class);
      p = database.getSchema().getType("D").getProperty("k");
      assertThat(p.getType()).isEqualTo(Type.STRING);
      assertThat(p.isMandatory()).isTrue();
      assertThat(database.getSchema().getType("D").getPolymorphicIndexByProperties("k")).isNotNull();
    }
  }

  /**
   * Regression test for issue #9645: {@code REQUIRE n.x IS :: TYPE} carried the OF, REGEXP, MIN, MAX and DEFAULT of the old
   * declaration onto the new type as they were, leaving a property no value could be written through. They are now carried
   * over only when the new type reads them the same way; otherwise the statement is refused naming them and the declaration is
   * left as it was.
   */
  @Nested
  class Issue9645TypedConstraintTypeDependentAttributes extends TestHelper {

    @Test
    void theIssueReproductionIsRefusedNamingTheAttributesAndLeavesTheDeclarationUntouched() {
      database.command("sql", "CREATE VERTEX TYPE B");
      database.command("sql",
          "CREATE PROPERTY B.code STRING (mandatory true, notnull true, readonly true, regexp '[A-Z]+', max 4, default 'AAA')");

      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT cb FOR (n:B) REQUIRE n.code IS :: INTEGER"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("B.code")
          .hasMessageContaining("REGEXP '[A-Z]+'")
          .hasMessageContaining("MAX '4'");

      final Property p = database.getSchema().getType("B").getProperty("code");
      assertThat(p.getType()).isEqualTo(Type.STRING);
      assertThat(p.getRegexp()).isEqualTo("[A-Z]+");
      assertThat(p.getMax()).isEqualTo("4");
      assertThat(p.getDefaultValue()).isEqualTo("AAA");
      assertThat(p.isMandatory()).isTrue();

      // the property still works as it did
      database.transaction(() -> database.command("sql", "INSERT INTO B SET other = 1").close());
      try (final ResultSet rs = database.query("sql", "SELECT code FROM B")) {
        assertThat(rs.next().<String>getProperty("code")).isEqualTo("AAA");
      }
    }

    @Test
    void aDefaultThatDoesNotEvaluateUnderTheNewTypeIsRefusedAndTheDeclarationIsRestored() {
      database.command("sql", "CREATE VERTEX TYPE D");
      database.command("sql", "CREATE PROPERTY D.code STRING (mandatory true, default 'AAA')");

      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:D) REQUIRE n.code IS :: INTEGER"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("DEFAULT `'AAA'`")
          // Cypher's INTEGER is a 64-bit integer, declared as LONG
          .hasMessageContaining("LONG");

      final Property p = database.getSchema().getType("D").getProperty("code");
      assertThat(p.getType()).isEqualTo(Type.STRING);
      assertThat(p.isMandatory()).isTrue();
      assertThat(p.getDefaultValue()).isEqualTo("AAA");
    }

    @Test
    void aDefaultThatConvertsToTheNewTypeIsCarriedOverAndApplied() {
      database.command("sql", "CREATE VERTEX TYPE E");
      database.command("sql", "CREATE PROPERTY E.code STRING (mandatory true, notnull true, default '12')");

      database.command("opencypher", "CREATE CONSTRAINT FOR (n:E) REQUIRE n.code IS :: INTEGER").close();

      final Property p = database.getSchema().getType("E").getProperty("code");
      assertThat(p.getType()).isEqualTo(Type.LONG);
      assertThat(p.isMandatory()).isTrue();
      assertThat(p.isNotNull()).isTrue();
      assertThat(p.getDefaultValue()).isEqualTo(12L);

      database.transaction(() -> database.command("sql", "INSERT INTO E SET other = 1").close());
      database.transaction(() -> database.command("opencypher", "CREATE (e:E {code: 7})").close());
      try (final ResultSet rs = database.query("sql", "SELECT code FROM E ORDER BY code")) {
        assertThat(rs.next().<Long>getProperty("code")).isEqualTo(7L);
        assertThat(rs.next().<Long>getProperty("code")).isEqualTo(12L);
      }
    }

    @Test
    void boundsAndDefaultBetweenTwoNumericTypesAreCarriedOverAndSurviveAReopen() {
      database.command("sql", "CREATE VERTEX TYPE N");
      database.command("sql", "CREATE PROPERTY N.n INTEGER (mandatory true, readonly true, min 1, max 100, default 5)");
      database.getSchema().getType("N").getProperty("n").setHidden(true).setCustomValue("note", "kept");

      database.command("opencypher", "CREATE CONSTRAINT FOR (n:N) REQUIRE n.n IS :: INTEGER").close();

      reopenDatabase();

      final Property p = database.getSchema().getType("N").getProperty("n");
      assertThat(p.getType()).isEqualTo(Type.LONG);
      assertThat(p.getMin()).isEqualTo("1");
      assertThat(p.getMax()).isEqualTo("100");
      assertThat(p.getDefaultValue()).isEqualTo(5L);
      assertThat(p.isMandatory()).isTrue();
      assertThat(p.isReadonly()).isTrue();
      assertThat(p.isHidden()).isTrue();
      assertThat(p.getCustomValue("note")).isEqualTo("kept");

      database.transaction(() -> database.command("sql", "INSERT INTO N SET other = 1").close());
      database.transaction(() -> database.command("sql", "INSERT INTO N SET n = 100").close());
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO N SET n = 101").close()))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("N.n");
    }

    @Test
    void aBoundThatChangesMeaningIsRefused() {
      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.n LONG (min 10)");

      // a minimum value of 10 on a LONG would become a minimum length of 10 characters on a STRING
      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:L) REQUIRE n.n IS :: STRING"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("MIN '10'");
      assertThat(database.getSchema().getType("L").getProperty("n").getType()).isEqualTo(Type.LONG);
    }

    @Test
    void aLengthBoundIsRefusedOnACollection() {
      database.command("sql", "CREATE VERTEX TYPE S");
      database.command("sql", "CREATE PROPERTY S.v STRING (max 4)");

      // 4 characters on a STRING would become 4 entries on a MAP
      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:S) REQUIRE n.v IS :: MAP"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("MAX '4'");
      assertThat(database.getSchema().getType("S").getProperty("v").getType()).isEqualTo(Type.STRING);
    }

    @Test
    void dateBoundsAndDefaultAreCarriedOverBetweenDateTypes() {
      database.command("sql", "CREATE VERTEX TYPE W");
      database.command("sql", "CREATE PROPERTY W.d DATE (min '2020-01-01', default '2021-06-01')");

      database.command("opencypher", "CREATE CONSTRAINT FOR (n:W) REQUIRE n.d IS :: LOCAL DATETIME").close();

      final Property p = database.getSchema().getType("W").getProperty("d");
      assertThat(p.getType()).isEqualTo(Type.DATETIME_NANOS);
      assertThat(p.getMin()).isEqualTo("2020-01-01");
      assertThat(p.getDefaultValue()).isNotNull();

      // the carried default gives the value the same default declared directly on a DATETIME gives
      database.command("sql", "CREATE VERTEX TYPE W2");
      database.command("sql", "CREATE PROPERTY W2.d DATETIME_NANOS (default '2021-06-01')");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO W SET other = 1").close();
        database.command("sql", "INSERT INTO W2 SET other = 1").close();
      });
      final Object direct;
      try (final ResultSet rs = database.query("sql", "SELECT d FROM W2")) {
        direct = rs.next().getProperty("d");
      }
      assertThat(direct).isNotNull();
      try (final ResultSet rs = database.query("sql", "SELECT d FROM W")) {
        assertThat(rs.next().<Object>getProperty("d")).isEqualTo(direct);
      }
      // the carried MIN is still enforced on the new type
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO W SET d = '2019-12-31 00:00:00'").close()))
          .isInstanceOf(ValidationException.class);
    }

    @Test
    void aNumericDefaultThatWouldLoseItsValueIsRefused() {
      database.command("sql", "CREATE VERTEX TYPE G");
      database.command("sql", "CREATE PROPERTY G.n DOUBLE (default 3.7)");

      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:G) REQUIRE n.n IS :: INTEGER"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("DEFAULT `3.7`");
      final Property p = database.getSchema().getType("G").getProperty("n");
      assertThat(p.getType()).isEqualTo(Type.DOUBLE);
      assertThat(p.getDefaultValue()).isEqualTo(3.7d);
    }

    @Test
    void aNumericDefaultThatKeepsItsValueIsCarriedOver() {
      database.command("sql", "CREATE VERTEX TYPE H");
      database.command("sql", "CREATE PROPERTY H.n DOUBLE (default 3.0)");

      database.command("opencypher", "CREATE CONSTRAINT FOR (n:H) REQUIRE n.n IS :: INTEGER").close();

      final Property p = database.getSchema().getType("H").getProperty("n");
      assertThat(p.getType()).isEqualTo(Type.LONG);
      assertThat(p.getDefaultValue()).isEqualTo(3L);
    }

    @Test
    void aBoundThatOverflowsTheNarrowerTypeIsRefused() {
      database.command("sql", "CREATE VERTEX TYPE O");
      database.command("sql", "CREATE PROPERTY O.n LONG (max 5000000000)");

      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:O) REQUIRE n.n IS :: INT32"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("MAX '5000000000'");
      assertThat(database.getSchema().getType("O").getProperty("n").getType()).isEqualTo(Type.LONG);
    }

    @Test
    void everyTypeThatTakesABoundHasAMeasure() {
      // boundMeasure and unreadableBound group the types in two switches: a type added to one must be added to the other
      for (final Type type : Type.values())
        if (DocumentValidator.isBoundApplicable(type))
          assertThat(DocumentValidator.boundMeasure(type)).as(type.name()).isNotNull();
    }

    @Test
    void aBoundTheNewTypeCannotReadIsRefused() {
      database.command("sql", "CREATE VERTEX TYPE F");
      database.command("sql", "CREATE PROPERTY F.n DOUBLE (max 2.5)");

      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:F) REQUIRE n.n IS :: INTEGER"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("MAX '2.5'");
      assertThat(database.getSchema().getType("F").getProperty("n").getType()).isEqualTo(Type.DOUBLE);
    }

    @Test
    void aRegexpIsCarriedOverOntoAStringProperty() {
      database.command("sql", "CREATE VERTEX TYPE R");
      database.command("sql", "CREATE PROPERTY R.code LONG (regexp '[0-9]+')");

      database.command("opencypher", "CREATE CONSTRAINT FOR (n:R) REQUIRE n.code IS :: STRING").close();

      final Property p = database.getSchema().getType("R").getProperty("code");
      assertThat(p.getType()).isEqualTo(Type.STRING);
      assertThat(p.getRegexp()).isEqualTo("[0-9]+");
      database.transaction(() -> database.command("sql", "INSERT INTO R SET code = '123'").close());
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO R SET code = 'abc'").close()))
          .hasMessageContaining("regular expression");
    }

    @Test
    void anElementTypeIsRefusedOnATypeWithoutElements() {
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE PROPERTY T.tags LIST OF STRING");

      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:T) REQUIRE n.tags IS :: STRING"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("OF 'STRING'");
      final Property p = database.getSchema().getType("T").getProperty("tags");
      assertThat(p.getType()).isEqualTo(Type.LIST);
      assertThat(p.getOfType()).isEqualTo("STRING");
    }

    @Test
    void anElementTypeIsCarriedOverBetweenListAndMap() {
      database.command("sql", "CREATE VERTEX TYPE M");
      database.command("sql", "CREATE PROPERTY M.tags LIST OF STRING");

      database.command("opencypher", "CREATE CONSTRAINT FOR (n:M) REQUIRE n.tags IS :: MAP").close();

      final Property p = database.getSchema().getType("M").getProperty("tags");
      assertThat(p.getType()).isEqualTo(Type.MAP);
      assertThat(p.getOfType()).isEqualTo("STRING");
    }
  }

  /**
   * Regression test for issue #9695: a value written into a property declared {@code STRING} is stored as its text, so the
   * same pattern has to find it again. Before, {@code MERGE (p:S {d: 1})} stored {@code '1'}, and neither the next
   * {@code MERGE} nor any {@code MATCH} ({@code {d: 1}} or {@code WHERE p.d = 1}) ever equalled it: every repeated MERGE
   * created a duplicate, or died on the UNIQUE index. A pattern operand is now compared with the text a write of it would
   * store, in MATCH and MERGE alike, and only against a declared STRING property: a schemaless {@code '1'} still differs
   * from {@code 1}, as the Cypher TCK requires.
   */
  @Nested
  class Issue9695DeclaredStringPattern extends TestHelper {

    private static final String[] OPERANDS = { "1", "-7", "1.5", "true", "datetime('2021-06-15T12:30:00Z')",
        "localdatetime('2021-06-15T12:30:00')", "date('2021-06-15')", "time('12:30:00Z')", "localtime('12:30:00')",
        "duration('P1DT2H')", "[1, 2]" };

    private long count(final String query) {
      return count(query, Map.of());
    }

    private long count(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        return rs.next().<Long>getProperty("c");
      }
    }

    private String merge(final String query) {
      final String[] tag = new String[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", query)) {
          final Result row = rs.next();
          tag[0] = row.getProperty("tag");
        }
      });
      return tag[0];
    }

    private void declare(final String type, final String index) {
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".d STRING");
      if (index != null)
        database.command("sql", "CREATE INDEX ON " + type + " (d) " + index);
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "NOTUNIQUE", "UNIQUE" })
    void aRepeatedMergeIsIdempotentOnADeclaredStringProperty(final String index) {
      final String type = "M" + index;
      declare(type, index.isEmpty() ? null : index);
      for (final String operand : OPERANDS) {
        database.transaction(() -> database.command("opencypher", "MATCH (n:" + type + ") DELETE n").close());
        final String described = type + " " + operand;

        assertThat(merge("MERGE (p:" + type + " {d: " + operand + "}) ON CREATE SET p.tag = 'CREATED' ON MATCH SET p.tag = 'MATCHED' RETURN p.tag AS tag"))
            .as(described + " first MERGE").isEqualTo("CREATED");
        assertThat(merge("MERGE (p:" + type + " {d: " + operand + "}) ON CREATE SET p.tag = 'CREATED' ON MATCH SET p.tag = 'MATCHED' RETURN p.tag AS tag"))
            .as(described + " second MERGE finds what the first stored").isEqualTo("MATCHED");
        assertThat(count("MATCH (p:" + type + ") RETURN count(p) AS c")).as(described + " total").isEqualTo(1L);
      }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "NOTUNIQUE", "UNIQUE" })
    void matchFindsWhatTheSamePatternStored(final String index) {
      final String type = "F" + index;
      declare(type, index.isEmpty() ? null : index);
      for (final String operand : OPERANDS) {
        database.transaction(() -> database.command("opencypher", "MATCH (n:" + type + ") DELETE n").close());
        database.transaction(() -> database.command("opencypher", "CREATE (:" + type + " {d: " + operand + "})").close());
        final String described = type + " " + operand;

        assertThat(count("MATCH (p:" + type + " {d: " + operand + "}) RETURN count(p) AS c")).as(described + " inline").isEqualTo(1L);
        assertThat(count("MATCH (p:" + type + ") WHERE p.d = " + operand + " RETURN count(p) AS c")).as(described + " WHERE =")
            .isEqualTo(1L);
        assertThat(count("MATCH (p:" + type + ") WHERE " + operand + " = p.d RETURN count(p) AS c")).as(described + " WHERE = reversed")
            .isEqualTo(1L);
        assertThat(count("MATCH (p:" + type + ") WHERE p.d <> " + operand + " RETURN count(p) AS c")).as(described + " WHERE <>")
            .isEqualTo(0L);
        // The same comparison inside a projection, which the expression evaluator answers
        assertThat(count("MATCH (p:" + type + ") RETURN sum(CASE WHEN p.d = " + operand + " THEN 1 ELSE 0 END) AS c"))
            .as(described + " projected =").isEqualTo(1L);
        assertThat(count("MATCH (p:" + type + ") WITH p.d = " + operand + " AS eq, p.d <> " + operand + " AS ne WHERE eq AND NOT ne RETURN count(*) AS c"))
            .as(described + " projected comparison").isEqualTo(1L);
      }
    }

    @Test
    void aDifferentValueStillDoesNotMatch() {
      declare("D", "NOTUNIQUE");
      database.transaction(() -> database.command("opencypher", "CREATE (:D {d: 1})").close());
      assertThat(count("MATCH (p:D {d: 2}) RETURN count(p) AS c")).isEqualTo(0L);
      assertThat(count("MATCH (p:D) WHERE p.d = 2 RETURN count(p) AS c")).isEqualTo(0L);
      assertThat(count("MATCH (p:D) WHERE p.d <> 2 RETURN count(p) AS c")).isEqualTo(1L);
      // A map is never stored, so it has no text to be found by
      assertThat(count("MATCH (p:D) WHERE p.d = {a: 1} RETURN count(p) AS c")).isEqualTo(0L);
      // 1.0 is stored as '1.0': a different text from the '1' the integer stored
      assertThat(count("MATCH (p:D {d: 1.0}) RETURN count(p) AS c")).isEqualTo(0L);
      assertThat(merge("MERGE (p:D {d: 2}) ON CREATE SET p.tag = 'CREATED' RETURN p.tag AS tag")).isEqualTo("CREATED");
      assertThat(count("MATCH (p:D) RETURN count(p) AS c")).isEqualTo(2L);
    }

    @Test
    void aTextOperandStillMatchesTheText() {
      declare("T", "UNIQUE");
      database.transaction(() -> database.command("opencypher", "CREATE (:T {d: 1})").close());
      assertThat(count("MATCH (p:T {d: '1'}) RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(merge("MERGE (p:T {d: '1'}) ON MATCH SET p.tag = 'MATCHED' RETURN p.tag AS tag")).isEqualTo("MATCHED");
    }

    /** The Cypher typing of {@code =} is unchanged where nothing is declared: {@code '1' = 1} is false (TCK). */
    @Test
    void aSchemalessPropertyKeepsCypherTyping() {
      database.command("sql", "CREATE VERTEX TYPE U");
      database.transaction(() -> database.command("opencypher", "CREATE (:U {d: '1'})").close());
      assertThat(count("MATCH (p:U {d: 1}) RETURN count(p) AS c")).isEqualTo(0L);
      assertThat(count("MATCH (p:U) WHERE p.d = 1 RETURN count(p) AS c")).isEqualTo(0L);
      database.transaction(() -> database.command("opencypher", "MERGE (p:U {d: 1})").close());
      assertThat(count("MATCH (p:U) RETURN count(p) AS c")).isEqualTo(2L);
    }

    @Test
    void anInheritedDeclarationCoercesToo() {
      declare("Base", null);
      database.command("sql", "CREATE VERTEX TYPE Child EXTENDS Base");
      database.transaction(() -> database.command("opencypher", "MERGE (:Child {d: 42})").close());
      database.transaction(() -> database.command("opencypher", "MERGE (:Child {d: 42})").close());
      assertThat(count("MATCH (p:Child) RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (p:Base {d: 42}) RETURN count(p) AS c")).isEqualTo(1L);
    }

    @Test
    void parametersAreCoercedLikeLiterals() {
      declare("P", "UNIQUE");
      final Map<String, Object> number = Map.of("d", 7L);
      final Map<String, Object> zoned = Map.of("d", ZonedDateTime.parse("2021-06-15T12:30:00Z"));
      for (final Map<String, Object> params : new Map[] { number, zoned }) {
        database.transaction(() -> database.command("opencypher", "MERGE (p:P {d: $d})", params).close());
        database.transaction(() -> database.command("opencypher", "MERGE (p:P {d: $d})", params).close());
        assertThat(count("MATCH (p:P {d: $d}) RETURN count(p) AS c", params)).as(params.toString()).isEqualTo(1L);
        assertThat(count("MATCH (p:P) WHERE p.d = $d RETURN count(p) AS c", params)).as(params.toString()).isEqualTo(1L);
      }
      assertThat(count("MATCH (p:P) RETURN count(p) AS c")).isEqualTo(2L);
    }

    @Test
    void aCompositeIndexCoercesItsStringKey() {
      database.command("sql", "CREATE VERTEX TYPE X");
      database.command("sql", "CREATE PROPERTY X.k INTEGER");
      database.command("sql", "CREATE PROPERTY X.d STRING");
      database.command("sql", "CREATE INDEX ON X (k, d) UNIQUE");
      database.transaction(() -> database.command("opencypher", "MERGE (p:X {k: 1, d: date('2021-06-15')})").close());
      assertThat(merge("MERGE (p:X {k: 1, d: date('2021-06-15')}) ON MATCH SET p.tag = 'MATCHED' RETURN p.tag AS tag")).isEqualTo("MATCHED");
      assertThat(count("MATCH (p:X {k: 1, d: date('2021-06-15')}) RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (p:X) RETURN count(p) AS c")).isEqualTo(1L);
    }

    @Test
    void aRelationshipPropertyDeclaredStringIsCoercedInEveryClause() {
      database.command("sql", "CREATE VERTEX TYPE A");
      database.command("sql", "CREATE EDGE TYPE R");
      database.command("sql", "CREATE PROPERTY R.d STRING");
      database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1}), (:A {id: 2})").close());
      final String anchors = "MATCH (a:A {id: 1}), (b:A {id: 2}) ";
      database.transaction(() -> database.command("opencypher", anchors + "MERGE (a)-[:R {d: 5}]->(b)").close());
      database.transaction(() -> database.command("opencypher", anchors + "MERGE (a)-[:R {d: 5}]->(b)").close());
      assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).as("MERGE").isEqualTo(1L);

      assertThat(count("MATCH ()-[r:R {d: 5}]->() RETURN count(r) AS c")).as("inline").isEqualTo(1L);
      assertThat(count("MATCH (:A {id: 1})-[r:R {d: 5}]->(:A {id: 2}) RETURN count(r) AS c")).as("anchored inline").isEqualTo(1L);
      assertThat(count("MATCH ()-[r:R]->() WHERE r.d = 5 RETURN count(r) AS c")).as("WHERE").isEqualTo(1L);
      assertThat(count("MATCH (a:A {id: 1}) RETURN size([(a)-[:R {d: 5}]->(b) | b]) AS c")).as("pattern comprehension").isEqualTo(1L);
      assertThat(count("MATCH (a:A {id: 1})-[:R*1..2 {d: 5}]->(b) RETURN count(b) AS c")).as("variable length").isEqualTo(1L);

      database.transaction(() -> database.command("opencypher",
          anchors + "CALL merge.relationship(a, 'R', {d: 5}, {}, b) YIELD rel RETURN rel").close());
      assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).as("merge.relationship").isEqualTo(1L);
    }

    @Test
    void aNodePatternInsideARelationshipPatternIsCoerced() {
      declare("N", null);
      database.command("sql", "CREATE EDGE TYPE L");
      database.transaction(() -> database.command("opencypher", "CREATE (:N {d: 3})-[:L]->(:N {d: true})").close());
      assertThat(count("MATCH (:N {d: 3})-[:L]->(b:N {d: true}) RETURN count(b) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (a:N) WHERE (a)-[:L]->(:N {d: true}) RETURN count(a) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (a:N) WHERE EXISTS { (a)-[:L]->(:N {d: true}) } RETURN count(a) AS c")).isEqualTo(1L);
    }

    /** Membership is equality against each element, so IN agrees with =, on a list and on a lazy range alike. */
    @Test
    void inAgreesWithEquals() {
      declare("I", null);
      database.command("sql", "CREATE VERTEX TYPE IU");
      database.transaction(() -> database.command("opencypher", "CREATE (:I {d: 5}), (:I {d: date('2021-06-15')}), (:I {d: 'x'}), (:IU {d: '5'})").close());
      assertThat(count("MATCH (p:I) WHERE p.d IN [5, 6] RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (p:I) WHERE p.d IN [date('2021-06-15')] RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (p:I) WHERE p.d IN range(0, 9) RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (p:I) WHERE p.d IN range(6, 9) RETURN count(p) AS c")).isEqualTo(0L);
      assertThat(count("MATCH (p:I) WHERE p.d IN ['x', 7] RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (p:I) WHERE NOT p.d IN [5] RETURN count(p) AS c")).isEqualTo(2L);
      // Only the canonical spelling of a long is a member of a range: '-0' and '05' were never written by an integer
      database.transaction(() -> database.command("opencypher", "CREATE (:I {d: '-0'}), (:I {d: '05'}), (:I {d: 0})").close());
      assertThat(count("MATCH (p:I) WHERE p.d IN range(-1, 1) RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (p:I) WHERE p.d IN range(0, 9) RETURN count(p) AS c")).isEqualTo(2L);
      // schemaless: '5' IN [5] stays false
      assertThat(count("MATCH (p:IU) WHERE p.d IN [5] RETURN count(p) AS c")).isEqualTo(0L);
      assertThat(count("MATCH (p:IU) WHERE p.d IN range(0, 9) RETURN count(p) AS c")).isEqualTo(0L);
    }

    /**
     * A correlated inline match ({@code UNWIND ... MATCH (n {d: x})}) over an unindexed type is answered from a hash of the
     * scanned values: an integer has to find the digits it was stored as there too.
     */
    @Test
    void aCorrelatedInlineMatchOverAnUnindexedTypeFindsTheStoredDigits() {
      declare("H", null);
      database.command("sql", "CREATE VERTEX TYPE HU");
      database.transaction(() -> {
        for (int i = 0; i < 20; i++) {
          database.command("opencypher", "CREATE (:H {d: " + i + "})").close();
          database.command("opencypher", "CREATE (:HU {d: '" + i + "'})").close();
        }
      });
      assertThat(count("UNWIND range(0, 19) AS x MATCH (n:H {d: x}) RETURN count(n) AS c")).isEqualTo(20L);
      assertThat(count("UNWIND range(0, 19) AS x MATCH (n:HU {d: x}) RETURN count(n) AS c")).as("schemaless").isEqualTo(0L);
      assertThat(count("UNWIND range(0, 19) AS x MATCH (n:HU {d: toString(x)}) RETURN count(n) AS c")).isEqualTo(20L);
    }

    /**
     * A join of two unconnected parts on {@code c.code = p.num} keeps the pairs the same WHERE accepts over their product:
     * by a hash join when nothing is indexed, and by an index seek per row when {@code code} is.
     */
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void aJoinOnADeclaredStringPropertyKeepsThePairsTheComparisonAccepts(final boolean indexed) {
      database.command("sql", "CREATE VERTEX TYPE JC");
      database.command("sql", "CREATE PROPERTY JC.code STRING");
      if (indexed)
        database.command("sql", "CREATE INDEX ON JC (code) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE JP");
      database.transaction(() -> {
        for (int i = 0; i < 30; i++)
          database.command("opencypher", "CREATE (:JC {id: " + i + ", code: " + i + "})").close();
        database.command("opencypher", "CREATE (:JC {id: 100, code: true}), (:JC {id: 101, code: '5.0'}), (:JC {id: 102, code: 'x'})").close();
        // Past 2^53 two longs share one double key: only the comparison above the join tells them apart
        database.command("opencypher", "CREATE (:JC {id: 103, code: 9007199254740993}), (:JC {id: 104, code: '9007199254740992'})").close();
        database.command("opencypher", "CREATE (:JC {id: 105, code: '02134'}), (:JC {id: 106, code: '1-2'}), (:JC {id: 107, code: -0.0})").close();
        // Texts made of a number's characters in no number's shape: their own keys, never a parse
        database.command("opencypher", "CREATE (:JC {id: 108, code: 'E'}), (:JC {id: 109, code: '2024-01'}), (:JC {id: 110, code: '1.'}), (:JC {id: 111, code: '1E'})").close();
        database.command("opencypher", "CREATE (:JP {id: 1, num: 5}), (:JP {id: 2, num: true}), (:JP {id: 3, num: 5.5}), (:JP {id: 4, num: '7'})")
            .close();
        database.command("opencypher", "CREATE (:JP {id: 5, num: 9007199254740993}), (:JP {id: 6, num: 2134}), (:JP {id: 7, num: 0.0})").close();
        // Either side of the 18-digit long fast path, and the longest long text there is
        database.command("opencypher", "CREATE (:JC {id: 112, code: 123456789012345678}), (:JC {id: 113, code: 1234567890123456789}), "
            + "(:JC {id: 114, code: -9223372036854775808})").close();
        database.command("opencypher", "CREATE (:JP {id: 8, num: 123456789012345678}), (:JP {id: 9, num: 1234567890123456789}), "
            + "(:JP {id: 10, num: -9223372036854775808})").close();
      });

      final String query = "MATCH (p:JP), (c:JC) WHERE c.code = p.num RETURN p.id AS a, c.id AS b ORDER BY a, b";
      final String product = query.replace("c.code = p.num", "(c.code = p.num OR rand() < 0)");
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
        assertThat(plan).contains(indexed ? "IndexNestedLoopJoin" : "ValueHashJoin");
      }
      final List<String> joined = pairs(query);
      assertThat(joined).containsExactly("1/5", "2/100", "4/7", "5/103", "8/112", "9/113", "10/114");
      assertThat(joined).isEqualTo(pairs(product));
    }

    private List<String> pairs(final String query) {
      final List<String> pairs = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          pairs.add(row.<Object>getProperty("a") + "/" + row.<Object>getProperty("b"));
        }
      }
      return pairs;
    }

    /** Over schemaless properties a join keeps Cypher typing: '5' and 5 share a hash key, and the comparison still rejects them. */
    @Test
    void aJoinOnASchemalessPropertyKeepsCypherTyping() {
      database.command("sql", "CREATE VERTEX TYPE SC");
      database.command("sql", "CREATE VERTEX TYPE SP2");
      database.transaction(() -> {
        for (int i = 0; i < 30; i++)
          database.command("opencypher", "CREATE (:SC {id: " + i + ", code: '" + i + "'})").close();
        database.command("opencypher", "CREATE (:SP2 {id: 1, num: 5}), (:SP2 {id: 2, num: '5'})").close();
      });
      assertThat(pairs("MATCH (p:SP2), (c:SC) WHERE c.code = p.num RETURN p.id AS a, c.id AS b ORDER BY a, b")).containsExactly("2/5");
      assertThat(count("MATCH (c:SC) WHERE c.code IN [5] RETURN count(c) AS c")).isEqualTo(0L);
      assertThat(count("MATCH (c:SC) WHERE c.code <> 5 RETURN count(c) AS c")).isEqualTo(30L);
    }

    /** NaN equals nothing, not even the 'NaN' it was stored as; a signed zero is found by the text it was stored as. */
    @Test
    void nanAndSignedZero() {
      declare("Z", null);
      database.transaction(() -> database.command("opencypher", "CREATE (:Z {id: 1, d: 0.0 / 0.0}), (:Z {id: 2, d: -0.0}), (:Z {id: 3, d: 0.0})").close());
      assertThat(count("MATCH (p:Z {d: 0.0 / 0.0}) RETURN count(p) AS c")).isEqualTo(0L);
      assertThat(count("MATCH (p:Z) WHERE p.d = 0.0 / 0.0 RETURN count(p) AS c")).isEqualTo(0L);
      assertThat(count("MATCH (p:Z {d: -0.0}) RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (p:Z {d: 0.0}) RETURN count(p) AS c")).isEqualTo(1L);
      database.transaction(() -> database.command("opencypher", "MERGE (p:Z {d: -0.0})").close());
      assertThat(count("MATCH (p:Z) RETURN count(p) AS c")).isEqualTo(3L);
    }

    /** A node reached through a persisted link is bound as its RID until a property is read: it is coerced all the same. */
    @Test
    void aNodeBoundThroughALinkIsCoercedToo() {
      declare("LT", null);
      database.command("sql", "CREATE VERTEX TYPE LH");
      database.transaction(() -> {
        final RID target = database.newVertex("LT").set("d", 3).save().getIdentity();
        database.newVertex("LH").set("link", target).save();
      });
      assertThat(count("MATCH (h:LH) WITH h.link AS t WHERE t.d = 3 RETURN count(*) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (h:LH) WITH h.link AS t WHERE t.d IN [3] RETURN count(*) AS c")).isEqualTo(1L);
    }

    @Test
    void mergeNodeProcedureIsIdempotent() {
      declare("PN", null);
      database.transaction(() -> database.command("opencypher", "CALL merge.node(['PN'], {d: 9}) YIELD node RETURN node").close());
      database.transaction(() -> database.command("opencypher", "CALL merge.node(['PN'], {d: 9}) YIELD node RETURN node").close());
      assertThat(count("MATCH (p:PN) RETURN count(p) AS c")).isEqualTo(1L);
    }
  }

  /**
   * Regression tests for issue #9701: the declared-STRING coercion of #9695 ({@code n.v = 1} finds the {@code '1'} a write of
   * {@code 1} stored in a property declared STRING) applied only to the spelling {@code n.v}. Every other spelling of the same
   * read - {@code n['v']}, {@code properties(n).v}, {@code properties(n)['v']}, a chained {@code a.link.v} - and the simple
   * {@code CASE n.v WHEN 1} kept strict typing, so the same test answered differently depending on how it was written.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9701DeclaredStringSpellings extends TestHelper {

    @BeforeEach
    void declare() {
      database.command("sql", "CREATE VERTEX TYPE S");
      database.command("sql", "CREATE PROPERTY S.v STRING");
      database.command("sql", "CREATE PROPERTY S.link LINK");
      database.command("sql", "CREATE VERTEX TYPE U");
      command("CREATE (a:S {name: 'a', v: 1})");
      command("MATCH (a:S {name: 'a'}) CREATE (b:S {name: 'b', v: 2}) SET b.link = a");
      // schemaless: the Cypher typing of = stands
      command("CREATE (:U {v: '1'})");
    }

    @ParameterizedTest
    @ValueSource(strings = { "n.v = 1", "n['v'] = 1", "properties(n).v = 1", "properties(n)['v'] = 1", "1 = n['v']",
        "n.v IN [1, 5]", "n['v'] IN [1, 5]", "properties(n).v IN [1]", "n['v'] <> 2", "NOT (n['v'] <> 1)" })
    void everySpellingOfTheReadFindsTheStoredText(final String predicate) {
      assertThat(names("MATCH (n:S) WHERE " + predicate + " RETURN n.name AS name ORDER BY name")).containsExactly("a");
    }

    @Test
    void aChainedReadThroughALinkIsCoerced() {
      assertThat(names("MATCH (b:S) WHERE b.link.v = 1 RETURN b.name AS name")).containsExactly("b");
      assertThat(names("MATCH (b:S) WHERE b.link['v'] = 1 RETURN b.name AS name")).containsExactly("b");
    }

    @Test
    void theSimpleCaseFormIsCoerced() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n:S {name: 'a'}) RETURN CASE n.v WHEN 1 THEN 'one' ELSE 'other' END AS c, "
              + "CASE n['v'] WHEN 1 THEN 'one' ELSE 'other' END AS d, CASE WHEN n.v = 1 THEN 'one' ELSE 'other' END AS e")) {
        final Result r = rs.next();
        assertThat(r.<String>getProperty("c")).isEqualTo("one");
        assertThat(r.<String>getProperty("d")).isEqualTo("one");
        assertThat(r.<String>getProperty("e")).isEqualTo("one");
      }
    }

    @ParameterizedTest
    @ValueSource(strings = { "n.v = 1", "n['v'] = 1", "properties(n).v = 1", "properties(n)['v'] = 1", "n['v'] IN [1]" })
    void anUndeclaredPropertyKeepsStrictTyping(final String predicate) {
      assertThat(names("MATCH (n:U) WHERE " + predicate + " RETURN n.v AS name")).isEmpty();
    }

    @Test
    void aProjectedValueKeepsStrictTyping() {
      // the projection carries no record, so nothing says what the text stands for
      assertThat(names("MATCH (n:S {name: 'a'}) WITH n.name AS name, n.v AS v WHERE v = 1 RETURN name")).isEmpty();
    }

    @Test
    void aMapLiteralIsNotARecord() {
      assertThat(names("WITH {v: '1'} AS m WHERE m['v'] = 1 OR m.v = 1 RETURN 'x' AS name")).isEmpty();
    }

    private void command(final String cypher) {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
      }
    }

    private List<String> names(final String cypher) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext())
          names.add(String.valueOf(rs.next().<Object>getProperty("name")));
      }
      return names;
    }
  }

  /**
   * Regression tests for issue #9706: a constraint created without a name could not be dropped (it had no name and there was
   * no {@code SHOW CONSTRAINTS} to find one), and dropping an {@code IS TYPED} constraint left the type restriction in place.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9706ConstraintNamesAndTypedDrop extends TestHelper {

    @Test
    void anUnnamedNotNullConstraintIsListedAndDroppable() {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS NOT NULL");

      final Map<String, Object> row = onlyRow("SHOW CONSTRAINTS");
      assertThat(row.get("name")).isEqualTo("constraint_L_p_existence");
      assertThat(row.get("type")).isEqualTo("NODE_PROPERTY_EXISTENCE");
      assertThat(row.get("entityType")).isEqualTo("NODE");
      assertThat(row.get("labelsOrTypes")).isEqualTo(List.of("L"));
      assertThat(row.get("properties")).isEqualTo(List.of("p"));

      assertThat(dropped("DROP CONSTRAINT constraint_L_p_existence")).isEqualTo(1);
      // the flag is cleared, and the STRING declaration the constraint made for itself goes with it
      assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
      assertThat(rows("SHOW CONSTRAINTS")).isEmpty();
      database.transaction(() -> database.newVertex("L").save());
    }

    @Test
    void anUnnamedTypedConstraintIsListedAndDroppingItLiftsTheType() {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: INTEGER");

      final Map<String, Object> row = onlyRow("SHOW CONSTRAINTS");
      assertThat(row.get("name")).isEqualTo("constraint_L_p_type");
      assertThat(row.get("type")).isEqualTo("NODE_PROPERTY_TYPE");
      assertThat(row.get("propertyType")).isEqualTo("INTEGER");

      command("DROP CONSTRAINT constraint_L_p_type");

      assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
      // a value of another type is accepted as written, not refused or converted
      command("CREATE (:L {p: 'text'})");
      assertThat(single("MATCH (n:L) RETURN n.p AS p").<String>getProperty("p")).isEqualTo("text");
    }

    @Test
    void aTypedConstraintThatReplacedADeclarationPutsItBack() {
      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.p STRING");

      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.p IS :: INTEGER");
      assertThat(property("L", "p").getType()).isEqualTo(Type.LONG);

      command("DROP CONSTRAINT t");
      assertThat(property("L", "p").getType()).isEqualTo(Type.STRING);
    }

    @Test
    void aTypedConstraintThatCannotPutItsDeclarationBackStaysInPlace() {
      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.p INTEGER");

      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.p IS :: STRING");
      command("CREATE (:L {p: 'not a number'})");

      assertThatThrownBy(() -> command("DROP CONSTRAINT t")).cause().isInstanceOf(SchemaException.class)
          .hasMessageContaining("cannot be restored").hasMessageContaining("INTEGER");
      // nothing was undone, and the constraint can still be found and dropped once the record is fixed
      assertThat(property("L", "p").getType()).isEqualTo(Type.STRING);
      assertThat(onlyRow("SHOW CONSTRAINTS").get("name")).isEqualTo("t");

      command("MATCH (n:L) SET n.p = '12'");
      command("DROP CONSTRAINT t");
      assertThat(property("L", "p").getType()).isEqualTo(Type.INTEGER);
    }

    @Test
    void theDeclarationStaysWhileAnotherConstraintStillNeedsIt() {
      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.p IS :: INTEGER");
      command("CREATE CONSTRAINT nn FOR (n:L) REQUIRE n.p IS NOT NULL");

      command("DROP CONSTRAINT t");
      // the NOT NULL needs a declared property for its MANDATORY flag: it owns the declaration now
      assertThat(property("L", "p").isMandatory()).isTrue();
      assertThat(names("SHOW CONSTRAINTS")).containsExactly("nn");

      command("DROP CONSTRAINT nn");
      assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
    }

    @Test
    void theDeclarationAUniqueConstraintIsBuiltOverStaysUntilItGoes() {
      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.p IS :: INTEGER");
      command("CREATE CONSTRAINT u FOR (n:L) REQUIRE n.p IS UNIQUE");

      command("DROP CONSTRAINT t");
      assertThat(property("L", "p").getType()).isEqualTo(Type.LONG);

      command("DROP CONSTRAINT u");
      assertThat(database.getSchema().getType("L").getAllIndexes(false)).isEmpty();
      assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
    }

    @Test
    void aNotNullDroppedBeforeTheTypedConstraintOnTheSamePropertyHandsItsDeclarationOver() {
      command("CREATE CONSTRAINT nn FOR (n:L) REQUIRE n.p IS NOT NULL");
      // the NOT NULL declared the property STRING; the type constraint replaces that declaration
      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.p IS :: INTEGER");

      command("DROP CONSTRAINT nn");
      assertThat(property("L", "p").getType()).isEqualTo(Type.LONG);
      assertThat(property("L", "p").isMandatory()).isFalse();

      // the declaration the type constraint replaced left with the NOT NULL, so there is nothing to put back
      command("DROP CONSTRAINT t");
      assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
    }

    @Test
    void aConflictingTypeConstraintIsRefused() {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: INTEGER");

      assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: STRING")).rootCause()
          .isInstanceOf(SchemaException.class).hasMessageContaining("constraint_L_p_type");
      assertThat(property("L", "p").getType()).isEqualTo(Type.LONG);
    }

    @Test
    void repeatingAnUnnamedConstraintListsItOnce() {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS UNIQUE");
      command("CREATE CONSTRAINT IF NOT EXISTS FOR (n:L) REQUIRE n.p IS UNIQUE");
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.q IS :: STRING");
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.q IS :: STRING");

      assertThat(names("SHOW CONSTRAINTS")).containsExactly("constraint_L_p_unique", "constraint_L_q_type");
    }

    @Test
    void anUnnamedNodeKeyDroppedByItsIndexNameIsUndoneWhole() {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS NODE KEY");
      final Map<String, Object> row = onlyRow("SHOW CONSTRAINTS");
      assertThat(row.get("name")).isEqualTo("constraint_L_p_key");
      assertThat(row.get("type")).isEqualTo("NODE_KEY");
      final String index = (String) row.get("ownedIndex");
      assertThat(index).isNotNull();

      command("DROP CONSTRAINT `" + index + "`");

      assertThat(database.getSchema().existsIndex(index)).isFalse();
      assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
      assertThat(rows("SHOW CONSTRAINTS")).isEmpty();
    }

    @Test
    void whatSqlDeclaredIsListedAndDroppable() {
      database.command("sql", "CREATE VERTEX TYPE S");
      database.command("sql", "CREATE PROPERTY S.code STRING (mandatory true)");
      database.command("sql", "CREATE PROPERTY S.n LONG");
      database.command("sql", "CREATE PROPERTY S.k LONG");
      database.command("sql", "CREATE INDEX ON S (k) UNIQUE");

      final List<Map<String, Object>> rows = rows("SHOW CONSTRAINTS YIELD name, type, propertyType ORDER BY name");
      assertThat(rows).extracting(r -> r.get("name"))
          .containsExactly("S[k]", "constraint_S_code_existence", "constraint_S_code_type", "constraint_S_k_type", "constraint_S_n_type");
      assertThat(rows.get(0).get("type")).isEqualTo("UNIQUENESS");
      assertThat(rows.get(3).get("propertyType")).isEqualTo("INTEGER");

      command("DROP CONSTRAINT constraint_S_code_existence");
      assertThat(property("S", "code").isMandatory()).isFalse();

      command("DROP CONSTRAINT constraint_S_n_type");
      assertThat(database.getSchema().getType("S").getPropertyIfExists("n")).isNull();

      // the declaration an index is built over cannot go while the index is there
      assertThatThrownBy(() -> command("DROP CONSTRAINT constraint_S_k_type")).rootCause().isInstanceOf(SchemaException.class)
          .hasMessageContaining("index");

      command("DROP CONSTRAINT `S[k]`");
      assertThat(database.getSchema().existsIndex("S[k]")).isFalse();
    }

    @Test
    void showConstraintsFilters() {
      command("CREATE CONSTRAINT u FOR (n:L) REQUIRE n.a IS UNIQUE");
      command("CREATE CONSTRAINT e FOR (n:L) REQUIRE n.b IS NOT NULL");
      command("CREATE CONSTRAINT k FOR (n:L) REQUIRE n.c IS NODE KEY");
      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.d IS :: LIST<STRING NOT NULL>");
      command("CREATE CONSTRAINT r FOR ()-[r:R]-() REQUIRE r.x IS UNIQUE");

      assertThat(names("SHOW CONSTRAINTS")).containsExactly("e", "k", "r", "t", "u");
      assertThat(names("SHOW ALL CONSTRAINTS")).containsExactly("e", "k", "r", "t", "u");
      assertThat(names("SHOW UNIQUE CONSTRAINTS")).containsExactly("r", "u");
      assertThat(names("SHOW NODE UNIQUENESS CONSTRAINTS")).containsExactly("u");
      assertThat(names("SHOW RELATIONSHIP UNIQUE CONSTRAINTS")).containsExactly("r");
      assertThat(names("SHOW EXISTENCE CONSTRAINTS")).containsExactly("e");
      assertThat(names("SHOW KEY CONSTRAINTS")).containsExactly("k");
      assertThat(names("SHOW PROPERTY TYPE CONSTRAINTS")).containsExactly("t");
      assertThat(names("SHOW CONSTRAINTS WHERE name STARTS WITH 'u'")).containsExactly("u");

      final Map<String, Object> typed = onlyRow("SHOW CONSTRAINTS YIELD name, propertyType, createStatement WHERE name = 't'");
      assertThat(typed.get("propertyType")).isEqualTo("LIST<STRING NOT NULL>");
      assertThat(typed.get("createStatement")).isEqualTo("CREATE CONSTRAINT `t` FOR (n:`L`) REQUIRE n.`d` IS :: LIST<STRING NOT NULL>");

      assertThat(onlyRow("SHOW CONSTRAINTS WHERE name = 'r'").get("type")).isEqualTo("RELATIONSHIP_UNIQUENESS");
    }

    @Test
    void showConstraintsIsAReadOnlyQuery() {
      command("CREATE CONSTRAINT u FOR (n:L) REQUIRE n.a IS UNIQUE");
      try (final ResultSet rs = database.query("opencypher", "SHOW CONSTRAINTS")) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("u");
      }
    }

    @Test
    void showIndexesNamesTheConstraintThatOwnsTheIndex() {
      command("CREATE CONSTRAINT u FOR (n:L) REQUIRE n.a IS UNIQUE");
      command("CREATE INDEX FOR (n:L) ON (n.b)");

      final List<Map<String, Object>> indexes = rows("SHOW INDEXES YIELD name, type, properties, owningConstraint, state ORDER BY name");
      assertThat(indexes).hasSize(2);
      assertThat(indexes.get(0).get("properties")).isEqualTo(List.of("a"));
      assertThat(indexes.get(0).get("owningConstraint")).isEqualTo("u");
      assertThat(indexes.get(0).get("state")).isEqualTo("ONLINE");
      assertThat(indexes.get(1).get("properties")).isEqualTo(List.of("b"));
      assertThat(indexes.get(1).get("owningConstraint")).isNull();
      assertThat(names("SHOW RANGE INDEXES")).hasSize(2);
      assertThat(names("SHOW FULLTEXT INDEXES")).isEmpty();
    }

    @Test
    void twoPropertySetsSpellingTheSameGeneratedNameGetTwoNames() {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE (n.a_b, n.c) IS UNIQUE");
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE (n.a, n.b_c) IS UNIQUE");

      assertThat(names("SHOW CONSTRAINTS")).containsExactly("constraint_L_a_b_c_unique", "constraint_L_a_b_c_unique_2");
      command("DROP CONSTRAINT constraint_L_a_b_c_unique_2");
      assertThat(rows("SHOW CONSTRAINTS YIELD name, properties")).singleElement()
          .satisfies(r -> assertThat(r.get("properties")).isEqualTo(List.of("a_b", "c")));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void concurrentUnnamedConstraintsAreRecordedOnceEachUnderDistinctNames() throws Exception {
      database.getSchema().createVertexType("L");
      final String[] statements = { "CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS NOT NULL",
          "CREATE CONSTRAINT IF NOT EXISTS FOR (n:L) REQUIRE (n.a_b, n.c) IS UNIQUE",
          "CREATE CONSTRAINT IF NOT EXISTS FOR (n:L) REQUIRE (n.a, n.b_c) IS UNIQUE" };
      final List<Thread> threads = new ArrayList<>();
      final List<Throwable> failures = new CopyOnWriteArrayList<>();
      for (int t = 0; t < 12; t++) {
        final String statement = statements[t % statements.length];
        final Thread thread = new Thread(() -> {
          try {
            command(statement);
          } catch (final Throwable e) {
            failures.add(e);
          }
        });
        threads.add(thread);
        thread.start();
      }
      for (final Thread thread : threads)
        thread.join();

      assertThat(failures).extracting(e -> e.getCause() != null ? e.getCause().toString() : e.toString()).isEmpty();
      assertThat(names("SHOW CONSTRAINTS")).containsExactly("constraint_L_a_b_c_unique", "constraint_L_a_b_c_unique_2",
          "constraint_L_p_existence");
    }

    @Test
    void twoLabelsSpellingTheSameGeneratedNameGetTwoNames() {
      command("CREATE CONSTRAINT FOR (n:A) REQUIRE n.b_c IS NOT NULL");
      command("CREATE CONSTRAINT FOR (n:A_b) REQUIRE n.c IS NOT NULL");
      // repeating either one changes nothing and records nothing new
      command("CREATE CONSTRAINT IF NOT EXISTS FOR (n:A_b) REQUIRE n.c IS NOT NULL");

      assertThat(names("SHOW CONSTRAINTS")).containsExactly("constraint_A_b_c_existence", "constraint_A_b_c_existence_2");

      command("DROP CONSTRAINT constraint_A_b_c_existence_2");
      assertThat(database.getSchema().getType("A_b").getPropertyIfExists("c")).isNull();
      assertThat(property("A", "b_c").isMandatory()).isTrue();
    }

    @Test
    void generatedNamesOfUnusualIdentifiersRoundTripThroughShowAndDrop() {
      command("CREATE CONSTRAINT FOR (n:`My Label`) REQUIRE n.`my prop` IS NOT NULL");
      command("CREATE CONSTRAINT FOR (n:`My Label`) REQUIRE n.`odd``name` IS :: STRING");

      final List<Object> names = names("SHOW CONSTRAINTS");
      assertThat(names).containsExactly("constraint_My Label_my prop_existence", "constraint_My Label_odd`name_type");

      for (final Object name : names)
        command("DROP CONSTRAINT `" + ((String) name).replace("`", "``") + "`");
      assertThat(rows("SHOW CONSTRAINTS")).isEmpty();
      assertThat(database.getSchema().getType("My Label").getPropertyIfExists("my prop")).isNull();
    }

    @Test
    void generatedNamesSurviveReopen() {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: STRING");

      reopenDatabase();

      assertThat(names("SHOW CONSTRAINTS")).containsExactly("constraint_L_p_type");
      command("DROP CONSTRAINT constraint_L_p_type");
      assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
    }

    private void command(final String cypher) {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
      }
    }

    private long dropped(final String cypher) {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        return rs.getStatistics().orElseThrow().getConstraintsRemoved();
      }
    }

    private List<Map<String, Object>> rows(final String cypher) {
      final List<Map<String, Object>> rows = new ArrayList<>();
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rows.add(rs.next().toMap());
      }
      return rows;
    }

    private List<Object> names(final String cypher) {
      return rows(cypher).stream().map(r -> r.get("name")).toList();
    }

    private Map<String, Object> onlyRow(final String cypher) {
      final List<Map<String, Object>> rows = rows(cypher);
      assertThat(rows).hasSize(1);
      return rows.getFirst();
    }

    private Result single(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        return rs.next();
      }
    }

    private Property property(final String typeName, final String propertyName) {
      final Property p = database.getSchema().getType(typeName).getPropertyIfExists(propertyName);
      assertThat(p).isNotNull();
      return p;
    }
  }

  /**
   * Regression tests for issue #9709: a property type constraint could not declare a list ({@code IS :: LIST<T>} was refused),
   * and the correlated defects found with it - every temporal type constraint but DATE declared a type its own values did not
   * survive (DURATION refused every write, ZONED DATETIME lost its zone), and the {@code IS ::} predicate and {@code valueType()}
   * did not recognise a single temporal value or point.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9709TypedConstraintListAndTemporal extends TestHelper {

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

  /**
   * Issue #9794: a declaration defines the text form of its values, so the same pattern has to find what it stored through
   * that form, or MERGE is not idempotent. The coercion that does it (#9695) covered one direction only, a non-text written
   * into a property declared STRING: a text written into a property declared INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN, DATE or
   * DATETIME was converted and stored, and then no spelling of the same pattern found it, so a second MERGE created a
   * duplicate, or failed on a UNIQUE index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9794DeclaredTypePattern extends TestHelper {
    private static final String[] DECLARED = { "STRING", "BYTE", "SHORT", "INTEGER", "LONG", "FLOAT", "DOUBLE", "DECIMAL", "BOOLEAN",
        "DATE", "DATETIME", "DATETIME_MICROS" };

    // A text, and a literal of every other Cypher type: a cell whose write the declaration refuses is skipped
    private static final String[] LITERALS = { "'5'", "'1.5'", "'true'", "'2024-01-02'", "'2024-01-02T10:00:00'", "5", "1.5", "true",
        "date('2024-01-02')", "localdatetime('2024-01-02T10:00:00')" };

    private static final String[] SPELLINGS = { //
        "MATCH (n:%s {p: %s}) RETURN count(n) AS c", //
        "MATCH (n:%s) WHERE n.p = %s RETURN count(n) AS c", //
        "MATCH (n:%s) WHERE %2$s = n.p RETURN count(n) AS c", //
        "MATCH (n:%s) WHERE n['p'] = %s RETURN count(n) AS c", //
        "MATCH (n:%s) WHERE properties(n).p = %s RETURN count(n) AS c", //
        "MATCH (n:%s) WHERE n.p IN [%s] RETURN count(n) AS c", //
        "MATCH (n:%s) WHERE CASE n.p WHEN %s THEN true ELSE false END RETURN count(n) AS c", //
        "MATCH (n:%s) WHERE NOT (n.p <> %s) RETURN count(n) AS c", //
        "UNWIND [%2$s] AS v MATCH (n:%1$s {p: v}) RETURN count(n) AS c", //
        "UNWIND [%2$s] AS v MATCH (n:%1$s) WHERE n.p = v RETURN count(n) AS c" };

    @Test
    void aRepeatedMergeFindsWhatTheFirstStoredThroughTheTextForm() {
      assertEveryCellIsFoundAgain(false);
    }

    @Test
    void anIndexedPropertyFindsItToo() {
      assertEveryCellIsFoundAgain(true);
    }

    @Test
    void aUniqueIndexNoLongerTurnsTheRepeatedMergeIntoAFailure() {
      database.command("sql", "CREATE VERTEX TYPE U");
      database.command("sql", "CREATE PROPERTY U.p INTEGER");
      database.command("sql", "CREATE INDEX ON U (p) UNIQUE");

      database.transaction(() -> database.command("opencypher", "MERGE (:U {p: '5'})").close());
      database.transaction(() -> database.command("opencypher", "MERGE (:U {p: '5'})").close());

      assertThat(count("MATCH (n:U) RETURN count(n) AS c")).isEqualTo(1);
      assertThat(count("MATCH (n:U {p: '5'}) RETURN count(n) AS c")).isEqualTo(1);
      assertThat(count("MATCH (n:U) WHERE n.p = '5' RETURN count(n) AS c")).isEqualTo(1);
    }

    @Test
    void aJoinOnATextFindsTheTypedValue() {
      final Map<String, List<String>> texts = new LinkedHashMap<>();
      texts.put("INTEGER", List.of("1", "2", "3"));
      texts.put("DOUBLE", List.of("1.5", "2.5", "3.5"));
      texts.put("BOOLEAN", List.of("true", "false"));
      texts.put("DATE", List.of("2024-01-02", "2024-01-03", "2024-01-04"));

      int type = 0;
      for (final Map.Entry<String, List<String>> entry : texts.entrySet()) {
        for (final boolean indexed : new boolean[] { false, true }) {
          final String target = "J" + type++;
          final String source = target + "S";
          final List<String> values = entry.getValue();
          database.command("sql", "CREATE VERTEX TYPE " + target);
          database.command("sql", "CREATE PROPERTY " + target + ".p " + entry.getKey());
          if (indexed)
            database.command("sql", "CREATE INDEX ON " + target + " (p) NOTUNIQUE");
          database.command("sql", "CREATE VERTEX TYPE " + source);
          database.transaction(() -> {
            // many targets and few sources, so the planner seeks the index once per source row
            for (int i = 0; i < 300; i++)
              database.command("opencypher", "CREATE (:" + target + " {p: '" + values.get(i % values.size()) + "'})").close();
            for (final String value : values)
              database.command("opencypher", "CREATE (:" + source + " {s: '" + value + "'})").close();
          });

          final String as = entry.getKey() + (indexed ? " indexed" : "");
          final String join = "MATCH (a:" + source + "), (n:" + target + ") WHERE n.p = a.s RETURN count(n) AS c";
          // the join seeks the index with the key a write of the text stores (a temporal key is not sought, it is scanned)
          if (indexed && !entry.getKey().equals("DATE"))
            assertThat(plan(join)).as(as).contains("IndexNestedLoopJoin");
          assertThat(count(join)).as(as).isEqualTo(300);
          assertThat(count("MATCH (a:" + source + ") MATCH (n:" + target + " {p: a.s}) RETURN count(n) AS c")).as(as).isEqualTo(300);
          assertThat(count("MATCH (a:" + source + ") OPTIONAL MATCH (n:" + target + " {p: a.s}) RETURN count(n) AS c")).as(as)
              .isEqualTo(300);
        }
      }
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      }
    }

    @Test
    void aSchemalessPropertyKeepsTheCypherTypingOfEquality() {
      database.command("sql", "CREATE VERTEX TYPE F");
      database.transaction(() -> database.command("opencypher", "CREATE (:F {p: 5}), (:F {p: '5'})").close());

      // with no declaration nothing says the text stands for the number: 5 = '5' is false in Cypher
      assertThat(count("MATCH (n:F {p: '5'}) RETURN count(n) AS c")).isEqualTo(1);
      assertThat(count("MATCH (n:F) WHERE n.p = 5 RETURN count(n) AS c")).isEqualTo(1);
      assertThat(count("MATCH (n:F) WHERE n.p <> '5' RETURN count(n) AS c")).isEqualTo(1);
    }

    @Test
    void twoValuesOfTheSameKindKeepTheCypherTyping() {
      database.command("sql", "CREATE VERTEX TYPE I");
      database.command("sql", "CREATE PROPERTY I.p INTEGER");
      database.command("sql", "CREATE VERTEX TYPE IX");
      database.command("sql", "CREATE PROPERTY IX.p INTEGER");
      database.command("sql", "CREATE INDEX ON IX (p) NOTUNIQUE");
      database.transaction(() -> database.command("opencypher", "CREATE (:I {p: 1}), (:IX {p: 1})").close());

      // only the text form is converted: a fraction equals no integer, with or without an index
      for (final String label : List.of("I", "IX")) {
        assertThat(count("MATCH (n:" + label + " {p: 1.5}) RETURN count(n) AS c")).as(label).isZero();
        assertThat(count("MATCH (n:" + label + ") WHERE n.p = 1.5 RETURN count(n) AS c")).as(label).isZero();
        assertThat(count("MATCH (n:" + label + ") WHERE n.p = 1.0 RETURN count(n) AS c")).as(label).isEqualTo(1);
      }
    }

    @Test
    void aTextTheDeclarationCannotReadMatchesNothing() {
      database.command("sql", "CREATE VERTEX TYPE N");
      database.command("sql", "CREATE PROPERTY N.p INTEGER");
      database.transaction(() -> database.command("opencypher", "CREATE (:N {p: 5})").close());

      assertThat(count("MATCH (n:N {p: 'five'}) RETURN count(n) AS c")).isZero();
      assertThat(count("MATCH (n:N) WHERE n.p = 'five' RETURN count(n) AS c")).isZero();
      assertThat(count("MATCH (n:N) WHERE n.p <> 'five' RETURN count(n) AS c")).isEqualTo(1);
      assertThat(count("MATCH (n:N) WHERE n.p IN ['five', '6'] RETURN count(n) AS c")).isZero();
      assertThat(count("MATCH (n:N) WHERE n.p IN ['five', '5'] RETURN count(n) AS c")).isEqualTo(1);
    }

    /**
     * A text matches exactly what a write of it stores: the declaration's own reading of the text, no more lenient. An integral
     * declaration reads a sign and digits ({@code '05'} and {@code '+5'} are 5, {@code '5.7'}, {@code ' 5'} and {@code '1e2'}
     * are refused by the write and match nothing); a DOUBLE reads every spelling {@code Double.parseDouble} does.
     */
    @Test
    void aNonCanonicalNumericTextMatchesWhatItsWriteStores() {
      database.command("sql", "CREATE VERTEX TYPE I");
      database.command("sql", "CREATE PROPERTY I.p INTEGER");
      database.command("sql", "CREATE VERTEX TYPE D");
      database.command("sql", "CREATE PROPERTY D.p DOUBLE");
      database.transaction(() -> database.command("opencypher", "CREATE (:I {p: 5}), (:D {p: 2.5})").close());

      for (final String text : List.of("'05'", "'+5'")) {
        assertThat(count("MATCH (n:I {p: " + text + "}) RETURN count(n) AS c")).as(text).isEqualTo(1);
        assertThat(count("MATCH (n:I) WHERE n.p = " + text + " RETURN count(n) AS c")).as(text).isEqualTo(1);
        database.transaction(() -> database.command("opencypher", "MERGE (:I {p: " + text + "})").close());
      }
      assertThat(count("MATCH (n:I) RETURN count(n) AS c")).isEqualTo(1);

      assertThat(count("MATCH (n:I {p: '0000000000005'}) RETURN count(n) AS c")).isEqualTo(1);
      for (final String text : List.of("'5.7'", "' 5'", "'1e2'", "''", "'five'", "'99999999999'")) {
        assertThat(count("MATCH (n:I {p: " + text + "}) RETURN count(n) AS c")).as(text).isZero();
        assertThat(count("MATCH (n:I) WHERE n.p = " + text + " RETURN count(n) AS c")).as(text).isZero();
        assertThat(count("MATCH (n:I) WHERE n.p <> " + text + " RETURN count(n) AS c")).as(text).isEqualTo(1);
      }

      for (final String text : List.of("'2.5'", "'2.50'", "' 2.5'", "'25e-1'"))
        assertThat(count("MATCH (n:D {p: " + text + "}) RETURN count(n) AS c")).as(text).isEqualTo(1);
      assertThat(count("MATCH (n:D {p: 'x2.5'}) RETURN count(n) AS c")).isZero();
    }

    /**
     * The shape check in front of the conversion only saves the conversion's exception: for every declared type and text it
     * answers exactly what {@code Type.convert} answers, so it can never refuse a text a write stores (and then miss the record).
     */
    @Test
    void theShapeCheckNeverDisagreesWithTheConversion() {
      final DocumentType type = database.getSchema().createVertexType("S");
      final List<String> texts = List.of("5", "+5", "-5", "005", "0000000000005", "5.7", " 5", "5 ", "1e2", "", "-", "+", "five",
          "127", "128", "-129", "32768", "2147483647", "2147483648", "9223372036854775807", "9223372036854775808",
          "99999999999999999999", "2.5", "2.50", " 2.5", "25e-1", ".5", "-.5", "NaN", "Infinity", "-Infinity", "0x1p3", "1.5d", "x2.5",
          "true", "TRUE", "False", "yes", "1", "0", "2024-01-02", "2024-01-02T10:00:00", "2024-13-45", "foo", "P1D", "10:15:30",
          "1704153600000", "-0", "1e3", "Nope", "Inf", "-NaN", "+Infinity", "+.5", "5.");
      for (final Type declared : List.of(Type.BYTE, Type.SHORT, Type.INTEGER, Type.LONG, Type.FLOAT, Type.DOUBLE, Type.DECIMAL,
          Type.BOOLEAN, Type.DATE, Type.DATETIME, Type.DATETIME_MICROS, Type.LOCAL_TIME, Type.DURATION)) {
        final Property property = type.createProperty("p" + declared.name().toLowerCase(), declared);
        for (final String text : texts) {
          Object written = Type.convertOrNull(database, text, declared.getJavaImplementation(database), property);
          if (written instanceof Double d && d.isNaN() || written instanceof Float f && f.isNaN())
            written = null; // NaN equals nothing, not even itself
          assertThat(InlineProperties.declaredValue(property, database, text)).as(declared + " <- '" + text + "'").isEqualTo(written);
        }
      }
    }

    @Test
    void aTextNoTemporalDeclarationReadsMatchesNothing() {
      database.command("sql", "CREATE VERTEX TYPE E");
      database.command("sql", "CREATE PROPERTY E.at DATETIME");
      database.transaction(() -> database.command("opencypher", "CREATE (:E {at: '2024-01-02T10:00:00'})").close());

      assertThat(count("MATCH (n:E) WHERE n.at = '2024-01-02T10:00:00' RETURN count(n) AS c")).isEqualTo(1);
      for (final String text : List.of("'foo'", "'2024-13-45T99:00:00'", "''")) {
        assertThat(count("MATCH (n:E {at: " + text + "}) RETURN count(n) AS c")).as(text).isZero();
        assertThat(count("MATCH (n:E) WHERE n.at = " + text + " RETURN count(n) AS c")).as(text).isZero();
      }
    }

    @Test
    void aRelationshipPropertyFollowsItsDeclarationToo() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E");
      database.command("sql", "CREATE PROPERTY E.w DOUBLE");
      database.transaction(() -> database.command("opencypher", "CREATE (:V {k: 1}), (:V {k: 2})").close());

      for (int i = 0; i < 2; i++)
        database.transaction(
            () -> database.command("opencypher", "MATCH (a:V {k: 1}), (b:V {k: 2}) MERGE (a)-[:E {w: '2.5'}]->(b)").close());

      assertThat(count("MATCH ()-[r:E]->() RETURN count(r) AS c")).isEqualTo(1);
      assertThat(count("MATCH ()-[r:E {w: '2.5'}]->() RETURN count(r) AS c")).isEqualTo(1);
      assertThat(count("MATCH ()-[r:E]->() WHERE r.w = '2.5' RETURN count(r) AS c")).isEqualTo(1);
    }

    /**
     * For every declared type and literal whose write converts through the text form - a text into any declaration, anything
     * into a STRING - or stores the literal as it is, MERGE run twice leaves one node and every spelling of the pattern finds it.
     */
    private void assertEveryCellIsFoundAgain(final boolean indexed) {
      final List<String> failures = new ArrayList<>();
      int cells = 0;
      int type = 0;
      for (final String declared : DECLARED) {
        for (final String literal : LITERALS) {
          if (!convertsThroughTheTextForm(declared, literal) && !isSameType(declared, literal))
            continue;

          final String label = (indexed ? "X" : "T") + type++;
          database.command("sql", "CREATE VERTEX TYPE " + label);
          database.command("sql", "CREATE PROPERTY " + label + ".p " + declared);
          // UNIQUE: a second MERGE that did not find the first node fails on the index instead of duplicating it silently
          if (indexed)
            database.command("sql", "CREATE INDEX ON " + label + " (p) UNIQUE");

          try {
            database.transaction(() -> database.command("opencypher", "MERGE (:" + label + " {p: " + literal + "})").close());
          } catch (final RuntimeException refused) {
            // the declaration cannot store this literal: no pattern stores anything, so there is nothing to find again
            continue;
          }
          cells++;

          try {
            database.transaction(() -> database.command("opencypher", "MERGE (:" + label + " {p: " + literal + "})").close());
          } catch (final RuntimeException e) {
            failures.add(declared + " <- " + literal + ": the second MERGE failed: " + e.getMessage());
          }
          if (count("MATCH (n:" + label + ") RETURN count(n) AS c") != 1)
            failures.add(declared + " <- " + literal + ": the second MERGE created a duplicate");
          // a text no declaration reads matches nothing, under the same plan
          if (count("MATCH (n:" + label + " {p: 'abc'}) RETURN count(n) AS c") != 0)
            failures.add(declared + " <- " + literal + ": 'abc' matched");

          for (final String spelling : SPELLINGS) {
            final String query = spelling.formatted(label, literal);
            if (count(query) != 1)
              failures.add(declared + " <- " + literal + ": " + query);
          }
        }
      }

      assertThat(cells).isGreaterThan(40);
      assertThat(failures).isEmpty();
    }

    private static boolean convertsThroughTheTextForm(final String declared, final String literal) {
      return declared.equals("STRING") != literal.startsWith("'");
    }

    private static boolean isSameType(final String declared, final String literal) {
      return switch (literal) {
        case "5" -> !declared.equals("STRING") && !declared.equals("BOOLEAN") && !declared.startsWith("DATE");
        case "1.5" -> declared.equals("FLOAT") || declared.equals("DOUBLE") || declared.equals("DECIMAL");
        case "true" -> declared.equals("BOOLEAN");
        case "date('2024-01-02')" -> declared.equals("DATE");
        case "localdatetime('2024-01-02T10:00:00')" -> declared.startsWith("DATETIME");
        default -> declared.equals("STRING");
      };
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        final Object c = rs.next().getProperty("c");
        return c instanceof Number n ? n.longValue() : -1;
      } catch (final RuntimeException e) {
        return -2;
      }
    }
  }

  /**
   * Issue #9991: a type constraint replaces the declared type of a property, and {@code DROP CONSTRAINT} puts the previous
   * declaration back through the same checks the retype went through. Those checks refused a REGEXP on any type but a STRING
   * with a hand-written rule, so a LONG carrying {@code '[0-9]+'}, retyped to STRING by a constraint, could never be put back:
   * the constraint could be created but not dropped, {@code IF EXISTS} included. The REGEXP now goes through the check the
   * declaration itself is held to, so whatever the schema accepted is what the drop restores.
   * <p>
   * The guarantee is the round trip below, over a matrix of declared types and constraint types: a constraint that could be
   * created can be dropped, and the drop gives back the declaration it found.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9991DropConstraintRestoresRegexp extends TestHelper {

    @Test
    void theConstraintOfTheIssueCanBeDropped() {
      database.command("sql", "CREATE VERTEX TYPE R");
      database.command("sql", "CREATE PROPERTY R.code LONG (regexp '[0-9]+')");
      database.command("opencypher", "CREATE CONSTRAINT rc FOR (n:R) REQUIRE n.code IS :: STRING").close();
      assertThat(property("R", "code").getType()).isEqualTo(Type.STRING);

      database.command("opencypher", "DROP CONSTRAINT rc").close();
      assertThat(property("R", "code").getType()).isEqualTo(Type.LONG);
      assertThat(property("R", "code").getRegexp()).isEqualTo("[0-9]+");
      assertThat(constraintNames()).doesNotContain("rc");

      // and so with IF EXISTS
      database.command("opencypher", "CREATE CONSTRAINT rc FOR (n:R) REQUIRE n.code IS :: STRING").close();
      database.command("opencypher", "DROP CONSTRAINT rc IF EXISTS").close();
      assertThat(property("R", "code").getType()).isEqualTo(Type.LONG);
      assertThat(constraintNames()).doesNotContain("rc");
    }

    @Test
    void aRegexpNoValueOfTheNewTypeMatchesIsStillRefusedByTheRetype() {
      database.command("sql", "CREATE VERTEX TYPE B");
      database.command("sql", "CREATE PROPERTY B.code STRING (regexp '[A-Z]+')");
      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:B) REQUIRE n.code IS :: INTEGER"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("REGEXP '[A-Z]+'")
          .hasMessageContaining("matches no text a value of LONG");
      assertThat(property("B", "code").getType()).isEqualTo(Type.STRING);

      // one some value of the new type matches is carried, both ways
      database.command("sql", "CREATE PROPERTY B.zip STRING (regexp '[0-9]{5}')");
      database.command("opencypher", "CREATE CONSTRAINT bz FOR (n:B) REQUIRE n.zip IS :: INTEGER").close();
      assertThat(property("B", "zip").getType()).isEqualTo(Type.LONG);
      assertThat(property("B", "zip").getRegexp()).isEqualTo("[0-9]{5}");
      database.command("opencypher", "DROP CONSTRAINT bz").close();
      assertThat(property("B", "zip").getType()).isEqualTo(Type.STRING);
      assertThat(property("B", "zip").getRegexp()).isEqualTo("[0-9]{5}");
    }

    @Test
    void everyConstraintThatCanBeCreatedCanBeDropped() {
      // declared type, then attributes the type accepts: a pattern some value of it matches, bounds where it has an order
      final List<String> declarations = List.of(//
          "LONG (regexp '[0-9]+')", "LONG (regexp '-?[0-9]{1,5}', min 0, max 99999)", "INTEGER (regexp '.*')",
          "DOUBLE (regexp '[0-9]+\\\\.[0-9]+', min 0)", "DECIMAL (regexp '[0-9]+')", "STRING (regexp '[0-9]+', max 10)",
          "STRING (regexp '[A-Za-z]+')", "STRING (regexp 'true|false')", "BOOLEAN (regexp 'true')",
          "DATE (regexp '[0-9]{4}-.*')", "DATETIME (regexp '.*T.*')", "STRING (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}')");
      final List<String> cypherTypes = List.of("STRING", "INTEGER", "FLOAT", "BOOLEAN", "DATE", "LOCAL DATETIME",
          "ZONED DATETIME", "DURATION");

      int created = 0;
      final List<String> notDroppable = new ArrayList<>();
      for (int d = 0; d < declarations.size(); d++)
        for (int c = 0; c < cypherTypes.size(); c++) {
          final String typeName = "M" + d + "_" + c;
          database.command("sql", "CREATE VERTEX TYPE " + typeName);
          database.command("sql", "CREATE PROPERTY " + typeName + ".p " + declarations.get(d));
          final Property before = property(typeName, "p");
          final Type declaredType = before.getType();
          final String regexp = before.getRegexp();
          final String min = before.getMin();
          final String max = before.getMax();

          final String constraint = "k" + d + "_" + c;
          try {
            database.command("opencypher",
                "CREATE CONSTRAINT " + constraint + " FOR (n:" + typeName + ") REQUIRE n.p IS :: " + cypherTypes.get(c)).close();
          } catch (final RuntimeException refused) {
            // a combination the retype refuses never exists, so there is nothing to drop
            continue;
          }
          ++created;

          try {
            database.command("opencypher", "DROP CONSTRAINT " + constraint).close();
          } catch (final RuntimeException e) {
            notDroppable.add(declarations.get(d) + " IS :: " + cypherTypes.get(c) + ": " + e.getMessage());
            continue;
          }
          final Property after = property(typeName, "p");
          assertThat(after.getType()).as("%s IS :: %s", declarations.get(d), cypherTypes.get(c)).isEqualTo(declaredType);
          assertThat(after.getRegexp()).as("%s IS :: %s", declarations.get(d), cypherTypes.get(c)).isEqualTo(regexp);
          assertThat(after.getMin()).as("%s IS :: %s", declarations.get(d), cypherTypes.get(c)).isEqualTo(min);
          assertThat(after.getMax()).as("%s IS :: %s", declarations.get(d), cypherTypes.get(c)).isEqualTo(max);
          assertThat(constraintNames()).doesNotContain(constraint);
        }

      assertThat(notDroppable).as("constraints that could be created but not dropped").isEmpty();
      // the matrix exercises the round trip, not only refusals
      assertThat(created).isGreaterThan(10);
    }

    private Property property(final String type, final String name) {
      return database.getSchema().getType(type).getProperty(name);
    }

    private List<String> constraintNames() {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "SHOW CONSTRAINTS")) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
      }
      return names;
    }
  }
}
