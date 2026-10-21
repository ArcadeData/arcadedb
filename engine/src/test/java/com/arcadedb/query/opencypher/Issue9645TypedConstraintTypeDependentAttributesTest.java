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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9645: {@code REQUIRE n.x IS :: TYPE} carried the OF, REGEXP, MIN, MAX and DEFAULT of the old
 * declaration onto the new type as they were, leaving a property no value could be written through. They are now carried
 * over only when the new type reads them the same way; otherwise the statement is refused naming them and the declaration is
 * left as it was.
 */
class Issue9645TypedConstraintTypeDependentAttributesTest extends TestHelper {

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
