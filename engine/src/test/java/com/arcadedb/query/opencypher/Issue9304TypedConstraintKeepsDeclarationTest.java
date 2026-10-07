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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9304: {@code REQUIRE n.x IS :: TYPE} dropped and recreated an existing property, losing every
 * other constraint of its declaration, and accepted a type the stored values do not satisfy.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9304TypedConstraintKeepsDeclarationTest extends TestHelper {

  @Test
  void typedConstraintKeepsTheOtherAttributesOfTheProperty() {
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE PROPERTY B.code STRING");
    final Property declared = database.getSchema().getType("B").getProperty("code");
    declared.setMandatory(true).setNotNull(true).setReadonly(true).setRegexp("[0-9]+").setMax("4").setDefaultValue("111");

    database.command("opencypher", "CREATE CONSTRAINT FOR (n:B) REQUIRE n.code IS :: INTEGER").close();

    final Property p = database.getSchema().getType("B").getProperty("code");
    assertThat(p.getType()).isEqualTo(Type.LONG);
    assertThat(p.isMandatory()).isTrue();
    assertThat(p.isNotNull()).isTrue();
    assertThat(p.isReadonly()).isTrue();
    assertThat(p.getRegexp()).isEqualTo("[0-9]+");
    assertThat(p.getMax()).isEqualTo("4");
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
