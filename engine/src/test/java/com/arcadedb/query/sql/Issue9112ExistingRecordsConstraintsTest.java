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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #9112 (merged #9016, #9017): CREATE PROPERTY, ALTER PROPERTY MIN/MAX/REGEXP and the openCypher
 * existence constraints did not look at the records already stored, so a record that violated the new declaration stayed
 * in the database (unable to take any update, or silently missing from an index-ordered read).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9112ExistingRecordsConstraintsTest extends TestHelper {

  private void insert(final String type, final String setClause) {
    database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET " + setClause).close());
  }

  private boolean hasProperty(final String type, final String property) {
    return database.getSchema().getType(type).existsProperty(property);
  }

  @Test
  void createPropertyRefusesStringOverInteger() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    insert("T", "v = 'abc'");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("T.v").hasMessageContaining("abc");
    assertThat(hasProperty("T", "v")).isFalse();
  }

  @Test
  void createPropertyRefusesBooleanOverInteger() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    insert("T", "v = true");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class);
    assertThat(hasProperty("T", "v")).isFalse();
  }

  @Test
  void createPropertyRefusesUnreadableDate() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    insert("T", "v = 'not a date'");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v DATETIME")).isInstanceOf(
        CommandExecutionException.class);
    assertThat(hasProperty("T", "v")).isFalse();
  }

  @Test
  void createPropertyAcceptsConvertibleValues() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    insert("T", "v = '12'");
    insert("T", "v = 7");
    insert("T", "id = 1");
    database.command("sql", "CREATE PROPERTY T.v INTEGER");
    assertThat(hasProperty("T", "v")).isTrue();
    database.command("sql", "CREATE INDEX ON T (v) NOTUNIQUE");
  }

  @Test
  void createPropertyOverSubtypeRecords() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    insert("C", "v = 'abc'");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY P.v INTEGER")).isInstanceOf(
        CommandExecutionException.class);
    assertThat(hasProperty("P", "v")).isFalse();
  }

  @Test
  void createPropertyRefusesMandatoryNotNullOverRecordWithoutValue() {
    database.command("sql", "CREATE DOCUMENT TYPE C");
    insert("C", "id = 1, v = 10");
    insert("C", "id = 2");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY C.v INTEGER (mandatory true, notnull true)")).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("C.v");
    assertThat(hasProperty("C", "v")).isFalse();
  }

  @Test
  void createPropertyRefusesNotNullOverNull() {
    database.command("sql", "CREATE DOCUMENT TYPE N");
    insert("N", "id = 1, v = null");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY N.v INTEGER (notnull true)")).isInstanceOf(
        CommandExecutionException.class);
    assertThat(hasProperty("N", "v")).isFalse();
  }

  @Test
  void createPropertyRefusesMinMaxRegexpViolations() {
    database.command("sql", "CREATE DOCUMENT TYPE M");
    insert("M", "id = 1, a = 5, b = 25, c = 'ABC'");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY M.a INTEGER (min 10)")).isInstanceOf(
        CommandExecutionException.class);
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY M.b INTEGER (max 20)")).isInstanceOf(
        CommandExecutionException.class);
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY M.c STRING (regexp '[a-z]+')")).isInstanceOf(
        CommandExecutionException.class);
    assertThat(hasProperty("M", "a") || hasProperty("M", "b") || hasProperty("M", "c")).isFalse();
  }

  @Test
  void createPropertyOverEmptyTypeAndConformingRecords() {
    database.command("sql", "CREATE DOCUMENT TYPE E");
    database.command("sql", "CREATE PROPERTY E.v INTEGER (mandatory true, notnull true, min 1, max 9)");
    insert("E", "v = 3");
    database.command("sql", "CREATE DOCUMENT TYPE F");
    insert("F", "v = 3, s = 'abc'");
    database.command("sql", "CREATE PROPERTY F.v INTEGER (mandatory true, notnull true, min 1, max 9)");
    database.command("sql", "CREATE PROPERTY F.s STRING (regexp '[a-z]+')");
    assertThat(hasProperty("F", "s")).isTrue();
  }

  @Test
  void alterPropertyMinMaxRegexpAreRefusedOverViolatingRecords() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.a INTEGER");
    database.command("sql", "CREATE PROPERTY T.b INTEGER");
    database.command("sql", "CREATE PROPERTY T.c STRING");
    insert("T", "a = 5, b = 25, c = 'ABC'");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.a MIN 10")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("T.a");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.b MAX 20")).isInstanceOf(CommandExecutionException.class);
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.c REGEXP '[a-z]+'")).isInstanceOf(
        CommandExecutionException.class);
    assertThat(database.getSchema().getType("T").getProperty("a").getMin()).isNull();
    assertThat(database.getSchema().getType("T").getProperty("b").getMax()).isNull();
    assertThat(database.getSchema().getType("T").getProperty("c").getRegexp()).isNull();
    // the same bounds are fine when the records conform
    database.command("sql", "ALTER PROPERTY T.a MIN 1");
    database.command("sql", "ALTER PROPERTY T.b MAX 30");
    database.command("sql", "ALTER PROPERTY T.c REGEXP '[A-Z]+'");
    // and clearing a bound never needs a scan
    database.command("sql", "ALTER PROPERTY T.a MIN null");
  }

  @Test
  void cypherNotNullConstraintIsRefusedOverNodeWithoutProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1, name: 'a'}), (:P {id: 2})"));
    assertThatThrownBy(
        () -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:P) REQUIRE n.name IS NOT NULL")).isInstanceOf(
        Exception.class).hasMessageContaining("name");
    assertThat(database.getSchema().getType("P").existsProperty("name") && database.getSchema().getType("P").getProperty("name")
        .isMandatory()).isFalse();
    // the node can still be updated
    database.transaction(() -> database.command("opencypher", "MATCH (n:P {id: 2}) SET n.note = 'x'"));
  }

  @Test
  void cypherNodeKeyConstraintIsRefusedOverNodeWithoutProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Q {id: 1, name: 'a'}), (:Q {id: 2})"));
    assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:Q) REQUIRE n.name IS NODE KEY")).isInstanceOf(
        Exception.class).hasMessageContaining("name");
    assertThat(database.getSchema().getType("Q").getIndexesByProperties("name")).isNullOrEmpty();
    database.transaction(() -> database.command("opencypher", "MATCH (n:Q {id: 2}) SET n.note = 'x'"));
  }

  @Test
  void cypherConstraintsOverConformingNodesStillWork() {
    database.transaction(() -> database.command("opencypher", "CREATE (:R {id: 1, name: 'a'}), (:R {id: 2, name: 'b'})"));
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:R) REQUIRE n.name IS NOT NULL");
    assertThat(database.getSchema().getType("R").getProperty("name").isMandatory()).isTrue();
  }

  @Test
  void refusedCreatePropertyCanBeRetriedAfterFixingTheData() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    insert("T", "v = 'abc'");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class);
    assertThat(hasProperty("T", "v")).isFalse();
    database.transaction(() -> database.command("sql", "UPDATE T SET v = 5").close());
    database.command("sql", "CREATE PROPERTY T.v INTEGER");
    assertThat(hasProperty("T", "v")).isTrue();
  }

  @Test
  void clearingABoundNeedsNoScan() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.a INTEGER (min 1)");
    insert("T", "a = 5");
    assertThat(database.command("sql", "ALTER PROPERTY T.a MIN null").hasNext()).isTrue();
  }

  @Test
  void invalidRegexpIsRefusedEvenOverAnEmptyType() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.s REGEXP '[a-'")).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("Invalid regular expression");
  }

  @Test
  void createPlainStringPropertyOverExistingRecords() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    insert("T", "s = 5");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    assertThat(hasProperty("T", "s")).isTrue();
  }

  @Test
  void createPropertyIfNotExistsOverExistingDataIsANoOp() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.v INTEGER");
    insert("T", "v = 1");
    database.command("sql", "CREATE PROPERTY T.v IF NOT EXISTS STRING");
    assertThat(database.getSchema().getType("T").getProperty("v").getType().name()).isEqualTo("INTEGER");
  }

  @Test
  void alterPropertyOnAParentTypeSeesViolatingSubtypeRecords() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.v INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    insert("C", "v = 5");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY P.v MIN 10")).isInstanceOf(CommandExecutionException.class);
    assertThat(database.getSchema().getType("P").getProperty("v").getMin()).isNull();
  }

  @Test
  void createPropertyRefusesALossyNarrowing() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    insert("T", "v = 3000000000");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("3000000000");
    assertThat(hasProperty("T", "v")).isFalse();
    database.command("sql", "CREATE PROPERTY T.v LONG");
  }

  @Test
  void malformedBoundIsRefusedOverAnEmptyType() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.a INTEGER");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.a MAX 'abc'")).isInstanceOf(CommandExecutionException.class);
    assertThat(database.getSchema().getType("T").getProperty("a").getMax()).isNull();
  }

  @Test
  void cypherCompositeNodeKeyIsRefusedWhenOnlyTheSecondPropertyIsMissing() {
    database.transaction(() -> database.command("opencypher", "CREATE (:K {a: 1, b: 2}), (:K {a: 3})"));
    assertThatThrownBy(
        () -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:K) REQUIRE (n.a, n.b) IS NODE KEY")).isInstanceOf(
        Exception.class).hasMessageContaining("b");
  }

  @Test
  void aFailingAttributeLeavesNoPropertyBehind() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    insert("T", "v = 5");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER (mandatory true, min 10)")).isInstanceOf(
        CommandExecutionException.class);
    assertThat(hasProperty("T", "v")).isFalse();
  }

  @Test
  void createPropertyRefusesNaNOverAnIntegralType() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> database.newDocument("T").set("v", Double.NaN).save());
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class);
    assertThat(hasProperty("T", "v")).isFalse();
  }
}
