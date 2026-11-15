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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #12030: {@code ALTER TYPE T SUPERTYPE +S} did not hold {@code T}'s stored records against the constraints
 * {@code T} inherits from {@code S}. It answered OK over records that violate a MANDATORY, NOTNULL, MIN, MAX or REGEXP of {@code S}, and
 * every later write to them failed, including updates of other properties. Declaring the same constraint on {@code T} directly is
 * refused over the same records (#8943, #9112), and so is the link now, with nothing changed.
 */
class SuperTypeStoredRecordsTest extends TestHelper {

  @ParameterizedTest(name = "{0}")
  @CsvSource(delimiter = '|', value = {
      "MANDATORY true  | INTEGER | INSERT INTO T SET other = 1 | MANDATORY",
      "NOTNULL true    | INTEGER | INSERT INTO T SET p = null  | NOT_NULL",
      "MIN 10          | INTEGER | INSERT INTO T SET p = 5     | MIN",
      "MAX 10          | INTEGER | INSERT INTO T SET p = 50    | MAX",
      "REGEXP '[a-z]+' | STRING  | INSERT INTO T SET p = 'ABC' | REGEXP" })
  void aSuperTypeWhoseConstraintTheStoredRecordsViolateIsRefused(final String constraint, final String propertyType,
      final String insert, final ValidationException.Kind kind) {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> {
      database.command("sql", insert);
      database.command("sql", insert);
    });
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p " + propertyType + " (" + constraint + ")");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot make type 'T' a sub type of 'S': 2 stored records of 'T' violate the properties it would inherit")
        .satisfies(e -> assertThat(((ValidationException) e).getKind()).isEqualTo(kind));

    assertThat(database.getSchema().getType("T").getSuperTypes()).isEmpty();
    // the records can still be written
    assertThatCode(() -> database.transaction(() -> database.command("sql", "UPDATE T SET other = 2"))).doesNotThrowAnyException();
  }

  @Test
  void theRecordsOfTheSubTypesAreHeldToItTooAndCounted() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE DOCUMENT TYPE U EXTENDS T");
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p INTEGER (MIN 10)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET p = 20");
      database.command("sql", "INSERT INTO U SET p = 1");
    });

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("1 stored record of 'T' violates the properties it would inherit");

    database.transaction(() -> database.command("sql", "UPDATE U SET p = 11"));
    assertThatCode(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S")).doesNotThrowAnyException();
    assertThat(database.getSchema().getType("T").getSuperTypes()).extracting(DocumentType::getName).containsExactly("S");
  }

  /** A value of another type than the inherited declaration is refused here too, as CREATE PROPERTY refuses it, rather than by the index. */
  @Test
  void aStoredValueOfAnotherTypeIsRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p INTEGER");
    database.command("sql", "CREATE INDEX ON S (p) NOTUNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET p = 'abc'"));

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot make type 'T' a sub type of 'S'");
    assertThat(database.getSchema().getType("T").getSuperTypes()).isEmpty();
  }

  /**
   * What the records already see is not inherited anew: a property the type declares itself shadows the super type's, and records
   * that conform are linked as before.
   */
  @Test
  void whatTheTypeAlreadySeesIsNotHeldAgainstIt() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.p INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p INTEGER (MIN 10)");
    database.command("sql", "CREATE PROPERTY S.q STRING (MANDATORY true)");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET p = 5, q = 'x'"));

    assertThatCode(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S")).doesNotThrowAnyException();
    assertThat(database.getSchema().getType("T").getSuperTypes()).extracting(DocumentType::getName).containsExactly("S");
  }
}
