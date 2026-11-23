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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;

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
    final List<String> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 2; i++)
        rids.add(insert(insert));
    });
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p " + propertyType + " (" + constraint + ")");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot make type 'T' a sub type of 'S': record ")
        .hasMessageContaining(" violates the properties it would inherit")
        // the first violating record is named: which of the two the scan meets first is the order of the buckets
        .satisfies(e -> assertThat(e.getMessage()).containsAnyOf(rids.toArray(new String[0])))
        .satisfies(e -> assertThat(((ValidationException) e).getKind()).isEqualTo(kind));

    assertThat(database.getSchema().getType("T").getSuperTypes()).isEmpty();
    // the records can still be written
    assertThatCode(() -> database.transaction(() -> database.command("sql", "UPDATE T SET other = 2"))).doesNotThrowAnyException();
  }

  @Test
  void theRecordsOfTheSubTypesAreHeldToItToo() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE DOCUMENT TYPE U EXTENDS T");
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p INTEGER (MIN 10)");
    final String[] violating = new String[1];
    database.transaction(() -> {
      insert("INSERT INTO T SET p = 20");
      violating[0] = insert("INSERT INTO U SET p = 1");
    });

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot make type 'T' a sub type of 'S': record " + violating[0] + " violates the properties it would inherit");

    database.transaction(() -> database.command("sql", "UPDATE U SET p = 11"));
    assertThatCode(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S")).doesNotThrowAnyException();
    assertThat(database.getSchema().getType("T").getSuperTypes()).extracting(DocumentType::getName).containsExactly("S");
  }

  /** A property the super type inherits from its own super type is inherited by T too, so it is held against T's records as well. */
  @Test
  void thePropertiesOfTheSuperTypesOwnSuperTypesAreHeldToo() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE PROPERTY A.p INTEGER (MIN 10)");
    database.command("sql", "CREATE DOCUMENT TYPE S EXTENDS A");
    database.command("sql", "CREATE DOCUMENT TYPE T");
    final String[] violating = new String[1];
    database.transaction(() -> violating[0] = insert("INSERT INTO T SET p = 5"));

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("record " + violating[0] + " violates the properties it would inherit");
    assertThat(database.getSchema().getType("T").getSuperTypes()).isEmpty();
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
   * What the records already see is not inherited anew: a property the type declares itself shadows the super type's, so its
   * declaration is the one the records meet, before and after the link, and records that conform are linked as before.
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
    // the premise of passing the shadowed S.p over: the record is not held to its MIN afterwards either
    assertThatCode(() -> database.transaction(() -> database.command("sql", "UPDATE T SET other = 2"))).doesNotThrowAnyException();
  }

  /** The same for a sub type that declares the property: its records meet its own declaration, T's are held to the inherited one. */
  @Test
  void aSubTypeThatDeclaresThePropertyShadowsTheInheritedOneForItsRecordsOnly() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE DOCUMENT TYPE U EXTENDS T");
    database.command("sql", "CREATE PROPERTY U.p INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p INTEGER (MIN 10)");
    final String[] violating = new String[1];
    database.transaction(() -> {
      insert("INSERT INTO T SET p = 20");
      insert("INSERT INTO U SET p = 5");
      violating[0] = insert("INSERT INTO T SET p = 6");
    });

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("record " + violating[0] + " violates the properties it would inherit");

    database.transaction(() -> database.command("sql", "UPDATE T SET p = 12 WHERE p = 6"));
    assertThatCode(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S")).doesNotThrowAnyException();
    assertThatCode(() -> database.transaction(() -> database.command("sql", "UPDATE U SET other = 2"))).doesNotThrowAnyException();
  }

  /**
   * A property of the same name and ANOTHER declared type does not shadow the inherited one: the write path holds a record to both,
   * so a record that fails the inherited one has to refuse the link, or every later write to it fails.
   */
  @Test
  void aSameNamePropertyOfAnotherTypeDoesNotShadowTheInheritedOne() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.p STRING");
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p INTEGER (MIN 10)");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET p = '5'"));

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot make type 'T' a sub type of 'S'");
    assertThat(database.getSchema().getType("T").getSuperTypes()).isEmpty();
    assertThatCode(() -> database.transaction(() -> database.command("sql", "UPDATE T SET other = 2"))).doesNotThrowAnyException();
  }

  /** Records that conform are linked, and a super type with nothing a stored record can violate is linked over any records. */
  @Test
  void conformingRecordsAreLinkedAsBefore() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p INTEGER (MIN 10, MAX 100)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET p = 10");
      database.command("sql", "INSERT INTO T SET p = 100");
      database.command("sql", "INSERT INTO T SET p = '50'");
    });

    assertThatCode(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S")).doesNotThrowAnyException();
    assertThat(database.getSchema().getType("T").getSuperTypes()).extracting(DocumentType::getName).containsExactly("S");
  }

  /**
   * A type made with its super type has no stored record: the link is made, and the inherited constraints hold for what is written
   * afterwards, so there is nothing for the scan to refuse and nothing is lost by running it.
   */
  @Test
  void aNewTypeMadeWithItsSuperTypeIsLinked() {
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p INTEGER (MIN 10, MANDATORY true)");

    database.command("sql", "CREATE DOCUMENT TYPE T EXTENDS S");

    assertThat(database.getSchema().getType("T").getSuperTypes()).extracting(DocumentType::getName).containsExactly("S");
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO T SET p = 5")))
        .isInstanceOf(ValidationException.class);
  }

  /** The route through the type builder, for a type that exists: the same refusal, and the type is left as it was. */
  @Test
  void aTypeMadeAgainWithAnotherSuperTypeIsRefusedOverViolatingRecords() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    final String[] violating = new String[1];
    database.transaction(() -> violating[0] = insert("INSERT INTO T SET p = 5"));
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p INTEGER (MIN 10)");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("T").withSuperType("S").withIgnoreIfExists(true).create())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("record " + violating[0] + " violates the properties it would inherit");
    assertThat(database.getSchema().getType("T").getSuperTypes()).isEmpty();

    database.transaction(() -> database.command("sql", "UPDATE T SET p = 11"));
    assertThatCode(() -> database.getSchema().buildDocumentType().withName("T").withSuperType("S").withIgnoreIfExists(true).create())
        .doesNotThrowAnyException();
    assertThat(database.getSchema().getType("T").getSuperTypes()).extracting(DocumentType::getName).containsExactly("S");
  }

  /** A super type with an index is linked over conforming records, and the index it propagates finds them. */
  @Test
  void aSuperTypeWithAnIndexIsLinkedOverConformingRecords() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p INTEGER (MIN 10)");
    database.command("sql", "CREATE INDEX ON S (p) NOTUNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET p = 20"));

    assertThatCode(() -> database.command("sql", "ALTER TYPE T SUPERTYPE +S")).doesNotThrowAnyException();

    try (final ResultSet resultSet = database.query("sql", "SELECT FROM S WHERE p = 20")) {
      assertThat(resultSet.stream().count()).isEqualTo(1L);
    }
  }

  /** @return the RID of the record the INSERT created */
  private String insert(final String sql) {
    try (final ResultSet resultSet = database.command("sql", sql)) {
      return resultSet.next().getIdentity().orElseThrow().toString();
    }
  }
}
