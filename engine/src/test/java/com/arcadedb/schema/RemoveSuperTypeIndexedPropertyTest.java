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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12038: {@code ALTER TYPE C SUPERTYPE -P} removed the declaration of a property an index of C (or of a type
 * below C) stands on, which {@code DROP PROPERTY P.k} refuses, and left the index keyed on a property C no longer declares.
 * The removal is refused, naming the index, and nothing changes; a property C still has (declared by C itself or inherited
 * through another super type) does not count.
 */
class RemoveSuperTypeIndexedPropertyTest extends TestHelper {

  @Test
  void indexOfTheTypeOnAPropertyOnlyTheSuperTypeDeclaresRefusesTheRemoval() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE INDEX ON C (k) UNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO C SET k = 1"));

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE C SUPERTYPE -P"))//
        .isInstanceOf(SchemaException.class)//
        .hasMessageContaining("'k'")//
        .hasMessageContaining("'C[k]'");

    assertUnchanged("C", "P", "C[k]");
    reopenDatabase();
    assertUnchanged("C", "P", "C[k]");

    // the property still converts the value the index keys: the string '1' is the integer 1 already held
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO C SET k = '1'")))//
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void propertyDeclaredAboveTheSuperTypeRefusesTheRemoval() {
    database.command("sql", "CREATE DOCUMENT TYPE G");
    database.command("sql", "CREATE PROPERTY G.k INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE P EXTENDS G");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.command("sql", "CREATE INDEX ON C (k) NOTUNIQUE");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE C SUPERTYPE -P"))//
        .isInstanceOf(SchemaException.class)//
        .hasMessageContaining("'C[k]'");

    assertUnchanged("C", "P", "C[k]");
  }

  @Test
  void indexOfATypeBelowRefusesTheRemoval() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.command("sql", "CREATE DOCUMENT TYPE X EXTENDS C");
    database.command("sql", "CREATE DOCUMENT TYPE Y EXTENDS X");
    database.command("sql", "CREATE INDEX ON Y (k) NOTUNIQUE");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE C SUPERTYPE -P"))//
        .isInstanceOf(SchemaException.class)//
        .hasMessageContaining("'Y[k]'");

    assertUnchanged("C", "P", "Y[k]");
    assertThat(database.getSchema().getType("Y").getPolymorphicPropertyNames()).contains("k");
  }

  @Test
  void compositeIndexRefusesTheRemoval() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.command("sql", "CREATE PROPERTY C.s STRING");
    database.command("sql", "CREATE INDEX ON C (s, k) UNIQUE");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE C SUPERTYPE -P"))//
        .isInstanceOf(SchemaException.class)//
        .hasMessageContaining("'k'")//
        .hasMessageContaining("'C[s,k]'");

    assertUnchanged("C", "P", "C[s,k]");
  }

  @Test
  void byItemIndexRefusesTheRemoval() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.tags LIST OF STRING");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.command("sql", "CREATE INDEX ON C (tags BY ITEM) NOTUNIQUE");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE C SUPERTYPE -P"))//
        .isInstanceOf(SchemaException.class)//
        .hasMessageContaining("'tags'");

    assertThat(database.getSchema().getType("C").isSubTypeOf("P")).isTrue();
  }

  @Test
  void propertyTheTypeDeclaresItselfAllowsTheRemoval() {
    // a type declares a property a super type also declares only when the link comes after both (it is warned about, not refused)
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE C");
    database.command("sql", "CREATE PROPERTY C.k INTEGER");
    database.command("sql", "CREATE INDEX ON C (k) UNIQUE");
    database.command("sql", "ALTER TYPE C SUPERTYPE +P");

    database.command("sql", "ALTER TYPE C SUPERTYPE -P");

    assertThat(database.getSchema().getType("C").isSubTypeOf("P")).isFalse();
    assertThat(database.getSchema().existsIndex("C[k]")).isTrue();
  }

  @Test
  void propertyInheritedThroughAnotherSuperTypeAllowsTheRemoval() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE Q");
    database.command("sql", "CREATE PROPERTY Q.k INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P, Q");
    database.command("sql", "CREATE INDEX ON C (k) UNIQUE");

    database.command("sql", "ALTER TYPE C SUPERTYPE -P");

    assertThat(database.getSchema().getType("C").isSubTypeOf("P")).isFalse();
    assertThat(database.getSchema().getType("C").isSubTypeOf("Q")).isTrue();
    assertThat(database.getSchema().existsIndex("C[k]")).isTrue();
  }

  @Test
  void indexOfTheSuperTypeOnlyAllowsTheRemoval() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE INDEX ON P (k) UNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");

    database.command("sql", "ALTER TYPE C SUPERTYPE -P");

    assertThat(database.getSchema().getType("C").isSubTypeOf("P")).isFalse();
    assertThat(database.getSchema().existsIndex("P[k]")).isTrue();
  }

  private void assertUnchanged(final String type, final String superType, final String indexName) {
    assertThat(database.getSchema().getType(type).isSubTypeOf(superType)).isTrue();
    assertThat(database.getSchema().getType(superType).getSubTypes()).extracting(DocumentType::getName).contains(type);
    assertThat(database.getSchema().getType(type).getPolymorphicPropertyNames()).contains("k");
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    assertThat(index.isValid()).isTrue();
  }
}
