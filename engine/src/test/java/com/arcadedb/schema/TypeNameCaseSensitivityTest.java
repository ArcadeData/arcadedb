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
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Type names are stored and looked up by an unadorned {@code Map<String,...>} ({@link LocalSchema#types}), with no
 * case normalization anywhere on the creation or lookup path. "Person", "person" and "PERSON" are therefore three
 * distinct, independently creatable types rather than the same type referenced three different ways.
 */
class TypeNameCaseSensitivityTest extends TestHelper {

  @Test
  void personPersonAndPERSONAreThreeDistinctTypes() {
    database.getSchema().createVertexType("Person", 1);
    database.getSchema().createEdgeType("person", 1);
    database.getSchema().createVertexType("PERSON", 1);

    assertThat(database.getSchema().existsType("Person")).isTrue();
    assertThat(database.getSchema().existsType("person")).isTrue();
    assertThat(database.getSchema().existsType("PERSON")).isTrue();

    assertThat(database.getSchema().getType("Person")).isInstanceOf(VertexType.class);
    assertThat(database.getSchema().getType("person")).isInstanceOf(EdgeType.class);
    assertThat(database.getSchema().getType("PERSON")).isInstanceOf(VertexType.class);

    assertThat(database.getSchema().getType("Person")).isNotSameAs(database.getSchema().getType("person"));
    assertThat(database.getSchema().getType("Person")).isNotSameAs(database.getSchema().getType("PERSON"));

    assertThat(database.getSchema().getTypes()).hasSize(3);
  }

  /**
   * The SQL layer does not uppercase/lowercase unquoted type-name identifiers either: a type created as "Foo" via
   * DDL is not found by a SELECT that spells it "foo", and CREATE VERTEX TYPE "foo" creates a second, independent
   * type rather than colliding with "Foo".
   */
  @Test
  void sqlDdlAndQueriesAreAlsoCaseSensitiveOnTypeNames() {
    database.command("sql", "CREATE VERTEX TYPE Foo");
    database.transaction(() -> database.command("sql", "INSERT INTO Foo SET name = 'a'"));

    assertThatThrownBy(() -> database.query("sql", "SELECT FROM foo").stream().toList())
        .isInstanceOf(SchemaException.class);

    database.command("sql", "CREATE VERTEX TYPE foo");
    assertThat(database.getSchema().existsType("Foo")).isTrue();
    assertThat(database.getSchema().existsType("foo")).isTrue();
    assertThat(database.getSchema().getType("Foo")).isNotSameAs(database.getSchema().getType("foo"));

    assertThat(database.query("sql", "SELECT FROM foo").stream().toList()).isEmpty();
    assertThat(database.query("sql", "SELECT FROM Foo").stream().toList()).hasSize(1);
  }
}
