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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12189: {@code schema:types} writes the NAME of the constant ({@code ARRAY_OF_LONGS}) while {@link Type#getTypeByName}
 * knows the five array types only by their declared name ({@code Long[]}), so a remote property declared as one had no type and
 * the values of its records stayed the list the JSON parser built. The type of a property is also what
 * {@code RemoteDocumentType.reload()} compares to tell a property that is still the same declaration from one dropped and
 * declared again: both places must resolve the name the same way, or an array property is rebuilt by every reload and a
 * reference a caller holds stops tracking {@code ALTER PROPERTY} (issue #12188).
 * <p>
 * The types are built from the records the server answers, with no connection: the end-to-end behaviour is covered by
 * {@code Issue12189RemoteArrayPropertyIT}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12189RemoteArrayPropertyTest {
  private static final String TYPE_NAME = "Doc12189";

  private static final List<Type> ARRAY_TYPES = List.of(Type.ARRAY_OF_SHORTS, Type.ARRAY_OF_INTEGERS, Type.ARRAY_OF_LONGS,
      Type.ARRAY_OF_FLOATS, Type.ARRAY_OF_DOUBLES);

  private RemoteDatabase database;

  @BeforeEach
  void setUp() {
    // The records the types are built from are made here and no request is ever sent: the port is never contacted
    database = new RemoteDatabase("127.0.0.1", 1, "issue12189", "root", "test", new ContextConfiguration()) {
      @Override
      void requestClusterConfiguration() {
        // NO HTTP CALLS IN A UNIT TEST
      }
    };
  }

  @AfterEach
  void tearDown() {
    database.close();
  }

  @Test
  void typeOfResolvesTheConstantNameOfEveryArrayType() {
    for (final Type type : ARRAY_TYPES) {
      assertThat(RemoteProperty.typeOf(type.name())).as(type.name()).isEqualTo(type);
      // what schema:types wrote before the constant name did, and what a declaration is stored under
      assertThat(RemoteProperty.typeOf(type.getName())).as(type.getName()).isEqualTo(type);
    }
  }

  @Test
  void typeOfKeepsResolvingEveryOtherType() {
    for (final Type type : Type.values())
      assertThat(RemoteProperty.typeOf(type.name())).as(type.name()).isEqualTo(type);
    assertThat(RemoteProperty.typeOf("string")).isEqualTo(Type.STRING);
    assertThat(RemoteProperty.typeOf("array_of_longs")).isEqualTo(Type.ARRAY_OF_LONGS);
  }

  @Test
  void typeOfAnUnknownOrMissingNameIsNull() {
    assertThat(RemoteProperty.typeOf(null)).isNull();
    assertThat(RemoteProperty.typeOf("")).isNull();
    assertThat(RemoteProperty.typeOf("NOT_A_TYPE")).isNull();
  }

  @Test
  void anArrayPropertyKnowsItsDeclaredType() {
    final RemoteDocumentType type = new RemoteDocumentType(database, schemaRecord(List.of(declared(1, "shorts", Type.ARRAY_OF_SHORTS),
        declared(2, "ints", Type.ARRAY_OF_INTEGERS), declared(3, "longs", Type.ARRAY_OF_LONGS),
        declared(4, "floats", Type.ARRAY_OF_FLOATS), declared(5, "doubles", Type.ARRAY_OF_DOUBLES))));

    assertThat(type.getPropertyIfExists("shorts").getType()).isEqualTo(Type.ARRAY_OF_SHORTS);
    assertThat(type.getPropertyIfExists("ints").getType()).isEqualTo(Type.ARRAY_OF_INTEGERS);
    assertThat(type.getPropertyIfExists("longs").getType()).isEqualTo(Type.ARRAY_OF_LONGS);
    assertThat(type.getPropertyIfExists("floats").getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
    assertThat(type.getPropertyIfExists("doubles").getType()).isEqualTo(Type.ARRAY_OF_DOUBLES);
  }

  @Test
  void anArrayPropertyStillListedKeepsItsInstanceAndTakesTheNewAttributes() {
    for (final Type arrayType : ARRAY_TYPES) {
      final RemoteDocumentType type = new RemoteDocumentType(database, schemaRecord(List.of(declared(1, "values", arrayType))));
      final Property kept = type.getPropertyIfExists("values");
      assertThat(kept.isMandatory()).isFalse();

      final Map<String, Object> altered = declared(1, "values", arrayType);
      altered.put("mandatory", true);
      type.reload(schemaRecord(List.of(altered)));

      // describes() must resolve the type name the way the constructor did, or this property is rebuilt by every reload
      assertThat(type.getPropertyIfExists("values")).as(arrayType.name()).isSameAs(kept);
      assertThat(kept.isMandatory()).as(arrayType.name()).isTrue();
      assertThat(kept.getType()).isEqualTo(arrayType);
    }
  }

  @Test
  void anArrayPropertyDeclaredAgainWithAnotherArrayTypeIsAnotherProperty() {
    final RemoteDocumentType type = new RemoteDocumentType(database, schemaRecord(List.of(declared(1, "values", Type.ARRAY_OF_LONGS))));
    final Property before = type.getPropertyIfExists("values");

    // dropped and declared again as ARRAY_OF_INTEGERS under the same id: only the declared type can tell
    type.reload(schemaRecord(List.of(declared(1, "values", Type.ARRAY_OF_INTEGERS))));

    assertThat(type.getPropertyIfExists("values")).isNotSameAs(before);
    assertThat(type.getPropertyIfExists("values").getType()).isEqualTo(Type.ARRAY_OF_INTEGERS);
  }

  @Test
  void anArrayPropertyDeclaredAgainAsAScalarIsAnotherProperty() {
    final RemoteDocumentType type = new RemoteDocumentType(database, schemaRecord(List.of(declared(1, "values", Type.ARRAY_OF_LONGS))));
    final Property before = type.getPropertyIfExists("values");

    type.reload(schemaRecord(List.of(declared(1, "values", Type.LONG))));

    assertThat(type.getPropertyIfExists("values")).isNotSameAs(before);
    assertThat(type.getPropertyIfExists("values").getType()).isEqualTo(Type.LONG);
  }

  /** One entry of the {@code properties} list of a {@code schema:types} record. */
  private static Map<String, Object> declared(final int id, final String name, final Type type) {
    final Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("id", id);
    entry.put("name", name);
    entry.put("type", type.name());
    return entry;
  }

  /** The {@code schema:types} record of {@link #TYPE_NAME} listing exactly these properties. */
  private static Result schemaRecord(final List<Map<String, Object>> properties) {
    final Map<String, Object> content = new LinkedHashMap<>();
    content.put("name", TYPE_NAME);
    content.put("type", "document");
    content.put("records", 0);
    content.put("buckets", List.of(TYPE_NAME + "_0"));
    content.put("bucketSelectionStrategy", "round-robin");
    content.put("parentTypes", List.of());
    content.put("properties", properties);
    content.put("custom", Map.of());
    return new ResultInternal(content);
  }
}
