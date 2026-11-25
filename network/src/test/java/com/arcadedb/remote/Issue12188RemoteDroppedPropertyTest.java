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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Issue #12188: {@code RemoteDocumentType.reload(Result)} added and updated the properties {@code schema:types} lists but
 * never removed the ones it stopped listing, so a property dropped or renamed stayed in the cached type for the life of the
 * connection and its declared type kept decoding the values of the records read through it. The same reload only applied
 * the attributes of a property the server listed, and the server lists one only while it is set, so a constraint that was
 * cleared stayed in force on the client.
 * <p>
 * The types are built from the records the server answers, with no connection: the end-to-end behaviour is covered by
 * {@code Issue12188RemoteDroppedPropertyIT}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12188RemoteDroppedPropertyTest {
  private static final String TYPE_NAME = "Doc12188";

  private RemoteDatabase database;

  @BeforeEach
  void setUp() {
    // The records the types are built from are made here and no request is ever sent: the port is never contacted
    database = new RemoteDatabase("127.0.0.1", 1, "issue12188", "root", "test", new ContextConfiguration()) {
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
  void aPropertyDroppedLeavesTheTypeOnReload() {
    final RemoteDocumentType type = new RemoteDocumentType(database,
        schemaRecord(List.of(declared(1, "created", Type.DATETIME), declared(2, "oldName", Type.STRING))));
    assertThat(type.getPropertyNames()).containsExactlyInAnyOrder("created", "oldName");

    type.reload(schemaRecord(List.of(declared(2, "oldName", Type.STRING))));

    assertThat(type.existsProperty("created")).isFalse();
    assertThat(type.getPropertyIfExists("created")).isNull();
    // the lookup RemoteImmutableDocument decodes a value through
    assertThat(type.getPolymorphicPropertyIfExists("created")).isNull();
    assertThat(type.getPropertyNames()).containsExactly("oldName");
    assertThat(type.getProperties()).hasSize(1);
  }

  @Test
  void aPropertyRenamedLeavesTheTypeUnderItsOldName() {
    final RemoteDocumentType type = new RemoteDocumentType(database, schemaRecord(List.of(declared(2, "oldName", Type.STRING))));

    type.reload(schemaRecord(List.of(declared(3, "newName", Type.STRING))));

    assertThat(type.existsProperty("oldName")).isFalse();
    assertThat(type.getPropertyNames()).containsExactly("newName");
  }

  @Test
  void everyPropertyDroppedLeavesTheTypeEmpty() {
    final RemoteDocumentType type = new RemoteDocumentType(database,
        schemaRecord(List.of(declared(1, "a", Type.STRING), declared(2, "b", Type.INTEGER))));

    type.reload(schemaRecord(List.of()));

    assertThat(type.getPropertyNames()).isEmpty();
    assertThat(type.getProperties()).isEmpty();
  }

  @Test
  void aPropertyStillListedKeepsItsInstanceAndTakesTheNewAttributes() {
    final RemoteDocumentType type = new RemoteDocumentType(database,
        schemaRecord(List.of(declared(1, "created", Type.DATETIME), declared(2, "oldName", Type.STRING))));
    final Property kept = type.getPropertyIfExists("oldName");
    assertThat(kept.isMandatory()).isFalse();

    final Map<String, Object> altered = declared(2, "oldName", Type.STRING);
    altered.put("mandatory", true);
    type.reload(schemaRecord(List.of(altered)));

    assertThat(type.getPropertyIfExists("oldName")).isSameAs(kept);
    assertThat(kept.isMandatory()).isTrue();
  }

  @Test
  void aPropertyDeclaredAgainWithAnotherTypeIsNotDecodedByTheOldDeclaration() {
    final RemoteDocumentType type = new RemoteDocumentType(database, schemaRecord(List.of(declared(1, "created", Type.DATETIME))));
    assertThat(type.getPropertyIfExists("created").getType()).isEqualTo(Type.DATETIME);

    // dropped and declared again as a LONG by another connection, or by raw DDL, between two reloads: the name is still
    // there, so only the declaration can tell, and a property's type is fixed when it is built
    type.reload(schemaRecord(List.of(declared(1, "created", Type.LONG))));

    assertThat(type.getPropertyIfExists("created").getType()).isEqualTo(Type.LONG);
    assertThat(type.getPolymorphicPropertyIfExists("created").getType()).isEqualTo(Type.LONG);
  }

  @Test
  void aViewOfTheNamesTakenBeforeAReloadIsNotBrokenByIt() {
    final RemoteDocumentType type = new RemoteDocumentType(database,
        schemaRecord(List.of(declared(1, "created", Type.DATETIME), declared(2, "oldName", Type.STRING))));
    final Iterator<String> names = type.getPropertyNames().iterator();

    type.reload(schemaRecord(List.of(declared(2, "oldName", Type.STRING))));

    // the reload publishes a new map, as RemoteSchema.reload() does with its own: it must not mutate the one a reader is walking
    assertThatCode(() -> {
      while (names.hasNext())
        names.next();
    }).doesNotThrowAnyException();
  }

  @Test
  void anAttributeTheServerNoLongerListsIsForgottenOnReload() {
    final Map<String, Object> constrained = declared(1, "tags", Type.LIST);
    constrained.put("ofType", "STRING");
    constrained.put("mandatory", true);
    constrained.put("readOnly", true);
    constrained.put("notNull", true);
    constrained.put("hidden", true);
    constrained.put("external", true);
    constrained.put("compression", "fast");
    constrained.put("min", "1");
    constrained.put("max", "9");
    constrained.put("default", "[]");
    constrained.put("regexp", "[a-z]+");
    constrained.put("custom", Map.of("k", "v"));
    final RemoteDocumentType type = new RemoteDocumentType(database, schemaRecord(List.of(constrained)));
    final Property property = type.getPropertyIfExists("tags");
    assertThat(property.getOfType()).isEqualTo("STRING");
    assertThat(property.isMandatory()).isTrue();
    assertThat(property.isReadonly()).isTrue();
    assertThat(property.isNotNull()).isTrue();
    assertThat(property.isHidden()).isTrue();
    assertThat(property.isExternal()).isTrue();
    assertThat(property.getCompression()).isEqualTo("fast");
    assertThat(property.getMin()).isEqualTo("1");
    assertThat(property.getMax()).isEqualTo("9");
    assertThat(property.getDefaultValueDefinition()).isEqualTo("[]");
    assertThat(property.getRegexp()).isEqualTo("[a-z]+");

    // ALTER PROPERTY ... clearing every one of them: FetchFromSchemaTypesStep then lists the property without them
    final Map<String, Object> cleared = declared(1, "tags", Type.LIST);
    cleared.put("custom", Map.of());
    type.reload(schemaRecord(List.of(cleared)));

    assertThat(type.getPropertyIfExists("tags")).isSameAs(property);
    assertThat(property.getOfType()).isNull();
    assertThat(property.isMandatory()).isFalse();
    assertThat(property.isReadonly()).isFalse();
    assertThat(property.isNotNull()).isFalse();
    assertThat(property.isHidden()).isFalse();
    assertThat(property.isExternal()).isFalse();
    assertThat(property.getCompression()).isEqualTo("none");
    assertThat(property.getMin()).isNull();
    assertThat(property.getMax()).isNull();
    assertThat(property.getDefaultValueDefinition()).isNull();
    assertThat(property.getRegexp()).isNull();
    assertThat(property.getCustomKeys()).isEmpty();
  }

  @Test
  void aFirstDeclarationWithNoAttributeIsAPlainProperty() {
    final RemoteDocumentType type = new RemoteDocumentType(database, schemaRecord(List.of(declared(1, "plain", Type.STRING))));

    final Property property = type.getPropertyIfExists("plain");

    assertThat(property.isMandatory()).isFalse();
    assertThat(property.getMin()).isNull();
    assertThat(property.getDefaultValueDefinition()).isNull();
    assertThat(property.getCompression()).isEqualTo("none");
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
