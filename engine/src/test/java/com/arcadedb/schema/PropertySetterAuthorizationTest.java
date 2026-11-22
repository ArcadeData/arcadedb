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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every setter of a {@link Property} (and its rename) is a schema mutation: it needs {@code UPDATE_SCHEMA} like ALTER PROPERTY,
 * it is refused while the database does not accept writes, and a refusal changes nothing. The check is the first thing each
 * setter does, so it holds even when the value asked for is the one the property already has.
 */
class PropertySetterAuthorizationTest {
  private static final String PATH   = "target/databases/PropertySetterAuthorizationTest";
  private static final String REASON = "restore in progress";

  /** A mutation of a property that changes it, and the test that tells it did not happen. */
  private record Mutation(Consumer<Property> apply, Predicate<Property> untouched) {
  }

  private final Map<String, Mutation> mutations = new LinkedHashMap<>();

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("name", Type.STRING);
    type.createProperty("tags", Type.LIST);
    type.createProperty("age", Type.INTEGER);
    type.createProperty("blob", Type.STRING);

    mutations.put("setDefaultValue", new Mutation(p -> p.setDefaultValue("'x'"), p -> p.getDefaultValue() == null));
    mutations.put("setOfType", new Mutation(p -> p.setOfType("STRING"), p -> p.getOfType() == null));
    mutations.put("setReadonly", new Mutation(p -> p.setReadonly(true), p -> !p.isReadonly()));
    mutations.put("setMandatory", new Mutation(p -> p.setMandatory(true), p -> !p.isMandatory()));
    mutations.put("setNotNull", new Mutation(p -> p.setNotNull(true), p -> !p.isNotNull()));
    mutations.put("setHidden", new Mutation(p -> p.setHidden(true), p -> !p.isHidden()));
    mutations.put("setExternal", new Mutation(p -> p.setExternal(true), p -> !p.isExternal()));
    mutations.put("setCompression", new Mutation(p -> p.setCompression("max"), p -> "none".equals(p.getCompression())));
    mutations.put("setMax", new Mutation(p -> p.setMax("10"), p -> p.getMax() == null));
    mutations.put("setMin", new Mutation(p -> p.setMin("1"), p -> p.getMin() == null));
    mutations.put("setRegexp", new Mutation(p -> p.setRegexp("[a-z]+"), p -> p.getRegexp() == null));
    mutations.put("setCustomValue", new Mutation(p -> p.setCustomValue("k", "v"), p -> p.getCustomValue("k") == null));
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    if (database != null && database.isOpen())
      ((LocalDatabase) database).acceptWrites(REASON);
    if (database != null && database.isOpen())
      database.drop();
    factory.close();
  }

  /** The property each mutation is tried on: a LIST for {@code setOfType}, a STRING for the rest. */
  private Property propertyFor(final String mutation) {
    final DocumentType type = database.getSchema().getType("Doc");
    return type.getProperty(mutation.equals("setOfType") ? "tags" : mutation.equals("setMax") ? "age" : "name");
  }

  @Test
  void everySetterNeedsSchemaPermission() {
    bindUserWithoutSchemaPermission();

    for (final Map.Entry<String, Mutation> entry : mutations.entrySet()) {
      final Property property = propertyFor(entry.getKey());
      assertThatThrownBy(() -> entry.getValue().apply().accept(property)).as(entry.getKey()).isInstanceOf(SecurityException.class)
          .hasMessageContaining("not allowed to update schema");
      assertThat(entry.getValue().untouched().test(property)).as(entry.getKey() + " left the property alone").isTrue();
    }
  }

  @Test
  void aSetterAsksBeforeItLooksAtTheValue() {
    // THE VALUE THE PROPERTY ALREADY HAS IS NOT A CHANGE, AND IT IS STILL REFUSED
    final Property name = database.getSchema().getType("Doc").getProperty("name");
    name.setMandatory(true);
    name.setNotNull(true);
    name.setReadonly(true);
    name.setHidden(true);
    name.setCustomValue("k", "v");
    bindUserWithoutSchemaPermission();

    assertThatThrownBy(() -> name.setMandatory(true)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> name.setNotNull(true)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> name.setReadonly(true)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> name.setHidden(true)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> name.setCustomValue("k", "v")).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> name.setExternal(false)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> name.setDefaultValue(null)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> name.setMax(null)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> name.setMin(null)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> name.setRegexp(null)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> name.setCompression(null)).isInstanceOf(SecurityException.class);
  }

  @Test
  void renameNeedsSchemaPermission() {
    bindUserWithoutSchemaPermission();

    final Property name = database.getSchema().getType("Doc").getProperty("name");
    assertThatThrownBy(() -> name.rename("label")).isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed to update schema");

    assertThat(database.getSchema().getType("Doc").existsProperty("name")).isTrue();
    assertThat(database.getSchema().getType("Doc").existsProperty("label")).isFalse();
  }

  @Test
  void everySetterIsRefusedWhileTheDatabaseDoesNotAcceptWrites() {
    ((LocalDatabase) database).refuseWrites(REASON);

    for (final Map.Entry<String, Mutation> entry : mutations.entrySet()) {
      final Property property = propertyFor(entry.getKey());
      assertThatThrownBy(() -> entry.getValue().apply().accept(property)).as(entry.getKey()).isInstanceOf(NeedRetryException.class)
          .hasMessageContaining(REASON);
      assertThat(entry.getValue().untouched().test(property)).as(entry.getKey() + " left the property alone").isTrue();
    }

    final Property name = database.getSchema().getType("Doc").getProperty("name");
    assertThatThrownBy(() -> name.rename("label")).isInstanceOf(NeedRetryException.class);
    assertThat(database.getSchema().getType("Doc").existsProperty("label")).isFalse();
  }

  @Test
  void everySetterWorksWithSchemaPermissionAndAcceptedWrites() {
    // POSITIVE CONTROL: THE MUTATIONS THE TESTS ABOVE REFUSE ARE VALID ONES
    for (final Map.Entry<String, Mutation> entry : mutations.entrySet()) {
      final Property property = propertyFor(entry.getKey());
      entry.getValue().apply().accept(property);
      assertThat(entry.getValue().untouched().test(property)).as(entry.getKey() + " changed the property").isFalse();
    }

    database.getSchema().getType("Doc").getProperty("blob").rename("label");
    assertThat(database.getSchema().getType("Doc").existsProperty("label")).isTrue();
    assertThat(database.getSchema().getType("Doc").existsProperty("blob")).isFalse();
  }

  private void bindUserWithoutSchemaPermission() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "reader";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return false;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }

  private void unbindUser() {
    if (database != null && database.isOpen())
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }
}
