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
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every schema mutation reachable through a {@link DocumentType} needs {@code UPDATE_SCHEMA} and is refused while the database does
 * not accept writes, and a refusal changes nothing: the check is the first thing each one does. The property setters have their own
 * class ({@link PropertySetterAuthorizationTest}), the edge type declarations theirs ({@link EdgeTypeDeclarationAuthorizationTest}).
 */
class TypeSchemaMutationAuthorizationTest {
  private static final String PATH   = "target/databases/TypeSchemaMutationAuthorizationTest";
  private static final String REASON = "restore in progress";

  /** A mutation of the type, and the test that tells it did not happen. */
  private record Mutation(Runnable apply, BooleanSupplier untouched) {
  }

  private final Map<String, Mutation> mutations = new TreeMap<>();

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    final Schema schema = database.getSchema();
    final DocumentType parent = schema.createDocumentType("Parent");
    final LocalDocumentType type = (LocalDocumentType) schema.createDocumentType("Doc");
    type.createProperty("name", Type.STRING);
    type.createProperty("other", Type.INTEGER);
    type.createProperty("indexed", Type.INTEGER);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "indexed");
    final DocumentType child = schema.createDocumentType("Child");
    child.addSuperType(parent);
    final Bucket spare = schema.createBucket("Spare");
    final Bucket attached = schema.createBucket("Attached");
    type.addBucket(attached);

    final DocumentType other = schema.createDocumentType("Other");

    mutations.put("addSuperType(String)", new Mutation(() -> other.addSuperType("Parent"), () -> other.getSuperTypes().isEmpty()));
    mutations.put("addSuperType(DocumentType)", new Mutation(() -> other.addSuperType(parent), () -> other.getSuperTypes().isEmpty()));
    mutations.put("removeSuperType(String)", new Mutation(() -> child.removeSuperType("Parent"), () -> child.getSuperTypes().size() == 1));
    mutations.put("removeSuperType(DocumentType)",
        new Mutation(() -> child.removeSuperType(parent), () -> child.getSuperTypes().size() == 1));
    mutations.put("setSuperTypes", new Mutation(() -> child.setSuperTypes(List.of()), () -> child.getSuperTypes().size() == 1));
    mutations.put("rename", new Mutation(() -> type.rename("Renamed"), () -> schema.existsType("Doc") && !schema.existsType("Renamed")));
    mutations.put("setAliases", new Mutation(() -> type.setAliases(Set.of("Alias")), () -> type.getAliases().isEmpty()));
    mutations.put("createProperty(String, Type)",
        new Mutation(() -> type.createProperty("p1", Type.STRING), () -> !type.existsProperty("p1")));
    mutations.put("createProperty(String, String)",
        new Mutation(() -> type.createProperty("p2", "STRING"), () -> !type.existsProperty("p2")));
    mutations.put("createProperty(String, Class)",
        new Mutation(() -> type.createProperty("p3", String.class), () -> !type.existsProperty("p3")));
    mutations.put("createProperty(String, Type, String)",
        new Mutation(() -> type.createProperty("p4", Type.LIST, "STRING"), () -> !type.existsProperty("p4")));
    mutations.put("dropProperty", new Mutation(() -> type.dropProperty("other"), () -> type.existsProperty("other")));
    mutations.put("dropPropertyAndItsIndexes",
        new Mutation(() -> type.dropPropertyAndItsIndexes("indexed", true), () -> type.existsProperty("indexed")
            && type.getAllIndexes(false).size() == 1));
    mutations.put("renameProperty", new Mutation(() -> type.renameProperty("name", "label"), () -> type.existsProperty("name")
        && !type.existsProperty("label")));
    mutations.put("createTypeIndex", new Mutation(() -> type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name"),
        () -> type.getAllIndexes(false).size() == 1));
    mutations.put("getOrCreateTypeIndex", new Mutation(() -> type.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "other"),
        () -> type.getAllIndexes(false).size() == 1));
    mutations.put("addBucket", new Mutation(() -> type.addBucket(spare), () -> type.getBuckets(false).size() == 2));
    mutations.put("removeBucket", new Mutation(() -> type.removeBucket(attached), () -> type.getBuckets(false).size() == 2));
    mutations.put("setBucketSelectionStrategy(strategy)", new Mutation(
        () -> type.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy()),
        () -> "round-robin".equals(type.getBucketSelectionStrategy().getName())));
    mutations.put("setBucketSelectionStrategy(name)", new Mutation(() -> type.setBucketSelectionStrategy("thread"),
        () -> "round-robin".equals(type.getBucketSelectionStrategy().getName())));
    mutations.put("setCustomValue", new Mutation(() -> type.setCustomValue("k", "v"), () -> type.getCustomValue("k") == null));
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

  @Test
  void everyMutationNeedsSchemaPermission() {
    bindUserWithoutSchemaPermission();

    for (final Map.Entry<String, Mutation> entry : mutations.entrySet()) {
      assertThatThrownBy(() -> entry.getValue().apply().run()).as(entry.getKey()).isInstanceOf(SecurityException.class)
          .hasMessageContaining("not allowed to update schema");
      assertThat(entry.getValue().untouched().getAsBoolean()).as(entry.getKey() + " left the type alone").isTrue();
    }
  }

  @Test
  void everyMutationIsRefusedWhileTheDatabaseDoesNotAcceptWrites() {
    ((LocalDatabase) database).refuseWrites(REASON);

    for (final Map.Entry<String, Mutation> entry : mutations.entrySet()) {
      assertThatThrownBy(() -> entry.getValue().apply().run()).as(entry.getKey()).isInstanceOf(NeedRetryException.class)
          .hasMessageContaining(REASON);
      assertThat(entry.getValue().untouched().getAsBoolean()).as(entry.getKey() + " left the type alone").isTrue();
    }
  }

  @Test
  void everyMutationWorksWithSchemaPermissionAndAcceptedWrites() {
    // POSITIVE CONTROL, ONE BY ONE: THE MUTATIONS THE TESTS ABOVE REFUSE ARE VALID ONES AND CHANGE THE TYPE
    for (final Map.Entry<String, Mutation> entry : mutations.entrySet()) {
      entry.getValue().apply().run();
      assertThat(entry.getValue().untouched().getAsBoolean()).as(entry.getKey() + " changed the type").isFalse();
      // BACK TO THE STARTING POINT FOR THE ONES THAT FOLLOW, WHICH MAY NEED WHAT THIS ONE TOOK AWAY
      tearDown();
      setUp();
    }
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
