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
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The edge-type declarations settable after creation ({@code lightweight}, {@code unique}) and the type-level
 * maintenance that rewrites the schema ({@code REBUILD TYPE}, {@code ALTER TYPE ... WITH repartition = true}) are
 * schema mutations, so they need {@code UPDATE_SCHEMA} like every other ALTER TYPE form. The bare
 * {@code ALTER TYPE <name> WITH <settings>} form has no items, so it used to reach the edge-type setters without
 * passing through any guarded method.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EdgeTypeDeclarationAuthorizationTest {
  private static final String PATH = "target/databases/EdgeTypeDeclarationAuthorizationTest";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.getSchema().createDocumentType("Note");
    database.getSchema().createEdgeType("FriendOf");
    database.getSchema().createEdgeType("UniqueLink");
    database.command("sql", "ALTER TYPE UniqueLink WITH unique = true").close();
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    if (database != null && database.isOpen())
      database.drop();
    factory.close();
  }

  @Test
  void alterTypeWithLightweightNeedsSchemaPermission() {
    bindUserWithoutSchemaPermission();

    assertRefused("ALTER TYPE FriendOf WITH lightweight = true");
    assertThat(edgeType("FriendOf").isLightweight()).isFalse();
  }

  @Test
  void alterTypeWithdrawingLightweightNeedsSchemaPermission() {
    database.command("sql", "ALTER TYPE FriendOf WITH lightweight = true").close();
    bindUserWithoutSchemaPermission();

    assertRefused("ALTER TYPE FriendOf WITH lightweight = false");
    assertThat(edgeType("FriendOf").isLightweight()).isTrue();
  }

  @Test
  void alterTypeWithUniqueNeedsSchemaPermission() {
    bindUserWithoutSchemaPermission();

    assertRefused("ALTER TYPE FriendOf WITH unique = true");
    assertThat(edgeType("FriendOf").isUnique()).isFalse();
    assertThat(database.getSchema().existsIndex(LocalEdgeType.uniqueIndexName("FriendOf"))).isFalse();
  }

  @Test
  void alterTypeWithdrawingUniqueNeedsSchemaPermission() {
    bindUserWithoutSchemaPermission();

    assertRefused("ALTER TYPE UniqueLink WITH unique = false");
    assertThat(edgeType("UniqueLink").isUnique()).isTrue();
    assertThat(database.getSchema().existsIndex(LocalEdgeType.uniqueIndexName("UniqueLink"))).isTrue();
  }

  @Test
  void alterTypeWithUniqueOnALightweightTypeNeedsSchemaPermission() {
    // On a lightweight type the flag is the whole constraint: no index is built, so no guarded index path is reached.
    database.command("sql", "ALTER TYPE FriendOf WITH lightweight = true").close();
    bindUserWithoutSchemaPermission();

    assertRefused("ALTER TYPE FriendOf WITH unique = true");
    assertThat(edgeType("FriendOf").isUnique()).isFalse();
  }

  @Test
  void edgeTypeSettersNeedSchemaPermission() {
    bindUserWithoutSchemaPermission();

    final LocalEdgeType friendOf = edgeType("FriendOf");
    assertThatThrownBy(() -> friendOf.setLightweight(true)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> friendOf.setLightweight(false)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> friendOf.setUnique(true)).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> edgeType("UniqueLink").setUnique(false)).isInstanceOf(SecurityException.class);

    assertThat(friendOf.isLightweight()).isFalse();
    assertThat(friendOf.isUnique()).isFalse();
    assertThat(edgeType("UniqueLink").isUnique()).isTrue();
  }

  @Test
  void typeRebuildNeedsSchemaPermission() {
    bindUserWithoutSchemaPermission();

    assertRefused("REBUILD TYPE Note");
    assertRefused("REBUILD TYPE Note WITH repartition = true");
    assertRefused("ALTER TYPE Note WITH repartition = true");
  }

  @Test
  void settingsStayAvailableWithSchemaPermission() {
    // Positive control: no bound user (embedded) keeps every form working.
    database.command("sql", "ALTER TYPE FriendOf WITH lightweight = true, unique = true").close();
    assertThat(edgeType("FriendOf").isLightweight()).isTrue();
    assertThat(edgeType("FriendOf").isUnique()).isTrue();

    database.command("sql", "ALTER TYPE UniqueLink WITH unique = false").close();
    assertThat(edgeType("UniqueLink").isUnique()).isFalse();
    assertThat(database.getSchema().existsIndex(LocalEdgeType.uniqueIndexName("UniqueLink"))).isFalse();

    database.command("sql", "REBUILD TYPE Note").close();
    database.command("sql", "ALTER TYPE Note WITH repartition = true").close();
  }

  private void assertRefused(final String sql) {
    assertThatThrownBy(() -> database.command("sql", sql).close())
        .as(sql)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed to update schema");
  }

  private LocalEdgeType edgeType(final String name) {
    return (LocalEdgeType) database.getSchema().getType(name);
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
