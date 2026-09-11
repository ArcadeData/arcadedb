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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A user granted only record access (create/read/update/delete on every type) could not run a Cypher
 * {@code CREATE (:ExistingType {...})}: the label resolution goes through {@code getOrCreateVertexType}, whose
 * builder demanded {@code UPDATE_SCHEMA} before even looking at whether the type already existed, so a statement
 * that changed nothing in the schema was refused with "not allowed to update schema" while the equivalent SQL
 * {@code INSERT INTO} worked (issue #7368).
 * <p>
 * The permission must be demanded exactly when the builder is about to mutate the schema: a missing type, a type
 * missing a requested bucket, or a type missing a requested super type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7368CreateOnExistingTypeWithoutSchemaPermissionTest {
  private static final String PATH = "target/databases/issue7368";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.getSchema().createVertexType("project_metadata");
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Employee");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    database.drop();
    factory.close();
  }

  @Test
  void cypherCreateOnAnExistingVertexTypeNeedsNoSchemaPermission() {
    bindRecordOnlyUser();

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "CREATE (p:project_metadata {projectId: 'test123', accountId: 1, name: 'test'}) RETURN p.projectId AS id")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("id")).isEqualTo("test123");
      }
    });

    assertThat(database.countType("project_metadata", false)).isEqualTo(1);
  }

  @Test
  void cypherCreateOfAnEdgeBetweenExistingTypesNeedsNoSchemaPermission() {
    bindRecordOnlyUser();

    database.transaction(() -> database.command("opencypher",
        "CREATE (a:Person {name: 'a'})-[:KNOWS]->(b:Person {name: 'b'})").close());

    assertThat(database.countType("KNOWS", false)).isEqualTo(1);
  }

  @Test
  void cypherCreateOnAMissingLabelIsStillRefused() {
    bindRecordOnlyUser();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "CREATE (p:NotDeclaredYet {name: 'x'})").close()))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed to update schema");

    assertThat(database.getSchema().existsType("NotDeclaredYet")).isFalse();
  }

  @Test
  void cypherCreateWithAMissingCompositeLabelTypeIsStillRefused() {
    // Both labels exist, but the composite type Person~Employee that a multi-label vertex is stored in does not,
    // so this CREATE would have to add a type and inherits the schema gate.
    bindRecordOnlyUser();

    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "CREATE (p:Person:Employee {name: 'x'})").close()))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed to update schema");
  }

  @Test
  void getOrCreateOnAnExistingTypeNeedsNoSchemaPermission() {
    bindRecordOnlyUser();

    final DocumentType existing = database.getSchema().getOrCreateVertexType("Person");
    assertThat(existing.getName()).isEqualTo("Person");
    assertThat(database.getSchema().getOrCreateEdgeType("KNOWS").getName()).isEqualTo("KNOWS");
    assertThat(database.getSchema().getOrCreateDocumentType("Person").getName()).isEqualTo("Person");
  }

  @Test
  void getOrCreateThatWouldAddBucketsOrSuperTypesIsStillRefused() {
    bindRecordOnlyUser();

    final int declared = database.getSchema().getType("Person").getBuckets(false).size();
    assertThatThrownBy(() -> database.getSchema().getOrCreateVertexType("Person", declared + 1))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed to update schema");
    assertThat(database.getSchema().getType("Person").getBuckets(false)).hasSize(declared);

    assertThatThrownBy(() -> database.getSchema().buildVertexType().withName("Employee").withSuperType("Person")
        .withIgnoreIfExists(true).create())
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed to update schema");
    assertThat(database.getSchema().getType("Employee").getSuperTypes()).isEmpty();

    assertThatThrownBy(() -> database.getSchema().getOrCreateVertexType("Brand_New"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed to update schema");
    assertThat(database.getSchema().existsType("Brand_New")).isFalse();
  }

  @Test
  void getOrCreateOnAnExistingTypeOfAnotherKindStillFailsAsBefore() {
    // The "already exists" fast path must keep refusing a kind mismatch even when the caller holds no schema grant:
    // the check answers "is this the type you asked for", not "may you change it".
    bindRecordOnlyUser();

    assertThatThrownBy(() -> database.getSchema().getOrCreateEdgeType("Person"))
        .hasMessageContaining("is not a edge type");
  }

  private void bindRecordOnlyUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "crud";
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
