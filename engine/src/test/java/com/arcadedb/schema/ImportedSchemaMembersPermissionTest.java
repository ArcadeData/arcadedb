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
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The schema members a file carries (triggers, function libraries, extensions, views, aggregates) are schema: restoring them from a
 * file handed to the engine needs the schema permission, as creating each of them by hand does, whatever else the principal may do.
 * Host code (a JAVASCRIPT or JAVA trigger, a {@code js} function library) needs the security permission on top of it, never instead.
 * The database's own schema file stays trusted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ImportedSchemaMembersPermissionTest {
  private static final String DATABASE_PATH = "target/databases/test-imported-schema-members-permission";

  private Database database;
  private LocalSchema schema;

  @BeforeEach
  void createDatabase() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    database = new DatabaseFactory(DATABASE_PATH).setSecurity(db -> {
    }).create();
    database.command("sql", "CREATE DOCUMENT TYPE Target");
    database.command("sql", "CREATE DOCUMENT TYPE AuditLog");
    schema = database.getSchema().getEmbedded();
  }

  @AfterEach
  void dropDatabase() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  private void actAs(final DATABASE_ACCESS... allowed) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(principalAllowing(Set.of(allowed)));
  }

  @Test
  void aSqlTriggerOfAnImportedSchemaNeedsTheSchemaPermission() {
    actAs(DATABASE_ACCESS.UPDATE_SECURITY);

    assertThatThrownBy(() -> schema.restoreSchemaMembersFromJSON(schemaDeclaringTrigger("SQL", "INSERT INTO AuditLog SET what = 'created'"),
        LocalSchema.SchemaMemberSource.IMPORTED_FILE)).isInstanceOf(SecurityException.class);

    assertThat(schema.existsTrigger("audit")).isFalse();
  }

  @Test
  void aHostCodeTriggerOfAnImportedSchemaNeedsTheSchemaPermissionToo() {
    actAs(DATABASE_ACCESS.UPDATE_SECURITY);

    assertThatThrownBy(() -> schema.restoreSchemaMembersFromJSON(schemaDeclaringTrigger("JAVASCRIPT", "true;"),
        LocalSchema.SchemaMemberSource.IMPORTED_FILE)).isInstanceOf(SecurityException.class);

    assertThat(schema.existsTrigger("audit")).isFalse();
  }

  @Test
  void aFunctionLibraryOfAnImportedSchemaNeedsTheSchemaPermission() {
    actAs(DATABASE_ACCESS.UPDATE_SECURITY);

    assertThatThrownBy(() -> schema.restoreSchemaMembersFromJSON(schemaDeclaringFunctionLibrary("plain", "sql", "SELECT 1"),
        LocalSchema.SchemaMemberSource.IMPORTED_FILE)).isInstanceOf(SecurityException.class);

    assertThat(schema.hasFunctionLibrary("plain")).isFalse();
  }

  @Test
  void anExtensionOfAnImportedSchemaNeedsTheSchemaPermission() {
    actAs(DATABASE_ACCESS.UPDATE_SECURITY, DATABASE_ACCESS.UPDATE_DATABASE_SETTINGS);

    assertThatThrownBy(() -> schema.restoreSchemaMembersFromJSON(
        new JSONObject().put("extensions", new JSONObject().put("someModule", new JSONObject().put("mode", "fast"))),
        LocalSchema.SchemaMemberSource.IMPORTED_FILE)).isInstanceOf(SecurityException.class);

    assertThat(schema.getExtension("someModule")).isNull();
  }

  @Test
  void aPrincipalWithTheSchemaAndSecurityPermissionsStillRestoresHostCode() {
    actAs(DATABASE_ACCESS.UPDATE_SCHEMA, DATABASE_ACCESS.UPDATE_SECURITY);

    assertThat(schema.restoreSchemaMembersFromJSON(schemaDeclaringTrigger("JAVASCRIPT", "true;"),
        LocalSchema.SchemaMemberSource.IMPORTED_FILE)).isZero();

    assertThat(schema.existsTrigger("audit")).isTrue();
  }

  @Test
  void aPrincipalWithTheSchemaPermissionStillRestoresDeclarativeMembers() {
    actAs(DATABASE_ACCESS.UPDATE_SCHEMA);

    assertThat(schema.restoreSchemaMembersFromJSON(schemaDeclaringTrigger("SQL", "INSERT INTO AuditLog SET what = 'created'"),
        LocalSchema.SchemaMemberSource.IMPORTED_FILE)).isZero();

    assertThat(schema.existsTrigger("audit")).isTrue();
  }

  @Test
  void theDatabasesOwnSchemaFileStaysTrusted() {
    actAs();

    assertThat(schema.restoreSchemaMembersFromJSON(schemaDeclaringTrigger("JAVASCRIPT", "true;"),
        LocalSchema.SchemaMemberSource.SCHEMA_FILE)).isZero();

    assertThat(schema.existsTrigger("audit")).isTrue();
  }

  @Test
  void aHostCodeTriggerNeedsTheSchemaPermissionAsWellAsTheSecurityOne() {
    actAs(DATABASE_ACCESS.UPDATE_SECURITY);

    assertThatThrownBy(() -> schema.createTrigger(new TriggerImpl("audit", Trigger.TriggerTiming.AFTER, Trigger.TriggerEvent.CREATE,
        "Target", Trigger.ActionType.JAVASCRIPT, "true;"))).isInstanceOf(SecurityException.class);
    assertThat(schema.existsTrigger("audit")).isFalse();

    actAs(DATABASE_ACCESS.UPDATE_SCHEMA);

    assertThatThrownBy(() -> schema.createTrigger(new TriggerImpl("audit", Trigger.TriggerTiming.AFTER, Trigger.TriggerEvent.CREATE,
        "Target", Trigger.ActionType.JAVASCRIPT, "true;"))).isInstanceOf(SecurityException.class);
    assertThat(schema.existsTrigger("audit")).isFalse();

    actAs(DATABASE_ACCESS.UPDATE_SCHEMA, DATABASE_ACCESS.UPDATE_SECURITY);

    schema.createTrigger(new TriggerImpl("audit", Trigger.TriggerTiming.AFTER, Trigger.TriggerEvent.CREATE, "Target",
        Trigger.ActionType.JAVASCRIPT, "true;"));
    assertThat(schema.existsTrigger("audit")).isTrue();
  }

  private static JSONObject schemaDeclaringTrigger(final String actionType, final String actionCode) {
    return new JSONObject().put("triggers", new JSONObject().put("audit", new JSONObject()
        .put("name", "audit")
        .put("typeName", "Target")
        .put("timing", "AFTER")
        .put("event", "CREATE")
        .put("actionType", actionType)
        .put("actionCode", actionCode)));
  }

  private static JSONObject schemaDeclaringFunctionLibrary(final String libraryName, final String language, final String code) {
    return new JSONObject().put("functions", new JSONObject().put(libraryName, new JSONObject()
        .put("language", language)
        .put("functions", new JSONObject().put("run", new JSONObject().put("code", code).put("parameters", new JSONArray())))));
  }

  /** A principal allowed exactly {@code allowed} on the database, and everything on every file. */
  private static SecurityDatabaseUser principalAllowing(final Set<DATABASE_ACCESS> allowed) {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "limited-user";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return allowed.contains(access);
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
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
    };
  }
}
