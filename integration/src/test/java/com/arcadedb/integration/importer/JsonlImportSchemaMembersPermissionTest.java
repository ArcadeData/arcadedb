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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A JSONL file whose schema line declares no type but carries schema members (a trigger here) is a schema change like any other:
 * {@code IMPORT DATABASE} of it needs the schema permission besides the one the statement itself asks, and installs nothing without it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class JsonlImportSchemaMembersPermissionTest {
  private static final String DATABASE_PATH = "target/databases/jsonl-import-schema-members-permission";

  private final File source = new File("target/jsonl-import-schema-members-permission.jsonl");
  private       Database database;

  @BeforeEach
  void createDatabase() throws IOException {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    database = new DatabaseFactory(DATABASE_PATH).setSecurity(db -> {
    }).create();
    database.command("sql", "CREATE DOCUMENT TYPE Target");

    final JSONObject schemaLine = new JSONObject().put("t", "schema").put("c", new JSONObject()
        .put("settings", new JSONObject()
            .put("dateFormat", "yyyy-MM-dd")
            .put("dateTimeFormat", "yyyy-MM-dd HH:mm:ss")
            .put("zoneId", "UTC"))
        .put("types", new JSONObject())
        .put("triggers", new JSONObject().put("restoredTrigger", new JSONObject()
            .put("name", "restoredTrigger")
            .put("typeName", "Target")
            .put("timing", "AFTER")
            .put("event", "CREATE")
            .put("actionType", "SQL")
            .put("actionCode", "SELECT 1"))));
    Files.writeString(source.toPath(), schemaLine + "\n", StandardCharsets.UTF_8);
  }

  @AfterEach
  void dropDatabase() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    source.delete();
    TestHelper.checkActiveDatabases();
  }

  @Test
  void anImportWithoutTheSchemaPermissionInstallsNoMember() {
    actAs(DATABASE_ACCESS.UPDATE_SECURITY, DATABASE_ACCESS.UPDATE_DATABASE_SETTINGS);

    assertThatThrownBy(() -> database.command("sql", "IMPORT DATABASE file://" + source.getAbsolutePath()))
        .isInstanceOf(SecurityException.class);

    assertThat(database.getSchema().getEmbedded().existsTrigger("restoredTrigger")).isFalse();
  }

  @Test
  void anImportWithThePermissionsOfASchemaAdministratorRestoresTheMembers() {
    actAs(DATABASE_ACCESS.UPDATE_SECURITY, DATABASE_ACCESS.UPDATE_DATABASE_SETTINGS, DATABASE_ACCESS.UPDATE_SCHEMA);

    database.command("sql", "IMPORT DATABASE file://" + source.getAbsolutePath());

    assertThat(database.getSchema().getEmbedded().existsTrigger("restoredTrigger")).isTrue();
  }

  private void actAs(final DATABASE_ACCESS... allowed) {
    final Set<DATABASE_ACCESS> granted = Set.of(allowed);
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "limited-user";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return granted.contains(access);
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
    });
  }
}
