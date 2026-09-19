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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code JAVASCRIPT} or {@code JAVA} trigger is arbitrary host code running with the engine's own privileges, so
 * {@link LocalSchema#createTrigger} gates it on {@code UPDATE_SECURITY} rather than {@code UPDATE_SCHEMA}
 * (GHSA-38pf-6hp2-pxww, mirroring GHSA-vwjc-v7x7-cm6g for {@code DEFINE FUNCTION ... LANGUAGE js}).
 * <p>
 * Restoring a schema from an IMPORTED file must not be a way around that gate: running an import needs only
 * {@code UPDATE_SCHEMA}, so a JSONL file whose schema line declares a JAVASCRIPT trigger would otherwise install
 * code that can reach {@code database.getSecurity().createUser(...)} - handing back exactly the escalation the
 * advisory closed. Found in review of PR #7943, which is what first gave the importer a trigger to restore.
 * <p>
 * The database's own {@code schema.json} is the other side of the same rule and deliberately not gated: it records
 * what this database already had installed, so nothing in it is an escalation, and refusing a member there would
 * make the database unopenable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ImportedHostCodeTriggerNeedsSecurityAdminTest {
  private static final String DATABASE_PATH = "target/databases/test-imported-host-code-trigger";

  /**
   * The schema line a hostile - or merely over-privileged - export would carry.
   */
  private static JSONObject schemaDeclaring(final String actionType, final String actionCode) {
    return new JSONObject()
        .put("triggers", new JSONObject()
            .put("evil", new JSONObject()
                .put("name", "evil")
                .put("typeName", "Target")
                .put("timing", "AFTER")
                .put("event", "CREATE")
                .put("actionType", actionType)
                .put("actionCode", actionCode)));
  }

  @Test
  void aJavascriptTriggerInAnImportedSchemaIsRefusedWithoutSecurityAdmin() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));

    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH).setSecurity(db -> {
    });
    final Database db = factory.create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Target");

      final LocalSchema schema = db.getSchema().getEmbedded();

      // A principal that may change the schema - which is all running an import takes - and nothing more.
      DatabaseContext.INSTANCE.getContext(db.getDatabasePath())
          .setCurrentUser(userAllowing(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA));

      final int failures = schema.restoreSchemaMembersFromJSON(
          schemaDeclaring("JAVASCRIPT", "true;"), LocalSchema.SchemaMemberSource.IMPORTED_FILE);

      assertThat(failures).as("the refusal is reported to the caller, which counts it as a warning").isEqualTo(1);
      assertThat(schema.existsTrigger("evil"))
          .as("host code arriving in a file does not install itself on UPDATE_SCHEMA alone")
          .isFalse();
    } finally {
      DatabaseContext.INSTANCE.getContext(db.getDatabasePath()).setCurrentUser(null);
      db.drop();
      FileUtils.deleteRecursively(new File(DATABASE_PATH));
    }
  }

  /**
   * The other half, so the gate cannot be "imports stopped restoring triggers": a declarative SQL trigger is not
   * host code and keeps the standard schema-level protection, exactly as {@code createTrigger} treats it.
   */
  @Test
  void anSqlTriggerInAnImportedSchemaStillRestoresOnUpdateSchema() {
    final String databasePath = DATABASE_PATH + "-sql";
    FileUtils.deleteRecursively(new File(databasePath));

    final DatabaseFactory factory = new DatabaseFactory(databasePath).setSecurity(db -> {
    });
    final Database db = factory.create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Target");
      db.command("sql", "CREATE DOCUMENT TYPE AuditLog");

      final LocalSchema schema = db.getSchema().getEmbedded();

      DatabaseContext.INSTANCE.getContext(db.getDatabasePath())
          .setCurrentUser(userAllowing(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA));

      final int failures = schema.restoreSchemaMembersFromJSON(
          schemaDeclaring("SQL", "INSERT INTO AuditLog SET what = 'created'"),
          LocalSchema.SchemaMemberSource.IMPORTED_FILE);

      assertThat(failures).isZero();
      assertThat(schema.existsTrigger("evil")).isTrue();
    } finally {
      DatabaseContext.INSTANCE.getContext(db.getDatabasePath()).setCurrentUser(null);
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }

  /**
   * And the trusted path stays trusted: the database's own schema file installs what it already had, whatever the
   * principal that happens to be opening it can do.
   */
  @Test
  void theDatabasesOwnSchemaFileIsNotGated() {
    final String databasePath = DATABASE_PATH + "-file";
    FileUtils.deleteRecursively(new File(databasePath));

    final DatabaseFactory factory = new DatabaseFactory(databasePath).setSecurity(db -> {
    });
    final Database db = factory.create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Target");

      final LocalSchema schema = db.getSchema().getEmbedded();

      DatabaseContext.INSTANCE.getContext(db.getDatabasePath())
          .setCurrentUser(userAllowing(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA));

      assertThat(schema.restoreSchemaMembersFromJSON(
          schemaDeclaring("JAVASCRIPT", "true;"), LocalSchema.SchemaMemberSource.SCHEMA_FILE))
          .as("refusing here would make a database with such a trigger unopenable")
          .isZero();
      assertThat(schema.existsTrigger("evil")).isTrue();
    } finally {
      DatabaseContext.INSTANCE.getContext(db.getDatabasePath()).setCurrentUser(null);
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }

  /** A principal allowed exactly {@code allowed} on the database, and everything on every file. */
  private static SecurityDatabaseUser userAllowing(final SecurityDatabaseUser.DATABASE_ACCESS allowed) {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "schema-admin";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return access == allowed;
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
