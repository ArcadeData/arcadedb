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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A trigger that is gone from {@code schema.json} must be gone from a schema that re-reads it.
 * <p>
 * {@code readConfiguration()} runs on a LIVE, already-populated schema and not only at open: an HA follower
 * refreshes its logical schema through it on every replicated schema change (see
 * {@code LocalSchema#registerNewComponentsAndRefreshSchema}). Materialized views, continuous aggregates, function
 * libraries and extensions were all dropped and repopulated there; triggers were not, so a {@code DROP TRIGGER} on
 * the leader left the trigger registered on every follower - and a trigger's listener adapter lives on its type's
 * event registry, so it went on FIRING while no longer appearing in the schema.
 * <p>
 * Forgetting the map entry is not enough for the same reason {@code dropTrigger()} unregisters before it removes:
 * found in review of PR #7943, alongside issue #7886's extraction of the restore into one method, which is what
 * made the four-out-of-five inconsistency visible.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaRefreshDropsRemovedTriggerTest {
  private static final String DATABASE_PATH = "target/databases/test-schema-refresh-drops-trigger";

  @Test
  void aTriggerRemovedFromTheSchemaFileStopsFiringOnRefresh() throws Exception {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));

    final Database db = new DatabaseFactory(DATABASE_PATH).create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Target");
      db.command("sql", "CREATE DOCUMENT TYPE AuditLog");
      db.command("sql", "CREATE PROPERTY AuditLog.what STRING");
      db.command("sql",
          "CREATE TRIGGER audit AFTER CREATE ON TYPE Target EXECUTE SQL \"INSERT INTO AuditLog SET what = 'created'\"");

      db.transaction(() -> db.command("sql", "INSERT INTO Target SET name = 'first'"));
      assertThat(db.countType("AuditLog", false)).as("the trigger is live to begin with").isEqualTo(1);

      final LocalSchema schema = db.getSchema().getEmbedded();
      schema.saveConfiguration();

      // What a follower is handed after a DROP TRIGGER on the leader: the same schema, with no triggers in it.
      final File schemaFile = new File(DATABASE_PATH, LocalSchema.SCHEMA_FILE_NAME);
      final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      root.put("triggers", new JSONObject());
      Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);

      schema.readConfiguration();

      assertThat(schema.existsTrigger("audit"))
          .as("the refreshed schema no longer carries the trigger")
          .isFalse();

      db.transaction(() -> db.command("sql", "INSERT INTO Target SET name = 'second'"));
      assertThat(db.countType("AuditLog", false))
          .as("and it no longer fires: forgetting the map entry without unregistering the listener would have left "
              + "it firing while invisible")
          .isEqualTo(1);
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(DATABASE_PATH));
    }
  }

  /**
   * The other half: a refresh that still carries the trigger keeps exactly one registration of it, rather than
   * stacking a second listener on the type each time the schema is re-read.
   */
  @Test
  void aTriggerStillInTheSchemaFileFiresExactlyOnceAfterARefresh() throws Exception {
    final String databasePath = DATABASE_PATH + "-kept";
    FileUtils.deleteRecursively(new File(databasePath));

    final Database db = new DatabaseFactory(databasePath).create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Target");
      db.command("sql", "CREATE DOCUMENT TYPE AuditLog");
      db.command("sql", "CREATE PROPERTY AuditLog.what STRING");
      db.command("sql",
          "CREATE TRIGGER audit AFTER CREATE ON TYPE Target EXECUTE SQL \"INSERT INTO AuditLog SET what = 'created'\"");

      final LocalSchema schema = db.getSchema().getEmbedded();
      schema.saveConfiguration();
      schema.readConfiguration();

      assertThat(schema.existsTrigger("audit")).isTrue();

      db.transaction(() -> db.command("sql", "INSERT INTO Target SET name = 'first'"));
      assertThat(db.countType("AuditLog", false))
          .as("one row per insert, not one per time the schema has been read")
          .isEqualTo(1);
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }
}
