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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9764: an imported restore runs inside a schema recording session, and a failure part way through must not
 * leave the members restored before it in memory only. There is no schema rollback, so the session closes normally -
 * saving, and under Raft HA shipping, the prefix that landed - and the failure is rethrown after it. Escaping the session
 * instead would fail it unpublished: the members would stay on this node with nothing on disk and nothing shipped.
 */
class Issue9764FailedImportRestoreKeepsPrefixTest {
  private static final String DATABASE_PATH = "target/databases/test-issue9764-failed-import-restore";

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void theMembersRestoredBeforeAMalformedSectionArePersisted() throws Exception {
    try (final Database db = new DatabaseFactory(DATABASE_PATH).create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Account");
      db.command("sql", "CREATE DOCUMENT TYPE AccountAudit");
      db.command("sql",
          "CREATE TRIGGER audit AFTER CREATE ON TYPE Account EXECUTE SQL \"INSERT INTO AccountAudit SET what = 'a'\"");

      final LocalSchema schema = db.getSchema().getEmbedded();
      final JSONObject triggers = schema.toJSON().getJSONObject("triggers");
      db.command("sql", "DROP TRIGGER audit");
      assertThat(schema.existsTrigger("audit")).isFalse();

      // Triggers are restored first; the views section that follows is not an object, which throws outside the
      // per-member try blocks
      final JSONObject imported = new JSONObject().put("triggers", triggers).put("materializedViews", "not-an-object");

      assertThatThrownBy(() -> schema.restoreSchemaMembersFromJSON(imported, LocalSchema.SchemaMemberSource.IMPORTED_FILE))
          .isInstanceOf(RuntimeException.class);

      assertThat(schema.existsTrigger("audit")).as("the trigger restored before the failure is installed").isTrue();

      final JSONObject onDisk = new JSONObject(
          Files.readString(new File(db.getDatabasePath(), "schema.json").toPath(), StandardCharsets.UTF_8));
      assertThat(onDisk.getJSONObject("triggers").has("audit"))
          .as("and saved, rather than left in memory only by a session that failed unpublished").isTrue();
    }
  }
}
