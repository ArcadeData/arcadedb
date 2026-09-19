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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The merge half of issue #7886's restore: a JSONL import goes into a database that may already have schema
 * members of its own, so {@code restoreSchemaMembersFromJSON} is called with {@code replaceExisting = false} and
 * must not drop what the target declared. What it must still do is REPLACE a member the export names by a name the
 * target has already used - the schema holds one trigger per name either way.
 * <p>
 * Found in review of PR #7943. Putting the new trigger into the map was all that used to happen: the previous
 * trigger's listener adapter stayed registered on ITS type's event registry with nothing pointing at it any more,
 * so it kept firing on every matching record and {@code DROP TRIGGER} could no longer reach it -
 * {@code triggerAdapters} had been overwritten with the newer one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7886MergedSchemaMemberCollisionIT {
  private static final String SOURCE_PATH = "target/databases/issue7886-merge-source";
  private static final String TARGET_PATH = "target/databases/issue7886-merge-target";
  private static final String FILE        = "target/issue7886-merge.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File(FILE).delete();
  }

  @Test
  void aTriggerTheExportReplacesStopsFiringOnTheTypeItUsedToWatch() throws Exception {
    // The export: one trigger named "audit", watching Account.
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Account");
      source.command("sql", "CREATE PROPERTY Account.name STRING");
      source.command("sql", "CREATE DOCUMENT TYPE AccountAudit");
      source.command("sql", "CREATE PROPERTY AccountAudit.what STRING");
      source.command("sql",
          "CREATE TRIGGER audit AFTER CREATE ON TYPE Account EXECUTE SQL \"INSERT INTO AccountAudit SET what = 'account'\"");
    }

    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    // The target: a trigger of the SAME NAME, watching a type the export knows nothing about. Kept OPEN across
    // the import, through the embedding Importer constructor, because that is where the bug lives: the orphaned
    // listener rides on the LIVE schema instance the import mutated. Reopening the database afterwards would hide
    // it - a fresh instance rebuilds its triggers from schema.json, which by then names only the export's.
    final Database target = new DatabaseFactory(TARGET_PATH).create();
    try {
      target.command("sql", "CREATE DOCUMENT TYPE Other");
      target.command("sql", "CREATE PROPERTY Other.name STRING");
      target.command("sql", "CREATE DOCUMENT TYPE OtherAudit");
      target.command("sql", "CREATE PROPERTY OtherAudit.what STRING");
      target.command("sql",
          "CREATE TRIGGER audit AFTER CREATE ON TYPE Other EXECUTE SQL \"INSERT INTO OtherAudit SET what = 'other'\"");

      target.transaction(() -> target.command("sql", "INSERT INTO Other SET name = 'before'"));
      assertThat(target.countType("OtherAudit", false)).as("the target's own trigger is live to begin with").isEqualTo(1);

      new Importer(target, "file://" + new File(FILE).getAbsolutePath()).load();

      assertThat(target.getSchema().getEmbedded().getTrigger("audit").getTypeName())
          .as("one trigger per name: the export's definition is the one that stands")
          .isEqualTo("Account");

      target.transaction(() -> target.command("sql", "INSERT INTO Other SET name = 'after'"));
      assertThat(target.countType("OtherAudit", false))
          .as("the replaced trigger no longer fires on the type it used to watch - its listener came off that "
              + "type's event registry instead of being orphaned on it")
          .isEqualTo(1);

      target.transaction(() -> target.command("sql", "INSERT INTO Account SET name = 'after'"));
      assertThat(target.countType("AccountAudit", false))
          .as("and the trigger the export brought fires exactly once, on the type it names")
          .isEqualTo(1);
    } finally {
      target.drop();
    }
  }
}
