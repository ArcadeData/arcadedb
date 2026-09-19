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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A materialized view whose refresh resources cannot be installed must not stay registered as if they had been.
 * <p>
 * {@code restoreSchemaMembersFromJSON} takes the previous view's listeners and schedule down before storing the
 * replacement, so a failure in between - {@code MaterializedViewBuilder.registerListeners} raises on the first
 * source type the target does not have, after the earlier ones are already registered - used to leave the name
 * mapped to a view that nothing refreshes. That reads as a working view and is not one, and on the merge path it
 * had also just silenced a view that WAS working.
 * <p>
 * Found by CodeRabbit in review of PR #7943.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FailedMaterializedViewRestoreIsUndoneTest {
  private static final String DATABASE_PATH = "target/databases/test-failed-mv-restore";

  private static JSONObject schemaDeclaringView(final String viewName, final String backingType,
      final String... sourceTypes) {
    final JSONArray sources = new JSONArray();
    for (final String sourceType : sourceTypes)
      sources.put(sourceType);

    return new JSONObject()
        .put("materializedViews", new JSONObject()
            .put(viewName, new JSONObject()
                .put("name", viewName)
                .put("query", "SELECT FROM " + sourceTypes[0])
                .put("backingType", backingType)
                .put("refreshMode", MaterializedViewRefreshMode.INCREMENTAL.name())
                .put("simpleQuery", true)
                .put("refreshInterval", 0)
                .put("lastRefreshTime", 0)
                .put("status", MaterializedViewStatus.VALID.name())
                .put("sourceTypes", sources)));
  }

  /**
   * The merge path: the target's own view stays registered AND refreshed when the import's replacement cannot be
   * installed.
   */
  @Test
  void aReplacementThatCannotInstallLeavesThePreviousViewRefreshed() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));

    final Database db = new DatabaseFactory(DATABASE_PATH).create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Account");
      db.command("sql", "CREATE PROPERTY Account.name STRING");
      db.command("sql", "CREATE MATERIALIZED VIEW MyView AS SELECT name FROM Account REFRESH INCREMENTAL");

      final LocalSchema schema = db.getSchema().getEmbedded();
      final MaterializedView installed = schema.getMaterializedView("MyView");
      assertThat(((MaterializedViewImpl) installed).getChangeListener())
          .as("the target's own view is refreshed to begin with")
          .isNotNull();

      // The import carries a view of the same name whose sources name a type this database does not have.
      final int failures = schema.restoreSchemaMembersFromJSON(
          schemaDeclaringView("MyView", "MyView", "Account", "NoSuchType"),
          LocalSchema.SchemaMemberSource.IMPORTED_FILE);

      assertThat(failures).as("the failure is reported to the caller").isEqualTo(1);

      final MaterializedViewImpl current = (MaterializedViewImpl) schema.getMaterializedView("MyView");
      assertThat(current)
          .as("the view the replacement displaced is put back, rather than the half-installed one being kept")
          .isSameAs(installed);
      assertThat(current.getChangeListener())
          .as("and it is refreshed again: its listeners were taken down to make room for a replacement that never "
              + "arrived")
          .isNotNull();
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(DATABASE_PATH));
    }
  }

  /**
   * And with nothing to put back, the name is left free rather than mapped to a view nothing refreshes.
   */
  @Test
  void aReplacementThatCannotInstallAndReplacesNothingLeavesNoView() {
    final String databasePath = DATABASE_PATH + "-fresh";
    FileUtils.deleteRecursively(new File(databasePath));

    final Database db = new DatabaseFactory(databasePath).create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Account");
      db.command("sql", "CREATE DOCUMENT TYPE Backing");

      final LocalSchema schema = db.getSchema().getEmbedded();

      assertThat(schema.restoreSchemaMembersFromJSON(
          schemaDeclaringView("MyView", "Backing", "Account", "NoSuchType"),
          LocalSchema.SchemaMemberSource.IMPORTED_FILE))
          .isEqualTo(1);

      assertThat(schema.existsMaterializedView("MyView"))
          .as("a view that could not be installed does not occupy the name")
          .isFalse();
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }
}
