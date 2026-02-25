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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaterializedViewSQLTest extends TestHelper {

  @BeforeEach
  void setupTypes() {
    if (!database.getSchema().existsType("Account"))
      database.transaction(() -> {
        database.getSchema().createDocumentType("Account");
        database.newDocument("Account").set("name", "Alice").set("active", true).save();
        database.newDocument("Account").set("name", "Bob").set("active", false).save();
      });
  }

  @Test
  void createViaSql() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW ActiveAccounts AS SELECT name FROM Account WHERE active = true");

    assertThat(database.getSchema().existsMaterializedView("ActiveAccounts")).isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT FROM ActiveAccounts")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }

    // Cleanup
    database.command("sql", "DROP MATERIALIZED VIEW ActiveAccounts");
  }

  @Test
  void createWithRefreshMode() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW ActiveAccountsInc AS SELECT name FROM Account WHERE active = true REFRESH INCREMENTAL");

    final MaterializedView view = database.getSchema().getMaterializedView("ActiveAccountsInc");
    assertThat(view.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.INCREMENTAL);

    database.command("sql", "DROP MATERIALIZED VIEW ActiveAccountsInc");
  }

  @Test
  void createWithBuckets() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW AccountView AS SELECT name FROM Account BUCKETS 4");

    assertThat(database.getSchema().existsMaterializedView("AccountView")).isTrue();

    database.command("sql", "DROP MATERIALIZED VIEW AccountView");
  }

  @Test
  void createIfNotExists() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW TestView AS SELECT FROM Account");
    // Should not throw
    database.command("sql",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS TestView AS SELECT FROM Account");

    database.command("sql", "DROP MATERIALIZED VIEW TestView");
  }

  @Test
  void refreshViaSql() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW RefreshView AS SELECT name FROM Account");

    // Add more data
    database.transaction(() ->
        database.newDocument("Account").set("name", "Charlie").set("active", true).save());

    database.command("sql", "REFRESH MATERIALIZED VIEW RefreshView");

    try (final ResultSet rs = database.query("sql", "SELECT FROM RefreshView")) {
      assertThat(rs.stream().count()).isEqualTo(3); // Alice, Bob, Charlie
    }

    database.command("sql", "DROP MATERIALIZED VIEW RefreshView");
  }

  @Test
  void dropViaSql() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW DropMe AS SELECT FROM Account");
    assertThat(database.getSchema().existsMaterializedView("DropMe")).isTrue();

    database.command("sql", "DROP MATERIALIZED VIEW DropMe");
    assertThat(database.getSchema().existsMaterializedView("DropMe")).isFalse();
  }

  @Test
  void dropIfExists() {
    // Should not throw
    database.command("sql", "DROP MATERIALIZED VIEW IF EXISTS NonExistent");
  }

  @Test
  void querySchemaMetadata() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW MetaView AS SELECT name FROM Account WHERE active = true");

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:materializedViews")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((String) result.getProperty("name")).isEqualTo("MetaView");
      assertThat((String) result.getProperty("query")).isNotNull();
      assertThat((String) result.getProperty("backingType")).isNotNull();
      assertThat((String) result.getProperty("refreshMode")).isEqualTo("MANUAL");
      assertThat((String) result.getProperty("status")).isEqualTo("VALID");
    }

    try (final ResultSet rs = database.query("sql",
        "SELECT FROM schema:materializedViews WHERE name = 'MetaView'")) {
      assertThat(rs.hasNext()).isTrue();
    }

    database.command("sql", "DROP MATERIALIZED VIEW MetaView");
  }

  @Test
  void alterToIncremental() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW AlterIncView AS SELECT name FROM Account");

    assertThat(database.getSchema().getMaterializedView("AlterIncView").getRefreshMode())
        .isEqualTo(MaterializedViewRefreshMode.MANUAL);

    database.command("sql", "ALTER MATERIALIZED VIEW AlterIncView REFRESH INCREMENTAL");

    assertThat(database.getSchema().getMaterializedView("AlterIncView").getRefreshMode())
        .isEqualTo(MaterializedViewRefreshMode.INCREMENTAL);

    // Insert new data — incremental listener should trigger post-commit refresh
    database.transaction(() ->
        database.newDocument("Account").set("name", "Dave").set("active", true).save());

    try (final ResultSet rs = database.query("sql", "SELECT FROM AlterIncView")) {
      assertThat(rs.stream().count()).isEqualTo(3); // Alice, Bob, Dave
    }

    database.command("sql", "DROP MATERIALIZED VIEW AlterIncView");
  }

  @Test
  void alterFromIncrementalToManual() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW AlterManView AS SELECT name FROM Account REFRESH INCREMENTAL");

    assertThat(database.getSchema().getMaterializedView("AlterManView").getRefreshMode())
        .isEqualTo(MaterializedViewRefreshMode.INCREMENTAL);

    database.command("sql", "ALTER MATERIALIZED VIEW AlterManView REFRESH MANUAL");

    assertThat(database.getSchema().getMaterializedView("AlterManView").getRefreshMode())
        .isEqualTo(MaterializedViewRefreshMode.MANUAL);

    // Insert — view should NOT auto-refresh (listener unregistered)
    database.transaction(() ->
        database.newDocument("Account").set("name", "Eve").set("active", true).save());

    // View should still have only the original 2 records
    try (final ResultSet rs = database.query("sql", "SELECT FROM AlterManView")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }

    database.command("sql", "DROP MATERIALIZED VIEW AlterManView");
  }

  @Test
  void alterToPeriodic() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW AlterPerView AS SELECT name FROM Account");

    database.command("sql", "ALTER MATERIALIZED VIEW AlterPerView REFRESH EVERY 10 MINUTE");

    final MaterializedView view = database.getSchema().getMaterializedView("AlterPerView");
    assertThat(view.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.PERIODIC);
    assertThat(view.getRefreshInterval()).isEqualTo(600_000L);

    database.command("sql", "DROP MATERIALIZED VIEW AlterPerView");
  }

  @Test
  void alterFromPeriodicToManual() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW AlterPMView AS SELECT name FROM Account REFRESH EVERY 1 SECOND");

    assertThat(database.getSchema().getMaterializedView("AlterPMView").getRefreshMode())
        .isEqualTo(MaterializedViewRefreshMode.PERIODIC);

    database.command("sql", "ALTER MATERIALIZED VIEW AlterPMView REFRESH MANUAL");

    assertThat(database.getSchema().getMaterializedView("AlterPMView").getRefreshMode())
        .isEqualTo(MaterializedViewRefreshMode.MANUAL);

    database.command("sql", "DROP MATERIALIZED VIEW AlterPMView");
  }

  @Test
  void alterNonExistentViewThrows() {
    assertThatThrownBy(() ->
        database.command("sql", "ALTER MATERIALIZED VIEW NonExistent REFRESH MANUAL"))
        .isInstanceOf(Exception.class);
  }

  @Test
  void alterSurvivesReopen() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW AlterReopenView AS SELECT name FROM Account");

    database.command("sql", "ALTER MATERIALIZED VIEW AlterReopenView REFRESH INCREMENTAL");

    // Reopen database
    database.close();
    database = factory.open();

    final MaterializedView view = database.getSchema().getMaterializedView("AlterReopenView");
    assertThat(view.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.INCREMENTAL);

    database.command("sql", "DROP MATERIALIZED VIEW AlterReopenView");
  }
}
