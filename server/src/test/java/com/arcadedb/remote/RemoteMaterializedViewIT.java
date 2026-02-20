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
package com.arcadedb.remote;

import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.MaterializedView;
import com.arcadedb.schema.MaterializedViewRefreshMode;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteMaterializedViewIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-matview-database";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void createAndQueryViaSql() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      // Create source type and insert data
      database.command("sql", "CREATE DOCUMENT TYPE Account");
      database.command("sql", "CREATE PROPERTY Account.name STRING");
      database.command("sql", "CREATE PROPERTY Account.active BOOLEAN");
      database.command("sql", "INSERT INTO Account SET name = 'Alice', active = true");
      database.command("sql", "INSERT INTO Account SET name = 'Bob', active = false");
      database.command("sql", "INSERT INTO Account SET name = 'Carol', active = true");

      // Create materialized view via SQL
      final ResultSet createResult = database.command("sql",
          "CREATE MATERIALIZED VIEW ActiveAccounts AS SELECT name FROM Account WHERE active = true");
      assertThat(createResult.hasNext()).isTrue();

      // Verify via direct SQL query first
      final ResultSet schemaResult = database.command("sql", "SELECT FROM schema:materializedViews");
      assertThat(schemaResult.hasNext()).isTrue();

      // Verify existsMaterializedView
      assertThat(database.getSchema().existsMaterializedView("ActiveAccounts")).isTrue();
      assertThat(database.getSchema().existsMaterializedView("NonExistent")).isFalse();

      // Verify getMaterializedView
      final MaterializedView view = database.getSchema().getMaterializedView("ActiveAccounts");
      assertThat(view).isNotNull();
      assertThat(view.getName()).isEqualTo("ActiveAccounts");
      assertThat(view.getQuery()).contains("Account");
      assertThat(view.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.MANUAL);
      assertThat(view.getStatus()).isEqualTo("VALID");

      // Verify getMaterializedView for non-existent throws
      assertThatThrownBy(() -> database.getSchema().getMaterializedView("NonExistent"))
          .isInstanceOf(SchemaException.class);

      // Verify getMaterializedViews
      final MaterializedView[] views = database.getSchema().getMaterializedViews();
      assertThat(views).hasSize(1);
      assertThat(views[0].getName()).isEqualTo("ActiveAccounts");

      // Verify the backing type exists via schema:types
      final ResultSet typesResult = database.command("sql",
          "SELECT FROM schema:types WHERE name = 'ActiveAccounts'");
      assertThat(typesResult.hasNext()).as("Backing type should exist in schema:types").isTrue();

      // Verify toJSON
      assertThat(view.toJSON()).isNotNull();
      assertThat(view.toJSON().getString("name")).isEqualTo("ActiveAccounts");

      // Drop the materialized view
      database.getSchema().dropMaterializedView("ActiveAccounts");

      // Verify it's gone
      assertThat(database.getSchema().existsMaterializedView("ActiveAccounts")).isFalse();
      assertThat(database.getSchema().getMaterializedViews()).hasSize(0);

      // Clean up
      database.command("sql", "DROP TYPE Account");
    });
  }

  @Test
  void buildMaterializedViewThrowsUnsupported() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      assertThatThrownBy(() -> database.getSchema().buildMaterializedView())
          .isInstanceOf(UnsupportedOperationException.class);
    });
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
