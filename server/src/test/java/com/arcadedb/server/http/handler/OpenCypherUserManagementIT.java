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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for Cypher user management commands (SHOW USERS, CREATE USER, DROP USER, ALTER USER).
 */
class OpenCypherUserManagementIT extends BaseGraphServerTest {

  @AfterEach
  public void cleanupTestUsers() {
    final Database database = getServerDatabase(0, getDatabaseName());
    if (database != null) {
      try {
        database.command("opencypher", "DROP USER testUser IF EXISTS");
      } catch (final Exception ignored) {
      }
      try {
        database.command("opencypher", "DROP USER anotherUser IF EXISTS");
      } catch (final Exception ignored) {
      }
    }
  }

  @Test
  void showUsersReturnsRootUser() {
    final Database database = getServerDatabase(0, getDatabaseName());
    final ResultSet resultSet = database.command("opencypher", "SHOW USERS");

    final List<Result> results = new ArrayList<>();
    while (resultSet.hasNext())
      results.add(resultSet.next());

    assertThat(results).isNotEmpty();

    boolean foundRoot = false;
    for (final Result result : results) {
      if ("root".equals(result.getProperty("user"))) {
        foundRoot = true;
        break;
      }
    }
    assertThat(foundRoot).isTrue();
  }

  @Test
  void showCurrentUserReturnsAuthenticatedUser() {
    final Database database = getServerDatabase(0, getDatabaseName());
    final ResultSet resultSet = database.command("opencypher", "SHOW CURRENT USER");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((String) result.getProperty("user")).isNotNull();
  }

  @Test
  void createUserAndVerify() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("opencypher", "CREATE USER testUser SET PASSWORD 'TestPass123!'");

    // Verify user appears in SHOW USERS
    final ResultSet resultSet = database.command("opencypher", "SHOW USERS");
    boolean found = false;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      if ("testUser".equals(result.getProperty("user"))) {
        found = true;
        break;
      }
    }
    assertThat(found).isTrue();
  }

  @Test
  void createUserIfNotExistsNoDuplicateError() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("opencypher", "CREATE USER testUser IF NOT EXISTS SET PASSWORD 'TestPass123!'");
    // Run again - should not throw
    database.command("opencypher", "CREATE USER testUser IF NOT EXISTS SET PASSWORD 'TestPass123!'");
  }

  @Test
  void createUserDuplicateThrows() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("opencypher", "CREATE USER testUser SET PASSWORD 'TestPass123!'");

    assertThatThrownBy(() -> database.command("opencypher", "CREATE USER testUser SET PASSWORD 'TestPass123!'"))
        .isInstanceOf(Exception.class);
  }

  @Test
  void dropUserAndVerify() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("opencypher", "CREATE USER testUser SET PASSWORD 'TestPass123!'");
    database.command("opencypher", "DROP USER testUser");

    // Verify user is removed
    final ResultSet resultSet = database.command("opencypher", "SHOW USERS");
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      assertThat(result.<String>getProperty("user")).isNotEqualTo("testUser");
    }
  }

  @Test
  void dropUserIfExistsNoError() {
    final Database database = getServerDatabase(0, getDatabaseName());
    // Should not throw even if user doesn't exist
    database.command("opencypher", "DROP USER nonExistentUser IF EXISTS");
  }

  @Test
  void dropNonExistentUserThrows() {
    final Database database = getServerDatabase(0, getDatabaseName());
    assertThatThrownBy(() -> database.command("opencypher", "DROP USER nonExistentUser"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("does not exist");
  }

  @Test
  void alterUserSetPassword() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("opencypher", "CREATE USER testUser SET PASSWORD 'TestPass123!'");
    database.command("opencypher", "ALTER USER testUser SET PASSWORD 'NewPass456!'");

    // Verify user can authenticate with new password
    assertThat(getServer(0).getSecurity().existsUser("testUser")).isTrue();
    // Verify the password was changed by authenticating
    getServer(0).getSecurity().authenticate("testUser", "NewPass456!", getDatabaseName());
  }

  @Test
  void alterNonExistentUserThrows() {
    final Database database = getServerDatabase(0, getDatabaseName());
    assertThatThrownBy(() -> database.command("opencypher", "ALTER USER nonExistentUser SET PASSWORD 'NewPass456!'"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("does not exist");
  }
}
