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
package com.arcadedb.postgres;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * An open Postgres connection must stop working when its user is deleted, loses the database grant or has its password
 * rotated: it used to keep the access it had at login time while HTTP refused the same credentials.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresStaleSessionRevalidationIT extends PostgresWireProtocolTestBase {
  private static final String USER     = "pgStaleUser";
  private static final String PASSWORD = "pgStalePassword1";

  @AfterEach
  @Override
  public void endTest() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(USER) != null)
      security.dropUser(USER);
    super.endTest();
  }

  @Test
  void deletedUserIsCutOff() throws Exception {
    run(security -> security.dropUser(USER));
  }

  @Test
  void revokedDatabaseGrantIsCutOff() throws Exception {
    // the stored hash is kept, so only the grant changes: a re-salted hash would read as a password rotation
    run(security -> security.updateUser(new JSONObject().put("name", USER).put("password", security.getUser(USER).getPassword())
        .put("databases", new JSONObject())));
  }

  @Test
  void rotatedPasswordIsCutOff() throws Exception {
    run(security -> security.updateUser(
        new JSONObject().put("name", USER).put("password", security.encodePassword("another-Password-2"))
            .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")))));
  }

  private void run(final Consumer<ServerSecurity> change) throws Exception {
    final ServerSecurity security = getServer(0).getSecurity();
    security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin"))));
    getServerDatabase(0, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE StaleDoc IF NOT EXISTS");

    try (final Connection connection = DriverManager.getConnection(getServerPostgresJdbcUrl() + "?sslmode=disable&preferQueryMode=simple",
        USER, PASSWORD); final Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO StaleDoc (name) VALUES ('before')");

      change.accept(security);

      final Throwable thrown = catchThrowable(() -> statement.execute("INSERT INTO StaleDoc (name) VALUES ('after')"));
      assertThat(thrown).as("a connection whose user was revoked must be refused").isNotNull();
      assertThat(getServerDatabase(0, getDatabaseName()).countType("StaleDoc", false)).isEqualTo(1L);
    }
  }
}
