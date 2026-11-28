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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * A connection names the database in its URL. A name reserved for the server's own use, and a directory of the database directory
 * that holds no database, are not databases: the connection is refused as for any other missing database, and nothing is
 * opened, registered or written on disk, whatever the grants of the user.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresDatabaseNameResolutionIT extends PostgresWireProtocolTestBase {
  private static final String   USER          = "pgAnyDatabaseUser";
  private static final String   PASSWORD      = "pgAnyDatabasePassword1";
  private static final String[] NOT_DATABASES = { ".raft", ".raft-storage", "plainFolder" };

  @AfterEach
  @Override
  public void endTest() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(USER) != null)
      security.dropUser(USER);
    for (final String name : NOT_DATABASES)
      FileUtils.deleteRecursively(new File(databaseDirectory(), name));
    super.endTest();
  }

  @Test
  void aConnectionNamingADirectoryThatHoldsNoDatabaseIsRefusedAndLeavesItUntouched() {
    final ArcadeDBServer server = getServer(0);
    final ServerSecurity security = server.getSecurity();
    security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))));

    for (final String name : NOT_DATABASES) {
      final File directory = new File(databaseDirectory(), name);
      assertThat(directory.mkdirs()).as("created '%s'", name).isTrue();

      final SQLException refused = catchThrowableOfType(SQLException.class, () -> DriverManager.getConnection(
          "jdbc:postgresql://localhost:" + getServerPostgresPort() + "/" + name + "?sslmode=disable&preferQueryMode=simple", USER,
          PASSWORD));

      // SQLException is an Iterable of its chained causes, so a null check on it is stated on a boolean
      assertThat(refused != null).as("connection to '%s' refused", name).isTrue();
      assertThat(refused.getSQLState()).as("invalid_catalog_name for '%s'", name).isEqualTo("3D000");
      assertThat(server.existsDatabase(name)).as("nothing registered under '%s'", name).isFalse();
      assertThat(new File(directory, LocalSchema.SCHEMA_FILE_NAME)).as("no schema written into '%s'", name).doesNotExist();
      assertThat(directory.list()).as("nothing written into '%s'", name).isEmpty();
    }
  }

  private String databaseDirectory() {
    return getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
  }
}
