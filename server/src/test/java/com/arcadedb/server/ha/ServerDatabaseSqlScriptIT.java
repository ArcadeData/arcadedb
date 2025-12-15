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
package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import java.io.*;

import static org.assertj.core.api.Assertions.*;

public class ServerDatabaseSqlScriptIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 3;
  }

  public ServerDatabaseSqlScriptIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  void executeSqlScript() {
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServer(i).getDatabase(getDatabaseName());

      database.command("sql", "create vertex type Photos if not exists");
      database.command("sql", "create edge type Connected if not exists");

      database.transaction(() -> {
        final ResultSet result = database.command("sqlscript",
            """
            LET photo1 = CREATE vertex Photos SET id = "3778f235a52d", name = "beach.jpg", status = "";
            LET photo2 = CREATE vertex Photos SET id = "23kfkd23223", name = "luca.jpg", status = "";
            LET connected = Create edge Connected FROM $photo1 to $photo2 set type = "User_Photos";return $photo1;\
            """);
        assertThat(result.hasNext()).isTrue();
        final Result response = result.next();
        assertThat(response.<String>getProperty("name")).isEqualTo("beach.jpg");
      });
    }
  }
}
