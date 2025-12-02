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
package com.arcadedb.console;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsoleBatchTest {
  @Test
  void batchMode() throws Exception {
    Console.execute(new String[] { "-b", "create database console; create vertex type ConsoleOnlyVertex;" });
    final Database db = new DatabaseFactory("./target/databases/console").open();
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    db.drop();
  }

  @Test
  void batchModeWithError() throws Exception {
    // This should fail
    assertThatThrownBy(() -> Console.execute(
        new String[] { "-b", """
          create database console;
          create vertex table WRONG_STATEMENT;
          create vertex type ConsoleOnlyVertex;
        """ }))
        .isInstanceOf(CommandSQLParsingException.class);

    final Database db = new DatabaseFactory("./target/databases/console").open();
    // the ConsoleOnlyVertex should not be created
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isFalse();
    db.drop();
  }

  @Test
  void batchModeWithFailAtEnd() throws Exception {
    // Error is only printed out
    Console.execute(
        new String[] { "-b", "-fae", """
          create database console;
          create vertex table WRONG_STATEMENT;
          create vertex type ConsoleOnlyVertex;
        """ });

    final Database db = new DatabaseFactory("./target/databases/console").open();
    // the ConsoleOnlyVertex is created
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    db.drop();
  }

  @Test
  void interactiveMode() throws Exception {
    Console.execute(new String[] { "create database console; create vertex type ConsoleOnlyVertex;exit" });
    final Database db = new DatabaseFactory("./target/databases/console").open();
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    db.drop();
  }

  @Test
  void swallowSettings() throws Exception {
    FileUtils.deleteRecursively(new File("./console"));
    Console.execute(new String[] { "-Darcadedb.server.databaseDirectory=.",
        "create database console; create vertex type ConsoleOnlyVertex;exit;" });
    final Database db = new DatabaseFactory("./console").open();
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    db.drop();
    GlobalConfiguration.resetAll();
  }

  @BeforeEach
  void cleanup() throws IOException {
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
