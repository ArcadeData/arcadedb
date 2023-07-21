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
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;

public class ConsoleBatchTest {
  @Test
  public void batchMode() throws IOException {
    Console.execute(new String[] { "-b", "create database console; create vertex type ConsoleOnlyVertex;" });
    final Database db = new DatabaseFactory("./target/databases/console").open();
    Assertions.assertTrue(db.getSchema().existsType("ConsoleOnlyVertex"));
    db.drop();
  }

  @Test
  public void interactiveMode() throws IOException {
    Console.execute(new String[] { "create database console; create vertex type ConsoleOnlyVertex;exit" });
    final Database db = new DatabaseFactory("./target/databases/console").open();
    Assertions.assertTrue(db.getSchema().existsType("ConsoleOnlyVertex"));
    db.drop();
  }

  @Test
  public void swallowSettings() throws IOException {
    FileUtils.deleteRecursively(new File("./console"));
    Console.execute(new String[] { "-Darcadedb.server.databaseDirectory=.", "create database console; create vertex type ConsoleOnlyVertex;exit;" });
    final Database db = new DatabaseFactory("./console").open();
    Assertions.assertTrue(db.getSchema().existsType("ConsoleOnlyVertex"));
    db.drop();
    GlobalConfiguration.resetAll();
  }

  @BeforeEach
  public void cleanup() throws IOException {
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  public void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
