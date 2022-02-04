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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;

public class ConsoleTest {
  private static final String  DB_NAME = "console";
  private static       Console console;

  @BeforeEach
  public void populate() throws IOException {
    FileUtils.deleteRecursively(new File("./target/databases"));
    console = new Console(false).setRootPath("./target");
    Assertions.assertTrue(console.parse("create database " + DB_NAME, false));
  }

  @AfterEach
  public void drop() {
    console.close();
    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
    FileUtils.deleteRecursively(new File("target/databases"));
  }

  @Test
  public void testConnect() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME + ";info types", false));
  }

  @Test
  public void testSetVerbose() throws IOException {
    try {
      console.parse("set verbose = 2; close; connect " + DB_NAME + "XX", false);
      Assertions.fail();
    } catch (DatabaseOperationException e) {
      // EXPECTED
    }
  }

  @Test
  public void testCreateClass() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME, false));
    Assertions.assertTrue(console.parse("create document type Person", false));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("info types", false));
    Assertions.assertTrue(buffer.toString().contains("Person"));
  }

  @Test
  public void testInsertAndSelectRecord() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME, false));
    Assertions.assertTrue(console.parse("create document type Person", false));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'", false));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("select from Person", false));
    Assertions.assertTrue(buffer.toString().contains("Jay"));
  }

  @Test
  public void testInsertAndRollback() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME, false));
    Assertions.assertTrue(console.parse("begin", false));
    Assertions.assertTrue(console.parse("create document type Person", false));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'", false));
    Assertions.assertTrue(console.parse("rollback", false));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("select from Person", false));
    Assertions.assertFalse(buffer.toString().contains("Jay"));
  }

  @Test
  public void testHelp() throws IOException {
    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("?", false));
    Assertions.assertTrue(buffer.toString().contains("quit"));
  }

  @Test
  public void testInfoError() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME, false));
    try {
      Assertions.assertTrue(console.parse("info blablabla", false));
      Assertions.fail();
    } catch (ConsoleException e) {
      // EXPECTED
    }
  }

  @Test
  public void testAllRecordTypes() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME, false));
    Assertions.assertTrue(console.parse("create document type D", false));
    Assertions.assertTrue(console.parse("create vertex type V", false));
    Assertions.assertTrue(console.parse("create edge type E", false));

    Assertions.assertTrue(console.parse("insert into D set name = 'Jay', lastname='Miner'", false));
    Assertions.assertTrue(console.parse("insert into V set name = 'Jay', lastname='Miner'", false));
    Assertions.assertTrue(console.parse("insert into V set name = 'Elon', lastname='Musk'", false));
    Assertions.assertTrue(console.parse("create edge E from (select from V where name ='Jay') to (select from V where name ='Elon')", false));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("select from D", false));
    Assertions.assertTrue(buffer.toString().contains("Jay"));

    Assertions.assertTrue(console.parse("select from V", false));
    Assertions.assertTrue(console.parse("select from E", false));
    Assertions.assertTrue(buffer.toString().contains("Elon"));
  }

}
