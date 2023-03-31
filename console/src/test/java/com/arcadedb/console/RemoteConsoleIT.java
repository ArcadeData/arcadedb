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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;

public class RemoteConsoleIT extends BaseGraphServerTest {
  private static final String  URL               = "remote:localhost:2480/console root " + DEFAULT_PASSWORD_FOR_TESTS;
  private static final String  URL_SHORT         = "remote:localhost/console root " + DEFAULT_PASSWORD_FOR_TESTS;
  private static final String  URL_NOCREDENTIALS = "remote:localhost/console";
  private static final String  URL_WRONGPASSWD   = "remote:localhost/console root wrong";
  private static       Console console;

  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_HTTP_TX_EXPIRE_TIMEOUT.setValue(1);
  }

  @Test
  public void testConnect() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL));
  }

  @Test
  public void testConnectShortURL() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL_SHORT));
  }

  @Test
  public void testConnectNoCredentials() throws IOException {
    try {
      Assertions.assertTrue(console.parse("connect " + URL_NOCREDENTIALS + ";create document type VVVV"));
      Assertions.fail("Security was bypassed!");
    } catch (final ConsoleException e) {
      // EXPECTED
    }
  }

  @Test
  public void testConnectWrongPassword() throws IOException {
    try {
      Assertions.assertTrue(console.parse("connect " + URL_WRONGPASSWD + ";create document type VVVV"));
      Assertions.fail("Security was bypassed!");
    } catch (final SecurityException e) {
      // EXPECTED
    }
  }

  @Test
  public void testCreateType() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL));
    Assertions.assertTrue(console.parse("create document type Person2"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("info types"));
    Assertions.assertTrue(buffer.toString().contains("Person2"));
    Assertions.assertTrue(console.parse("drop type Person2"));
  }

  @Test
  public void testInsertAndSelectRecord() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL));
    Assertions.assertTrue(console.parse("create document type Person2"));
    Assertions.assertTrue(console.parse("insert into Person2 set name = 'Jay', lastname='Miner'"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("select from Person2"));
    Assertions.assertTrue(buffer.toString().contains("Jay"));
    Assertions.assertTrue(console.parse("drop type Person2"));
  }

  @Test
  public void testListDatabases() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL));
    Assertions.assertTrue(console.parse("list databases;"));
  }

  @Test
  public void testInsertAndRollback() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL));
    Assertions.assertTrue(console.parse("begin"));
    Assertions.assertTrue(console.parse("create document type Person"));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'"));
    Assertions.assertTrue(console.parse("rollback"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("select from Person"));
    Assertions.assertFalse(buffer.toString().contains("Jay"));
  }

  @Test
  public void testInsertAndCommit() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL));
    Assertions.assertTrue(console.parse("begin"));
    Assertions.assertTrue(console.parse("create document type Person"));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'"));
    Assertions.assertTrue(console.parse("commit"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("select from Person"));
    Assertions.assertTrue(buffer.toString().contains("Jay"));
  }

  @Test
  public void testTransactionExpired() throws IOException, InterruptedException {
    Assertions.assertTrue(console.parse("connect " + URL));
    Assertions.assertTrue(console.parse("begin"));
    Assertions.assertTrue(console.parse("create document type Person"));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'"));
    Thread.sleep(5000);
    try {
      Assertions.assertTrue(console.parse("commit"));
      Assertions.fail();
    } catch (final Exception e) {
      // EXPECTED
    }

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("select from Person"));
    Assertions.assertFalse(buffer.toString().contains("Jay"));
  }

  @Test
  public void testUserMgmt() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL));
    try {
      Assertions.assertTrue(console.parse("drop user elon"));
    } catch (final Exception e) {
      // EXPECTED IF ALREADY EXISTENT
    }

    try {
      Assertions.assertTrue(console.parse("create user jay identified by m"));
      Assertions.fail();
    } catch (final RuntimeException e) {
      // PASSWORD MUST BE AT LEAST 5 CHARS
    }

    try {
      String longPassword = "";
      for (int i = 0; i < 257; i++)
        longPassword += "P";

      Assertions.assertTrue(console.parse("create user jay identified by " + longPassword));
      Assertions.fail();
    } catch (final RuntimeException e) {
      // PASSWORD MUST BE MAX 256 CHARS LONG
    }

    Assertions.assertTrue(console.parse("create user elon identified by musk"));
    Assertions.assertTrue(console.parse("drop user elon"));

    // TEST SYNTAX ERROR
    try {
      Assertions.assertTrue(console.parse("create user elon identified by musk grand connect on db1"));
      Assertions.fail();
    } catch (final Exception e) {
      // EXPECTED
    }

    Assertions.assertTrue(console.parse("create user elon identified by musk grant connect to db1"));
    Assertions.assertTrue(console.parse("drop user elon"));
  }

  @Test
  public void testHelp() throws IOException {
    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("?"));
    Assertions.assertTrue(buffer.toString().contains("quit"));
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/726
   */
  @Test
  public void testProjectionOrder() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL));
    Assertions.assertTrue(console.parse("create document type Order"));
    Assertions.assertTrue(console.parse(
        "insert into Order set processor = 'SIR1LRM-7.1', vstart = '20220319_002624.404379', vstop = '20220319_002826.525650', status = 'PENDING'"));

    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      Assertions.assertTrue(console.parse("select processor, vstart, vstop, pstart, pstop, status, node from Order"));

      int pos = buffer.toString().indexOf("processor");
      Assertions.assertTrue(pos > -1);
      pos = buffer.toString().indexOf("vstart", pos);
      Assertions.assertTrue(pos > -1);
      pos = buffer.toString().indexOf("vstop", pos);
      Assertions.assertTrue(pos > -1);
      pos = buffer.toString().indexOf("pstart", pos);
      Assertions.assertTrue(pos > -1);
      pos = buffer.toString().indexOf("pstop", pos);
      Assertions.assertTrue(pos > -1);
      pos = buffer.toString().indexOf("status", pos);
      Assertions.assertTrue(pos > -1);
      pos = buffer.toString().indexOf("node", pos);
      Assertions.assertTrue(pos > -1);
    }
  }

  @Test
  public void testCustomPropertyInSchema() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL));
    Assertions.assertTrue(console.parse("CREATE DOCUMENT TYPE doc;"));
    Assertions.assertTrue(console.parse("CREATE PROPERTY doc.prop STRING;"));
    Assertions.assertTrue(console.parse("ALTER PROPERTY doc.prop CUSTOM test = true;"));

    Assertions.assertEquals(Type.BOOLEAN.name().toUpperCase(),
        console.getDatabase().query("sql", "SELECT properties.custom.test[0].type() as type FROM schema:types").next().getProperty("type"));

    Assertions.assertEquals(Type.BOOLEAN.name().toUpperCase(),
        console.getDatabase().command("sql", "SELECT properties.custom.test[0].type() as type FROM schema:types").next().getProperty("type"));
  }

  @Test
  public void testIfWithSchemaResult() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL));
    Assertions.assertTrue(console.parse("CREATE DOCUMENT TYPE doc;"));
    Assertions.assertTrue(console.parse("CREATE PROPERTY doc.prop STRING;"));

    Assertions.assertTrue(console.parse("INSERT INTO doc set name = 'doc'"));

    ResultSet resultSet = console.getDatabase().command("sql",
        "SELECT name, (name = 'doc') as name2, if( (name = 'doc'), true, false) as name3, if( (name IN ['doc','XXX']), true, false) as name4, ifnull( (name = 'doc'), null) as name5 FROM schema:types");

    Assertions.assertTrue(resultSet.hasNext());

    Result result = resultSet.next();

    Assertions.assertEquals("doc", result.getProperty("name"));
    Assertions.assertTrue((boolean) result.getProperty("name2"));
    Assertions.assertTrue((boolean) result.getProperty("name3"));
    Assertions.assertTrue((boolean) result.getProperty("name4"));
    Assertions.assertTrue((boolean) result.getProperty("name5"));

    resultSet = console.getDatabase().command("sql", "SELECT name FROM schema:types WHERE 'a_b' = 'a b'.replace(' ','_')");

    Assertions.assertTrue(resultSet.hasNext());

    result = resultSet.next();

    Assertions.assertEquals("doc", result.getProperty("name"));
  }

  @Override
  protected void populateDatabase() {
    // EXECUTE BEGIN-END TESTS
  }

  @Override
  protected String getDatabaseName() {
    return "console";
  }

  @BeforeEach
  public void beginTest() {
    deleteDatabaseFolders();

    super.beginTest();

    try {
      console = new Console();
      console.parse("close");
    } catch (final IOException e) {
      Assertions.fail(e);
    }
  }

  @AfterEach
  public void endTest() {
    super.endTest();
    if (console != null)
      console.close();
  }

  @AfterAll
  public static void afterAll() {
    GlobalConfiguration.SERVER_HTTP_TX_EXPIRE_TIMEOUT.setValue(GlobalConfiguration.SERVER_HTTP_TX_EXPIRE_TIMEOUT.getDefValue());
  }
}
