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
import com.arcadedb.remote.RemoteException;
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
    Assertions.assertTrue(console.parse("connect " + URL, false));
  }

  @Test
  public void testConnectShortURL() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL_SHORT, false));
  }

  @Test
  public void testConnectNoCredentials() throws IOException {
    try {
      Assertions.assertTrue(console.parse("connect " + URL_NOCREDENTIALS + ";create document type VVVV", false));
      Assertions.fail("Security was bypassed!");
    } catch (final ConsoleException e) {
    }
  }

  @Test
  public void testConnectWrongPassword() throws IOException {
    try {
      Assertions.assertTrue(console.parse("connect " + URL_WRONGPASSWD + ";create document type VVVV", false));
      Assertions.fail("Security was bypassed!");
    } catch (final RemoteException e) {
    }
  }

  @Test
  public void testCreateType() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL, false));
    Assertions.assertTrue(console.parse("create document type Person2", false));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("info types", false));
    Assertions.assertTrue(buffer.toString().contains("Person2"));
    Assertions.assertTrue(console.parse("drop type Person2", false));
  }

  @Test
  public void testInsertAndSelectRecord() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL, false));
    Assertions.assertTrue(console.parse("create document type Person2", false));
    Assertions.assertTrue(console.parse("insert into Person2 set name = 'Jay', lastname='Miner'", false));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("select from Person2", false));
    Assertions.assertTrue(buffer.toString().contains("Jay"));
    Assertions.assertTrue(console.parse("drop type Person2", false));
  }

  @Test
  public void testInsertAndRollback() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL, false));
    Assertions.assertTrue(console.parse("begin", false));
    Assertions.assertTrue(console.parse("create document type Person", false));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'", false));
    Assertions.assertTrue(console.parse("rollback", false));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("select from Person", false));
    Assertions.assertFalse(buffer.toString().contains("Jay"));
  }

  @Test
  public void testInsertAndCommit() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL, false));
    Assertions.assertTrue(console.parse("begin", false));
    Assertions.assertTrue(console.parse("create document type Person", false));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'", false));
    Assertions.assertTrue(console.parse("commit", false));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("select from Person", false));
    Assertions.assertTrue(buffer.toString().contains("Jay"));
  }

  @Test
  public void testTransactionExpired() throws IOException, InterruptedException {
    Assertions.assertTrue(console.parse("connect " + URL, false));
    Assertions.assertTrue(console.parse("begin", false));
    Assertions.assertTrue(console.parse("create document type Person", false));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'", false));
    Thread.sleep(5000);
    try {
      Assertions.assertTrue(console.parse("commit", false));
      Assertions.fail();
    } catch (final Exception e) {
      // EXPECTED
    }

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("select from Person", false));
    Assertions.assertFalse(buffer.toString().contains("Jay"));
  }

  @Test
  public void testUserMgmt() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL, false));
    try {
      Assertions.assertTrue(console.parse("drop user elon", false));
    } catch (final Exception e) {
      // EXPECTED IF ALREADY EXISTENT
    }

    try {
      Assertions.assertTrue(console.parse("create user jay identified by m", false));
      Assertions.fail();
    } catch (final RuntimeException e) {
      // PASSWORD MUST BE AT LEAST 5 CHARS
    }

    try {
      String longPassword = "";
      for (int i = 0; i < 257; i++)
        longPassword += "P";

      Assertions.assertTrue(console.parse("create user jay identified by " + longPassword, false));
      Assertions.fail();
    } catch (final RuntimeException e) {
      // PASSWORD MUST BE MAX 256 CHARS LONG
    }

    Assertions.assertTrue(console.parse("create user elon identified by musk", false));
    Assertions.assertTrue(console.parse("drop user elon", false));

    // TEST SYNTAX ERROR
    try {
      Assertions.assertTrue(console.parse("create user elon identified by musk grand connect on db1", false));
      Assertions.fail();
    } catch (final Exception e) {
      // EXPECTED
    }

    Assertions.assertTrue(console.parse("create user elon identified by musk grant connect to db1", false));
    Assertions.assertTrue(console.parse("drop user elon", false));
  }

  @Test
  public void testHelp() throws IOException {
    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    Assertions.assertTrue(console.parse("?", false));
    Assertions.assertTrue(buffer.toString().contains("quit"));
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/726
   */
  @Test
  public void testProjectionOrder() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL, false));
    Assertions.assertTrue(console.parse("create document type Order", false));
    Assertions.assertTrue(console.parse(
        "insert into Order set processor = 'SIR1LRM-7.1', vstart = '20220319_002624.404379', vstop = '20220319_002826.525650', status = 'PENDING'", false));

    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      Assertions.assertTrue(console.parse("select processor, vstart, vstop, pstart, pstop, status, node from Order", false));

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
      console = new Console(false);
      console.parse("close", false);
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
