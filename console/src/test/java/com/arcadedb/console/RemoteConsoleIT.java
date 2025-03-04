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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class RemoteConsoleIT extends BaseGraphServerTest {
  private static final String URL               = "remote:localhost:2480/console root " + DEFAULT_PASSWORD_FOR_TESTS;
  private static final String URL_SHORT         = "remote:localhost/console root " + DEFAULT_PASSWORD_FOR_TESTS;
  private static final String URL_NOCREDENTIALS = "remote:localhost/console";
  private static final String URL_WRONGPASSWD   = "remote:localhost/console root wrong";
  private static final String URL_NEW_DB        = "remote:localhost/consoleNew root " + DEFAULT_PASSWORD_FOR_TESTS;

  private static Console console;

  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_HTTP_SESSION_EXPIRE_TIMEOUT.setValue(1);
  }

  @Test
  public void testCreateDatabase() throws IOException {
    assertThat(console.parse("create database " + URL_NEW_DB)).isTrue();
  }

  @Test
  public void testConnect() throws IOException {
    assertThat(console.parse("connect " + URL)).isTrue();
  }

  @Test
  public void testConnectShortURL() throws IOException {
    assertThat(console.parse("connect " + URL_SHORT)).isTrue();
  }

  @Test
  public void testConnectNoCredentials() throws IOException {
    try {
      assertThat(console.parse("connect " + URL_NOCREDENTIALS + ";create document type VVVV")).isTrue();
      fail("Security was bypassed!");
    } catch (final ConsoleException e) {
      // EXPECTED
    }
  }

  @Test
  public void testConnectWrongPassword() throws IOException {
    try {
      assertThat(console.parse("connect " + URL_WRONGPASSWD + ";create document type VVVV")).isTrue();
      fail("Security was bypassed!");
    } catch (final SecurityException e) {
      // EXPECTED
    }
  }

  @Test
  public void testCreateType() throws IOException {
    assertThat(console.parse("connect " + URL)).isTrue();
    assertThat(console.parse("create document type Person2")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    assertThat(console.parse("info types")).isTrue();
    assertThat(buffer.toString().contains("Person2")).isTrue();
    assertThat(console.parse("drop type Person2")).isTrue();
  }

  @Test
  public void testInsertAndSelectRecord() throws IOException {
    assertThat(console.parse("connect " + URL)).isTrue();
    assertThat(console.parse("create document type Person2")).isTrue();
    assertThat(console.parse("insert into Person2 set name = 'Jay', lastname='Miner'")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    assertThat(console.parse("select from Person2")).isTrue();
    assertThat(buffer.toString().contains("Jay")).isTrue();
    assertThat(console.parse("drop type Person2")).isTrue();
  }

  @Test
  public void testListDatabases() throws IOException {
    assertThat(console.parse("connect " + URL)).isTrue();
    assertThat(console.parse("list databases;")).isTrue();
  }

  @Test
  public void testInsertAndRollback() throws IOException {
    assertThat(console.parse("connect " + URL)).isTrue();
    assertThat(console.parse("begin")).isTrue();
    assertThat(console.parse("create document type Person")).isTrue();
    assertThat(console.parse("insert into Person set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.parse("rollback")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    assertThat(console.parse("select from Person")).isTrue();
    assertThat(buffer.toString().contains("Jay")).isFalse();
  }

  @Test
  public void testInsertAndCommit() throws IOException {
    assertThat(console.parse("connect " + URL)).isTrue();
    assertThat(console.parse("begin")).isTrue();
    assertThat(console.parse("create document type Person")).isTrue();
    assertThat(console.parse("insert into Person set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.parse("commit")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    assertThat(console.parse("select from Person")).isTrue();
    assertThat(buffer.toString().contains("Jay")).isTrue();
  }

  @Test
  public void testTransactionExpired() throws IOException, InterruptedException {
    assertThat(console.parse("connect " + URL)).isTrue();
    assertThat(console.parse("begin")).isTrue();
    assertThat(console.parse("create document type Person")).isTrue();
    assertThat(console.parse("insert into Person set name = 'Jay', lastname='Miner'")).isTrue();
    Thread.sleep(5000);
    try {
      assertThat(console.parse("commit")).isTrue();
      fail("");
    } catch (final Exception e) {
      // EXPECTED
    }

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    assertThat(console.parse("select from Person")).isTrue();
    assertThat(buffer.toString().contains("Jay")).isFalse();
  }

  @Test
  public void testUserMgmt() throws IOException {
    assertThat(console.parse("connect " + URL)).isTrue();
    try {
      assertThat(console.parse("drop user elon")).isTrue();
    } catch (final Exception e) {
      // EXPECTED IF ALREADY EXISTENT
    }

    try {
      assertThat(console.parse("create user jay identified by m")).isTrue();
      fail("");
    } catch (final RuntimeException e) {
      // PASSWORD MUST BE AT LEAST 5 CHARS
    }

    try {
      String longPassword = "";
      for (int i = 0; i < 257; i++)
        longPassword += "P";

      assertThat(console.parse("create user jay identified by " + longPassword)).isTrue();
      fail("");
    } catch (final RuntimeException e) {
      // PASSWORD MUST BE MAX 256 CHARS LONG
    }

    assertThat(console.parse("create user albert identified by einstein")).isTrue();
    assertThat(console.parse("drop user elon")).isTrue();

    // TEST SYNTAX ERROR
    try {
      assertThat(console.parse("create user albert identified by einstein grand connect on db1")).isTrue();
      fail("");
    } catch (final Exception e) {
      // EXPECTED
    }

    assertThat(console.parse("create user albert identified by einstein grant connect to db1")).isTrue();
    assertThat(console.parse("create user jeff identified by amazon grant connect to db1:readonly")).isTrue();
    assertThat(console.parse("drop user elon")).isTrue();
  }

  @Test
  public void testHelp() throws IOException {
    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);
    assertThat(console.parse("?")).isTrue();
    assertThat(buffer.toString().contains("quit")).isTrue();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/726
   */
  @Test
  public void testProjectionOrder() throws IOException {
    assertThat(console.parse("connect " + URL)).isTrue();
    assertThat(console.parse("create document type Order")).isTrue();
    assertThat(console.parse(
      "insert into Order set processor = 'SIR1LRM-7.1', vstart = '20220319_002624.404379', vstop = '20220319_002826.525650', status = 'PENDING'")).isTrue();

    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      assertThat(console.parse("select processor, vstart, vstop, pstart, pstop, status, node from Order")).isTrue();

      int pos = buffer.toString().indexOf("processor");
      assertThat(pos > -1).isTrue();
      pos = buffer.toString().indexOf("vstart", pos);
      assertThat(pos > -1).isTrue();
      pos = buffer.toString().indexOf("vstop", pos);
      assertThat(pos > -1).isTrue();
      pos = buffer.toString().indexOf("pstart", pos);
      assertThat(pos > -1).isTrue();
      pos = buffer.toString().indexOf("pstop", pos);
      assertThat(pos > -1).isTrue();
      pos = buffer.toString().indexOf("status", pos);
      assertThat(pos > -1).isTrue();
      pos = buffer.toString().indexOf("node", pos);
      assertThat(pos > -1).isTrue();
    }
  }

  @Test
  public void testCustomPropertyInSchema() throws IOException {
    assertThat(console.parse("connect " + URL)).isTrue();
    assertThat(console.parse("CREATE DOCUMENT TYPE doc;")).isTrue();
    assertThat(console.parse("ALTER TYPE doc CUSTOM testType = 444;")).isTrue();
    assertThat(console.parse("CREATE PROPERTY doc.prop STRING;")).isTrue();
    assertThat(console.parse("ALTER PROPERTY doc.prop CUSTOM test = true;")).isTrue();

    assertThat(console.getDatabase().getSchema().getType("doc").getCustomValue("testType")).isEqualTo(444);
    assertThat(console.getDatabase().getSchema().getType("doc").getProperty("prop").getCustomValue("test")).isEqualTo(true);

    console.getDatabase().getSchema().getType("doc").setCustomValue("testType", "555");
    assertThat(console.getDatabase().getSchema().getType("doc").getCustomValue("testType")).isEqualTo("555");

    assertThat(console.getDatabase().query("sql", "SELECT properties.custom.test[0].type() as type FROM schema:types").next()
      .<String>getProperty("type")).isEqualTo(Type.BOOLEAN.name().toUpperCase());

    assertThat(console.getDatabase().command("sql", "SELECT properties.custom.test[0].type() as type FROM schema:types").next()
      .<String>getProperty("type")).isEqualTo(Type.BOOLEAN.name().toUpperCase());
  }

  @Test
  public void testIfWithSchemaResult() throws IOException {
    assertThat(console.parse("connect " + URL)).isTrue();
    assertThat(console.parse("CREATE DOCUMENT TYPE doc;")).isTrue();
    assertThat(console.parse("CREATE PROPERTY doc.prop STRING;")).isTrue();

    assertThat(console.parse("INSERT INTO doc set name = 'doc'")).isTrue();

    ResultSet resultSet = console.getDatabase().command("sql",
        "SELECT name, (name = 'doc') as name2, if( (name = 'doc'), true, false) as name3, if( (name IN ['doc','XXX']), true, false) as name4, ifnull( (name = 'doc'), null) as name5 FROM schema:types");

    assertThat(resultSet.hasNext()).isTrue();

    Result result = resultSet.next();

    assertThat(result.<String>getProperty("name")).isEqualTo("doc");
    assertThat( result.<Boolean>getProperty("name2")).isTrue();
    assertThat( result.<Boolean>getProperty("name3")).isTrue();
    assertThat( result.<Boolean>getProperty("name4")).isTrue();
    assertThat( result.<Boolean>getProperty("name5")).isTrue();

    resultSet = console.getDatabase().command("sql", "SELECT name FROM schema:types WHERE 'a_b' = 'a b'.replace(' ','_')");

    assertThat(resultSet.hasNext()).isTrue();

    result = resultSet.next();

    assertThat(result.<String>getProperty("name")).isEqualTo("doc");
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
      fail("", e);
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
    GlobalConfiguration.SERVER_HTTP_SESSION_EXPIRE_TIMEOUT.setValue(
        GlobalConfiguration.SERVER_HTTP_SESSION_EXPIRE_TIMEOUT.getDefValue());
  }
}
