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
 */
package com.arcadedb.console;

import com.arcadedb.remote.RemoteException;
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
    } catch (ConsoleException e) {
    }
  }

  @Test
  public void testConnectWrongPassword() throws IOException {
    try {
      Assertions.assertTrue(console.parse("connect " + URL_WRONGPASSWD + ";create document type VVVV", false));
      Assertions.fail("Security was bypassed!");
    } catch (RemoteException e) {
    }
  }

  @Test
  public void testCreateType() throws IOException {
    Assertions.assertTrue(console.parse("connect " + URL, false));
    Assertions.assertTrue(console.parse("create document type Person2", false));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(new ConsoleOutput() {
      @Override
      public void onOutput(final String output) {
        buffer.append(output);
      }
    });
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
    console.setOutput(new ConsoleOutput() {
      @Override
      public void onOutput(final String output) {
        buffer.append(output);
      }
    });
    Assertions.assertTrue(console.parse("select from Person2", false));
    Assertions.assertTrue(buffer.toString().contains("Jay"));
    Assertions.assertTrue(console.parse("drop type Person2", false));
  }
//
//  @Test
//  public void testInsertAndRollback() throws IOException {
//    Assertions.assertTrue(console.parse("connect " + URL));
//    Assertions.assertTrue(console.parse("begin"));
//    Assertions.assertTrue(console.parse("create document type Person"));
//    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'"));
//    Assertions.assertTrue(console.parse("rollback"));
//
//    final StringBuilder buffer = new StringBuilder();
//    console.setOutput(new ConsoleOutput() {
//      @Override
//      public void onOutput(final String output) {
//        buffer.append(output);
//      }
//    });
//    Assertions.assertTrue(console.parse("select from Person"));
//    Assertions.assertFalse(buffer.toString().contains("Jay"));
//  }

  @Test
  public void testHelp() throws IOException {
    final StringBuilder buffer = new StringBuilder();
    console.setOutput(new ConsoleOutput() {
      @Override
      public void onOutput(final String output) {
        buffer.append(output);
      }
    });
    Assertions.assertTrue(console.parse("?", false));
    Assertions.assertTrue(buffer.toString().contains("quit"));
  }

  @Override
  protected boolean isPopulateDatabase() {
    return false;
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
    } catch (IOException e) {
      Assertions.fail(e);
    }
  }

  @AfterEach
  public void endTest() {
    super.endTest();
    if (console != null)
      console.close();
  }
}
