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
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.text.*;
import java.util.*;

public class ConsoleTest {
  private static final String  DB_NAME = "console";
  private static       Console console;
  private static       String  absoluteDBPath;

  @BeforeEach
  public void populate() throws IOException {
    File dbFile = new File("./target/databases");
    absoluteDBPath = dbFile.getAbsolutePath();
    FileUtils.deleteRecursively(dbFile);
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    console = new Console();
    Assertions.assertTrue(console.parse("create database " + DB_NAME + "; close"));
  }

  @AfterEach
  public void drop() throws IOException {
    console.close();
    TestServerHelper.checkActiveDatabases();
    Assertions.assertTrue(console.parse("drop database " + DB_NAME + "; close", false));
  }

  @Test
  public void testDropCreateWithLocalUrl() throws IOException {
    String localUrl = "local:/" + absoluteDBPath + "/" + DB_NAME;
    Assertions.assertTrue(console.parse("drop database " + localUrl + "; close", false));
    Assertions.assertTrue(console.parse("create database " + localUrl + "; close", false));
  }

  @Test
  public void testNull() throws IOException {
    Assertions.assertTrue(console.parse(null));
  }

  @Test
  public void testEmpty() throws IOException {
    Assertions.assertTrue(console.parse(""));
  }

  @Test
  public void testEmpty2() throws IOException {
    Assertions.assertTrue(console.parse(" "));
  }

  @Test
  public void testEmpty3() throws IOException {
    Assertions.assertTrue(console.parse(";"));
  }

  @Test
  public void testComment() throws IOException {
    Assertions.assertTrue(console.parse("-- This is a comment;"));
  }

  @Test
  public void testListDatabases() throws IOException {
    Assertions.assertTrue(console.parse("list databases;"));
  }

  @Test
  public void testConnect() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME + ";info types"));
  }

  @Test
  public void testLocalConnect() throws IOException {
    Assertions.assertTrue(console.parse("connect local:/" + absoluteDBPath + "/" + DB_NAME + ";info types", false));
  }

  @Test
  public void testSetVerbose() throws IOException {
    try {
      console.parse("set verbose = 2; close; connect " + DB_NAME + "XX");
      Assertions.fail();
    } catch (final DatabaseOperationException e) {
      // EXPECTED
    }
  }

  @Test
  public void testSetLanguage() throws IOException {
    console.parse("connect " + DB_NAME + ";set language = sql; select 1");
  }

  @Test
  public void testCreateClass() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME));
    Assertions.assertTrue(console.parse("create document type Person"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("info types"));
    Assertions.assertTrue(buffer.toString().contains("Person"));
  }

  @Test
  public void testInsertAndSelectRecord() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME));
    Assertions.assertTrue(console.parse("create document type Person"));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("select from Person"));
    Assertions.assertTrue(buffer.toString().contains("Jay"));
  }

  @Test
  public void testInsertAndRollback() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME));
    Assertions.assertTrue(console.parse("begin"));
    Assertions.assertTrue(console.parse("create document type Person"));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'"));
    Assertions.assertTrue(console.parse("rollback"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("select from Person"));
    Assertions.assertFalse(buffer.toString().contains("Jay"));
  }

  @Test
  public void testHelp() throws IOException {
    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("?"));
    Assertions.assertTrue(buffer.toString().contains("quit"));
  }

  @Test
  public void testInfoError() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME));
    try {
      Assertions.assertTrue(console.parse("info blablabla"));
      Assertions.fail();
    } catch (final ConsoleException e) {
      // EXPECTED
    }
  }

  @Test
  public void testAllRecordTypes() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME));
    Assertions.assertTrue(console.parse("create document type D"));
    Assertions.assertTrue(console.parse("create vertex type V"));
    Assertions.assertTrue(console.parse("create edge type E"));

    Assertions.assertTrue(console.parse("insert into D set name = 'Jay', lastname='Miner'"));
    Assertions.assertTrue(console.parse("insert into V set name = 'Jay', lastname='Miner'"));
    Assertions.assertTrue(console.parse("insert into V set name = 'Elon', lastname='Musk'"));
    Assertions.assertTrue(console.parse("create edge E from (select from V where name ='Jay') to (select from V where name ='Elon')"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("select from D"));
    Assertions.assertTrue(buffer.toString().contains("Jay"));

    Assertions.assertTrue(console.parse("select from V"));
    Assertions.assertTrue(console.parse("select from E"));
    Assertions.assertTrue(buffer.toString().contains("Elon"));
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/691
   */
  @Test
  public void testNotStringProperties() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME));
    Assertions.assertTrue(console.parse("CREATE VERTEX TYPE v"));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.s STRING"));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.i INTEGER"));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.b BOOLEAN"));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.sh SHORT"));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.d DOUBLE"));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.da DATETIME"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("CREATE VERTEX v SET s=\"abc\", i=1, b=true, sh=2, d=3.5, da=\"2022-12-20 18:00\""));
    Assertions.assertTrue(buffer.toString().contains("true"));
  }

  @Test
  public void testUserMgmtLocalError() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME));
    try {
      Assertions.assertTrue(console.parse("create user elon identified by musk"));
      Assertions.fail("local connection allowed user creation");
    } catch (final Exception e) {
      // EXPECTED
    }

    try {
      Assertions.assertTrue(console.parse("drop user jack"));
      Assertions.fail("local connection allowed user deletion");
    } catch (final Exception e) {
      // EXPECTED
    }
  }

  @Test
  public void testImportNeo4jConsoleOK() throws IOException {
    final String DATABASE_PATH = "testNeo4j";

    FileUtils.deleteRecursively(new File("databases/" + DATABASE_PATH));

    new Console().parse("create database " + DATABASE_PATH + ";import database file://src/test/resources/neo4j-export-mini.jsonl");

    try (final DatabaseFactory factory = new DatabaseFactory("./target/databases/" + DATABASE_PATH)) {
      try (final Database database = factory.open()) {
        final DocumentType personType = database.getSchema().getType("User");
        Assertions.assertNotNull(personType);
        Assertions.assertEquals(3, database.countType("User", true));

        final IndexCursor cursor = database.lookupByKey("User", "id", "0");
        Assertions.assertTrue(cursor.hasNext());
        final Vertex v = cursor.next().asVertex();
        Assertions.assertEquals("Adam", v.get("name"));
        Assertions.assertEquals("2015-07-04T19:32:24", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(v.getLong("born")));

        final Map<String, Object> place = (Map<String, Object>) v.get("place");
        Assertions.assertEquals(33.46789, ((Number) place.get("latitude")).doubleValue());
        Assertions.assertNull(place.get("height"));

        Assertions.assertEquals(Arrays.asList("Sam", "Anna", "Grace"), v.get("kids"));

        final DocumentType friendType = database.getSchema().getType("KNOWS");
        Assertions.assertNotNull(friendType);
        Assertions.assertEquals(1, database.countType("KNOWS", true));

        final Iterator<Edge> relationships = v.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
        Assertions.assertTrue(relationships.hasNext());
        final Edge e = relationships.next();

        Assertions.assertEquals(1993, e.get("since"));
        Assertions.assertEquals("P5M1DT12H", e.get("bffSince"));
      }
    }
  }

  @Test
  public void testNullValues() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME));
    Assertions.assertTrue(console.parse("create document type Person"));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner', nothing = null"));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Thom', lastname='Yorke', nothing = 'something'"));

    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      Assertions.assertTrue(console.parse("select from Person where nothing is null"));
      Assertions.assertTrue(buffer.toString().contains("<null>"));
    }
    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      Assertions.assertTrue(console.parse("select nothing, lastname, name from Person where nothing is null"));
      Assertions.assertTrue(buffer.toString().contains("<null>"));
    }
    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      Assertions.assertTrue(console.parse("select nothing, lastname, name from Person"));
      Assertions.assertTrue(buffer.toString().contains("<null>"));
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/726
   */
  @Test
  public void testProjectionOrder() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME));
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
}
