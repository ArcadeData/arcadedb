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

  @BeforeEach
  public void populate() throws IOException {
    FileUtils.deleteRecursively(new File("./target/databases"));
    console = new Console(false).setRootPath("./target");
    Assertions.assertTrue(console.parse("create database " + DB_NAME, false));
  }

  @AfterEach
  public void drop() {
    console.close();
    TestServerHelper.checkActiveDatabases();
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
  public void testSetLanguage() throws IOException {
    console.parse("set language = sql; select 1", false);
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

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/691
   */
  @Test
  public void testNotStringProperties() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME, false));
    Assertions.assertTrue(console.parse("CREATE VERTEX TYPE v", false));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.s STRING", false));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.i INTEGER", false));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.b BOOLEAN", false));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.sh SHORT", false));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.d DOUBLE", false));
    Assertions.assertTrue(console.parse("CREATE PROPERTY v.da DATETIME", false));


    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    Assertions.assertTrue(console.parse("CREATE VERTEX v SET s=\"abc\", i=1, b=true, sh=2, d=3.5, da=\"2022-12-20 18:00\"", false));
    Assertions.assertTrue(buffer.toString().contains("true"));
  }

  @Test
  public void testUserMgmtLocalError() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_NAME, false));
    try {
      Assertions.assertTrue(console.parse("create user elon identified by musk", false));
      Assertions.fail("local connection allowed user creation");
    } catch (Exception e) {
      // EXPECTED
    }

    try {
      Assertions.assertTrue(console.parse("drop user jack", false));
      Assertions.fail("local connection allowed user deletion");
    } catch (Exception e) {
      // EXPECTED
    }
  }

  @Test
  public void testImportNeo4jConsoleOK() throws IOException {
    final String DATABASE_PATH = "testNeo4j";

    FileUtils.deleteRecursively(new File("databases/" + DATABASE_PATH));

    Console.main(new String[] { "create database " + DATABASE_PATH + ";import database file://src/test/resources/neo4j-export-mini.jsonl" });

    try (final DatabaseFactory factory = new DatabaseFactory("databases/" + DATABASE_PATH)) {
      try (Database database = factory.open()) {
        DocumentType personType = database.getSchema().getType("User");
        Assertions.assertNotNull(personType);
        Assertions.assertEquals(3, database.countType("User", true));

        IndexCursor cursor = database.lookupByKey("User", "id", "0");
        Assertions.assertTrue(cursor.hasNext());
        Vertex v = cursor.next().asVertex();
        Assertions.assertEquals("Adam", v.get("name"));
        Assertions.assertEquals("2015-07-04T19:32:24", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(v.getLong("born")));

        Map<String, Object> place = (Map<String, Object>) v.get("place");
        Assertions.assertEquals(33.46789, ((Number) place.get("latitude")).doubleValue());
        Assertions.assertNull(place.get("height"));

        Assertions.assertEquals(Arrays.asList("Sam", "Anna", "Grace"), v.get("kids"));

        DocumentType friendType = database.getSchema().getType("KNOWS");
        Assertions.assertNotNull(friendType);
        Assertions.assertEquals(1, database.countType("KNOWS", true));

        Iterator<Edge> relationships = v.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
        Assertions.assertTrue(relationships.hasNext());
        Edge e = relationships.next();

        Assertions.assertEquals(1993, e.get("since"));
        Assertions.assertEquals("P5M1DT12H", e.get("bffSince"));
      }
    }
  }
}
