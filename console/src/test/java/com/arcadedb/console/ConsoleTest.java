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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.text.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class ConsoleTest {
  private static final String  DB_NAME = "console";
  private static       Console console;
  private static       String  absoluteDBPath;

  @BeforeEach
  public void populate() throws IOException {
    File dbFile = new File("./target/databases");
    absoluteDBPath = dbFile.getAbsolutePath().replace('\\', '/');
    FileUtils.deleteRecursively(dbFile);
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    console = new Console();
    assertThat(console.parse("create database " + DB_NAME + "; close")).isTrue();
  }

  @AfterEach
  public void drop() throws IOException {
    console.close();
    TestServerHelper.checkActiveDatabases();
    assertThat(console.parse("drop database " + DB_NAME + "; close", false)).isTrue();
    GlobalConfiguration.resetAll();
  }

  @Test
  public void testDropCreateWithLocalUrl() throws IOException {
    if (System.getProperty("os.name").toLowerCase().contains("windows"))
      return;

    String localUrl = "local:/" + absoluteDBPath + "/" + DB_NAME;
    assertThat(console.parse("drop database " + localUrl + "; close", false)).isTrue();
    assertThat(console.parse("create database " + localUrl + "; close", false)).isTrue();
  }

  @Test
  public void testNull() throws IOException {
    assertThat(console.parse(null)).isTrue();
  }

  @Test
  public void testEmpty() throws IOException {
    assertThat(console.parse("")).isTrue();
  }

  @Test
  public void testEmpty2() throws IOException {
    assertThat(console.parse(" ")).isTrue();
  }

  @Test
  public void testEmpty3() throws IOException {
    assertThat(console.parse(";")).isTrue();
  }

  @Test
  public void testComment() throws IOException {
    assertThat(console.parse("-- This is a comment;")).isTrue();
  }

  @Test
  public void testListDatabases() throws IOException {
    assertThat(console.parse("list databases;")).isTrue();
  }

  @Test
  public void testConnect() throws IOException {
    assertThat(console.parse("connect " + DB_NAME + ";info types")).isTrue();
  }

  @Test
  public void testLocalConnect() throws IOException {
    if (System.getProperty("os.name").toLowerCase().contains("windows"))
      return;
    assertThat(console.parse("connect local:/" + absoluteDBPath + "/" + DB_NAME + ";info types", false)).isTrue();
  }

  @Test
  public void testSetVerbose() throws IOException {
    try {
      console.parse("set verbose = 2; close; connect " + DB_NAME + "XX");
      fail("");
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
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create document type Person")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("info types")).isTrue();
    assertThat(buffer.toString().contains("Person")).isTrue();

    buffer.setLength(0);
    assertThat(console.parse("info type Person")).isTrue();
    assertThat(buffer.toString().contains("DOCUMENT TYPE 'Person'")).isTrue();
  }

  @Test
  public void testInsertAndSelectRecord() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create document type Person")).isTrue();
    assertThat(console.parse("insert into Person set name = 'Jay', lastname='Miner'")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("select from Person")).isTrue();
    assertThat(buffer.toString().contains("Jay")).isTrue();
  }

  @Test
  public void testInsertAndRollback() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("begin")).isTrue();
    assertThat(console.parse("create document type Person")).isTrue();
    assertThat(console.parse("insert into Person set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.parse("rollback")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("select from Person")).isTrue();
    assertThat(buffer.toString().contains("Jay")).isFalse();
  }

  @Test
  public void testHelp() throws IOException {
    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("?")).isTrue();
    assertThat(buffer.toString().contains("quit")).isTrue();
  }

  @Test
  public void testInfoError() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    try {
      assertThat(console.parse("info blablabla")).isTrue();
      fail("");
    } catch (final ConsoleException e) {
      // EXPECTED
    }
  }

  @Test
  public void testAllRecordTypes() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create document type D")).isTrue();
    assertThat(console.parse("create vertex type V")).isTrue();
    assertThat(console.parse("create edge type E")).isTrue();

    assertThat(console.parse("insert into D set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.parse("insert into V set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.parse("insert into V set name = 'Elon', lastname='Musk'")).isTrue();
    assertThat(console.parse("create edge E from (select from V where name ='Jay') to (select from V where name ='Elon')")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("select from D")).isTrue();
    assertThat(buffer.toString().contains("Jay")).isTrue();

    assertThat(console.parse("select from V")).isTrue();
    assertThat(console.parse("select from E")).isTrue();
    assertThat(buffer.toString().contains("Elon")).isTrue();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/691
   */
  @Test
  public void testNotStringProperties() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("CREATE VERTEX TYPE v")).isTrue();
    assertThat(console.parse("CREATE PROPERTY v.s STRING")).isTrue();
    assertThat(console.parse("CREATE PROPERTY v.i INTEGER")).isTrue();
    assertThat(console.parse("CREATE PROPERTY v.b BOOLEAN")).isTrue();
    assertThat(console.parse("CREATE PROPERTY v.sh SHORT")).isTrue();
    assertThat(console.parse("CREATE PROPERTY v.d DOUBLE")).isTrue();
    assertThat(console.parse("CREATE PROPERTY v.da DATETIME")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("CREATE VERTEX v SET s=\"abc\", i=1, b=true, sh=2, d=3.5, da=\"2022-12-20 18:00\"")).isTrue();
    assertThat(buffer.toString().contains("true")).isTrue();
  }

  @Test
  public void testUserMgmtLocalError() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    try {
      assertThat(console.parse("create user elon identified by musk")).isTrue();
      fail("local connection allowed user creation");
    } catch (final Exception e) {
      // EXPECTED
    }

    try {
      assertThat(console.parse("drop user jack")).isTrue();
      fail("local connection allowed user deletion");
    } catch (final Exception e) {
      // EXPECTED
    }
  }

  @Test
  public void testImportNeo4jConsoleOK() throws IOException {
    final String DATABASE_PATH = "testNeo4j";

    FileUtils.deleteRecursively(new File("databases/" + DATABASE_PATH));

    final Console newConsole = new Console();
    newConsole.parse("create database " + DATABASE_PATH + ";import database file://src/test/resources/neo4j-export-mini.jsonl");
    newConsole.close();

    try (final DatabaseFactory factory = new DatabaseFactory("./target/databases/" + DATABASE_PATH)) {
      try (final Database database = factory.open()) {
        final DocumentType personType = database.getSchema().getType("User");
        assertThat(personType).isNotNull();
        assertThat(database.countType("User", true)).isEqualTo(3);

        final IndexCursor cursor = database.lookupByKey("User", "id", "0");
        assertThat(cursor.hasNext()).isTrue();
        final Vertex v = cursor.next().asVertex();
        assertThat(v.get("name")).isEqualTo("Adam");
        assertThat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(v.getLong("born"))).isEqualTo("2015-07-04T19:32:24");

        final Map<String, Object> place = (Map<String, Object>) v.get("place");
        assertThat(((Number) place.get("latitude")).doubleValue()).isEqualTo(33.46789);
        assertThat(place.get("height")).isNull();

        assertThat(v.get("kids")).isEqualTo(Arrays.asList("Sam", "Anna", "Grace"));

        final DocumentType friendType = database.getSchema().getType("KNOWS");
        assertThat(friendType).isNotNull();
        assertThat(database.countType("KNOWS", true)).isEqualTo(1);

        final Iterator<Edge> relationships = v.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
        assertThat(relationships.hasNext()).isTrue();
        final Edge e = relationships.next();

        assertThat(e.get("since")).isEqualTo(1993);
        assertThat(e.get("bffSince")).isEqualTo("P5M1DT12H");
      }
    }
  }

  @Test
  public void testImportCSVConsoleOK() throws IOException {
    final String DATABASE_PATH = "testCSV";

    FileUtils.deleteRecursively(new File("databases/" + DATABASE_PATH));

    final Console newConsole = new Console();
    newConsole.parse("create database " + DATABASE_PATH + "");
    newConsole.parse("set arcadedb.asyncWorkerThreads = 1");
    newConsole.parse("import database with "//
        + "vertices = `file://src/test/resources/nodes.csv`,"//
        + "verticesHeader = 'id',"//
        + "verticesSkipEntries = 0,"//
        + "vertexType = 'Page',"//
        + "typeIdProperty = 'id',"//
        + "typeIdPropertyIsUnique = true,"//
        + "typeIdType = 'long',"//
        + "edges = `file://src/test/resources/edges.csv`,"//
        + "edgesHeader = 'from,to',"//
        + "edgesSkipEntries = 0,"//
        + "edgeType = 'Links',"//
        + "edgeFromField = 'from'," //
        + "edgeToField = 'to'" //
    );
    newConsole.close();

    int vertices = 0;
    long edges = 0;

    try (final DatabaseFactory factory = new DatabaseFactory("./target/databases/" + DATABASE_PATH)) {
      try (final Database database = factory.open()) {
        for (Iterator<Record> it = database.iterateType("Page", true); it.hasNext(); ) {
          final Vertex rec = it.next().asVertex();
          ++vertices;
          edges += rec.countEdges(Vertex.DIRECTION.OUT, "Links");
        }
      }
    }

    assertThat(vertices).isEqualTo(101);
    assertThat(edges).isEqualTo(135);
  }

  @Test
  public void testNullValues() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create document type Person")).isTrue();
    assertThat(console.parse("insert into Person set name = 'Jay', lastname='Miner', nothing = null")).isTrue();
    assertThat(console.parse("insert into Person set name = 'Thom', lastname='Yorke', nothing = 'something'")).isTrue();

    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      assertThat(console.parse("select from Person where nothing is null")).isTrue();
      assertThat(buffer.toString().contains("<null>")).isTrue();
    }
    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      assertThat(console.parse("select nothing, lastname, name from Person where nothing is null")).isTrue();
      assertThat(buffer.toString().contains("<null>")).isTrue();
    }
    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      assertThat(console.parse("select nothing, lastname, name from Person")).isTrue();
      assertThat(buffer.toString().contains("<null>")).isTrue();
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/726
   */
  @Test
  public void testProjectionOrder() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
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
  public void testAsyncMode() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create document type D")).isTrue();
    assertThat(console.parse("create vertex type V")).isTrue();
    assertThat(console.parse("create edge type E")).isTrue();

    assertThat(console.parse("insert into D set name = 'Jay', lastname='Miner'")).isTrue();

    int asyncOperations = (int) ((DatabaseAsyncExecutorImpl) ((DatabaseInternal) console.getDatabase()).async()).getStats().scheduledTasks;
    assertThat(asyncOperations).isEqualTo(0);

    assertThat(console.parse("set asyncMode = true")).isTrue();

    assertThat(console.parse("insert into V set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.parse("insert into V set name = 'Elon', lastname='Musk'")).isTrue();

    assertThat(console.parse("set asyncMode = false")).isTrue();

    asyncOperations = (int) ((DatabaseAsyncExecutorImpl) ((DatabaseInternal) console.getDatabase()).async()).getStats().scheduledTasks;
    assertThat(asyncOperations).isEqualTo(2);
  }

  @Test
  public void testBatchMode() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create document type D")).isTrue();
    assertThat(console.parse("create vertex type V")).isTrue();
    assertThat(console.parse("create edge type E")).isTrue();

    assertThat(console.parse("set transactionBatchSize = 2")).isTrue();

    assertThat(console.parse("insert into D set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.currentOperationsInBatch).isEqualTo(1);

    assertThat(((DatabaseInternal) console.getDatabase()).getTransaction().isActive()).isTrue();
    assertThat(((DatabaseInternal) console.getDatabase()).getTransaction().getModifiedPages() > 0).isTrue();

    assertThat(console.parse("insert into V set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.currentOperationsInBatch).isEqualTo(2);
    assertThat(console.parse("insert into V set name = 'Elon', lastname='Musk'")).isTrue();
    assertThat(console.currentOperationsInBatch).isEqualTo(1);

    assertThat(console.parse("set transactionBatchSize = 0")).isTrue();
  }

  @Test
  public void testLoad() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("load " + new File("src/test/resources/console-batch.sql").toString().replace('\\', '/'))).isTrue();

    final String[] urls = new String[] { "http://arcadedb.com", "https://www.arcadedb.com", "file://this/is/myfile.txt" };

    // VALIDATE WITH PLAIN JAVA REGEXP FIRST
    for (String url : urls)
      assertThat(url.matches("^([a-zA-Z]{1,15}:)(\\/\\/)?[^\\s\\/$.?#].[^\\s]*$")).as("Cannot validate URL: " + url).isTrue();

    // VALIDATE WITH DATABASE SCHEMA
    for (String url : urls)
      console.getDatabase().newDocument("doc").set("uri1", url).validate();
  }

  @Test
  public void testCustomPropertyInSchema() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("CREATE DOCUMENT TYPE doc;")).isTrue();
    assertThat(console.parse("CREATE PROPERTY doc.prop STRING;")).isTrue();
    assertThat(console.parse("ALTER PROPERTY doc.prop CUSTOM test = true;")).isTrue();
    assertThat(console.getDatabase().getSchema().getType("doc").getProperty("prop").getCustomValue("test")).isEqualTo(true);

    assertThat(console.getDatabase().query("sql", "SELECT properties.custom.test[0].type() as type FROM schema:types").next()
      .<String>getProperty("type")).isEqualTo(Type.BOOLEAN.name().toUpperCase());

    assertThat(console.getDatabase().command("sql", "SELECT properties.custom.test[0].type() as type FROM schema:types").next()
      .<String>getProperty("type")).isEqualTo(Type.BOOLEAN.name().toUpperCase());
  }

  /**
   * Test case for https://github.com/ArcadeData/arcadedb/issues/885
   */
  @Test
  public void testNotNullProperties() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("CREATE DOCUMENT TYPE doc;")).isTrue();
    assertThat(console.parse("CREATE PROPERTY doc.prop STRING (notnull);")).isTrue();
    assertThat(console.getDatabase().getSchema().getType("doc").getProperty("prop").isNotNull()).isTrue();

    assertThat(console.parse("INSERT INTO doc set a = null;")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("INSERT INTO doc set prop = null;")).isTrue();

    int pos = buffer.toString().indexOf("ValidationException");
    assertThat(pos > -1).isTrue();

    assertThat(console.getDatabase().query("sql", "SELECT FROM doc").nextIfAvailable().<String>getProperty("prop")).isNull();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/958
   */
  @Test
  public void testPercentWildcardInQuery() throws IOException {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create document type Person")).isTrue();
    assertThat(console.parse("insert into Person set name = 'Jay', lastname='Miner', nothing = null")).isTrue();
    assertThat(console.parse("insert into Person set name = 'Thom', lastname='Yorke', nothing = 'something'")).isTrue();

    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      assertThat(console.parse("select from Person where name like 'Thom%'")).isTrue();
      assertThat(buffer.toString().contains("Yorke")).isTrue();
    }

    {
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      assertThat(console.parse("select from Person where not ( name like 'Thom%' )")).isTrue();
      assertThat(buffer.toString().contains("Miner")).isTrue();
    }
  }
}
