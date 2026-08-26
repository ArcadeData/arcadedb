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
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsoleTest {
  private static final String  DB_NAME = "console";
  private static       Console console;
  private static       String  absoluteDBPath;

  @BeforeEach
  void populate() throws IOException {
    File dbFile = new File("./target/databases");
    absoluteDBPath = dbFile.getAbsolutePath().replace('\\', '/');
    FileUtils.deleteRecursively(dbFile);
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    console = new Console();
    assertThat(console.parse("create database " + DB_NAME + "; close")).isTrue();
  }

  @AfterEach
  void drop() throws IOException {
    console.close();
    TestServerHelper.checkActiveDatabases();
    assertThat(console.parse("drop database " + DB_NAME + "; close", false)).isTrue();
    GlobalConfiguration.resetAll();
  }

  @Test
  @DisabledOnOs({OS.WINDOWS})
  void dropCreateWithLocalUrl() throws Exception {
    String localUrl = "local:/" + absoluteDBPath + "/" + DB_NAME;
    assertThat(console.parse("drop database " + localUrl + "; close", false)).isTrue();
    assertThat(console.parse("create database " + localUrl + "; close", false)).isTrue();
  }

  @Test
  void testNull() throws Exception {
    assertThat(console.parse(null)).isTrue();
  }

  @Test
  void empty() throws Exception {
    assertThat(console.parse("")).isTrue();
  }

  @Test
  void empty2() throws Exception {
    assertThat(console.parse(" ")).isTrue();
  }

  @Test
  void empty3() throws Exception {
    assertThat(console.parse(";")).isTrue();
  }

  @Test
  void comment() throws Exception {
    assertThat(console.parse("-- This is a comment;")).isTrue();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5457: a semicolon inside a comment must not terminate the command.
   */
  @Test
  void commentWithSemicolon() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));

    assertThat(console.parse("select 11 as value  -- A comment with a semicolon ; errors here")).isTrue();
    assertThat(buffer.toString()).contains("11").doesNotContain("ERROR");

    buffer.setLength(0);
    assertThat(console.parse("select 22 as value /* a block ; comment */; select 33 as value")).isTrue();
    assertThat(buffer.toString()).contains("22").contains("33").doesNotContain("ERROR");

    buffer.setLength(0);
    assertThat(console.parse("-- a full line comment ; with a semicolon")).isTrue();
    assertThat(buffer.toString()).doesNotContain("ERROR");

    buffer.setLength(0);
    assertThat(console.parse("select 'a ; b' as value")).isTrue();
    assertThat(buffer.toString()).contains("a ; b").doesNotContain("ERROR");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5457: with Cypher the line comment is `//`, while `--` is an undirected
   * relationship in a pattern.
   */
  @Test
  void cypherCommentWithSemicolon() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create vertex type Person")).isTrue();
    assertThat(console.parse("set language = cypher")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));

    assertThat(console.parse("RETURN 44 AS value // a comment with a semicolon ; here")).isTrue();
    assertThat(buffer.toString()).contains("44").doesNotContain("ERROR");

    buffer.setLength(0);
    assertThat(console.parse("MATCH (a:Person) -- (b:Person) RETURN count(a) AS value")).isTrue();
    assertThat(buffer.toString()).doesNotContain("ERROR");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5457: a script is loaded line by line, but a block comment can span
   * multiple lines.
   */
  @Test
  void loadScriptWithComments() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();

    final File script = new File("./target/issue-5457.sql");
    try {
      Files.writeString(script.toPath(), """
          /* a block comment
             spanning ; multiple lines */
          create document type Loaded;
          insert into Loaded set id = 55; -- a trailing ; comment
          """);

      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));

      assertThat(console.parse("load " + script.getAbsolutePath())).isTrue();
      assertThat(buffer.toString()).doesNotContain("ERROR");

      buffer.setLength(0);
      assertThat(console.parse("select from Loaded")).isTrue();
      assertThat(buffer.toString()).contains("55").doesNotContain("ERROR");
    } finally {
      script.delete();
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/6372: an empty line in a script loaded with LOAD must not echo an
   * empty prompt of its own - only the real commands are echoed.
   */
  @Test
  void loadScriptWithEmptyLinesDoesNotEchoEmptyPrompts() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();

    final File script = new File("./target/issue-6372.sql");
    try {
      Files.writeString(script.toPath(), """
          CREATE DOCUMENT TYPE Loaded;


          INSERT INTO Loaded SET id = 1;

          INSERT INTO Loaded SET id = 2
          """);

      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));

      assertThat(console.parse("load " + script.getAbsolutePath())).isTrue();

      final String output = buffer.toString();
      assertThat(output).doesNotContain("ERROR");

      // ONLY THE 3 REAL COMMANDS (CREATE, INSERT, INSERT) MUST BE ECHOED WITH A PROMPT - THE 3 EMPTY LINES IN
      // BETWEEN MUST NOT PRODUCE A PROMPT OF THEIR OWN
      final String marker = "{" + DB_NAME + "}> ";
      int promptCount = 0;
      for (int idx = output.indexOf(marker); idx != -1; idx = output.indexOf(marker, idx + marker.length()))
        ++promptCount;
      assertThat(promptCount).isEqualTo(3);

      buffer.setLength(0);
      assertThat(console.parse("select count(*) as count from Loaded")).isTrue();
      assertThat(buffer.toString()).contains("2");
    } finally {
      script.delete();
    }
  }

  @Test
  void listDatabases() throws Exception {
    assertThat(console.parse("list databases;")).isTrue();
  }

  @Test
  void connect() throws Exception {
    assertThat(console.parse("connect " + DB_NAME + ";info types")).isTrue();
  }

  @Test
  @DisabledOnOs({OS.WINDOWS})
  void localConnect() throws Exception {
    assertThat(console.parse("connect local:/" + absoluteDBPath + "/" + DB_NAME + ";info types", false)).isTrue();
  }

  @Test
  void setVerbose() throws Exception {
    assertThatThrownBy(() -> console.parse("set verbose = 2; close; connect " + DB_NAME + "XX")).isInstanceOf(DatabaseOperationException.class);
  }

  @Test
  void setLanguage() throws Exception {
    console.parse("connect " + DB_NAME + ";set language = sql; select 1");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/6439: `set language = 'sql'` used to store the value with its quotes,
   * and `'sql'.startsWith("sql")` is false, so TerminalParser.setLanguage() picked the non-SQL `//` comment marker. That left
   * `--` unrecognised, so the semicolon inside the comment below would have split the command in two instead of being dropped.
   */
  @Test
  void setLanguageWithQuotedSqlValueKeepsTheSqlCommentMarker() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("set language = 'sql'")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));

    assertThat(console.parse("select 11 as value -- a comment with a semicolon ; here")).isTrue();
    assertThat(buffer.toString()).contains("11").doesNotContain("ERROR");
  }

  @Test
  void setLanguageWithDoubleQuotedValueStripsTheQuotes() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("set language = \"sql\"")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));

    assertThat(console.parse("select 11 as value -- a comment with a semicolon ; here")).isTrue();
    assertThat(buffer.toString()).contains("11").doesNotContain("ERROR");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/6439: a SET value that starts with a quote character but never closes
   * it is always a typo, so it must be rejected instead of being stored half-quoted.
   */
  @Test
  void setWithUnbalancedQuoteIsRejected() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThatThrownBy(() -> console.parse("set language = 'sql")).isInstanceOf(ConsoleException.class)
        .hasMessageContaining("unbalanced quote");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/6439: an unclosed '{' in a CONTENT clause used to swallow every
   * following command into one malformed statement, which then failed downstream with a confusing syntax error pointing at
   * text typed several statements earlier. The commands before the corrupted one must still run, and the corrupted tail must
   * be reported clearly instead of being forwarded to the query engine.
   */
  @Test
  void unbalancedOpeningBraceRunsEarlierCommandsThenReportsTheCorruptedTail() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));

    assertThatThrownBy(
        () -> console.parse("select 11 as value; insert into doc content {\"a\": 1 ; select 22 as value"))
        .isInstanceOf(ConsoleException.class)
        .hasMessageContaining("Unbalanced '{'");

    assertThat(buffer.toString()).contains("11").contains("ERROR");
  }

  @Test
  void createClass() throws Exception {
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

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5931
   */
  @Test
  void infoTypeWithQuoteInName() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("CREATE DOCUMENT TYPE `a\"b`")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("info type a\"b")).isTrue();
    assertThat(buffer.toString()).contains("DOCUMENT TYPE 'a\"b'").doesNotContain("ERROR");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5929
   */
  @Test
  void progressBarClampsPercentageAboveHundred() {
    assertThat(Console.formatProgressLine("import", "loading", 1, 2, 110)).contains("|" + "=".repeat(20) + "| 110%");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5928
   */
  @Test
  void systemPropertyArgumentWithoutValueDoesNotCrash() throws Exception {
    final String key = "arcadedb.test.issue5928";
    try {
      assertThatCode(() -> Console.execute(new String[] { "-D" + key + "=", "-b" })).doesNotThrowAnyException();
      assertThat(System.getProperty(key)).isEqualTo("");

      System.clearProperty(key);

      assertThatCode(() -> Console.execute(new String[] { "-D" + key, "-b" })).doesNotThrowAnyException();
      assertThat(System.getProperty(key)).isEqualTo("");
    } finally {
      System.clearProperty(key);
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5928: a value containing further '=' characters must be kept whole,
   * not truncated at the second one.
   */
  @Test
  void systemPropertyArgumentWithEmbeddedEqualsKeepsFullValue() throws Exception {
    final String key = "arcadedb.test.issue5928.multi";
    try {
      assertThatCode(() -> Console.execute(new String[] { "-D" + key + "=bar=baz", "-b" })).doesNotThrowAnyException();
      assertThat(System.getProperty(key)).isEqualTo("bar=baz");
    } finally {
      System.clearProperty(key);
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5928: an argument with no key at all ('-D' alone or '-D=value') must
   * not crash the console with IllegalArgumentException from System.setProperty.
   */
  @Test
  void systemPropertyArgumentWithoutKeyDoesNotCrash() throws Exception {
    assertThatCode(() -> Console.execute(new String[] { "-D", "-b" })).doesNotThrowAnyException();
    assertThatCode(() -> Console.execute(new String[] { "-D=value", "-b" })).doesNotThrowAnyException();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/6392: SET used to split the argument on every '=', so a value that
   * contains one (a connection string, a base64 padding, a date pattern) was rejected as a syntax error. It is the same rule
   * already fixed for the `-D<key>=<value>` arguments in #5928.
   */
  @Test
  void setKeepsAValueContainingTheSeparator() throws Exception {
    try {
      assertThat(console.parse("set " + GlobalConfiguration.SERVER_BACKUP_DIRECTORY.getKey() + " = ./target/backups?a=b")).isTrue();
      assertThat(GlobalConfiguration.SERVER_BACKUP_DIRECTORY.getValueAsString()).isEqualTo("./target/backups?a=b");
    } finally {
      GlobalConfiguration.SERVER_BACKUP_DIRECTORY.reset();
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/6392: an empty value dropped the trailing token, leaving a single part
   * that was rejected instead of clearing the setting.
   */
  @Test
  void setAcceptsAnEmptyValue() throws Exception {
    try {
      assertThat(console.parse("set " + GlobalConfiguration.SERVER_BACKUP_DIRECTORY.getKey() + " =")).isTrue();
      assertThat(GlobalConfiguration.SERVER_BACKUP_DIRECTORY.getValueAsString()).isEmpty();
    } finally {
      GlobalConfiguration.SERVER_BACKUP_DIRECTORY.reset();
    }
  }

  @Test
  void setStillWorksWithAPlainValue() throws Exception {
    assertThatCode(() -> console.parse("set limit = 7")).doesNotThrowAnyException();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/6392: what is still malformed must stay malformed, and the message says
   * which half is missing so the user does not have to guess.
   */
  @Test
  void setWithoutTheSeparatorIsRejected() {
    assertThatThrownBy(() -> console.parse("set limit")).isInstanceOf(ConsoleException.class)
        .hasMessageContaining("Invalid syntax for SET, use");
  }

  @Test
  void setWithoutAKeyIsRejected() {
    assertThatThrownBy(() -> console.parse("set = 7")).isInstanceOf(ConsoleException.class).hasMessageContaining("missing name");
    assertThatThrownBy(() -> console.parse("set    = 7")).isInstanceOf(ConsoleException.class).hasMessageContaining("missing name");
  }

  /**
   * The setting names are ASCII, so their case must fold in English: with a Turkish default locale the dotless lowercase of 'I'
   * used to make `LIMIT` miss its own branch and fall through to the global configuration.
   */
  @Test
  void setNameIsFoldedInEnglishWhateverTheDefaultLocale() throws Exception {
    final Locale defaultLocale = Locale.getDefault();
    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      console.parse("set LIMIT = 7");
    } finally {
      Locale.setDefault(defaultLocale);
    }
    assertThat(buffer.toString()).contains("Set new limit to 7");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5927
   */
  @Test
  void listDatabasesOnFreshInstallDoesNotThrow() throws Exception {
    final File freshRoot = new File("./target/fresh-install-5927");
    FileUtils.deleteRecursively(freshRoot);
    assertThat(freshRoot.mkdirs()).isTrue();

    final String previousRootPath = GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString();
    GlobalConfiguration.SERVER_ROOT_PATH.setValue(freshRoot.getAbsolutePath());
    try {
      assertThat(new File(freshRoot, "databases").exists()).isFalse();

      final Console freshConsole = new Console();
      try {
        assertThatCode(() -> freshConsole.parse("list databases")).doesNotThrowAnyException();
      } finally {
        freshConsole.close();
      }
    } finally {
      GlobalConfiguration.SERVER_ROOT_PATH.setValue(previousRootPath);
      FileUtils.deleteRecursively(freshRoot);
    }
  }

  @Test
  void insertAndSelectRecord() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create document type Person")).isTrue();
    assertThat(console.parse("insert into Person set name = 'Jay', lastname='Miner'")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("select from Person")).isTrue();
    assertThat(buffer.toString().contains("Jay")).isTrue();
  }

  @Test
  void insertAndRollback() throws Exception {
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
  void help() throws Exception {
    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("?")).isTrue();
    assertThat(buffer.toString().contains("quit")).isTrue();
  }

  @Test
  void infoError() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThatThrownBy(() -> assertThat(console.parse("info blablabla")).isTrue()).isInstanceOf(ConsoleException.class);
  }

  @Test
  void allRecordTypes() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create document type D")).isTrue();
    assertThat(console.parse("create vertex type V")).isTrue();
    assertThat(console.parse("create edge type E")).isTrue();

    assertThat(console.parse("insert into D set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.parse("insert into V set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.parse("insert into V set name = 'John', lastname='Red'")).isTrue();
    assertThat(
        console.parse("create edge E from (select from V where name ='Jay') to (select from V where name ='John')")).isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(output -> buffer.append(output));
    assertThat(console.parse("select from D")).isTrue();
    assertThat(buffer.toString().contains("Jay")).isTrue();

    assertThat(console.parse("select from V")).isTrue();
    assertThat(console.parse("select from E")).isTrue();
    assertThat(buffer.toString().contains("John")).isTrue();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/691
   */
  @Test
  void notStringProperties() throws Exception {
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
  void userMgmtLocalError() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThatThrownBy(() -> assertThat(console.parse("create user albert identified by einstein")).isTrue()).isInstanceOf(Exception.class);

    assertThatThrownBy(() -> assertThat(console.parse("drop user jack")).isTrue()).isInstanceOf(Exception.class);
  }

  @Test
  void importNeo4jConsoleOK() throws Exception {
    final String DATABASE_PATH = "testNeo4j";

    final Console newConsole = new Console();
    newConsole.parse("create database " + DATABASE_PATH + ";import database file://src/test/resources/neo4j-export-mini.neo");
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
  void importCSVConsoleOK() throws Exception {
    final String DATABASE_PATH = "testCSV";

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
    assertThat(edges).isEqualTo(141);
  }

  @Test
  void nullValues() throws Exception {
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
  void projectionOrder() throws Exception {
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
  void asyncMode() throws Exception {
    assertThat(console.parse("connect " + DB_NAME)).isTrue();
    assertThat(console.parse("create document type D")).isTrue();
    assertThat(console.parse("create vertex type V")).isTrue();
    assertThat(console.parse("create edge type E")).isTrue();

    assertThat(console.parse("insert into D set name = 'Jay', lastname='Miner'")).isTrue();

    int asyncOperations = (int) ((DatabaseAsyncExecutorImpl) ((DatabaseInternal) console.getDatabase()).async()).getStats().scheduledTasks;
    assertThat(asyncOperations).isEqualTo(0);

    assertThat(console.parse("set asyncMode = true")).isTrue();

    assertThat(console.parse("insert into V set name = 'Jay', lastname='Miner'")).isTrue();
    assertThat(console.parse("insert into V set name = 'John', lastname='Red'")).isTrue();

    assertThat(console.parse("set asyncMode = false")).isTrue();

    asyncOperations = (int) ((DatabaseAsyncExecutorImpl) ((DatabaseInternal) console.getDatabase()).async()).getStats().scheduledTasks;
    assertThat(asyncOperations).isEqualTo(2);
  }

  @Test
  void batchMode() throws Exception {
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
    assertThat(console.parse("insert into V set name = 'John', lastname='Red'")).isTrue();
    assertThat(console.currentOperationsInBatch).isEqualTo(1);

    assertThat(console.parse("set transactionBatchSize = 0")).isTrue();
  }

  @Test
  void load() throws Exception {
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
  void customPropertyInSchema() throws Exception {
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
  void notNullProperties() throws Exception {
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
  void percentWildcardInQuery() throws Exception {
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

  /**
   *
   * Issue https://github.com/ArcadeData/arcadedb/issues/1760
   */
  @Test
  void duplicateEntries() throws Exception {
    FileUtils.deleteRecursively(new File("./target/databases/duptest"));
    assertThat(console.parse("create database duptest")).isTrue();

    assertThat(console.parse("CREATE DOCUMENT TYPE doc")).isTrue();
    assertThat(console.parse("CREATE PROPERTY doc.num LONG")).isTrue();
    assertThat(console.parse("CREATE INDEX ON doc (num) NOTUNIQUE")).isTrue();

    assertThat(console.parse("INSERT INTO doc SET num = 1")).isTrue();
    assertThat(console.parse("INSERT INTO doc SET num = 2")).isTrue();
    assertThat(console.parse("INSERT INTO doc SET num = 2")).isTrue();

    assertThat(console.getDatabase().query("sql", "SELECT count(*) AS count FROM index:`doc[num]`").next()
        .<Long>getProperty("count")).isEqualTo(3);
  }
}
