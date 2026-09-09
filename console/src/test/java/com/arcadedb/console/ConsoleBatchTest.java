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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsoleBatchTest {
  @Test
  void batchMode() throws Exception {
    Console.execute(new String[] { "-b", "create database console; create vertex type ConsoleOnlyVertex;" });
    final Database db = new DatabaseFactory("./target/databases/console").open();
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    db.drop();
    assertThat(Console.isErrored()).isFalse();
  }

  @Test
  void okSqlCreationIfNotExists() throws Exception {
    Console.execute(new String[] { "-b",
        "create database console;" +
            "CREATE VERTEX TYPE Z IF NOT EXISTS;" +
            "CREATE PROPERTY Z.prop IF NOT EXISTS STRING;" +
            "CREATE INDEX IF NOT EXISTS ON Z (prop) UNIQUE;" +
            // THIS SHOULD NOT FAIL
            "CREATE INDEX IF NOT EXISTS ON Z (prop) UNIQUE;" });
  }

  @Test
  void okBatchMultiLine() throws Exception {
    Console.execute(new String[] { "-b",
        """
        create database console;
        create vertex type Batchtest;
        create vertex Batchtest set id = 1;
        create vertex Batchtest set id = 2;
        create vertex Batchtest set id = 3;
        LET x=SELECT FROM Batchtest;
        if($x.size()>0){
          return true;
        }
        return false;
        """ });

    final Database db = new DatabaseFactory("./target/databases/console").open();
    assertThat(db.getSchema().existsType("Batchtest")).isTrue();
    db.drop();
  }

  @Test
  void batchModeWithError() throws Exception {
    // This should fail
    assertThatThrownBy(() -> Console.execute(
        new String[] { "-b", """
          create database console;
          create vertex table WRONG_STATEMENT;
          create vertex type ConsoleOnlyVertex;
        """ }))
        .isInstanceOf(CommandSQLParsingException.class);

    final Database db = new DatabaseFactory("./target/databases/console").open();
    // the ConsoleOnlyVertex should not be created
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isFalse();
    db.drop();
  }

  @Test
  void batchModeWithFailAtEnd() throws Exception {
    // Error is only printed out
    Console.execute(
        new String[] { "-b", "-fae", """
          create database console;
          create vertex table WRONG_STATEMENT;
          create vertex type ConsoleOnlyVertex;
        """ });

    final Database db = new DatabaseFactory("./target/databases/console").open();
    // the ConsoleOnlyVertex is created
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    db.drop();
    assertThat(Console.isErrored()).isTrue();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/7115: in asyncMode a failed statement is reported by the async callback
   * on a worker thread. It must mark the run as errored exactly like the synchronous path does, otherwise the process exits 0.
   */
  @Test
  void batchModeWithErrorInAsyncMode() throws Exception {
    Console.execute(new String[] { "-b", """
        create database console;
        set asyncMode = true;
        insert into NoSuchType set a = 1;
        """ });
    assertThat(Console.isErrored()).isTrue();
  }

  /**
   * The other half of the same contract: a script that succeeds in asyncMode must NOT be marked errored. A false
   * positive here is worse than the bug, because it fails every green script in CI rather than passing a red one.
   */
  @Test
  void batchModeInAsyncModeIsNotErroredWhenEveryStatementSucceeds() throws Exception {
    // THE TYPE IS CREATED BEFORE asyncMode IS TURNED ON, ON PURPOSE: THE ASYNC EXECUTOR HAS SEVERAL WORKERS AND DOES
    // NOT ORDER THE STATEMENTS IT IS HANDED, SO AN async INSERT THAT DEPENDS ON AN async DDL RACES IT AND FAILS WITH
    // "type not found" ON A LOADED MACHINE - WHICH WOULD MAKE THIS TEST RED FOR A REASON THAT IS NOT ITS SUBJECT
    Console.execute(new String[] { "-b", """
        create database console;
        create vertex type ConsoleOnlyVertex;
        set asyncMode = true;
        insert into ConsoleOnlyVertex set a = 1;
        """ });
    assertThat(Console.isErrored()).isFalse();

    final Database db = new DatabaseFactory("./target/databases/console").open();
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    // THE async INSERT ITSELF LANDED, NOT JUST THE SYNCHRONOUS DDL BEFORE IT: WITHOUT THIS THE TEST WOULD STILL BE
    // GREEN IF THE STATEMENT HAD NEVER RUN, AND "NOT ERRORED" WOULD MEAN NOTHING. IT ALSO PINS THE DRAIN THE FLAG
    // DEPENDS ON - LocalDatabase.close() WAITS ON async.waitCompletion() BEFORE execute() RETURNS
    assertThat(db.countType("ConsoleOnlyVertex", false)).isEqualTo(1);
    db.drop();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/7300, follow-up to #7115: the OTHER async error channel.
   * <p>
   * Turning {@code asyncMode} on forces {@code ASYNC_TX_BATCH_SIZE = 1}, so the worker commits from its own run loop
   * OUTSIDE {@code DatabaseAsyncCommand.execute} - a failure raised by that commit therefore never reaches the
   * per-statement callback #7115 fixed, it goes to the executor-wide {@code async().onError()} handler the console
   * registers here. That handler printed and returned, so a unique-index violation surfaced at commit, a full volume
   * or a WAL write failure all printed their error and still exited 0: in a CI pipeline, indistinguishable from
   * success, which is the harm #7115 was filed about.
   * <p>
   * The duplicate is inserted AFTER the index and the first row exist and are committed, so the only thing that can
   * fail is the async worker's own commit of the second row.
   */
  @Test
  void batchModeWithCommitFailureInAsyncMode() throws Exception {
    Console.execute(new String[] { "-b", """
        create database console;
        create vertex type ConsoleOnlyVertex;
        create property ConsoleOnlyVertex.id integer;
        create index on ConsoleOnlyVertex (id) unique;
        insert into ConsoleOnlyVertex set id = 1;
        set asyncMode = true;
        insert into ConsoleOnlyVertex set id = 1;
        """ });
    assertThat(Console.isErrored())
        .as("a commit-time failure in asyncMode must decide the exit code like every other failed write")
        .isTrue();

    final Database db = new DatabaseFactory("./target/databases/console").open();
    // THE VIOLATION REALLY WAS REFUSED: WITHOUT THIS THE FLAG COULD BE TRUE FOR ANY OTHER REASON AND THE TEST WOULD
    // STILL BE GREEN
    assertThat(db.countType("ConsoleOnlyVertex", false)).isEqualTo(1);
    db.drop();
  }

  /**
   * The flag is static, so a failed run must not decide the exit code of the next one in the same JVM - which is what
   * an embedder calling {@link Console#execute(String[])} twice, and this very test class, both do.
   */
  @Test
  void aFailedRunDoesNotLeaveTheNextRunErrored() throws Exception {
    Console.execute(new String[] { "-b", """
        create database console;
        set asyncMode = true;
        insert into NoSuchType set a = 1;
        """ });
    assertThat(Console.isErrored()).isTrue();

    FileUtils.deleteRecursively(new File("./target/databases"));

    Console.execute(new String[] { "-b", "create database console; create vertex type ConsoleOnlyVertex;" });
    assertThat(Console.isErrored())
        .as("execute() starts every run clean, so the previous failure cannot decide this run's exit code")
        .isFalse();

    final Database db = new DatabaseFactory("./target/databases/console").open();
    db.drop();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5457: comments (even when they contain a semicolon) must not break the
   * script, and a comment at the end of an argument must not swallow the following argument.
   */
  @Test
  void batchModeWithComments() throws Exception {
    Console.execute(new String[] { "-b", """
        create database console; -- creates the database ; and this is a comment
        /* a block comment
           spanning ; multiple lines */
        create vertex type ConsoleOnlyVertex; -- creates the type
        """ });

    Database db = new DatabaseFactory("./target/databases/console").open();
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    db.drop();

    Console.execute(
        new String[] { "-b", "create database console -- a trailing comment ; here", "create vertex type ConsoleOnlyVertex" });

    db = new DatabaseFactory("./target/databases/console").open();
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    db.drop();
  }

  @Test
  void interactiveMode() throws Exception {
    Console.execute(new String[] { "create database console; create vertex type ConsoleOnlyVertex;exit" });
    final Database db = new DatabaseFactory("./target/databases/console").open();
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    db.drop();
  }

  @Test
  void swallowSettings() throws Exception {
    FileUtils.deleteRecursively(new File("./console"));
    Console.execute(new String[] { "-Darcadedb.server.databaseDirectory=.",
        "create database console; create vertex type ConsoleOnlyVertex;exit;" });
    final Database db = new DatabaseFactory("./console").open();
    assertThat(db.getSchema().existsType("ConsoleOnlyVertex")).isTrue();
    db.drop();
    GlobalConfiguration.resetAll();
  }

  @BeforeEach
  void cleanup() throws IOException {
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
