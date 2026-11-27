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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The script and java engines run the user's code on a pool of their own: what that code does against the database is checked
 * against the user who issued the command, not against a pool thread that has no user at all.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class UserCodeCallerUserTest {
  private static final String PATH      = "target/databases/UserCodeCallerUserTest";
  private static final String READER    = UserCodeCallerUserTest.class.getName() + "$Reader";
  private static final String USER_NAME = "scripter";

  /** Read by {@link Reader}, which the java engine invokes without handing it the database. */
  private static volatile Database databaseUnderTest;

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    // A SECURITY MANAGER, OR THE DATABASE CHECKS NO PERMISSION AT ALL
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.command("sql", "CREATE DOCUMENT TYPE Visible");
    database.command("sql", "CREATE DOCUMENT TYPE Restricted");
    database.transaction(() -> {
      database.newDocument("Visible").set("n", 1).save();
      database.newDocument("Restricted").set("n", 2).save();
    });
    databaseUnderTest = database;
  }

  @AfterEach
  void tearDown() {
    databaseUnderTest = null;
    GlobalConfiguration.POLYGLOT_COMMAND_TIMEOUT.reset();
    bindUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aScriptReadsWhatItsCallerMayRead() {
    bindUser(callerAllowedOnVisibleOnly());
    assertThat(scriptCount("Visible")).isEqualTo(1);
  }

  @Test
  void aScriptCannotReadATypeItsCallerMayNotRead() {
    bindUser(callerAllowedOnVisibleOnly());
    assertThatThrownBy(() -> scriptCount("Restricted")).isInstanceOf(CommandExecutionException.class)
        .hasStackTraceContaining("is not allowed to");
  }

  @Test
  void aScriptCannotWriteATypeItsCallerMayNotWrite() {
    bindUser(callerAllowedOnVisibleOnly());
    // THE SCRIPT OWNS ITS TRANSACTION: ON ITS POOL THREAD NONE IS OPEN WHEN IT STARTS
    final String insert = "database.begin(); try { database.command('sql', 'INSERT INTO Restricted SET n = 3'); database.commit(); }"
        + " catch (e) { database.rollback(); throw e; } 1";
    assertThatThrownBy(() -> database.command("js", insert)).isInstanceOf(CommandExecutionException.class)
        .hasStackTraceContaining("is not allowed to");

    bindUser(null);
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Restricted")) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(1);
    }
  }

  @Test
  void aScriptRunsAsTheUserWhoIssuedTheCommand() {
    bindUser(callerAllowedOnVisibleOnly());
    try (final ResultSet rs = database.command("js", "database.getCurrentUserName()")) {
      assertThat(rs.next().<String>getProperty("value")).isEqualTo(USER_NAME);
    }
  }

  @Test
  void aScriptOfAnEmbeddedCallerRunsWithoutUser() {
    try (final ResultSet rs = database.command("js", "database.getCurrentUserName()")) {
      assertThat(rs.next().<String>getProperty("value")).isNull();
    }
    assertThat(scriptCount("Restricted")).isEqualTo(1);
  }

  @Test
  void aJavaMethodCannotReadATypeItsCallerMayNotRead() {
    database.getQueryEngine("java").registerFunctions(READER);

    bindUser(callerAllowedOnVisibleOnly());
    assertThat(javaCount("countVisible")).isEqualTo(1);
    assertThatThrownBy(() -> javaCount("countRestricted")).isInstanceOf(CommandExecutionException.class)
        .hasStackTraceContaining("is not allowed to");
  }

  @Test
  void aJavaMethodRunsAsTheUserWhoIssuedTheCommand() {
    database.getQueryEngine("java").registerFunctions(READER);

    bindUser(callerAllowedOnVisibleOnly());
    try (final ResultSet rs = database.command("java", READER + "::userName")) {
      assertThat(rs.next().<String>getProperty("value")).isEqualTo(USER_NAME);
    }

    bindUser(null);
    try (final ResultSet rs = database.command("java", READER + "::userName")) {
      assertThat(rs.next().<Object>getProperty("value")).isNull();
    }
  }

  /** The pool threads are reused by every caller of the database: none of them may keep the user of the command it ran last. */
  @Test
  void aPoolThreadDoesNotKeepTheUserOfItsPreviousCommand() {
    database.getQueryEngine("java").registerFunctions(READER);

    // MORE COMMANDS THAN THE POOL HAS THREADS, SO EVERY THREAD RUNS AT LEAST ONE
    for (int i = 0; i < 20; i++) {
      bindUser(callerAllowedOnVisibleOnly());
      try (final ResultSet rs = database.command("js", "database.getCurrentUserName()")) {
        assertThat(rs.next().<String>getProperty("value")).isEqualTo(USER_NAME);
      }
      try (final ResultSet rs = database.command("java", READER + "::userName")) {
        assertThat(rs.next().<String>getProperty("value")).isEqualTo(USER_NAME);
      }
    }

    bindUser(null);
    for (int i = 0; i < 20; i++) {
      try (final ResultSet rs = database.command("js", "database.getCurrentUserName()")) {
        assertThat(rs.next().<Object>getProperty("value")).isNull();
      }
      try (final ResultSet rs = database.command("java", READER + "::userName")) {
        assertThat(rs.next().<Object>getProperty("value")).isNull();
      }
    }
  }

  /** A command that ran out of time unwinds on its pool thread: the thread must not stay bound to the user who issued it. */
  @Test
  void aPoolThreadDoesNotKeepTheUserOfACommandThatTimedOut() {
    GlobalConfiguration.POLYGLOT_COMMAND_TIMEOUT.setValue(1000);

    bindUser(callerAllowedOnVisibleOnly());
    // ONE MORE THAN THE POOL HAS THREADS: EACH COMMAND THAT IS SUBMITTED WHILE THE POOL IS BELOW ITS SIZE GETS A THREAD OF ITS OWN
    for (int i = 0; i < 9; i++)
      assertThatThrownBy(() -> database.command("js", "while(true);")).isInstanceOf(CommandExecutionException.class);

    bindUser(null);
    for (int i = 0; i < 20; i++)
      try (final ResultSet rs = database.command("js", "database.getCurrentUserName()")) {
        assertThat(rs.next().<Object>getProperty("value")).isNull();
      }
  }

  private int scriptCount(final String typeName) {
    try (final ResultSet rs = database.command("js",
        "const rs = database.query('sql', 'SELECT FROM " + typeName + "'); let n = 0; while (rs.hasNext()) { rs.next(); n++; } n")) {
      return rs.next().<Number>getProperty("value").intValue();
    }
  }

  private int javaCount(final String method) {
    try (final ResultSet rs = database.command("java", READER + "::" + method)) {
      return rs.next().<Number>getProperty("value").intValue();
    }
  }

  private void bindUser(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(user);
  }

  /** May do everything on the database, update its security included, and reads and writes every type but "Restricted". */
  private SecurityDatabaseUser callerAllowedOnVisibleOnly() {
    final Set<Integer> restrictedFiles = new HashSet<>();
    for (final Bucket bucket : database.getSchema().getType("Restricted").getBuckets(true))
      restrictedFiles.add(bucket.getFileId());

    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return USER_NAME;
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !restrictedFiles.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return !"Restricted".equals(typeName);
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }

  /** What a host application registers with the java engine: the engine hands the method no database. */
  public static class Reader {
    public static int countVisible() {
      return count("Visible");
    }

    public static int countRestricted() {
      return count("Restricted");
    }

    public static String userName() {
      return databaseUnderTest.getCurrentUserName();
    }

    private static int count(final String typeName) {
      int n = 0;
      try (final ResultSet rs = databaseUnderTest.query("sql", "SELECT FROM " + typeName)) {
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
      }
      return n;
    }
  }
}
