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
package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public abstract class TestHelper {
  protected static final int             PARALLEL_LEVEL = 4;
  protected final        DatabaseFactory factory;
  protected              Database        database;
  protected              boolean         autoStartTx    = false;

  public interface DatabaseTest<PAR> {
    void call(PAR iArgument) throws Exception;
  }

  protected TestHelper() {
    this(true);
  }

  protected TestHelper(final boolean cleanBeforeTest) {
    GlobalConfiguration.PROFILE.setValue(getPerformanceProfile());

    checkActiveDatabases();

    if (cleanBeforeTest)
      FileUtils.deleteRecursively(new File(getDatabasePath()));
    factory = new DatabaseFactory(getDatabasePath());
    database = factory.exists() ? factory.open() : factory.create();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isEqualTo(database);

    if (autoStartTx)
      database.begin();
  }

  protected boolean isCheckingDatabaseIntegrity() {
    return true;
  }

  public static void executeInNewDatabase(final DatabaseTest<Database> callback) throws Exception {
    executeInNewDatabase(UUID.randomUUID().toString(), callback);
  }

  public static DocumentType createRandomType(final Database database) {
    return database.getSchema().createDocumentType("RandomType" + new Random().nextInt(100_000));
  }

  public static void executeInNewDatabase(final String testName, final DatabaseTest<Database> callback) throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory("./target/databases" + testName)) {
      if (factory.exists()) {
        factory.open().drop();
        assertThat(DatabaseFactory.getActiveDatabaseInstance(factory.getDatabasePath())).isNull();
      }

      final Database database = factory.create();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(factory.getDatabasePath())).isEqualTo(database);
      try {
        database.begin();
        callback.call(database);
        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }
  }

  public static Database createDatabase(final String databaseName) {
    return dropDatabase(databaseName).create();
  }

  public static DatabaseFactory dropDatabase(final String databaseName) {
    final DatabaseFactory factory = new DatabaseFactory(databaseName);
    if (factory.exists())
      factory.open().drop();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(factory.getDatabasePath())).isNull();
    return factory;
  }

  protected void reopenDatabase() {
    if (database != null) {
      database.close();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isNull();
    }
    database = factory.open();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isEqualTo(database);
  }

  protected void reopenDatabaseInReadOnlyMode() {
    if (database != null) {
      database.close();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isNull();
    }

    database = factory.open(ComponentFile.MODE.READ_ONLY);
    assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isEqualTo(database);
  }

  protected String getDatabasePath() {
    return "target/databases/" + getClass().getSimpleName();
  }

  protected void beginTest() {
    // SUB CLASS CAN EXTEND THIS
  }

  protected void endTest() {
    // SUB CLASS CAN EXTEND THIS
  }

  @BeforeEach
  public void beforeTest() {
    if (autoStartTx && !database.isTransactionActive())
      database.begin();
    beginTest();
  }

  @AfterEach
  public void afterTest() {
    endTest();

    if (database.isTransactionActive())
      database.commit();

    if (database != null && database.isOpen()) {
      if (isCheckingDatabaseIntegrity())
        checkDatabaseIntegrity();

      if (database.getMode() == ComponentFile.MODE.READ_ONLY)
        reopenDatabase();

      ((DatabaseInternal) database).getEmbedded().drop();
      database = null;
    }

    checkActiveDatabases();
    FileUtils.deleteRecursively(new File(getDatabasePath()));
  }

  @AfterAll
  public static void endAllTests() {
    GlobalConfiguration.resetAll();
  }

  protected String getPerformanceProfile() {
    return "default";
  }

  public static void expectException(final CallableNoReturn callback, final Class<? extends Throwable> expectedException)
      throws Exception {
    try {
      callback.call();
      fail("");
    } catch (final Throwable e) {
      if (e.getClass().equals(expectedException))
        // EXPECTED
        return;

      if (e instanceof Exception)
        throw (Exception) e;

      throw new Exception(e);
    }
  }

  protected void checkDatabaseIntegrity() {
    final ResultSet result = database.command("sql", "check database");
    while (result.hasNext()) {
      final Result row = result.next();

      assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
      assertThat((Long) row.getProperty("autoFix")).isEqualTo(0);
      assertThat(((Collection<?>) row.getProperty("corruptedRecords")).size()).isEqualTo(0);
      assertThat((Long) row.getProperty("invalidLinks")).isEqualTo(0);
      assertThat(((Collection<?>) row.getProperty("warnings")).size()).as("Warnings" + row.getProperty("warnings")).isEqualTo(0);
    }
  }

  public static void checkActiveDatabases() {
    final Collection<Database> activeDatabases = DatabaseFactory.getActiveDatabaseInstances();

    if (!activeDatabases.isEmpty())
      LogManager.instance()
          .log(TestHelper.class, Level.SEVERE, "Found active databases: " + activeDatabases + ". Forced closing...");

    for (final Database db : activeDatabases)
      db.close();

    assertThat(activeDatabases.isEmpty()).as("Found active databases: " + activeDatabases).isTrue();
  }
}
