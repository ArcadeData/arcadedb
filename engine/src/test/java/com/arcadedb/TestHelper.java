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
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

public abstract class TestHelper {
  private static final int             PARALLEL_LEVEL = 4;
  protected final      DatabaseFactory factory;
  protected            Database        database;
  protected            boolean         autoStartTx    = false;

  public interface DatabaseTest<PAR> {
    void call(PAR iArgument) throws Exception;
  }

  protected TestHelper() {
    this(true);
  }

  protected TestHelper(final boolean cleanBeforeTest) {
    GlobalConfiguration.PROFILE.setValue(getPerformanceProfile());

    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());

    if (cleanBeforeTest)
      FileUtils.deleteRecursively(new File(getDatabasePath()));
    factory = new DatabaseFactory(getDatabasePath());
    database = factory.exists() ? factory.open() : factory.create();
    Assertions.assertEquals(database, DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath()));

    database.async().setParallelLevel(PARALLEL_LEVEL);

    if (autoStartTx)
      database.begin();
  }

  protected boolean isCheckingDatabaseIntegrity() {
    return true;
  }

  public static void executeInNewDatabase(final DatabaseTest<Database> callback) throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory("./target/databases/" + UUID.randomUUID())) {
      if (factory.exists()) {
        factory.open().drop();
        Assertions.assertNull(DatabaseFactory.getActiveDatabaseInstance(factory.getDatabasePath()));
      }

      final Database database = factory.create();
      Assertions.assertEquals(database, DatabaseFactory.getActiveDatabaseInstance(factory.getDatabasePath()));
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

  public static DocumentType createRandomType(final Database database) {
    return database.getSchema().createDocumentType("RandomType" + new Random().nextInt(100_000));
  }

  public static void executeInNewDatabase(final String testName, final DatabaseTest<DatabaseInternal> callback) throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory("./target/" + testName)) {
      if (factory.exists())
        factory.open().drop();

      final DatabaseInternal database = (DatabaseInternal) factory.create();
      Assertions.assertEquals(database, DatabaseFactory.getActiveDatabaseInstance(factory.getDatabasePath()));
      try {
        callback.call(database);
      } finally {
        database.drop();
        Assertions.assertNull(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath()));
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
    Assertions.assertNull(DatabaseFactory.getActiveDatabaseInstance(factory.getDatabasePath()));
    return factory;
  }

  protected void reopenDatabase() {
    if (database != null) {
      database.close();
      Assertions.assertNull(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath()));
    }
    database = factory.open();
    Assertions.assertEquals(database, DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath()));
  }

  protected void reopenDatabaseInReadOnlyMode() {
    if (database != null) {
      database.close();
      Assertions.assertNull(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath()));
    }

    database = factory.open(PaginatedFile.MODE.READ_ONLY);
    Assertions.assertEquals(database, DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath()));
  }

  protected String getDatabasePath() {
    return "target/databases/" + getClass().getSimpleName();
  }

  protected void beginTest() {
  }

  protected void endTest() {
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

      if (database.getMode() == PaginatedFile.MODE.READ_ONLY)
        reopenDatabase();

      ((DatabaseInternal) database).getEmbedded().drop();
      database = null;
    }

    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
    FileUtils.deleteRecursively(new File(getDatabasePath()));
  }

  @AfterAll
  public static void endAllTests() {
    GlobalConfiguration.resetAll();
  }

  protected String getPerformanceProfile() {
    return "default";
  }

  public static void expectException(final CallableNoReturn callback, final Class<? extends Throwable> expectedException) throws Exception {
    try {
      callback.call();
      Assertions.fail();
    } catch (Throwable e) {
      if (e.getClass().equals(expectedException))
        // EXPECTED
        return;

      if (e instanceof Exception)
        throw (Exception) e;

      throw new Exception(e);
    }
  }

  protected void checkDatabaseIntegrity() {
    ResultSet result = database.command("sql", "check database");
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(0, (Long) row.getProperty("autoFix"));
      Assertions.assertEquals(0, ((Collection) row.getProperty("corruptedRecords")).size());
      Assertions.assertEquals(0, (Long) row.getProperty("invalidLinks"));
      Assertions.assertEquals(0, ((Collection) row.getProperty("warnings")).size(), "Warnings" + row.getProperty("warnings"));
    }
  }
}
