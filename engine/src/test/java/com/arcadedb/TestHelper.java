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
package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.*;
import java.util.*;

public abstract class TestHelper {
  protected final DatabaseFactory factory;
  protected       Database        database;
  protected       boolean         autoStartTx = false;

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

    if (autoStartTx)
      database.begin();
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
      if (database.getMode() == PaginatedFile.MODE.READ_ONLY)
        reopenDatabase();

      ((DatabaseInternal) database).getWrappedDatabaseInstance().drop();
      database = null;
    }

    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
    FileUtils.deleteRecursively(new File(getDatabasePath()));
  }

  protected String getPerformanceProfile() {
    return "default";
  }
}
