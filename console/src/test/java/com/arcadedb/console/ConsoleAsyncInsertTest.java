/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.console;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static com.arcadedb.server.StaticBaseServerTest.DEFAULT_PASSWORD_FOR_TESTS;

/**
 * From Discussion https://github.com/ArcadeData/arcadedb/discussions/1129#discussioncomment-6226545
 */
public class ConsoleAsyncInsertTest {
  static final String DATABASE_NAME  = "ConsoleAsyncInsertTest";
  static final int    PARALLEL_LEVEL = 6;

  private static class Product {
    private final String        fileName;
    private final String        fileType;
    private final LocalDateTime startValidity;
    private final LocalDateTime stopValidity;
    private final String        version;

    public Product(String fileName, String fileType, LocalDateTime startValidity, LocalDateTime stopValidity, String version) {
      this.fileName = fileName;
      this.fileType = fileType;
      this.startValidity = startValidity;
      this.stopValidity = stopValidity;
      this.version = version;
    }

    public String getFileName() {
      return fileName;
    }

    public String getFileType() {
      return fileType;
    }

    public LocalDateTime getStartValidity() {
      return startValidity;
    }

    public LocalDateTime getStopValidity() {
      return stopValidity;
    }

    public String getVersion() {
      return version;
    }
  }

  @Test
  public void testBulkAsyncInsertProductsUsingSQL() {
    GlobalConfiguration.SERVER_ROOT_PATH.setValue(".");
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("databases");

    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/" + DATABASE_NAME)) {
      if (databaseFactory.exists()) {
        databaseFactory.open().drop();
      }
      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtProducts = db.getSchema().buildDocumentType().withName("Product").withTotalBuckets(PARALLEL_LEVEL).create();
          dtProducts.createProperty("name", Type.STRING);
          dtProducts.createProperty("type", Type.STRING);
          dtProducts.createProperty("start", Type.DATETIME_MICROS);
          dtProducts.createProperty("stop", Type.DATETIME_MICROS);
          dtProducts.createProperty("v", Type.STRING);
          dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
          dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "type", "start", "stop");

          dtProducts.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());

          Assertions.assertEquals(PARALLEL_LEVEL, dtProducts.getBuckets(false).size());
        });
      }
    }
    String dateTimePattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS";
    GlobalConfiguration.SERVER_METRICS.setValue(false);
    GlobalConfiguration.HA_ENABLED.setValue(false);
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(java.time.LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue(dateTimePattern);
    GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(PARALLEL_LEVEL);

    ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);

    AtomicLong txErrorCounter = new AtomicLong();
    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(configuration);
    arcadeDBServer.start();
    ServerSecurity serverSecurity = arcadeDBServer.getSecurity();
    final String userName = "root";
    final String password = com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
    if (serverSecurity.getUser(userName) == null) {
      serverSecurity.createUser(new JSONObject().put("name", userName).put("password", serverSecurity.encodePassword(password))
          .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "admin" }))));
    }
    Database database = arcadeDBServer.getDatabase(DATABASE_NAME);
    database.async().setParallelLevel(PARALLEL_LEVEL);
    database.async().onError(exception -> {
      System.out.println("database.async() error: " + exception.getMessage());
      exception.printStackTrace();
      txErrorCounter.incrementAndGet();
    });
    final AtomicLong okCount = new AtomicLong();
    final AtomicLong errCount = new AtomicLong();
    String name;
    final long N = 100_000;
    Product product;
    LocalDateTime start, stop, ref;
    ref = LocalDateTime.now().minusMonths(1);

    final long begin = System.currentTimeMillis();

    for (int i = 0; i < N; i++) {
      name = UUID.randomUUID().toString();
      start = ref.plusMinutes(i);
      stop = ref.plusMinutes(i + 1);
      product = new Product(name, "SIR1LRM_0_", start, stop, "0001");
      inventoryProductAsyncWithSQL(database, product, okCount, errCount);
    }

    try {
      checkResults(txErrorCounter, userName, password, database, okCount, errCount, N, begin);
    } finally {
      arcadeDBServer.stop();
    }
  }

  @Test
  public void testBulkAsyncInsertProductsUsingAPI() {
    GlobalConfiguration.SERVER_ROOT_PATH.setValue(".");
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("databases");

    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/" + DATABASE_NAME)) {
      if (databaseFactory.exists()) {
        databaseFactory.open().drop();
      }
      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtProducts = db.getSchema().buildDocumentType().withName("Product").withTotalBuckets(PARALLEL_LEVEL).create();
          dtProducts.createProperty("name", Type.STRING);
          dtProducts.createProperty("type", Type.STRING);
          dtProducts.createProperty("start", Type.DATETIME_MICROS);
          dtProducts.createProperty("stop", Type.DATETIME_MICROS);
          dtProducts.createProperty("v", Type.STRING);
          dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
          dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "type", "start", "stop");

          //dtProducts.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
          dtProducts.setBucketSelectionStrategy(new PartitionedBucketSelectionStrategy(List.of("name")));

          Assertions.assertEquals(PARALLEL_LEVEL, dtProducts.getBuckets(false).size());
        });
      }
    }
    String dateTimePattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS";
    GlobalConfiguration.SERVER_METRICS.setValue(false);
    GlobalConfiguration.HA_ENABLED.setValue(false);
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(java.time.LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue(dateTimePattern);
    GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(PARALLEL_LEVEL);

    ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);

    AtomicLong txErrorCounter = new AtomicLong();
    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(configuration);
    arcadeDBServer.start();
    ServerSecurity serverSecurity = arcadeDBServer.getSecurity();
    final String userName = "root";
    final String password = com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
    if (serverSecurity.getUser(userName) == null) {
      serverSecurity.createUser(new JSONObject().put("name", userName).put("password", serverSecurity.encodePassword(password))
          .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "admin" }))));
    }
    Database database = arcadeDBServer.getDatabase(DATABASE_NAME);
    database.async().setParallelLevel(PARALLEL_LEVEL);
    database.async().onError(exception -> {
      System.out.println("database.async() error: " + exception.getMessage());
      exception.printStackTrace();
      txErrorCounter.incrementAndGet();
    });
    final AtomicLong okCount = new AtomicLong();
    final AtomicLong errCount = new AtomicLong();
    String name;
    final long N = 100_000;
    Product product;
    LocalDateTime start, stop, ref;
    ref = LocalDateTime.now().minusMonths(1);

    final long begin = System.currentTimeMillis();

    for (int i = 0; i < N; i++) {
      name = UUID.randomUUID().toString();
      start = ref.plusMinutes(i);
      stop = ref.plusMinutes(i + 1);
      product = new Product(name, "SIR1LRM_0_", start, stop, "0001");
      inventoryProductAsyncWithAPI(database, product, okCount, errCount);
    }

    try {
      checkResults(txErrorCounter, userName, password, database, okCount, errCount, N, begin);
    } finally {
      arcadeDBServer.stop();
    }
  }

  private static void checkResults(AtomicLong txErrorCounter, String userName, String password, Database database, AtomicLong okCount, AtomicLong errCount,
      long N, long begin) {
    Assertions.assertTrue(database.async().waitCompletion(30_000));

    System.out.println("Total async insertion of " + N + " elements in " + (System.currentTimeMillis() - begin));

    Assertions.assertEquals(okCount.get(), N);
    Assertions.assertEquals(errCount.get(), 0);
    Assertions.assertEquals(txErrorCounter.get(), 0);
    try (ResultSet resultSet = database.query("sql", "SELECT count(*) as total FROM Product")) {
      Result result = resultSet.next();
      Assertions.assertEquals((Long) result.getProperty("total"), N);
      Console console = new Console();
      String URL = "remote:localhost/" + DATABASE_NAME + " " + userName + " " + password;
      Assertions.assertTrue(console.parse("connect " + URL));
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(output -> buffer.append(output));
      Assertions.assertTrue(console.parse("select count(*) from Product"));
      String[] lines = buffer.toString().split("\\r?\\n|\\r");
      int count = Integer.parseInt(lines[4].split("\\|")[2].trim());
      Assertions.assertEquals(N, count);
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

  private void inventoryProductAsyncWithAPI(Database database, Product p, AtomicLong okCount, AtomicLong errCount) {
    String fileName = p.getFileName();
    try {
      final IndexCursor cursor = database.lookupByKey("Product", "name", fileName);
      if (cursor.hasNext()) {

        final MutableDocument record = cursor.next().asDocument(true).modify();
        database.async().updateRecord(record, updatedRecord -> okCount.incrementAndGet(), exception -> {
          errCount.incrementAndGet();
          System.out.println("database.async.command() error: " + exception.getMessage());
        });

      } else {

        final MutableDocument record = database.newDocument("Product")
            .set("name", fileName, "type", p.getFileType(), "start", p.getStartValidity(), "stop", p.getStopValidity(), "v", p.getVersion());
        database.async().createRecord(record, newRecord -> okCount.incrementAndGet(), exception -> {
          errCount.incrementAndGet();
          System.out.println("database.async.command() error: " + exception.getMessage());
        });

      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void inventoryProductAsyncWithSQL(Database database, Product p, AtomicLong okCount, AtomicLong errCount) {
    String UPSERT_PRODUCT = "UPDATE Product SET name = ?, type = ?, start = ?, stop = ?, v = ? UPSERT WHERE name = ?";
    String fileName = p.getFileName();
    try {
      database.async().command("sql", UPSERT_PRODUCT, new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet resultset) {
          okCount.incrementAndGet();
        }

        @Override
        public void onError(final Exception exception) {
          errCount.incrementAndGet();
          System.out.println("database.async.command() error: " + exception.getMessage());
        }
      }, fileName, p.getFileType(), p.getStartValidity(), p.getStopValidity(), p.getVersion(), fileName);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterEach
  public void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
