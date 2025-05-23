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
import com.arcadedb.index.TypeIndex;
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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.server.StaticBaseServerTest.DEFAULT_PASSWORD_FOR_TESTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * From Discussion <a href="https://github.com/ArcadeData/arcadedb/discussions/1129#discussioncomment-6226545">...</a>
 */
public class ConsoleAsyncInsertTest {
  static final String DATABASE_NAME              = "ConsoleAsyncInsertTest";
  static final int    PARALLEL_LEVEL             = 6;
  static final String RECORD_TIME_FORMAT_PATTERN = "yyyyMMdd'_'HHmmss.SSSSSS";
  static final String userName                   = "user";
  static final String password                   = com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

  AtomicInteger autoIncrementOrderId = new AtomicInteger(0);

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

  private static class CandidateOrder {
    private final String        processor;
    private final String        triggerRid;
    private final LocalDateTime start;
    private final LocalDateTime stop;
    private final String        node;
    private final String        orderStatus;

    private CandidateOrder(String processor, String triggerRid, LocalDateTime start, LocalDateTime stop, String node,
        String orderStatus) {
      this.processor = processor;
      this.triggerRid = triggerRid;
      this.start = start;
      this.stop = stop;
      this.node = node;
      this.orderStatus = orderStatus;
    }

    public String getProcessor() {
      return processor;
    }

    public LocalDateTime getStart() {
      return start;
    }

    public LocalDateTime getStop() {
      return stop;
    }

    public String getTriggerRid() {
      return triggerRid;
    }

    public String getNode() {
      return node;
    }

    public String getOrderStatus() {
      return orderStatus;
    }
  }

  @Test
  public void testBulkAsyncInsertProductsUsingSQL() {
    GlobalConfiguration.SERVER_ROOT_PATH.setValue(".");
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");

    try (DatabaseFactory databaseFactory = new DatabaseFactory("./target/databases/" + DATABASE_NAME)) {
      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtProducts = db.getSchema().buildDocumentType().withName("Product").withTotalBuckets(PARALLEL_LEVEL)
              .create();
          dtProducts.createProperty("name", Type.STRING);
          dtProducts.createProperty("type", Type.STRING);
          dtProducts.createProperty("start", Type.DATETIME_MICROS);
          dtProducts.createProperty("stop", Type.DATETIME_MICROS);
          dtProducts.createProperty("v", Type.STRING);
          dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
          dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "type", "start", "stop");

          dtProducts.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());

          assertThat(dtProducts.getBuckets(false).size()).isEqualTo(PARALLEL_LEVEL);
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

    try {
      ServerSecurity serverSecurity = arcadeDBServer.getSecurity();
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

      checkResults(txErrorCounter, database, okCount, errCount, N, begin);
    } finally {
      arcadeDBServer.stop();
      FileUtils.deleteRecursively(new File(arcadeDBServer.getRootPath() + File.separator + "config"));
    }
  }

  @Test
  public void testBulkAsyncInsertProductsUsingAPI() {
    GlobalConfiguration.SERVER_ROOT_PATH.setValue(".");
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");

    try (DatabaseFactory databaseFactory = new DatabaseFactory("./target/databases/" + DATABASE_NAME)) {
      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtProducts = db.getSchema().buildDocumentType().withName("Product").withTotalBuckets(PARALLEL_LEVEL)
              .create();
          dtProducts.createProperty("name", Type.STRING);
          dtProducts.createProperty("type", Type.STRING);
          dtProducts.createProperty("start", Type.DATETIME_MICROS);
          dtProducts.createProperty("stop", Type.DATETIME_MICROS);
          dtProducts.createProperty("v", Type.STRING);
          dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
          dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "type", "start", "stop");

          //dtProducts.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
          dtProducts.setBucketSelectionStrategy(new PartitionedBucketSelectionStrategy(List.of("name")));

          assertThat(dtProducts.getBuckets(false).size()).isEqualTo(PARALLEL_LEVEL);
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

    try {
      ServerSecurity serverSecurity = arcadeDBServer.getSecurity();
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

      checkResults(txErrorCounter, database, okCount, errCount, N, begin);
    } finally {
      arcadeDBServer.stop();
      FileUtils.deleteRecursively(new File(arcadeDBServer.getRootPath() + File.separator + "config"));
    }
  }

  @Test
  public void testOrderByAfterDeleteInsert() {
    GlobalConfiguration.SERVER_ROOT_PATH.setValue(".");
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");

    final String DATABASE_NAME = "test";
    final int PARALLEL_LEVEL = 1;
    try (DatabaseFactory databaseFactory = new DatabaseFactory("./target/databases/" + DATABASE_NAME)) {
      try (Database db = databaseFactory.create()) {
        DocumentType dtOrders = db.getSchema().buildDocumentType().withName("Order").withTotalBuckets(PARALLEL_LEVEL).create();
        dtOrders.createProperty("id", Type.INTEGER);
        dtOrders.createProperty("processor", Type.STRING);
        dtOrders.createProperty("trigger", Type.LINK);
        dtOrders.createProperty("vstart", Type.DATETIME_MICROS);
        dtOrders.createProperty("vstop", Type.DATETIME_MICROS);
        dtOrders.createProperty("status", Type.STRING);
        dtOrders.createProperty("node", Type.STRING);
        dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
        dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status", "id");
        dtOrders.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
        DocumentType dtProducts = db.getSchema().buildDocumentType().withName("Product").withTotalBuckets(PARALLEL_LEVEL).create();
        dtProducts.createProperty("name", Type.STRING);
        dtProducts.createProperty("type", Type.STRING);
        dtProducts.createProperty("start", Type.DATETIME_MICROS);
        dtProducts.createProperty("stop", Type.DATETIME_MICROS);
        dtProducts.createProperty("v", Type.STRING);
        dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
        dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "type", "start", "stop");
        dtProducts.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
      }
    }
    String dateTimePattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS";
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(java.time.LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue(dateTimePattern);
    GlobalConfiguration.SERVER_METRICS.setValue(false);
    GlobalConfiguration.HA_ENABLED.setValue(false);
    GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(PARALLEL_LEVEL);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue("CRYO_CSRS");
    AtomicLong txErrorCounter = new AtomicLong();
    ContextConfiguration configuration = new ContextConfiguration();
    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(configuration);
    arcadeDBServer.start();
    Database database = arcadeDBServer.getDatabase(DATABASE_NAME);
    database.async().onError(exception -> {
      System.out.println("database.async() error: " + exception.getMessage());
      exception.printStackTrace();
      txErrorCounter.incrementAndGet();
    });
    final int TOTAL = 2;
    database.async().setParallelLevel(PARALLEL_LEVEL);
    String rid;
    Product p = new Product("CS_OPER_SIR1LRM_0__20130201T001643_20130201T002230_0001.DBL", "SIR1LRM_0_",
        LocalDateTime.now().minusMinutes(5), LocalDateTime.now(), "0001");
    try (ResultSet resultSet = database.command("sql",
        "insert into Product set name = ?, type = ?, start = ?, stop = ?, v = ? return @rid", arcadeDBServer.getConfiguration(),
        p.fileName, p.fileType, p.getStartValidity(), p.getStopValidity(), p.getVersion())) {
      assertThat(resultSet.hasNext()).isTrue();
      Result result = resultSet.next();
      rid = result.getProperty(RID_PROPERTY).toString();
    }
    List<CandidateOrder> orders = new ArrayList<>(TOTAL);
    LocalDateTime start, stop, ref;
    ref = LocalDateTime.now().minusMonths(1);
    for (int i = 0; i < TOTAL; i++) {
      start = ref.plusMinutes(i);
      stop = ref.plusMinutes(i + 1);
      orders.add(new CandidateOrder("SIR1LRM-7.1", rid, start, stop, "cs2minipds-test", "PENDING"));
    }
    JSONObject insertResult = insertOrdersAsync(database, orders);
    assertThat(TOTAL).isEqualTo(insertResult.getInt("totalRows"));
    int firstOrderId = 1;
    int lastOrderId = TOTAL;
    try (ResultSet resultSet = database.query("sql", "select from Order order by id")) {
      assertThat(resultSet.hasNext()).isTrue();
    }
    String DELETE_ORDERS = "DELETE FROM Order WHERE id >= ? AND id <= ?";
    try (ResultSet resultSet = database.command("sql", DELETE_ORDERS, firstOrderId, lastOrderId)) {
      assertThat((Long) resultSet.next().getProperty("count")).isEqualTo(TOTAL);
    }
    try (ResultSet resultSet = database.query("sql", "select from Order order by id")) {
      assertThat(resultSet.hasNext()).isFalse();
    }
    insertResult = insertOrdersAsync(database, orders);
    assertThat(insertResult.getInt("totalRows")).isEqualTo(TOTAL);
    try (ResultSet resultSet = database.query("sql", "select from Order")) {
      assertThat(resultSet.hasNext()).isTrue();
    }
    try (ResultSet resultSet = database.query("sql", "select from Order order by processor")) {
      assertThat(resultSet.hasNext()).isTrue();
    }
    try (ResultSet resultSet = database.query("sql", "select from Order order by id")) {
      assertThat(resultSet.hasNext()).isTrue();
    } finally {
      arcadeDBServer.stop();
      FileUtils.deleteRecursively(new File(arcadeDBServer.getRootPath() + File.separator + "config"));
    }
  }

  private void checkResults(AtomicLong txErrorCounter, Database database, AtomicLong okCount, AtomicLong errCount, long N,
      long begin) {
    assertThat(database.async().waitCompletion(30_000)).isTrue();

    System.out.println("Total async insertion of " + N + " elements in " + (System.currentTimeMillis() - begin));

    assertThat(N).isEqualTo(okCount.get());
    assertThat(errCount.get()).isEqualTo(0);
    assertThat(txErrorCounter.get()).isEqualTo(0);
    try (ResultSet resultSet = database.query("sql", "SELECT count(*) as total FROM Product")) {
      Result result = resultSet.next();
      assertThat(N).isEqualTo((Long) result.getProperty("total"));
      Console console = new Console();
      String URL = "remote:localhost/" + DATABASE_NAME + " " + userName + " " + password;
      assertThat(console.parse("connect " + URL)).isTrue();
      final StringBuilder buffer = new StringBuilder();
      console.setOutput(buffer::append);
      assertThat(console.parse("select count(*) from Product")).isTrue();
      String[] lines = buffer.toString().split("\\r?\\n|\\r");
      int count = Integer.parseInt(lines[4].split("\\|")[2].trim());
      assertThat(count).isEqualTo(N);
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
            .set("name", fileName, "type", p.getFileType(), "start", p.getStartValidity(), "stop", p.getStopValidity(), "v",
                p.getVersion());
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

  public JSONObject insertOrdersAsync(Database database, List<CandidateOrder> orders) {
    JSONObject result = new JSONObject();
    if (orders.isEmpty()) {
      result.put("totalRows", 0);
      result.put("firstOrderId", 0);
      result.put("lastOrderId", 0);
      return result;
    }
    IndexCursor indexCursor;
    MutableDocument record;
    final AtomicInteger totalRows = new AtomicInteger();
    final int[] firstOrderId = new int[1];
    TypeIndex insertOrdersIndex = database.getSchema().getType("Order")
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "processor", "vstart", "vstop");
    for (CandidateOrder order : orders) {
      indexCursor = database.lookupByKey("Order", new String[] { "processor", "vstart", "vstop" },
          new Object[] { order.getProcessor(), order.getStart(), order.getStop() });
      if (indexCursor.hasNext()) {
        System.out.println("found existing record");
        record = indexCursor.next().getRecord().asDocument(true).modify();
        record.set("processor", order.getProcessor());
        record.set("trigger", order.getTriggerRid());
        record.set("vstart", order.getStart());
        record.set("vstop", order.getStop());
        record.set("status", order.getOrderStatus());
        totalRows.incrementAndGet();
        if (totalRows.get() == 1) {
          firstOrderId[0] = (int) record.get("id");
        }
        database.async().updateRecord(record, newRecord -> {
        }, exception -> System.out.println(exception.getMessage()));
      } else {
        record = database.newDocument("Order");
        record.set("id", autoIncrementOrderId.incrementAndGet());
        totalRows.incrementAndGet();
        if (totalRows.get() == 1) {
          firstOrderId[0] = autoIncrementOrderId.get();
        }
        record.set("processor", order.getProcessor());
        record.set("trigger", order.getTriggerRid());
        record.set("vstart", order.getStart());
        record.set("vstop", order.getStop());
        record.set("status", order.getOrderStatus());
        database.async().createRecord(record, newRecord -> {
        }, exception -> {
          System.out.println(exception.getMessage());
          autoIncrementOrderId.decrementAndGet();
        });
      }
    }
    if (!database.async().waitCompletion(5000)) {
      System.out.println("timeout expired before order insertion completed");
    }
    database.getSchema().dropIndex(insertOrdersIndex.getName());
    result.put("totalRows", totalRows.get());
    result.put("firstOrderId", firstOrderId[0]);
    result.put("lastOrderId", firstOrderId[0] + totalRows.get() - 1);
    return result;
  }

  @BeforeEach
  public void cleanup() {
    FileUtils.deleteRecursively(new File("./target/databases/"));
  }

  @AfterEach
  public void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
