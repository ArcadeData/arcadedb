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
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Issue https://github.com/ArcadeData/arcadedb/issues/1233
 */
public class CompositeIndexUpdateTest {
  private AtomicInteger autoIncrementOrderId = new AtomicInteger();

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

    public String getString() {
      return orderStatus;
    }

    public Object getStartTime() {
      return start;
    }

    public Object getStopTime() {
      return stop;
    }

    public Object getStatus() {
      return orderStatus;
    }
  }

  @Test
  public void testWhereAfterAsyncInsert() {
    final int PARALLEL_LEVEL = 6;
    final String DATABASE_NAME = "test";
    final int TOTAL_ORDERS = 7;
    try (DatabaseFactory databaseFactory = new DatabaseFactory("./target/databases/" + DATABASE_NAME)) {
      if (databaseFactory.exists()) {
        databaseFactory.open().drop();
      }
      try (Database db = databaseFactory.create()) {
        DocumentType dtOrders = db.getSchema().buildDocumentType().withName("Order").withTotalBuckets(PARALLEL_LEVEL).create();
        dtOrders.createProperty("id", Type.INTEGER);
        dtOrders.createProperty("processor", Type.STRING);
        dtOrders.createProperty("vstart", Type.DATETIME_MICROS);
        dtOrders.createProperty("vstop", Type.DATETIME_MICROS);
        dtOrders.createProperty("status", Type.STRING);
        dtOrders.createProperty("node", Type.STRING);
        dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
        dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status", "id");
        dtOrders.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
      }
    }
    String dateTimePattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS";
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
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
    database.async().setParallelLevel(PARALLEL_LEVEL);
    System.out.println();

    // insert TOTAL_ORDERS orders
    Assertions.assertEquals(insertOrders(database, TOTAL_ORDERS).get("totalRows"), TOTAL_ORDERS);

    // retrieve next eligible, it should return order id = 1
    Assertions.assertEquals(retrieveNextEligibleOrder(database), 1);

    // update order 1 to ERROR
    updateOrderAsync(database, 1, "ERROR", LocalDateTime.now(), LocalDateTime.now().plusMinutes(5), "cs2minipds-test");

    // retrieve next eligible, it should return order id = 2
    Assertions.assertEquals(retrieveNextEligibleOrder(database), 2);
    database.async().waitCompletion();

    // re-submit order
    MutableDocument document = resubmitOrder(database, 1);
    System.out.println("re-submit result = " + document.toJSON());

    // retrieve correct order by id
    try (ResultSet resultSet = database.query("sql", "select status from Order where id = 1")) {
      Assertions.assertEquals(resultSet.next().getProperty("status"), "PENDING");
    }

    try (ResultSet resultSet = database.query("sql", "SELECT  FROM Order WHERE status = 'PENDING' ORDER BY id ASC")) {
      while (resultSet.hasNext()) {
        Result result = resultSet.next();
        System.out.println("-> " + result.toJSON());
      }
    }

    // retrieve next eligible, it should return order id = 1
    try {
      Assertions.assertEquals(1, retrieveNextEligibleOrder(database));
    } finally {
      arcadeDBServer.stop();
    }
  }

  private JSONObject insertOrders(Database database, int nOrders) {
    List<CandidateOrder> orders = new ArrayList<>(nOrders);
    LocalDateTime start, stop, ref;
    ref = LocalDateTime.now().minusMonths(1);
    // insert orders
    for (int i = 0; i < nOrders; i++) {
      start = ref.plusMinutes(i);
      stop = ref.plusMinutes(i + 1);
      orders.add(new CandidateOrder("SIR1LRM-7.1", "#2:0", start, stop, "cs2minipds-test", "PENDING"));
    }
    JSONObject jsonObject = insertOrdersAsync(database, orders);
    System.out.println("insert result = " + jsonObject.toString());
    return jsonObject;
  }

  public JSONObject insertOrdersAsync(Database database, List<CandidateOrder> orders) {
    JSONObject result = new JSONObject();
    if (orders.size() == 0) {
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
        record.set("status", order.getString());
        totalRows.incrementAndGet();
        if (totalRows.get() == 1) {
          firstOrderId[0] = (int) record.get("id");
        }
        record.save();
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
        record.set("status", order.getString());
        record.save();
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

  private int retrieveNextEligibleOrder(Database database) {
    String RETRIEVE_NEXT_ELIGIBLE_ORDERS = "SELECT id, processor, trigger, vstart, vstop, status FROM Order WHERE status = ? ORDER BY id ASC LIMIT ?";

    Result result;
    try (ResultSet resultSet = database.query("sql", RETRIEVE_NEXT_ELIGIBLE_ORDERS, "PENDING", 1)) {
      if (resultSet.hasNext()) {
        result = resultSet.next();
        System.out.println("retrieve result = " + result.toJSON());
        return result.getProperty("id");
      } else {
        Assertions.fail("no orders found");
        return 0;
      }
    }
  }

  public long updateOrderAsync(Database database, int orderId, String orderStatus, LocalDateTime procStart, LocalDateTime procStop,
      String node) {
    final AtomicLong updateCount = new AtomicLong();
    IndexCursor indexCursor = database.lookupByKey("Order", "id", orderId);
    MutableDocument record;
    if (indexCursor.hasNext()) {
      record = indexCursor.next().getRecord().asDocument(true).modify();
      record.set("status", orderStatus);
      if (node != null) {
        record.set("node", node);
      }
      System.out.println("modified record = " + record);
      record.save();
    } else {
      Assertions.fail("could not find order id = " + orderId);
    }
    if (!database.async().waitCompletion(3000)) {
      Assertions.fail("timeout expired before order update completion");
    }
    return updateCount.get();
  }

  private MutableDocument resubmitOrder(Database database, int orderId) {
    MutableDocument[] record = new MutableDocument[1];
    try {
      database.transaction(() -> {
        IndexCursor indexCursor = database.lookupByKey("Order", "id", orderId);
        if (indexCursor.hasNext()) {
          record[0] = indexCursor.next().getRecord().asDocument(true).modify();
          record[0].set("status", "PENDING");
          record[0].save();
        }
      });
    } catch (Exception e) {
      Assertions.fail();
    }
    return record[0];
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
