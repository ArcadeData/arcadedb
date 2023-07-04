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

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.integration.misc.IntegrationUtils;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

/**
 * From Issue https://github.com/ArcadeData/arcadedb/discussion/1129
 */
public class BatchInsertUpdateTest {
  static final String DATABASE_NAME = "BatchInsertUpdateTest";

  private class CandidateOrder {
    private final String[] values;

    private CandidateOrder(final String[] values) {
      this.values = values;
    }

    public String getProcessor() {
      return values[0];
    }

    public String getTriggerRid() {
      return values[1];
    }

    public String getStartTime() {
      return values[2];
    }

    public String getStopTime() {
      return values[3];
    }

    public String getStatus() {
      return values[4];
    }

    public void setStatus(String newStatus) {
      values[4] = newStatus;
    }
  }

  @Test
  public void testBatchAsyncInsertUpdate() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    try (DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/" + DATABASE_NAME)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();

      try (Database database = databaseFactory.create()) {
        DocumentType type = database.getSchema().getOrCreateDocumentType("Order");
        type.createProperty("processor", Type.STRING);
        type.createProperty("vstart", Type.STRING);
        type.createProperty("vstop", Type.STRING);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "processor", "vstart", "vstop");
      }

      ArcadeDBServer arcadeDBServer = new ArcadeDBServer(serverConfiguration);
      arcadeDBServer.start();
      Database database = arcadeDBServer.getDatabase(DATABASE_NAME);

      try {
        int TOTAL = 100_000;
        final List<CandidateOrder> ordersToProcess = new ArrayList<>(TOTAL);
        for (int i = 0; i < TOTAL; i++)
          ordersToProcess.add(new CandidateOrder(new String[] { "" + i, "" + i, "" + i, "" + i, "created" }));

        insertOrdersAsync(database, ordersToProcess);

        for (int i = 0; i < TOTAL / 2; i++)
          ordersToProcess.get(i).setStatus("updated");
        for (int i = TOTAL / 2; i < TOTAL; i++) {
          int k = TOTAL + i;
          ordersToProcess.set(i, new CandidateOrder(new String[] { "" + k, "" + k, "" + k, "" + k, "created" }));
        }

        insertOrdersAsync(database, ordersToProcess);

        int created = 0;
        int updated = 0;
        for (Iterator<Record> it = database.iterateType("Order", false); it.hasNext(); ) {
          final String status = it.next().asDocument().getString("status");
          if (status.equals("created"))
            ++created;
          else if (status.equals("updated"))
            ++updated;
        }
        Assertions.assertEquals(TOTAL, created);
        Assertions.assertEquals(TOTAL / 2, updated);

      } finally {
        arcadeDBServer.stop();
      }
    }
  }

  private JSONObject insertOrdersAsync(final Database database, List<CandidateOrder> orders) {
    final long begin = System.currentTimeMillis();

    JSONObject result = new JSONObject();
    CountDownLatch countDownLatch = new CountDownLatch(orders.size());
    AtomicLong counter = new AtomicLong();
    int[] firstOrderId = new int[1];
    database.async().onError(exception -> {
      exception.printStackTrace();
    });

    final AtomicLong autoIncrementOrderId = new AtomicLong();

    for (CandidateOrder order : orders) {
      IndexCursor indexCursor = database.lookupByKey("Order", new String[] { "processor", "vstart", "vstop" },
          new Object[] { order.getProcessor(), order.getStartTime(), order.getStopTime() });
      MutableDocument record;

      if (indexCursor.hasNext()) {
        record = indexCursor.next().getRecord().asDocument(true).modify();
        record.set("id", autoIncrementOrderId.incrementAndGet());
        record.set("processor", order.getProcessor());
        record.set("trigger", order.getTriggerRid());
        record.set("vstart", order.getStartTime());
        record.set("vstop", order.getStopTime());
        record.set("status", order.getStatus());

        database.async().updateRecord(record, newRecord -> {
          counter.incrementAndGet();
          countDownLatch.countDown();
        }, exception -> {
          exception.printStackTrace();
        });
      } else {
        record = database.newDocument("Order");
        record.set("id", autoIncrementOrderId.incrementAndGet());
        record.set("processor", order.getProcessor());
        record.set("trigger", order.getTriggerRid());
        record.set("vstart", order.getStartTime());
        record.set("vstop", order.getStopTime());
        record.set("status", order.getStatus());

        database.async().createRecord(record, newRecord -> {
          counter.incrementAndGet();
          countDownLatch.countDown();
        }, exception -> {
          exception.printStackTrace();
          autoIncrementOrderId.decrementAndGet();
        });
      }
    }

    database.async().waitCompletion(10_000);

    try {
      countDownLatch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.out.println("interrupted");
    }

    result.put("totalRows", counter.get());
    System.out.println("insert orders result = " + result + " in " + (System.currentTimeMillis() - begin) + "ms");
    return result;
  }

  @BeforeEach
  public void beginTests() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);
    //GlobalConfiguration.TYPE_DEFAULT_BUCKETS.setValue(1);
    try (DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/" + DATABASE_NAME)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();
    }
  }

  @AfterEach
  public void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
