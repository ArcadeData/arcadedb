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
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.integration.misc.IntegrationUtils;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

/**
 * From Issue https://github.com/ArcadeData/arcadedb/issues/741
 */
public class SelectOrderRIDTest {

  @Test
  public void testRIDOrdering() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);
    GlobalConfiguration.TYPE_DEFAULT_BUCKETS.setValue(1);
    try (DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/SelectOrderRIDTest")) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();

      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtOrders = db.getSchema().createDocumentType("Order");
          dtOrders.createProperty("processor", Type.STRING);
          dtOrders.createProperty("vstart", Type.STRING);
          dtOrders.createProperty("vstop", Type.STRING);
          dtOrders.createProperty("pstart", Type.STRING);
          dtOrders.createProperty("pstop", Type.STRING);
          dtOrders.createProperty("status", Type.STRING);
          dtOrders.createProperty("node", Type.STRING);
          dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status");
        });
      }

      ArcadeDBServer arcadeDBServer = new ArcadeDBServer(serverConfiguration);
      arcadeDBServer.start();
      Database database = arcadeDBServer.getDatabase("SelectOrderRIDTest");
      String customBucketName = "O202203";
      Bucket customBucket;

      if (!database.getSchema().existsBucket(customBucketName)) {
        customBucket = database.getSchema().createBucket(customBucketName);
        database.getSchema().getType("Order").addBucket(customBucket);
      }
      String processor = "SIR1LRM-7.1";
      String vstart = "20220319_002905.423534";
      String vstop = "20220319_003419.571172";
      String status = "PENDING";
      String UPSERT_ORDER = "UPDATE BUCKET:{bucket_name} SET processor = ?, vstart = ?, vstop = ?, status = ? UPSERT RETURN AFTER WHERE processor = ? AND vstart = ? AND vstop = ?";
      String sqlString = UPSERT_ORDER.replace("{bucket_name}", customBucketName);

      final RID[] rids = new RID[2];
      database.begin();
      try (ResultSet resultSet = database.command("sql", sqlString, processor, vstart, vstop, status, processor, vstart, vstop)) {
        rids[0] = resultSet.next().getElement().get().getIdentity();
      }
      vstart = "20220319_002624.404379";
      vstop = "20220319_002826.525650";
      try (ResultSet resultSet = database.command("sql", sqlString, processor, vstart, vstop, status, processor, vstart, vstop)) {
        rids[1] = resultSet.next().getElement().get().getIdentity();
      }
      database.commit();

      Object parameter = rids[1].toString();
      sqlString = "SELECT from Order WHERE @rid < ? ORDER BY @rid DESC LIMIT 10";

      Result result;

      database.begin();
      try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
        while (resultSet.hasNext()) {
          Assertions.assertEquals(rids[0], resultSet.next().getIdentity().get());
        }
      }
      database.commit();

      parameter = rids[0].toString();
      sqlString = "SELECT from Order WHERE @rid > ? ORDER BY @rid DESC LIMIT 10";

      database.begin();
      try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
        while (resultSet.hasNext()) {
          Assertions.assertEquals(rids[1], resultSet.next().getIdentity().get());
        }
      }
      database.commit();

      parameter = rids[0].toString();
      sqlString = "SELECT from Order WHERE @rid = ? ORDER BY @rid DESC LIMIT 10";

      database.begin();
      try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
        while (resultSet.hasNext()) {
          Assertions.assertEquals(rids[0], resultSet.next().getIdentity().get());
        }
      }
      database.commit();

      sqlString = "SELECT from Order WHERE @rid < #3:1 ORDER BY @rid DESC LIMIT 10";

      database.begin();
      try (ResultSet resultSet = database.query("sql", sqlString)) {
        while (resultSet.hasNext()) {
          Assertions.assertEquals(rids[0], resultSet.next().getIdentity().get());
        }
      }
      database.commit();

      sqlString = "SELECT from Order WHERE @rid = '#3:0' ORDER BY @rid DESC LIMIT 10";

      database.begin();
      try (ResultSet resultSet = database.query("sql", sqlString)) {
        while (resultSet.hasNext()) {
          Assertions.assertEquals(rids[0], resultSet.next().getIdentity().get());
        }
      }
      database.commit();

      sqlString = "SELECT from Order WHERE @rid < ? ORDER BY @rid DESC LIMIT 10";
      parameter = new RID(database, "#3:1");

      database.begin();
      try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
        while (resultSet.hasNext()) {
          Assertions.assertEquals(rids[0], resultSet.next().getIdentity().get());
        }
      }
      database.commit();

      arcadeDBServer.stop();

      databaseFactory.open().drop();
    }
  }

  @AfterAll
  public static void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
