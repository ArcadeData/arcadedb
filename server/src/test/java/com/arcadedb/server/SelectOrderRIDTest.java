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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

/**
 * From Issue https://github.com/ArcadeData/arcadedb/issues/741
 */
public class SelectOrderRIDTest {

  @Test
  public void testRIDOrdering() {
    GlobalConfiguration.TYPE_DEFAULT_BUCKETS.setValue(1);
    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/test")) {
      if (!databaseFactory.exists()) {
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
      }
      ArcadeDBServer arcadeDBServer = new ArcadeDBServer(new ContextConfiguration());
      arcadeDBServer.start();
      Database database = arcadeDBServer.getDatabase("test");
      String customBucketName = "O202203";
      Bucket customBucket;
      System.out.println();
      if (!database.getSchema().existsBucket(customBucketName)) {
        customBucket = database.getSchema().createBucket(customBucketName);
        database.getSchema().getType("Order").addBucket(customBucket);
        System.out.println("created bucket " + customBucketName);
      }
      String processor = "SIR1LRM-7.1";
      String vstart = "20220319_002905.423534";
      String vstop = "20220319_003419.571172";
      String status = "PENDING";
      String UPSERT_ORDER = "UPDATE BUCKET:{bucket_name} SET processor = ?, vstart = ?, vstop = ?, status = ? UPSERT RETURN AFTER WHERE processor = ? AND vstart = ? AND vstop = ?";
      String sqlString = UPSERT_ORDER.replace("{bucket_name}", customBucketName);
      try {
        database.begin();
        try (ResultSet resultSet = database.command("sql", sqlString, processor, vstart, vstop, status, processor, vstart, vstop)) {
          System.out.print("insert record: sqlString = " + sqlString);
          System.out.println(", result = " + resultSet.next().toJSON());
        }
        vstart = "20220319_002624.404379";
        vstop = "20220319_002826.525650";
        try (ResultSet resultSet = database.command("sql", sqlString, processor, vstart, vstop, status, processor, vstart, vstop)) {
          System.out.print("insert record: sqlString = " + sqlString);
          System.out.println(", result = " + resultSet.next().toJSON());
        }
        database.commit();
      } catch (Exception e) {
        System.out.println(e.getMessage());
        database.rollback();
      }
      System.out.println("Test 1: select with @rid < #3:1 (as String) using prepared statement");
      Object parameter = "#3:1";
      sqlString = "SELECT from Order WHERE @rid < ? ORDER BY @rid DESC LIMIT 10";
      System.out.println("sqlString = " + sqlString);
      Result result;
      try {
        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
          System.out.print("result: ");
          while (resultSet.hasNext()) {
            result = resultSet.next();
            System.out.print("@rid = " + result.getIdentity().get() + ", ");
            System.out.print("processor = " + result.getProperty("processor") + ", ");
            System.out.print("vstart = " + result.getProperty("vstart") + ", ");
            System.out.print("vstop = " + result.getProperty("vstop") + ", ");
            System.out.print("pstart = " + result.getProperty("pstart") + ", ");
            System.out.print("pstop = " + result.getProperty("pstop") + ", ");
            System.out.print("status = " + result.getProperty("status") + ", ");
            System.out.println("node = " + result.getProperty("node"));
          }
          System.out.println();
        }
        database.commit();
      } catch (Exception e) {
        System.out.println(e.getMessage());
        database.rollback();
      }
      System.out.println("Test 2: select with @rid > #3:0 (as String) using prepared statement");
      parameter = "#3:0";
      sqlString = "SELECT from Order WHERE @rid > ? ORDER BY @rid DESC LIMIT 10";
      System.out.println("sqlString = " + sqlString);
      try {
        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
          System.out.print("result: ");
          while (resultSet.hasNext()) {
            result = resultSet.next();
            System.out.print("@rid = " + result.getIdentity().get() + ", ");
            System.out.print("processor = " + result.getProperty("processor") + ", ");
            System.out.print("vstart = " + result.getProperty("vstart") + ", ");
            System.out.print("vstop = " + result.getProperty("vstop") + ", ");
            System.out.print("pstart = " + result.getProperty("pstart") + ", ");
            System.out.print("pstop = " + result.getProperty("pstop") + ", ");
            System.out.print("status = " + result.getProperty("status") + ", ");
            System.out.println("node = " + result.getProperty("node"));
          }
          System.out.println();
        }
        database.commit();
      } catch (Exception e) {
        System.out.println(e.getMessage());
        database.rollback();
      }
      System.out.println("Test 3: select with @rid = #3:0 (as String) using prepared statement");
      parameter = "#3:0";
      sqlString = "SELECT from Order WHERE @rid = ? ORDER BY @rid DESC LIMIT 10";
      System.out.println("sqlString = " + sqlString);
      try {
        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
          System.out.print("result: ");
          while (resultSet.hasNext()) {
            result = resultSet.next();
            System.out.print("@rid = " + result.getIdentity().get() + ", ");
            System.out.print("processor = " + result.getProperty("processor") + ", ");
            System.out.print("vstart = " + result.getProperty("vstart") + ", ");
            System.out.print("vstop = " + result.getProperty("vstop") + ", ");
            System.out.print("pstart = " + result.getProperty("pstart") + ", ");
            System.out.print("pstop = " + result.getProperty("pstop") + ", ");
            System.out.print("status = " + result.getProperty("status") + ", ");
            System.out.println("node = " + result.getProperty("node"));
          }
          System.out.println();
        }
        database.commit();
      } catch (Exception e) {
        System.out.println(e.getMessage());
        database.rollback();
      }
      System.out.println("Test 4: select with @rid < #3:1 using simple statement");
      sqlString = "SELECT from Order WHERE @rid < #3:1 ORDER BY @rid DESC LIMIT 10";
      System.out.println("sqlString = " + sqlString);
      try {
        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString)) {
          System.out.print("result: ");
          while (resultSet.hasNext()) {
            result = resultSet.next();
            System.out.print("@rid = " + result.getIdentity().get() + ", ");
            System.out.print("processor = " + result.getProperty("processor") + ", ");
            System.out.print("vstart = " + result.getProperty("vstart") + ", ");
            System.out.print("vstop = " + result.getProperty("vstop") + ", ");
            System.out.print("pstart = " + result.getProperty("pstart") + ", ");
            System.out.print("pstop = " + result.getProperty("pstop") + ", ");
            System.out.print("status = " + result.getProperty("status") + ", ");
            System.out.println("node = " + result.getProperty("node"));
          }
          System.out.println();
        }
        database.commit();
      } catch (Exception e) {
        System.out.println(e.getMessage());
        database.rollback();
      }
      System.out.println("Test 5: select with @rid = '#3:0' using simple statement");
      sqlString = "SELECT from Order WHERE @rid = '#3:0' ORDER BY @rid DESC LIMIT 10";
      System.out.println("sqlString = " + sqlString);
      try {
        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString)) {
          System.out.print("result: ");
          while (resultSet.hasNext()) {
            result = resultSet.next();
            System.out.print("@rid = " + result.getIdentity().get() + ", ");
            System.out.print("processor = " + result.getProperty("processor") + ", ");
            System.out.print("vstart = " + result.getProperty("vstart") + ", ");
            System.out.print("vstop = " + result.getProperty("vstop") + ", ");
            System.out.print("pstart = " + result.getProperty("pstart") + ", ");
            System.out.print("pstop = " + result.getProperty("pstop") + ", ");
            System.out.print("status = " + result.getProperty("status") + ", ");
            System.out.println("node = " + result.getProperty("node"));
          }
          System.out.println();
        }
        database.commit();
      } catch (Exception e) {
        System.out.println(e.getMessage());
        database.rollback();
      }
      System.out.println("Test 6: select with @rid < #3:1 (as RID object) using prepared statement");
      sqlString = "SELECT from Order WHERE @rid < ? ORDER BY @rid DESC LIMIT 10";
      parameter = new RID(database, "#3:1");
      System.out.println("sqlString = " + sqlString);
      try {
        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
          System.out.print("result: ");
          while (resultSet.hasNext()) {
            result = resultSet.next();
            System.out.print("@rid = " + result.getIdentity().get() + ", ");
            System.out.print("processor = " + result.getProperty("processor") + ", ");
            System.out.print("vstart = " + result.getProperty("vstart") + ", ");
            System.out.print("vstop = " + result.getProperty("vstop") + ", ");
            System.out.print("pstart = " + result.getProperty("pstart") + ", ");
            System.out.print("pstop = " + result.getProperty("pstop") + ", ");
            System.out.print("status = " + result.getProperty("status") + ", ");
            System.out.println("node = " + result.getProperty("node"));
          }
          System.out.println();
        }
        database.commit();
      } catch (Exception e) {
        System.out.println(e.getMessage());
        database.rollback();
      }
      arcadeDBServer.stop();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }
}
