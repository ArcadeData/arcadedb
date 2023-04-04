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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.integration.misc.IntegrationUtils;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.format.*;
import java.util.*;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteQueriesIT {

  @Test
  public void testWhereEqualsAfterUpdate() {
    // create database
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    GlobalConfiguration.TX_RETRIES.setValue(0);
    final TypeIndex[] typeIndex = new TypeIndex[1];

    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/remotequeries");
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (Database db = databaseFactory.create()) {
      db.transaction(() -> {
        DocumentType dtOrders = db.getSchema().createDocumentType("Order");
        dtOrders.createProperty("id", Type.STRING);
        dtOrders.createProperty("processor", Type.STRING);
        dtOrders.createProperty("status", Type.STRING);
        typeIndex[0] = dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status", "id");
      });
    }

    serverConfiguration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(serverConfiguration);
    arcadeDBServer.start();

    Database database = arcadeDBServer.getDatabase("remotequeries");
    try {
      // insert 2 records
      String processor = "SIR1LRM-7.1";
      String status = "PENDING";
      for (int i = 0; i < 2; i++) {
        int id = i + 1;
        database.transaction(() -> {
          String sqlString = "INSERT INTO Order SET id = ?, status = ?, processor = ?";
          System.out.print(sqlString);
          System.out.println("; parameters: " + id + ", " + processor + ", " + status);
          try (ResultSet resultSet1 = database.command("sql", sqlString, id, status, processor)) {
            Assertions.assertEquals("" + id, resultSet1.next().getProperty("id"));
          }
        });
      }
      // update first record
      database.transaction(() -> {
        Object[] parameters2 = { "ERROR", 1 };
        String sqlString = "UPDATE Order SET status = ? RETURN AFTER WHERE id = ?";
        System.out.print(sqlString);
        System.out.println(", parameters: " + Arrays.toString(parameters2));
        try (ResultSet resultSet1 = database.command("sql", sqlString, parameters2)) {
          Assertions.assertEquals("1", resultSet1.next().getProperty("id"));
        } catch (Exception e) {
          System.out.println(e.getMessage());
          e.printStackTrace();
        }
      });

      //database.command("sql", "rebuild index `" + typeIndex[0].getName() + "`");

      // select records with status = 'PENDING'
      database.transaction(() -> {
        Object[] parameters2 = { "PENDING" };
        String sqlString = "SELECT id, processor, status FROM Order WHERE status = ?";
        System.out.print(sqlString);
        System.out.println(", parameters: " + Arrays.toString(parameters2));
        try (ResultSet resultSet1 = database.query("sql", sqlString, parameters2)) {
          Assertions.assertEquals("PENDING", resultSet1.next().getProperty("status"));
        } catch (Exception e) {
          System.out.println(e.getMessage());
          e.printStackTrace();
        }
      });
      // drop index
      database.getSchema().dropIndex(typeIndex[0].getName());

      System.out.println("index dropped");
      // repeat select records with status = 'PENDING'
      database.transaction(() -> {
        Object[] parameters2 = { "PENDING" };
        String sqlString = "SELECT id, processor, status FROM Order WHERE status = ?";
        System.out.print(sqlString);
        System.out.println(", parameters: " + Arrays.toString(parameters2));
        try (ResultSet resultSet1 = database.query("sql", sqlString, parameters2)) {
          Assertions.assertEquals("PENDING", resultSet1.next().getProperty("status"));
        } catch (Exception e) {
          System.out.println(e.getMessage());
          e.printStackTrace();
        }
      });

    } finally {
      arcadeDBServer.stop();

      if (databaseFactory.exists())
        databaseFactory.open().drop();
    }
  }

  @Test
  public void testLocalDateTimeOrderBy() {
    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/testLocalDateTimeOrderBy")) {
      if (databaseFactory.exists()) {
        databaseFactory.open().drop();
      }
      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtProduct = db.getSchema().createDocumentType("Product");
          dtProduct.createProperty("name", Type.STRING);
          dtProduct.createProperty("type", Type.STRING);
          dtProduct.createProperty("start", Type.DATETIME_MICROS);
          dtProduct.createProperty("stop", Type.DATETIME_MICROS);
          dtProduct.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
          dtProduct.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "type", "start", "stop");
        });
      }
    }
    ContextConfiguration configuration = new ContextConfiguration();
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(java.time.LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    Assertions.assertTrue(configuration.getValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION) == java.time.LocalDateTime.class);
    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(configuration);
    arcadeDBServer.start();
    Database database = arcadeDBServer.getDatabase("testLocalDateTimeOrderBy");
    String name, type;
    LocalDateTime start, stop;
    DateTimeFormatter FILENAME_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
    Result result;
    String sqlString = "INSERT INTO Product SET name = ?, type = ?, start = ?, stop = ?";

    name = "CS_OPER_AUX_ORBDOR_20220318T215523_20220320T002323_F001.EEF";
    type = "AUX_ORBDOR";
    start = LocalDateTime.parse("20220318T215523", FILENAME_TIME_FORMAT);
    stop = LocalDateTime.parse("20220320T002323", FILENAME_TIME_FORMAT);
    Object[] parameters1 = { name, type, start, stop };
    try (ResultSet resultSet = database.command("sql", sqlString, parameters1)) {
      Assertions.assertTrue(resultSet.hasNext());
      result = resultSet.next();
      Assertions.assertTrue(result.getProperty("start").equals(start), "start value retrieved does not match start value inserted");
    } catch (Exception e) {
      e.printStackTrace();
    }
    sqlString = "SELECT name, start, stop FROM Product WHERE type = ? AND start <= ? AND stop >= ? ORDER BY start DESC";
    type = "AUX_ORBDOR";
    start = LocalDateTime.parse("2022-03-19T00:26:24.404379", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
    stop = LocalDateTime.parse("2022-03-19T00:28:26.525650", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
    Object[] parameters2 = { type, start, stop };
    try (ResultSet resultSet = database.query("sql", sqlString, parameters2)) {
      Assertions.assertTrue(resultSet.hasNext());
      while (resultSet.hasNext()) {
        result = resultSet.next();
        //Assertions.assertTrue(result.getProperty("start").equals(start), "start value retrieved does not match start value inserted");
      }
    }
    arcadeDBServer.stop();
  }

  @AfterAll
  public static void afterAll() {
    GlobalConfiguration.resetAll();
  }
}
