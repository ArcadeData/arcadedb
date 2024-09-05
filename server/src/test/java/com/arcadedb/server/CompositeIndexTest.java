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
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.integration.misc.IntegrationUtils;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.time.*;
import java.time.format.*;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

/**
 * From Issue https://github.com/ArcadeData/arcadedb/issues/741
 */
public class CompositeIndexTest {
  static final String DATABASE_NAME = "SelectOrderTest";

  @Test
  public void testWhereAfterUpdate() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    try (DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/" + DATABASE_NAME)) {
      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtOrders = db.getSchema().createDocumentType("Order");
          dtOrders.createProperty("id", Type.INTEGER);
          dtOrders.createProperty("processor", Type.STRING);
          dtOrders.createProperty("vstart", Type.DATETIME_MICROS);
          dtOrders.createProperty("vstop", Type.DATETIME_MICROS);
          dtOrders.createProperty("status", Type.STRING);
          dtOrders.createProperty("node", Type.STRING);
          dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
          dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status", "id");
          dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "processor", "vstart", "vstop");
          dtOrders.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
        });
      }
    }
    String dateTimePattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS";
    ContextConfiguration configuration = new ContextConfiguration();
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(java.time.LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue(dateTimePattern);
    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(configuration);
    arcadeDBServer.start();
    Database database = arcadeDBServer.getDatabase(DATABASE_NAME);
    String sqlString = "UPDATE Order SET id = ?, processor = ?, vstart = ?, vstop = ?, status = ? UPSERT RETURN AFTER WHERE processor = ? AND vstart = ? AND vstop = ?";
    LocalDateTime vstart, vstop;
    String processor = "SIR1LRM-7.1";
    String status;
    status = "PENDING";
    try {
      // insert 2 orders
      database.begin();
      vstart = LocalDateTime.parse("2019-05-05T00:06:04.069841", DateTimeFormatter.ofPattern(dateTimePattern));
      vstop = LocalDateTime.parse("2019-05-05T00:07:57.423797", DateTimeFormatter.ofPattern(dateTimePattern));
      try (ResultSet resultSet = database.command("sql", sqlString, 1, processor, vstart, vstop, status, processor, vstart,
          vstop)) {
        Assertions.assertTrue(resultSet.hasNext());
        resultSet.next().toJSON();
      }
      vstart = LocalDateTime.parse("2019-05-05T00:10:37.288211", DateTimeFormatter.ofPattern(dateTimePattern));
      vstop = LocalDateTime.parse("2019-05-05T00:12:38.236835", DateTimeFormatter.ofPattern(dateTimePattern));
      try (ResultSet resultSet = database.command("sql", sqlString, 2, processor, vstart, vstop, status, processor, vstart,
          vstop)) {
        Assertions.assertTrue(resultSet.hasNext());
        resultSet.next().toJSON();
      }
      database.commit();
      // update one order
      sqlString = "UPDATE Order SET status = ? RETURN AFTER WHERE id = ?";
      database.begin();
      status = "COMPLETED";
      try (ResultSet resultSet = database.command("sql", sqlString, status, 1)) {
        resultSet.next().toJSON();
      }
      database.commit();
      // select orders
      sqlString = "SELECT FROM Order WHERE status = ?";
      try (ResultSet resultSet = database.query("sql", sqlString, "COMPLETED")) {
        Assertions.assertTrue(resultSet.hasNext());
        resultSet.next().toJSON();
      }
      try (ResultSet resultSet = database.query("sql", "SELECT FROM Order WHERE id = 2")) {
        Assertions.assertTrue(resultSet.hasNext());
        Result result = resultSet.next();
        Assertions.assertTrue(result.getProperty("status").equals("PENDING"));
        result.toJSON();
      }
      try (ResultSet resultSet = database.query("sql", sqlString, "PENDING")) {
        Assertions.assertTrue(resultSet.hasNext());
        resultSet.next().toJSON();
      }
    } catch (Exception e) {
      e.printStackTrace();
      if (database.isTransactionActive()) {
        database.rollback();
      }
    }
    arcadeDBServer.stop();
  }

  @BeforeEach
  public void beginTests() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);
    FileUtils.deleteRecursively(new File(rootPath + "/databases"));

    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);
  }

  @AfterEach
  public void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
