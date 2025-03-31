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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.misc.IntegrationUtils;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * From Issue https://github.com/ArcadeData/arcadedb/issues/741
 */
public class SelectOrderTest {
  static final String DATABASE_NAME = "SelectOrderTest";

  @Test
  public void testRIDOrdering() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    try (DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/" + DATABASE_NAME)) {
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
          dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "status");
          dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "processor", "vstart", "vstop");
        });
      }

      ArcadeDBServer arcadeDBServer = new ArcadeDBServer(serverConfiguration);
      arcadeDBServer.start();
      Database database = arcadeDBServer.getDatabase(DATABASE_NAME);

      try {
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

        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
          while (resultSet.hasNext()) {
            assertThat(resultSet.next().getIdentity().get()).isEqualTo(rids[0]);
          }
        }
        database.commit();

        parameter = rids[0].toString();
        sqlString = "SELECT from Order WHERE @rid > ? ORDER BY @rid DESC LIMIT 10";

        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
          while (resultSet.hasNext()) {
            assertThat(resultSet.next().getIdentity().get()).isEqualTo(rids[1]);
          }
        }
        database.commit();

        parameter = rids[0].toString();
        sqlString = "SELECT from Order WHERE @rid = ? ORDER BY @rid DESC LIMIT 10";

        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
          while (resultSet.hasNext()) {
            assertThat(resultSet.next().getIdentity().get()).isEqualTo(rids[0]);
          }
        }
        database.commit();

        sqlString = "SELECT from Order WHERE @rid < #3:1 ORDER BY @rid DESC LIMIT 10";

        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString)) {
          while (resultSet.hasNext()) {
            assertThat(resultSet.next().getIdentity().get()).isEqualTo(rids[0]);
          }
        }
        database.commit();

        sqlString = "SELECT from Order WHERE @rid = '#3:0' ORDER BY @rid DESC LIMIT 10";

        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString)) {
          while (resultSet.hasNext()) {
            assertThat(resultSet.next().getIdentity().get()).isEqualTo(rids[0]);
          }
        }
        database.commit();

        sqlString = "SELECT from Order WHERE @rid < ? ORDER BY @rid DESC LIMIT 10";
        parameter = new RID(database, "#3:1");

        database.begin();
        try (ResultSet resultSet = database.query("sql", sqlString, parameter)) {
          while (resultSet.hasNext()) {
            assertThat(resultSet.next().getIdentity().get()).isEqualTo(rids[0]);
          }
        }
        database.commit();

      } finally {
        arcadeDBServer.stop();
      }
    }
  }

  @Test
  public void testLocalDateTimeOrderBy() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    try (DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/" + DATABASE_NAME)) {
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

      GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

      ArcadeDBServer arcadeDBServer = new ArcadeDBServer(serverConfiguration);
      arcadeDBServer.start();
      Database database = arcadeDBServer.getDatabase(DATABASE_NAME);

      try {
        database.transaction(() -> {
          String name;
          String type;
          LocalDateTime start;
          LocalDateTime stop;
          DateTimeFormatter FILENAME_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
          System.out.println();
          Result result;
          String sqlString = "INSERT INTO Product SET name = ?, type = ?, start = ?, stop = ?";

          try {
            name = "CS_OPER_AUX_ORBDOR_20220318T215523_20220320T002323_F001.EEF";
            type = "AUX_ORBDOR";
            start = LocalDateTime.parse("20220318T215523", FILENAME_TIME_FORMAT);
            stop = LocalDateTime.parse("20220320T002323", FILENAME_TIME_FORMAT);
            Object[] parameters1 = { name, type, start, stop };
            try (ResultSet resultSet = database.command("sql", sqlString, parameters1)) {

              assertThat(resultSet.hasNext()).isTrue();
              result = resultSet.next();
            } catch (Exception e) {
              System.out.println(e.getMessage());
              e.printStackTrace();
            }

            sqlString = "SELECT name, start, stop FROM Product WHERE type = ? AND start <= ? AND stop >= ? ORDER BY start DESC";
            type = "AUX_ORBDOR";
            start = LocalDateTime.parse("2022-03-19T00:26:24.404379", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
            stop = LocalDateTime.parse("2022-03-19T00:28:26.525650", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
            Object[] parameters3 = { type, start, stop };

            LocalDateTime lastStart = null;

            try (ResultSet resultSet = database.query("sql", sqlString, parameters3)) {

              assertThat(resultSet.hasNext()).isTrue();

              while (resultSet.hasNext()) {
                result = resultSet.next();

                start = result.getProperty("start");

                if (lastStart != null)
                  assertThat(start.compareTo(lastStart)).isLessThanOrEqualTo(0)
                      .withFailMessage("" + start + " is greater than " + lastStart);

                lastStart = start;
              }
            }

            lastStart = null;

            sqlString = "SELECT name, start, stop FROM Product WHERE type = ? AND start <= ? AND stop >= ? ORDER BY start ASC";
            try (ResultSet resultSet = database.query("sql", sqlString, parameters3)) {
              assertThat(resultSet.hasNext()).isTrue();

              while (resultSet.hasNext()) {
                result = resultSet.next();

                start = result.getProperty("start");

                if (lastStart != null)
                  assertThat(start.compareTo(lastStart)).isGreaterThanOrEqualTo(0)
                      .withFailMessage("" + start + " is smaller than " + lastStart);

                lastStart = start;
              }
            }
          } catch (Exception e) {
            System.out.println("Error");
            e.printStackTrace();
          }
        });

      } finally {
        arcadeDBServer.stop();
      }
    }
  }

  @Test
  public void testRIDOrderingDesc() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    try (DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/" + DATABASE_NAME)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();

      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtProduct = db.getSchema().createVertexType("Product", 1);
          dtProduct.createProperty("name", Type.STRING);
        });

        db.transaction(() -> {
          for (int i = 0; i < 1_000; i++) {
            final MutableVertex v = db.newVertex("Product");
            v.set("id", i);
            v.save();
          }

          for (int i = 0; i < 10; i++) {
            int last = Integer.MAX_VALUE;
            int total = 0;
            final ResultSet resultset = db.query("sql", "select from Product order by @rid desc");
            while (resultset.hasNext()) {
              final Vertex v = resultset.next().getVertex().get();
              final Integer id = v.getInteger("id");
              assertThat(id).isLessThan(last);

              last = id;
              ++total;
            }
            assertThat(total).isEqualTo(1_000);
          }
        });
      }
    }
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
