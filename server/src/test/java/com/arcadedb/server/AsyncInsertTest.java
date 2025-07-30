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
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
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

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * From Issue https://github.com/ArcadeData/arcadedb/issues/1126
 */
public class AsyncInsertTest {
  private static ArcadeDBServer arcadeDBServer;
  static final   String         DATABASE_NAME = "AsyncInsertTest";

  @Test
  public void testBulkAsyncInsertConflict() {
    final int CONCURRENCY_LEVEL = 24;
    ContextConfiguration configuration = new ContextConfiguration();
    GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(CONCURRENCY_LEVEL);
    arcadeDBServer = new ArcadeDBServer(configuration);
    arcadeDBServer.start();

    Database database = arcadeDBServer.getDatabase(DATABASE_NAME);
    database.async().setParallelLevel(CONCURRENCY_LEVEL);

    database.transaction(() -> {
      DocumentType dtProducts = database.getSchema().buildDocumentType().withName("Product").withTotalBuckets(8).create();
      dtProducts.createProperty("name", Type.STRING);
      dtProducts.createTypeIndex(Schema.IndexType.LSM_TREE, true, "name");
      dtProducts.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
    });

    final AtomicLong okCount = new AtomicLong();
    final AtomicLong errCount = new AtomicLong();
    final String sqlString = "UPDATE Product SET name = ? UPSERT WHERE name = ?";
    String name;
    final long N = 20000;

    database.async().onError(exception -> errCount.incrementAndGet());

    assertThat(database.getSchema().getType("Product").getBuckets(false).size()).isNotEqualTo(database.async().getParallelLevel());
    for (int i = 0; i < N; i++) {
      name = UUID.randomUUID().toString();
      database.async().command("sql", sqlString, new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet resultset) {
          okCount.incrementAndGet();
        }

        @Override
        public void onError(final Exception exception) {
          errCount.incrementAndGet();
        }
      }, name, name);
    }

    assertThat(database.async().waitCompletion(3000)).isTrue();

    assertThat(okCount.get()).isEqualTo(N);
    assertThat(errCount.get()).isNotEqualTo(0);

    try (ResultSet resultSet = database.query("sql", "SELECT count(*) as total FROM Product")) {
      Result result = resultSet.next();
      assertThat((Long) result.getProperty("total")).isNotEqualTo(N);
    }
  }

  @Test
  public void testBulkAsyncInsertOk() {
    ContextConfiguration configuration = new ContextConfiguration();
    GlobalConfiguration.TYPE_DEFAULT_BUCKETS.setValue(4);
    GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(4);
    arcadeDBServer = new ArcadeDBServer(configuration);
    arcadeDBServer.start();
    Database database = arcadeDBServer.getDatabase(DATABASE_NAME);

    final AtomicLong okCount = new AtomicLong();
    final AtomicLong errCount = new AtomicLong();
    final String sqlString = "UPDATE Product SET name = ? UPSERT WHERE name = ?";
    String name;
    final long N = 20000;

    database.transaction(() -> {
      DocumentType dtProducts = database.getSchema().buildDocumentType().withName("Product").withTotalBuckets(4).create();
      dtProducts.createProperty("name", Type.STRING);
      dtProducts.createTypeIndex(Schema.IndexType.LSM_TREE, true, "name");
      dtProducts.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
    });

    database.async().setParallelLevel(4);
    database.async().onError(exception -> errCount.incrementAndGet());

    assertThat(database.getSchema().getType("Product").getBuckets(false).size()).isEqualTo(database.async().getParallelLevel());
    for (int i = 0; i < N; i++) {
      name = UUID.randomUUID().toString();
      database.async().command("sql", sqlString, new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet resultset) {
          okCount.incrementAndGet();
        }

        @Override
        public void onError(final Exception exception) {
          errCount.incrementAndGet();
        }
      }, name, name);
    }

    assertThat(database.async().waitCompletion(3000)).isTrue();

    assertThat(okCount.get()).isEqualTo(N);
    assertThat(errCount.get()).isEqualTo(0);

    try (ResultSet resultSet = database.query("sql", "SELECT count(*) as total FROM Product")) {
      Result result = resultSet.next();
      assertThat((Long) result.getProperty("total")).isEqualTo(N);
    }
  }

  @BeforeEach
  public void beginTests() {
    try (DatabaseFactory databaseFactory = new DatabaseFactory("./databases/" + DATABASE_NAME)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();
    }

    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);
    FileUtils.deleteRecursively(new File(rootPath + "/databases"));

    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);

    try (DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/" + DATABASE_NAME)) {
      try (Database db = databaseFactory.create()) {
      }
    }
  }

  @AfterEach
  public void endTests() {
    arcadeDBServer.stop();
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
