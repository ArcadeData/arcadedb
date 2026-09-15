/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
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

import java.io.File;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * From Issue https://github.com/ArcadeData/arcadedb/issues/1126
 */
class AsyncInsertTest {
  private static ArcadeDBServer arcadeDBServer;
  static final   String         DATABASE_NAME = "AsyncInsertTest";

  /**
   * More async workers ({@code CONCURRENCY_LEVEL}) than buckets forces several worker threads to share the same
   * bucket/index pages, so their periodic {@code commitEvery} boundary commits genuinely collide with
   * {@code ConcurrentModificationException} - the exact shape of issue #7615. Before that fix this asserted the bug's
   * own symptom as the expected outcome ({@code errCount != 0}, stored count {@code != N}): a batch that lost the
   * race was rolled back silently, with nothing telling the submitters of the discarded writes. Now that boundary
   * commit retries transparently by replaying the batch, so every one of the N UPSERTs (each on its own random UUID,
   * never a genuine unique-index collision) ends up stored with no error surfacing at all - same as
   * {@link #bulkAsyncInsertOk}, just reached under contention instead of by construction.
   */
  @Test
  void bulkAsyncInsertConflict() {
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
      dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
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
    // #7615: a periodic-boundary conflict is now retried transparently instead of silently discarding the batch,
    // so it must not surface as an error here either.
    assertThat(errCount.get()).isEqualTo(0);

    try (ResultSet resultSet = database.query("sql", "SELECT count(*) as total FROM Product")) {
      Result result = resultSet.next();
      // #7615: every one of the N UPSERTs must be durably stored - none may be lost to a retried conflict.
      assertThat((Long) result.getProperty("total")).isEqualTo(N);
    }
  }

  @Test
  void bulkAsyncInsertOk() {
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
      dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
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
  void beginTests() {
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");

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
  void endTests() {
    arcadeDBServer.stop();
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
