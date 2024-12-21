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
package com.arcadedb;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class AsyncTest extends TestHelper {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";

  @Test
  public void testScan() {
    database.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();

      database.async().scanType(TYPE_NAME, true, record -> {
        callbackInvoked.incrementAndGet();
        return true;
      });

      assertThat(callbackInvoked.get()).isEqualTo(TOT);

      database.async().waitCompletion();
      database.async().waitCompletion();
      database.async().waitCompletion();

    } finally {
      database.commit();
    }
  }

  @Test
  public void testSyncScanAndAsyncUpdate() {
    final AtomicLong callbackInvoked = new AtomicLong();
    final AtomicLong updatedRecords = new AtomicLong();

    database.begin();
    try {

      database.scanType(TYPE_NAME, true, record -> {
        callbackInvoked.incrementAndGet();
        record.modify().set("updated", true).save();
        updatedRecords.incrementAndGet();
        return true;
      });

      database.async().waitCompletion();

    } finally {
      database.commit();
    }

    assertThat(callbackInvoked.get()).isEqualTo(TOT);
    assertThat(updatedRecords.get()).isEqualTo(TOT);

    ResultSet resultSet = database.query("sql", "select from " + TYPE_NAME + " where updated <> true");
    assertThat(resultSet.hasNext()).isFalse();

    resultSet = database.query("sql", "select count(*) as count from " + TYPE_NAME + " where updated = true");

    assertThat(resultSet.hasNext()).isTrue();
    assertThat(((Number) resultSet.next().getProperty("count")).intValue()).isEqualTo(TOT);
  }

  @Test
  public void testAsyncDelete() {
    database.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();
      final AtomicLong deletedRecords = new AtomicLong();

      database.scanType(TYPE_NAME, true, record -> {
        callbackInvoked.incrementAndGet();
        database.async().deleteRecord(record.modify().set("updated", true), newRecord -> deletedRecords.incrementAndGet());
        return true;
      });

      database.async().waitCompletion();

      assertThat(callbackInvoked.get()).isEqualTo(TOT);
      assertThat(deletedRecords.get()).isEqualTo(TOT);

      final ResultSet resultSet = database.query("sql", "select count(*) as count from " + TYPE_NAME + " where updated = true");

      assertThat(resultSet.hasNext()).isTrue();
      assertThat(((Number) resultSet.next().getProperty("count")).intValue()).isEqualTo(0);

      populateDatabase();

    } finally {
      database.commit();
    }
  }

  @Test
  public void testScanInterrupt() {
    database.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();

      database.async().scanType(TYPE_NAME, true, record -> {
        if (callbackInvoked.get() > 9)
          return false;

        return callbackInvoked.getAndIncrement() < 10;
      });

      assertThat(callbackInvoked.get() < 20).isTrue();

    } finally {
      database.commit();
    }
  }

  @Test
  public void testQueryFetch() {
    database.begin();
    try {
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().query("sql", "select from " + TYPE_NAME, new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet resultset) {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(final Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      });

      database.async().waitCompletion(5_000);

      assertThat(completeCallbackInvoked.get()).isEqualTo(1);
      assertThat(errorCallbackInvoked.get()).isEqualTo(0);

    } finally {
      database.commit();
    }
  }

  @Test
  public void testParallelQueries() throws InterruptedException {
    database.begin();
    try {
      CountDownLatch counter = new CountDownLatch(3);

      final ResultSet[] resultSets = new ResultSet[3];
      database.async().query("sql", "select from " + TYPE_NAME, resultset -> {
        resultSets[0] = resultset;
        counter.countDown();
      });
      database.async().query("sql", "select from " + TYPE_NAME, resultset -> {
        resultSets[1] = resultset;
        counter.countDown();
      });
      database.async().query("sql", "select from " + TYPE_NAME, resultset -> {
        resultSets[2] = resultset;
        counter.countDown();
      });

      // WAIT INDEFINITELY
      counter.await();

      assertThat(resultSets[0].hasNext()).isTrue();
      assertThat(resultSets[1].hasNext()).isTrue();
      assertThat(resultSets[2].hasNext()).isTrue();

    } finally {
      database.commit();
    }
  }

  @Test
  public void testCommandFetch() {
    database.begin();
    try {
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().command("sql", "select from " + TYPE_NAME, new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet resultset) {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(final Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      });

      database.async().waitCompletion(5_000);

      assertThat(completeCallbackInvoked.get()).isEqualTo(1);
      assertThat(errorCallbackInvoked.get()).isEqualTo(0);

    } finally {
      database.commit();
    }
  }

  @Test
  public void testCommandFetchVarargParamsNoCallback() {
    database.begin();
    try {
      database.async().command("sql", "insert into " + TYPE_NAME + " set id = :id", null, Integer.MAX_VALUE);

      database.async().waitCompletion(5_000);

      final IndexCursor resultSet = database.lookupByKey(TYPE_NAME, "id", Integer.MAX_VALUE);

      assertThat(resultSet.hasNext()).isTrue();
      final Document record = resultSet.next().asDocument();
      assertThat(record.get("id")).isEqualTo(Integer.MAX_VALUE);

    } finally {
      database.commit();
    }
  }

  @Test
  public void testCommandFetchVarargParamsCallback() {
    database.begin();
    try {
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().command("sql", "select from " + TYPE_NAME + " where id = ?", new AsyncResultsetCallback() {

        @Override
        public void onComplete(final ResultSet resultset) {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(final Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      }, 0);

      database.async().waitCompletion(5_000);

      assertThat(completeCallbackInvoked.get()).isEqualTo(1);
      assertThat(errorCallbackInvoked.get()).isEqualTo(0);

    } finally {
      database.commit();
    }
  }

  @Test
  public void testCommandFetchParamsMap() {
    database.begin();
    try {
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().command("sql", "select from " + TYPE_NAME + " where id = :id", new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet resultset) {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(final Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      }, Map.of("id", 0));

      database.async().waitCompletion(5_000);

      assertThat(completeCallbackInvoked.get()).isEqualTo(1);
      assertThat(errorCallbackInvoked.get()).isEqualTo(0);

    } finally {
      database.commit();
    }
  }

  @Test
  public void testCommandFetchError() {
    database.begin();
    try {
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().command("sql", "select from DSdededde", new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet resultset) {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(final Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      });

      database.async().waitCompletion(5_000);

      assertThat(completeCallbackInvoked.get()).isEqualTo(0);
      assertThat(errorCallbackInvoked.get()).isEqualTo(1);

    } finally {
      database.commit();
    }
  }

  @Override
  protected void beginTest() {
    database.begin();

    assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

    final AtomicLong okCallbackInvoked = new AtomicLong();

    database.async().setCommitEvery(5000);
    database.async().setParallelLevel(3);
    database.async().onOk(() -> okCallbackInvoked.incrementAndGet());

    database.async().onError(exception -> fail("Error on creating async record", exception));

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    type.createProperty("surname", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, 20000);

    database.commit();

    populateDatabase();

    assertThat(okCallbackInvoked.get() > 0).isTrue();
  }

  private void populateDatabase() {
    for (int i = 0; i < TOT; ++i) {
      final MutableDocument v = database.newDocument(TYPE_NAME);
      v.set("id", i);
      v.set("name", "Jay");
      v.set("surname", "Miner");

      database.async().createRecord(v, null);
    }

    database.async().waitCompletion();
  }
}
