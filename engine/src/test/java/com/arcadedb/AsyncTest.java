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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;

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

      Assertions.assertEquals(TOT, callbackInvoked.get());

      database.async().waitCompletion();
      database.async().waitCompletion();
      database.async().waitCompletion();

    } finally {
      database.commit();
    }
  }

  @Test
  public void testSyncScanAndAsyncUpdate() {
    database.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();
      final AtomicLong updatedRecords = new AtomicLong();

      database.scanType(TYPE_NAME, true, record -> {
        callbackInvoked.incrementAndGet();
        database.async().updateRecord(record.modify().set("updated", true), newRecord -> updatedRecords.incrementAndGet());
        return true;
      });

      database.async().waitCompletion();

      Assertions.assertEquals(TOT, callbackInvoked.get());
      Assertions.assertEquals(TOT, updatedRecords.get());

      final ResultSet resultSet = database.query("sql", "select count(*) as count from " + TYPE_NAME + " where updated = true");

      Assertions.assertTrue(resultSet.hasNext());
      Assertions.assertEquals(TOT, ((Number) resultSet.next().getProperty("count")).intValue());

    } finally {
      database.commit();
    }
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

      Assertions.assertEquals(TOT, callbackInvoked.get());
      Assertions.assertEquals(TOT, deletedRecords.get());

      final ResultSet resultSet = database.query("sql", "select count(*) as count from " + TYPE_NAME + " where updated = true");

      Assertions.assertTrue(resultSet.hasNext());
      Assertions.assertEquals(0, ((Number) resultSet.next().getProperty("count")).intValue());

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

      Assertions.assertTrue(callbackInvoked.get() < 20);

    } finally {
      database.commit();
    }
  }

  @Test
  public void testQueryFetch() {
    database.begin();
    try {
      final AtomicLong startCallbackInvoked = new AtomicLong();
      final AtomicLong nextCallbackInvoked = new AtomicLong();
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().query("sql", "select from " + TYPE_NAME, new AsyncResultsetCallback() {
        @Override
        public void onStart(ResultSet resultset) {
          startCallbackInvoked.incrementAndGet();
        }

        @Override
        public boolean onNext(Result result) {
          nextCallbackInvoked.incrementAndGet();
          return true;
        }

        @Override
        public void onComplete() {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      });

      database.async().waitCompletion(5_000);

      Assertions.assertEquals(1, startCallbackInvoked.get());
      Assertions.assertEquals(database.countType(TYPE_NAME, true), nextCallbackInvoked.get());
      Assertions.assertEquals(1, completeCallbackInvoked.get());
      Assertions.assertEquals(0, errorCallbackInvoked.get());

    } finally {
      database.commit();
    }
  }

  @Test
  public void testCommandFetch() {
    database.begin();
    try {
      final AtomicLong startCallbackInvoked = new AtomicLong();
      final AtomicLong nextCallbackInvoked = new AtomicLong();
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().command("sql", "select from " + TYPE_NAME, new AsyncResultsetCallback() {
        @Override
        public void onStart(ResultSet resultset) {
          startCallbackInvoked.incrementAndGet();
        }

        @Override
        public boolean onNext(Result result) {
          nextCallbackInvoked.incrementAndGet();
          return true;
        }

        @Override
        public void onComplete() {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      });

      database.async().waitCompletion(5_000);

      Assertions.assertEquals(1, startCallbackInvoked.get());
      Assertions.assertEquals(database.countType(TYPE_NAME, true), nextCallbackInvoked.get());
      Assertions.assertEquals(1, completeCallbackInvoked.get());
      Assertions.assertEquals(0, errorCallbackInvoked.get());

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

      Assertions.assertTrue(resultSet.hasNext());
      final Document record = resultSet.next().asDocument();
      Assertions.assertEquals(Integer.MAX_VALUE, record.get("id"));

    } finally {
      database.commit();
    }
  }

  @Test
  public void testCommandFetchVarargParamsCallback() {
    database.begin();
    try {
      final AtomicLong startCallbackInvoked = new AtomicLong();
      final AtomicLong nextCallbackInvoked = new AtomicLong();
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().command("sql", "select from " + TYPE_NAME + " where id = ?", new AsyncResultsetCallback() {
        @Override
        public void onStart(ResultSet resultset) {
          startCallbackInvoked.incrementAndGet();
        }

        @Override
        public boolean onNext(Result result) {
          nextCallbackInvoked.incrementAndGet();
          return true;
        }

        @Override
        public void onComplete() {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      }, 0);

      database.async().waitCompletion(5_000);

      Assertions.assertEquals(1, startCallbackInvoked.get());
      Assertions.assertEquals(1, nextCallbackInvoked.get());
      Assertions.assertEquals(1, completeCallbackInvoked.get());
      Assertions.assertEquals(0, errorCallbackInvoked.get());

    } finally {
      database.commit();
    }
  }

  @Test
  public void testCommandFetchParamsMap() {
    database.begin();
    try {
      final AtomicLong startCallbackInvoked = new AtomicLong();
      final AtomicLong nextCallbackInvoked = new AtomicLong();
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().command("sql", "select from " + TYPE_NAME + " where id = :id", new AsyncResultsetCallback() {
        @Override
        public void onStart(ResultSet resultset) {
          startCallbackInvoked.incrementAndGet();
        }

        @Override
        public boolean onNext(Result result) {
          nextCallbackInvoked.incrementAndGet();
          return true;
        }

        @Override
        public void onComplete() {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      }, Map.of("id", 0));

      database.async().waitCompletion(5_000);

      Assertions.assertEquals(1, startCallbackInvoked.get());
      Assertions.assertEquals(1, nextCallbackInvoked.get());
      Assertions.assertEquals(1, completeCallbackInvoked.get());
      Assertions.assertEquals(0, errorCallbackInvoked.get());

    } finally {
      database.commit();
    }
  }

  @Test
  public void testCommandFetchStop() {
    database.begin();
    try {
      final AtomicLong startCallbackInvoked = new AtomicLong();
      final AtomicLong nextCallbackInvoked = new AtomicLong();
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().command("sql", "select from " + TYPE_NAME, new AsyncResultsetCallback() {
        @Override
        public void onStart(ResultSet resultset) {
          startCallbackInvoked.incrementAndGet();
        }

        @Override
        public boolean onNext(Result result) {
          return nextCallbackInvoked.incrementAndGet() < 3;
        }

        @Override
        public void onComplete() {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      });

      database.async().waitCompletion(5_000);

      Assertions.assertEquals(1, startCallbackInvoked.get());
      Assertions.assertEquals(3, nextCallbackInvoked.get());
      Assertions.assertEquals(0, completeCallbackInvoked.get());
      Assertions.assertEquals(0, errorCallbackInvoked.get());

    } finally {
      database.commit();
    }
  }

  @Test
  public void testCommandFetchError() {
    database.begin();
    try {
      final AtomicLong startCallbackInvoked = new AtomicLong();
      final AtomicLong nextCallbackInvoked = new AtomicLong();
      final AtomicLong completeCallbackInvoked = new AtomicLong();
      final AtomicLong errorCallbackInvoked = new AtomicLong();

      database.async().command("sql", "select from DSdededde", new AsyncResultsetCallback() {
        @Override
        public void onStart(ResultSet resultset) {
          startCallbackInvoked.incrementAndGet();
        }

        @Override
        public boolean onNext(Result result) {
          return nextCallbackInvoked.incrementAndGet() < 3;
        }

        @Override
        public void onComplete() {
          completeCallbackInvoked.incrementAndGet();
        }

        @Override
        public void onError(Exception exception) {
          errorCallbackInvoked.incrementAndGet();
        }
      });

      database.async().waitCompletion(5_000);

      Assertions.assertEquals(0, startCallbackInvoked.get());
      Assertions.assertEquals(0, nextCallbackInvoked.get());
      Assertions.assertEquals(0, completeCallbackInvoked.get());
      Assertions.assertEquals(1, errorCallbackInvoked.get());

    } finally {
      database.commit();
    }
  }

  @Override
  protected void beginTest() {
    database.begin();

    Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

    final AtomicLong okCallbackInvoked = new AtomicLong();

    database.async().setCommitEvery(5000);
    database.async().setParallelLevel(3);
    database.async().onOk(() -> okCallbackInvoked.incrementAndGet());

    database.async().onError(exception -> Assertions.fail("Error on creating async record", exception));

    final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    type.createProperty("surname", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, 20000);

    database.commit();

    populateDatabase();

    Assertions.assertTrue(okCallbackInvoked.get() > 0);
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
