/*
 * Copyright 2021 Arcade Data Ltd
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

package com.arcadedb;

import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EmbeddedSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

public class AsyncTest extends TestHelper {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";

  @Test
  public void testScan() {
    database.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();

      database.async().scanType(TYPE_NAME, true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          callbackInvoked.incrementAndGet();
          return true;
        }
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
  public void testScanInterrupt() {
    database.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();

      database.async().scanType(TYPE_NAME, true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          if (callbackInvoked.get() > 9)
            return false;

          return callbackInvoked.getAndIncrement() < 10;
        }
      });

      Assertions.assertTrue(callbackInvoked.get() < 20);

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
    database.async().onOk(new OkCallback() {
      @Override
      public void call() {
        okCallbackInvoked.incrementAndGet();
      }
    });

    database.async().onError(new ErrorCallback() {
      @Override
      public void call(Throwable exception) {
        Assertions.fail("Error on creating async record", exception);
      }
    });

    final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    type.createProperty("surname", String.class);
    database.getSchema().createTypeIndex(EmbeddedSchema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, 20000);

    database.commit();

    for (int i = 0; i < TOT; ++i) {
      final MutableDocument v = database.newDocument(TYPE_NAME);
      v.set("id", i);
      v.set("name", "Jay");
      v.set("surname", "Miner");

      database.async().createRecord(v, null);

    }

    database.async().waitCompletion();

    Assertions.assertTrue(okCallbackInvoked.get() > 0);
  }
}
