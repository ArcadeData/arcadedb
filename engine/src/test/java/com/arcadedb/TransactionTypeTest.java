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

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class TransactionTypeTest extends TestHelper {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";

  @Test
  public void testPopulate() {
  }

  @Test
  public void testScan() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanType(TYPE_NAME, true, record -> {
      Assertions.assertNotNull(record);

      Set<String> prop = new HashSet<String>();
        prop.addAll(record.getPropertyNames());

      Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
      Assertions.assertTrue(prop.contains("id"));
      Assertions.assertTrue(prop.contains("name"));
      Assertions.assertTrue(prop.contains("surname"));

      total.incrementAndGet();
      return true;
    });

    Assertions.assertEquals(TOT, total.get());

    database.commit();
  }

  @Test
  public void testLookupAllRecordsByRID() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanType(TYPE_NAME, true, record -> {
      final Document record2 = (Document) database.lookupByRID(record.getIdentity(), false);
      Assertions.assertNotNull(record2);
      Assertions.assertEquals(record, record2);

      Set<String> prop = new HashSet<String>();
        prop.addAll(record2.getPropertyNames());

      Assertions.assertEquals(record2.getPropertyNames().size(), 3);
      Assertions.assertTrue(prop.contains("id"));
      Assertions.assertTrue(prop.contains("name"));
      Assertions.assertTrue(prop.contains("surname"));

      total.incrementAndGet();
      return true;
    });

    database.commit();

    Assertions.assertEquals(TOT, total.get());
  }

  @Test
  public void testLookupAllRecordsByKey() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    for (int i = 0; i < TOT; i++) {
      final IndexCursor result = database.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { i });
      Assertions.assertNotNull(result);
      Assertions.assertEquals(1, result.size());

      final Document record2 = (Document) result.next().getRecord();

      Assertions.assertEquals(i, record2.get("id"));

      Set<String> prop = new HashSet<String>();
        prop.addAll(record2.getPropertyNames());

      Assertions.assertEquals(record2.getPropertyNames().size(), 3);
      Assertions.assertTrue(prop.contains("id"));
      Assertions.assertTrue(prop.contains("name"));
      Assertions.assertTrue(prop.contains("surname"));

      total.incrementAndGet();
    }

    database.commit();

    Assertions.assertEquals(TOT, total.get());
  }

  @Test
  public void testDeleteAllRecordsReuseSpace() throws IOException {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanType(TYPE_NAME, true, record -> {
      database.deleteRecord(record);
      total.incrementAndGet();
      return true;
    });

    database.commit();

    Assertions.assertEquals(TOT, total.get());

    database.begin();

    Assertions.assertEquals(0, database.countType(TYPE_NAME, true));

    // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
    final Index[] indexes = database.getSchema().getIndexes();
    for (int i = 0; i < TOT; ++i) {
      for (Index index : indexes)
        Assertions.assertFalse(index.get(new Object[] { i }).hasNext(), "Found item with key " + i);
    }

    beginTest();

    database.transaction(() -> Assertions.assertEquals(TOT, database.countType(TYPE_NAME, true)));
  }

  @Test
  public void testDeleteRecordsCheckScanAndIterators() throws IOException {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    final long originalCount = database.countType(TYPE_NAME, true);

    database.scanType(TYPE_NAME, true, record -> {
      database.deleteRecord(record);
      total.incrementAndGet();
      return false;
    });

    database.commit();

    Assertions.assertEquals(1, total.get());

    database.begin();

    Assertions.assertEquals(originalCount - 1, database.countType(TYPE_NAME, true));

    // COUNT WITH SCAN
    total.set(0);
    database.scanType(TYPE_NAME, true, record -> {
      total.incrementAndGet();
      return true;
    });
    Assertions.assertEquals(originalCount - 1, total.get());

    // COUNT WITH ITERATE TYPE
    total.set(0);
    for (Iterator<Record> it = database.iterateType(TYPE_NAME, true); it.hasNext(); it.next())
      total.incrementAndGet();

    Assertions.assertEquals(originalCount - 1, total.get());
  }

  @Test
  public void testPlaceholderOnScanAndIterate() throws IOException {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    final long originalCount = database.countType(TYPE_NAME, true);

    database.scanType(TYPE_NAME, true, record -> {
      record.modify().set("additionalProperty", "Something just to create a placeholder").save();
      total.incrementAndGet();
      return false;
    });

    database.commit();

    Assertions.assertEquals(1, total.get());

    database.begin();

    Assertions.assertEquals(originalCount, database.countType(TYPE_NAME, true));

    // COUNT WITH SCAN
    total.set(0);
    database.scanType(TYPE_NAME, true, record -> {
      total.incrementAndGet();
      return true;
    });
    Assertions.assertEquals(originalCount, total.get());

    // COUNT WITH ITERATE TYPE
    total.set(0);
    for (Iterator<Record> it = database.iterateType(TYPE_NAME, true); it.hasNext(); it.next())
      total.incrementAndGet();

    Assertions.assertEquals(originalCount, total.get());
  }

  @Test
  public void testDeleteFail() {
    reopenDatabaseInReadOnlyMode();

    Assertions.assertThrows(DatabaseIsReadOnlyException.class, () -> {

      database.begin();

      database.scanType(TYPE_NAME, true, record -> {
        database.deleteRecord(record);
        return true;
      });

      database.commit();
    });

    reopenDatabase();
  }

  @Test
  public void testNestedTx() {
    database.transaction(() -> {
      database.newDocument(TYPE_NAME).set("id", -1, "tx", 1).save();
      database.transaction(() -> database.newDocument(TYPE_NAME).set("id", -2, "tx", 2).save());
    });

    Assertions.assertEquals(0, database.query("sql", "select from " + TYPE_NAME + " where tx = 0").countEntries());
    Assertions.assertEquals(1, database.query("sql", "select from " + TYPE_NAME + " where tx = 1").countEntries());
    Assertions.assertEquals(1, database.query("sql", "select from " + TYPE_NAME + " where tx = 2").countEntries());
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
        type.createProperty("id", Integer.class);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, "id");
      }

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");

        v.save();
      }
    });
  }
}
