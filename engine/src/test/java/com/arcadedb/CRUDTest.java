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

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

public class CRUDTest extends TestHelper {
  private static final int TOT = Bucket.DEF_PAGE_SIZE * 2;

  @Override
  protected void beginTest() {
    createAll();
  }

  @Test
  public void testUpdate() {
    final Database db = database;
    db.begin();
    try {

      db.scanType("V", true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          final MutableDocument document = record.modify();
          document.set("update", true);
          document.set("largeField", "This is a large field to force the page overlap at some point"); // FORCE THE PAGE OVERLAP
          document.save();
          return true;
        }
      });

      db.commit();

      db.begin();

      Assertions.assertEquals(TOT, db.countType("V", true));

      db.scanType("V", true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          Assertions.assertEquals(true, record.get("update"));
          Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField"));
          return true;
        }
      });

    } finally {
      new DatabaseChecker().check(database);
    }
  }

  @Test
  public void testMultiUpdatesOverlap() {
    final Database db = database;

    try {
      db.begin();

      for (int i = 0; i < 10; ++i) {
        updateAll("largeField" + i);

        Assertions.assertEquals(TOT, db.countType("V", true));

        db.commit();
        db.begin();

        Assertions.assertEquals(TOT, db.countType("V", true));

        LogManager.instance().log(this, Level.INFO, "Completed %d cycle of updates", null, i);
      }

      db.scanType("V", true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          Assertions.assertEquals(true, record.get("update"));

          for (int i = 0; i < 10; ++i)
            Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField" + i));

          return true;
        }
      });

    } finally {
      new DatabaseChecker().check(database);
    }
  }

  @Test
  public void testMultiUpdatesAndDeleteOverlap() {
    final Database db = database;
    try {

      for (int i = 0; i < 10; ++i) {
        final int counter = i;

        db.begin();

        Assertions.assertEquals(TOT, db.countType("V", true));

        updateAll("largeField" + i);

        Assertions.assertEquals(TOT, db.countType("V", true));

        db.commit();
        db.begin();

        Assertions.assertEquals(TOT, db.countType("V", true));

        db.scanType("V", true, new DocumentCallback() {
          @Override
          public boolean onRecord(Document record) {
            Assertions.assertEquals(true, record.get("update"));

            Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField" + counter));

            return true;
          }
        });

        deleteAll();

        Assertions.assertEquals(0, db.countType("V", true));

        db.commit();

        database.transaction((tx) -> {
          Assertions.assertEquals(0, db.countType("V", true));
        });

        LogManager.instance().log(this, Level.INFO, "Completed %d cycle of updates+delete", null, i);

        createAll();

        database.transaction((tx) -> {
          Assertions.assertEquals(TOT, db.countType("V", true));
        });
      }

    } finally {
      new DatabaseChecker().check(database);
    }
  }

  private void createAll() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createDocumentType("V");

        for (int i = 0; i < TOT; ++i) {
          final MutableDocument v = database.newDocument("V");
          v.set("id", i);
          v.set("name", "V" + i);
          v.save();
        }
      }
    });
  }

  private void updateAll(String largeField) {
    database.scanType("V", true, new DocumentCallback() {
      @Override
      public boolean onRecord(Document record) {
        final MutableDocument document = (MutableDocument) record.modify();
        document.set("update", true);
        document.set(largeField, "This is a large field to force the page overlap at some point"); // FORCE THE PAGE OVERLAP
        document.save();
        return true;
      }
    });
  }

  private void deleteAll() {
    database.scanType("V", true, new DocumentCallback() {
      @Override
      public boolean onRecord(Document record) {
        database.deleteRecord(record);
        return true;
      }
    });
  }

}
