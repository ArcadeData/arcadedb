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

import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;
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

      db.scanType("V", true, record -> {
        final MutableDocument document = record.modify();
        document.set("update", true);
        document.set("largeField", "This is a large field to force the page overlap at some point"); // FORCE THE PAGE OVERLAP
        document.save();
        return true;
      });

      db.commit();

      db.begin();

      Assertions.assertEquals(TOT, db.countType("V", true));

      db.scanType("V", true, record -> {
        Assertions.assertEquals(true, record.get("update"));
        Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField"));
        return true;
      });

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
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

        LogManager.instance().log(this, Level.FINE, "Completed %d cycle of updates", i);
      }

      db.scanType("V", true, record -> {
        Assertions.assertEquals(true, record.get("update"));

        for (int i = 0; i < 10; ++i)
          Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField" + i));

        return true;
      });

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  public void testUpdateAndDelete() {
    final Database db = database;

    try {
      db.getSchema().getType("V").createProperty("id", Type.STRING);
      db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");

      // ALL IN THE SAME TX
      db.transaction(() -> {
        final MutableDocument doc = database.newDocument("V").set("id", "0").save();
        doc.set("id", "is an update").save();
        doc.delete();
      });

      // SEPARATE TXs
      final AtomicReference<RID> rid = new AtomicReference<>();
      db.transaction(() -> {
        final MutableDocument doc = database.newDocument("V").set("id", "is a test").save();
        doc.set("id", "is an update again").save();
        rid.set(doc.getIdentity());
      });

      db.transaction(() -> {
        MutableDocument doc = rid.get().getRecord(true).asDocument().modify().set("id", "this is an update");
        doc.save();

        database.deleteRecord(doc);
      });

    } finally {
      db.getSchema().dropIndex("V[id]");
      new DatabaseChecker(database).setVerboseLevel(0).check();
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

        db.scanType("V", true, record -> {
          Assertions.assertEquals(true, record.get("update"));

          Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField" + counter));

          return true;
        });

        deleteAll();

        Assertions.assertEquals(0, db.countType("V", true));

        db.commit();

        database.transaction(() -> Assertions.assertEquals(0, db.countType("V", true)));

        LogManager.instance().log(this, Level.FINE, "Completed %d cycle of updates+delete", i);

        createAll();

        database.transaction(() -> Assertions.assertEquals(TOT, db.countType("V", true)));
      }

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  private void createAll() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("V"))
        database.getSchema().createDocumentType("V");

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        v.set("name", "V" + i);
        v.save();
      }
    });
  }

  private void updateAll(String largeField) {
    database.scanType("V", true, record -> {
      final MutableDocument document = record.modify();
      document.set("update", true);
      document.set(largeField, "This is a large field to force the page overlap at some point"); // FORCE THE PAGE OVERLAP
      document.save();
      return true;
    });
  }

  private void deleteAll() {
    database.scanType("V", true, record -> {
      database.deleteRecord(record);
      return true;
    });
  }

}
