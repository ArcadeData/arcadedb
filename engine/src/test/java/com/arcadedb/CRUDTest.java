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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.*;
import java.util.logging.*;

import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

class CRUDTest extends TestHelper {
  private static final int TOT = ((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()) * 2;

  @Override
  protected void beginTest() {
    createAll();
  }

  @Test
  void update() {
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

      assertThat(db.countType("V", true)).isEqualTo(TOT);

      db.scanType("V", true, record -> {
        assertThat(record.get("update")).isEqualTo(true);
        assertThat(record.get("largeField")).isEqualTo("This is a large field to force the page overlap at some point");
        return true;
      });

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  @Tag("slow")
  void multiUpdatesOverlap() {
    final Database db = database;

    try {
      db.begin();

      for (int i = 0; i < 3; ++i) {
        final String largeField = "largeField" + i;
        updateAll(largeField);

        assertThat(db.countType("V", true)).isEqualTo(TOT);

        assertThat(((Long) db.query("sql", "select count(*) as count from V where " + largeField + " is not null").nextIfAvailable()
            .getProperty("count")).intValue()).as("Count not expected for field '" + largeField + "'").isEqualTo(TOT);

        db.commit();
        db.begin();

        assertThat(db.countType("V", true)).isEqualTo(TOT);

        LogManager.instance().log(this, Level.FINE, "Completed %d cycle of updates", i);
      }

      database.scanType("V", true, record -> {
        final MutableDocument document = record.modify();
        document.set("update", true);
        document.remove("largeField0");
        document.remove("largeField1");
        document.remove("largeField2");
        document.save();
        return true;
      });

      for (int i = 0; i < 10; ++i) {
        final String largeField = "largeField" + i;
        updateAll(largeField);

        assertThat(db.countType("V", true)).isEqualTo(TOT);
        assertThat(((Long) db.query("sql", "select count(*) as count from V where " + largeField + " is not null").nextIfAvailable()
            .getProperty("count")).intValue()).as("Count not expected for field '" + largeField + "'").isEqualTo(TOT);

        db.commit();
        db.begin();

        assertThat(db.countType("V", true)).isEqualTo(TOT);

        LogManager.instance().log(this, Level.FINE, "Completed %d cycle of updates", i);
      }

      db.scanType("V", true, record -> {
        assertThat(record.get("update")).isEqualTo(true);

        for (int i = 0; i < 10; ++i)
          assertThat(record.get("largeField" + i)).isEqualTo("This is a large field to force the page overlap at some point");

        return true;
      });

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  void updateAndDelete() {
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
        final MutableDocument doc = rid.get().getRecord(true).asDocument().modify().set("id", "this is an update");
        doc.save();

        database.deleteRecord(doc);
      });

    } finally {
      db.getSchema().dropIndex("V[id]");
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  @Tag("slow")
  void multiUpdatesAndDeleteOverlap() {
    final Database db = database;
    try {

      for (int i = 0; i < 10; ++i) {
        final int counter = i;

        db.begin();

        assertThat(db.countType("V", true)).isEqualTo(TOT);

        updateAll("largeField" + i);

        assertThat(db.countType("V", true)).isEqualTo(TOT);

        db.commit();
        db.begin();

        assertThat(db.countType("V", true)).isEqualTo(TOT);

        db.scanType("V", true, record -> {
          assertThat(record.get("update")).withFailMessage("Record " + record.toJSON()).isEqualTo(true);

          assertThat(record.get("largeField" + counter)).isEqualTo("This is a large field to force the page overlap at some point");
          return true;
        });

        deleteAll();

        assertThat(db.countType("V", true)).isEqualTo(0);

        db.commit();

        database.transaction(() -> assertThat(db.countType("V", true)).isEqualTo(0));

        LogManager.instance().log(this, Level.FINE, "Completed %d cycle of updates+delete", i);

        createAll();

        database.transaction(() -> assertThat(db.countType("V", true)).isEqualTo(TOT));
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

  private void updateAll(final String largeField) {
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
