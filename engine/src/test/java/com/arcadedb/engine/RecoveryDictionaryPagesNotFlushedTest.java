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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A crash before the flush wrote the dictionary's pages (issue #12111). The schema of a crashed database is loaded before its
 * WAL replay, and that load used to register the property names it did not find in the dictionary, each in a transaction of its
 * own. Those commits took the dictionary's page past the versions the WAL holds for it, so the replay skipped the dead process's
 * dictionary pages as superseded, and the recovery aborted on the first later entry it could not apply, losing every committed
 * transaction after it.
 */
class RecoveryDictionaryPagesNotFlushedTest {
  private static final String DB_PATH = "target/databases/RecoveryDictionaryPagesNotFlushedTest";

  @BeforeEach
  @AfterEach
  void cleanup() {
    // A failed open can leave its instance in this thread's context, where the next test would take it for its own
    DatabaseContext.INSTANCE.removeCurrentThreadContexts();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      if (factory.exists())
        factory.open().drop();
    } catch (final Exception e) {
      // a failed recovery leaves the database unopenable: removed on disk below
    }
    factory.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /** The shape of StatementSavepointTest and ACIDTransactionTest: the schema declared in a transaction, a row in the next. */
  @Test
  void aRowCommittedAfterTheSchemaSurvivesACrashBeforeTheDictionaryIsFlushed() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      // Nothing the transactions below commit reaches the disk: the WAL is all the next open has
      assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true))
          .as("flush suspension must engage or the pages could reach disk, invalidating the test").isTrue();
      db.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);
      db.transaction(() -> {
        final DocumentType v = db.getSchema().createDocumentType("V");
        v.createProperty("id", Integer.class);
        v.createProperty("name", String.class);
        v.createProperty("surname", String.class);
      });
      db.transaction(() -> db.newDocument("V").set("id", 0, "name", "Crash", "surname", "Test").save());
      ((LocalDatabase) db).kill();
      db.close();

      final Database reopened = factory.open();
      try {
        final List<Result> rows = new ArrayList<>();
        try (final ResultSet rs = reopened.query("sql", "SELECT id, name, surname FROM V")) {
          rs.forEachRemaining(rows::add);
        }
        assertThat(rows).as("the committed row the WAL holds").hasSize(1);
        assertThat(rows.getFirst().<String>getProperty("name")).isEqualTo("Crash");
        assertThat(rows.getFirst().<String>getProperty("surname")).isEqualTo("Test");
        assertThat(rows.getFirst().<Integer>getProperty("id")).isZero();
      } finally {
        reopened.close();
      }
    } finally {
      factory.close();
    }
  }

  /**
   * The open writes the header page of a dictionary it finds empty, the image of the dictionary's creation, and the replay goes
   * on from it: the names come back, and the dictionary is not reported lost (what takes an HA node's copy out of service).
   */
  @Test
  void anEmptiedDictionaryTheReplayRestoresIsNotReportedLost() throws IOException {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      db.getSchema().createDocumentType("T").createProperty("id", Long.class);
      db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "T", "id");
      db.transaction(() -> {
        for (long id = 1; id <= 3; id++)
          db.newDocument("T").set("id", id, "name", "row-" + id).save();
      });
      final String dictionaryPath = db.getSchema().getEmbedded().getDictionary().getOSFile().getPath();
      ((LocalDatabase) db).kill();
      db.close();

      // What the kill leaves when the flush thread had not written the dictionary's header page yet
      try (final RandomAccessFile raw = new RandomAccessFile(dictionaryPath, "rw")) {
        raw.setLength(0);
      }

      final Database reopened = factory.open();
      try {
        final List<Long> ids = new ArrayList<>();
        try (final ResultSet rs = reopened.query("sql", "SELECT id, name FROM T ORDER BY id")) {
          while (rs.hasNext()) {
            final Result row = rs.next();
            ids.add(row.<Number>getProperty("id").longValue());
            assertThat(row.<String>getProperty("name")).isEqualTo("row-" + ids.getLast());
          }
        }
        assertThat(ids).as("the committed rows the WAL holds").containsExactly(1L, 2L, 3L);
        assertThat(((DatabaseInternal) reopened).getSchema().getEmbedded().getDictionary().wasHeaderPageRecreated())
            .as("the replay restored the names on the header page the open wrote").isFalse();
        assertThat(((DatabaseInternal) reopened).getSchema().getEmbedded().isDictionaryLostForExistingTypes()).isFalse();
      } finally {
        reopened.close();
      }
    } finally {
      factory.close();
    }
  }

  /** The other side of the one above: when the WAL holds no page of the dictionary, its names are gone and that is reported. */
  @Test
  void anEmptiedDictionaryTheWalHoldsNoPageOfStaysReportedLost() throws IOException {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      final DatabaseInternal created = (DatabaseInternal) factory.create();
      created.getSchema().createDocumentType("T").createProperty("id", Long.class);
      final String dictionaryPath = created.getSchema().getEmbedded().getDictionary().getOSFile().getPath();
      // A clean close: the dictionary is on disk and no WAL holds it any longer
      created.close();

      final DatabaseInternal db = (DatabaseInternal) factory.open();
      // A row whose only property name is in the dictionary already: the WAL the kill leaves holds no page of it
      db.transaction(() -> db.newDocument("T").set("id", 1L).save());
      ((LocalDatabase) db).kill();
      db.close();

      try (final RandomAccessFile raw = new RandomAccessFile(dictionaryPath, "rw")) {
        raw.setLength(0);
      }

      final Database reopened = factory.open();
      try {
        assertThat(((DatabaseInternal) reopened).getSchema().getEmbedded().getDictionary().wasHeaderPageRecreated())
            .as("nothing restored the names the emptied file held").isTrue();
        assertThat(((DatabaseInternal) reopened).getSchema().getEmbedded().isDictionaryLostForExistingTypes()).isTrue();
      } finally {
        reopened.close();
      }
    } finally {
      factory.close();
    }
  }
}
