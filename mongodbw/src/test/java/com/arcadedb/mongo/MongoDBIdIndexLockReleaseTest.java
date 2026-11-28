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
package com.arcadedb.mongo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.DatabaseIsReopeningException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.locks.Lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * The delete and update commands take the read lock of the collection's {@code _id} index for the life of their own transaction.
 * They used to take it BEFORE beginning that transaction and release it only in the finally of the try that follows the begin, so a
 * begin that throws - a database refusing new transactions while an idle close reopens it ({@code DatabaseIsReopeningException},
 * which the client retries) - left the read lock held by the thread forever, and every later drop or rebuild of the {@code _id}
 * index, which takes the write lock, hung. Found by the {@code lock-not-released-in-finally} opengrep rule.
 */
class MongoDBIdIndexLockReleaseTest {
  private static final String DB_PATH = "./target/databases/mongoIdIndexLockRelease";

  private Database database;

  @BeforeEach
  void beginTest() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.getSchema().createDocumentType("Coll").createProperty("_id", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Coll", "_id");
    database.transaction(() -> database.newDocument("Coll").set("_id", "a").set("v", 1).save());
  }

  @AfterEach
  void endTest() {
    if (database != null) {
      while (database.isTransactionActive())
        database.rollback();
      ((DatabaseInternal) database).getEmbedded().drop();
    }
  }

  @Test
  void aDeleteWhoseTransactionCannotBeginReleasesTheIdIndexLock() {
    final MongoDBDatabaseWrapper wrapper = new MongoDBDatabaseWrapper(refusingNewTransactions(), null, null);

    assertThatThrownBy(() -> wrapper.handleCommand(null, "delete",
        new Document("delete", "Coll").append("deletes", List.of(new Document("q", new Document("_id", "a")).append("limit", 1))), null,
        null)).hasRootCauseInstanceOf(DatabaseIsReopeningException.class);

    assertIdIndexLockIsFree();
  }

  @Test
  void anUpdateWhoseTransactionCannotBeginReleasesTheIdIndexLock() {
    final MongoDBDatabaseWrapper wrapper = new MongoDBDatabaseWrapper(refusingNewTransactions(), null, null);

    assertThatThrownBy(() -> wrapper.handleCommand(null, "update", new Document("update", "Coll").append("updates",
        List.of(new Document("q", new Document("_id", "a")).append("u", new Document("$set", new Document("v", 2))))), null, null))
        .hasRootCauseInstanceOf(DatabaseIsReopeningException.class);

    assertIdIndexLockIsFree();
  }

  /** The database as an idle close reopening it presents itself: open, and refusing to begin a transaction. */
  private Database refusingNewTransactions() {
    final Database refusing = spy(database);
    doThrow(new DatabaseIsReopeningException(database.getName())).when(refusing).begin();
    return refusing;
  }

  /**
   * The write lock is what a drop or a rebuild of the {@code _id} index takes. Asked on this thread, the one the command ran on: a
   * thread still holding the read lock cannot take the write lock either, so a leaked read lock answers false here.
   */
  private void assertIdIndexLockIsFree() {
    final Lock writeLock = MongoDBCollectionWrapper.idIndexLock(database, "Coll").writeLock();
    final boolean acquired = writeLock.tryLock();
    if (acquired)
      writeLock.unlock();
    assertThat(acquired).as("the _id index lock the failed command took must have been released").isTrue();
  }
}
