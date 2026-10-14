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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9977: a MongoDB write opens its own transaction whatever the caller holds, and an ArcadeDB nested transaction is
 * independent rather than a savepoint. The insert used to close with a rollback guarded by {@code isTransactionActive()}, which
 * after its own commit answers for the CALLER's transaction and discarded it on the successful path; the delete and update
 * commands rolled back whatever was on top of the thread on a failure. Each must touch only the transaction it opened.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9977MongoOwnTransactionTest {
  private static final String DB_PATH = "./target/databases/issue9977";

  private Database database;

  @BeforeEach
  void beginTest() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.getSchema().createDocumentType("Witness");
    database.getSchema().createDocumentType("Coll").createProperty("_id", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Coll", "_id");
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
  void aSuccessfulInsertKeepsTheCallerTransaction() {
    database.begin();
    database.newDocument("Witness").set("name", "caller").save();

    new MongoDBCollectionWrapper(database, "Coll").insertDocuments(List.of(new Document("_id", "a").append("v", 1)));

    assertThat(database.isTransactionActive()).as("the caller's transaction must survive the Mongo insert").isTrue();
    database.commit();

    assertThat(count("Witness")).isEqualTo(1L);
    assertThat(count("Coll")).isEqualTo(1L);
  }

  @Test
  void aFailedInsertRollsBackOnlyItsOwnTransaction() {
    database.transaction(() -> database.newDocument("Coll").set("_id", "dup").save());

    database.begin();
    database.newDocument("Witness").set("name", "caller").save();

    final MongoDBCollectionWrapper collection = new MongoDBCollectionWrapper(database, "Coll");
    assertThatThrownBy(() -> collection.insertDocuments(List.of(new Document("_id", "fresh"), new Document("_id", "dup")))).hasMessageContaining(
        "dup");

    assertThat(database.isTransactionActive()).as("the caller's transaction must survive the failed insert").isTrue();
    database.commit();

    assertThat(count("Witness")).isEqualTo(1L);
    assertThat(count("Coll")).as("the failed batch is undone, the earlier record stays").isEqualTo(1L);
  }

  @Test
  void aSuccessfulDeleteAndUpdateKeepTheCallerTransaction() {
    database.transaction(() -> {
      database.newDocument("Coll").set("_id", "a").set("v", 1).save();
      database.newDocument("Coll").set("_id", "b").set("v", 2).save();
    });

    final MongoDBDatabaseWrapper wrapper = new MongoDBDatabaseWrapper(database, null, null);

    database.begin();
    database.newDocument("Witness").set("name", "caller").save();

    final Document update = wrapper.handleCommand(null, "update", new Document("update", "Coll").append("updates",
        List.of(new Document("q", new Document("_id", "a")).append("u", new Document("$set", new Document("v", 10))))), null, null);
    assertThat(update.get("n")).isEqualTo(1);

    final Document delete = wrapper.handleCommand(null, "delete",
        new Document("delete", "Coll").append("deletes", List.of(new Document("q", new Document("_id", "b")).append("limit", 1))), null, null);
    assertThat(delete.get("n")).isEqualTo(1);

    assertThat(database.isTransactionActive()).isTrue();
    database.commit();

    assertThat(count("Witness")).isEqualTo(1L);
    assertThat(count("Coll")).isEqualTo(1L);
  }

  @Test
  void aFailedUpdateRollsBackOnlyItsOwnTransaction() {
    database.transaction(() -> database.newDocument("Coll").set("_id", "a").set("v", 1).save());

    final MongoDBDatabaseWrapper wrapper = new MongoDBDatabaseWrapper(database, null, null);

    database.begin();
    database.newDocument("Witness").set("name", "caller").save();

    // changing the immutable _id is refused (error 66) once the update's own transaction has begun
    assertThatThrownBy(() -> wrapper.handleCommand(null, "update", new Document("update", "Coll").append("updates",
        List.of(new Document("q", new Document("_id", "a")).append("u", new Document("$set", new Document("_id", "z"))))), null, null)).hasStackTraceContaining(
        "_id");

    assertThat(database.isTransactionActive()).as("the caller's transaction must survive the failed update").isTrue();
    database.commit();

    assertThat(count("Witness")).isEqualTo(1L);
  }

  /**
   * The arm the {@code catch (RuntimeException) -> rollback()} got wrong: a commit that throws has already popped the command's own
   * transaction, so the rollback after it reached the caller's.
   */
  @Test
  void anUpdateWhoseCommitFailsRollsBackOnlyItsOwnTransaction() {
    database.getSchema().getType("Coll").createProperty("v", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Coll", "v");
    database.transaction(() -> {
      database.newDocument("Coll").set("_id", "a").set("v", 1).save();
      database.newDocument("Coll").set("_id", "b").set("v", 2).save();
    });

    final MongoDBDatabaseWrapper wrapper = new MongoDBDatabaseWrapper(database, null, null);

    database.begin();
    database.newDocument("Witness").set("name", "caller").save();

    // the unique key the update takes belongs to a committed record: refused when the command's transaction commits
    assertThatThrownBy(() -> wrapper.handleCommand(null, "update", new Document("update", "Coll").append("updates",
        List.of(new Document("q", new Document("_id", "a")).append("u", new Document("$set", new Document("v", 2))))), null, null)).isNotNull();

    assertThat(database.isTransactionActive()).as("the caller's transaction must survive the failed update").isTrue();
    database.commit();

    assertThat(count("Witness")).isEqualTo(1L);
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
