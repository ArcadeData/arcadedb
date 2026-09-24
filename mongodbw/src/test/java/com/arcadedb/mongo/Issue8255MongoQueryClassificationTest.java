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
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8255: {@code MongoQueryEngine} classified a query as a write by
 * substring-matching the whole JSON text, so an ordinary find whose filter contained a value like
 * {@code "delete"} was refused as non-idempotent. The classifier must look at the parsed JSON's
 * top-level keys instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8255MongoQueryClassificationTest {

  private Database database;

  @BeforeEach
  void beginTest() {
    FileUtils.deleteRecursively(new File("./target/databases/issue8255"));

    database = new DatabaseFactory("./target/databases/issue8255").create();

    database.getSchema().createDocumentType("events");
    database.getSchema().createDocumentType("orders");

    database.transaction(() -> {
      database.newDocument("orders").set("status", "open").save();
      database.newDocument("events").set("action", "delete").save();
      database.newDocument("events").set("action", "update").save();
      database.newDocument("events").set("action", "insert").save();
      database.newDocument("events").set("kind", "remove").save();
      database.newDocument("events").set("delete", true).save();
    });
  }

  @AfterEach
  void endTest() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      ((DatabaseInternal) database).getEmbedded().drop();
    }
  }

  @Test
  void findsWhoseFilterValueSpellsAWriteVerbAreAdmittedAsReads() {
    assertFindIsAdmitted("{ collection: 'orders', query: { status: 'open' } }", 1);
    assertFindIsAdmitted("{ collection: 'events', query: { action: 'delete' } }", 1);
    assertFindIsAdmitted("{ collection: 'events', query: { action: 'update' } }", 1);
    assertFindIsAdmitted("{ collection: 'events', query: { action: 'insert' } }", 1);
    assertFindIsAdmitted("{ collection: 'events', query: { kind: 'remove' } }", 1);
    assertFindIsAdmitted("{ collection: 'events', query: { delete: true } }", 1);
  }

  private void assertFindIsAdmitted(final String query, final int expectedCount) {
    int count = 0;
    try (ResultSet resultSet = database.query("mongo", query)) {
      while (resultSet.hasNext()) {
        resultSet.next();
        ++count;
      }
    }
    assertThat(count).isEqualTo(expectedCount);
  }

  @Test
  void classificationIsDrivenByTopLevelKeysNotByAnySubstring() {
    final QueryEngine.AnalyzedQuery find = analyze("{ collection: 'orders', query: { action: 'delete' } }");
    assertThat(find.isIdempotent()).isTrue();

    final QueryEngine.AnalyzedQuery insert = analyze("{ insert: 'orders', documents: [ { status: 'open' } ] }");
    assertThat(insert.isIdempotent()).isFalse();

    final QueryEngine.AnalyzedQuery update = analyze("{ update: 'orders', updates: [] }");
    assertThat(update.isIdempotent()).isFalse();

    final QueryEngine.AnalyzedQuery delete = analyze("{ delete: 'orders', deletes: [] }");
    assertThat(delete.isIdempotent()).isFalse();
  }

  @Test
  void schemaCommandIsNotShadowedByTheReadSignalOnCollection() {
    // "collection" is present on every accepted query, so an explicit schema verb like
    // "dropCollection" must still win over it rather than being classified as a plain READ.
    final QueryEngine.AnalyzedQuery dropCollection = analyze("{ collection: 'users', dropCollection: 1 }");
    assertThat(dropCollection.isDDL()).isTrue();
    assertThat(dropCollection.isIdempotent()).isFalse();
  }

  @Test
  void unparseableQueryFallsBackToTheConservativeAllWritesClassification() {
    final QueryEngine.AnalyzedQuery notJson = analyze("this is not json");
    assertThat(notJson.isIdempotent()).isFalse();
    assertThat(notJson.isDDL()).isFalse();
  }

  private QueryEngine.AnalyzedQuery analyze(final String query) {
    return database.getQueryEngine("mongo").analyze(query);
  }
}
