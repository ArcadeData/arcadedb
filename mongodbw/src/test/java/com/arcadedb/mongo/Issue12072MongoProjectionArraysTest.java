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
import com.arcadedb.utility.FileUtils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12072: a dotted find projection only followed embedded documents. When the path reached an array, an inclusion
 * ({"items.name": 1}) dropped the whole field and an exclusion ({"items.price": 0}) removed nothing; the included fields also came
 * back in the projection's order instead of the document's. The expected documents below are mongod 7's answers for the same
 * document and projection.
 */
class Issue12072MongoProjectionArraysTest {
  private static final String DB_PATH = "./target/databases/issue12072";

  private Database               database;
  private MongoDBDatabaseWrapper wrapper;

  @BeforeEach
  void beginTest() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.getSchema().createDocumentType("Orders");
    new MongoDBCollectionWrapper(database, "Orders").insertDocuments(List.of(order()));
    wrapper = new MongoDBDatabaseWrapper(database, null, null);
  }

  @AfterEach
  void endTest() {
    if (database != null) {
      while (database.isTransactionActive())
        database.rollback();
      ((DatabaseInternal) database).getEmbedded().drop();
    }
  }

  private static Document order() {
    return new Document("_id", 1).append("a", new Document("b", 1).append("c", 2))//
        .append("items", List.of(//
            new Document("name", "x").append("price", 1).append("q", new Document("r", 1).append("s", 2)),//
            5,//
            List.of(new Document("name", "y")),//
            new Document("other", 1)))//
        .append("tags", List.of())//
        .append("z", 3);
  }

  private Document project(final Document projection) {
    final Document reply = wrapper.handleCommand(null, "find",
        new Document("find", "Orders").append("filter", new Document()).append("projection", projection), null, null);
    final List<?> batch = (List<?>) ((Document) reply.get("cursor")).get("firstBatch");
    assertThat(batch).hasSize(1);
    return (Document) batch.getFirst();
  }

  @Test
  void anInclusionThroughAnArrayProjectsEachDocumentAndDropsTheScalars() {
    assertThat(project(new Document("items.name", 1))).isEqualTo(new Document("_id", 1).append("items",
        List.of(new Document("name", "x"), List.of(new Document("name", "y")), new Document())));
    assertThat(project(new Document("items.name", 1).append("items.price", 1))).isEqualTo(new Document("_id", 1).append("items",
        List.of(new Document("name", "x").append("price", 1), List.of(new Document("name", "y")), new Document())));
    assertThat(project(new Document("items.q.r", 1))).isEqualTo(new Document("_id", 1).append("items",
        List.of(new Document("q", new Document("r", 1)), List.of(new Document()), new Document())));
    assertThat(project(new Document("tags.name", 1).append("_id", 0))).isEqualTo(new Document("tags", List.of()));
  }

  @Test
  void anExclusionThroughAnArrayRemovesTheFieldFromEachDocumentAndKeepsTheScalars() {
    final Document expected = order();
    expected.put("items", List.of(//
        new Document("name", "x").append("q", new Document("r", 1).append("s", 2)), 5, List.of(new Document("name", "y")),
        new Document("other", 1)));
    assertThat(project(new Document("items.price", 0))).isEqualTo(expected);

    final Document deep = order();
    deep.put("items", List.of(new Document("name", "x").append("price", 1).append("q", new Document("s", 2)), 5,
        List.of(new Document("name", "y")), new Document("other", 1)));
    assertThat(project(new Document("items.q.r", 0))).isEqualTo(deep);
  }

  @Test
  void includedFieldsComeBackInTheOrderOfTheDocument() {
    assertThat(new ArrayList<>(project(new Document("z", 1).append("items.name", 1)).keySet())).containsExactly("_id", "items", "z");
    assertThat(new ArrayList<>(project(new Document("z", 1).append("a", 1)).keySet())).containsExactly("_id", "a", "z");
  }

  @Test
  void embeddedDocumentsAndTheIdStillProjectAsBefore() {
    assertThat(project(new Document("a.b", 1).append("_id", 0))).isEqualTo(new Document("a", new Document("b", 1)));
    assertThat(project(new Document("_id", 1))).isEqualTo(new Document("_id", 1));
    final Document withoutId = order();
    withoutId.remove("_id");
    assertThat(project(new Document("_id", 0))).isEqualTo(withoutId);
    assertThat(project(new Document("z.w", 1))).isEqualTo(new Document("_id", 1));
  }

  @Test
  void aPathAndOneBelowItCollideLikeMongod() {
    assertThatThrownBy(() -> project(new Document("a", 1).append("a.b", 1))).isInstanceOf(MongoServerError.class)
        .hasMessageContaining("Path collision at a.b remaining portion b");
    assertThatThrownBy(() -> project(new Document("a.b", 0).append("a", 0))).isInstanceOf(MongoServerError.class)
        .hasMessageContaining("Path collision at a");
  }
}
