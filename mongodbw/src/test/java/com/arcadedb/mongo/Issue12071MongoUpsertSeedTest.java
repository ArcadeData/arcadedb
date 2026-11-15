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
 * Issue #12071: an upsert that matched nothing seeded the inserted document from the top-level fields of its filter only, and copied
 * a dotted key as it was. {@code {'a.b': 5}} stored a field literally named "a.b" that no later {@code {'a.b': 5}} matched, and the
 * equalities under {@code $and}, a single-branch {@code $or}, a one-value {@code $in} and an embedded-document equality (also on
 * {@code _id}) were dropped, so running the same upsert twice inserted two documents. A replacement upsert copied every filter field
 * where mongod takes the {@code _id} alone, and the generated {@code _id} was stored as the last field instead of the first.
 */
class Issue12071MongoUpsertSeedTest {
  private static final String DB_PATH = "./target/databases/issue12071";

  private Database               database;
  private MongoDBDatabaseWrapper wrapper;

  @BeforeEach
  void beginTest() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
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

  @Test
  void aDottedEqualitySeedsTheEmbeddedDocumentAndARepeatedUpsertMatchesIt() {
    final Document filter = new Document("a.b", 5);
    final Document first = upsert("C1", filter, new Document("$inc", new Document("hits", 1)));
    final Document second = upsert("C1", filter, new Document("$inc", new Document("hits", 1)));

    assertThat(first.get("upserted")).isNotNull();
    assertThat(second.get("upserted")).as("the second run matches the document the first inserted").isNull();
    assertThat(second.get("n")).isEqualTo(1);
    final List<Document> stored = findAll("C1");
    assertThat(stored).hasSize(1);
    assertThat(stored.getFirst().get("a")).isEqualTo(new Document("b", 5));
    assertThat(stored.getFirst().get("hits")).isEqualTo(2);
    assertThat(stored.getFirst().containsKey("a.b")).isFalse();
  }

  @Test
  void equalitiesUnderAndAndASingleBranchOrAreSeeded() {
    upsert("C2", new Document("$and", List.of(new Document("a", 1), new Document("b", 2))), new Document("$set", new Document("c", 1)));
    upsert("C2", new Document("$and", List.of(new Document("a", 1), new Document("b", 2))), new Document("$set", new Document("c", 2)));
    final List<Document> and = findAll("C2");
    assertThat(and).hasSize(1);
    assertThat(and.getFirst()).containsEntry("a", 1).containsEntry("b", 2).containsEntry("c", 2);

    upsert("C3", new Document("$or", List.of(new Document("a", 1))), new Document("$set", new Document("c", 1)));
    assertThat(findAll("C3").getFirst()).containsEntry("a", 1);

    // several branches say nothing about which one the document satisfies
    upsert("C4", new Document("$or", List.of(new Document("a", 1), new Document("b", 2))), new Document("$set", new Document("c", 1)));
    assertThat(findAll("C4").getFirst()).doesNotContainKeys("a", "b");
  }

  @Test
  void aOneValueInAndAnEmbeddedDocumentEqualityAreSeeded() {
    upsert("C5", new Document("a", new Document("$in", List.of(7))), new Document("$set", new Document("c", 1)));
    assertThat(findAll("C5").getFirst()).containsEntry("a", 7);

    upsert("C6", new Document("a", new Document("x", 1)), new Document("$set", new Document("a.y", 2)));
    assertThat(findAll("C6").getFirst().get("a")).isEqualTo(new Document("x", 1).append("y", 2));

    final Document reply = upsert("C7", new Document("_id", new Document("k", 2)), new Document("$set", new Document("c", 1)));
    assertThat(((Document) ((List<?>) reply.get("upserted")).getFirst()).get("_id")).isEqualTo(new Document("k", 2));
    assertThat(findAll("C7").getFirst().get("_id")).isEqualTo(new Document("k", 2));
  }

  @Test
  void aReplacementUpsertTakesOnlyTheIdFromTheFilter() {
    upsert("C8", new Document("b", 1).append("_id", 5), new Document("x", 1));
    final Document stored = findAll("C8").getFirst();
    assertThat(stored).containsEntry("_id", 5).containsEntry("x", 1).doesNotContainKey("b");
  }

  @Test
  void theIdIsTheFirstFieldOfTheInsertedDocument() {
    upsert("C9", new Document("a", 1), new Document("$set", new Document("c", 1)));
    upsert("C10", new Document("a", 1), new Document("$set", new Document("c", 1).append("_id", 9)));
    for (final String collection : List.of("C9", "C10"))
      assertThat(new ArrayList<>(findAll(collection).getFirst().keySet())).as(collection).containsExactly("_id", "a", "c");
  }

  @Test
  void aPathMatchedTwiceIsRefusedLikeMongod() {
    for (final Document filter : List.of(//
        new Document("$and", List.of(new Document("a", 1), new Document("a", 2))),//
        new Document("a", 1).append("a.b", 2),//
        new Document("a", new Document("$all", List.of(5, 6)))))
      assertThatThrownBy(() -> upsert("C11", filter, new Document("$set", new Document("c", 1)))).as("%s", filter)
          .isInstanceOf(MongoServerError.class).satisfies(e -> assertThat(((MongoServerError) e).getCode()).isEqualTo(54));
    assertThat(database.getSchema().existsType("C11") ? findAll("C11") : List.of()).isEmpty();
  }

  private Document upsert(final String collection, final Document filter, final Document u) {
    return wrapper.handleCommand(null, "update", new Document("update", collection).append("updates",
        List.of(new Document("q", filter).append("u", u).append("upsert", true))), null, null);
  }

  private List<Document> findAll(final String collection) {
    final Document reply = wrapper.handleCommand(null, "find", new Document("find", collection).append("filter", new Document()), null,
        null);
    final List<Document> documents = new ArrayList<>();
    for (final Object document : (List<?>) ((Document) reply.get("cursor")).get("firstBatch"))
      documents.add((Document) document);
    return documents;
  }
}
