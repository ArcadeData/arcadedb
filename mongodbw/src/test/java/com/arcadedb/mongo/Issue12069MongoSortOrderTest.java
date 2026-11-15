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
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.MongoServerError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12069: a find with a sort was answered by SQL ORDER BY, which does not follow MongoDB's BSON comparison order: a field
 * holding values of several types, an array (sorted by its smallest element ascending, its largest descending), a dotted path into
 * an array of documents and numbers of different types beyond 2^53 came back in an order mongod never produces, and skip/limit paged
 * that order. The expected orders below are mongod 7's.
 */
class Issue12069MongoSortOrderTest {
  private static final String DB_PATH = "./target/databases/issue12069";

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
  void mixedTypesSortByTypeBracketThenValue() {
    insert("Mixed", doc(1, 1), doc(2, "a"), doc(3, null), new Document("_id", 4), doc(5, true), doc(6, new Document("x", 1)),
        doc(7, new Date(1_577_836_800_000L)), doc(8, new ObjectId("5f0000000000000000000001")), doc(9, "10"), doc(10, false));

    assertThat(ids("Mixed", sort("v", 1), 0, 0)).containsExactly(3, 4, 1, 9, 2, 6, 8, 10, 5, 7);
    assertThat(ids("Mixed", sort("v", -1), 0, 0)).containsExactly(7, 5, 10, 8, 6, 2, 9, 1, 3, 4);
  }

  @Test
  void anArraySortsByItsSmallestElementAscendingAndItsLargestDescending() {
    insert("Arrays", doc(1, List.of(5, 1)), doc(2, List.of(2, 3)), doc(3, 4), doc(4, List.of()), new Document("_id", 5));

    assertThat(ids("Arrays", sort("v", 1), 0, 0)).containsExactly(4, 5, 1, 2, 3);
    assertThat(ids("Arrays", sort("v", -1), 0, 0)).containsExactly(1, 3, 2, 5, 4);
  }

  @Test
  void aDottedPathIntoAnArrayOfDocumentsSortsByTheValuesItReaches() {
    insert("Dotted", doc(1, List.of(new Document("x", 5), new Document("x", 1))), doc(2, List.of(new Document("x", 3))),
        doc(3, new Document("x", 2)));

    assertThat(ids("Dotted", new Document("v.x", 1).append("_id", 1), 0, 0)).containsExactly(1, 3, 2);
    assertThat(ids("Dotted", new Document("v.x", -1).append("_id", 1), 0, 0)).containsExactly(1, 2, 3);
  }

  @Test
  void numbersOfDifferentTypesSortByExactValue() {
    insert("Numbers", doc(1, (1L << 62) + 1), doc(2, (double) (1L << 62)), doc(3, new Decimal128(new BigDecimal("4611686018427387904.5"))));

    assertThat(ids("Numbers", sort("v", 1), 0, 0)).containsExactly(2, 3, 1);
  }

  @Test
  void skipAndLimitPageTheMongoOrderWithAFilterAndWithout() {
    final List<Document> documents = new ArrayList<>();
    for (int i = 0; i < 20; i++)
      documents.add(doc(i, i % 2 == 0 ? List.of(20 - i, 100) : (Object) (20 - i)));
    insert("Paged", documents.toArray(new Document[0]));

    // ascending by v (the smallest element of an array): _id 19 has 1, 18 has [2, 100], 17 has 3...
    assertThat(ids("Paged", sort("v", 1), 2, 3)).containsExactly(17, 16, 15);
    assertThat(ids("Paged", sort("v", 1), 0, 0)).hasSize(20).startsWith(19, 18, 17);
    // descending by the largest element: every even _id holds 100
    assertThat(ids("Paged", new Document("v", -1).append("_id", 1), 0, 3)).containsExactly(0, 2, 4);

    final Document reply = wrapper.handleCommand(null, "find", new Document("find", "Paged").append("filter",
            new Document("_id", new Document("$gte", 10))).append("sort", sort("v", 1)).append("skip", 1).append("limit", 2), null,
        null);
    assertThat(idsOf(reply)).containsExactly(18, 17);
  }

  @Test
  void aSortDirectionOtherThanOneOrMinusOneIsRefusedLikeMongod() {
    insert("Refused", doc(1, 1));
    assertThatThrownBy(() -> ids("Refused", sort("v", 0), 0, 0)).isInstanceOf(MongoServerError.class)
        .satisfies(e -> assertThat(((MongoServerError) e).getCode()).isEqualTo(15975));
    assertThatThrownBy(() -> ids("Refused", new Document("v", "asc"), 0, 0)).isInstanceOf(MongoServerError.class)
        .satisfies(e -> assertThat(((MongoServerError) e).getCode()).isEqualTo(15974));
  }

  private static Document doc(final int id, final Object value) {
    return new Document("_id", id).append("v", value);
  }

  private static Document sort(final String field, final int direction) {
    return new Document(field, direction).append("_id", 1);
  }

  private void insert(final String collection, final Document... documents) {
    database.getSchema().getOrCreateDocumentType(collection);
    new MongoDBCollectionWrapper(database, collection).insertDocuments(List.of(documents));
  }

  private List<Object> ids(final String collection, final Document sort, final int skip, final int limit) {
    return idsOf(wrapper.handleCommand(null, "find",
        new Document("find", collection).append("filter", new Document()).append("sort", sort).append("skip", skip).append("limit", limit),
        null, null));
  }

  private static List<Object> idsOf(final Document reply) {
    final List<Object> ids = new ArrayList<>();
    for (final Object document : (List<?>) ((Document) reply.get("cursor")).get("firstBatch"))
      ids.add(((Document) document).get("_id"));
    return ids;
  }
}
