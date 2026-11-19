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

import com.arcadedb.GlobalConfiguration;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12069: {@code find().sort()} must follow the BSON comparison order of {@code mongod} (type brackets, an array by its
 * smallest element ascending and its largest descending, an empty array before null, a dotted path through an array of documents,
 * numbers of different types by exact value), not the order of SQL {@code ORDER BY}. Every expectation is the answer of a MongoDB 7
 * server, measured in the issue.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12069MongoSortOrderTest extends BaseMongoServerTest {
  private MongoClient client;
  private int         collections;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    getDatabase(0);
    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  private List<Document> mixedTypes() {
    final List<Document> documents = new ArrayList<>();
    documents.add(new Document("_id", 1).append("v", 1));
    documents.add(new Document("_id", 2).append("v", "a"));
    documents.add(new Document("_id", 3).append("v", null));
    documents.add(new Document("_id", 4));
    documents.add(new Document("_id", 5).append("v", true));
    documents.add(new Document("_id", 6).append("v", new Document("x", 1)));
    documents.add(new Document("_id", 7).append("v", new Date(1577836800000L)));
    documents.add(new Document("_id", 8).append("v", new ObjectId("5f0000000000000000000001")));
    documents.add(new Document("_id", 9).append("v", "10"));
    documents.add(new Document("_id", 10).append("v", false));
    return documents;
  }

  @Test
  void mixedTypesSortByTypeBracketAscending() {
    final MongoCollection<Document> collection = collection(mixedTypes());
    assertThat(ids(collection.find().sort(Document.parse("{v: 1, _id: 1}")))).containsExactly(3, 4, 1, 9, 2, 6, 8, 10, 5, 7);
  }

  @Test
  void mixedTypesSortByTypeBracketDescending() {
    final MongoCollection<Document> collection = collection(mixedTypes());
    assertThat(ids(collection.find().sort(Document.parse("{v: -1, _id: 1}")))).containsExactly(7, 5, 10, 8, 6, 2, 9, 1, 3, 4);
  }

  @Test
  void numbersSortBeforeStringsBeforeBooleans() {
    final MongoCollection<Document> collection = collection(List.of(Document.parse("{_id: 1, v: true}"), Document.parse("{_id: 2, v: 5}"),
        Document.parse("{_id: 3, v: 'x'}")));
    assertThat(ids(collection.find().sort(Document.parse("{v: 1}")))).containsExactly(2, 3, 1);
  }

  @Test
  void anArraySortsByItsSmallestElementAscendingAndItsLargestDescending() {
    final MongoCollection<Document> collection = collection(
        List.of(Document.parse("{_id: 1, v: [5, 1]}"), Document.parse("{_id: 2, v: [2, 3]}"), Document.parse("{_id: 3, v: 4}"),
            Document.parse("{_id: 4, v: []}"), Document.parse("{_id: 5}")));
    assertThat(ids(collection.find().sort(Document.parse("{v: 1, _id: 1}")))).containsExactly(4, 5, 1, 2, 3);
    assertThat(ids(collection.find().sort(Document.parse("{v: -1, _id: 1}")))).containsExactly(1, 3, 2, 5, 4);
  }

  @Test
  void aDottedPathThroughAnArrayOfDocumentsSortsByTheValuesItReaches() {
    final MongoCollection<Document> collection = collection(
        List.of(Document.parse("{_id: 1, v: [{x: 5}, {x: 1}]}"), Document.parse("{_id: 2, v: [{x: 3}]}"), Document.parse("{_id: 3, v: {x: 2}}")));
    assertThat(ids(collection.find().sort(Document.parse("{'v.x': 1, _id: 1}")))).containsExactly(1, 3, 2);
    assertThat(ids(collection.find().sort(Document.parse("{'v.x': -1, _id: 1}")))).containsExactly(1, 2, 3);
  }

  @Test
  void numbersOfDifferentTypesSortByExactValue() {
    final MongoCollection<Document> collection = collection(List.of(new Document("_id", 1).append("v", (1L << 62) + 1),
        new Document("_id", 2).append("v", (double) (1L << 62)),
        new Document("_id", 3).append("v", new Decimal128(new BigDecimal("4611686018427387904.5")))));
    assertThat(ids(collection.find().sort(Document.parse("{v: 1}")))).containsExactly(2, 3, 1);
  }

  @Test
  void skipAndLimitPaginateTheBsonOrder() {
    final MongoCollection<Document> collection = collection(mixedTypes());
    assertThat(ids(collection.find().sort(Document.parse("{v: 1, _id: 1}")).skip(2).limit(3))).containsExactly(1, 9, 2);
    assertThat(ids(collection.find().sort(Document.parse("{v: -1, _id: 1}")).limit(2))).containsExactly(7, 5);
    // a filter is applied before the order, skip and limit
    assertThat(ids(collection.find(Document.parse("{_id: {$gte: 3}}")).sort(Document.parse("{v: 1, _id: 1}")).skip(1).limit(3)))
        .containsExactly(4, 9, 6);
  }

  @Test
  void anEmbeddedDocumentFieldSortsInsideTheDocument() {
    final MongoCollection<Document> collection = collection(
        List.of(Document.parse("{_id: 1, o: {n: 'b'}}"), Document.parse("{_id: 2, o: {n: 'a'}}"), Document.parse("{_id: 3, o: 7}")));
    assertThat(ids(collection.find().sort(Document.parse("{'o.n': 1}")))).containsExactly(3, 2, 1);
  }

  @Test
  void aSortOnTheIdOrdersEveryKindOfId() {
    final MongoCollection<Document> collection = collection(
        List.of(new Document("_id", "b"), new Document("_id", 2), new Document("_id", new ObjectId("5f0000000000000000000001")),
            new Document("_id", "a"), new Document("_id", 10)));
    final List<Object> ids = new ArrayList<>();
    for (final Document d : collection.find().sort(Document.parse("{_id: 1}")))
      ids.add(d.get("_id"));
    assertThat(ids).containsExactly(2, 10, "a", "b", new ObjectId("5f0000000000000000000001"));
  }

  @Test
  void anInvalidSortDirectionIsRefused() {
    final MongoCollection<Document> collection = collection(List.of(Document.parse("{_id: 1, v: 1}")));
    assertThatThrownBy(() -> collection.find().sort(Document.parse("{v: 0}")).first()).isInstanceOf(MongoException.class);
    assertThatThrownBy(() -> collection.find().sort(Document.parse("{v: 'up'}")).first()).isInstanceOf(MongoException.class);
  }

  @Test
  void fieldsThroughTheSameArrayTakeTheirValuesFromTheSameElement() {
    // A's candidate keys are (1, 9) and (2, 0): its key is (1, 9), not the (1, 0) of two independent minima
    final MongoCollection<Document> collection = collection(
        List.of(Document.parse("{_id: 'A', a: [{x: 1, y: 9}, {x: 2, y: 0}]}"), Document.parse("{_id: 'B', a: [{x: 1, y: 5}]}")));
    assertThat(ids(collection.find().sort(Document.parse("{'a.x': 1, 'a.y': 1}")))).containsExactly("B", "A");
    assertThat(ids(collection.find().sort(Document.parse("{'a.x': 1, 'a.y': 1}")).limit(1))).containsExactly("B");
    // descending on the second field: A's key is (1, 9), B's (1, 5)
    assertThat(ids(collection.find().sort(Document.parse("{'a.x': 1, 'a.y': -1}")))).containsExactly("A", "B");
  }

  @Test
  void pathsThroughTwoDifferentArraysAreRefused() {
    final MongoCollection<Document> collection = collection(List.of(Document.parse("{_id: 1, a: [1, 2], b: [3, 4]}")));
    assertThatThrownBy(() -> collection.find().sort(Document.parse("{a: 1, b: 1}")).first()).isInstanceOf(MongoException.class);
    // one array and a scalar are fine
    final MongoCollection<Document> fine = collection(List.of(Document.parse("{_id: 1, a: [1, 2], b: 3}")));
    assertThat(ids(fine.find().sort(Document.parse("{a: 1, b: 1}")))).containsExactly(1);
  }

  @Test
  void aScalarElementOfAnArrayIsNullOnADottedPath() {
    final MongoCollection<Document> collection = collection(List.of(Document.parse("{_id: 'A', a: [1, {b: 5}]}"), Document.parse("{_id: 'B', a: {b: 2}}")));
    assertThat(ids(collection.find().sort(Document.parse("{'a.b': 1}")))).containsExactly("A", "B");
  }

  @Test
  void minKeySortsBelowAnEmptyArray() {
    final MongoCollection<Document> collection = collection(List.of(new Document("_id", 1).append("v", new MinKey()), new Document("_id", 2).append("v", List.of()),
        new Document("_id", 3).append("v", null), new Document("_id", 4).append("v", new MaxKey())));
    assertThat(ids(collection.find().sort(Document.parse("{v: 1}")))).containsExactly(1, 2, 3, 4);
    assertThat(ids(collection.find().sort(Document.parse("{v: -1}")))).containsExactly(4, 3, 2, 1);
  }

  @Test
  void stringsSortByCodePoint() {
    // U+E000 is one UTF-16 unit, U+10000 a surrogate pair: by code point (UTF-8 bytes) U+E000 is first, by UTF-16 units it is not
    final MongoCollection<Document> collection = collection(
        List.of(new Document("_id", 1).append("v", "𐀀"), new Document("_id", 2).append("v", ""), new Document("_id", 3).append("v", "a")));
    assertThat(ids(collection.find().sort(Document.parse("{v: 1}")))).containsExactly(3, 2, 1);
    assertThat(ids(collection.find(Document.parse("{v: {$gt: ''}}")))).containsExactly(1);
  }

  @Test
  void aSortThatIsNotADocumentIsRefused() {
    final MongoCollection<Document> collection = collection(List.of(Document.parse("{_id: 1, v: 1}")));
    assertThatThrownBy(() -> client.getDatabase(getDatabaseName()).runCommand(new Document("find", collection.getNamespace().getCollectionName()).append("sort", "v")))
        .isInstanceOfSatisfying(MongoException.class, e -> assertThat(e.getCode()).isEqualTo(14));
  }

  private static List<Object> ids(final FindIterable<Document> found) {
    final List<Object> ids = new ArrayList<>();
    for (final Document d : found)
      ids.add(d.get("_id"));
    return ids;
  }

  private MongoCollection<Document> collection(final List<Document> documents) {
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("s" + (collections++));
    collection.insertMany(documents);
    return collection;
  }
}
