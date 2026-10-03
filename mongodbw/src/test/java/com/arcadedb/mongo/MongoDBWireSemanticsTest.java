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
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the MongoDB wire semantics of the mongodbw plugin: null-aware {@code $ne/$nin/$in/$not} (#9061),
 * dotted {@code $set} (#9063), projection and record metadata (#9068), regular expressions (#9069), a duplicated {@code _id}
 * (#9060) and a {@code replaceOne} that keeps the {@code _id} (#9064). Every expected answer is the one MongoDB 7 gives.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBWireSemanticsTest extends BaseMongoServerTest {
  private MongoClient               client;
  private MongoCollection<Document> collection;

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
    client.getDatabase(getDatabaseName()).createCollection("c");
    collection = client.getDatabase(getDatabaseName()).getCollection("c");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  private void seedAges() {
    collection.insertMany(List.of(Document.parse("{_id: 1, age: 30}"), Document.parse("{_id: 2, age: 25}"),
        Document.parse("{_id: 3, age: null}"), Document.parse("{_id: 4}")));
  }

  private List<Object> ids(final String filter) {
    final List<Object> ids = new ArrayList<>();
    for (final Document d : collection.find(Document.parse(filter)).sort(new Document("_id", 1)))
      ids.add(d.get("_id"));
    return ids;
  }

  @Test
  void notEqualMatchesNullAndMissing() {
    seedAges();
    assertThat(ids("{age: {$ne: 30}}")).containsExactly(2, 3, 4);
    assertThat(ids("{age: {$nin: [25]}}")).containsExactly(1, 3, 4);
    assertThat(ids("{age: {$not: {$gt: 26}}}")).containsExactly(2, 3, 4);
    assertThat(ids("{age: {$not: {$eq: 30}}}")).containsExactly(2, 3, 4);
    assertThat(ids("{age: {$in: [25, null]}}")).containsExactly(2, 3, 4);
    assertThat(ids("{age: {$nin: [25, null]}}")).containsExactly(1);
    assertThat(ids("{nosuch: {$ne: 1}}")).containsExactly(1, 2, 3, 4);
    assertThat(ids("{age: {$ne: null}}")).containsExactly(1, 2);
  }

  @Test
  void deleteManyWithNotEqualDeletesNullAndMissing() {
    seedAges();
    assertThat(collection.deleteMany(Document.parse("{age: {$ne: 30}}")).getDeletedCount()).isEqualTo(3);
    assertThat(ids("{}")).containsExactly(1);
  }

  @Test
  void setOnDottedPathUpdatesTheEmbeddedDocument() {
    collection.insertMany(List.of(Document.parse("{_id: 1, addr: {city: 'X', zip: '1'}}"), Document.parse("{_id: 2}")));

    collection.updateOne(new Document("_id", 1), Document.parse("{$set: {'addr.city': 'Z'}}"));
    collection.updateOne(new Document("_id", 2), Document.parse("{$set: {'a.b.c': 1}}"));

    final Document first = collection.find(new Document("_id", 1)).first();
    assertThat(first.get("addr", Document.class)).containsEntry("city", "Z").containsEntry("zip", "1");
    assertThat(first.containsKey("addr.city")).isFalse();
    assertThat(collection.countDocuments(Document.parse("{'addr.city': 'Z'}"))).isEqualTo(1);
    assertThat(collection.countDocuments(Document.parse("{'addr.city': 'X'}"))).isZero();

    final Document second = collection.find(new Document("_id", 2)).first();
    assertThat(second.containsKey("a.b.c")).isFalse();
    assertThat(second.get("a", Document.class).get("b", Document.class)).containsEntry("c", 1);
  }

  @Test
  void unsetAndIncOnDottedPaths() {
    collection.insertOne(Document.parse("{_id: 1, addr: {city: 'X', zip: '1', n: 1}}"));
    collection.updateOne(new Document("_id", 1), Document.parse("{$set: {'addr.city': 'Z'}, $unset: {'addr.zip': ''}, $inc: {'addr.n': 2}}"));

    final Document doc = collection.find(new Document("_id", 1)).first();
    assertThat(doc.get("addr", Document.class)).containsEntry("city", "Z").containsEntry("n", 3).doesNotContainKey("zip");
  }

  @Test
  void projectionAndNoRecordMetadata() {
    collection.insertOne(Document.parse("{_id: 1, name: 'a', age: 30, tags: ['x']}"));
    collection.insertOne(new Document("_id", 99));

    final Document all = collection.find(new Document("_id", 1)).first();
    assertThat(all.keySet()).containsExactlyInAnyOrder("_id", "name", "age", "tags");
    assertThat(collection.find(new Document("_id", 99)).first().keySet()).containsExactly("_id");

    assertThat(collection.find(new Document("_id", 1)).projection(Document.parse("{name: 1}")).first().keySet()).containsExactly("_id", "name");
    assertThat(collection.find(new Document("_id", 1)).projection(Document.parse("{name: 1, _id: 0}")).first().keySet()).containsExactly("name");
    assertThat(collection.find(new Document("_id", 1)).projection(Document.parse("{age: 0, tags: 0}")).first().keySet())
        .containsExactlyInAnyOrder("_id", "name");

    assertThatThrownBy(() -> collection.find().projection(Document.parse("{name: 1, age: 0}")).first()).isInstanceOf(RuntimeException.class);
  }

  @Test
  void regularExpressions() {
    collection.insertMany(List.of(Document.parse("{_id: 1, name: 'alice'}"), Document.parse("{_id: 2, name: 'Eve'}"),
        Document.parse("{_id: 3, name: 'bob'}"), Document.parse("{_id: 4}")));

    assertThat(ids("{name: {$regex: '^a'}}")).containsExactly(1);
    assertThat(ids("{name: {$regex: '^e', $options: 'i'}}")).containsExactly(2);
    assertThat(ids("{name: {$regex: 'b'}}")).containsExactly(3);

    final List<Object> literal = new ArrayList<>();
    for (final Document d : collection.find(new Document("name", Pattern.compile("^a"))))
      literal.add(d.get("_id"));
    assertThat(literal).containsExactly(1);
    assertThat(collection.countDocuments(new Document("name", Pattern.compile("^A", Pattern.CASE_INSENSITIVE)))).isEqualTo(1);

    assertThat(ids("{name: {$not: {$regex: '^a'}}}")).containsExactly(2, 3, 4);

    assertThat(collection.deleteMany(new Document("name", Pattern.compile("^a"))).getDeletedCount()).isEqualTo(1);
    assertThat(collection.countDocuments()).isEqualTo(3);
  }

  @Test
  void duplicatedIdIsRejected() {
    collection.insertOne(new Document("_id", 1));

    assertThatThrownBy(() -> collection.insertOne(new Document("_id", 1))).isInstanceOf(MongoWriteException.class)
        .hasMessageContaining("E11000");
    assertThat(collection.countDocuments(new Document("_id", 1))).isEqualTo(1);

    assertThatThrownBy(() -> collection.insertMany(List.of(new Document("_id", 2), new Document("_id", 2), new Document("_id", 3))))
        .isInstanceOf(MongoBulkWriteException.class);
    // ordered: stops at the first failure
    assertThat(collection.countDocuments()).isEqualTo(2);

    assertThatThrownBy(() -> collection.insertMany(List.of(new Document("_id", 3), new Document("_id", 3), new Document("_id", 4)),
        new InsertManyOptions().ordered(false))).isInstanceOf(MongoBulkWriteException.class);
    // unordered: the others are kept
    assertThat(collection.countDocuments()).isEqualTo(4);
  }

  @Test
  void insertCreatesTheCollectionAndItsIdIndex() {
    final MongoCollection<Document> fresh = client.getDatabase(getDatabaseName()).getCollection("fresh");
    fresh.insertOne(new Document("_id", 1));
    assertThatThrownBy(() -> fresh.insertOne(new Document("_id", 1))).isInstanceOf(MongoWriteException.class);
    assertThat(fresh.countDocuments()).isEqualTo(1);
  }

  @Test
  void replaceOneKeepsTheId() {
    collection.insertOne(Document.parse("{_id: 2, name: 'bob', age: 25}"));

    assertThat(collection.replaceOne(new Document("_id", 2), Document.parse("{name: 'bob2'}")).getMatchedCount()).isEqualTo(1);
    assertThat(collection.countDocuments(new Document("_id", 2))).isEqualTo(1);
    assertThat(collection.replaceOne(new Document("_id", 2), Document.parse("{name: 'bob3'}")).getMatchedCount()).isEqualTo(1);

    final Document stored = collection.find(new Document("_id", 2)).first();
    assertThat(stored.keySet()).containsExactlyInAnyOrder("_id", "name");
    assertThat(stored.getString("name")).isEqualTo("bob3");
  }

  @Test
  void replaceOneCannotChangeTheId() {
    collection.insertOne(Document.parse("{_id: 2, name: 'bob'}"));
    assertThatThrownBy(() -> collection.replaceOne(new Document("_id", 2), Document.parse("{_id: 5, name: 'x'}")))
        .isInstanceOf(RuntimeException.class);
    assertThat(collection.countDocuments(new Document("_id", 2))).isEqualTo(1);
  }
}
