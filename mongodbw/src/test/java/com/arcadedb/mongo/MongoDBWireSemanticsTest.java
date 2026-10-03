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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

  @Test
  void setBelowAScalarOrArrayIsRefusedAndKeepsTheValue() {
    collection.insertOne(Document.parse("{_id: 1, name: 'bob', tags: ['a', 'b']}"));

    assertThatThrownBy(() -> collection.updateOne(new Document("_id", 1), Document.parse("{$set: {'name.first': 'x'}}")))
        .isInstanceOf(MongoCommandException.class);
    assertThatThrownBy(() -> collection.updateOne(new Document("_id", 1), Document.parse("{$set: {'tags.x': 1}}")))
        .isInstanceOf(MongoCommandException.class);

    final Document doc = collection.find(new Document("_id", 1)).first();
    assertThat(doc.getString("name")).isEqualTo("bob");
    assertThat(doc.getList("tags", String.class)).containsExactly("a", "b");
  }

  @Test
  void setThroughAnArrayIndex() {
    collection.insertOne(Document.parse("{_id: 1, items: [{n: 1}, {n: 2}]}"));
    collection.updateOne(new Document("_id", 1), Document.parse("{$set: {'items.1.n': 9}}"));
    final List<Document> items = collection.find(new Document("_id", 1)).first().getList("items", Document.class);
    assertThat(items.get(0).get("n")).isEqualTo(1);
    assertThat(items.get(1).get("n")).isEqualTo(9);
  }

  @Test
  void notOfNullAwareOperators() {
    seedAges();
    assertThat(ids("{age: {$not: {$ne: 30}}}")).containsExactly(1);
    assertThat(ids("{age: {$not: {$nin: [25]}}}")).containsExactly(2);
    assertThat(ids("{age: {$in: [null]}}")).containsExactly(3, 4);
    assertThat(ids("{age: {$nin: [null]}}")).containsExactly(1, 2);
  }

  @Test
  void invalidRegularExpressionsAreRefused() {
    collection.insertOne(Document.parse("{_id: 1, name: 'a'}"));
    assertThatThrownBy(() -> ids("{name: {$regex: 'a)|(b'}}")).isInstanceOf(MongoQueryException.class);
    assertThatThrownBy(() -> ids("{name: {$regex: 'a', $options: 'z'}}")).isInstanceOf(MongoQueryException.class);
  }

  @Test
  void replaceOneWithTheSameIdIsAllowed() {
    collection.insertOne(Document.parse("{_id: 2, name: 'bob'}"));
    assertThat(collection.replaceOne(new Document("_id", 2), Document.parse("{_id: 2, name: 'x'}")).getMatchedCount()).isEqualTo(1);
    assertThat(collection.find(new Document("_id", 2)).first().getString("name")).isEqualTo("x");
  }

  @Test
  void duplicatedIdOnATypeCreatedBySql() {
    getServerDatabase(0, getDatabaseName()).getSchema().createDocumentType("bysql");
    final MongoCollection<Document> sqlCollection = client.getDatabase(getDatabaseName()).getCollection("bysql");
    // the plugin adds the unique index on the first insert, so both a batch and a later insert are checked
    assertThatThrownBy(() -> sqlCollection.insertMany(List.of(new Document("_id", 1), new Document("_id", 1))))
        .isInstanceOf(MongoBulkWriteException.class);
    sqlCollection.insertOne(new Document("_id", 5));
    assertThatThrownBy(() -> sqlCollection.insertOne(new Document("_id", 5))).isInstanceOf(MongoWriteException.class);
  }

  @Test
  void projectionOnDottedPath() {
    collection.insertOne(Document.parse("{_id: 1, addr: {city: 'X', zip: '1'}, n: 1}"));
    final Document projected = collection.find().projection(Document.parse("{'addr.city': 1}")).first();
    assertThat(projected.keySet()).containsExactly("_id", "addr");
    assertThat(projected.get("addr", Document.class).keySet()).containsExactly("city");
  }

  @Test
  void standaloneIncAndUnsetOnDottedPaths() {
    collection.insertMany(List.of(Document.parse("{_id: 1, addr: {n: 1, zip: '1'}}"), Document.parse("{_id: 2}")));

    collection.updateOne(new Document("_id", 1), Document.parse("{$inc: {'addr.n': 2}}"));
    collection.updateOne(new Document("_id", 1), Document.parse("{$unset: {'addr.zip': ''}}"));
    collection.updateOne(new Document("_id", 2), Document.parse("{$inc: {'a.b': 1}}"));

    final Document first = collection.find(new Document("_id", 1)).first();
    assertThat(first.get("addr", Document.class)).containsEntry("n", 3).doesNotContainKey("zip");
    assertThat(collection.find(new Document("_id", 2)).first().get("a", Document.class)).containsEntry("b", 1);
  }

  @Test
  void setOnAHugeArrayIndexIsRefused() {
    collection.insertOne(Document.parse("{_id: 1, tags: ['a']}"));
    assertThatThrownBy(() -> collection.updateOne(new Document("_id", 1), Document.parse("{$set: {'tags.999999999': 1}}")))
        .isInstanceOf(MongoCommandException.class);
    assertThat(collection.find(new Document("_id", 1)).first().getList("tags", String.class)).containsExactly("a");
  }

  @Test
  void duplicateOnAUserDefinedUniqueIndexIsNotReportedAsId() {
    collection.createIndex(new Document("email", 1), new IndexOptions().unique(true));
    collection.insertOne(Document.parse("{_id: 1, email: 'a@x'}"));

    assertThatThrownBy(() -> collection.insertOne(Document.parse("{_id: 2, email: 'a@x'}"))).isInstanceOf(MongoWriteException.class)
        .hasMessageContaining("E11000").hasMessageContaining("a@x");
  }

  @Test
  void inlineCommentsModeRegexIsACleanError() {
    collection.insertOne(Document.parse("{_id: 1, name: 'a'}"));
    assertThatThrownBy(() -> ids("{name: {$regex: '(?x) a # note'}}")).isInstanceOf(MongoQueryException.class);
  }

  @Test
  void duplicatedIdOnATypeThatCannotGetTheIndex() {
    // the type already holds duplicated _id values, so the unique index cannot be built: the insert checks by hand
    final var db = getServerDatabase(0, getDatabaseName());
    db.getSchema().createDocumentType("dups");
    db.transaction(() -> {
      db.command("sql", "insert into dups set _id = 1");
      db.command("sql", "insert into dups set _id = 1");
    });
    final MongoCollection<Document> dups = client.getDatabase(getDatabaseName()).getCollection("dups");

    assertThatThrownBy(() -> dups.insertOne(new Document("_id", 1))).isInstanceOf(MongoWriteException.class);
    assertThatThrownBy(() -> dups.insertMany(List.of(new Document("_id", 7), new Document("_id", 1)))).isInstanceOf(MongoBulkWriteException.class);
    assertThat(dups.countDocuments(new Document("_id", 7))).isEqualTo(1);
  }

  @Test
  void replaceOneAcceptsANumericallyEqualId() {
    collection.insertOne(Document.parse("{_id: 2, name: 'bob'}"));
    assertThat(collection.replaceOne(new Document("_id", 2), new Document("_id", 2L).append("name", "x")).getMatchedCount()).isEqualTo(1);
  }

  @Test
  void incOnANonNumericValueIsATypeError() {
    collection.insertOne(Document.parse("{_id: 1, s: 'text'}"));
    assertThatThrownBy(() -> collection.updateOne(new Document("_id", 1), Document.parse("{$inc: {'s.x': 1}}")))
        .isInstanceOf(MongoCommandException.class);
  }

  @Test
  void notEqualInsideOrAndNestedAnd() {
    seedAges();
    assertThat(ids("{$or: [{age: {$ne: 30}}, {age: 30}]}")).containsExactly(1, 2, 3, 4);
    assertThat(ids("{$and: [{age: {$ne: 30}}, {age: {$nin: [25]}}]}")).containsExactly(3, 4);
  }

  @Test
  void rangeAndSortOnNumericIdsWithTheIdIndex() {
    collection.insertMany(List.of(new Document("_id", 100), new Document("_id", 9), new Document("_id", 2), new Document("_id", 10)));

    assertThat(ids("{_id: {$gt: 5}}")).containsExactly(9, 10, 100);
    assertThat(ids("{_id: {$lt: 10}}")).containsExactly(2, 9);
    assertThat(ids("{_id: {$gte: 9, $lte: 10}}")).containsExactly(9, 10);

    final List<Object> sorted = new ArrayList<>();
    for (final Document d : collection.find().sort(new Document("_id", 1)))
      sorted.add(d.get("_id"));
    assertThat(sorted).containsExactly(2, 9, 10, 100);
  }

  @Test
  void updateManyWithDottedSetAndDuplicateInUnorderedBatch() {
    collection.insertMany(List.of(Document.parse("{_id: 1, a: {n: 1}}"), Document.parse("{_id: 2, a: {n: 2}}")));
    assertThat(collection.updateMany(new Document(), Document.parse("{$set: {'a.m': 5}}")).getModifiedCount()).isEqualTo(2);
    assertThat(ids("{'a.m': 5}")).containsExactly(1, 2);

    assertThatThrownBy(() -> collection.insertMany(
        List.of(new Document("_id", 3), new Document("_id", 1), new Document("_id", 4), new Document("_id", 2)),
        new InsertManyOptions().ordered(false))).isInstanceOfSatisfying(MongoBulkWriteException.class,
        e -> assertThat(e.getWriteErrors()).extracting(w -> w.getIndex()).containsExactly(1, 3));
    assertThat(collection.countDocuments()).isEqualTo(4);
  }

  @Test
  void mixedIdKindsKeepUniqueness() {
    collection.insertOne(new Document("_id", 1));
    collection.insertOne(new Document("_id", "abc"));
    assertThatThrownBy(() -> collection.insertOne(new Document("_id", 1))).isInstanceOf(MongoWriteException.class);
    assertThatThrownBy(() -> collection.insertOne(new Document("_id", "abc"))).isInstanceOf(MongoWriteException.class);
    assertThat(collection.countDocuments()).isEqualTo(2);
  }

  @Test
  void upsertOnAFreshCollection() {
    collection.updateOne(new Document("_id", 7), Document.parse("{$set: {a: 1}}"), new UpdateOptions().upsert(true));
    collection.updateOne(new Document("k", "x"), Document.parse("{$set: {a: 1}}"), new UpdateOptions().upsert(true));
    assertThat(collection.countDocuments()).isEqualTo(2);
    assertThatThrownBy(() -> collection.insertOne(new Document("_id", 7))).isInstanceOf(MongoWriteException.class);
  }

  @Test
  void upsertsKeepTheNumericIdIndexNumeric() {
    collection.insertMany(List.of(new Document("_id", 2), new Document("_id", 9), new Document("_id", 10)));

    // matches an existing document, has an operator _id, or inserts with a generated id: none may break numeric ordering
    collection.updateOne(new Document("_id", 9), Document.parse("{$set: {a: 1}}"), new UpdateOptions().upsert(true));
    collection.updateOne(Document.parse("{_id: {$gt: 100}}"), Document.parse("{$set: {a: 1}}"), new UpdateOptions().upsert(false));
    assertThat(ids("{_id: {$gt: 5}}")).containsExactly(9, 10);
  }

  @Test
  void mixedIntegralAndFloatingIdsStayNumeric() {
    collection.insertOne(new Document("_id", 10));
    collection.insertOne(new Document("_id", 2.5));
    collection.insertOne(new Document("_id", 9));
    assertThat(ids("{_id: {$gt: 5}}")).containsExactly(9, 10);
    assertThatThrownBy(() -> collection.insertOne(new Document("_id", 10.0))).isInstanceOf(MongoWriteException.class);
  }

  @Test
  void concurrentFirstInsertsWithMixedIdKinds() throws Exception {
    final MongoCollection<Document> fresh = client.getDatabase(getDatabaseName()).getCollection("racing");
    final ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < 4; t++) {
        final int thread = t;
        futures.add(pool.submit(() -> {
          for (int i = 0; i < 25; i++) {
            final Document doc = thread % 2 == 0 ? new Document("_id", thread * 1000 + i) : new Document("_id", "s" + thread + "-" + i);
            // a WriteConflict (112) is the retryable answer to two transactions on the same page: a client retries it
            for (int attempt = 0; ; attempt++)
              try {
                fresh.insertOne(doc);
                break;
              } catch (final MongoCommandException e) {
                if (e.getErrorCode() != 112 || attempt > 20)
                  throw e;
              }
          }
        }));
      }
      for (final Future<?> f : futures)
        f.get();
    } finally {
      pool.shutdownNow();
    }
    assertThat(fresh.countDocuments()).isEqualTo(100);
  }

  @Test
  void exclusionProjectionMayIncludeTheId() {
    collection.insertOne(Document.parse("{_id: 1, a: 1, b: 2}"));
    assertThat(collection.find().projection(Document.parse("{a: 0, _id: 1}")).first().keySet()).containsExactlyInAnyOrder("_id", "b");
    assertThat(collection.find().projection(Document.parse("{_id: 1}")).first().keySet()).containsExactly("_id");
  }

  @Test
  void aFailedIdIndexBuildIsNotRetriedOnEveryInsert() {
    final var db = getServerDatabase(0, getDatabaseName());
    db.getSchema().createDocumentType("gaveup");
    db.transaction(() -> {
      db.command("sql", "insert into gaveup set _id = 1");
      db.command("sql", "insert into gaveup set _id = 1");
    });
    final MongoCollection<Document> gaveUp = client.getDatabase(getDatabaseName()).getCollection("gaveup");

    for (int i = 10; i < 30; i++)
      gaveUp.insertOne(new Document("_id", i));
    assertThatThrownBy(() -> gaveUp.insertOne(new Document("_id", 15))).isInstanceOf(MongoWriteException.class);
    assertThat(db.getSchema().getType("gaveup").getAllIndexes(false)).isEmpty();
  }

  @Test
  void idIsImmutableUnderSetAndUnset() {
    collection.insertOne(Document.parse("{_id: 1, a: 1}"));

    assertThatThrownBy(() -> collection.updateOne(new Document("_id", 1), Document.parse("{$set: {_id: 99}}")))
        .isInstanceOf(MongoCommandException.class);
    assertThatThrownBy(() -> collection.updateOne(new Document("_id", 1), Document.parse("{$unset: {_id: ''}}")))
        .isInstanceOf(MongoCommandException.class);
    // the same value is allowed
    collection.updateOne(new Document("_id", 1), Document.parse("{$set: {_id: 1, a: 2}}"));

    assertThat(collection.countDocuments(new Document("_id", 1))).isEqualTo(1);
    assertThat(collection.find(new Document("_id", 1)).first().getInteger("a")).isEqualTo(2);
  }

  @Test
  void upsertWithTheIdInTheUpdateKeepsTheNumericIndex() {
    collection.insertMany(List.of(new Document("_id", 2), new Document("_id", 9), new Document("_id", 10)));
    collection.updateOne(new Document("name", "x"), Document.parse("{$set: {_id: 50, name: 'x'}}"), new UpdateOptions().upsert(true));
    assertThat(ids("{_id: {$gt: 5}}")).containsExactly(9, 10, 50);
  }
}
