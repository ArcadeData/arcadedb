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
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12070: an update refuses what {@code mongod} refuses, whether or not it has a filter, and changes nothing: {@code $inc} of
 * a non-numeric or null value, {@code $inc} by a non-numeric or null amount, {@code $inc} past the range of a long, and two operators
 * on the same path. Every expectation is the answer of a MongoDB 7 server, measured in the issue.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12070MongoUpdateOperatorsTest extends BaseMongoServerTest {
  private static final Document NO_FILTER = new Document();
  private static final Document BY_ID     = new Document("_id", 1);

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

  @Test
  void incOfANonNumericValueIsATypeMismatchWithOrWithoutAFilter() {
    final List<Object> values = new ArrayList<>();
    values.add("abc");
    values.add(true);
    values.add(List.of(1));
    values.add(new Date(1577836800000L));
    values.add(new Document("x", 1));
    values.add(null);
    for (final Object value : values)
      for (final Document filter : List.of(NO_FILTER, BY_ID)) {
        final MongoCollection<Document> collection = collection(new Document("_id", 1).append("s", value));
        assertRefused(collection, filter, Document.parse("{$inc: {s: 1}}"), 14);
        assertThat(stored(collection)).as("s = %s, filter %s", value, filter.toJson()).isEqualTo(new Document("_id", 1).append("s", value));
      }
  }

  @Test
  void incByANonNumericOrNullAmountIsATypeMismatch() {
    for (final Document filter : List.of(NO_FILTER, BY_ID)) {
      final MongoCollection<Document> collection = collection(Document.parse("{_id: 1, s: 1}"));
      assertRefused(collection, filter, Document.parse("{$inc: {s: null}}"), 14);
      assertRefused(collection, filter, Document.parse("{$inc: {s: 'a'}}"), 14);
      assertThat(stored(collection).get("s")).isEqualTo(1);
    }
    // refused even when nothing matches, like mongod refuses it when it parses the update
    final MongoCollection<Document> collection = collection(Document.parse("{_id: 1, s: 1}"));
    assertRefused(collection, new Document("_id", 99), Document.parse("{$inc: {s: null}}"), 14);
  }

  @Test
  void incPastTheRangeOfALongIsRefused() {
    for (final Document filter : List.of(NO_FILTER, BY_ID)) {
      final MongoCollection<Document> collection = collection(new Document("_id", 1).append("s", Long.MAX_VALUE));
      assertRefused(collection, filter, Document.parse("{$inc: {s: 1}}"), 2);
      assertThat(stored(collection).get("s")).isEqualTo(Long.MAX_VALUE);
    }
  }

  @Test
  void anIntThatOverflowsBecomesALong() {
    for (final Document filter : List.of(NO_FILTER, BY_ID)) {
      final MongoCollection<Document> collection = collection(new Document("_id", 1).append("s", Integer.MAX_VALUE));
      collection.updateMany(filter, Document.parse("{$inc: {s: 1}}"));
      assertThat(stored(collection).get("s")).isEqualTo(Integer.MAX_VALUE + 1L);
    }
  }

  @Test
  void incOfAMissingFieldSetsIt() {
    for (final Document filter : List.of(NO_FILTER, BY_ID)) {
      final MongoCollection<Document> collection = collection(Document.parse("{_id: 1}"));
      assertThat(collection.updateMany(filter, Document.parse("{$inc: {s: 2, t: 1.5}}")).getModifiedCount()).isEqualTo(1);
      assertThat(stored(collection)).isEqualTo(Document.parse("{_id: 1, s: 2, t: 1.5}"));
    }
  }

  @Test
  void twoOperatorsOnTheSamePathAreAConflict() {
    for (final Document filter : List.of(NO_FILTER, BY_ID)) {
      final MongoCollection<Document> collection = collection(Document.parse("{_id: 1, s: 1, a: {b: 1}}"));
      assertRefused(collection, filter, Document.parse("{$set: {s: 2}, $unset: {s: ''}}"), 40);
      assertRefused(collection, filter, Document.parse("{$set: {s: 2}, $inc: {s: 1}}"), 40);
      // a path and one below it conflict too, in either order
      assertRefused(collection, filter, Document.parse("{$set: {a: 2}, $inc: {'a.b': 1}}"), 40);
      assertRefused(collection, filter, Document.parse("{$unset: {'a.b': ''}, $set: {a: 1}}"), 40);
      assertThat(stored(collection)).isEqualTo(Document.parse("{_id: 1, s: 1, a: {b: 1}}"));
    }
    // sibling paths with a common prefix of characters only do not conflict
    final MongoCollection<Document> collection = collection(Document.parse("{_id: 1, a: {b: 1}, ab: 1}"));
    collection.updateMany(NO_FILTER, Document.parse("{$set: {ab: 2}, $inc: {'a.b': 1}}"));
    assertThat(stored(collection)).isEqualTo(Document.parse("{_id: 1, a: {b: 2}, ab: 2}"));
  }

  private static void assertRefused(final MongoCollection<Document> collection, final Document filter, final Document update, final int code) {
    assertThatThrownBy(() -> collection.updateMany(filter, update)).as("updateMany(%s, %s)", filter.toJson(), update.toJson())
        .isInstanceOfSatisfying(MongoException.class, e -> assertThat(e.getCode()).isEqualTo(code));
  }

  private static Document stored(final MongoCollection<Document> collection) {
    return collection.find().first();
  }

  private MongoCollection<Document> collection(final Document document) {
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("u" + (collections++));
    collection.insertMany(List.of(document));
    return collection;
  }
}
