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
import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12594: a regular expression was only ever a pattern to the matcher, which compared a stored regular expression with the
 * filter's by object identity. {@code {a: {$eq: /x/}}} found no document, not even one holding {@code /x/}, and an upsert filtered by
 * it inserted again at every run. Every expectation below is the answer of a MongoDB 8.0 server on the same documents: a regular
 * expression operand of {@code $eq} (and of a plain equality on an array or an embedded document) is a value, equal to a stored
 * regular expression with the same pattern and options; a pattern match ({@code /x/}, {@code $regex}, {@code $in}, {@code $all},
 * {@code $not}) matches a string by the pattern and that same stored regular expression; {@code $ne} and the range operators refuse a
 * regular expression operand.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12594MongoRegexEqualityTest extends BaseMongoServerTest {
  private static final BsonRegularExpression X   = new BsonRegularExpression("x");
  private static final BsonRegularExpression X_I = new BsonRegularExpression("x", "i");

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
  void aRegularExpressionUnderEqIsComparedWithTheStoredRegularExpression() {
    assertMatches(new Document("a", new Document("$eq", X)), List.of(2, 5));
    assertMatches(new Document("a", new Document("$eq", X_I)), List.of(4));
    assertMatches(new Document("a", new Document("$eq", new BsonRegularExpression("x", "mi"))), List.of(7));
    assertMatches(new Document("a", new Document("$eq", new BsonRegularExpression("y"))), List.of());
    assertMatches(new Document("a", new Document("$not", new Document("$eq", X))), List.of(1, 3, 4, 6, 7));
    assertMatches(new Document("a", new Document("$elemMatch", new Document("$eq", X))), List.of(5));
  }

  @Test
  void aRegularExpressionInsideAnArrayOrADocumentOperandIsAValue() {
    assertMatches(new Document("a", List.of(X)), List.of(5));
    assertMatches(new Document("a", new Document("$eq", List.of(X))), List.of(5));
    assertMatches(new Document("a", new Document("$in", List.of(List.of(X)))), List.of(5));
    assertMatches(new Document("a", new Document("b", X)), List.of(6));
    assertMatches(new Document("a", new Document("$ne", List.of(X))), List.of(1, 2, 3, 4, 6, 7));
    assertMatches(new Document("a", new Document("$ne", new Document("b", X))), List.of(1, 2, 3, 4, 5, 7));
  }

  @Test
  void aPatternAlsoMatchesTheSameStoredRegularExpression() {
    assertMatches(new Document("a", X), List.of(1, 2, 5));
    assertMatches(new Document("a", X_I), List.of(1, 4));
    assertMatches(new Document("a", new Document("$regex", "x")), List.of(1, 2, 5));
    assertMatches(new Document("a", new Document("$regex", "x").append("$options", "i")), List.of(1, 4));
    assertMatches(new Document("a", new Document("$in", List.of(X))), List.of(1, 2, 5));
    assertMatches(new Document("a", new Document("$nin", List.of(X))), List.of(3, 4, 6, 7));
    assertMatches(new Document("a", new Document("$all", List.of(X))), List.of(1, 2, 5));
    assertMatches(new Document("a", new Document("$not", X)), List.of(3, 4, 6, 7));
  }

  @Test
  void neAndTheRangeOperatorsRefuseARegularExpression() {
    final MongoCollection<Document> collection = collection();
    for (final Document filter : List.of(new Document("a", new Document("$ne", X)),
        new Document("a", new Document("$not", new Document("$ne", X))),
        new Document("a", new Document("$elemMatch", new Document("$ne", X))),
        new Document("a", new Document("$elemMatch", new Document("b", new Document("$ne", X)))),
        new Document("a", new Document("$exists", true).append("$ne", X)),
        new Document("$or", List.of(new Document("a", new Document("$ne", X))))))
      assertRefused(collection, filter, "Can't have regex as arg to $ne.");

    for (final String operator : List.of("$gt", "$gte", "$lt", "$lte")) {
      assertRefused(collection, new Document("a", new Document(operator, X)),
          "Can't have RegEx as arg to non-equality predicate over field 'a'.");
      assertRefused(collection, new Document("a", new Document("$not", new Document(operator, X))),
          "Can't have RegEx as arg to non-equality predicate over field 'a'.");
    }
    assertRefused(collection, new Document("_id", new Document("$lte", X)),
        "Can't have RegEx as arg to non-equality predicate over field '_id'.");
    assertRefused(collection, new Document("a", new Document("$elemMatch", new Document("$lt", X))),
        "Can't have RegEx as arg to non-equality predicate over field '(None)'.");
  }

  @Test
  void anAggregationMatchComparesAndRefusesTheSameWay() {
    final MongoCollection<Document> collection = collection();
    final List<Object> found = new ArrayList<>();
    for (final Document d : collection.aggregate(
        List.of(new Document("$match", new Document("a", new Document("$eq", X))), new Document("$sort", new Document("_id", 1)))))
      found.add(d.get("_id"));
    assertThat(found).containsExactly(2, 5);

    assertThatThrownBy(() -> collection.aggregate(List.of(new Document("$match", new Document("a", new Document("$ne", X))))).first())
        .isInstanceOf(MongoException.class).hasMessageContaining("Can't have regex as arg to $ne.");
  }

  @Test
  void anUpsertFilteredByARegularExpressionUnderEqSeedsItAndMatchesItAtTheNextRun() {
    for (final Document filter : List.of(new Document("a", new Document("$eq", X)), new Document("a", new Document("$eq", X_I)),
        new Document("a", List.of(X)), new Document("a", new Document("b", X)))) {
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("u" + (collections++));
      collection.updateOne(filter, new Document("$inc", new Document("n", 1)), new UpdateOptions().upsert(true));
      final long matched = collection.updateOne(filter, new Document("$inc", new Document("n", 1)), new UpdateOptions().upsert(true))
          .getMatchedCount();

      assertThat(matched).as("the second run of %s matches the document the first inserted", filter.toJson()).isEqualTo(1);
      final List<Document> stored = collection.find().into(new ArrayList<>());
      assertThat(stored).as("%s", filter.toJson()).hasSize(1);
      final Object expected = filter.get("a") instanceof Document operators && operators.containsKey("$eq") ? operators.get("$eq") : filter.get("a");
      assertThat(stored.getFirst().get("a")).as("%s", filter.toJson()).isEqualTo(expected);
      assertThat(stored.getFirst().get("n")).isEqualTo(2);
    }
  }

  @Test
  void aPatternIsStillNoEqualityOfAnUpsert() {
    // {a: /x/}, {$in: [/x/]} and {$all: [/x/]} match a pattern: mongod leaves a out of the inserted document
    for (final Document filter : List.of(new Document("a", X), new Document("a", new Document("$in", List.of(X))),
        new Document("a", new Document("$all", List.of(X))))) {
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("p" + (collections++));
      collection.updateOne(filter, new Document("$set", new Document("n", 1)), new UpdateOptions().upsert(true));
      final List<Document> stored = collection.find().into(new ArrayList<>());
      assertThat(stored).as("%s", filter.toJson()).hasSize(1);
      assertThat(stored.getFirst().containsKey("a")).as("%s", filter.toJson()).isFalse();
    }
  }

  private void assertRefused(final MongoCollection<Document> collection, final Document filter, final String message) {
    assertThatThrownBy(() -> collection.find(filter).first()).as("find(%s)", filter.toJson()).isInstanceOf(MongoException.class)
        .hasMessageContaining(message).satisfies(e -> assertThat(((MongoException) e).getCode()).isEqualTo(2));
    assertThatThrownBy(() -> collection.countDocuments(filter)).as("countDocuments(%s)", filter.toJson())
        .isInstanceOf(MongoException.class).hasMessageContaining(message);
    assertThatThrownBy(() -> collection.deleteMany(filter)).as("deleteMany(%s)", filter.toJson()).isInstanceOf(MongoException.class)
        .hasMessageContaining(message);
  }

  /** find, count, updateMany and deleteMany must all select {@code expected}, on the documents measured on mongod. */
  private void assertMatches(final Document filter, final List<Integer> expected) {
    final MongoCollection<Document> collection = collection();
    final List<Object> found = new ArrayList<>();
    for (final Document d : collection.find(filter).sort(new Document("_id", 1)))
      found.add(d.get("_id"));
    assertThat(found).as("find(%s)", filter.toJson()).containsExactlyElementsOf(expected);
    assertThat(collection.countDocuments(filter)).as("countDocuments(%s)", filter.toJson()).isEqualTo(expected.size());
    assertThat(collection.updateMany(filter, new Document("$set", new Document("touched", true))).getMatchedCount())
        .as("updateMany(%s)", filter.toJson()).isEqualTo(expected.size());
    assertThat(collection.deleteMany(filter).getDeletedCount()).as("deleteMany(%s)", filter.toJson()).isEqualTo(expected.size());
  }

  private MongoCollection<Document> collection() {
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("c" + (collections++));
    collection.insertMany(List.of(new Document("_id", 1).append("a", "xyz"), new Document("_id", 2).append("a", X),
        new Document("_id", 3).append("a", "abc"), new Document("_id", 4).append("a", X_I), new Document("_id", 5).append("a", List.of(X)),
        new Document("_id", 6).append("a", new Document("b", X)), new Document("_id", 7).append("a", new BsonRegularExpression("x", "im"))));
    return collection;
  }
}
