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
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9162: since #9156 a filter on a field other than the {@code _id} was evaluated on every document of the type, so
 * {@code find({sku: "x"})} on a unique index, and above all a bulk upsert, were O(n). A SQL pre-filter now narrows the candidates
 * (the matcher still tests the whole filter on them) where it provably cannot be narrower than MongoDB: a field whose declared type
 * is a scalar, so no array is ever stored in it, compared with an operand of that kind. Whatever else is left to the matcher alone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9162MongoFilterIndexPrefilterTest extends BaseMongoServerTest {
  private MongoClient client;

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

  private Database db() {
    return getServerDatabase(0, getDatabaseName());
  }

  /** A typed collection: scalar declared properties, the unique index on the sku. */
  private void declareProducts() {
    final Database db = db();
    db.command("sql", "CREATE DOCUMENT TYPE products");
    db.command("sql", "CREATE PROPERTY products.sku STRING");
    db.command("sql", "CREATE PROPERTY products.qty INTEGER");
    db.command("sql", "CREATE PROPERTY products.big LONG");
    db.command("sql", "CREATE PROPERTY products.price DOUBLE");
    db.command("sql", "CREATE PROPERTY products.active BOOLEAN");
    db.command("sql", "CREATE PROPERTY products.created DATETIME");
    db.command("sql", "CREATE PROPERTY products.tags LIST");
    db.command("sql", "CREATE INDEX ON products (sku) UNIQUE");
    db.command("sql", "CREATE INDEX ON products (qty) NOTUNIQUE");
  }

  private static Document parse(final String json) {
    return Document.parse(json);
  }

  private static de.bwaldvogel.mongo.bson.Document toMongo(final String json) {
    return (de.bwaldvogel.mongo.bson.Document) convert(org.bson.Document.parse(json));
  }

  private static Object convert(final Object value) {
    if (value instanceof org.bson.Document document) {
      final de.bwaldvogel.mongo.bson.Document result = new de.bwaldvogel.mongo.bson.Document();
      for (final Map.Entry<String, Object> entry : document.entrySet())
        result.put(entry.getKey(), convert(entry.getValue()));
      return result;
    }
    if (value instanceof List<?> list)
      return list.stream().map(Issue9162MongoFilterIndexPrefilterTest::convert).toList();
    return value;
  }

  private String candidateWhere(final String collection, final String filter) {
    final DocumentType type = db().getSchema().getTypeOrNull(collection);
    final StringBuilder sql = new StringBuilder();
    new MongoFilter(db(), toMongo(filter)).appendCandidateWhere(sql, new HashMap<>(), type);
    return sql.toString();
  }

  @Test
  void aScalarEqualityOnAnIndexedDeclaredFieldReadsTheIndex() {
    declareProducts();
    final Map<String, Object> params = new HashMap<>();
    final StringBuilder sql = new StringBuilder("select from products");
    new MongoFilter(db(), toMongo("{sku: 'x'}")).appendCandidateWhere(sql, params, db().getSchema().getType("products"));

    assertThat(sql.toString()).contains(" WHERE ");
    try (final ResultSet rs = db().query("sql", "EXPLAIN " + sql, params)) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("FETCH FROM INDEX products[sku]");
    }
  }

  @Test
  void whatMayNarrowTheCandidates() {
    declareProducts();
    assertThat(candidateWhere("products", "{sku: 'x'}")).contains("WHERE").contains("sku");
    assertThat(candidateWhere("products", "{sku: {$eq: 'x'}}")).contains("WHERE");
    assertThat(candidateWhere("products", "{sku: {$in: ['a', 'b']}}")).contains("WHERE");
    assertThat(candidateWhere("products", "{qty: 5}")).contains("WHERE");
    assertThat(candidateWhere("products", "{qty: {$gt: 5, $lte: 9}}")).contains("WHERE").contains(">").contains("<=");
    assertThat(candidateWhere("products", "{big: {$gte: 5000000000}}")).contains("WHERE");
    assertThat(candidateWhere("products", "{price: 1.5}")).contains("WHERE");
    assertThat(candidateWhere("products", "{price: 2}")).contains("WHERE");
    assertThat(candidateWhere("products", "{active: true}")).contains("WHERE");
    assertThat(candidateWhere("products", "{$and: [{sku: 'x'}, {qty: 1}]}")).contains("WHERE");
    assertThat(candidateWhere("products", "{$or: [{sku: 'x'}, {sku: 'y'}]}")).contains("WHERE");
    assertThat(candidateWhere("products", "{$or: [{sku: 'x'}, {qty: 3}]}")).contains("WHERE");
    // a conjunct that cannot narrow is dropped, the one that can is kept
    assertThat(candidateWhere("products", "{sku: 'x', tags: 'a', qty: {$ne: 3}}")).contains("WHERE").contains("sku").doesNotContain("tags")
        .doesNotContain("<>");
    assertThat(candidateWhere("products", "{qty: {$gt: 5, $ne: 7}}")).contains("WHERE").doesNotContain("<>");
  }

  @Test
  void whatMayNeverNarrowTheCandidates() {
    declareProducts();
    // an array can hold the value: MongoDB matches through the elements, SQL compares the list as a whole
    assertThat(candidateWhere("products", "{tags: 'a'}")).isEmpty();
    // undeclared: could hold an array
    assertThat(candidateWhere("products", "{unknown: 'a'}")).isEmpty();
    assertThat(candidateWhere("nothere", "{sku: 'a'}")).isEmpty();
    // dotted path: traverses arrays and embedded documents
    assertThat(candidateWhere("products", "{'sku.x': 'a'}")).isEmpty();
    // a null operand also matches a missing field
    assertThat(candidateWhere("products", "{sku: null}")).isEmpty();
    assertThat(candidateWhere("products", "{sku: {$in: ['a', null]}}")).isEmpty();
    assertThat(candidateWhere("products", "{sku: {$in: []}}")).isEmpty();
    // negations, existence, size, patterns, element tests
    assertThat(candidateWhere("products", "{sku: {$ne: 'a'}}")).isEmpty();
    assertThat(candidateWhere("products", "{sku: {$nin: ['a']}}")).isEmpty();
    assertThat(candidateWhere("products", "{sku: {$not: {$eq: 'a'}}}")).isEmpty();
    assertThat(candidateWhere("products", "{sku: {$exists: true}}")).isEmpty();
    assertThat(candidateWhere("products", "{sku: {$regex: '^a'}}")).isEmpty();
    assertThat(candidateWhere("products", "{sku: {$all: ['a']}}")).isEmpty();
    // the operand is not of the kind of the field: MongoDB brackets the types, SQL coerces them
    assertThat(candidateWhere("products", "{sku: 5}")).isEmpty();
    assertThat(candidateWhere("products", "{qty: '5'}")).isEmpty();
    assertThat(candidateWhere("products", "{qty: true}")).isEmpty();
    assertThat(candidateWhere("products", "{active: 1}")).isEmpty();
    assertThat(candidateWhere("products", "{sku: ['a']}")).isEmpty();
    assertThat(candidateWhere("products", "{sku: {a: 1}}")).isEmpty();
    // a range on a string follows the collation of an index, not the code point order MongoDB uses
    assertThat(candidateWhere("products", "{sku: {$gt: 'a'}}")).isEmpty();
    // a fractional operand against an integer field: the index would round it
    assertThat(candidateWhere("products", "{qty: {$lt: 5.5}}")).isEmpty();
    assertThat(candidateWhere("products", "{qty: 5.5}")).isEmpty();
    // an operand the field type cannot hold
    assertThat(candidateWhere("products", "{qty: {$lt: 3000000000}}")).isEmpty();
    // a range on a floating point field: NaN and the rounding of the stored value
    assertThat(candidateWhere("products", "{price: {$lt: 5}}")).isEmpty();
    // temporal values
    assertThat(candidateWhere("products", "{created: {$gt: {$date: '2020-01-01T00:00:00Z'}}}")).isEmpty();
    // a branch that cannot narrow takes the whole $or with it, and $nor never narrows
    assertThat(candidateWhere("products", "{$or: [{sku: 'x'}, {tags: 'y'}]}")).isEmpty();
    assertThat(candidateWhere("products", "{$nor: [{sku: 'x'}]}")).isEmpty();
  }

  @Test
  void theIdPartAndTheFieldPartAreJoined() {
    declareProducts();
    final String where = candidateWhere("products", "{_id: 'x', sku: 'y'}");
    assertThat(where).contains("WHERE").contains("_id").contains("sku").contains("AND");
  }

  @Test
  void findOnADeclaredFieldKeepsMongoSemantics() {
    declareProducts();
    final MongoCollection<Document> c = client.getDatabase(getDatabaseName()).getCollection("products");
    c.insertMany(List.of(parse("{_id: 1, sku: 'a', qty: 1, big: 5000000000, price: 1.5, active: true}"),
        parse("{_id: 2, sku: 'b', qty: 2, big: 1, price: 2.0, active: false}"),
        parse("{_id: 3, sku: 'c', qty: 3, price: 3.5}"),
        parse("{_id: 4, sku: 'A', qty: 40}"),
        parse("{_id: 5, qty: 5}")));

    assertThat(ids(c, "{sku: 'a'}")).containsExactly(1);
    assertThat(ids(c, "{sku: 'A'}")).containsExactly(4);
    assertThat(ids(c, "{sku: {$in: ['a', 'c', 'zz']}}")).containsExactly(1, 3);
    assertThat(ids(c, "{sku: 5}")).isEmpty();
    assertThat(ids(c, "{sku: null}")).containsExactly(5);
    assertThat(ids(c, "{qty: {$gt: 1, $lt: 40}}")).containsExactly(2, 3, 5);
    assertThat(ids(c, "{qty: {$gte: 3}}")).containsExactly(3, 4, 5);
    assertThat(ids(c, "{qty: 2.0}")).containsExactly(2);
    assertThat(ids(c, "{qty: {$lt: 2.5}}")).containsExactly(1, 2);
    assertThat(ids(c, "{big: {$gt: 4000000000}}")).containsExactly(1);
    assertThat(ids(c, "{price: 2}")).containsExactly(2);
    assertThat(ids(c, "{price: {$lt: 3}}")).containsExactly(1, 2);
    assertThat(ids(c, "{active: true}")).containsExactly(1);
    assertThat(ids(c, "{$or: [{sku: 'a'}, {qty: 5}]}")).containsExactly(1, 5);
    assertThat(ids(c, "{$or: [{sku: 'a'}, {sku: 'b'}]}")).containsExactly(1, 2);
    assertThat(ids(c, "{$and: [{qty: {$gt: 1}}, {sku: {$in: ['b', 'c']}}]}")).containsExactly(2, 3);
    assertThat(ids(c, "{sku: {$ne: 'a'}, qty: {$gt: 1}}")).containsExactly(2, 3, 4, 5);
    assertThat(ids(c, "{sku: {$regex: '^[ab]$'}}")).containsExactly(1, 2);
    assertThat(c.count(parse("{qty: {$gte: 3}}"))).isEqualTo(3);
    assertThat(ids(c, "{qty: {$gt: 1}}", "{qty: -1}")).containsExactly(4, 5, 3, 2);
  }

  @Test
  void aNegativeZeroAndTheIdBoundTogetherStillMatch() {
    declareProducts();
    final MongoCollection<Document> c = client.getDatabase(getDatabaseName()).getCollection("products");
    c.insertMany(List.of(parse("{_id: 1, sku: 'a', price: -0.0}"), parse("{_id: 2, sku: 'b', price: 0.0}"),
        parse("{_id: 3, sku: 'c', price: 1.0}")));

    assertThat(ids(c, "{price: 0}")).containsExactly(1, 2);
    assertThat(ids(c, "{price: 0.0}")).containsExactly(1, 2);
    assertThat(ids(c, "{price: -0.0}")).containsExactly(1, 2);
    assertThat(ids(c, "{price: {$in: [0, 5]}}")).containsExactly(1, 2);
    // both parts bind their own parameters: the _id $in and the field $in must not overwrite each other
    assertThat(ids(c, "{_id: {$in: [1, 3]}, sku: {$in: ['a', 'b']}}")).containsExactly(1);
    assertThat(ids(c, "{_id: {$in: [1, 2, 3]}, sku: 'c', price: {$in: [1]}}")).containsExactly(3);
    assertThat(c.updateOne(parse("{_id: 2, sku: 'b'}"), parse("{$set: {hit: 1}}")).getModifiedCount()).isEqualTo(1);
    assertThat(c.updateOne(parse("{_id: 2, sku: 'a'}"), parse("{$set: {hit: 2}}")).getModifiedCount()).isZero();
  }

  @Test
  void aListInADeclaredListFieldStillMatchesThroughItsElements() {
    declareProducts();
    final MongoCollection<Document> c = client.getDatabase(getDatabaseName()).getCollection("products");
    c.insertMany(List.of(parse("{_id: 1, sku: 'a', tags: ['x', 'y']}"), parse("{_id: 2, sku: 'b', tags: ['y']}")));
    assertThat(ids(c, "{tags: 'x'}")).containsExactly(1);
    assertThat(ids(c, "{tags: 'y'}")).containsExactly(1, 2);
    assertThat(ids(c, "{sku: 'a', tags: 'y'}")).containsExactly(1);
  }

  @Test
  void anIndexOnAnUndeclaredFieldIsNoProofThatItHoldsNoArray() {
    final MongoCollection<Document> c = client.getDatabase(getDatabaseName()).getCollection("loose");
    c.insertOne(parse("{_id: 0, sku: 'seed'}"));
    c.createIndex(parse("{sku: 1}"), new IndexOptions());
    c.insertOne(parse("{_id: 1, sku: 'a'}"));
    c.insertOne(parse("{_id: 2, sku: ['x', 'y']}"));

    assertThat(candidateWhere("loose", "{sku: 'x'}")).isEmpty();
    assertThat(ids(c, "{sku: 'x'}")).containsExactly(2);
    assertThat(ids(c, "{sku: 'a'}")).containsExactly(1);
    assertThat(c.updateOne(parse("{sku: 'y'}"), parse("{$set: {hit: 1}}")).getModifiedCount()).isEqualTo(1);
  }

  @Test
  void updateAndDeleteOnADeclaredFieldTouchExactlyTheMatches() {
    declareProducts();
    final MongoCollection<Document> c = client.getDatabase(getDatabaseName()).getCollection("products");
    c.insertMany(List.of(parse("{_id: 1, sku: 'a', qty: 1}"), parse("{_id: 2, sku: 'b', qty: 2}"), parse("{_id: 3, sku: 'c', qty: 2}")));

    assertThat(c.updateOne(parse("{sku: 'b'}"), parse("{$set: {hit: 1}}")).getModifiedCount()).isEqualTo(1);
    assertThat(c.updateMany(parse("{qty: 2}"), parse("{$set: {seen: true}}")).getModifiedCount()).isEqualTo(2);
    assertThat(ids(c, "{hit: 1}")).containsExactly(2);
    assertThat(c.deleteOne(parse("{sku: 'a'}")).getDeletedCount()).isEqualTo(1);
    assertThat(c.deleteMany(parse("{qty: {$gte: 2}}")).getDeletedCount()).isEqualTo(2);
    assertThat(ids(c, "{}")).isEmpty();
  }

  @Test
  void aBulkUpsertByTheUniqueFieldInsertsOnceAndThenUpdates() {
    declareProducts();
    final MongoCollection<Document> c = client.getDatabase(getDatabaseName()).getCollection("products");
    final int total = 300;

    final List<WriteModel<Document>> first = new ArrayList<>();
    for (int i = 0; i < total; i++)
      first.add(new UpdateOneModel<>(new Document("sku", "s" + i), new Document("$set", new Document("qty", i)), new UpdateOptions().upsert(true)));
    assertThat(c.bulkWrite(first).getUpserts()).hasSize(total);
    assertThat(c.count()).isEqualTo(total);

    final List<WriteModel<Document>> second = new ArrayList<>();
    for (int i = 0; i < total; i++)
      second.add(new UpdateOneModel<>(new Document("sku", "s" + i), new Document("$set", new Document("qty", i + 1000)),
          new UpdateOptions().upsert(true)));
    final BulkWriteResult result = c.bulkWrite(second);
    assertThat(result.getUpserts()).isEmpty();
    assertThat(result.getMatchedCount()).isEqualTo(total);
    assertThat(c.count()).isEqualTo(total);
    assertThat(c.find(new Document("sku", "s7")).first().get("qty")).isEqualTo(1007);
  }

  private static List<Object> ids(final MongoCollection<Document> collection, final String filter) {
    return ids(collection, filter, "{_id: 1}");
  }

  private static List<Object> ids(final MongoCollection<Document> collection, final String filter, final String sort) {
    final List<Object> ids = new ArrayList<>();
    for (final Document d : collection.find(Document.parse(filter)).sort(Document.parse(sort)))
      ids.add(d.get("_id"));
    return ids;
  }
}
