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
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12073: an ObjectId and the String with the same hex are two values, to the {@code _id} unique index, to the filters, to
 * updates and deletes, and to {@code $type}; a Decimal128 keeps its negative zero and its NaN and infinities. Every expectation is
 * the answer of a MongoDB 7 server, measured in the issue.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12073MongoValueIdentityTest extends BaseMongoServerTest {
  private static final String   HEX = "5f0000000000000000000001";
  private static final ObjectId OID = new ObjectId(HEX);

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

  private MongoCollection<Document> threeKindsOfId() {
    return collection(new Document("_id", OID).append("k", "objectid"), new Document("_id", "abc").append("k", "string"),
        new Document("_id", 3).append("k", "int"));
  }

  @Test
  void aFilterOnTheIdTellsAnObjectIdFromItsHexString() {
    final MongoCollection<Document> collection = threeKindsOfId();
    assertThat(ks(collection, new Document("_id", HEX))).isEmpty();
    assertThat(ks(collection, new Document("_id", OID))).containsExactly("objectid");
    assertThat(ks(collection, Document.parse("{_id: {$type: 'objectId'}}"))).containsExactly("objectid");
    assertThat(ks(collection, Document.parse("{_id: {$type: 'string'}}"))).containsExactly("string");
    assertThat(ks(collection, new Document("_id", Pattern.compile("^5f")))).isEmpty();
    assertThat(ks(collection, Document.parse("{_id: {$lt: 'z'}}"))).containsExactly("string");
    assertThat(ks(collection, new Document("_id", new Document("$gt", new ObjectId("000000000000000000000000"))))).containsExactly("objectid");
    assertThat(ks(collection, new Document("_id", new Document("$in", List.of(HEX, 3))))).containsExactly("int");
    assertThat(ks(collection, new Document("_id", new Document("$in", List.of(OID, "abc"))))).containsExactly("objectid", "string");
  }

  @Test
  void anObjectIdAndItsHexStringAreTwoIds() {
    final MongoCollection<Document> collection = collection(new Document("_id", OID).append("k", "objectid"));
    collection.insertOne(new Document("_id", HEX).append("k", "string"));
    assertThat(ks(collection, new Document())).containsExactly("objectid", "string");

    // each one reads back with its own type
    final List<Object> ids = new ArrayList<>();
    for (final Document d : collection.find().sort(new Document("_id", 1)))
      ids.add(d.get("_id"));
    assertThat(ids).containsExactly(HEX, OID);

    assertThat(collection.find(new Document("_id", HEX)).first().get("k")).isEqualTo("string");
    assertThat(collection.find(new Document("_id", OID)).first().get("k")).isEqualTo("objectid");
  }

  @Test
  void aHexStringIdRoundTripsAsAString() {
    final MongoCollection<Document> collection = collection(new Document("_id", HEX).append("k", "string"));
    final Document found = collection.find().first();
    assertThat(found.get("_id")).isEqualTo(HEX);
    assertThat(collection.countDocuments(new Document("_id", HEX))).isEqualTo(1);
    assertThat(collection.countDocuments(new Document("_id", OID))).isZero();
  }

  @Test
  void updateAndDeleteByTheHexStringLeaveTheObjectIdAlone() {
    final MongoCollection<Document> collection = collection(new Document("_id", OID).append("k", "objectid"));
    assertThat(collection.deleteMany(new Document("_id", HEX)).getDeletedCount()).isZero();
    assertThat(collection.countDocuments()).isEqualTo(1);
    assertThat(collection.updateOne(new Document("_id", HEX), Document.parse("{$set: {k: 'changed'}}")).getMatchedCount()).isZero();
    assertThat(collection.find().first().get("k")).isEqualTo("objectid");

    // an upsert by the hex string inserts a second document, whose _id is the String
    final Object upserted = collection.updateOne(new Document("_id", HEX), Document.parse("{$set: {k: 'string'}}"),
        new UpdateOptions().upsert(true)).getUpsertedId().asString().getValue();
    assertThat(upserted).isEqualTo(HEX);
    assertThat(ks(collection, new Document())).containsExactly("objectid", "string");
    assertThat(collection.find(new Document("_id", HEX)).first().get("_id")).isEqualTo(HEX);
  }

  @Test
  void anObjectIdEqualityOnAFieldDoesNotMatchTheHexString() {
    final MongoCollection<Document> collection = collection(new Document("_id", 1).append("o", OID), new Document("_id", 2).append("o", HEX));
    assertThat(ids(collection, new Document("o", OID))).containsExactly(1);
    assertThat(ids(collection, new Document("o", HEX))).containsExactly(2);
    assertThat(ids(collection, new Document("o", new Document("$in", List.of(OID))))).containsExactly(1);
    assertThat(ids(collection, new Document("o", new Document("$ne", OID)))).containsExactly(2);
    assertThat(ids(collection, new Document("o", new Document("$nin", List.of(HEX))))).containsExactly(1);
  }

  @Test
  void aDecimal128KeepsItsNegativeZeroAndItsSpecialValues() {
    for (final String value : List.of("-0", "-0.00", "NaN", "-Infinity", "Infinity", "1.50", "-1E+3")) {
      final MongoCollection<Document> collection = collection(new Document("_id", 1).append("d", Decimal128.parse(value)));
      final Object stored = collection.find().first().get("d");
      assertThat(stored).as(value).isInstanceOf(Decimal128.class);
      assertThat(stored.toString()).as(value).isEqualTo(Decimal128.parse(value).toString());
    }
  }

  @Test
  void aSpecialDecimal128IsFoundAndSorted() {
    final MongoCollection<Document> collection = collection(new Document("_id", 1).append("d", Decimal128.parse("NaN")),
        new Document("_id", 2).append("d", Decimal128.parse("-Infinity")), new Document("_id", 3).append("d", 5),
        new Document("_id", 4).append("d", Decimal128.parse("-0")), new Document("_id", 5).append("d", Decimal128.parse("Infinity")));
    assertThat(ids(collection, new Document("d", Decimal128.parse("NaN")))).containsExactly(1);
    assertThat(ids(collection, new Document("d", 0))).containsExactly(4);
    assertThat(ids(collection, Document.parse("{d: {$gt: 0}}"))).containsExactly(3, 5);
    final List<Object> sorted = new ArrayList<>();
    for (final Document d : collection.find().sort(new Document("d", 1)))
      sorted.add(d.get("_id"));
    assertThat(sorted).containsExactly(1, 2, 4, 3, 5);

    // $inc on a special value follows the decimal arithmetic
    collection.updateOne(new Document("_id", 2), Document.parse("{$inc: {d: 1}}"));
    collection.updateOne(new Document("_id", 4), Document.parse("{$inc: {d: 1}}"));
    assertThat(collection.find(new Document("_id", 2)).first().get("d")).isEqualTo(Decimal128.NEGATIVE_INFINITY);
    assertThat(collection.find(new Document("_id", 4)).first().get("d")).isEqualTo(new Decimal128(BigDecimal.ONE));
  }

  private static List<Object> ks(final MongoCollection<Document> collection, final Document filter) {
    final List<Object> ks = new ArrayList<>();
    for (final Document d : collection.find(filter))
      ks.add(d.get("k"));
    ks.sort((a, b) -> a.toString().compareTo(b.toString()));
    return ks;
  }

  private static List<Object> ids(final MongoCollection<Document> collection, final Document filter) {
    final List<Object> ids = new ArrayList<>();
    for (final Document d : collection.find(filter).sort(new Document("_id", 1)))
      ids.add(d.get("_id"));
    return ids;
  }

  private MongoCollection<Document> collection(final Document... documents) {
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("v" + (collections++));
    collection.insertMany(List.of(documents));
    return collection;
  }
}
