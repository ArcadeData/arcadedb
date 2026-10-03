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
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #9062: a BSON binary (subtype 0), regular expression, timestamp, MinKey, MaxKey and JavaScript code were
 * silently dropped on insert, and a Decimal128 came back as a double.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBBsonTypesRoundTripTest extends BaseMongoServerTest {

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
    client.getDatabase(getDatabaseName()).createCollection("bson");
    collection = client.getDatabase(getDatabaseName()).getCollection("bson");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  private Object roundTrip(final int id, final Object value) {
    collection.insertOne(new Document("_id", id).append("v", value));
    final Document back = collection.find(eq("_id", id)).first();
    assertThat(back).isNotNull();
    assertThat(back.containsKey("v")).as("field v is stored").isTrue();
    return back.get("v");
  }

  @Test
  void binarySubtypeZero() {
    final Object v = roundTrip(1, new Binary((byte) 0, new byte[] { 1, 2, 3 }));
    assertThat(v).isInstanceOf(Binary.class);
    assertThat(((Binary) v).getData()).containsExactly(1, 2, 3);
  }

  @Test
  void regularExpression() {
    final Object v = roundTrip(2, new BsonRegularExpression("^a", "i"));
    assertThat(v).isEqualTo(new BsonRegularExpression("^a", "i"));
  }

  @Test
  void timestamp() {
    assertThat(roundTrip(3, new BsonTimestamp(1700000000, 5))).isEqualTo(new BsonTimestamp(1700000000, 5));
  }

  @Test
  void minAndMaxKey() {
    assertThat(roundTrip(4, new MinKey())).isInstanceOf(MinKey.class);
    assertThat(roundTrip(5, new MaxKey())).isInstanceOf(MaxKey.class);
  }

  @Test
  void javascriptCode() {
    assertThat(roundTrip(6, new Code("function(){}"))).isEqualTo(new Code("function(){}"));
  }

  @Test
  void decimal128KeepsItsPrecision() {
    final Decimal128 d = new Decimal128(new BigDecimal("12345678901234567890.12345"));
    assertThat(roundTrip(7, d)).isEqualTo(d);
  }

  @Test
  void typesNestedInEmbeddedDocumentAndArray() {
    collection.insertOne(new Document("_id", 8).append("doc", new Document("t", new BsonTimestamp(10, 1)))
        .append("arr", List.of(new Binary(new byte[] { 9 }), new MaxKey())));
    final Document back = collection.find(eq("_id", 8)).first();
    assertThat(((Document) back.get("doc")).get("t")).isEqualTo(new BsonTimestamp(10, 1));
    final List<?> arr = (List<?>) back.get("arr");
    assertThat(((Binary) arr.get(0)).getData()).containsExactly(9);
    assertThat(arr.get(1)).isInstanceOf(MaxKey.class);
  }

  @Test
  void typesWrittenByUpdate() {
    collection.insertOne(new Document("_id", 9).append("x", 1));
    collection.updateOne(eq("_id", 9), set("v", new BsonRegularExpression("b+", "m")));
    assertThat(collection.find(eq("_id", 9)).first().get("v")).isEqualTo(new BsonRegularExpression("b+", "m"));
  }

  @Test
  void typesWrittenByReplaceAndUpsert() {
    collection.insertOne(new Document("_id", 10).append("x", 1));
    collection.replaceOne(eq("_id", 10), new Document("_id", 10).append("v", new MinKey()));
    assertThat(collection.find(eq("_id", 10)).first().get("v")).isInstanceOf(MinKey.class);

    collection.updateOne(eq("_id", 11), set("v", new BsonTimestamp(5, 6)), new UpdateOptions().upsert(true));
    assertThat(collection.find(eq("_id", 11)).first().get("v")).isEqualTo(new BsonTimestamp(5, 6));
  }

  @Test
  void filterOnStoredTypes() {
    collection.insertOne(new Document("_id", 12).append("v", new BsonTimestamp(7, 8)));
    collection.insertOne(new Document("_id", 13).append("v", new Decimal128(new BigDecimal("1.5"))));
    assertThat(collection.find(eq("v", new BsonTimestamp(7, 8))).first().get("_id")).isEqualTo(12);
    assertThat(collection.find(eq("v", new Decimal128(new BigDecimal("1.5")))).first().get("_id")).isEqualTo(13);
    assertThat(collection.find(in("v", List.of(new BsonTimestamp(7, 8)))).first().get("_id")).isEqualTo(12);
  }

  @Test
  void nanDecimalIsRefusedNotDropped() {
    assertThatThrownBy(() -> collection.insertOne(new Document("_id", 14).append("v", Decimal128.NaN))).isInstanceOf(MongoCommandException.class);
    assertThat(collection.find(eq("_id", 14)).first()).isNull();
  }

  // the Java driver refuses a $-prefixed field name itself; the server-side check is covered by MongoBsonValuesTest
  @Test
  void clientCannotUseTheReservedTag() {
    assertThatThrownBy(() -> collection.insertOne(new Document("_id", 15).append("v", new Document("$bson", "timestamp"))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(collection.find(eq("_id", 15)).first()).isNull();
  }

  @Test
  void refusalLeavesNoOpenTransaction() {
    assertThatThrownBy(() -> collection.insertOne(new Document("_id", 16).append("v", Decimal128.POSITIVE_INFINITY)))
        .isInstanceOf(MongoCommandException.class);
    collection.insertOne(new Document("_id", 17).append("v", 1));
    assertThat(collection.find(eq("_id", 17)).first()).isNotNull();
    assertThat(collection.find(eq("_id", 16)).first()).isNull();
  }

  @Test
  void reservedTagIsRefusedByUpdateToo() {
    collection.insertOne(new Document("_id", 18).append("x", 1));
    assertThatThrownBy(() -> collection.updateOne(eq("_id", 18), set("v", new Document("$bson", "timestamp").append("value", 5L))))
        .isInstanceOf(MongoCommandException.class);
    assertThat(collection.find(eq("_id", 18)).first().containsKey("v")).isFalse();
  }

  @Test
  void negativeZeroDecimalIsStoredAsZero() {
    final Object v = roundTrip(19, Decimal128.NEGATIVE_ZERO);
    assertThat(((Decimal128) v).bigDecimalValue().signum()).isZero();
  }

  @Test
  void inFilterOnMinAndMaxKey() {
    collection.insertOne(new Document("_id", 20).append("v", new MinKey()));
    collection.insertOne(new Document("_id", 21).append("v", new MaxKey()));
    assertThat(collection.find(in("v", List.of(new MaxKey()))).first().get("_id")).isEqualTo(21);
  }

  @Test
  void decimalWiderThanDecimal128DoesNotFailTheRead() {
    final Database db = getServer(0).getDatabase(getDatabaseName());
    db.transaction(() -> db.newDocument("bson").set("_id", 30).set("v", new BigDecimal("1234567890123456789012345678901234567890.5")).save());
    final Document back = collection.find(eq("_id", 30)).first();
    assertThat(back).isNotNull();
    assertThat(((Number) back.get("v")).doubleValue()).isGreaterThan(1e39);
  }

  @Test
  void binaryEqualityFilterMatches() {
    collection.insertOne(new Document("_id", 31).append("v", new Binary(new byte[] { 1, 2, 3 })));
    collection.insertOne(new Document("_id", 32).append("v", new Binary(new byte[] { 4 })));
    assertThat(collection.find(eq("v", new Binary(new byte[] { 1, 2, 3 }))).first().get("_id")).isEqualTo(31);
  }

  @Test
  void incOnDecimalFieldKeepsPrecision() {
    collection.insertOne(new Document("_id", 34).append("v", new Decimal128(new BigDecimal("0.1"))));
    collection.updateOne(eq("_id", 34), inc("v", new Decimal128(new BigDecimal("0.2"))));
    assertThat(collection.find(eq("_id", 34)).first().get("v")).isEqualTo(new Decimal128(new BigDecimal("0.3")));
  }

  @Test
  void driverRefusesReservedTagWhateverItsValueType() {
    assertThatThrownBy(() -> collection.insertOne(new Document("_id", 35).append("v", new Document("$bson", 1)))).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void upsertIncOnMissingFieldStoresDecimal() {
    collection.updateOne(eq("_id", 40), inc("v", new Decimal128(new BigDecimal("2.5"))), new UpdateOptions().upsert(true));
    assertThat(collection.find(eq("_id", 40)).first().get("v")).isEqualTo(new Decimal128(new BigDecimal("2.5")));
  }

  @Test
  void upsertIncOnExistingDecimalKeepsPrecision() {
    collection.insertOne(new Document("_id", 41).append("v", new Decimal128(new BigDecimal("0.1"))));
    collection.updateOne(eq("_id", 41), inc("v", new Decimal128(new BigDecimal("0.2"))), new UpdateOptions().upsert(true));
    assertThat(collection.find(eq("_id", 41)).first().get("v")).isEqualTo(new Decimal128(new BigDecimal("0.3")));
  }

  @Test
  void rangeFilterOnDecimalField() {
    collection.insertOne(new Document("_id", 42).append("v", new Decimal128(new BigDecimal("1.5"))));
    collection.insertOne(new Document("_id", 43).append("v", new Decimal128(new BigDecimal("9.5"))));
    assertThat(collection.find(gt("v", new Decimal128(new BigDecimal("5")))).first().get("_id")).isEqualTo(43);
    assertThat(collection.find(lt("v", 5)).first().get("_id")).isEqualTo(42);
  }

  @Test
  void orFilterContainingTaggedValues() {
    collection.insertOne(new Document("_id", 44).append("v", new BsonTimestamp(1, 1)));
    collection.insertOne(new Document("_id", 45).append("v", new MaxKey()));
    assertThat(collection.find(or(eq("v", new BsonTimestamp(1, 1)), eq("v", new MinKey()))).first().get("_id")).isEqualTo(44);
  }

  @Test
  void regexWithoutOptionsRoundTrips() {
    assertThat(roundTrip(46, new BsonRegularExpression("abc"))).isEqualTo(new BsonRegularExpression("abc"));
  }

  @Test
  void upsertDoesNotSeedARegexFilterIntoTheDocument() {
    collection.updateOne(new Document("name", new BsonRegularExpression("^a")), set("x", 1), new UpdateOptions().upsert(true));
    final Document back = collection.find(eq("x", 1)).first();
    assertThat(back).isNotNull();
    assertThat(back.containsKey("name")).isFalse();
  }

  @Test
  void setWithNanDecimalIsRefused() {
    collection.insertOne(new Document("_id", 48).append("x", 1));
    assertThatThrownBy(() -> collection.updateOne(eq("_id", 48), set("v", Decimal128.NaN))).isInstanceOf(MongoCommandException.class);
    assertThat(collection.find(eq("_id", 48)).first().containsKey("v")).isFalse();
  }

  @Test
  void malformedTagWithNestedDateStillReads() {
    final Database db = getServer(0).getDatabase(getDatabaseName());
    db.transaction(() -> db.newDocument("bson").set("_id", 50)
        .set("v", Map.of(MongoBsonValues.TAG, "timestamp", "when", LocalDateTime.of(2024, 1, 2, 3, 4))).save());
    final Document back = collection.find(eq("_id", 50)).first();
    assertThat(back).isNotNull();
    assertThat(((Document) back.get("v")).get(MongoBsonValues.TAG)).isEqualTo("timestamp");
  }

  @Test
  void objectIdInANonIdFieldKeepsItsType() {
    final ObjectId oid = new ObjectId("507f1f77bcf86cd799439011");
    assertThat(roundTrip(60, oid)).isEqualTo(oid);
    assertThat(collection.find(eq("v", oid)).first().get("_id")).isEqualTo(60);
    assertThat(collection.find(in("v", List.of(oid))).first().get("_id")).isEqualTo(60);
  }

  @Test
  void objectIdInAnArrayAndAfterUpdateKeepsItsType() {
    final ObjectId oid = new ObjectId("507f1f77bcf86cd799439012");
    collection.insertOne(new Document("_id", 61).append("refs", List.of(oid)));
    assertThat(((List<?>) collection.find(eq("_id", 61)).first().get("refs")).get(0)).isEqualTo(oid);
    collection.updateOne(eq("_id", 61), set("ref", oid));
    assertThat(collection.find(eq("_id", 61)).first().get("ref")).isEqualTo(oid);
  }

  @Test
  void filterMatchesAnObjectIdStoredAsHexBeforeTagging() {
    final ObjectId oid = new ObjectId("507f1f77bcf86cd799439013");
    final Database db = getServer(0).getDatabase(getDatabaseName());
    db.transaction(() -> db.newDocument("bson").set("_id", 62).set("ref", oid.toHexString()).save());
    assertThat(collection.find(eq("ref", oid)).first().get("_id")).isEqualTo(62);
    assertThat(collection.find(in("ref", List.of(oid))).first().get("_id")).isEqualTo(62);
  }

  @Test
  void indexedNonIdObjectIdFieldStillWorks() {
    collection.createIndex(new Document("ref", 1));
    final ObjectId oid = new ObjectId("507f1f77bcf86cd799439014");
    collection.insertOne(new Document("_id", 63).append("ref", oid));
    collection.insertOne(new Document("_id", 64).append("ref", new ObjectId("507f1f77bcf86cd799439015")));
    assertThat(collection.find(eq("ref", oid)).first().get("_id")).isEqualTo(63);
    assertThat(collection.find(in("ref", List.of(oid))).first().get("_id")).isEqualTo(63);
  }

  @Test
  void aStringThatLooksLikeAnEncodedObjectIdStaysAString() {
    assertThat(roundTrip(65, "$oid:507f1f77bcf86cd799439011")).isEqualTo("$oid:507f1f77bcf86cd799439011");
    assertThat(roundTrip(66, "$str:abc")).isEqualTo("$str:abc");
    assertThat(roundTrip(67, "$other")).isEqualTo("$other");
    assertThat(collection.find(eq("v", "$oid:507f1f77bcf86cd799439011")).first().get("_id")).isEqualTo(65);
  }

  @Test
  void explicitEqAndNeOnAnObjectIdMatchBothStoredForms() {
    final ObjectId oid = new ObjectId("507f1f77bcf86cd799439016");
    final Database db = getServer(0).getDatabase(getDatabaseName());
    db.transaction(() -> db.newDocument("bson").set("_id", 68).set("ref", oid.toHexString()).save());
    collection.insertOne(new Document("_id", 69).append("ref", oid));
    assertThat(collection.find(new Document("ref", new Document("$eq", oid))).into(new ArrayList<>())).hasSize(2);
    assertThat(collection.find(new Document("ref", new Document("$ne", oid))).into(new ArrayList<>())).isEmpty();
  }

  @Test
  void idStringsWithAReservedPrefixRoundTripAndMatchFilters() {
    collection.insertOne(new Document("_id", "$oid:abc").append("n", 1));
    collection.insertOne(new Document("_id", "$str:x").append("n", 2));
    assertThat(collection.find(eq("_id", "$oid:abc")).first().get("n")).isEqualTo(1);
    assertThat(collection.find(in("_id", List.of("$str:x"))).first().get("n")).isEqualTo(2);
    assertThat(collection.find(eq("_id", "$str:x")).first().get("_id")).isEqualTo("$str:x");
  }

  @Test
  void notEqOnAnObjectIdExcludesBothStoredForms() {
    final ObjectId oid = new ObjectId("507f1f77bcf86cd799439017");
    final Database db = getServer(0).getDatabase(getDatabaseName());
    db.transaction(() -> db.newDocument("bson").set("_id", 70).set("ref", oid.toHexString()).save());
    collection.insertOne(new Document("_id", 71).append("ref", oid));
    collection.insertOne(new Document("_id", 72).append("ref", new ObjectId("507f1f77bcf86cd799439018")));
    final List<Document> found = collection.find(new Document("ref", new Document("$not", new Document("$eq", oid)))).into(new ArrayList<>());
    assertThat(found).hasSize(1);
    assertThat(found.getFirst().get("_id")).isEqualTo(72);
  }
}
