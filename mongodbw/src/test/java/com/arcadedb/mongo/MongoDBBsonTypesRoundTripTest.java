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
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.assertj.core.api.Assertions.assertThat;

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
}
