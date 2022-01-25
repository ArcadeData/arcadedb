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

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCursor;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MongoDBServerTest extends BaseGraphServerTest {

  private static final int                                                   DEF_PORT = 27017;
  private              com.mongodb.client.MongoCollection<org.bson.Document> collection;
  private              MongoClient                                           client;
  private              Document                                              obj;

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

    client = new MongoClient(new ServerAddress("localhost", DEF_PORT));
    client.getDatabase(getDatabaseName()).createCollection("MongoDBCollection");

    collection = client.getDatabase(getDatabaseName()).getCollection("MongoDBCollection");
    assertEquals(0, collection.countDocuments());

    obj = new Document("id", 0).append("name", "Jay").append("lastName", "Miner").append("id", 0);
    collection.insertOne(obj);
    for (int i = 1; i < 10; i++)
      collection.insertOne(new Document("id", 0).append("name", "Jay").append("lastName", "Miner").append("id", i));
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    client.close();
    super.endTest();
  }

  @Test
  public void testSimpleInsertQuery() {
    assertEquals(10, collection.countDocuments());
    assertEquals(obj, collection.find().first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ name: \"Jay\" } ")).first());
    assertNull(collection.find(BsonDocument.parse("{ name: \"Jay2\" } ")).first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $eq: \"Jay\" } } ")).first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $ne: \"Jay2\" } } ")).first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $in: [ \"Jay\", \"John\" ] } } ")).first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $nin: [ \"Jay2\", \"John\" ] } } ")).first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $lt: \"Jay2\" } } ")).first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $lte: \"Jay2\" } } ")).first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $gt: \"A\" } } ")).first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $gte: \"A\" } } ")).first());
    assertEquals(obj, collection.find(and(gt("name", "A"), lte("name", "Jay"))).first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ $or: [ { name: { $eq: 'Jay' } }, { lastName: 'Miner222'} ] }")).first());
    assertEquals(obj, collection.find(BsonDocument.parse("{ $not: { name: { $eq: 'Jay2' } } }")).first());
    assertEquals(obj, collection.find(BsonDocument.parse(
        "{ $and: [ { name: { $eq: 'Jay' } }, { lastName: { $exists: true } }, { lastName: { $eq: 'Miner' } }, { lastName: { $ne: 'Miner22' } } ] }")).first());
    assertEquals(obj, collection.find(and(eq("name", "Jay"), exists("lastName"), eq("lastName", "Miner"), ne("lastName", "Miner22"))).first());
  }

  @Test
  public void testOrderBy() {
    int i = 0;
    for (MongoCursor<Document> it = collection.find(BsonDocument.parse(
            "{ $and: [ { name: { $eq: 'Jay' } }, { lastName: { $exists: true } }, { lastName: { $eq: 'Miner' } }, { lastName: { $ne: 'Miner22' } } ], $orderBy: { id: 1 } }"))
        .iterator(); it.hasNext(); ++i) {
      final Document doc = it.next();
      assertEquals(i, doc.get("id"));
    }

    i = 9;
    for (MongoCursor<Document> it = collection.find(BsonDocument.parse(
            "{ $and: [ { name: { $eq: 'Jay' } }, { lastName: { $exists: true } }, { lastName: { $eq: 'Miner' } }, { lastName: { $ne: 'Miner22' } } ], $orderBy: { id: -1 } }"))
        .iterator(); it.hasNext(); --i) {
      final Document doc = it.next();
      assertEquals(i, doc.get("id"));
    }
  }
}
