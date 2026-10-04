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
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #9137 (array fields), #9136 (BSON type bracketing), #9138 (nested {@code $exists}), #9139 (dotted
 * collection names) and #9065 (implicit collection creation). Every expectation is the answer of a MongoDB 7 server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MongoDBFilterSemanticsTest extends BaseMongoServerTest {

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

  private MongoCollection<Document> collection(final String name, final String... documents) {
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection(name);
    final List<Document> list = new ArrayList<>();
    for (final String document : documents)
      list.add(Document.parse(document));
    collection.insertMany(list);
    return collection;
  }

  private static List<Object> ids(final MongoCollection<Document> collection, final String filter) {
    final List<Object> ids = new ArrayList<>();
    for (final Document d : collection.find(Document.parse(filter)).sort(Document.parse("{_id:1}")))
      ids.add(d.get("_id"));
    return ids;
  }

  private MongoCollection<Document> arrays() {
    return collection("arrays", "{_id:1, tags:['a','b']}", "{_id:2, tags:['b']}", "{_id:3, tags:[]}", "{_id:4}", "{_id:5, tags:[1,2,3]}",
        "{_id:6, tags:[{k:1},{k:2}]}");
  }

  @Test
  void arrayFieldsMatchTheirElements() {
    final MongoCollection<Document> c = arrays();
    assertThat(ids(c, "{tags:'a'}")).containsExactly(1);
    assertThat(ids(c, "{tags:'b'}")).containsExactly(1, 2);
    assertThat(ids(c, "{tags:{$in:['a']}}")).containsExactly(1);
    assertThat(ids(c, "{'tags.0':'a'}")).containsExactly(1);
    assertThat(ids(c, "{'tags.k':1}")).containsExactly(6);
    assertThat(ids(c, "{tags:2}")).containsExactly(5);
    assertThat(ids(c, "{tags:{$gt:2}}")).containsExactly(5);
    assertThat(ids(c, "{tags:{$size:0}}")).containsExactly(3);
    assertThat(ids(c, "{tags:{$all:['a','b']}}")).containsExactly(1);
    assertThat(ids(c, "{tags:{$elemMatch:{k:2}}}")).containsExactly(6);
    assertThat(ids(c, "{tags:['b']}")).containsExactly(2);
  }

  @Test
  void negationsOnArraysExcludeTheDocumentsHoldingTheElement() {
    final MongoCollection<Document> c = arrays();
    assertThat(ids(c, "{tags:{$ne:'a'}}")).containsExactly(2, 3, 4, 5, 6);
    assertThat(ids(c, "{tags:{$nin:['a']}}")).containsExactly(2, 3, 4, 5, 6);
  }

  @Test
  void deleteManyWithNeKeepsTheDocumentHoldingTheElement() {
    final MongoCollection<Document> c = arrays();
    assertThat(c.deleteMany(Document.parse("{tags:{$ne:'a'}}")).getDeletedCount()).isEqualTo(5);
    assertThat(ids(c, "{}")).containsExactly(1);
  }

  @Test
  void updateManyOnArrayElement() {
    final MongoCollection<Document> c = arrays();
    assertThat(c.updateMany(Document.parse("{tags:'b'}"), Document.parse("{$set:{hit:1}}")).getModifiedCount()).isEqualTo(2);
    assertThat(ids(c, "{hit:1}")).containsExactly(1, 2);
  }

  @Test
  void typesAreBracketed() {
    final MongoCollection<Document> c = collection("types", "{_id:1, n:5}", "{_id:2, n:'5'}", "{_id:3, n:5.0}", "{_id:4, n:true}",
        "{_id:5, n:'true'}", "{_id:6, n:1}");
    assertThat(ids(c, "{n:5}")).containsExactly(1, 3);
    assertThat(ids(c, "{n:'5'}")).containsExactly(2);
    assertThat(ids(c, "{n:true}")).containsExactly(4);
    assertThat(ids(c, "{n:1}")).containsExactly(6);
    assertThat(ids(c, "{n:{$gt:0}}")).containsExactly(1, 3, 6);

    final MongoCollection<Document> ages = collection("ages", "{_id:1, age:30}", "{_id:2, age:'40'}", "{_id:3, age:'abc'}", "{_id:4, age:20}");
    assertThat(ids(ages, "{age:{$gt:26}}")).containsExactly(1);
    assertThat(ids(ages, "{age:{$lt:'50'}}")).containsExactly(2);
  }

  @Test
  void dateDoesNotEqualALong() {
    final MongoCollection<Document> c = client.getDatabase(getDatabaseName()).getCollection("dates");
    c.insertOne(new Document("_id", 1).append("d", new Date(5000)));
    c.insertOne(new Document("_id", 2).append("d", 5000L));
    final List<Object> found = new ArrayList<>();
    for (final Document d : c.find(new Document("d", new Date(5000))))
      found.add(d.get("_id"));
    assertThat(found).containsExactly(1);
  }

  @Test
  void writesOnlyTouchTheDocumentsOfTheFilterType() {
    final MongoCollection<Document> c = collection("bytype", "{_id:1, n:5}", "{_id:2, n:'5'}");
    assertThat(c.updateMany(Document.parse("{n:'5'}"), Document.parse("{$set:{hit:1}}")).getModifiedCount()).isEqualTo(1);
    assertThat(c.deleteMany(Document.parse("{n:5}")).getDeletedCount()).isEqualTo(1);
    assertThat(ids(c, "{}")).containsExactly(2);
  }

  @Test
  void existsOnNestedPath() {
    final MongoCollection<Document> c = collection("nested", "{_id:1, a:{b:1, c:{d:'x'}}}", "{_id:2, a:{c:1}}", "{_id:3, a:5}", "{_id:4, z:1}",
        "{_id:5, a:null}", "{_id:6, a:{}}");
    assertThat(ids(c, "{'a.b':{$exists:true}}")).containsExactly(1);
    assertThat(ids(c, "{'a.b':{$exists:false}}")).containsExactly(2, 3, 4, 5, 6);
    assertThat(ids(c, "{'a.c.d':{$exists:true}}")).containsExactly(1);
    assertThat(ids(c, "{'a.c.d':{$exists:false}}")).containsExactly(2, 3, 4, 5, 6);
    assertThat(ids(c, "{'a.c':{$exists:true}}")).containsExactly(1, 2);
    assertThat(ids(c, "{a:{$exists:true}}")).containsExactly(1, 2, 3, 5, 6);
    assertThat(ids(c, "{a:{$exists:false}}")).containsExactly(4);
    assertThat(c.deleteMany(Document.parse("{'a.b':{$exists:false}}")).getDeletedCount()).isEqualTo(5);
  }

  @Test
  void dottedCollectionNameIsReadable() {
    final MongoCollection<Document> c = collection("fs.files", "{_id:1, k:'a'}", "{_id:2, k:'b'}");
    assertThat(c.countDocuments()).isEqualTo(2);
    assertThat(ids(c, "{}")).containsExactly(1, 2);
    assertThat(ids(c, "{k:'b'}")).containsExactly(2);
    assertThat(ids(c, "{_id:1}")).containsExactly(1);

    final MongoCollection<Document> deep = collection("a.b.c", "{_id:1, k:'a'}");
    assertThat(ids(deep, "{k:'a'}")).containsExactly(1);
  }

  @Test
  void insertAndCountOnMissingCollection() {
    final MongoCollection<Document> missing = client.getDatabase(getDatabaseName()).getCollection("never_created_0");
    assertThat(missing.countDocuments()).isZero();
    assertThat(missing.countDocuments(Document.parse("{k:1}"))).isZero();

    final MongoCollection<Document> one = client.getDatabase(getDatabaseName()).getCollection("never_created_1");
    one.insertOne(new Document("_id", 1));
    assertThat(one.countDocuments()).isEqualTo(1);

    final MongoCollection<Document> many = client.getDatabase(getDatabaseName()).getCollection("never_created_2");
    many.insertMany(List.of(new Document("_id", 1), new Document("_id", 2)));
    assertThat(many.countDocuments()).isEqualTo(2);
  }

  @Test
  void regexStillMatches() {
    final MongoCollection<Document> c = collection("regex", "{_id:1, s:'Hello'}", "{_id:2, s:'world'}", "{_id:3, s:['x','hello']}");
    assertThat(ids(c, "{s:{$regex:'^hel', $options:'i'}}")).containsExactly(1, 3);
    assertThat(ids(c, "{s:{$regex:'^hel', $options:'i', $ne:'Hello'}}")).containsExactly(3);
    assertThat(ids(c, "{s:{$not:{$regex:'^hel', $options:'i'}}}")).containsExactly(2);
  }
}
