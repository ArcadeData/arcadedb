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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12072: a dotted {@code find} projection applies the rest of the path to each element of an array (keeping embedded
 * documents, dropping scalars on inclusion), and an inclusion answers the fields in the order of the stored document. Every
 * expectation is the answer of a MongoDB 7 server, measured in the issue; the JSON is compared field by field, in order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12072MongoProjectionArraysTest extends BaseMongoServerTest {
  private static final String ORDER = "{_id: 1, sku: 'A1', items: [{name: 'pen', price: 2}, {name: 'ink', price: 5}], tags: [], owner: {name: 'x', id: 7}}";

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
  void anInclusionThroughAnArrayProjectsEachEmbeddedDocument() {
    assertProjection(ORDER, "{'items.name': 1}", "{_id: 1, items: [{name: 'pen'}, {name: 'ink'}]}");
    assertProjection(ORDER, "{'items.name': 1, _id: 0}", "{items: [{name: 'pen'}, {name: 'ink'}]}");
  }

  @Test
  void anExclusionThroughAnArrayRemovesFromEachEmbeddedDocument() {
    assertProjection(ORDER, "{'items.price': 0}",
        "{_id: 1, sku: 'A1', items: [{name: 'pen'}, {name: 'ink'}], tags: [], owner: {name: 'x', id: 7}}");
  }

  @Test
  void anEmptyArrayStaysAnEmptyArray() {
    assertProjection(ORDER, "{'tags.name': 1}", "{_id: 1, tags: []}");
  }

  @Test
  void anEmbeddedDocumentStillProjects() {
    assertProjection(ORDER, "{'owner.name': 1}", "{_id: 1, owner: {name: 'x'}}");
  }

  @Test
  void anInclusionAnswersTheFieldsInTheOrderOfTheDocument() {
    assertProjection(ORDER, "{owner: 1, sku: 1}", "{_id: 1, sku: 'A1', owner: {name: 'x', id: 7}}");
    assertProjection(ORDER, "{'owner.id': 1, 'owner.name': 1}", "{_id: 1, owner: {name: 'x', id: 7}}");
  }

  @Test
  void scalarsOfAnArrayAreDroppedAndNestedArraysAreEntered() {
    assertProjection("{_id: 2, v: [1, {x: 2}, [{x: 3}]]}", "{'v.x': 1}", "{_id: 2, v: [{x: 2}, [{x: 3}]]}");
    // an embedded document without the field stays, empty
    assertProjection("{_id: 3, v: [{x: 1, y: 1}, {y: 2}]}", "{'v.x': 1}", "{_id: 3, v: [{x: 1}, {}]}");
    // the exclusion keeps the scalars and enters the nested arrays
    assertProjection("{_id: 2, v: [1, {x: 2, y: 1}, [{x: 3}]]}", "{'v.x': 0}", "{_id: 2, v: [1, {y: 1}, [{}]]}");
  }

  @Test
  void aScalarOnTheWayIsLeftOutOfAnInclusionAndKeptByAnExclusion() {
    assertProjection("{_id: 4, a: 5, b: 1}", "{'a.x': 1}", "{_id: 4}");
    assertProjection("{_id: 4, a: 5, b: 1}", "{'a.x': 0}", "{_id: 4, a: 5, b: 1}");
  }

  @Test
  void aPathCollisionIsRefused() {
    final MongoCollection<Document> collection = collection(ORDER);
    assertThatThrownBy(() -> collection.find().projection(Document.parse("{owner: 1, 'owner.name': 1}")).first())
        .isInstanceOf(MongoException.class);
    assertThatThrownBy(() -> collection.find().projection(Document.parse("{'owner.name': 0, owner: 0}")).first())
        .isInstanceOf(MongoException.class);
  }

  private void assertProjection(final String document, final String projection, final String expected) {
    final MongoCollection<Document> collection = collection(document);
    final Document found = collection.find().projection(Document.parse(projection)).first();
    assertThat(found).isNotNull();
    assertThat(found.toJson()).as("find({}, %s)", projection).isEqualTo(Document.parse(expected).toJson());
  }

  private MongoCollection<Document> collection(final String document) {
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("p" + (collections++));
    collection.insertMany(List.of(Document.parse(document)));
    return collection;
  }
}
