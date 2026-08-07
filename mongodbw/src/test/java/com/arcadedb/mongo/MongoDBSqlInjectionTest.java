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
import com.arcadedb.server.BaseGraphServerTest;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A MongoDB command is translated into a SQL statement, so every field name and every value taken off the wire is attacker
 * controlled text that ends up inside that statement. Names are back-tick quoted and values single quoted; neither may be able to
 * close its quoting and append clauses of its own.
 */
public class MongoDBSqlInjectionTest extends BaseGraphServerTest {

  private static final int                       DEF_PORT = 27017;
  private              MongoClient               client;
  private              MongoCollection<Document> collection;

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
    client = new MongoClient(new ServerAddress("localhost", DEF_PORT),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    client.getDatabase(getDatabaseName()).createCollection("doc");
    collection = client.getDatabase(getDatabaseName()).getCollection("doc");
    for (int i = 0; i < 5; i++)
      collection.insertOne(new Document("name", "v" + i).append("victim", "present"));
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
  void aQuoteInAFilterValueCannotExtendTheWhereClause() {
    // the crafted value closes the literal and adds an always-true disjunction, which would widen the update to every document
    final UpdateResult result = collection.updateMany(new Document("name", "v1' OR 'x' = 'x"),
        new Document("$set", new Document("pwned", "yes")));

    assertThat(result.getModifiedCount()).isZero();
    assertThat(collection.countDocuments(new Document("pwned", "yes"))).isZero();
  }

  @Test
  void aBackTickInAnUnsetFieldNameCannotNameASecondProperty() {
    // the crafted name closes the identifier and names a property the client never asked to remove
    collection.updateOne(eq("name", "v1"), new Document("$unset", new Document("harmless`, `victim", "")));

    final Document after = collection.find(eq("name", "v1")).first();
    assertThat(after).isNotNull();
    assertThat(after.containsKey("victim")).isTrue();
  }

  @Test
  void aCraftedFilterFieldNameCannotBecomeAPredicate() {
    // the field name is appended as an identifier, so it cannot introduce an always-true condition of its own
    final UpdateResult result = collection.updateMany(new Document("1 = 1 OR name", "nomatch"),
        new Document("$set", new Document("pwned", "yes")));

    assertThat(result.getModifiedCount()).isZero();
    assertThat(collection.countDocuments(new Document("pwned", "yes"))).isZero();
  }

  @Test
  void ordinaryFiltersAndUpdatesStillWork() {
    assertThat(collection.countDocuments(new Document("name", "v1"))).isEqualTo(1);

    final UpdateResult result = collection.updateMany(new Document("name", "v2"),
        new Document("$set", new Document("touched", "yes")));
    assertThat(result.getModifiedCount()).isEqualTo(1);

    collection.updateOne(eq("name", "v3"), new Document("$unset", new Document("victim", "")));
    final Document after = collection.find(eq("name", "v3")).first();
    assertThat(after).isNotNull();
    assertThat(after.containsKey("victim")).isFalse();
  }
}
