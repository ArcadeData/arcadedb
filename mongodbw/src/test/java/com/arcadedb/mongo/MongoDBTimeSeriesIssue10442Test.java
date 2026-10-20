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
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10442 (follow-up of #10440): an unfiltered {@code db.Ev.find()} over a TIMESERIES type {@code Ev} returned no
 * document, and a filtered count answered 0, while {@code db.Ev.count()} answered the number of samples. The samples live in the
 * type's own engine, not in its buckets, so every read now goes through SQL, which serves them.
 */
public class MongoDBTimeSeriesIssue10442Test extends BaseMongoServerTest {
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
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sqlscript", """
        CREATE TIMESERIES TYPE Ev TIMESTAMP ts PRECISION MILLISECOND TAGS (k STRING) FIELDS (v DOUBLE);
        INSERT INTO Ev SET ts = 1792396800000, k = 'a', v = 1.0;
        INSERT INTO Ev SET ts = 1792396860000, k = 'a', v = 2.0;
        INSERT INTO Ev SET ts = 1792396920000, k = 'b', v = 3.0;
        """);

    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    final MongoDatabase mongoDatabase = client.getDatabase(getDatabaseName());
    collection = mongoDatabase.getCollection("Ev");
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
  void unfilteredFindReturnsTheSamples() {
    final List<Document> all = collection.find().into(new ArrayList<>());
    assertThat(all).hasSize(3);
    final List<Object> values = new ArrayList<>();
    for (final Document d : all)
      values.add(((Number) d.get("v")).doubleValue());
    assertThat(values).containsExactlyInAnyOrder(1.0, 2.0, 3.0);
  }

  @Test
  void unfilteredFindHonorsSkipAndLimit() {
    assertThat(collection.find().skip(1).into(new ArrayList<>())).hasSize(2);
    assertThat(collection.find().limit(2).into(new ArrayList<>())).hasSize(2);
  }

  @Test
  void filteredAndSortedFindReturnTheMatchingSamples() {
    assertThat(collection.find(Filters.eq("k", "a")).into(new ArrayList<>())).hasSize(2);
    assertThat(collection.find(Filters.gt("v", 1.5)).into(new ArrayList<>())).hasSize(2);

    final List<Document> sorted = collection.find().sort(Sorts.descending("v")).into(new ArrayList<>());
    assertThat(sorted).hasSize(3);
    assertThat(((Number) sorted.getFirst().get("v")).doubleValue()).isEqualTo(3.0);
  }

  @Test
  void countsAgreeWithTheSamples() {
    // the raw count command: unfiltered (answered from the type), filtered (scanned) and with skip/limit
    assertThat(collection.count()).isEqualTo(3);
    assertThat(collection.count(Filters.eq("k", "a"))).isEqualTo(2);
    assertThat(collection.count(Filters.gt("v", 1.5))).isEqualTo(2);
    assertThat(collection.count(new Document(), new CountOptions().skip(1))).isEqualTo(2);
    assertThat(collection.count(Filters.eq("k", "a"), new CountOptions().skip(1))).isEqualTo(1);
    // countDocuments() is an aggregate pipeline over the collection
    assertThat(collection.countDocuments()).isEqualTo(3);
    assertThat(collection.countDocuments(Filters.eq("k", "b"))).isEqualTo(1);
    assertThat(collection.estimatedDocumentCount()).isEqualTo(3);
  }

  @Test
  void mongoQueryLanguageReturnsTheSamples() {
    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(countRows(database.query("mongo", "{ collection: 'Ev', query: {} }"))).isEqualTo(3);
    assertThat(countRows(database.query("mongo", "{ collection: 'Ev', query: { k: { $eq: 'b' } } }"))).isEqualTo(1);
  }

  private static long countRows(final ResultSet rs) {
    long n = 0;
    try (rs) {
      while (rs.hasNext()) {
        rs.next();
        ++n;
      }
    }
    return n;
  }
}
