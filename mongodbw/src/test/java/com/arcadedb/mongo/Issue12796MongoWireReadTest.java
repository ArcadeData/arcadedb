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
import com.arcadedb.database.MutableDocument;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12796, over the wire: a record written through SQL with a FLOAT, SHORT, BYTE, BINARY, ARRAY_OF_*, LINK or EMBEDDED value
 * made the reply of every {@code find} that reached it impossible to encode, and the server closed the connection. A real MongoDB
 * client reads the collection here, and filters and sorts on those values, which are compared in the form they are returned in.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12796MongoWireReadTest extends BaseMongoServerTest {
  private MongoClient               client;
  private MongoCollection<Document> collection;
  private String                    linked;

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
    database.command("sql", "CREATE VERTEX TYPE Target12796");
    database.command("sql", "CREATE DOCUMENT TYPE X12796");
    database.command("sql", "CREATE PROPERTY X12796.f FLOAT");
    database.command("sql", "CREATE PROPERTY X12796.sh SHORT");
    database.command("sql", "CREATE PROPERTY X12796.bt BYTE");
    database.command("sql", "CREATE PROPERTY X12796.bin BINARY");
    database.command("sql", "CREATE PROPERTY X12796.ash ARRAY_OF_SHORTS");
    database.command("sql", "CREATE PROPERTY X12796.ai ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY X12796.al ARRAY_OF_LONGS");
    database.command("sql", "CREATE PROPERTY X12796.af ARRAY_OF_FLOATS");
    database.command("sql", "CREATE PROPERTY X12796.ad ARRAY_OF_DOUBLES");
    database.command("sql", "CREATE PROPERTY X12796.e EMBEDDED");
    database.command("sql", "CREATE PROPERTY X12796.lk LINK");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Target12796 SET n = 1");
      database.command("sql",
          "INSERT INTO X12796 SET k = 'a', f = 1.1f, sh = 5, bt = 3, ash = [1, 2], ai = [3, 4], al = [5, 6], af = [1.5f, 2.5f],"
              + " ad = [1.25, 2.5], e = {'@type': 'X12796', 'sh': 9}, lk = (SELECT FROM Target12796 LIMIT 1),"
              + " lf = [1.5f, 2, 'a', {'x': 2.5f}], mp = {'k': 7.5f}");
      database.command("sql", "INSERT INTO X12796 SET k = 'b', f = 0.5f, sh = 7, bt = 4");
      final MutableDocument binary = database.newDocument("X12796");
      binary.set("k", "c");
      binary.set("bin", new byte[] { 1, 2, 3 });
      binary.save();
    });
    linked = database.query("sql", "SELECT FROM Target12796").next().getIdentity().get().toString();

    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    collection = client.getDatabase(getDatabaseName()).getCollection("X12796");
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
  void aClientReadsEveryRecordOfTheCollection() {
    assertThat(ks(new Document())).containsExactlyInAnyOrder("a", "b", "c");

    final Document a = collection.find(new Document("k", "a")).first();
    assertThat(a.get("f")).as("a FLOAT reads as the decimal text it was written as").isEqualTo(1.1d);
    assertThat(a.get("sh")).isEqualTo(5);
    assertThat(a.get("bt")).isEqualTo(3);
    assertThat(a.get("ash")).isEqualTo(List.of(1, 2));
    assertThat(a.get("ai")).isEqualTo(List.of(3, 4));
    assertThat(a.get("al")).isEqualTo(List.of(5L, 6L));
    assertThat(a.get("af")).isEqualTo(List.of(1.5d, 2.5d));
    assertThat(a.get("ad")).isEqualTo(List.of(1.25d, 2.5d));
    assertThat(a.get("e")).isEqualTo(new Document("sh", 9));
    assertThat(a.get("lk")).isEqualTo(linked);
    assertThat(a.get("lf")).isEqualTo(List.of(1.5d, 2, "a", new Document("x", 2.5d)));
    assertThat(a.get("mp")).isEqualTo(new Document("k", 7.5d));

    final Document c = collection.find(new Document("k", "c")).first();
    assertThat(((Binary) c.get("bin")).getData()).containsExactly(1, 2, 3);
  }

  @Test
  void aFilterComparesTheValueTheClientReads() {
    assertThat(ks(new Document("f", 1.1d))).containsExactly("a");
    assertThat(ks(Document.parse("{f: {$gt: 1}}"))).containsExactly("a");
    assertThat(ks(Document.parse("{f: {$lt: 1}}"))).containsExactly("b");
    assertThat(ks(new Document("sh", 5))).containsExactly("a");
    assertThat(ks(Document.parse("{bt: {$gte: 4}}"))).containsExactly("b");
    assertThat(ks(new Document("lk", linked))).containsExactly("a");
    assertThat(ks(new Document("ai", 4))).containsExactly("a");
    assertThat(ks(new Document("af", 2.5d))).containsExactly("a");
    assertThat(ks(new Document("e.sh", 9))).containsExactly("a");
  }

  @Test
  void aSortOrdersByTheValueTheClientReads() {
    final List<Object> ascending = new ArrayList<>();
    for (final Document document : collection.find(Document.parse("{f: {$exists: true}}")).sort(new Document("f", 1)))
      ascending.add(document.get("k"));
    assertThat(ascending).containsExactly("b", "a");

    final List<Object> descending = new ArrayList<>();
    for (final Document document : collection.find(Document.parse("{sh: {$exists: true}}")).sort(new Document("sh", -1)).limit(1))
      descending.add(document.get("k"));
    assertThat(descending).containsExactly("b");
  }

  private List<Object> ks(final Document filter) {
    final List<Object> ks = new ArrayList<>();
    for (final Document document : collection.find(filter))
      ks.add(document.get("k"));
    return ks;
  }
}
