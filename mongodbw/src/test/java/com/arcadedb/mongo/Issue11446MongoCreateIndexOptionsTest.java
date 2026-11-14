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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11446: {@code createIndexes} acknowledged every index option with {@code ok:1} and then ignored all but {@code key} and
 * {@code unique}, so a partial unique index rejected documents its filter excludes, a TTL index never expired anything, and a
 * {@code text} key became a plain ascending index. An option the plugin cannot honour is now refused with
 * {@code 67 CannotCreateIndex} naming it, and nothing is created.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11446MongoCreateIndexOptionsTest extends BaseMongoServerTest {

  private MongoClient   client;
  private MongoDatabase mongoDatabase;

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
    mongoDatabase = client.getDatabase(getDatabaseName());
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  /** Every option of the MongoDB index specification this plugin cannot honour, with a value that asks for its feature. */
  private static Map<String, Object> refusedOptions() {
    final Map<String, Object> options = new LinkedHashMap<>();
    options.put("partialFilterExpression", new Document("active", true));
    options.put("expireAfterSeconds", 1);
    options.put("sparse", true);
    options.put("hidden", true);
    options.put("collation", new Document("locale", "en").append("strength", 2));
    options.put("weights", new Document("k", 10));
    options.put("default_language", "english");
    options.put("language_override", "lang");
    options.put("textIndexVersion", 3);
    options.put("2dsphereIndexVersion", 3);
    options.put("bits", 26);
    options.put("min", -180);
    options.put("max", 180);
    options.put("bucketSize", 1);
    options.put("storageEngine", new Document("wiredTiger", new Document()));
    options.put("wildcardProjection", new Document("k", 1));
    options.put("prepareUnique", true);
    options.put("dropDups", true);
    return options;
  }

  @Test
  void everyUnsupportedIndexOptionIsRefusedByName() {
    for (final Map.Entry<String, Object> option : refusedOptions().entrySet()) {
      final String collection = "c_" + option.getKey();
      final Document spec = new Document("key", new Document("k", 1)).append("name", "k_1").append(option.getKey(), option.getValue());

      assertThatThrownBy(() -> createIndexes(collection, spec)).as(option.getKey())
          .isInstanceOfSatisfying(MongoCommandException.class, e -> {
            assertThat(e.getErrorCode()).as(option.getKey()).isEqualTo(67);
            assertThat(e.getErrorCodeName()).as(option.getKey()).isEqualTo("CannotCreateIndex");
            assertThat(e.getErrorMessage()).as(option.getKey()).contains("'" + option.getKey() + "'");
          });

      // A REFUSED SPECIFICATION CREATES NOTHING, NOT EVEN THE COLLECTION THE COMMAND WOULD HAVE CREATED AUTOMATICALLY
      assertThat(serverDatabase().getSchema().existsType(collection)).as(option.getKey()).isFalse();
    }
  }

  @Test
  void everySupportedIndexOptionIsAccepted() {
    final Document spec = new Document("key", new Document("k", 1)).append("name", "k_1").append("unique", true).append("background", true)
        .append("v", 2).append("ns", getDatabaseName() + ".supported").append("sparse", false).append("hidden", false);

    final Document reply = createIndexes("supported", spec);

    assertThat(reply.get("ok")).isEqualTo(1.0);
    assertThat(serverDatabase().getSchema().getType("supported").getIndexByProperties("k").isUnique()).isTrue();
  }

  @Test
  void specialIndexKindsAreRefusedByName() {
    for (final Object kind : List.of("text", "hashed", "2dsphere", "2d", "geoHaystack", 0)) {
      final Document spec = new Document("key", new Document("b", kind)).append("name", "b_" + kind);

      assertThatThrownBy(() -> createIndexes("kinds", spec)).as("%s", kind)
          .isInstanceOfSatisfying(MongoCommandException.class, e -> {
            assertThat(e.getErrorCode()).as("%s", kind).isEqualTo(67);
            assertThat(e.getErrorMessage()).as("%s", kind).contains(String.valueOf(kind)).contains("'b'");
          });
    }
    assertThat(serverDatabase().getSchema().existsType("kinds")).isFalse();
  }

  @Test
  void ascendingAndDescendingKeysOfAnyNumericTypeAreAccepted() {
    createIndexes("directions", new Document("key", new Document("a", 1).append("b", -1L).append("c", 1.0)).append("name", "abc"));

    assertThat(serverDatabase().getSchema().getType("directions").getIndexByProperties("a", "b", "c")).isNotNull();
  }

  @Test
  void aSpecificationWithoutKeyFailsToParse() {
    assertThatThrownBy(() -> createIndexes("nokey", new Document("name", "x")))
        .isInstanceOfSatisfying(MongoCommandException.class, e -> {
          assertThat(e.getErrorCode()).isEqualTo(9);
          assertThat(e.getErrorMessage()).contains("'key'");
        });
    assertThat(serverDatabase().getSchema().existsType("nokey")).isFalse();
  }

  @Test
  void aMalformedIndexesFieldFailsToParse() {
    for (final Object indexes : List.<Object>of("k_1", List.of("k_1"), List.of(new Document("key", new Document("k", 1)).append("name", "k_1"), 7))) {
      assertThatThrownBy(() -> mongoDatabase.runCommand(new Document("createIndexes", "malformed").append("indexes", indexes))).as("%s", indexes)
          .isInstanceOfSatisfying(MongoCommandException.class, e -> {
            assertThat(e.getErrorCode()).as("%s", indexes).isEqualTo(9);
            assertThat(e.getErrorMessage()).as("%s", indexes).contains("'indexes'");
          });
    }
    assertThat(serverDatabase().getSchema().existsType("malformed")).isFalse();
  }

  @Test
  void oneRefusedSpecificationRefusesTheWholeBatch() {
    final Document good = new Document("key", new Document("a", 1)).append("name", "a_1");
    final Document bad = new Document("key", new Document("b", 1)).append("name", "b_ttl").append("expireAfterSeconds", 60);

    assertThatThrownBy(() -> createIndexes("batch", good, bad)).isInstanceOf(MongoCommandException.class);

    assertThat(serverDatabase().getSchema().existsType("batch")).isFalse();
  }

  /**
   * The reporter's scenario through the driver's helper: the partial unique index is refused, so it cannot reject the second
   * document its filter excludes.
   */
  @Test
  void aPartialUniqueIndexIsRefusedInsteadOfBecomingUnconditional() {
    final MongoCollection<Document> pc = mongoDatabase.getCollection("pc");
    pc.insertOne(new Document("_id", 1).append("k", "a").append("active", false));

    assertThatThrownBy(() -> pc.createIndex(new Document("k", 1),
        new IndexOptions().name("k_u").unique(true).partialFilterExpression(new Document("active", true))))
        .isInstanceOfSatisfying(MongoCommandException.class, e -> assertThat(e.getErrorMessage()).contains("partialFilterExpression"));

    pc.insertOne(new Document("_id", 2).append("k", "a").append("active", false));
    assertThat(pc.countDocuments()).isEqualTo(2);
  }

  @Test
  void aTtlIndexIsRefusedThroughTheDriverHelper() {
    final MongoCollection<Document> ttl = mongoDatabase.getCollection("ttlc");
    assertThatThrownBy(() -> ttl.createIndex(new Document("ts", 1), new IndexOptions().expireAfter(1L, TimeUnit.SECONDS)))
        .isInstanceOfSatisfying(MongoCommandException.class, e -> assertThat(e.getErrorMessage()).contains("expireAfterSeconds"));
  }

  private Document createIndexes(final String collection, final Document... specs) {
    return mongoDatabase.runCommand(new Document("createIndexes", collection).append("indexes", List.of(specs)));
  }

  private Database serverDatabase() {
    return getServerDatabase(0, getDatabaseName());
  }
}
