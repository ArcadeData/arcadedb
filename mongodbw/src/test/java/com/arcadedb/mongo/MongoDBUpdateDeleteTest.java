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
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4750: support the MongoDB {@code update} (replaceOne/updateOne/...) and
 * {@code delete} (deleteOne/deleteMany) commands in the mongodbw plugin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBUpdateDeleteTest extends BaseGraphServerTest {

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

    client = new MongoClient(new ServerAddress("localhost", DEF_PORT), MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()), MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    client.getDatabase(getDatabaseName()).createCollection("doc");
    collection = client.getDatabase(getDatabaseName()).getCollection("doc");

    for (int i = 0; i < 10; i++)
      collection.insertOne(new Document("test", "v" + i).append("counter", i));
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
  void deleteOne() {
    final DeleteResult result = collection.deleteOne(eq("test", "v5"));
    assertThat(result.getDeletedCount()).isEqualTo(1);
    assertThat(collection.countDocuments()).isEqualTo(9);
    assertThat(collection.find(eq("test", "v5")).first()).isNull();
  }

  @Test
  void deleteOneMatchingManyRemovesSingle() {
    // All 10 documents match; deleteOne must remove exactly one.
    final DeleteResult result = collection.deleteOne(new Document());
    assertThat(result.getDeletedCount()).isEqualTo(1);
    assertThat(collection.countDocuments()).isEqualTo(9);
  }

  @Test
  void deleteMany() {
    final DeleteResult result = collection.deleteMany(new Document("counter", new Document("$gte", 5)));
    assertThat(result.getDeletedCount()).isEqualTo(5);
    assertThat(collection.countDocuments()).isEqualTo(5);
  }

  @Test
  void replaceOne() {
    // This is the exact call from issue #4750: db.doc.replaceOne({test:{$eq:"v3"}},{test:"vwxyz"})
    final UpdateResult result = collection.replaceOne(new Document("test", new Document("$eq", "v3")),
        new Document("test", "vwxyz"));
    assertThat(result.getModifiedCount()).isEqualTo(1);

    assertThat(collection.find(eq("test", "v3")).first()).isNull();
    final Document replaced = collection.find(eq("test", "vwxyz")).first();
    assertThat(replaced).isNotNull();
    // The replacement document fully replaces the old one: the 'counter' field is gone.
    assertThat(replaced.get("counter")).isNull();
  }

  @Test
  void updateOneWithSet() {
    final UpdateResult result = collection.updateOne(eq("test", "v2"), new Document("$set", new Document("name", "Jay")));
    assertThat(result.getModifiedCount()).isEqualTo(1);

    final Document updated = collection.find(eq("test", "v2")).first();
    assertThat(updated).isNotNull();
    assertThat(updated.get("name")).isEqualTo("Jay");
    // $set keeps the other fields.
    assertThat(updated.get("counter")).isEqualTo(2);
  }

  @Test
  void updateManyWithInc() {
    final UpdateResult result = collection.updateMany(new Document("counter", new Document("$gte", 8)),
        new Document("$inc", new Document("counter", 100)));
    assertThat(result.getModifiedCount()).isEqualTo(2);

    assertThat(collection.find(eq("counter", 108)).first()).isNotNull();
    assertThat(collection.find(eq("counter", 109)).first()).isNotNull();
  }

  @Test
  void updateOneWithUnset() {
    final UpdateResult result = collection.updateOne(eq("test", "v1"), new Document("$unset", new Document("counter", "")));
    assertThat(result.getModifiedCount()).isEqualTo(1);

    final Document updated = collection.find(eq("test", "v1")).first();
    assertThat(updated).isNotNull();
    assertThat(updated.get("counter")).isNull();
  }

  @Test
  void upsertInsertsWhenNoMatch() {
    final UpdateResult result = collection.updateOne(eq("test", "missing"),
        new Document("$set", new Document("counter", 999)), new UpdateOptions().upsert(true));

    assertThat(result.getUpsertedId()).isNotNull();
    final Document inserted = collection.find(eq("test", "missing")).first();
    assertThat(inserted).isNotNull();
    assertThat(inserted.get("counter")).isEqualTo(999);
  }

  /**
   * Regression test for issue #6941: {@code executeUpsert} seeded the new record's {@code _id} from the filter
   * but then unconditionally overwrote it with a freshly generated one, so repeating an upsert filtered on
   * {@code _id} (the idiomatic "upsert by primary key" pattern) never matched its own previous insert and kept
   * creating duplicates instead of updating in place.
   */
  @Test
  void upsertFilteredOnIdIsIdempotent() {
    final ObjectId id = new ObjectId();

    final UpdateResult first = collection.updateOne(eq("_id", id), new Document("$set", new Document("v", 1)),
        new UpdateOptions().upsert(true));
    assertThat(first.getUpsertedId()).isNotNull();
    assertThat(first.getUpsertedId().asObjectId().getValue()).isEqualTo(id);

    final UpdateResult second = collection.updateOne(eq("_id", id), new Document("$set", new Document("v", 2)),
        new UpdateOptions().upsert(true));
    assertThat(second.getUpsertedId()).isNull();
    assertThat(second.getModifiedCount()).isEqualTo(1);

    final Document found = collection.find(eq("_id", id)).first();
    assertThat(found).isNotNull();
    assertThat(found.get("v")).isEqualTo(2);
  }

  /**
   * Follow-up to #6941 flagged by review: a client-supplied {@code String _id} that happens to be exactly 24 hex
   * characters is indistinguishable, once stored, from a real ObjectId's hex encoding. {@code executeUpsert} used
   * to promote every such stored hex string back to an ObjectId in the response regardless of what the client
   * actually sent, silently changing the wire type of an id the client chose as a plain string.
   */
  @Test
  void upsertFilteredOnHexLookingStringIdReportsItAsAStringNotAnObjectId() {
    final String hexLookingId = "abcdef0123456789abcdef01";

    final UpdateResult result = collection.updateOne(eq("_id", hexLookingId), new Document("$set", new Document("v", 1)),
        new UpdateOptions().upsert(true));

    assertThat(result.getUpsertedId()).isNotNull();
    assertThat(result.getUpsertedId().isString()).isTrue();
    assertThat(result.getUpsertedId().asString().getValue()).isEqualTo(hexLookingId);
  }

  /**
   * Follow-up to #6941 flagged by review: an explicit {@code _id: null} filter is a legal, if unusual, BSON _id.
   * {@code executeUpsert} must tell that apart from a genuinely absent {@code _id} and preserve it, rather than
   * discarding it for a freshly generated one the way the original #6941 bug discarded a seeded ObjectId.
   * <p>
   * This does not assert that a second {@code eq("_id", null)} upsert is idempotent: matching a stored {@code null}
   * by equality is a separate, pre-existing gap in the filter-to-SQL translation (an "=" comparison against a bound
   * null parameter, rather than "IS NULL") that affects every {@code null}-valued filter, not just this upsert path
   * - tracked separately rather than fixed here.
   */
  @Test
  void upsertFilteredOnNullIdPreservesTheNullId() {
    final UpdateResult result = collection.updateOne(eq("_id", null), new Document("$set", new Document("v", 1)),
        new UpdateOptions().upsert(true));

    assertThat(result.getUpsertedId()).isNotNull();
    assertThat(result.getUpsertedId().isNull()).isTrue();

    final Document inserted = collection.find(eq("v", 1)).first();
    assertThat(inserted).isNotNull();
    // containsKey, not just get() == null: a Map.get returns null for a missing key too, so this confirms the
    // wire response actually carries an "_id" field rather than having dropped it.
    assertThat(inserted.containsKey("_id")).isTrue();
    assertThat(inserted.get("_id")).isNull();
  }
}
