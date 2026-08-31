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
   * The upsert is now repeated with the same {@code eq("_id", null)} filter to confirm it is idempotent: before
   * #6952 was fixed, matching a stored {@code null} by equality compiled to {@code "_id" = :p0} with {@code p0}
   * bound to {@code null}, which never matches a stored null the way SQL's {@code IS NULL} does, so this second
   * call would have inserted a duplicate instead of updating in place.
   */
  @Test
  void upsertFilteredOnNullIdPreservesTheNullId() {
    final UpdateResult first = collection.updateOne(eq("_id", null), new Document("$set", new Document("v", 1)),
        new UpdateOptions().upsert(true));

    assertThat(first.getUpsertedId()).isNotNull();
    assertThat(first.getUpsertedId().isNull()).isTrue();

    final Document inserted = collection.find(eq("v", 1)).first();
    assertThat(inserted).isNotNull();
    // containsKey, not just get() == null: a Map.get returns null for a missing key too, so this confirms the
    // wire response actually carries an "_id" field rather than having dropped it.
    assertThat(inserted.containsKey("_id")).isTrue();
    assertThat(inserted.get("_id")).isNull();

    final UpdateResult second = collection.updateOne(eq("_id", null), new Document("$set", new Document("v", 2)),
        new UpdateOptions().upsert(true));
    assertThat(second.getUpsertedId()).isNull();
    assertThat(second.getModifiedCount()).isEqualTo(1);
    assertThat(collection.countDocuments(eq("_id", null))).isEqualTo(1);
    assertThat(collection.find(eq("_id", null)).first().get("v")).isEqualTo(2);
  }

  /**
   * Regression test for issue #6952: a filter on a null-valued field must match a document that actually stores
   * that field as {@code null}, the same way MongoDB's null-equality does. Before the fix, {@code {field: null}}
   * compiled to {@code "field" = :param} with {@code param} bound to {@code null}, which never matches.
   * <p>
   * A dedicated collection is used because MongoDB's null-equality also matches a document where the field is
   * simply absent, which is exactly the semantics being fixed here - the shared {@code collection} already holds
   * ten unrelated documents with no {@code tag} field at all, and those would match too.
   */
  @Test
  void findByNullValuedFieldMatchesTheStoredNull() {
    client.getDatabase(getDatabaseName()).createCollection("nullFilterDoc");
    final MongoCollection<Document> nullDocs = client.getDatabase(getDatabaseName()).getCollection("nullFilterDoc");
    nullDocs.insertOne(new Document("test", "null-field").append("tag", null));

    final Document found = nullDocs.find(eq("tag", null)).first();

    assertThat(found).isNotNull();
    assertThat(found.getString("test")).isEqualTo("null-field");
  }

  /**
   * Follow-up to #6952, flagged by review: MongoDB's {@code {field: null}} matches both a stored {@code null} and a
   * document where the field is simply absent - {@code findByNullValuedFieldMatchesTheStoredNull} only pins the
   * first half of that claim (routing around the second half by using a dedicated collection). This closes the loop
   * by asserting the second half directly: a document that never set {@code tag} at all is still matched.
   */
  @Test
  void findByNullValuedFieldAlsoMatchesADocumentWhereTheFieldIsAbsent() {
    client.getDatabase(getDatabaseName()).createCollection("nullFilterAbsent");
    final MongoCollection<Document> nullDocs = client.getDatabase(getDatabaseName()).getCollection("nullFilterAbsent");
    nullDocs.insertOne(new Document("test", "absent-field"));

    final Document found = nullDocs.find(eq("tag", null)).first();

    assertThat(found).isNotNull();
    assertThat(found.getString("test")).isEqualTo("absent-field");
  }

  /**
   * Follow-up to #6952: {@code $ne} shares the same root cause as plain equality and {@code $eq}, so a filter of
   * {@code {field: {$ne: null}}} must exclude a document whose field is stored as {@code null}.
   * <p>
   * Unlike plain {@code {field: null}} equality, {@code {field: {$ne: null}}} is not its exact negation: per
   * MongoDB's own documentation it matches only a field that exists and is not null, excluding a document where the
   * field is missing too (rather than including it). A document with no {@code tag} field at all is included here
   * to pin that half of the contract as well.
   */
  @Test
  void neNullExcludesTheStoredNullAndAnAbsentFieldButMatchesEverythingElse() {
    collection.insertOne(new Document("test", "null-field").append("tag", null));
    collection.insertOne(new Document("test", "absent-field"));
    collection.insertOne(new Document("test", "set-field").append("tag", "x"));

    final Document found = collection.find(new Document("tag", new Document("$ne", null))).first();

    assertThat(found).isNotNull();
    assertThat(found.getString("test")).isEqualTo("set-field");
  }

  /**
   * Regression test for issue #6952: {@code deleteMany} on a null-valued filter must remove the document whose
   * field is actually stored as {@code null}, not silently delete nothing.
   * <p>
   * A dedicated collection is used for the same reason as {@link #findByNullValuedFieldMatchesTheStoredNull}: the
   * null filter also matches a document where the field is absent, so the shared {@code collection}'s ten unrelated
   * documents (no {@code tag} field) would be swept up by {@code deleteMany} too. This is exercised directly by
   * inserting an absent-field document alongside the explicit-null one and asserting {@code deleteMany} removes
   * both, closing the same missing-field half of the contract for the delete path.
   */
  @Test
  void deleteManyByNullValuedFieldRemovesBothTheStoredNullAndAnAbsentField() {
    client.getDatabase(getDatabaseName()).createCollection("nullFilterDelete");
    final MongoCollection<Document> nullDocs = client.getDatabase(getDatabaseName()).getCollection("nullFilterDelete");
    nullDocs.insertOne(new Document("test", "null-field").append("tag", null));
    nullDocs.insertOne(new Document("test", "absent-field"));

    final DeleteResult result = nullDocs.deleteMany(new Document("tag", null));

    assertThat(result.getDeletedCount()).isEqualTo(2);
    assertThat(nullDocs.find(eq("test", "null-field")).first()).isNull();
    assertThat(nullDocs.find(eq("test", "absent-field")).first()).isNull();
  }

  /**
   * Regression test for issue #6953: {@code executeUpsert}'s replacement branch (a full {@code replaceOne} upsert)
   * used to copy the update document's values verbatim, so a client-supplied {@code _id} that is an actual
   * {@code ObjectId} was stored as that raw driver object rather than the hex-string convention every other
   * {@code _id} write path in this class follows. That would make the stored {@code _id} fail to match a later
   * {@code eq("_id", ...)} filter, which binds the ObjectId as its hex string.
   */
  @Test
  void replaceOneUpsertNormalizesAReplacementObjectIdIdToItsHexString() {
    final ObjectId id = new ObjectId();

    final UpdateResult result = collection.replaceOne(eq("test", "no-such-value"),
        new Document("_id", id).append("replaced", true), new UpdateOptions().upsert(true));

    assertThat(result.getUpsertedId()).isNotNull();
    assertThat(result.getUpsertedId().isObjectId()).isTrue();
    assertThat(result.getUpsertedId().asObjectId().getValue()).isEqualTo(id);

    final Document found = collection.find(eq("_id", id)).first();
    assertThat(found).isNotNull();
    assertThat(found.get("replaced")).isEqualTo(true);
  }

  /**
   * Regression test for issue #6953: the same gap as above, reached through the {@code $set} branch of an upsert
   * (rather than a full replacement) - {@code applyOperatorsToDocument} stored a {@code $set}-supplied {@code _id}
   * verbatim too.
   */
  @Test
  void setUpsertNormalizesAnObjectIdIdToItsHexString() {
    final ObjectId id = new ObjectId();

    final UpdateResult result = collection.updateOne(eq("test", "no-such-value-2"),
        new Document("$set", new Document("_id", id).append("v", 42)), new UpdateOptions().upsert(true));

    assertThat(result.getUpsertedId()).isNotNull();
    assertThat(result.getUpsertedId().isObjectId()).isTrue();
    assertThat(result.getUpsertedId().asObjectId().getValue()).isEqualTo(id);

    final Document found = collection.find(eq("_id", id)).first();
    assertThat(found).isNotNull();
    assertThat(found.get("v")).isEqualTo(42);
  }

  /**
   * Follow-up to #6953, flagged by review: the replacement branch's fix normalizes every field's value the same way
   * the pre-existing filter-seeding loop already does, not just {@code _id} - ArcadeDB has no native ObjectId type,
   * so an ObjectId-valued non-{@code _id} field would otherwise be stored as the raw driver object too, and later
   * fail to match a filter on that field the same way an un-normalized {@code _id} did.
   */
  @Test
  void replaceOneUpsertNormalizesANonIdObjectIdFieldToo() {
    final ObjectId ref = new ObjectId();

    final UpdateResult result = collection.replaceOne(eq("test", "no-such-value-3"),
        new Document("ref", ref).append("replaced", true), new UpdateOptions().upsert(true));

    assertThat(result.getUpsertedId()).isNotNull();

    final Document found = collection.find(eq("ref", ref)).first();
    assertThat(found).isNotNull();
    assertThat(found.get("replaced")).isEqualTo(true);
  }
}
