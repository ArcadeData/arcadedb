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
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Documents a known limitation flagged by issue #6955, deliberately left as a documented limitation rather than
 * fixed (see the maintainer's decision on the issue): a client-supplied {@code String _id} that happens to be
 * exactly 24 hex characters is, once stored, indistinguishable from a real {@code ObjectId}'s hex encoding - both
 * end up as the same bare hex string on disk. A plain {@code insertOne} followed by a separate {@code find} call
 * therefore round-trips such a {@code String _id} back as an {@code ObjectId} rather than the original {@code
 * String}.
 * <p>
 * This is different from - and not fixed by - issue #6941/#6951's upsert fix: {@code executeUpsert} can track the
 * original BSON type in memory because the filter and the seeded record are handled within the same method call.
 * {@code insertOne} and the later {@code find} are separate wire calls, potentially on separate connections, so
 * there is no equivalent in-memory flag to bridge them. Fixing this for real requires a storage-format decision
 * (persisting the original BSON type alongside {@code _id}), not a narrow code change - see
 * {@link MongoDBToSqlTranslator#convertIdToMongoDB} for the full rationale.
 * <p>
 * This test pins today's actual (documented-limitation) behavior so a future storage-format change updates it
 * deliberately instead of it silently starting to pass or fail.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBHexStringIdAmbiguityTest extends BaseGraphServerTest {

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
  void hexLookingStringIdInsertedPlainRoundTripsAsAnObjectIdOnFind() {
    final String hexLookingId = "abcdef0123456789abcdef01";
    collection.insertOne(new Document("_id", hexLookingId).append("name", "x"));

    final Document found = collection.find().first();

    assertThat(found).isNotNull();
    // Documented limitation (#6955): the client sent a String, but it comes back as an ObjectId because the stored
    // hex form is indistinguishable from a real ObjectId's. Contrast with MongoDBNonObjectIdTest, which covers an
    // odd-length or non-hex-looking String _id: those are not ambiguous and correctly round-trip as a String.
    assertThat(found.get("_id")).isInstanceOf(org.bson.types.ObjectId.class);
    assertThat(found.get("_id").toString()).isEqualTo(hexLookingId);
  }
}
