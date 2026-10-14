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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoServerException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9870: a real follower refusing a real MongoDB client. Issue #9845 made the refusal {@code 10107
 * NotWritablePrimary} naming the leader, tested on the mapping helper alone; this drives it through the driver, the
 * wrapper's command dispatch and the cluster's own refusal.
 * <p>
 * No injection is needed here: a MongoDB collection is an ArcadeDB type, and a command that creates one runs the schema
 * change on the node the client is connected to, which a follower refuses naming the leader. Each test is one of the
 * three commands that create a collection.
 */
@Tag("slow")
class Issue9870MongoFollowerLeaderRefusalIT extends BaseRaftHATest {

  // One Mongo port per server, drawn free from the same ledger as the Raft ports
  private final int[] mongoPorts = allocateFixturePorts(3);

  private MongoClient client;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final int index = serverIndexOf(config);
    config.setValue(GlobalConfiguration.SERVER_PLUGINS.getKey(), "MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
    config.setValue(GlobalConfiguration.MONGO_PORT.getKey(), String.valueOf(mongoPorts[index]));
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void closeClient() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  @Test
  @Timeout(120)
  void createCollectionOnAFollowerIsNotWritablePrimaryNamingTheLeader() {
    final int follower = firstFollowerOf(requireLeaderIndex());
    final Throwable refused = catchThrowable(() -> followerDatabase(follower).createCollection("issue9870create"));
    assertNotWritablePrimaryNamingTheLeader(refused, follower);
  }

  @Test
  @Timeout(120)
  void insertIntoANewCollectionOnAFollowerIsNotWritablePrimaryNamingTheLeader() {
    final int follower = firstFollowerOf(requireLeaderIndex());
    final Throwable refused = catchThrowable(
        () -> followerDatabase(follower).getCollection("issue9870insert").insertOne(new Document("name", "refused")));
    assertNotWritablePrimaryNamingTheLeader(refused, follower);
  }

  @Test
  @Timeout(120)
  void createIndexOnANewCollectionOnAFollowerIsNotWritablePrimaryNamingTheLeader() {
    final int follower = firstFollowerOf(requireLeaderIndex());
    final Throwable refused = catchThrowable(
        () -> followerDatabase(follower).getCollection("issue9870index").createIndex(new Document("name", 1)));
    assertNotWritablePrimaryNamingTheLeader(refused, follower);
  }

  private void assertNotWritablePrimaryNamingTheLeader(final Throwable refused, final int follower) {
    assertThat(refused).as("the follower must refuse the schema change").isInstanceOf(MongoServerException.class);
    assertThat(((MongoServerException) refused).getCode())
        .as("a driver rescans the topology on NotWritablePrimary, and retries WriteConflict in place (issue #9845): %s",
            refused.getMessage())
        .isEqualTo(10107);
    final String leaderAddress = getRaftPlugin(follower).getRaftHAServer().getLeaderHttpAddress();
    assertThat(leaderAddress).as("the follower must know where the leader is").isNotBlank();
    assertThat(refused.getMessage())
        .as("a client without topology discovery, or an operator, has only the text to find the leader in")
        .contains("not primary")
        .contains("The leader is server '" + leaderAddress + "'");
  }

  private MongoDatabase followerDatabase(final int follower) {
    client = new MongoClient(new ServerAddress("localhost", mongoPorts[follower]),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).retryWrites(false).build());
    return client.getDatabase(getDatabaseName());
  }
}
