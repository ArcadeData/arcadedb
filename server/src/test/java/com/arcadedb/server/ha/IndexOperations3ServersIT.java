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
package com.arcadedb.server.ha;

import com.arcadedb.database.Database;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.TestServerHelper;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.assertThat;

class IndexOperations3ServersIT extends BaseGraphServerTest {

  private static final int TOTAL_RECORDS = 1_000;
  private static final int TX_CHUNK      = 500;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void rebuildIndex() throws Exception {
    final Database database = getServerDatabase(getLeaderIndex(), getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Person BUCKETS 3");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.command("sql", "CREATE PROPERTY Person.uuid STRING");
    database.command("sql", "CREATE INDEX ON Person (uuid) UNIQUE");

    LogManager.instance().log(this, Level.FINE, "Inserting 1M records with 2 indexes...");
    // CREATE 1M RECORD IN 10 TX CHUNKS OF 100K EACH
    database.transaction(() -> insertRecords(database));

    testEachServer((serverIndex) -> {
      LogManager.instance()
          .log(this, Level.FINE, "Rebuild index Person[id] on server %s...", getServer(serverIndex).getHA().getServerName());
      final String response1 = command(serverIndex, "rebuild index `Person[id]`");
      assertThat(new JSONObject(response1).getJSONArray("result").getJSONObject(0).getLong("totalIndexed")).isEqualTo(TOTAL_RECORDS);

      LogManager.instance()
          .log(this, Level.FINE, "Rebuild index Person[uuid] on server %s...", getServer(serverIndex).getHA().getServerName());
      final String response2 = command(serverIndex, "rebuild index `Person[uuid]`");
      assertThat(new JSONObject(response2).getJSONArray("result").getJSONObject(0).getLong("totalIndexed")).isEqualTo(TOTAL_RECORDS);

      LogManager.instance()
          .log(this, Level.FINE, "Rebuild index * on server %s...", getServer(serverIndex).getHA().getServerName());
      final String response3 = command(serverIndex, "rebuild index *");
      assertThat(new JSONObject(response3).getJSONArray("result").getJSONObject(0).getLong("totalIndexed")).isEqualTo(TOTAL_RECORDS * 2);
    });
  }

  @Test
  void createIndexLater() throws Exception {
    final Database database = getServerDatabase(getLeaderIndex(), getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Person BUCKETS 3");

    LogManager.instance().log(this, Level.FINE, "Inserting %d records without indexes first...", TOTAL_RECORDS);
    database.transaction(() -> insertRecords(database));

    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.command("sql", "CREATE PROPERTY Person.uuid STRING");
    database.command("sql", "CREATE INDEX ON Person (uuid) UNIQUE");

    testEachServer((serverIndex) -> {
      LogManager.instance()
          .log(this, Level.FINE, "Rebuild index Person[id] on server %s...", getServer(serverIndex).getHA().getServerName());
      final String response1 = command(serverIndex, "rebuild index `Person[id]`");
      assertThat(new JSONObject(response1).getJSONArray("result").getJSONObject(0).getLong("totalIndexed")).isEqualTo(TOTAL_RECORDS);

      LogManager.instance()
          .log(this, Level.FINE, "Rebuild index Person[uuid] on server %s...", getServer(serverIndex).getHA().getServerName());
      final String response2 = command(serverIndex, "rebuild index `Person[uuid]`");
      assertThat(new JSONObject(response2).getJSONArray("result").getJSONObject(0).getLong("totalIndexed")).isEqualTo(TOTAL_RECORDS);

      LogManager.instance()
          .log(this, Level.FINE, "Rebuild index * on server %s...", getServer(serverIndex).getHA().getServerName());
      final String response3 = command(serverIndex, "rebuild index *");
      assertThat(new JSONObject(response3).getJSONArray("result").getJSONObject(0).getLong("totalIndexed")).isEqualTo(TOTAL_RECORDS * 2);
    });
  }

  @Test
  void createIndexLaterDistributed() throws Exception {
    final Database database = getServerDatabase(getLeaderIndex(), getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Person BUCKETS 3");

    // Run on leader only - schema changes via direct API only work on leader with Ratis
    LogManager.instance().log(this, Level.FINE, "Inserting %d records without indexes first...", TOTAL_RECORDS);
    database.transaction(() -> insertRecords(database));

    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.command("sql", "CREATE PROPERTY Person.uuid STRING");
    database.command("sql", "CREATE INDEX ON Person (uuid) UNIQUE");

    // TRY CREATING A DUPLICATE
    TestServerHelper.expectException(() -> database.newVertex("Person").set("id", 0, "uuid", UUID.randomUUID().toString()).save(),
        DuplicatedKeyException.class);

    // TRY DROPPING A PROPERTY WITH AN INDEX
    TestServerHelper.expectException(() -> database.getSchema().getType("Person").dropProperty("id"), SchemaException.class);

    database.command("sql", "DROP INDEX `Person[id]`");
    database.command("sql", "DROP PROPERTY Person.id");

    TestServerHelper.expectException(() -> database.getSchema().getType("Person").dropProperty("uuid"), SchemaException.class);

    database.command("sql", "DROP INDEX `Person[uuid]`");
    database.command("sql", "DROP PROPERTY Person.uuid");

    database.command("sql", "DELETE FROM Person");
  }

  @Test
  void createIndexErrorDistributed() throws Exception {
    final Database database = getServerDatabase(getLeaderIndex(), getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Person BUCKETS 3");

    // Run on leader only
    LogManager.instance().log(this, Level.FINE, "Inserting records with duplicated IDs...");
    database.transaction(() -> {
      insertRecords(database);
      insertRecords(database);
    });

    database.command("sql", "CREATE PROPERTY Person.id LONG");

    // TRY CREATING INDEX WITH DUPLICATES (should fail)
    TestServerHelper.expectException(() -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id"),
        IndexException.class);

    // Clean up any partial index files left by the failed creation on the leader.
    // The Java API createTypeIndex may create physical files before discovering duplicate keys.
    try {
      database.command("sql", "DROP INDEX `Person[id]`");
    } catch (final Exception ignored) {
    }

    TestServerHelper.expectException(() -> database.getSchema().getIndexByName("Person[id]"), SchemaException.class);

    database.command("sql", "CREATE PROPERTY Person.uuid STRING");
    database.command("sql", "CREATE INDEX ON Person (uuid) UNIQUE");

    database.command("sql", "DROP PROPERTY Person.id");
    database.command("sql", "DROP INDEX `Person[uuid]`");
    database.command("sql", "DROP PROPERTY Person.uuid");
    database.command("sql", "DELETE FROM Person");
  }

  private void insertRecords(final Database database) {
    for (int i = 0; i < TOTAL_RECORDS; i++) {
      database.newVertex("Person").set("id", i, "uuid", UUID.randomUUID().toString()).save();

      if (i % TX_CHUNK == 0) {
        database.commit();
        database.begin();
      }
    }
  }
}
