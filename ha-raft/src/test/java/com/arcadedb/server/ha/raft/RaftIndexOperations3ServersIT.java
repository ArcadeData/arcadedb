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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7873: {@link #rebuildIndex()} and {@link #createIndexLater()} were {@code @Disabled} on an engine claim
 * recorded only in the annotation string - "rebuild index triggers compaction which is not replicated via Raft -
 * checkDatabasesAreIdentical fails in endTest" - with no issue behind it and no re-check since.
 * <p>
 * Both halves of that claim are false, and the second one measurably so.
 * <p>
 * Compaction output IS replicated: {@link Issue5517BloomFilterFullCompactionIT}, a 3-node test in this very
 * package, asserts end to end that a full compaction ships the new file, that the followers drop the retired one,
 * and that they then serve lookups out of replicated bloom filters they never built.
 * <p>
 * And {@code checkDatabasesAreIdentical()} does not fail here. The obvious fix was to suppress the shared
 * comparator the way {@code Issue5517BloomFilterFullCompactionIT} has to - a compacted file name embeds a per-node
 * {@code nanoTime}, so the comparator cannot pair bucket indexes by name afterwards - but running these two with
 * the comparator left ON shows it is not needed: all four tests in this class pass with it. That test forces a
 * FULL compaction by setting {@code INDEX_COMPACTION_FULL_SERIES}; the implicit compaction a {@code REBUILD INDEX}
 * of {@value #TOTAL_RECORDS} records triggers does not produce a renamed compacted file, so there is nothing for
 * the comparator to mispair. Suppressing it would have cost the cross-server check on all four tests to work
 * around a problem these two do not have (review on PR #7953).
 * <p>
 * So the comparator stays, and the two re-enabled tests additionally assert what it cannot: that every key is
 * findable THROUGH the rebuilt index on each of the three nodes, rather than only that the pages match.
 */
class RaftIndexOperations3ServersIT extends BaseRaftHATest {

  private static final int TOTAL_RECORDS = 500;
  private static final int TX_CHUNK      = 100;
  private static final String TYPE_NAME  = "RaftPerson";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  /**
   * Rebuilds both indexes, on all 3 servers, with the type and the indexes created BEFORE the data.
   */
  @Test
  void rebuildIndex() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());
    final VertexType v = database.getSchema().buildVertexType().withName("RaftPerson").withTotalBuckets(3).create();
    v.createProperty("id", Long.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "id");
    v.createProperty("uuid", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "uuid");

    final List<Long> insertedIds = new ArrayList<>(TOTAL_RECORDS);
    final List<String> insertedUuids = new ArrayList<>(TOTAL_RECORDS);
    database.transaction(() -> insertRecords(database, insertedIds, insertedUuids));

    testEachServer(serverIndex -> {
      LogManager.instance().log(this, Level.FINE, "Rebuild RaftPerson[id] on server %s",
          getServer(serverIndex).getServerName());
      final String response1 = command(serverIndex, "rebuild index `RaftPerson[id]`");
      assertThat(new JSONObject(response1).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo(TOTAL_RECORDS);

      LogManager.instance().log(this, Level.FINE, "Rebuild RaftPerson[uuid] on server %s",
          getServer(serverIndex).getServerName());
      final String response2 = command(serverIndex, "rebuild index `RaftPerson[uuid]`");
      assertThat(new JSONObject(response2).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo(TOTAL_RECORDS);

      LogManager.instance().log(this, Level.FINE, "Rebuild * on server %s",
          getServer(serverIndex).getServerName());
      final String response3 = command(serverIndex, "rebuild index *");
      assertThat(new JSONObject(response3).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo((long) TOTAL_RECORDS * 2);
    });

    assertEveryServerServesTheWholeIndex(insertedIds, insertedUuids);
  }

  /**
   * The same rebuilds with the indexes created AFTER the data, which is the path that has to index the records
   * already on disk rather than only the ones a later insert adds.
   */
  @Test
  void createIndexLater() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());
    final VertexType v = database.getSchema().buildVertexType().withName("RaftPerson").withTotalBuckets(3).create();

    final List<Long> insertedIds = new ArrayList<>(TOTAL_RECORDS);
    final List<String> insertedUuids = new ArrayList<>(TOTAL_RECORDS);
    database.transaction(() -> insertRecords(database, insertedIds, insertedUuids));

    v.createProperty("id", Long.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "id");
    v.createProperty("uuid", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "uuid");

    testEachServer(serverIndex -> {
      final String response1 = command(serverIndex, "rebuild index `RaftPerson[id]`");
      assertThat(new JSONObject(response1).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo(TOTAL_RECORDS);

      final String response2 = command(serverIndex, "rebuild index `RaftPerson[uuid]`");
      assertThat(new JSONObject(response2).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo(TOTAL_RECORDS);

      final String response3 = command(serverIndex, "rebuild index *");
      assertThat(new JSONObject(response3).getJSONArray("result").getJSONObject(0).getLong("totalIndexed"))
          .isEqualTo((long) TOTAL_RECORDS * 2);
    });

    assertEveryServerServesTheWholeIndex(insertedIds, insertedUuids);
  }

  /**
   * What must hold after a rebuild, on every one of the three servers, and what the {@code @Disabled} pair of
   * issue #7873 never asserted: they checked only the {@code totalIndexed} figure each server's own REBUILD
   * command reported, which is that server talking about the work it just did.
   * <p>
   * Additional to {@code checkDatabasesAreIdentical()}, not a replacement for it - see this class's javadoc for
   * why the comparator can stay. It answers a question the comparator does not ask: the comparator establishes
   * that the pages match, and this establishes that the rebuilt index can actually FIND the keys on every node.
   * <p>
   * The lookups go THROUGH the index rather than through a query the planner could answer with a type scan, which
   * is the difference between "the records are here" and "the index that was rebuilt can find them".
   */
  private void assertEveryServerServesTheWholeIndex(final List<Long> ids, final List<String> uuids) throws Exception {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> {
      final Database database = getServerDatabase(serverIndex, getDatabaseName());

      assertThat(database.countType(TYPE_NAME, false))
          .as("every record must be on server %d after the rebuild", serverIndex).isEqualTo(TOTAL_RECORDS);

      for (final String indexName : new String[] { TYPE_NAME + "[id]", TYPE_NAME + "[uuid]" })
        assertThat(database.getSchema().getIndexByName(indexName).countEntries())
            .as("index %s must hold the whole type on server %d", indexName, serverIndex).isEqualTo(TOTAL_RECORDS);

      final Index byId = database.getSchema().getIndexByName(TYPE_NAME + "[id]");
      for (final Long id : ids)
        assertThat(byId.get(new Object[] { id }).hasNext())
            .as("id %d must be findable through the index on server %d", id, serverIndex).isTrue();

      final Index byUuid = database.getSchema().getIndexByName(TYPE_NAME + "[uuid]");
      for (final String uuid : uuids)
        assertThat(byUuid.get(new Object[] { uuid }).hasNext())
            .as("uuid %s must be findable through the index on server %d", uuid, serverIndex).isTrue();
    });
  }

  /**
   * Tests creating an index later in a distributed fashion, with drop and re-create cycles.
   */
  @Test
  void createIndexLaterDistributed() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());
    final VertexType v = database.getSchema().buildVertexType().withName("RaftPerson").withTotalBuckets(3).create();

    testEachServer(serverIndex -> {
      database.transaction(() -> insertRecords(database));

      v.createProperty("id", Long.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "id");
      v.createProperty("uuid", String.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "uuid");

      TestServerHelper.expectException(
          () -> database.newVertex("RaftPerson").set("id", 0, "uuid", UUID.randomUUID().toString()).save(),
          DuplicatedKeyException.class);

      TestServerHelper.expectException(
          () -> database.getSchema().getType("RaftPerson").dropProperty("id"),
          SchemaException.class);

      database.getSchema().dropIndex("RaftPerson[id]");
      database.getSchema().getType("RaftPerson").dropProperty("id");

      TestServerHelper.expectException(
          () -> database.getSchema().getType("RaftPerson").dropProperty("uuid"),
          SchemaException.class);

      database.getSchema().dropIndex("RaftPerson[uuid]");
      database.getSchema().getType("RaftPerson").dropProperty("uuid");
      database.command("sql", "delete from RaftPerson");
    });
  }

  /**
   * Tests that creating a unique index with duplicate data raises an error on all servers.
   */
  @Test
  void createIndexErrorDistributed() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());
    final VertexType v = database.getSchema().buildVertexType().withName("RaftPerson").withTotalBuckets(3).create();

    testEachServer(serverIndex -> {
      database.transaction(() -> {
        insertRecords(database);
        insertRecords(database);
      });

      v.createProperty("id", Long.class);

      TestServerHelper.expectException(
          () -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "id"),
          IndexException.class);

      TestServerHelper.expectException(
          () -> database.getSchema().getIndexByName("RaftPerson[id]"),
          SchemaException.class);

      v.createProperty("uuid", String.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "uuid");

      database.getSchema().getType("RaftPerson").dropProperty("id");
      database.getSchema().dropIndex("RaftPerson[uuid]");
      database.getSchema().getType("RaftPerson").dropProperty("uuid");
      database.command("sql", "delete from RaftPerson");
    });
  }

  private void insertRecords(final Database database) {
    insertRecords(database, null, null);
  }

  /**
   * Inserts the records and, when given the two collectors, records the keys it actually wrote. The keys are
   * collected rather than recomputed because one of the two is a random UUID: asserting against a regenerated one
   * would assert nothing (issue #7873).
   */
  private void insertRecords(final Database database, final List<Long> ids, final List<String> uuids) {
    for (int i = 0; i < TOTAL_RECORDS; i++) {
      final String uuid = UUID.randomUUID().toString();
      database.newVertex(TYPE_NAME).set("id", i, "uuid", uuid).save();
      if (ids != null) {
        ids.add((long) i);
        uuids.add(uuid);
      }
      if (i % TX_CHUNK == 0) {
        database.commit();
        database.begin();
      }
    }
  }
}
