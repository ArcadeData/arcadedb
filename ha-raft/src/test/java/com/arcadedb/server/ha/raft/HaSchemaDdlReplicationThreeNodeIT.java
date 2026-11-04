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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * DDL replication on ONE three-node cluster shared by every test ({@link SharedClusterRaftHATest}), sibling of
 * {@link HaSchemaDdlReplicationIT}. Merged from {@code Issue10349UniqueIndexBuildOnReplicatedDatabaseIT},
 * {@code Issue10506RefusedUniqueIndexBuildIT} and {@code Issue8404FunctionDdlReplicationIT}, which ran on the same
 * fixture: three servers, the default quorum, in-memory Raft storage and no configuration override.
 * <ul>
 * <li><b>#10349</b>: {@code CREATE INDEX ... UNIQUE} on a populated type stayed quadratic on a replicated database, where
 * the sorted build is refused. The record-at-a-time build of a large type now checks the uniqueness of its keys once,
 * over the finished index. What a replicated database must still give: the index on every node with every entry, and the
 * live index refusing duplicates afterwards.</li>
 * <li><b>#10506</b>: a {@code CREATE INDEX ... UNIQUE} over a large type holding a duplicate key is refused by the pass
 * over the finished index, after every batch of the build was committed. The refusal used to quarantine the leader's
 * database and keep a partially built sub-index the replicas never had until the resync. Now it costs the caller an
 * error and nothing else: no node is quarantined, no node has the index, and the same index is built once the duplicate
 * is gone. #10569 is the same refusal when the build REPLACES a plain index.</li>
 * <li><b>#8404</b>: {@code DEFINE FUNCTION} and {@code DELETE FUNCTION} changed only the node that executed them: both
 * persisted the schema with a bare {@code saveConfiguration()}, so the leader never proposed a {@code SCHEMA_ENTRY}. Each
 * statement is driven through both of its cluster entry points - executed on the leader, and sent to a follower, which
 * forwards it - and must land on every node. #8879 is {@code Schema.unregisterFunctionLibrary}.</li>
 * </ul>
 * Why these share a cluster: each index test builds its own type, named through {@link #uniqueName(String)}, and every
 * count it asserts ({@code RECORDS}, {@code RECORDS + 1}) is of that type's index, not of the database. Function
 * libraries are database-global, so each one is named through {@link #uniqueName(String)} too. The "no node is
 * quarantined" and "the leader keeps its leadership" assertions of #10506 hold on a cluster an earlier test left healthy,
 * which the shared fixture's health check guarantees (it reboots a cluster with a quarantined database). No test stops a
 * node or moves leadership. The 12,000 records each index test writes stay in the shared database, which costs the final
 * database comparison a few seconds and changes no assertion.
 * <p>
 * #8404's source class was tagged {@code slow}, so its tests are.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaSchemaDdlReplicationThreeNodeIT extends SharedClusterRaftHATest {
  // ABOVE arcadedb.index.buildSortedUniqueMinRecords (10,000), the size the build takes the deferred check from
  private static final int RECORDS = 12_000;

  @Override
  protected int getServerCount() {
    return 3;
  }

  // ------------------------------------------------------------------ unique index builds (#10349, #10506)

  /** Issue #10349, from {@code Issue10349UniqueIndexBuildOnReplicatedDatabaseIT}. */
  @Test
  void aLargeUniqueIndexIsOnEveryNodeWithEveryEntry() throws Exception {
    final String work = uniqueName("Work");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    populate(leader, work);
    assertClusterConsistency();

    leader.command("sql", "CREATE INDEX ON " + work + " (id) UNIQUE").close();
    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      final TypeIndex index = withResyncRetry(serverIndex, db -> db.getSchema().getType(work).getIndexByProperties("id"));
      assertThat(index).as("server %d", serverIndex).isNotNull();
      assertThat(index.isUnique()).as("server %d", serverIndex).isTrue();
      assertThat(index.countEntries()).as("server %d", serverIndex).isEqualTo(RECORDS);
      assertThat(index.get(new Object[] { key(RECORDS / 2) }).hasNext()).as("server %d", serverIndex).isTrue();
    }

    // The index is live: a duplicate is refused on the leader, a new key is accepted and reaches the replicas
    assertThatThrownBy(() -> leader.transaction(() -> leader.newDocument(work).set("id", key(7)).save()))
        .isInstanceOf(DuplicatedKeyException.class);
    leader.transaction(() -> leader.newDocument(work).set("id", "fresh").save());
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final long entries = withResyncRetry(i, db -> db.getSchema().getType(work).getIndexByProperties("id").countEntries());
      assertThat(entries).as("server %d", i).isEqualTo(RECORDS + 1);
    }
  }

  /** Issue #10506, from {@code Issue10506RefusedUniqueIndexBuildIT}. */
  @Test
  void aRefusedUniqueIndexBuildQuarantinesNoNodeAndLeavesNoIndex() throws Exception {
    final String work = uniqueName("Work");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    populate(leader, work);
    // In ANOTHER bucket than the record already holding the key: the build of each bucket commits, and only the pass over the
    // finished index meets the duplicate. In the same bucket the build of that bucket refuses it before it commits anything
    final String duplicatedKey = key(RECORDS / 2);
    final String twinBucket = otherBucketThanTheOneHolding(leader, work, duplicatedKey);
    leader.transaction(() -> leader.newDocument(work).set("id", duplicatedKey).save(twinBucket));
    assertClusterConsistency();

    assertThatThrownBy(() -> leader.command("sql", "CREATE INDEX ON " + work + " (id) UNIQUE").close())
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getRaftPlugin(i).getRaftHAServer().getStateMachine().quarantineCause(getDatabaseName()))
          .as("server %d is not quarantined by a refused CREATE INDEX", i).isNull();
    assertThat(findLeaderIndex()).as("the leader keeps its leadership").isEqualTo(leaderIndex);

    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      final Object index = withResyncRetry(serverIndex, db -> db.getSchema().getType(work).getIndexByProperties("id"));
      assertThat(index).as("server %d has no index from the refused build", serverIndex).isNull();
    }

    // The duplicate gone, the same statement builds the index on every node
    leader.transaction(() -> leader.command("sql", "DELETE FROM " + work + " WHERE id = ? LIMIT 1", duplicatedKey).close());
    leader.command("sql", "CREATE INDEX ON " + work + " (id) UNIQUE").close();
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final long entries = withResyncRetry(i, db -> db.getSchema().getType(work).getIndexByProperties("id").countEntries());
      assertThat(entries).as("server %d", i).isEqualTo(RECORDS);
    }
  }

  /**
   * Issue #10506, from {@code Issue10506RefusedUniqueIndexBuildIT}.
   * <p>
   * Issue #10569: the same refusal when the build REPLACES a plain index on the same property. The replaced index is built again
   * inside the schema session and the net state is published: no node is quarantined, and every node ends with the plain index
   * back, complete.
   */
  @Test
  void aRefusedReplacingIndexBuildQuarantinesNoNodeAndRestoresTheReplacedIndexEverywhere() throws Exception {
    final String work = uniqueName("Work");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    populate(leader, work);
    final String duplicatedKey = key(RECORDS / 2);
    final String twinBucket = otherBucketThanTheOneHolding(leader, work, duplicatedKey);
    leader.transaction(() -> leader.newDocument(work).set("id", duplicatedKey).save(twinBucket));
    leader.command("sql", "CREATE INDEX ON " + work + " (id) NOTUNIQUE").close();
    assertClusterConsistency();

    assertThatThrownBy(() -> leader.getSchema().buildTypeIndex(work, new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true).withReplaceIfIncompatible(true).create()).hasRootCauseInstanceOf(DuplicatedKeyException.class);

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getRaftPlugin(i).getRaftHAServer().getStateMachine().quarantineCause(getDatabaseName()))
          .as("server %d is not quarantined by a refused replacing CREATE INDEX", i).isNull();
    assertThat(findLeaderIndex()).as("the leader keeps its leadership").isEqualTo(leaderIndex);

    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      final boolean unique = withResyncRetry(serverIndex, db -> db.getSchema().getType(work).getIndexByProperties("id").isUnique());
      assertThat(unique).as("server %d has the replaced, non-unique index back", serverIndex).isFalse();
      final long entries = withResyncRetry(serverIndex,
          db -> db.getSchema().getType(work).getIndexByProperties("id").countEntries());
      assertThat(entries).as("server %d", serverIndex).isEqualTo(RECORDS + 1L);
    }
  }

  // ------------------------------------------------------------------ function DDL (#8404)

  /** Issue #8404, from {@code Issue8404FunctionDdlReplicationIT}. */
  @Test
  @Tag("slow")
  @Timeout(180)
  void defineAndDeleteFunctionReachEveryNode() {
    final String lib = uniqueName("lib8404");
    final String sqlLib = uniqueName("sql8404");
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);
    final Database onLeader = getServer(leader).getDatabaseForOperator(getDatabaseName());
    final Database onFollower = getServer(follower).getDatabaseForOperator(getDatabaseName());

    // DEFINE FUNCTION on the leader, creating the library: the case the issue reports.
    onLeader.command("sql", "DEFINE FUNCTION " + lib + ".twice \"return n * 2;\" PARAMETERS [n] LANGUAGE js");
    awaitFunctionOnEveryServer(lib, "twice", true);

    // The replicated definition is a working function on the follower, not only a schema entry.
    try (final ResultSet rs = onFollower.query("sql", "SELECT `" + lib + ".twice`(21) AS r")) {
      assertThat(((Number) rs.next().getProperty("r")).intValue()).isEqualTo(42);
    }

    // DEFINE FUNCTION into the library that now exists: the other branch of the statement.
    onLeader.command("sql", "DEFINE FUNCTION " + lib + ".thrice \"return n * 3;\" PARAMETERS [n] LANGUAGE js");
    awaitFunctionOnEveryServer(lib, "thrice", true);

    // DEFINE FUNCTION sent to a follower, in a declarative language: forwarded to the leader, replicated back.
    onFollower.command("sql", "DEFINE FUNCTION " + sqlLib + ".plusOne \"SELECT :n + 1 AS r\" PARAMETERS [n] LANGUAGE sql");
    awaitFunctionOnEveryServer(sqlLib, "plusOne", true);

    // DELETE FUNCTION on the leader.
    onLeader.command("sql", "DELETE FUNCTION " + lib + ".thrice");
    awaitFunctionOnEveryServer(lib, "thrice", false);

    // DELETE FUNCTION sent to a follower.
    onFollower.command("sql", "DELETE FUNCTION " + lib + ".twice");
    awaitFunctionOnEveryServer(lib, "twice", false);

    // Nothing else moved: the functions not deleted are still on every node.
    awaitFunctionOnEveryServer(sqlLib, "plusOne", true);
  }

  /**
   * Issue #8404, from {@code Issue8404FunctionDdlReplicationIT}.
   * <p>
   * Issue #8879: {@code Schema.unregisterFunctionLibrary} removed the library from the leader's memory only, so the
   * followers kept it until some later DDL happened to ship a schema file without it.
   */
  @Test
  @Tag("slow")
  @Timeout(180)
  void unregisterFunctionLibraryReachesEveryNode() {
    final String lib = uniqueName("lib8879");
    final String keep = uniqueName("keep8879");
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database onLeader = getServer(leader).getDatabaseForOperator(getDatabaseName());

    onLeader.command("sql", "DEFINE FUNCTION " + lib + ".f \"SELECT 1 AS r\" LANGUAGE sql");
    onLeader.command("sql", "DEFINE FUNCTION " + keep + ".f \"SELECT 2 AS r\" LANGUAGE sql");
    awaitFunctionOnEveryServer(lib, "f", true);
    awaitFunctionOnEveryServer(keep, "f", true);

    onLeader.getSchema().unregisterFunctionLibrary(lib);

    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServer(i).getDatabaseForOperator(getDatabaseName()).getSchema().hasFunctionLibrary(lib))
            .as("library %s on server %d", lib, i).isFalse();
    });
    awaitFunctionOnEveryServer(keep, "f", true);
  }

  // ------------------------------------------------------------------ helpers

  /** #10349, #10506: a type of {@link #RECORDS} documents with a STRING {@code id}, over three buckets. */
  private static void populate(final Database database, final String typeName) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(3).create();
    type.createProperty("id", Type.STRING);
    for (int from = 0; from < RECORDS; from += 2_000) {
      final int first = from;
      database.transaction(() -> {
        for (int i = first; i < Math.min(RECORDS, first + 2_000); i++)
          database.newDocument(typeName).set("id", key(i)).save();
      });
    }
  }

  /** #10506. */
  private static String otherBucketThanTheOneHolding(final Database database, final String typeName, final String key) {
    final int holder = database.query("sql", "SELECT FROM " + typeName + " WHERE id = ?", key).nextIfAvailable().getIdentity().get()
        .getBucketId();
    for (final Bucket bucket : database.getSchema().getType(typeName).getBuckets(false))
      if (bucket.getFileId() != holder)
        return bucket.getName();
    throw new IllegalStateException("type " + typeName + " has a single bucket");
  }

  /** #10349, #10506: ids in a scrambled order: the keys never arrive sorted. */
  private static String key(final int i) {
    return "https://openalex.org/W" + (1_000_000_000L + ((i * 2_654_435_761L) & 0xFFFFFFFFL));
  }

  /** #8404. */
  private void awaitFunctionOnEveryServer(final String library, final String function, final boolean present) {
    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++) {
        final Schema schema = getServer(i).getDatabaseForOperator(getDatabaseName()).getSchema();
        final boolean defined = schema.hasFunctionLibrary(library) && schema.getFunctionLibrary(library).hasFunction(function);
        assertThat(defined).as("function %s.%s on server %d", library, function, i).isEqualTo(present);
      }
    });
  }

  /** #8404. */
  private int firstFollower(final int leader) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }
}
