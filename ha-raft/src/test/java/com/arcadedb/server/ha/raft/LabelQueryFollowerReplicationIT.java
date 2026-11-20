/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.misc.SQLFunctionEnsureLabelTypes;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * Composite labels, multi-label creates and polymorphic counts must give the same answer on every node of a 3-node
 * cluster.
 */
class LabelQueryFollowerReplicationIT extends BaseRaftHATest {
  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Issue #10093: a composite openCypher label type over an indexed type ({@code CREATE (m:V1:NewLabel {id: ...})}, where {@code V1} has an
   * index on {@code id}) was reported "configured differently" between the leader and its followers by the database comparison that ends
   * every test of the fixture, whoever created it. The two sides printed the same JSON, so the difference was in something the JSON does not
   * carry: the sub-indexes the super type's index grew for the new buckets. The comparison is NOT overridden here, so the fixture's
   * end-of-test check is what fails if the type or its indexes differ again, and the assertions below name what must hold.
   */
  // Regression for #10093: a composite label over an indexed type is the same type on every node
  @Test
  @Timeout(180)
  void aCompositeLabelOverAnIndexedTypeIsTheSameTypeOnEveryNode() {
    final int leader = requireLeaderIndex();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());

    leaderDb.begin();
    leaderDb.command("opencypher", "CREATE (m:V1:Issue10093Label {id: 1004001})").close();
    leaderDb.commit();

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
        final Database db = getServerDatabase(server, getDatabaseName());
        assertThat(db.getSchema().existsType("Issue10093Label~V1")).as("server %d has the composite type", server).isTrue();
        try (final ResultSet rs = db.query("sql", "SELECT FROM V1 WHERE id = ?", 1004001L)) {
          assertThat(rs.stream().count()).as("server %d finds the record through the index of V1", server).isEqualTo(1L);
        }
      });
    }

    // The index of the super type covers the buckets of the composite type, and does so with the same sub-indexes on every node
    final List<String> onLeader = subIndexesOfV1(getServerDatabase(leader, getDatabaseName()));
    assertThat(onLeader).as("the composite type's buckets are covered by the index of V1").isNotEmpty();
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      assertThat(subIndexesOfV1(db)).as("server %d against the leader", i).containsExactlyInAnyOrderElementsOf(onLeader);

      // Each sub-index is registered with the type that owns its bucket - the composite type - on every node, the leader that
      // created it and the followers that loaded the schema alike. The comparison that ends the test checks the same thing.
      final DocumentType composite = db.getSchema().getType("Issue10093Label~V1");
      final DocumentType v1 = db.getSchema().getType("V1");
      for (final Bucket bucket : composite.getBuckets(false)) {
        assertThat(composite.getPolymorphicBucketIndexByBucketId(bucket.getFileId(), null))
            .as("server %d: the composite type registers the sub-index of its bucket %s", i, bucket.getName()).hasSize(1);
        assertThat(v1.getPolymorphicBucketIndexByBucketId(bucket.getFileId(), null))
            .as("server %d: V1 does not register the sub-index of a bucket it does not own (%s)", i, bucket.getName()).isEmpty();
      }
    }
  }

  /** Every sub-index of the indexes on V1: its name, its bucket and the properties it covers. */
  private static List<String> subIndexesOfV1(final Database db) {
    final DocumentType v1 = db.getSchema().getType("V1");
    final List<String> result = new ArrayList<>();
    for (final TypeIndex typeIndex : v1.getAllIndexes(false))
      for (final Index sub : typeIndex.getIndexesOnBuckets())
        result.add(sub.getName() + "|" + sub.getAssociatedBucketId() + "|" + sub.getPropertyNames());
    return result;
  }

  /**
   * Issue #11064, the two residues of #11063: a multi-label {@code CREATE} in a client transaction on a follower.
   * <ol>
   * <li>A composite that exists but is incomplete (a label type missing, or not linked) used to skip the leader hand-off, so the
   * follower opened a local schema frame and was refused.</li>
   * <li>A leader that predates the {@code ensureLabelTypes} function answers a SQL error, not the retryable refusal: the follower now
   * falls back to the refusal it gave before the function existed.</li>
   * </ol>
   */
  // Regression for #11064: a composite created by hand without inheritance is completed by the leader
  @Test
  @Timeout(180)
  void aCompositeCreatedByHandWithoutInheritanceIsCompletedByTheLeader() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    leaderDb.command("sql", "CREATE VERTEX TYPE Issue11064A").close();
    leaderDb.command("sql", "CREATE VERTEX TYPE Issue11064B").close();
    leaderDb.command("sql", "CREATE VERTEX TYPE `Issue11064A~Issue11064B`").close();
    awaitType(followerDb, "Issue11064A~Issue11064B");
    assertThat(followerDb.getSchema().getType("Issue11064A~Issue11064B").instanceOf("Issue11064A")).isFalse();

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher", "CREATE (n:Issue11064A:Issue11064B {name: 'x'})").close();
      followerDb.commit();
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
        final Schema schema = getServerDatabase(server, getDatabaseName()).getSchema();
        assertThat(schema.getType("Issue11064A~Issue11064B").instanceOf("Issue11064A")).as("server %d", server).isTrue();
        assertThat(schema.getType("Issue11064A~Issue11064B").instanceOf("Issue11064B")).as("server %d", server).isTrue();
        assertThat(countOn(getServerDatabase(server, getDatabaseName()), "Issue11064A")).as("server %d", server).isEqualTo(1L);
      });
    }
  }

  // Regression for #11064: a composite whose label type is missing is completed by the leader
  @Test
  @Timeout(180)
  void aCompositeWhoseLabelTypeIsMissingIsCompletedByTheLeader() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    leaderDb.command("sql", "CREATE VERTEX TYPE `Issue11064C~Issue11064D`").close();
    awaitType(followerDb, "Issue11064C~Issue11064D");
    assertThat(followerDb.getSchema().existsType("Issue11064C")).isFalse();

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher", "CREATE (n:Issue11064C:Issue11064D {name: 'x'})").close();
      followerDb.commit();
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
        final Schema schema = getServerDatabase(server, getDatabaseName()).getSchema();
        assertThat(schema.existsType("Issue11064C")).as("server %d", server).isTrue();
        assertThat(schema.getType("Issue11064C~Issue11064D").instanceOf("Issue11064D")).as("server %d", server).isTrue();
      });
    }
  }

  /**
   * A leader without the function answers "Function not found": the follower gives the retryable refusal of before, not that error.
   */
  // Regression for #11064: a leader without the function gives the refusal of before it
  @Test
  @Timeout(180)
  void aLeaderWithoutTheFunctionGivesTheRefusalOfBeforeIt() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    final String key = SQLFunctionEnsureLabelTypes.NAME.toLowerCase(Locale.ENGLISH);
    // The factory is shared by every node of this JVM: removing the function makes every leader one that predates it
    factory.unregister(key);
    try {
      assertThat(factory.getFunctions()).doesNotContainKey(key);
      followerDb.begin();
      try {
        markClientManaged(followerDb);
        final Throwable error = catchThrowable(
            () -> followerDb.command("opencypher", "CREATE (n:Issue11064E:Issue11064F {name: 'x'})").close());
        assertThat(CauseChain.find(error, ServerIsNotTheLeaderException.class)).as("got %s", error).isNotNull();
      } finally {
        if (followerDb.isTransactionActive())
          followerDb.rollback();
      }
    } finally {
      factory.register(SQLFunctionEnsureLabelTypes.NAME, SQLFunctionEnsureLabelTypes.class);
    }
    assertThat(factory.getFunctions()).containsKey(key);
  }

  private static void awaitType(final Database db, final String type) {
    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(db.getSchema().existsType(type)).isTrue());
  }

  private static void markClientManaged(final Database db) {
    ((DatabaseInternal) db).getTransaction().setClientManaged(true);
  }

  private static long countOn(final Database db, final String label) {
    try (final ResultSet rs = db.query("opencypher", "MATCH (n:" + label + ") RETURN count(n) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    } catch (final RuntimeException e) {
      return 0L;
    }
  }

  private static final int TENANTS = 12;
  private static final int AGENTS  = 31;

  /**
   * Regression test for issue #5297: `SELECT count(*) FROM V` on a base type returned a different (and wrong) total on
   * the leader than on the followers, while the per-type counts agreed everywhere. The leader mutates its in-memory
   * schema incrementally, whereas the followers rebuild theirs from the replicated schema.json: any bucket added to a
   * sub type after the inheritance was declared was missing from the leader's polymorphic bucket cache only.
   */
  // Regression for #5297: polymorphic count is identical on every node
  @Test
  void polymorphicCountIsIdenticalOnEveryNode() {
    final int leaderIndex = findLeaderIndex();
    final Database leader = getServer(leaderIndex).getDatabaseForOperator(getDatabaseName());

    leader.getSchema().createVertexType("Base");
    leader.getSchema().createVertexType("Tenant").addSuperType("Base");
    leader.getSchema().createVertexType("Agent").addSuperType("Base");

    // GROW THE SUB TYPES *AFTER* THE INHERITANCE HAS BEEN DECLARED: THIS IS WHAT DRIFTED THE LEADER'S CACHE
    leader.command("sql", "ALTER TYPE Tenant BUCKET +Tenant_extra");
    leader.command("sql", "ALTER TYPE Agent BUCKET +Agent_extra");

    leader.transaction(() -> {
      for (int i = 0; i < TENANTS; i++)
        leader.newVertex("Tenant").set("id", i).save();
      for (int i = 0; i < AGENTS; i++)
        leader.newVertex("Agent").set("id", i).save();
    });

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServer(i).getDatabaseForOperator(getDatabaseName());
      final String role = i == leaderIndex ? "leader" : "follower";

      assertThat(count(db, "Tenant")).as("per-type count of Tenant on the %s (server %d)", role, i).isEqualTo(TENANTS);
      assertThat(count(db, "Agent")).as("per-type count of Agent on the %s (server %d)", role, i).isEqualTo(AGENTS);
      assertThat(count(db, "Base")).as("polymorphic count of Base on the %s (server %d)", role, i)
          .isEqualTo(TENANTS + AGENTS);
    }
  }

  private long count(final Database db, final String typeName) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) as c FROM " + typeName)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
