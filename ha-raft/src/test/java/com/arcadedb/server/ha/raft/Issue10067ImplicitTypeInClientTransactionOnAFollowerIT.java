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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * Issue #10067: since issue #10040 a write sent to a follower inside a transaction the client opened (Bolt {@code BEGIN},
 * Postgres {@code BEGIN}, an HTTP session) runs in that transaction on the follower instead of being forwarded. An openCypher
 * {@code CREATE (n:NewLabel ...)} whose label has no type yet is not DDL to the analyzer, it creates the type implicitly while
 * executing, and on a follower that is a schema change only the leader may make: the statement was refused, where before #10040
 * it was forwarded and succeeded.
 * <p>
 * The type is now created by the leader on the statement's behalf (an idempotent {@code CREATE ... TYPE ... IF NOT EXISTS}, the
 * schema being non-transactional anywhere) and applied here before the statement goes on; the DATA write stays in the client's
 * transaction, so it commits or rolls back with it, which is what #10040 exists to give.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10067ImplicitTypeInClientTransactionOnAFollowerIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /** The case of the issue: the statement succeeds, and its write is the client transaction's, not a request of its own. */
  @Test
  @Timeout(180)
  void aNewLabelInAClientTransactionOnAFollowerCreatesItsTypeThroughTheLeaderAndCommitsWithTheTransaction() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher", "CREATE (n:Issue10067Commit {name: 'kept'})").close();

      assertThat(followerDb.getSchema().existsType("Issue10067Commit")).as("the type is applied on the follower").isTrue();
      assertThat(countOn(leaderDb, "Issue10067Commit")).as("the DATA write is the client's transaction's, not yet committed")
          .isZero();
      followerDb.commit();
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }

    awaitOnEveryServer("Issue10067Commit", 1);
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType("Issue10067Commit"))
          .as("server %d holds the type", i).isTrue();
  }

  /** The ROLLBACK #10040 protects: the node is gone with the transaction, and the type, which is not transactional, stays. */
  @Test
  @Timeout(180)
  void aRollbackDiscardsTheDataAndLeavesTheType() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    followerDb.begin();
    markClientManaged(followerDb);
    followerDb.command("opencypher", "CREATE (n:Issue10067Rollback {name: 'discarded'})").close();
    followerDb.rollback();

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() ->
          assertThat(getServerDatabase(server, getDatabaseName()).getSchema().existsType("Issue10067Rollback"))
              .as("server %d holds the type", server).isTrue());
      assertThat(countOn(getServerDatabase(i, getDatabaseName()), "Issue10067Rollback")).as("server %d holds no node", i).isZero();
    }
  }

  /** Several labels (the composite type extends them) and a relationship type: every type the statement needs is created. */
  @Test
  @Timeout(180)
  void severalLabelsAndARelationshipTypeAreCreatedToo() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher",
          "CREATE (a:Issue10067A:Issue10067B {name: 'a'})-[:ISSUE10067_REL {since: 1}]->(c:Issue10067C {name: 'c'})").close();
      followerDb.commit();
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }

    awaitOnEveryServer("Issue10067C", 1);
    for (int i = 0; i < getServerCount(); i++) {
      final Schema schema = getServerDatabase(i, getDatabaseName()).getSchema();
      assertThat(schema.existsType("Issue10067A")).as("server %d", i).isTrue();
      assertThat(schema.existsType("Issue10067B")).as("server %d", i).isTrue();
      assertThat(schema.existsType("Issue10067A~Issue10067B")).as("server %d", i).isTrue();
      assertThat(schema.getType("Issue10067A~Issue10067B").instanceOf("Issue10067B")).as("server %d", i).isTrue();
      assertThat(schema.existsType("ISSUE10067_REL")).as("server %d", i).isTrue();
    }
    try (final ResultSet rs = getServerDatabase(leader, getDatabaseName()).query("opencypher",
        "MATCH (:Issue10067A)-[r:ISSUE10067_REL]->(:Issue10067C) RETURN count(r) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    }
  }

  /**
   * A transaction that wrote before it met the new label was prepared under the schema without the type, which the leader's check
   * (issue #8686) cannot tell from a change to a type the transaction wrote to. When the new type is the ONLY schema change since the
   * transaction was prepared, nothing it saved depended on anything the type changed, and the commit goes through, both statements'
   * work in the one transaction.
   */
  @Test
  @Timeout(180)
  void aTransactionThatAlreadyWroteGoesThroughWhenTheTypeIsTheOnlySchemaChange() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher", "CREATE (n:V1 {id: 1001000})").close();
      // The write above is staged: the type below is created under a transaction that already holds work
      followerDb.command("opencypher", "CREATE (a:V1 {id: 1001001}), (b:Issue10067Late {name: 'late'})").close();
      followerDb.commit();
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }

    awaitOnEveryServer("Issue10067Late", 1);
    for (int i = 0; i < getServerCount(); i++)
      try (final ResultSet rs = getServerDatabase(i, getDatabaseName()).query("opencypher",
          "MATCH (n:V1) WHERE n.id IN [1001000, 1001001] RETURN count(n) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).as("server %d holds both nodes of the transaction", i).isEqualTo(2L);
      }
  }

  /**
   * Two clients creating the SAME new label at the same time through the TWO followers: one has the leader create the type, for the
   * other the leader's {@code IF NOT EXISTS} is a no-op, and both end with the type and their node. A refusal along the way may only be
   * the retryable one (the two commits can meet on a page of the new type), and the retry goes through.
   */
  @Test
  @Timeout(240)
  void twoFollowersCreatingTheSameNewLabelAtOnceBothEndWithTheirNode() throws Exception {
    final int leader = requireLeaderIndex();
    final int[] followers = new int[2];
    for (int i = 0, n = 0; i < getServerCount(); i++)
      if (i != leader)
        followers[n++] = i;

    final CyclicBarrier together = new CyclicBarrier(2);
    final ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      final List<Future<Boolean>> results = new ArrayList<>();
      for (int f = 0; f < 2; f++) {
        final int server = followers[f];
        final int name = f;
        results.add(pool.submit(() -> {
          final Database db = getServerDatabase(server, getDatabaseName());
          together.await(30, TimeUnit.SECONDS);
          for (int attempt = 1; attempt <= 5; attempt++) {
            db.begin();
            try {
              markClientManaged(db);
              db.command("opencypher", "CREATE (n:Issue10067Race {name: 'client" + name + "'})").close();
              db.commit();
              return true;
            } catch (final RuntimeException e) {
              assertThat(CauseChain.find(e, NeedRetryException.class)).as("a refusal here must be the retryable one, got %s", messageChainOf(e))
                  .isNotNull();
            } finally {
              if (db.isTransactionActive())
                db.rollback();
            }
          }
          return false;
        }));
      }
      for (final Future<Boolean> result : results)
        assertThat(result.get(180, TimeUnit.SECONDS)).as("each client's transaction commits, within its retries").isTrue();
    } finally {
      pool.shutdownNow();
    }

    awaitOnEveryServer("Issue10067Race", 2);
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType("Issue10067Race")).as("server %d", i).isTrue();
  }

  /** A label with a back-tick and a space is quoted into the statement the leader runs, and names the type exactly. */
  @Test
  @Timeout(180)
  void aLabelWithQuotingCharactersCreatesExactlyThatType() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());
    final String label = "`Issue10067 we``ird`";

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher", "CREATE (n:" + label + " {name: 'quoted'})").close();
      followerDb.commit();
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }

    awaitOnEveryServer(label, 1);
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType("Issue10067 we`ird")).as("server %d", i).isTrue();
  }

  /**
   * A transaction overtaken by ANOTHER schema change cannot be vouched for, and the leader would refuse its commit after the rest of its
   * work. It is told so at once, with the retryable error, and the retry, which finds the type, goes through.
   */
  @Test
  @Timeout(180)
  void aTransactionOvertakenByAnotherSchemaChangeIsToldToRetryAndTheRetrySucceeds() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());
    final String statement = "CREATE (a:V1 {id: 1002001}), (b:Issue10067Overtaken {name: 'overtaken'})";

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher", "CREATE (n:V1 {id: 1002000})").close();

      // Somebody else changes the schema while the transaction is open, and this node applies it
      getServerDatabase(leader, getDatabaseName()).command("sql", "CREATE VERTEX TYPE Issue10067Bystander").close();
      await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() ->
          assertThat(followerDb.getSchema().existsType("Issue10067Bystander")).isTrue());

      final Throwable error = catchThrowable(() -> followerDb.command("opencypher", statement).close());
      assertThat(CauseChain.find(error, NeedRetryException.class)).as("got %s", error).isNotNull();
      assertThat(messageChainOf(error)).contains("Issue10067Overtaken").contains("retry the transaction");
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }
    assertThat(followerDb.getSchema().existsType("Issue10067Overtaken")).as("the type was created for the retry").isTrue();

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher", "CREATE (n:V1 {id: 1002000})").close();
      followerDb.command("opencypher", statement).close();
      followerDb.commit();
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }
    awaitOnEveryServer("Issue10067Overtaken", 1);
  }

  /**
   * The type is created for the USER of the statement, not for the cluster: a user whose group grants records and not the schema is
   * refused (no forward is sent, no type appears anywhere), and the same statement succeeds once the group grants
   * {@code updateSchema} - which is what decides it.
   */
  @Test
  @Timeout(240)
  void aUserWithoutTheSchemaPermissionCannotHaveATypeCreatedForIt() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final ServerSecurity leaderSecurity = getServer(leader).getSecurity();
    final String group = "issue10067writers";
    final String user = "issue10067writer";

    leaderSecurity.saveGroupClusterWide(getDatabaseName(), group, recordsOnlyGroup(false));
    leaderSecurity.createUserClusterWide(new JSONObject().put("name", user).put("password", leaderSecurity.encodePassword("issue10067pwd"))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(group))));
    await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      assertThat(getServer(follower).getSecurity().existsUser(user)).isTrue();
      assertThat(groupOf(getServer(follower).getSecurity(), group)).isNotNull();
    });

    final DatabaseInternal followerDb = (DatabaseInternal) getServerDatabase(follower, getDatabaseName());
    final SecurityDatabaseUser restricted = getServer(follower).getSecurity().getUser(user).getDatabaseUser(followerDb);
    final SecurityDatabaseUser root = getServer(follower).getSecurity().getUser("root").getDatabaseUser(followerDb);
    try {
      DatabaseContext.INSTANCE.init(followerDb).setCurrentUser(restricted);
      followerDb.begin();
      try {
        markClientManaged(followerDb);
        final Throwable refused = catchThrowable(
            () -> followerDb.command("opencypher", "CREATE (n:Issue10067Denied {name: 'denied'})").close());
        assertThat(refused).as("a user without updateSchema must be refused").isNotNull();
        assertThat(messageChainOf(refused)).contains("is not allowed to update schema");
      } finally {
        if (followerDb.isTransactionActive())
          followerDb.rollback();
      }
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType("Issue10067Denied"))
            .as("server %d must not hold a type created for a user who may not create it", i).isFalse();

      // The same user, once the group grants the schema: the statement goes through and the type is created for it
      leaderSecurity.saveGroupClusterWide(getDatabaseName(), group, recordsOnlyGroup(true));
      await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() ->
          assertThat(groupOf(getServer(follower).getSecurity(), group).getJSONArray("access").toList()).contains("updateSchema"));
      final SecurityDatabaseUser granted = getServer(follower).getSecurity().getUser(user).getDatabaseUser(followerDb);
      DatabaseContext.INSTANCE.init(followerDb).setCurrentUser(granted);
      followerDb.begin();
      try {
        markClientManaged(followerDb);
        followerDb.command("opencypher", "CREATE (n:Issue10067Granted {name: 'granted'})").close();
        followerDb.commit();
      } finally {
        if (followerDb.isTransactionActive())
          followerDb.rollback();
      }
    } finally {
      DatabaseContext.INSTANCE.init(followerDb).setCurrentUser(root);
    }
    awaitOnEveryServer("Issue10067Granted", 1);
  }

  private JSONObject recordsOnlyGroup(final boolean withSchema) {
    final JSONArray access = new JSONArray();
    if (withSchema)
      access.put("updateSchema");
    return new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L).put("access", access)
        .put("types", new JSONObject().put("*", new JSONObject().put("access",
            new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"))));
  }

  private JSONObject groupOf(final ServerSecurity security, final String group) {
    return security.groupsToJSON().getJSONObject("databases", new JSONObject()).getJSONObject(getDatabaseName(), new JSONObject())
        .getJSONObject("groups", new JSONObject()).getJSONObject(group, null);
  }

  /**
   * The leader makes the type with ITS default bucket count unless the caller chose one: a follower configured with a different
   * default must take the type the leader made, not count it against its own and fall back to a local schema change it cannot make.
   */
  @Test
  @Timeout(180)
  void aFollowerWhoseDefaultBucketCountDiffersTakesTheTypeTheLeaderMade() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());
    final ContextConfiguration followerConfig = ((DatabaseInternal) followerDb).getConfiguration();
    final int leaderDefault = ((DatabaseInternal) getServerDatabase(leader, getDatabaseName())).getConfiguration()
        .getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    final int followerDefault = leaderDefault + 3;

    followerConfig.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, followerDefault);
    try {
      followerDb.begin();
      try {
        markClientManaged(followerDb);
        followerDb.command("opencypher", "CREATE (n:Issue10067Buckets {name: 'buckets'})").close();
        followerDb.commit();
      } finally {
        if (followerDb.isTransactionActive())
          followerDb.rollback();
      }
    } finally {
      followerConfig.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, leaderDefault);
    }

    awaitOnEveryServer("Issue10067Buckets", 1);
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().getType("Issue10067Buckets").getBuckets(false))
          .as("server %d holds the type with the leader's bucket count", i).hasSize(leaderDefault);
  }

  /** {@code MERGE} reaches the same place as {@code CREATE}. */
  @Test
  @Timeout(180)
  void aMergeWithANewLabelIsCreatedToo() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher", "MERGE (n:Issue10067Merge {name: 'merged'})").close();
      followerDb.commit();
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }

    awaitOnEveryServer("Issue10067Merge", 1);
  }

  /**
   * The forward the type creation needs is refused when this node cannot tell its peers apart (issue #6191): the statement is then
   * refused as a leader refusal naming the leader, which a routing driver acts on, and no data is written.
   */
  @Test
  @Timeout(180)
  void whenTheLeaderCannotBeReachedTheRefusalNamesIt() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    final Runnable restore = refuseForwardedWritesOn(follower);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());
    try {
      followerDb.begin();
      try {
        markClientManaged(followerDb);
        final Throwable error = catchThrowable(
            () -> followerDb.command("opencypher", "CREATE (n:Issue10067Refused {name: 'refused'})").close());
        final ServerIsNotTheLeaderException refusal = CauseChain.find(error, ServerIsNotTheLeaderException.class);
        assertThat(refusal).as("the statement must be refused as a leader refusal, got %s", error).isNotNull();
        assertThat(raft.isOwnHttpAddress(refusal.getLeaderAddress())).isFalse();
        assertThat(refusal.getLeaderAddress()).isEqualTo(raft.getLeaderName());
      } finally {
        if (followerDb.isTransactionActive())
          followerDb.rollback();
      }
      assertThat(followerDb.getSchema().existsType("Issue10067Refused")).isFalse();
    } finally {
      restore.run();
    }
  }

  /**
   * A schema change that is NOT made from a client's transaction keeps being refused: an embedded caller that creates a type on a
   * follower is told to go to the leader, as before.
   */
  @Test
  @Timeout(180)
  void aSchemaChangeOutsideAClientTransactionIsStillRefused() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    final Throwable error = catchThrowable(() -> followerDb.getSchema().getOrCreateVertexType("Issue10067Embedded"));
    assertThat(CauseChain.find(error, ServerIsNotTheLeaderException.class)).as("got %s", error).isNotNull();
    assertThat(followerDb.getSchema().existsType("Issue10067Embedded")).isFalse();

    followerDb.begin();
    try {
      // an ordinary transaction: not one the client opened, so the rule of issue #10040 does not apply to it
      final Throwable inTx = catchThrowable(() -> followerDb.getSchema().getOrCreateVertexType("Issue10067Embedded"));
      assertThat(CauseChain.find(inTx, ServerIsNotTheLeaderException.class)).as("got %s", inTx).isNotNull();
    } finally {
      followerDb.rollback();
    }
  }

  private static String messageChainOf(final Throwable error) {
    final StringBuilder chain = new StringBuilder();
    for (Throwable t = error; t != null; t = t.getCause())
      chain.append(t.getMessage()).append(" | ");
    return chain.toString();
  }

  private static void markClientManaged(final Database db) {
    // What the Bolt and Postgres executors mark on the transaction a client opens with BEGIN (issue #10040)
    ((DatabaseInternal) db).getTransaction().setClientManaged(true);
  }

  private static long countOn(final Database db, final String label) {
    try (final ResultSet rs = db.query("opencypher", "MATCH (n:" + label + ") RETURN count(n) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    } catch (final RuntimeException e) {
      // The type does not exist (yet) on this node: no node of it either
      return 0L;
    }
  }

  private void awaitOnEveryServer(final String label, final long expected) {
    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() ->
          assertThat(countOn(getServerDatabase(server, getDatabaseName()), label)).as("server %d, label %s", server, label)
              .isEqualTo(expected));
    }
  }
}
