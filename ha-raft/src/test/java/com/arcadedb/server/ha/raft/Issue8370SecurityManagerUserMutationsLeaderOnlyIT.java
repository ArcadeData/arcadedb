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

import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8370, follow-up to #8109: every CLIENT route that mutates the user document reaches the leader, but the
 * {@code SecurityManager} entry points of {@code ServerSecurity} - {@code createUser(String, String)},
 * {@code dropUser(String)} and {@code setUserPassword(String, String)} - are also reachable from host code running
 * with the engine's privileges: a JavaScript/Java trigger or a {@code DEFINE FUNCTION ... LANGUAGE js} body gets the
 * real {@code database} object. Such code runs on a FOLLOWER without any forward, for example a JS function called
 * from an idempotent {@code SELECT}, which {@code RaftReplicatedDatabase.query} executes locally.
 * <p>
 * Those three entry points now refuse off-leader, as the gRPC admin RPCs do, instead of submitting the whole user
 * list built from the follower's own (possibly lagging) view. The openCypher {@code CREATE/ALTER/DROP USER} commands,
 * the other consumer of the same entry points, are unaffected: {@code RaftReplicatedDatabase.command} forwards them
 * to the leader before the engine reaches {@code SecurityManager} (proved by
 * {@link Issue8109SecurityMutationsReachTheLeaderIT}).
 */
@Tag("slow")
class Issue8370SecurityManagerUserMutationsLeaderOnlyIT extends BaseRaftHATest {

  private static final String PASSWORD = "issue8370password";

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * The three entry points called directly on a follower are refused with a not-the-leader error naming the leader,
   * and change nothing on any node. The same calls on the leader are the control: they succeed and replicate.
   */
  @Test
  @Timeout(180)
  void securityManagerUserMutationsAreRefusedOnAFollowerAndReplicatedFromTheLeader() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);
    final ServerSecurity followerSecurity = getServer(follower).getSecurity();
    final ServerSecurity leaderSecurity = getServer(leader).getSecurity();

    // Control: on the leader the entry point works and replicates to every node.
    final String existing = "issue8370existing";
    leaderSecurity.createUser(existing, PASSWORD);
    awaitOnEveryServer(existing, true);
    final String passwordHashBefore = leaderSecurity.getUser(existing).getPassword();

    final String never = "issue8370nevercreated";
    assertRefusedOffLeader(() -> followerSecurity.createUser(never, PASSWORD));
    assertRefusedOffLeader(() -> followerSecurity.setUserPassword(existing, PASSWORD + "2"));
    assertRefusedOffLeader(() -> followerSecurity.dropUser(existing));

    for (int i = 0; i < getServerCount(); i++) {
      final ServerSecurity security = getServer(i).getSecurity();
      assertThat(security.existsUser(never)).as("server %d must not hold the refused user", i).isFalse();
      final ServerSecurityUser user = security.getUser(existing);
      assertThat(user).as("server %d must still hold the user whose drop was refused", i).isNotNull();
      assertThat(user.getPassword()).as("server %d must keep the password whose rotation was refused", i)
          .isEqualTo(passwordHashBefore);
    }

    // Control, the other two entry points on the leader.
    leaderSecurity.setUserPassword(existing, PASSWORD + "2");
    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServer(i).getSecurity().getUser(existing).getPassword())
            .as("server %d must apply the leader's rotation", i).isNotEqualTo(passwordHashBefore);
    });
    assertThat(leaderSecurity.dropUser(existing)).isTrue();
    awaitOnEveryServer(existing, false);
  }

  /**
   * The door the issue names: a JS function body reaches {@code database.getSecurity().createUser(...)}, and an
   * idempotent {@code SELECT} calling it on a follower is executed there, not forwarded. It must be refused.
   */
  @Test
  @Timeout(180)
  void hostCodeOnAFollowerCannotMutateTheUserDocument() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    final String user = "issue8370hostcode";
    final String define = "DEFINE FUNCTION lib8370.mint \"database.getSecurity().createUser('" + user + "', '" + PASSWORD
        + "'); return 1;\" LANGUAGE js";
    // Defined once, on the leader: DEFINE FUNCTION replicates like any other DDL since issue #8404.
    getServer(leader).getDatabase(getDatabaseName()).command("sql", define);
    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() ->
        assertThat(getServer(follower).getDatabase(getDatabaseName()).getSchema().hasFunctionLibrary("lib8370"))
            .as("the function must reach the follower").isTrue());

    assertThatThrownBy(() -> {
      try (final ResultSet rs = getServer(follower).getDatabase(getDatabaseName())
          .query("sql", "SELECT `lib8370.mint`() AS r")) {
        rs.next();
      }
    }).as("host code on a follower must not mutate the user document").satisfies(e ->
        assertThat(messageChainOf(e)).contains("must run on the cluster leader"));

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServer(i).getSecurity().existsUser(user)).as("server %d must not hold the refused user", i)
          .isFalse();

    // Control: the same host code on the leader succeeds and replicates.
    try (final ResultSet rs = getServer(leader).getDatabase(getDatabaseName())
        .query("sql", "SELECT `lib8370.mint`() AS r")) {
      rs.next();
    }
    awaitOnEveryServer(user, true);
    getServer(leader).getSecurity().dropUser(user);
    awaitOnEveryServer(user, false);
  }

  private static void assertRefusedOffLeader(final Runnable call) {
    assertThatThrownBy(call::run).isInstanceOf(ServerIsNotTheLeaderException.class)
        .hasMessageContaining("must run on the cluster leader");
  }

  private void awaitOnEveryServer(final String user, final boolean present) {
    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServer(i).getSecurity().existsUser(user)).as("user %s on server %d", user, i)
            .isEqualTo(present);
    });
  }

  private static String messageChainOf(final Throwable e) {
    final StringBuilder sb = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause())
      sb.append(t.getClass().getName()).append(": ").append(t.getMessage()).append('\n');
    return sb.toString();
  }

  private int firstFollower(final int leader) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }
}
