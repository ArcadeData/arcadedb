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
package com.arcadedb.server.security;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8405: host code (a JavaScript/Java trigger, a {@code LANGUAGE js} function, a polyglot
 * command) reaches the security manager through {@code database.getSecurity()}, and that used to be the real
 * {@link ServerSecurity} instance. GraalVM resolves members against the runtime class, so a script could call any public
 * {@code ServerSecurity} mutator - the cluster-wide ones off the leader, and the node-local ones on any node, silently
 * diverging that node's user and group documents from the rest of the cluster. A Java trigger could do the same with a
 * cast.
 * <p>
 * The invariant: the security manager a database hands out exposes the {@link SecurityManager} interface and nothing
 * else of {@link ServerSecurity}, while the interface methods keep working through it.
 */
class Issue8405HostCodeSecurityViewIT extends BaseGraphServerTest {

  /**
   * Every public {@link ServerSecurity} mutator the issue names, none of which is part of {@link SecurityManager}.
   */
  private static final List<String> SERVER_ONLY_MUTATORS = List.of(//
      "createUserClusterWide", "updateUserClusterWide", "dropUserClusterWide", "saveGroupClusterWide",
      "deleteGroupClusterWide", "createApiTokenClusterWide", "deleteApiTokenClusterWide", //
      "updateUser", "dropUserLocally", "saveGroup", "deleteGroup", "saveUsers", "saveGroups", "loadUsers",
      "applyReplicatedUsers", "applyReplicatedGroups", "stopService", "getUser", "authenticate");

  @Test
  void databaseSecurityManagerIsNotTheServerSecurityInstance() {
    final DatabaseInternal database = (DatabaseInternal) getServer(0).getDatabase(getDatabaseName());
    final SecurityManager security = database.getSecurity();

    assertThat(security).isNotNull();
    // What a Java trigger would cast to: no longer reachable.
    assertThat(security).isNotInstanceOf(ServerSecurity.class);
    assertThat(security).isNotSameAs(getServer(0).getSecurity());
    // The embedded instance under the server wrapper carries the same view.
    assertThat(database.getEmbedded().getSecurity()).isSameAs(security);
  }

  @Test
  void databaseSecurityManagerExposesOnlyTheSecurityManagerInterface() {
    final SecurityManager security = ((DatabaseInternal) getServer(0).getDatabase(getDatabaseName())).getSecurity();

    final Set<String> allowed = new TreeSet<>();
    for (final Method m : SecurityManager.class.getMethods())
      allowed.add(m.getName());
    for (final Method m : Object.class.getMethods())
      allowed.add(m.getName());

    // Public methods declared by the runtime class itself: what a GraalVM host lookup or a cast could reach.
    final Set<String> exposed = new TreeSet<>();
    for (final Method m : security.getClass().getMethods())
      if (Modifier.isPublic(m.getModifiers()))
        exposed.add(m.getName());

    assertThat(exposed).isSubsetOf(allowed);
    assertThat(Modifier.isPublic(security.getClass().getModifiers())).isFalse();
  }

  @Test
  void hostCodeCannotReachServerSecurityMutators() {
    final Database database = getServer(0).getDatabase(getDatabaseName());

    for (final String method : SERVER_ONLY_MUTATORS)
      try (final ResultSet rs = database.command("js", "typeof database.getSecurity()." + method)) {
        assertThat(rs.next().<String>getProperty("value"))
            .as("host code must not see ServerSecurity.%s through database.getSecurity()", method)
            .isEqualTo("undefined");
      }
  }

  @Test
  void hostCodeStillReachesTheSecurityManagerInterface() {
    final Database database = getServer(0).getDatabase(getDatabaseName());
    final ServerSecurity serverSecurity = getServer(0).getSecurity();
    final String user = "u8405host";

    try {
      try (final ResultSet rs = database.command("js",
          "database.getSecurity().createUser('" + user + "', 'pwd8405host'); database.getSecurity().existsUser('" + user
              + "')")) {
        assertThat(rs.next().<Boolean>getProperty("value")).isTrue();
      }
      assertThat(serverSecurity.existsUser(user)).isTrue();

      try (final ResultSet rs = database.command("js", "database.getSecurity().getUsers().contains('" + user + "')")) {
        assertThat(rs.next().<Boolean>getProperty("value")).isTrue();
      }

      final String previousHash = serverSecurity.getUser(user).getPassword();
      database.command("js", "database.getSecurity().setUserPassword('" + user + "', 'pwd8405other'); true").close();
      assertThat(serverSecurity.getUser(user).getPassword()).isNotEqualTo(previousHash);

      try (final ResultSet rs = database.command("js", "database.getSecurity().dropUser('" + user + "')")) {
        assertThat(rs.next().<Boolean>getProperty("value")).isTrue();
      }
      assertThat(serverSecurity.existsUser(user)).isFalse();
    } finally {
      if (serverSecurity.existsUser(user))
        serverSecurity.dropUser(user);
    }
  }

  /**
   * The interface's read methods must not hand out the live user state either: {@code getUsers()} was the live key set
   * of the user map, so {@code remove('root')} dropped root from this node's memory with nothing persisted or
   * replicated, and {@code getUserInfo(...).databases} was the user's live authorized-database set.
   */
  @Test
  void hostCodeCannotMutateTheUserStateThroughTheReadMethods() {
    final Database database = getServer(0).getDatabase(getDatabaseName());
    final ServerSecurity serverSecurity = getServer(0).getSecurity();

    try (final ResultSet rs = database.command("js",
        "try { database.getSecurity().getUsers().remove('root'); 'mutated' } catch (e) { 'refused' }")) {
      assertThat(rs.next().<String>getProperty("value")).isEqualTo("refused");
    }
    assertThat(serverSecurity.existsUser("root")).isTrue();

    try (final ResultSet rs = database.command("js",
        "try { database.getSecurity().getUserInfo('root').get('databases').add('x8405'); 'mutated' } catch (e) { 'refused' }")) {
      assertThat(rs.next().<String>getProperty("value")).isEqualTo("refused");
    }
    assertThat(serverSecurity.getUser("root").getAuthorizedDatabases()).doesNotContain("x8405");
  }

  @Test
  void interfaceCallsDelegateToTheServerSecurity() {
    final SecurityManager security = ((DatabaseInternal) getServer(0).getDatabase(getDatabaseName())).getSecurity();
    final ServerSecurity serverSecurity = getServer(0).getSecurity();

    assertThat(security.getUsers()).containsExactlyInAnyOrderElementsOf(serverSecurity.getUsers());
    assertThat(security.existsUser("root")).isTrue();
    assertThat(security.getUserInfo("root")).isEqualTo(serverSecurity.getUserInfo("root"));
    assertThat(serverSecurity.passwordMatch("x8405", security.encodePassword("x8405"))).isTrue();
  }
}
