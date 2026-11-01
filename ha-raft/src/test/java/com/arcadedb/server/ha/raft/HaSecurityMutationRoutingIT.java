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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Where a cluster-wide security mutation runs, end to end on ONE three-node cluster shared by every test
 * ({@link SharedClusterRaftHATest}). Merged from {@code Issue7380RestUserRoutesLeaderGateIT},
 * {@code Issue8109SecurityMutationsReachTheLeaderIT}, {@code Issue8370SecurityManagerUserMutationsLeaderOnlyIT} and
 * {@code Issue7559SecurityPreconditionOnFollowerIT}, which ran on the same fixture: three servers, {@code majority}
 * quorum (#7559 set it explicitly, the others had it as the default), no other configuration override.
 * <ul>
 * <li><b>#7380</b>: {@code POST}, {@code PUT} and {@code DELETE /api/v1/server/users} called the cluster-wide user
 * mutators on whichever node served the request, which on an HA cluster submits a Raft entry from a follower. The same
 * operations were already gated on the command path ({@code POST /api/v1/server} forwards) and on gRPC (refuses, #7304,
 * #7309). The REST routes now forward too.</li>
 * <li><b>#8109</b> (absorbing #7826): the REST group and API-token routes ran their mutation where the request landed.
 * With every security mutation on the leader, the {@code ServerSecurity} monitor serialises the cluster rather than one
 * node, and the compare-and-set of #7509 is a backstop rather than the only defence. openCypher
 * {@code CREATE/ALTER/DROP USER} needed no change: the statement is not idempotent, so
 * {@code RaftReplicatedDatabase.command} forwards it before the engine reaches {@code SecurityManager}; the one-hop
 * refusal proves that too.</li>
 * <li><b>#8370</b>, follow-up to #8109: the {@code SecurityManager} entry points ({@code createUser},
 * {@code dropUser}, {@code setUserPassword}) are reachable from host code with the engine's privileges (a JS trigger or
 * {@code DEFINE FUNCTION ... LANGUAGE js}), which runs on a FOLLOWER without any forward, e.g. from an idempotent
 * {@code SELECT}. They now refuse off-leader instead of submitting the user list built from the follower's own
 * (possibly lagging) view.</li>
 * <li><b>#7559</b>: the #7509 compare-and-set must reach the same verdict on every node, and reached it only on the
 * leader: a follower's capability registry was empty, so it dropped the precondition, on exactly the routes that do not
 * forward. On a mixed-version cluster the leader and the follower must also withhold it TOGETHER, or a losing entry is
 * refused on some nodes and applied on others and the security state diverges. The "old" node is built with
 * {@link RaftHAServer#setAdvertisedCapabilities}, as {@code HaMixedVersionSchemaDeltaIT} does; the decision
 * consults nothing but what a peer said it can read. The assertions poll, because a node that was recently leader holds
 * advertisements for up to {@link PeerCapabilityRegistry#ADVERTISEMENT_TTL_MS} (#7301).</li>
 * </ul>
 * The distinguishing assertion of #7380 and #8109 is the <b>one-hop refusal</b>: a request that arrives on a follower
 * already marked as forwarded by a peer (cluster token plus the marker, the only form in which it is trusted, see
 * {@link LeaderForwardContext}) must be refused there; a route that does not forward never looks at the marker and
 * answers 200/201. The happy-path tests are the controls that keep the refusals honest.
 * <p>
 * Why these share a cluster: every user, group and API token a test creates is named through
 * {@link #uniqueName(String)}, and no test counts principals or lists tokens exactly. The one failed login (#7380) is
 * against a principal of its own, so the brute-force guard cannot lock out another test's user. #7559 changes what one
 * node advertises and restores it in a {@code finally}, then waits until both the leader and the follower reach the uniform
 * verdict again; the token it removes ({@code SECURITY_PRECONDITION}) gates only the precondition, which no other test
 * here asserts. #8370's JS function library {@code lib8370} is defined once per cluster boot. No test stops a node,
 * moves leadership or rewrites a peer address.
 * <p>
 * Siblings: {@link HaSecurityMutationRoutingTwoNodeIT} (#7833, #8034, #7549, two nodes) and
 * {@link HaSecurityMutationRoutingDisruptiveIT} (#8034, moves leadership). The refusal of a {@code SecurityManager}
 * mutation that NAMES the leader (#10066) is with the address family: {@link HaPeerAddressRoutingIT} and
 * {@link HaPeerAddressRoutingMapMutationIT}.
 * <p>
 * Every source class was tagged {@code slow}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("slow")
class HaSecurityMutationRoutingIT extends SharedClusterRaftHATest {

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  private static final String PASSWORD_7380     = "issue7380password";
  private static final String NEW_PASSWORD_7380 = "issue7380password2";
  private static final String PASSWORD_8109     = "issue8109password";
  private static final String PASSWORD_8370     = "issue8370password";

  /** #7559's fingerprint. */
  private static final String FINGERPRINT = "7f".repeat(32);

  /** #7559: how long a node is given to reach a verdict, generously over the advertisement TTL plus a few rounds. */
  private static final long VERDICT_TIMEOUT_MS = 60_000L;

  /** #7559: how long a node is given to observe a peer's advertisement change (a few refresh rounds). */
  private static final long CAPABILITY_CONVERGENCE_TIMEOUT_MS = 30_000L;

  @Override
  protected int getServerCount() {
    return 3;
  }

  // ------------------------------------------------------------------ REST user routes (#7380)

  /**
   * Issue #7380, from {@code Issue7380RestUserRoutesLeaderGateIT}.
   * <p>
   * Every write route that reaches a cluster-wide security mutator has to refuse a request a peer already forwarded,
   * rather than run the mutation where the request landed. {@code POST /api/v1/server} is asserted alongside the three
   * REST routes because the forwarding moved into a shared collaborator: without it, a refactor that broke the command
   * path would still be green.
   */
  @Test
  @Timeout(180)
  void theThreeRestUserRoutesRefuseARequestAPeerAlreadyForwarded() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollowerOf(leader);

    final String clusterToken = getRaftPlugin(follower).getRaftHAServer().getClusterToken();
    assertThat(clusterToken).as("peers authenticate to each other with a cluster token").isNotBlank();

    // An existing user for PUT and DELETE to aim at, created the normal way (on the leader) so it is
    // present on every node before the refusals are driven.
    final String existing = uniqueName("issue7380marked");
    assertThat(asRoot(leader, "POST", "/api/v1/server/users",
        new JSONObject().put("name", existing).put("password", PASSWORD_7380).toString()).statusCode())
        .isEqualTo(201);
    awaitUserOnEveryServer(existing, true);

    final String created = uniqueName("issue7380nevercreated");

    assertRefusedAsAlreadyForwarded(
        asPeer(follower, clusterToken, "POST", "/api/v1/server/users",
            new JSONObject().put("name", created).put("password", PASSWORD_7380).toString()),
        "POST /server/users");

    assertRefusedAsAlreadyForwarded(
        asPeer(follower, clusterToken, "PUT", "/api/v1/server/users?name=" + existing,
            new JSONObject().put("password", NEW_PASSWORD_7380).toString()),
        "PUT /server/users");

    assertRefusedAsAlreadyForwarded(
        asPeer(follower, clusterToken, "DELETE", "/api/v1/server/users?name=" + existing, null),
        "DELETE /server/users");

    assertRefusedAsAlreadyForwarded(
        asPeer(follower, clusterToken, "POST", "/api/v1/server",
            new JSONObject().put("command", "create user { \"name\": \"" + created + "\", \"password\": \"" + PASSWORD_7380
                + "\", \"databases\": {} }").toString()),
        "POST /server create user");

    // Nothing may have been written on the way to any of those refusals.
    for (int i = 0; i < getServerCount(); i++) {
      assertThat(getServer(i).getSecurity().existsUser(created))
          .as("server %d must not hold a user every route refused to create", i).isFalse();
      assertThat(getServer(i).getSecurity().existsUser(existing))
          .as("server %d must still hold the user DELETE refused to drop", i).isTrue();
    }
    // One wrong-password attempt, deliberately: the brute-force guard locks a principal out after five
    // within 30 seconds, which would then make the positive check below fail for the wrong reason.
    assertThat(canLogIn(leader, existing, NEW_PASSWORD_7380))
        .as("the password PUT refused to change must not have changed").isFalse();
    assertThat(canLogIn(leader, existing, PASSWORD_7380))
        .as("the original password must still work").isTrue();
  }

  /**
   * Issue #7380, from {@code Issue7380RestUserRoutesLeaderGateIT}.
   * <p>
   * The control the refusals need: with the cluster's real addresses, each of the three routes called on a follower is
   * forwarded to the leader, executed there, and replicated back. Without it a build that refused every write on a
   * follower would satisfy the test above.
   */
  @Test
  @Timeout(180)
  void theThreeRestUserRoutesCalledOnAFollowerAreExecutedOnTheLeader() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollowerOf(leader);

    final String name = uniqueName("issue7380forwarded");

    final HttpResponse<String> created = asRoot(follower, "POST", "/api/v1/server/users",
        new JSONObject().put("name", name).put("password", PASSWORD_7380).toString());
    assertThat(created.statusCode()).as("POST on a follower, body: %s", created.body()).isEqualTo(201);
    awaitUserOnEveryServer(name, true);

    // Read after awaitUserOnEveryServer above, so every node already holds this same hash.
    final String originalHash = passwordHashOn(leader, name);

    final HttpResponse<String> updated = asRoot(follower, "PUT", "/api/v1/server/users?name=" + name,
        new JSONObject().put("password", NEW_PASSWORD_7380).toString());
    assertThat(updated.statusCode()).as("PUT on a follower, body: %s", updated.body()).isEqualTo(200);
    // The stored hash rather than a login attempt: five failed logins in 30 seconds lock a principal out, so
    // polling for the new password to take effect would be a test that sabotages itself.
    //
    // On EVERY node, not just the leader. The claim being tested is that the forwarded update replicates,
    // and a version that only reached the node it executed on would satisfy a leader-only wait and then be
    // swept away by the DELETE below before anything noticed.
    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (originalHash.equals(passwordHashOn(i, name)))
          return false;
      return true;
    });
    assertThat(canLogIn(leader, name, NEW_PASSWORD_7380))
        .as("the password the forwarded PUT set must authenticate on the leader").isTrue();

    final HttpResponse<String> dropped = asRoot(follower, "DELETE", "/api/v1/server/users?name=" + name, null);
    assertThat(dropped.statusCode()).as("DELETE on a follower, body: %s", dropped.body()).isEqualTo(200);
    awaitUserOnEveryServer(name, false);

    // The command path through the same collaborator. It dials the path of the request being served rather
    // than a hard-coded one now, so a build where that resolved to something undialable would answer here
    // and nowhere else - the refusals above never get as far as building a URL.
    final String viaCommand = uniqueName("issue7380viacommand");
    final HttpResponse<String> command = asRoot(follower, "POST", "/api/v1/server",
        new JSONObject().put("command", "create user { \"name\": \"" + viaCommand + "\", \"password\": \""
            + PASSWORD_7380 + "\", \"databases\": {} }").toString());
    assertThat(command.statusCode())
        .as("POST /server 'create user' on a follower, body: %s", command.body()).isEqualTo(200);
    awaitUserOnEveryServer(viaCommand, true);
  }

  // ------------------------------------------------------------------ group, API-token and openCypher routes (#8109)

  /** Issue #8109, from {@code Issue8109SecurityMutationsReachTheLeaderIT}. */
  @Test
  @Timeout(180)
  void everySecurityMutationRouteRefusesARequestAPeerAlreadyForwarded() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollowerOf(leader);

    final String clusterToken = getRaftPlugin(follower).getRaftHAServer().getClusterToken();
    assertThat(clusterToken).as("peers authenticate to each other with a cluster token").isNotBlank();

    // Existing state for the DELETE routes to aim at, created on the leader so it is on every node first.
    final String existingGroup = uniqueName("issue8109existinggroup");
    assertThat(asRoot(leader, "POST", "/api/v1/server/groups",
        new JSONObject().put("database", "*").put("name", existingGroup).toString()).statusCode()).isEqualTo(200);
    awaitOnEveryServer(s -> hasGroup(s, existingGroup), "group " + existingGroup + " present");

    final String existingToken = uniqueName("issue8109existingtoken");
    assertThat(asRoot(leader, "POST", "/api/v1/server/api-tokens",
        new JSONObject().put("name", existingToken).toString()).statusCode()).isEqualTo(201);
    awaitOnEveryServer(s -> tokenHashOf(s, existingToken) != null, "token " + existingToken + " present");
    final String existingTokenHash = tokenHashOf(getServer(leader).getSecurity(), existingToken);

    final String newGroup = uniqueName("issue8109nevercreatedgroup");
    final String newToken = uniqueName("issue8109nevercreatedtoken");
    final String newUser = uniqueName("issue8109nevercreateduser");

    assertRefusedAsAlreadyForwarded(asPeer(follower, clusterToken, "POST", "/api/v1/server/groups",
        new JSONObject().put("database", "*").put("name", newGroup).toString()), "POST /server/groups");
    assertRefusedAsAlreadyForwarded(asPeer(follower, clusterToken, "DELETE",
        "/api/v1/server/groups?database=*&name=" + existingGroup, null), "DELETE /server/groups");
    assertRefusedAsAlreadyForwarded(asPeer(follower, clusterToken, "POST", "/api/v1/server/api-tokens",
        new JSONObject().put("name", newToken).toString()), "POST /server/api-tokens");
    assertRefusedAsAlreadyForwarded(asPeer(follower, clusterToken, "DELETE",
        "/api/v1/server/api-tokens?token=" + existingTokenHash, null), "DELETE /server/api-tokens");
    assertRefusedAsAlreadyForwarded(asPeer(follower, clusterToken, "POST", "/api/v1/command/" + getDatabaseName(),
            cypher("CREATE USER " + newUser + " SET PASSWORD '" + PASSWORD_8109 + "'")),
        "openCypher CREATE USER over /api/v1/command");

    for (int i = 0; i < getServerCount(); i++) {
      final ServerSecurity security = getServer(i).getSecurity();
      assertThat(hasGroup(security, newGroup)).as("server %d must not hold the refused group", i).isFalse();
      assertThat(hasGroup(security, existingGroup)).as("server %d must still hold the group DELETE refused", i)
          .isTrue();
      assertThat(tokenHashOf(security, newToken)).as("server %d must not hold the refused token", i).isNull();
      assertThat(tokenHashOf(security, existingToken)).as("server %d must still hold the token DELETE refused", i)
          .isNotNull();
      assertThat(security.existsUser(newUser)).as("server %d must not hold the refused user", i).isFalse();
    }
  }

  /**
   * Issue #8109, from {@code Issue8109SecurityMutationsReachTheLeaderIT}.
   * <p>
   * A plaintext token handed to {@code DELETE /server/api-tokens} is refused on the node that received it, before the
   * forward: relaying it would copy the live token into a second node's request path. Sent marked as already forwarded,
   * so a build that forwarded first would answer with the one-hop refusal instead.
   */
  @Test
  @Timeout(180)
  void aPlaintextTokenIsRefusedOnTheFollowerBeforeTheForward() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollowerOf(leader);
    final String clusterToken = getRaftPlugin(follower).getRaftHAServer().getClusterToken();

    final HttpResponse<String> minted = asRoot(leader, "POST", "/api/v1/server/api-tokens",
        new JSONObject().put("name", uniqueName("issue8109plaintext")).toString());
    assertThat(minted.statusCode()).as("mint on the leader, body: %s", minted.body()).isEqualTo(201);
    final String plaintext = new JSONObject(minted.body()).getJSONObject("result").getString("token");

    final HttpResponse<String> refused = asPeer(follower, clusterToken, "DELETE",
        "/api/v1/server/api-tokens?token=" + plaintext, null);
    assertThat(refused.statusCode()).as("body: %s", refused.body()).isEqualTo(400);
    assertThat(refused.body()).contains("token hash").doesNotContain("already forwarded");
  }

  /**
   * Issue #8109, from {@code Issue8109SecurityMutationsReachTheLeaderIT}.
   * <p>
   * The control: with the cluster's real addresses, each route called on a follower is executed on the leader and
   * replicated back. Without it a build that refused every write on a follower would satisfy the refusal test.
   */
  @Test
  @Timeout(180)
  void everySecurityMutationRouteCalledOnAFollowerIsExecutedOnTheLeader() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollowerOf(leader);

    final String group = uniqueName("issue8109forwardedgroup");
    final HttpResponse<String> savedGroup = asRoot(follower, "POST", "/api/v1/server/groups",
        new JSONObject().put("database", "*").put("name", group).toString());
    assertThat(savedGroup.statusCode()).as("POST /server/groups on a follower, body: %s", savedGroup.body())
        .isEqualTo(200);
    awaitOnEveryServer(s -> hasGroup(s, group), "group " + group + " present");

    final HttpResponse<String> deletedGroup = asRoot(follower, "DELETE",
        "/api/v1/server/groups?database=*&name=" + group, null);
    assertThat(deletedGroup.statusCode()).as("DELETE /server/groups on a follower, body: %s", deletedGroup.body())
        .isEqualTo(200);
    awaitOnEveryServer(s -> !hasGroup(s, group), "group " + group + " gone");

    final String token = uniqueName("issue8109forwardedtoken");
    final HttpResponse<String> minted = asRoot(follower, "POST", "/api/v1/server/api-tokens",
        new JSONObject().put("name", token).toString());
    assertThat(minted.statusCode()).as("POST /server/api-tokens on a follower, body: %s", minted.body())
        .isEqualTo(201);
    assertThat(new JSONObject(minted.body()).getJSONObject("result").getString("token", ""))
        .as("the plaintext token the leader minted must be relayed to the client").isNotBlank();
    awaitOnEveryServer(s -> tokenHashOf(s, token) != null, "token " + token + " present");

    final String tokenHash = tokenHashOf(getServer(leader).getSecurity(), token);
    final HttpResponse<String> revoked = asRoot(follower, "DELETE", "/api/v1/server/api-tokens?token=" + tokenHash,
        null);
    assertThat(revoked.statusCode()).as("DELETE /server/api-tokens on a follower, body: %s", revoked.body())
        .isEqualTo(200);
    awaitOnEveryServer(s -> tokenHashOf(s, token) == null, "token " + token + " gone");

    // openCypher user administration through the database command route of a follower.
    final String user = uniqueName("issue8109cypheruser");
    final HttpResponse<String> created = asRoot(follower, "POST", "/api/v1/command/" + getDatabaseName(),
        cypher("CREATE USER " + user + " SET PASSWORD '" + PASSWORD_8109 + "'"));
    assertThat(created.statusCode()).as("CREATE USER on a follower, body: %s", created.body()).isEqualTo(200);
    awaitOnEveryServer(s -> s.existsUser(user), "user " + user + " present");

    final String originalHash = getServer(leader).getSecurity().getUser(user).getPassword();
    final HttpResponse<String> altered = asRoot(follower, "POST", "/api/v1/command/" + getDatabaseName(),
        cypher("ALTER USER " + user + " SET PASSWORD '" + PASSWORD_8109 + "2'"));
    assertThat(altered.statusCode()).as("ALTER USER on a follower, body: %s", altered.body()).isEqualTo(200);
    awaitOnEveryServer(s -> !originalHash.equals(s.getUser(user).getPassword()), "user " + user + " password changed");

    final HttpResponse<String> dropped = asRoot(follower, "POST", "/api/v1/command/" + getDatabaseName(),
        cypher("DROP USER " + user));
    assertThat(dropped.statusCode()).as("DROP USER on a follower, body: %s", dropped.body()).isEqualTo(200);
    awaitOnEveryServer(s -> !s.existsUser(user), "user " + user + " gone");
  }

  // ------------------------------------------------------------------ SecurityManager entry points (#8370)

  /**
   * Issue #8370, from {@code Issue8370SecurityManagerUserMutationsLeaderOnlyIT}.
   * <p>
   * The three entry points called directly on a follower are refused with a not-the-leader error naming the leader, and
   * change nothing on any node. The same calls on the leader are the control: they succeed and replicate.
   */
  @Test
  @Timeout(180)
  void securityManagerUserMutationsAreRefusedOnAFollowerAndReplicatedFromTheLeader() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollowerOf(leader);
    final ServerSecurity followerSecurity = getServer(follower).getSecurity();
    final ServerSecurity leaderSecurity = getServer(leader).getSecurity();

    // Control: on the leader the entry point works and replicates to every node.
    final String existing = uniqueName("issue8370existing");
    leaderSecurity.createUser(existing, PASSWORD_8370);
    awaitUserOnEveryServer(existing, true);
    final String passwordHashBefore = leaderSecurity.getUser(existing).getPassword();

    final String never = uniqueName("issue8370nevercreated");
    assertRefusedOffLeader(() -> followerSecurity.createUser(never, PASSWORD_8370));
    assertRefusedOffLeader(() -> followerSecurity.setUserPassword(existing, PASSWORD_8370 + "2"));
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
    leaderSecurity.setUserPassword(existing, PASSWORD_8370 + "2");
    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServer(i).getSecurity().getUser(existing).getPassword())
            .as("server %d must apply the leader's rotation", i).isNotEqualTo(passwordHashBefore);
    });
    assertThat(leaderSecurity.dropUser(existing)).isTrue();
    awaitUserOnEveryServer(existing, false);
  }

  /**
   * Issue #8370, from {@code Issue8370SecurityManagerUserMutationsLeaderOnlyIT}.
   * <p>
   * The door the issue names: a JS function body reaches {@code database.getSecurity().createUser(...)}, and an
   * idempotent {@code SELECT} calling it on a follower is executed there, not forwarded. It must be refused.
   */
  @Test
  @Timeout(180)
  void hostCodeOnAFollowerCannotMutateTheUserDocument() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollowerOf(leader);

    final String user = uniqueName("issue8370hostcode");
    final String define = "DEFINE FUNCTION lib8370.mint \"database.getSecurity().createUser('" + user + "', '" + PASSWORD_8370
        + "'); return 1;\" LANGUAGE js";
    // Defined once, on the leader: DEFINE FUNCTION replicates like any other DDL since issue #8404.
    getServer(leader).getDatabaseForOperator(getDatabaseName()).command("sql", define);
    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() ->
        assertThat(getServer(follower).getDatabaseForOperator(getDatabaseName()).getSchema().hasFunctionLibrary("lib8370"))
            .as("the function must reach the follower").isTrue());

    assertThatThrownBy(() -> {
      try (final ResultSet rs = getServer(follower).getDatabaseForOperator(getDatabaseName())
          .query("sql", "SELECT `lib8370.mint`() AS r")) {
        rs.next();
      }
    }).as("host code on a follower must not mutate the user document").satisfies(e ->
        assertThat(messageChainOf(e)).contains("must run on the cluster leader"));

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServer(i).getSecurity().existsUser(user)).as("server %d must not hold the refused user", i)
          .isFalse();

    // Control: the same host code on the leader succeeds and replicates.
    try (final ResultSet rs = getServer(leader).getDatabaseForOperator(getDatabaseName())
        .query("sql", "SELECT `lib8370.mint`() AS r")) {
      rs.next();
    }
    awaitUserOnEveryServer(user, true);
    getServer(leader).getSecurity().dropUser(user);
    awaitUserOnEveryServer(user, false);
  }

  // ------------------------------------------------------------------ compare-and-set verdict on a follower (#7559)

  /** Issue #7559, from {@code Issue7559SecurityPreconditionOnFollowerIT}. */
  @Test
  void aFollowerEngagesTheCompareAndSetAndWithholdsItWithTheLeaderOnAMixedCluster() throws InterruptedException {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int laggingIndex = (leaderIndex + 1) % getServerCount();
    final int upgradedFollowerIndex = (leaderIndex + 2) % getServerCount();
    final String laggingPeerId = peerIdForIndex(laggingIndex);

    assertThat(raftServerOf(upgradedFollowerIndex).isLeader())
        .as("the node this test calls a follower must not be the leader")
        .isFalse();

    // ---- a uniform cluster: the check engages wherever the mutation was submitted -------------------------
    // This is the bug. The follower runs no capability monitor, so its registry names every peer as missing and
    // the precondition was dropped - silently, on exactly the routes that do NOT forward to the leader.
    awaitVerdict(leaderIndex, FINGERPRINT,
        "the leader writes the precondition on a cluster where every node can read one");
    awaitVerdict(upgradedFollowerIndex, FINGERPRINT,
        "a follower must reach the same answer, by asking its peers itself");

    // ---- a mixed cluster: both nodes withhold it, and withhold it together --------------------------------
    // Restored in a finally: the cluster is shared, so a failure here must not leave the node advertising an older build
    final RaftHAServer lagging = raftServerOf(laggingIndex);
    lagging.setAdvertisedCapabilities(withoutPrecondition(PeerCapabilities.LOCAL));
    try {
      awaitObserved(leaderIndex, laggingPeerId,
          "the leader must be told peer '" + laggingPeerId + "' cannot read a precondition");

      awaitVerdict(leaderIndex, null,
          "the leader withholds the precondition while one node predates the section");
      awaitVerdict(upgradedFollowerIndex, null,
          "and the follower reaches the SAME verdict, which is what stops the security state diverging");
    } finally {
      // ---- finish the rolling upgrade; no setting change, no restart, no operator step ---------------------
      lagging.setAdvertisedCapabilities(PeerCapabilities.LOCAL);
    }

    awaitVerdict(leaderIndex, FINGERPRINT, "the check resumes on the leader by itself");
    awaitVerdict(upgradedFollowerIndex, FINGERPRINT, "and on the follower by itself");
  }

  // ------------------------------------------------------------------ helpers of #7380 and #8109

  private static String cypher(final String command) {
    return new JSONObject().put("language", "opencypher").put("command", command).toString();
  }

  private static boolean hasGroup(final ServerSecurity security, final String name) {
    return security.groupsToJSON().getJSONObject("databases").getJSONObject("*", new JSONObject())
        .getJSONObject("groups", new JSONObject()).has(name);
  }

  private static String tokenHashOf(final ServerSecurity security, final String name) {
    for (final JSONObject token : security.getApiTokenConfiguration().listTokens())
      if (name.equals(token.getString("name", null)))
        return token.getString("tokenHash");
    return null;
  }

  // Sent with no intended leader id, as a peer on an older build does: nothing proves the address wrong, so the
  // refusal is the retryable 503 rather than the configuration error's 400 (issue #8393).
  private void assertRefusedAsAlreadyForwarded(final HttpResponse<String> response, final String route) {
    assertThat(response.statusCode())
        .as("%s arriving already forwarded must be refused on this follower, body: %s", route, response.body())
        .isEqualTo(503);
    assertThat(response.body()).as("%s refusal must say why", route).contains("already forwarded");
  }

  private void awaitOnEveryServer(final Predicate<ServerSecurity> condition, final String what) {
    await().alias(what).atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (!condition.test(getServer(i).getSecurity()))
          return false;
      return true;
    });
  }

  /** #7380 and #8370: waits until every node holds the user, or until none does. */
  private void awaitUserOnEveryServer(final String user, final boolean present) {
    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServer(i).getSecurity().existsUser(user)).as("user %s on server %d", user, i)
            .isEqualTo(present);
    });
  }

  /**
   * Whether these credentials authenticate on that node. {@code POST /api/v1/login} is the cheapest question that
   * actually checks the password: {@code existsUser} would answer true for a user whose password never changed.
   */
  private boolean canLogIn(final int serverIndex, final String user, final String password) throws Exception {
    return send(serverIndex, "POST", "/api/v1/login", null,
        b -> b.header("Authorization", basic(user, password))).statusCode() == 200;
  }

  private String passwordHashOn(final int serverIndex, final String name) {
    return getServer(serverIndex).getSecurity().getUser(name).getPassword();
  }

  private HttpResponse<String> asRoot(final int serverIndex, final String method, final String path,
      final String body) throws Exception {
    return send(serverIndex, method, path, body,
        b -> b.header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)));
  }

  /**
   * A request shaped the way a cluster peer sends a forwarded one: cluster-token authentication plus the marker that
   * says a peer already relayed it.
   */
  private HttpResponse<String> asPeer(final int serverIndex, final String clusterToken, final String method,
      final String path, final String body) throws Exception {
    return send(serverIndex, method, path, body, b -> b
        .header("X-ArcadeDB-Cluster-Token", clusterToken)
        .header("X-ArcadeDB-Forwarded-User", "root")
        .header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true"));
  }

  private HttpResponse<String> send(final int serverIndex, final String method, final String path,
      final String body, final UnaryOperator<HttpRequest.Builder> decorate) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + port + path))
        .timeout(Duration.ofSeconds(60));
    builder = decorate.apply(builder);
    if (body != null) {
      builder.header("Content-Type", "application/json");
      builder.method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    } else
      builder.method(method, HttpRequest.BodyPublishers.noBody());
    return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static String basic(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  // ------------------------------------------------------------------ helpers of #8370

  private static void assertRefusedOffLeader(final Runnable call) {
    assertThatThrownBy(call::run).isInstanceOf(ServerIsNotTheLeaderException.class)
        .hasMessageContaining("must run on the cluster leader");
  }

  private static String messageChainOf(final Throwable e) {
    final StringBuilder sb = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause())
      sb.append(t.getClass().getName()).append(": ").append(t.getMessage()).append('\n');
    return sb.toString();
  }

  // ------------------------------------------------------------------ helpers of #7559

  /** Everything this build advertises except the one token under test. */
  private static Set<String> withoutPrecondition(final Set<String> capabilities) {
    final Set<String> reduced = new HashSet<>(capabilities);
    reduced.remove(PeerCapabilities.SECURITY_PRECONDITION);
    return Set.copyOf(reduced);
  }

  /**
   * Waits until {@code serverIndex} decides {@code expected} about a submission carrying {@link #FINGERPRINT}.
   * <p>
   * The real decision, driven through the plugin method every security mutation funnels into, rather than a
   * re-implementation of it in the test: the whole defect was which accessor that method reads.
   */
  private void awaitVerdict(final int serverIndex, final String expected, final String what)
      throws InterruptedException {
    final RaftHAPlugin plugin = getRaftPlugin(serverIndex);
    assertThat(plugin).as("server %d runs the Raft HA plugin", serverIndex).isNotNull();

    final long deadline = System.currentTimeMillis() + VERDICT_TIMEOUT_MS;
    String verdict = plugin.preconditionEveryPeerCanRead(FINGERPRINT);
    while (!Objects.equals(expected, verdict) && System.currentTimeMillis() < deadline) {
      Thread.sleep(500);
      verdict = plugin.preconditionEveryPeerCanRead(FINGERPRINT);
    }

    assertThat(verdict).as(what).isEqualTo(expected);
  }

  /**
   * Waits until the LEADER has actually probed {@code peerId} and recorded an advertisement without the precondition
   * token.
   * <p>
   * Deliberately not "until the leader reports the peer as missing the capability": a peer that was never successfully
   * probed reports exactly the same way, so a run in which the capability RPC was broken end to end would satisfy that
   * weaker condition instantly and the withholding below would pass for the wrong reason.
   */
  private void awaitObserved(final int observerIndex, final String peerId, final String what)
      throws InterruptedException {
    final RaftHAServer observer = raftServerOf(observerIndex);
    final Predicate<Set<String>> settled =
        capabilities -> capabilities != null && !capabilities.contains(PeerCapabilities.SECURITY_PRECONDITION);

    final long deadline = System.currentTimeMillis() + CAPABILITY_CONVERGENCE_TIMEOUT_MS;
    Set<String> observed = observedCapabilities(observer, peerId);
    while (!settled.test(observed) && observer.isLeader() && System.currentTimeMillis() < deadline) {
      Thread.sleep(200);
      observed = observedCapabilities(observer, peerId);
    }

    // This test pins the leader whose capability registry it reads: an election mid-test changes which node's view
    // the assertion is about, which would read here as "the negotiation is broken" and cost the next reader an hour.
    // So an election is a failed PRECONDITION and says so.
    assertThat(observer.isLeader())
        .as("this test pins the leader it elected at the start; %s lost leadership mid-test, so the capability "
            + "refresh it drives stopped", observer.getLocalPeerId())
        .isTrue();
    assertThat(settled.test(observed))
        .as(what + " (last observed advertisement: %s)", observed)
        .isTrue();
  }

  private static Set<String> observedCapabilities(final RaftHAServer observer, final String peerId) {
    final PeerCapabilityRegistry.Advertisement advertisement =
        observer.getPeerCapabilityRegistry().freshAdvertisementOf(peerId);
    return advertisement != null ? advertisement.capabilities() : null;
  }

  private RaftHAServer raftServerOf(final int serverIndex) {
    final RaftHAPlugin plugin = getRaftPlugin(serverIndex);
    assertThat(plugin).as("server %d runs the Raft HA plugin", serverIndex).isNotNull();
    assertThat(plugin.getRaftHAServer()).as("server %d has started its Raft server", serverIndex).isNotNull();
    return plugin.getRaftHAServer();
  }
}
