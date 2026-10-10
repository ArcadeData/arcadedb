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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.ratis.protocol.RaftPeer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9589, against real Ratis: the security half of #9510. A node removed from the Raft configuration keeps an
 * open division that receives no more appends, while its Raft client still reaches the leader, which accepts an entry
 * from a non-member. Before the fix a user created (or a group saved, or an API token minted) on such a node was
 * committed on every member of the cluster and never applied on the node that answered the request. It is now refused,
 * retryably, and nothing reaches the log.
 * <p>
 * Each test then commits a marker change from a member and waits for it everywhere, so "the refused change is absent"
 * is read after a later entry has been applied, not merely before the refused one could have arrived.
 */
@Tag("slow")
class Issue9589RemovedNodeRefusesSecurityMutationsIT extends BaseRaftHATest {

  private static final String PASSWORD = "ThisIsAStrongPassword1!";

  /** The server that left the cluster: excluded from the end-of-test comparison, it diverges by design. */
  private volatile int leftIndex = -1;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int[] getServerToCheck() {
    return serversMatching(i -> i != leftIndex && getServer(i) != null && getServer(i).isStarted());
  }

  @Test
  void aUserCreatedOnARemovedNodeIsRefusedAndReachesNoMember() {
    final int removed = leaveFromAFollower();

    assertThatThrownBy(() -> security(removed).createUserClusterWide(user("issue9589refused")))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member");
    assertThat(security(removed).existsUser("issue9589refused")).isFalse();

    final int member = getServerToCheck()[0];
    security(member).createUserClusterWide(user("issue9589marker"));
    for (final int index : getServerToCheck()) {
      awaitOn(index, () -> security(index).existsUser("issue9589marker"));
      assertThat(security(index).existsUser("issue9589refused")).as("refused user on server %d", index).isFalse();
    }
  }

  @Test
  void aGroupSavedOnARemovedNodeIsRefusedAndReachesNoMember() {
    final int removed = leaveFromAFollower();

    assertThatThrownBy(() -> security(removed).saveGroupClusterWide("*", "issue9589refused", readerGroup()))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member");
    assertThat(hasGroup(removed, "issue9589refused")).isFalse();

    final int member = getServerToCheck()[0];
    security(member).saveGroupClusterWide("*", "issue9589marker", readerGroup());
    for (final int index : getServerToCheck()) {
      awaitOn(index, () -> hasGroup(index, "issue9589marker"));
      assertThat(hasGroup(index, "issue9589refused")).as("refused group on server %d", index).isFalse();
    }
  }

  @Test
  void anApiTokenMintedOnARemovedNodeIsRefusedAndReachesNoMember() {
    final int removed = leaveFromAFollower();

    assertThatThrownBy(() -> security(removed).createApiTokenClusterWide("issue9589refused", "*", 0, new JSONObject()))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member");
    assertThat(hasToken(removed, "issue9589refused")).isFalse();

    final int member = getServerToCheck()[0];
    security(member).createApiTokenClusterWide("issue9589marker", "*", 0, new JSONObject());
    for (final int index : getServerToCheck()) {
      awaitOn(index, () -> hasToken(index, "issue9589marker"));
      assertThat(hasToken(index, "issue9589refused")).as("refused token on server %d", index).isFalse();
    }
  }

  /** Removes a follower from the configuration and waits until its own division has seen the removal. */
  private int leaveFromAFollower() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int index = (leaderIndex + 1) % getServerCount();
    leftIndex = index;
    final RaftHAServer raft = getRaftPlugin(index).getRaftHAServer();
    final String peerId = raft.getLocalPeerId().toString();
    raft.leaveCluster(false);
    Awaitility.await("server " + index + " sees its own removal from the Raft configuration")
        .atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> !containsPeer(raft.getCommittedPeersOrNull(), peerId) && raft.isRemovedFromConfiguration());
    return index;
  }

  private ServerSecurity security(final int index) {
    return getServer(index).getSecurity();
  }

  private boolean hasGroup(final int index, final String group) {
    final JSONObject databases = new JSONObject(security(index).getGroupsJsonPayload()).getJSONObject("databases",
        new JSONObject());
    final JSONObject any = databases.getJSONObject("*", new JSONObject());
    return any.getJSONObject("groups", new JSONObject()).has(group);
  }

  private boolean hasToken(final int index, final String name) {
    final JSONArray tokens = new JSONObject(security(index).getApiTokensJsonPayload()).getJSONArray("tokens",
        new JSONArray());
    for (int i = 0; i < tokens.length(); i++)
      if (name.equals(tokens.getJSONObject(i).getString("name", null)))
        return true;
    return false;
  }

  private static void awaitOn(final int index, final Callable<Boolean> condition) {
    Awaitility.await("the marker change is applied on server " + index).atMost(30, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS).until(condition);
  }

  private static boolean containsPeer(final Collection<RaftPeer> peers, final String peerId) {
    if (peers == null)
      return true;
    for (final RaftPeer peer : peers)
      if (peer.getId().toString().equals(peerId))
        return true;
    return false;
  }

  private static JSONObject user(final String name) {
    return new JSONObject().put("name", name).put("password", PASSWORD).put("databases", new JSONObject());
  }

  private static JSONObject readerGroup() {
    return new JSONObject()
        .put("access", new JSONArray().put("readRecord"))
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))));
  }
}
