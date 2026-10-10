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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9589: the security entries take the route #9510 closed for transactions, drops and installs. A node removed
 * from the Raft configuration still reaches the leader through its Raft client, the leader accepts the entry from a
 * non-member and commits it on every member, and this node's own division never receives it. A user created there is
 * therefore created everywhere except on the node that answered the request.
 * <p>
 * {@code RaftHAPlugin.replicateSecurityUsers/Groups/ApiTokens} is the single chokepoint every security mutation and
 * every seed ends at, so the refusal lives there and is pinned through it, for each document and for both the
 * fingerprinted (user-initiated) and the bare (seed) form. The live-cluster half is
 * {@code Issue9589RemovedNodeRefusesSecurityMutationsIT}.
 */
class Issue9589SecurityEntryRefusedWhileRemovedFromConfigurationTest {

  private static final String FINGERPRINT = "7f".repeat(32);
  private static final String USERS       = "[{\"name\":\"root\"}]";
  private static final String GROUPS      = "{\"databases\":{}}";
  private static final String API_TOKENS  = "{\"version\":1,\"tokens\":[]}";

  @Test
  void aUsersEntryIsRefusedRetryablyAndNothingIsSubmitted() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(removedNode(broker));

    assertThatThrownBy(() -> plugin.replicateSecurityUsers(USERS, FINGERPRINT))
        .isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member of the Raft cluster's configuration");
    assertThat(broker.calls("replicateSecurityUsers")).isEmpty();
  }

  @Test
  void aGroupsEntryIsRefusedRetryablyAndNothingIsSubmitted() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(removedNode(broker));

    assertThatThrownBy(() -> plugin.replicateSecurityGroups(GROUPS, FINGERPRINT)).isInstanceOf(NeedRetryException.class);
    assertThat(broker.calls("replicateSecurityGroups")).isEmpty();
  }

  @Test
  void anApiTokensEntryIsRefusedRetryablyAndNothingIsSubmitted() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(removedNode(broker));

    assertThatThrownBy(() -> plugin.replicateSecurityApiTokens(API_TOKENS, FINGERPRINT))
        .isInstanceOf(NeedRetryException.class);
    assertThat(broker.calls("replicateSecurityApiTokens")).isEmpty();
  }

  /**
   * The seed form (no fingerprint) is refused too, deliberately. Since issues #7531 and #7834 a seed runs only on the
   * leader's {@code MembershipSecuritySeeder}, which the joining node never is, so the refusal cannot reach a join; on a
   * removed node it would commit documents everywhere but here, exactly like a user-initiated change.
   * {@code ServerSecurity.seedSecurityStateClusterWide} reports a refused document as failed and retries it.
   */
  @Test
  void theSeedFormIsRefusedTooForEveryDocument() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(removedNode(broker));

    assertThatThrownBy(() -> plugin.replicateSecurityUsers(USERS)).isInstanceOf(NeedRetryException.class);
    assertThatThrownBy(() -> plugin.replicateSecurityGroups(GROUPS)).isInstanceOf(NeedRetryException.class);
    assertThatThrownBy(() -> plugin.replicateSecurityApiTokens(API_TOKENS)).isInstanceOf(NeedRetryException.class);
    assertThat(broker.calls("replicateSecurityUsers")).isEmpty();
    assertThat(broker.calls("replicateSecurityGroups")).isEmpty();
    assertThat(broker.calls("replicateSecurityApiTokens")).isEmpty();
  }

  /**
   * Refused before the #7511 capability gate, which may dial every peer: a node outside the configuration must not
   * pay for a probe round on a submission it is going to refuse anyway.
   */
  @Test
  void theRefusalComesBeforeTheCapabilityProbe() {
    final FakeRaftHAServer raft = removedNode(new FakeRaftTransactionBroker());
    final RaftHAPlugin plugin = pluginOn(raft);

    assertThatThrownBy(() -> plugin.replicateSecurityGroups(GROUPS, FINGERPRINT)).isInstanceOf(NeedRetryException.class);
    assertThat(raft.calls("peersMissingCapabilityNow")).isEmpty();
  }

  /** The other direction: a member submits exactly as before. */
  @Test
  void aMemberStillSubmitsEveryDocument() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().removedFromConfiguration(false);
    raft.transactionBroker(broker);
    raft.returns("peersMissingCapabilityNow", List.of());
    final RaftHAPlugin plugin = pluginOn(raft);

    plugin.replicateSecurityUsers(USERS);
    plugin.replicateSecurityGroups(GROUPS);
    plugin.replicateSecurityApiTokens(API_TOKENS);

    assertThat(broker.calls("replicateSecurityUsers")).containsOnlyOnce(Arrays.asList(USERS, null));
    assertThat(broker.calls("replicateSecurityGroups")).containsOnlyOnce(Arrays.asList(GROUPS, null));
    assertThat(broker.calls("replicateSecurityApiTokens")).containsOnlyOnce(Arrays.asList(API_TOKENS, null));
  }

  private static FakeRaftHAServer removedNode(final RaftTransactionBroker broker) {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().removedFromConfiguration(true);
    raft.transactionBroker(broker);
    raft.returns("peersMissingCapabilityNow", List.of());
    return raft;
  }

  private static RaftHAPlugin pluginOn(final RaftHAServer raft) {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, true);
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, configuration);
    plugin.configure(server, configuration);
    plugin.setRaftHAServer(raft);
    return plugin;
  }
}
