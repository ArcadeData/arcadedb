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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ReplicatedUsersPersistenceException;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8317: a node removed from the cluster and re-added with its config volume retained still holds a recorded
 * replicated fingerprint for every security document from its PREVIOUS membership, so a gate that asked only
 * "was this document ever installed here" released it on the first probe. Convergence on a runtime joiner is now
 * measured from the configuration that (last) added it: a document counts only when an entry after that index
 * installed it.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8317ReAddedNodeSecurityConvergenceTest {

  private static final RaftPeerId SELF        = RaftPeerId.valueOf("arcadedb-3");
  private static final String     USERS_JSON  = "[{\"name\":\"root\",\"password\":\"x\"}]";
  private static final String     GROUPS_JSON = "{\"version\":1,\"databases\":{}}";
  private static final String     TOKENS_JSON = "{\"tokens\":[]}";

  /** The issue's scenario: joined, converged, removed, re-added. What it installed the first time no longer counts. */
  @Test
  void aReAddedNodeDoesNotCountTheInstallsOfItsPreviousMembership() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1"), List.of(), 1);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), peers("arcadedb-0", "arcadedb-1"), 10);
    installAll(detector, 12);
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).as("converged after the first join").isEmpty();

    // Removed while it was down, then re-added by KubernetesAutoJoin's startup self-check or 'connect cluster'.
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1"), peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), 20);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1"), List.of(), 21);
    assertThat(detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1"), 30)).as("the re-add moves the join forward").isTrue();

    assertThat(detector.securityDocumentsNotInstalledSinceJoin())
        .as("the fingerprints on its retained volume are from the previous membership")
        .containsExactly("users", "groups", "API tokens");

    // The re-admission seed lands.
    installAll(detector, 32);
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
  }

  /** The same, when the re-add is observed only as a final configuration following one without this node. */
  @Test
  void aReAddObservedAsAFinalConfigurationAlsoMovesTheJoin() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"), 10);
    installAll(detector, 11);
    detector.onConfiguration(SELF, peers("arcadedb-0"), List.of(), 20);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), List.of(), 40);

    assertThat(detector.joinIndex()).isEqualTo(40L);
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  /** Each document converges on its own: a 'create user' after the re-add converges users, not groups or tokens. */
  @Test
  void eachDocumentConvergesOnItsOwn() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"), 10);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, 5);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, 11);

    assertThat(detector.securityDocumentsNotInstalledSinceJoin())
        .as("groups were installed BEFORE the join, users after it").containsExactly("groups", "API tokens");
  }

  /** An install at the join's own index is not after it. */
  @Test
  void anInstallAtTheJoinIndexIsNotAfterIt() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"), 10);
    installAll(detector, 10);

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  /**
   * The snapshot-install callback can deliver a configuration older than one the apply loop already did. It must
   * not pull the join back, or installs from before the re-add would count again.
   */
  @Test
  void anOlderConfigurationNeverMovesTheJoinBack() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"), 30);
    assertThat(detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"), 10)).isFalse();

    assertThat(detector.joinIndex()).isEqualTo(30L);
  }

  /** A node that did not join at runtime is not armed, so nothing is awaited on it whatever it installed. */
  @Test
  void aStaticMemberAwaitsNothing() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), List.of(), 1);

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
  }

  /**
   * The wiring: the three security applies of the state machine report the entry index to the detector it was
   * given, so an install from the log after the join converges it and one before it does not.
   */
  @Test
  void theStateMachineReportsEachInstallWithItsIndex(@TempDir final Path databaseDirectory) throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.setServer(serverWith(mock(ServerSecurity.class), databaseDirectory));
      final RuntimeJoinDetector detector = new RuntimeJoinDetector();
      sm.setRuntimeJoinDetector(detector);

      // From the previous membership, before the re-add at index 10.
      apply(sm, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON), 5);
      apply(sm, RaftLogEntryCodec.encodeSecurityGroupsEntry(GROUPS_JSON), 6);
      apply(sm, RaftLogEntryCodec.encodeSecurityApiTokensEntry(TOKENS_JSON), 7);
      detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"), 10);
      assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");

      apply(sm, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON), 11);
      assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("groups", "API tokens");
      apply(sm, RaftLogEntryCodec.encodeSecurityGroupsEntry(GROUPS_JSON), 12);
      apply(sm, RaftLogEntryCodec.encodeSecurityApiTokensEntry(TOKENS_JSON), 13);
      assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
    } finally {
      sm.close();
    }
  }

  /**
   * A conditional entry whose precondition no longer held installed nothing (issue #7509), so it is not
   * convergence.
   */
  @Test
  void aSupersededEntryIsNotAnInstall(@TempDir final Path databaseDirectory) throws Exception {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.applyReplicatedUsers(anyString(), anyString())).thenReturn(false);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.setServer(serverWith(security, databaseDirectory));
      final RuntimeJoinDetector detector = new RuntimeJoinDetector();
      sm.setRuntimeJoinDetector(detector);
      detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"), 10);

      apply(sm, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON, "stale-fingerprint"), 11);

      assertThat(detector.securityDocumentsNotInstalledSinceJoin()).contains("users");
    } finally {
      sm.close();
    }
  }

  /**
   * A document whose local write failed is still in force in memory (issue #7137), so it is what this node
   * enforces: it counts, exactly as the replicated fingerprint does.
   */
  @Test
  void aDocumentInForceInMemoryWhoseWriteFailedIsAnInstall(@TempDir final Path databaseDirectory) throws Exception {
    final ServerSecurity security = mock(ServerSecurity.class);
    doThrow(new ReplicatedUsersPersistenceException("applied in memory, not persisted",
        new IOException("No space left on device"))).when(security).applyReplicatedUsers(anyString());
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.setServer(serverWith(security, databaseDirectory));
      final RuntimeJoinDetector detector = new RuntimeJoinDetector();
      sm.setRuntimeJoinDetector(detector);
      detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"), 10);

      apply(sm, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON), 11);

      assertThat(detector.securityDocumentsNotInstalledSinceJoin()).doesNotContain("users");
    } finally {
      sm.close();
    }
  }

  /**
   * The plugin answers "nothing converged" when its Raft server is not readable: the gate asks only after the
   * join was read as true, and a Raft server gone between the two reads is no evidence of convergence.
   */
  @Test
  void aPluginWithoutARaftServerReportsEveryDocumentAsAwaited() {
    assertThat(new RaftHAPlugin().securityDocumentsNotInstalledSinceRuntimeJoin())
        .containsExactly("users", "groups", "API tokens");
  }

  // -----------------------------------------------------------------------------------------------------------

  private static void installAll(final RuntimeJoinDetector detector, final long index) {
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, index);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, index);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, index);
  }

  private static void apply(final ArcadeStateMachine sm, final ByteString payload, final long index) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(1L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    sm.applyTransaction(TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build());
  }

  private static ArcadeDBServer serverWith(final ServerSecurity security, final Path databaseDirectory) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDirectory.toString());

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getSecurity()).thenReturn(security);
    when(server.getConfiguration()).thenReturn(configuration);
    return server;
  }

  private static List<RaftPeerId> peers(final String... ids) {
    final List<RaftPeerId> peers = new ArrayList<>(ids.length);
    for (final String id : ids)
      peers.add(RaftPeerId.valueOf(id));
    return peers;
  }
}
