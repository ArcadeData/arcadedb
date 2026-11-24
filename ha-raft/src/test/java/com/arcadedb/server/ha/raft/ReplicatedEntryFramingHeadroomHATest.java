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
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An entry whose payload sits just under the appender byte limit is above it once Ratis has wrapped it. On a running
 * cluster the entry has to be refused where it is submitted, and the leader keeps its role and its term.
 * <p>
 * The limit is set only in the server configuration, as an operator sets it, and is small so the test needs no large
 * allocation.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ReplicatedEntryFramingHeadroomHATest extends BaseRaftHATest {

  private static final long APPENDER_LIMIT = 256L * 1024;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, APPENDER_LIMIT + "");
    config.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, 2 * APPENDER_LIMIT + "");
  }

  @Test
  void anEntryJustUnderTheAppenderLimitIsRefusedAndTheLeaderKeepsItsRole() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);
    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final long term = leader.getCurrentTerm();
    final String databaseName = getDatabaseName();

    assertThat(leader.getStateMachine().appenderByteLimit()).as("the state machine reads the limit the Raft server was built with")
        .isEqualTo(APPENDER_LIMIT);

    final String changes = configurationOfSize(databaseName, (int) APPENDER_LIMIT - 20);
    assertThat(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(databaseName, changes).size()).isEqualTo((int) APPENDER_LIMIT - 20);

    assertThatThrownBy(() -> leader.getTransactionBroker().replicateDatabaseConfiguration(databaseName, changes))
        .isInstanceOf(ReplicatedEntryTooLargeException.class);

    assertThat(leader.isLeader()).as("the leader keeps its role").isTrue();
    assertThat(leader.getCurrentTerm()).as("and its term: no election was needed").isEqualTo(term);

    final Database database = getServerDatabase(leaderIndex, databaseName);
    database.getSchema().createDocumentType("AfterTheRefusedEntry");
    final int followerIndex = 1 - leaderIndex;
    assertThat(awaitReplicationIsCompleted(followerIndex)).as("the follower applies the leader's entries").isTrue();
    assertThat(getServerDatabase(followerIndex, databaseName).getSchema().existsType("AfterTheRefusedEntry"))
        .as("a write after the refusal still replicates").isTrue();
  }

  @Test
  void theLeaderRefusesAnEntryAboveItsOwnLimitBeforeAppendingIt() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);
    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final long term = leader.getCurrentTerm();
    final String databaseName = getDatabaseName();

    final long payloadLimit = GlobalConfiguration.replicatedPayloadLimit(APPENDER_LIMIT);
    final RaftClientRequest atTheLimit = request(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(databaseName,
        configurationOfSize(databaseName, (int) payloadLimit)));
    final RaftClientRequest aboveTheLimit = request(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(databaseName,
        configurationOfSize(databaseName, (int) payloadLimit + 1)));

    assertThat(leader.getStateMachine().startTransaction(atTheLimit).getException()).isNull();
    assertThat(leader.getStateMachine().startTransaction(aboveTheLimit).getException())
        .isInstanceOf(ReplicatedEntryTooLargeException.class);

    assertThat(leader.isLeader()).isTrue();
    assertThat(leader.getCurrentTerm()).isEqualTo(term);
  }

  /**
   * A submitter configured with a larger limit than the leader's: its own size check lets the entry through, and the leader's
   * refusal reaches the caller as the same definite, non-retryable answer, with the leader's role and term untouched.
   */
  @Test
  void anEntryAboveTheLeadersLimitFromASubmitterWithALargerLimitIsRefusedByTheLeader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);
    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final long term = leader.getCurrentTerm();
    final String databaseName = getDatabaseName();

    final ByteString entry = RaftLogEntryCodec.encodeDatabaseConfigurationEntry(databaseName,
        configurationOfSize(databaseName, (int) APPENDER_LIMIT - 20));
    final RaftGroupCommitter submitterWithALargerLimit = new RaftGroupCommitter(leader.getClient(), leader.getQuorum(),
        leader.getQuorumTimeout(), 500, 10_000, 100, 4 * APPENDER_LIMIT, null);
    try {
      assertThatThrownBy(() -> submitterWithALargerLimit.submitAndWait(entry.toByteArray()))
          .isInstanceOf(ReplicatedEntryTooLargeException.class).hasMessageContaining("accepted by this node");
    } finally {
      submitterWithALargerLimit.stop();
    }

    assertThat(leader.isLeader()).isTrue();
    assertThat(leader.getCurrentTerm()).isEqualTo(term);
  }

  /** A configuration document padded so that the encoded entry for {@code databaseName} is {@code entrySize} bytes. */
  private static String configurationOfSize(final String databaseName, final int entrySize) {
    final int overhead = 1 + 2 + databaseName.length() + 4;
    return "x".repeat(entrySize - overhead);
  }

  private static RaftClientRequest request(final ByteString entry) {
    return RaftClientRequest.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(RaftPeerId.valueOf("peer-0"))
        .setGroupId(RaftGroupId.randomId())
        .setCallId(1L)
        .setMessage(Message.valueOf(entry))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }
}
