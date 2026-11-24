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
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The payload of a replicated entry travels inside a Ratis log entry (term, index, client id, call id, type and the
 * length prefixes), and it is that wrapped entry which the log measures against the appender byte limit. The size
 * the replication layer allows for a payload has to leave room for the wrapping, or a payload just under the limit
 * passes the local check and is refused by the log.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ReplicatedEntryFramingHeadroomTest {

  /** The size Ratis measures for an entry carrying {@code payloadSize} bytes, with the widest values of every varint field. */
  private static long wrappedSize(final long payloadSize) {
    final StateMachineLogEntryProto stateMachineEntry = LogProtoUtils.toStateMachineLogEntryProto(ClientId.randomId(),
        Long.MAX_VALUE, StateMachineLogEntryProto.Type.WRITE, ByteString.copyFrom(new byte[(int) payloadSize]), null);
    return LogProtoUtils.toLogEntryProto(stateMachineEntry, Long.MAX_VALUE, Long.MAX_VALUE).getSerializedSize();
  }

  private static ContextConfiguration configuration(final String appendBufferSize) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, appendBufferSize);
    return configuration;
  }

  private static long appenderLimit(final ContextConfiguration configuration) {
    return RaftServerConfigKeys.Log.Appender.bufferByteLimit(RaftPropertiesBuilder.build(configuration)).getSize();
  }

  @ParameterizedTest
  @ValueSource(strings = { "16KB", "64KB", "1MB", "4MB", "32MB" })
  void aPayloadAtTheReplicatedCapFitsTheAppenderLimitOnceWrapped(final String appendBufferSize) {
    final ContextConfiguration configuration = configuration(appendBufferSize);
    final long limit = appenderLimit(configuration);
    final long cap = RaftPropertiesBuilder.maxReplicatedEntrySize(configuration);

    assertThat(wrappedSize(cap)).as("the wrapped entry of a payload at the cap must not exceed the %d bytes the log accepts", limit)
        .isLessThanOrEqualTo(limit);
    assertThat(limit - wrappedSize(cap)).as("the room kept for the wrapping stays small next to the limit")
        .isLessThan(GlobalConfiguration.REPLICATED_ENTRY_FRAMING_BYTES);
  }

  @Test
  void theGroupCommitterRefusesAPayloadTheLogWouldRefuseOnceWrapped() {
    final ContextConfiguration configuration = configuration("1MB");
    final long limit = appenderLimit(configuration);
    final long cap = RaftPropertiesBuilder.maxReplicatedEntrySize(configuration);
    final int justUnderTheLimit = (int) limit - 1;
    assertThat(wrappedSize(justUnderTheLimit)).as("this payload is refused by the log once wrapped").isGreaterThan(limit);

    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500, 500, 10_000, 100, cap, null);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[justUnderTheLimit]))
          .isInstanceOf(ReplicatedEntryTooLargeException.class).isNotInstanceOf(NeedRetryException.class);
      assertThatThrownBy(() -> committer.submitAndWait(new byte[(int) cap + 1]))
          .isInstanceOf(ReplicatedEntryTooLargeException.class);

      // The cap itself is accepted: it gets past the size check and fails on the missing client like any dispatched entry.
      assertThatThrownBy(() -> committer.submitAndWait(new byte[(int) cap]))
          .isNotInstanceOf(ReplicatedEntryTooLargeException.class);
    } finally {
      committer.stop();
    }
  }

  @Test
  void theCapStaysBelowTheTransportLimitWhenThatIsTheSmallerOne() {
    final ContextConfiguration configuration = configuration("32MB");
    configuration.setValue(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX, 2L * 1024 * 1024);

    assertThat(RaftPropertiesBuilder.maxReplicatedEntrySize(configuration))
        .isEqualTo(2L * 1024 * 1024 - GlobalConfiguration.REPLICATED_ENTRY_FRAMING_BYTES);
  }

  @Test
  void aLimitTooSmallToSpareTheWrappingKeepsHalfOfItForThePayload() {
    assertThat(GlobalConfiguration.replicatedPayloadLimit(0L)).isZero();
    assertThat(GlobalConfiguration.replicatedPayloadLimit(100L)).isEqualTo(50L);
    assertThat(GlobalConfiguration.replicatedPayloadLimit(2L * GlobalConfiguration.REPLICATED_ENTRY_FRAMING_BYTES))
        .isEqualTo(GlobalConfiguration.REPLICATED_ENTRY_FRAMING_BYTES);
    assertThat(GlobalConfiguration.replicatedPayloadLimit(32L * 1024 * 1024))
        .isEqualTo(32L * 1024 * 1024 - GlobalConfiguration.REPLICATED_ENTRY_FRAMING_BYTES);
  }

  @Test
  void theLeaderRefusesAnEntryAboveItsOwnAppenderLimitBeforeAppendingIt() throws Exception {
    final long limit = 64L * 1024;
    final ArcadeStateMachine stateMachine = new ArcadeStateMachine() {
      @Override
      long appenderByteLimit() {
        return limit;
      }
    };
    final int payloadLimit = (int) GlobalConfiguration.replicatedPayloadLimit(limit);

    assertThat(wrappedSize(payloadLimit)).isLessThanOrEqualTo(limit);
    assertThat(stateMachine.startTransaction(request(entry(payloadLimit))).getException()).isNull();

    final Exception refusal = stateMachine.startTransaction(request(entry(payloadLimit + 1))).getException();
    assertThat(refusal).isInstanceOf(ReplicatedEntryTooLargeException.class).isNotInstanceOf(NeedRetryException.class);
    assertThat(refusal.getMessage()).contains(String.valueOf(payloadLimit + 1)).contains(String.valueOf(payloadLimit));

    // An entry in the window the wrapping used to hide is refused too.
    assertThat(stateMachine.startTransaction(request(entry((int) limit - 1))).getException())
        .isInstanceOf(ReplicatedEntryTooLargeException.class);
  }

  @Test
  void aStateMachineWithoutALogRefusesNothingOnSize() throws Exception {
    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();

    assertThat(stateMachine.startTransaction(request(entry(256 * 1024))).getException()).isNull();
  }

  @Test
  void aSizeRefusalFromTheLeaderIsADefiniteOutcomeForTheCaller() {
    final ReplicatedEntryTooLargeException refusal = new ReplicatedEntryTooLargeException("Replicated entry size 70000 bytes ...");

    assertThat(RaftGroupCommitter.refusedAsTooLarge(new StateMachineException("refused", refusal, false))).isSameAs(refusal);
    assertThat(RaftGroupCommitter.refusedAsTooLarge(new StateMachineException("refused", new IllegalStateException("x"), false)))
        .isNull();
    assertThat(RaftGroupCommitter.refusedAsTooLarge(null)).isNull();
  }

  private static ByteString entry(final int size) {
    final byte[] bytes = new byte[size];
    bytes[0] = RaftLogEntryType.SECURITY_USERS_ENTRY.getId();
    return ByteString.copyFrom(bytes);
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
