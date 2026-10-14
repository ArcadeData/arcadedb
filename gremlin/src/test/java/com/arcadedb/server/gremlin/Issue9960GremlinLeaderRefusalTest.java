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
package com.arcadedb.server.gremlin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9960, the Gremlin Server member of the #9845 family: a follower's refusal of leader-only work was answered with
 * the refusal's bare message, which carries no address, and in production mode with the concealment placeholder, so a
 * client could tell neither where the leader is nor that a leader problem happened at all. The answer now names the
 * leader in both modes, after the placeholder in production, as every other transport does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9960GremlinLeaderRefusalTest {
  private static final String LEADER   = "arcadedb-0:2480";
  private static final String MESSAGE  = "Changes to the schema must be executed on the leader server";
  private static final String SENTENCE = "The leader is server '" + LEADER + "'";

  @TempDir
  Path root;

  /** What TinkerPop writes for a failed evaluation: the top-level message and the class of every throwable in the chain. */
  private static ResponseMessage failure(final Throwable error) {
    return ResponseMessage.build(UUID.randomUUID()).code(ResponseStatusCode.SERVER_ERROR_EVALUATION).statusMessage(error.getMessage())
        .statusAttributeException(error).create();
  }

  private static Throwable refusal() {
    return new TransactionException("commit", new ServerIsNotTheLeaderException(MESSAGE, LEADER));
  }

  @Test
  void aRefusalAnywhereAlongTheChainIsRecognised() {
    assertThat(ConcealingWebSocketChannelizer.isLeaderRefusal(failure(refusal()))).isTrue();
    assertThat(ConcealingWebSocketChannelizer.isLeaderRefusal(failure(new ConcurrentModificationException("conflict")))).isFalse();
    assertThat(ConcealingWebSocketChannelizer.isLeaderRefusal(
        ResponseMessage.build(UUID.randomUUID()).code(ResponseStatusCode.SERVER_ERROR).statusMessage("no attributes").create())).isFalse();
  }

  @Test
  void productionModeNamesTheLeaderAfterThePlaceholder() {
    final ResponseMessage original = failure(refusal());
    final ResponseMessage answer = ConcealingWebSocketChannelizer.conceal(true, LEADER, original);

    assertThat(answer.getStatus().getMessage()).startsWith(ArcadeDBServer.CONCEALED_ERROR_MESSAGE + ". " + SENTENCE);
    assertThat(answer.getStatus().getCode()).isEqualTo(ResponseStatusCode.SERVER_ERROR_EVALUATION);
    assertThat(answer.getRequestId()).isEqualTo(original.getRequestId());
    assertThat(answer.getStatus().getAttributes()).as("no exception class or stack trace").isEmpty();
  }

  @Test
  void developmentModeNamesTheLeaderAfterTheMessage() {
    final ResponseMessage original = failure(refusal());
    final ResponseMessage answer = ConcealingWebSocketChannelizer.conceal(false, LEADER, original);

    assertThat(answer.getStatus().getMessage()).startsWith(original.getStatus().getMessage() + ". " + SENTENCE);
    assertThat(answer.getRequestId()).isEqualTo(original.getRequestId());
    assertThat(answer.getStatus().getAttributes()).isEqualTo(original.getStatus().getAttributes());
  }

  @Test
  void theAddressCannotForgeTheRestOfTheText() {
    final ResponseMessage answer = ConcealingWebSocketChannelizer.conceal(true, "host\r\nforged", failure(refusal()));

    assertThat(answer.getStatus().getMessage()).doesNotContain("\r").doesNotContain("\n");
  }

  @Test
  void withoutALeaderTheAnswerIsUnchanged() {
    assertThat(ConcealingWebSocketChannelizer.leaderAddress(null)).isNull();

    final ResponseMessage original = failure(refusal());
    assertThat(ConcealingWebSocketChannelizer.conceal(false, null, original)).isSameAs(original);
    assertThat(ConcealingWebSocketChannelizer.conceal(true, null, original).getStatus().getMessage())
        .isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  /** A server outside a cluster has no leader to name: the refusal arm stays out of every ordinary answer. */
  @Test
  void aServerWithoutAClusterAnswersAsBefore() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, "production");
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, configuration);

    assertThat(ConcealingWebSocketChannelizer.conceal(server, failure(refusal())).getStatus().getMessage())
        .isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }
}
