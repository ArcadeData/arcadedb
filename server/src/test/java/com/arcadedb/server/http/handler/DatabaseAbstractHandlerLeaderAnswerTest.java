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
package com.arcadedb.server.http.handler;

import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.server.ForwardedRequestIdContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A request that forwarded one write to the leader and then failed on this node is answered with the leader's answer: the write
 * is durable on the leader, so a retryable refusal in its place makes the client replay it, and the replay applies it twice.
 */
class DatabaseAbstractHandlerLeaderAnswerTest {

  @AfterEach
  void clearContext() {
    ForwardedRequestIdContext.clear();
  }

  @Test
  void aFailureAfterOneCommittedForwardIsAnsweredWithTheLeadersAnswer() {
    ForwardedRequestIdContext.armCommittedWriteAnswer();
    ForwardedRequestIdContext.publishCommittedWriteAnswer("{\"result\":[{\"k\":7}]}");
    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();

    final boolean answered = DatabaseAbstractHandler.answerWithTheLeadersAnswer(response,
        new NeedRetryException("Database 'db' is being replaced from the leader"));

    assertThat(answered).isTrue();
    assertThat(response.get().getCode()).isEqualTo(200);
    assertThat(response.get().getResponse()).isEqualTo("{\"result\":[{\"k\":7}]}");
  }

  /** The handler had rendered an answer already: it is the request's answer, the leader's is only what makes the failure moot. */
  @Test
  void anAnswerTheHandlerAlreadyRenderedIsKept() {
    ForwardedRequestIdContext.armCommittedWriteAnswer();
    ForwardedRequestIdContext.publishCommittedWriteAnswer("from the leader");
    final ExecutionResponse rendered = new ExecutionResponse(200, "rendered");
    final AtomicReference<ExecutionResponse> response = new AtomicReference<>(rendered);

    assertThat(DatabaseAbstractHandler.answerWithTheLeadersAnswer(response, new IllegalStateException("late failure"))).isTrue();
    assertThat(response.get()).isSameAs(rendered);
  }

  @Test
  void aFailureOfARequestThatForwardedNothingIsRaisedAsItIs() {
    ForwardedRequestIdContext.armCommittedWriteAnswer();
    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();

    assertThat(DatabaseAbstractHandler.answerWithTheLeadersAnswer(response, new NeedRetryException("refused"))).isFalse();
    assertThat(response.get()).isNull();
  }

  @Test
  void aFailureOfARequestThatForwardedSeveralWritesIsRaisedAsItIs() {
    ForwardedRequestIdContext.armCommittedWriteAnswer();
    ForwardedRequestIdContext.publishCommittedWriteAnswer("first");
    ForwardedRequestIdContext.publishCommittedWriteAnswer("second");
    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();

    assertThat(DatabaseAbstractHandler.answerWithTheLeadersAnswer(response, new NeedRetryException("refused"))).isFalse();
    assertThat(response.get()).isNull();
  }
}
