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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TransactionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Issue #11064: how a follower recognizes the answer of a leader that predates the {@code ensureLabelTypes} function. */
class Issue11064UnknownFunctionAnswerTest {

  @Test
  void theAnswerOfALeaderWithoutTheFunctionIsRecognized() {
    final RuntimeException error = RaftReplicatedDatabase.reconstructLeaderException(500,
        "{\"error\":\"Error on command execution\",\"detail\":\"Function not found: ensureLabelTypes\","
            + "\"exception\":\"com.arcadedb.exception.CommandExecutionException\"}");
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(error, "ensureLabelTypes")).isTrue();
  }

  @Test
  void theLookupWordingIsRecognizedToo() {
    final RuntimeException error = RaftReplicatedDatabase.reconstructLeaderException(500,
        "{\"error\":\"Error on command execution\",\"detail\":\"Unknown function name 'ensureLabelTypes'\","
            + "\"exception\":\"com.arcadedb.exception.CommandExecutionException\"}");
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(error, "ensureLabelTypes")).isTrue();
  }

  @Test
  void theNameIsMatchedAnyCaseAndThroughTheCauseChain() {
    final RuntimeException error = new TransactionException("wrapped", new CommandExecutionException("Function not found: ENSURELABELTYPES"));
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(error, "ensureLabelTypes")).isTrue();
  }

  @Test
  void anyOtherErrorIsNotAnUnknownFunction() {
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(new TransactionException("boom"), "ensureLabelTypes")).isFalse();
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(new CommandExecutionException("Function not found: other"),
        "ensureLabelTypes")).isFalse();
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(new TransactionException((String) null), "ensureLabelTypes")).isFalse();
  }
}
