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
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12436: a DDL statement a follower forwards to the leader is refused there for a caller's mistake (a bare word where a value
 * is expected, a bound that cannot be read, a bucket a type owns) as an {@link InvalidArgumentException}, which the leader answers
 * 400. The follower rebuilds the exception from the answer, by its class name: without an entry for it the refusal fell to the
 * generic {@link TransactionException}, and the follower's client got a 500 "Error on transaction commit" for the same statement
 * the leader answers 400.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12436ForwardedDdlRefusalTest {

  @Test
  void aRefusedValueIsRebuiltAsTheInvalidArgumentTheLeaderRaised() {
    final String body = new JSONObject().put("error", "Cannot execute command").put("exception", InvalidArgumentException.class.getName())
        .put("detail", "Cannot drop bucket 'T_0' because used by type 'T'").toString();

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(400, body, null);

    assertThat(rebuilt).isInstanceOf(InvalidArgumentException.class).isInstanceOf(CommandExecutionException.class)
        .hasMessage("Cannot drop bucket 'T_0' because used by type 'T'");
    assertThat(rebuilt).isNotInstanceOf(TransactionException.class);
    assertThat(ErrorCategory.of(rebuilt)).as("the caller's mistake, as on the leader").isEqualTo(ErrorCategory.VALIDATION);
  }

  /** The refusal of a constraint the stored records break reaches the follower as the validation error with its kind. */
  @Test
  void aConstraintTheStoredDataBreakKeepsItsKindAcrossTheForward() {
    final String body = new JSONObject().put("error", "Constraint violated (MANDATORY)").put("exception", ValidationException.class.getName())
        .put("exceptionArgs", "MANDATORY").put("detail", "Cannot set MANDATORY: record #1:0 violates it").toString();

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(400, body, null);

    assertThat(rebuilt).isInstanceOf(ValidationException.class);
    assertThat(((ValidationException) rebuilt).getKind()).isEqualTo(ValidationException.Kind.MANDATORY);
    assertThat(ErrorCategory.of(rebuilt)).isEqualTo(ErrorCategory.VALIDATION);
  }
}
