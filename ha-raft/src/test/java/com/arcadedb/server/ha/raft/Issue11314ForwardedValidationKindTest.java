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

import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11314: a write a follower forwards to the leader and the leader refuses for a violated constraint comes back as the
 * {@link ValidationException} the leader raised, with the constraint the leader named in {@code exceptionArgs}, so the follower
 * answers its client with the same kind; an unknown function comes back as itself rather than as a server-side
 * {@code TransactionException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11314ForwardedValidationKindTest {

  @Test
  void aViolatedConstraintKeepsItsKindAcrossTheForward() {
    final String body = new JSONObject().put("error", "Constraint violated (NOT_NULL)").put("exception", ValidationException.class.getName())
        .put("exceptionArgs", "NOT_NULL").put("detail", "The property 'T.p' cannot be null").toString();

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(400, body, null);

    assertThat(rebuilt).isInstanceOf(ValidationException.class).hasMessage("The property 'T.p' cannot be null");
    assertThat(((ValidationException) rebuilt).getKind()).isEqualTo(ValidationException.Kind.NOT_NULL);
  }

  @Test
  void aLeaderThatNamesNoKindIsRebuiltWithNone() {
    final String body = new JSONObject().put("error", "Validation error").put("exception", ValidationException.class.getName())
        .put("detail", "refused").toString();

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(400, body, null);

    assertThat(rebuilt).isInstanceOf(ValidationException.class);
    assertThat(((ValidationException) rebuilt).getKind()).isNull();
  }

  @Test
  void anUnknownFunctionIsRebuiltAsItself() {
    final String body = new JSONObject().put("error", "Cannot execute command").put("exception", UnknownFunctionException.class.getName())
        .put("detail", "Unknown function name 'foo'").toString();

    assertThat(RaftReplicatedDatabase.reconstructLeaderException(400, body, null)).isInstanceOf(UnknownFunctionException.class)
        .hasMessage("Unknown function name 'foo'");
  }
}
