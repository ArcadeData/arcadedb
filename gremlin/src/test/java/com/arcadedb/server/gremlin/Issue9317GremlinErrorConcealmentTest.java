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

import com.arcadedb.server.ArcadeDBServer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #9317: the Gremlin Server answers an evaluation failure with {@code Throwable.getMessage()} verbatim, which for a
 * duplicated key holds the customer's stored values. In production mode the answer now carries the one placeholder every
 * other surface uses, and the exception attributes (class names, stack trace) are dropped with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9317GremlinErrorConcealmentTest {
  private static final String SECRET = "Error on executing gremlin query: Duplicated key [already-there] found on index 'T[k]'";

  private static ResponseMessage failure(final ResponseStatusCode code) {
    return ResponseMessage.build(UUID.randomUUID()).code(code).statusMessage(SECRET).statusAttributeException(new IllegalStateException(SECRET))
        .create();
  }

  private static ResponseMessage send(final boolean production, final ResponseMessage message) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.isProductionMode()).thenReturn(production);
    return ConcealingWebSocketChannelizer.conceal(server, message);
  }

  @Test
  void productionModeConcealsEveryServerFailureCode() {
    for (final ResponseStatusCode code : new ResponseStatusCode[] { ResponseStatusCode.SERVER_ERROR, ResponseStatusCode.SERVER_ERROR_EVALUATION,
        ResponseStatusCode.SERVER_ERROR_TEMPORARY, ResponseStatusCode.SERVER_ERROR_SERIALIZATION }) {
      final ResponseMessage original = failure(code);
      final ResponseMessage answer = send(true, original);

      assertThat(answer.getStatus().getMessage()).as(code.name()).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
      assertThat(answer.getStatus().getCode()).isEqualTo(code);
      assertThat(answer.getRequestId()).isEqualTo(original.getRequestId());
      assertThat(answer.getStatus().getAttributes()).as("no exception class or stack trace").isEmpty();
    }
  }

  @Test
  void developmentModeKeepsTheMessage() {
    final ResponseMessage answer = send(false, failure(ResponseStatusCode.SERVER_ERROR_EVALUATION));

    assertThat(answer.getStatus().getMessage()).isEqualTo(SECRET);
  }

  @Test
  void answersThatCarryBoundedTextAreLeftAlone() {
    for (final ResponseStatusCode code : new ResponseStatusCode[] { ResponseStatusCode.UNAUTHORIZED, ResponseStatusCode.FORBIDDEN,
        ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST, ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS,
        ResponseStatusCode.SERVER_ERROR_TIMEOUT, ResponseStatusCode.SUCCESS }) {
      final ResponseMessage answer = send(true, failure(code));

      assertThat(answer.getStatus().getMessage()).as(code.name()).isEqualTo(SECRET);
    }
  }
}
