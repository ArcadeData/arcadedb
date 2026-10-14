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

import com.arcadedb.query.RunningQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9962: the Gremlin Server's pipeline publishes the connection of a request for the length of its read, where the
 * evaluation is submitted to the Gremlin pool and its {@code list queries} entry picks the client up. Pinned here at the
 * publishing itself, independently of the TinkerPop internals the end-to-end test ({@code Issue9689GremlinTerminateTest})
 * relies on. Netty is relocated in the module's shaded jar, so the channel handler around it is reached only end to end.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9962GremlinRequestClientTest {
  private static final RunningQuery.Client CLIENT = new RunningQuery.Client("c0ffee", "/10.0.0.7:52100");

  @AfterEach
  void clear() {
    GremlinAuthContext.setClient(null);
  }

  @Test
  void aReadSeesItsConnectionAndLeavesNothingBehind() throws Exception {
    final List<RunningQuery.Client> seen = new ArrayList<>();
    GremlinAuthContext.publishing(CLIENT, () -> seen.add(GremlinAuthContext.getClient()));

    assertThat(seen).containsExactly(CLIENT);
    assertThat(GremlinAuthContext.getClient()).as("cleared once the read is over").isNull();
  }

  @Test
  void aFailedReadLeavesNothingBehindEither() {
    final List<RunningQuery.Client> seen = new ArrayList<>();
    assertThatThrownBy(() -> GremlinAuthContext.publishing(CLIENT, () -> {
      seen.add(GremlinAuthContext.getClient());
      throw new IllegalStateException("evaluation could not be submitted");
    })).hasMessage("evaluation could not be submitted");

    assertThat(seen).containsExactly(CLIENT);
    assertThat(GremlinAuthContext.getClient()).as("a reused Netty thread must not keep it for the next channel").isNull();
  }

  @Test
  void aSocketPeerTheTransportDoesNotReportIsNoAddress() {
    assertThat(RunningQuery.Client.of("c0ffee", null).address()).isNull();
  }
}
