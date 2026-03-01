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
package com.arcadedb.server.ha.exception;

import com.arcadedb.server.ha.ReplicationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReplicationExceptionTest {

  @Test
  void testTransientExceptionIsReplicationException() {
    ReplicationTransientException ex = new ReplicationTransientException("timeout");
    assertThat(ex).isInstanceOf(ReplicationException.class);
    assertThat(ex.getMessage()).isEqualTo("timeout");
  }

  @Test
  void testPermanentExceptionIsReplicationException() {
    ReplicationPermanentException ex = new ReplicationPermanentException("protocol error");
    assertThat(ex).isInstanceOf(ReplicationException.class);
    assertThat(ex.getMessage()).isEqualTo("protocol error");
  }

  @Test
  void testLeadershipChangeExceptionIsReplicationException() {
    LeadershipChangeException ex = new LeadershipChangeException("leader changed", "server1", "server2");
    assertThat(ex).isInstanceOf(ReplicationException.class);
    assertThat(ex.getMessage()).isEqualTo("leader changed");
    assertThat(ex.getFormerLeader()).isEqualTo("server1");
    assertThat(ex.getNewLeader()).isEqualTo("server2");
  }

  @Test
  void testTransientExceptionWithCause() {
    Exception cause = new java.net.SocketTimeoutException("connection timeout");
    ReplicationTransientException ex = new ReplicationTransientException("timeout", cause);
    assertThat(ex.getCause()).isEqualTo(cause);
  }
}
