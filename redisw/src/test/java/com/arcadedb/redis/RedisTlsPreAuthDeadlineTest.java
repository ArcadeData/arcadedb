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
package com.arcadedb.redis;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.SocketTimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The pre-authentication deadline of the Redis listener when {@code arcadedb.redis.tls} is on: a connection that has not
 * authenticated by the deadline is closed, and one that has is not.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class RedisTlsPreAuthDeadlineTest extends RedisTlsPreAuthTestBase {

  private static final long DEADLINE_MS = 2_000;

  @Override
  long deadlineMs() {
    return DEADLINE_MS;
  }

  @Test
  void anAuthenticatedTlsConnectionOutlivesTheDeadline() throws Exception {
    final SSLSocket client = connectTls();

    send(client, "AUTH root " + DEFAULT_PASSWORD_FOR_TESTS + "\r\n");
    assertThat(readLine(client)).isEqualTo("+OK");

    Thread.sleep(DEADLINE_MS + 2_500);

    send(client, "PING\r\n");
    assertThat(readLine(client)).isEqualTo("+PONG");
  }

  @Test
  void aTlsConnectionThatKeepsSendingCommandsIsClosedAtTheDeadline() throws Exception {
    final SSLSocket client = connectTls();

    // a hang detector, not a latency bound: the deadline is 2 s and the reaper ticks every second
    final long giveUpAt = System.currentTimeMillis() + 30_000;
    boolean closed = false;
    while (!closed && System.currentTimeMillis() < giveUpAt)
      try {
        send(client, "PING\r\n");
        assertThat(readLine(client)).startsWith("-NOAUTH");
        Thread.sleep(100);
      } catch (final SocketTimeoutException e) {
        throw e; // silence is not a close
      } catch (final IOException e) {
        closed = true;
      }

    assertThat(closed).as("the connection is closed at the deadline").isTrue();
  }
}
