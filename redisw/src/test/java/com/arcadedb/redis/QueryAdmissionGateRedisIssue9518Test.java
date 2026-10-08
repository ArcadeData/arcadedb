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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9518 over RESP: a command the query admission gate does not start is refused with {@code TRYAGAIN}, the
 * retryable kind of this protocol; {@code PING} is never held behind the queries; and every command gives its slot back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryAdmissionGateRedisIssue9518Test extends BaseRedisServerTest {
  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aCommandTheGateDoesNotStartIsRefusedWithTryAgain() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);

      try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
        assertThatThrownBy(() -> jedis.set("gate9518", "v")).isInstanceOf(JedisDataException.class).hasMessageStartingWith("TRYAGAIN");
        assertThat(jedis.ping()).as("PING is never held behind the queries").isEqualTo("PONG");
      }

      // EVERY COMMAND GIVES ITS SLOT BACK: WITH ONE SLOT AND NO WAITING, A LEAKED ONE WOULD REFUSE THE NEXT
      assertThat(jedis.set("gate9518", "v")).isEqualTo("OK");
      assertThat(jedis.get("gate9518")).isEqualTo("v");
    }
    assertThat(gate.getRunning()).isZero();
  }
}
