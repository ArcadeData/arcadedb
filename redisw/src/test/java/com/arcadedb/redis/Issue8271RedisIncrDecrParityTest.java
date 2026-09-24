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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8271: {@code RedisQueryEngine} (the {@code redis} query language, reached over the
 * HTTP command endpoint) and {@code RedisNetworkExecutor} (the RESP wire path, reached with a real Redis client)
 * used to disagree on INCR/DECR arithmetic and validation, even though both edit the same stored value. Each test
 * below drives the identical edge case through both surfaces, on the same database-prefixed key
 * (see {@code RedisNetworkExecutor.resolveKeyAndDatabase}), and checks they now agree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8271RedisIncrDecrParityTest extends BaseRedisServerTest {

  @Test
  void incrbyAcceptsA64BitIncrementOnBothSurfaces() throws Exception {
    // The query language's own increment parsing used to be Integer.parseInt, so a value outside the 32-bit range
    // failed there while the RESP path accepted it.
    final Jedis jedis = connect();
    try {
      assertThat(jedis.incrBy(getDatabaseName() + ".issue8271resp1", 3_000_000_000L)).isEqualTo(3_000_000_000L);
    } finally {
      jedis.close();
    }

    final JSONObject response = executeCommand(0, "redis", "INCRBY issue8271query1 3000000000");
    assertThat(getResultValueAsLong(response)).isEqualTo(3_000_000_000L);
  }

  @Test
  void a64BitOverflowIsRefusedOnBothSurfaces() throws Exception {
    // The query language used to add with plain Type.increment, which wraps silently instead of refusing.
    final String respKey = getDatabaseName() + ".issue8271resp2";
    final Jedis jedis = connect();
    try {
      jedis.set(respKey, String.valueOf(Long.MAX_VALUE));
      assertThatThrownBy(() -> jedis.incr(respKey)).isInstanceOf(JedisDataException.class)
          .hasMessageContaining("increment or decrement would overflow");
      // Refused, not partially applied: the value the overflowing INCR was refused on is still there.
      assertThat(jedis.get(respKey)).isEqualTo(String.valueOf(Long.MAX_VALUE));
    } finally {
      jedis.close();
    }

    executeCommand(0, "redis", "SET issue8271query2 " + Long.MAX_VALUE);
    assertThatThrownBy(() -> executeCommand(0, "redis", "INCR issue8271query2"))
        .hasMessageContaining("increment or decrement would overflow");
  }

  @Test
  void incrOnADoubleLeftByIncrbyfloatIsRefusedOnBothSurfaces() throws Exception {
    // The query language used to keep accepting INCR/DECR on a Double and kept producing doubles; real Redis (and
    // the RESP path) refuses it.
    final String respKey = getDatabaseName() + ".issue8271resp3";
    final Jedis jedis = connect();
    try {
      jedis.incrByFloat(respKey, 1.5);
      assertThatThrownBy(() -> jedis.incr(respKey)).isInstanceOf(JedisDataException.class)
          .hasMessageContaining("value is not an integer or out of range");
    } finally {
      jedis.close();
    }

    executeCommand(0, "redis", "INCRBYFLOAT issue8271query3 1.5");
    assertThatThrownBy(() -> executeCommand(0, "redis", "INCR issue8271query3"))
        .hasMessageContaining("value is not an integer or out of range");
  }

  @Test
  void incrbyfloatAcceptsAStoredFloatStringOnBothSurfaces() throws Exception {
    // The query language used to parse only integral strings for INCRBYFLOAT and refuse "3.3" with "is not a
    // number"; the RESP path already accepted it.
    final String respKey = getDatabaseName() + ".issue8271resp4";
    final Jedis jedis = connect();
    try {
      jedis.set(respKey, "3.3");
      assertThat(jedis.incrByFloat(respKey, 0.1)).isEqualTo(3.4);
    } finally {
      jedis.close();
    }

    executeCommand(0, "redis", "SET issue8271query4 3.3");
    final JSONObject response = executeCommand(0, "redis", "INCRBYFLOAT issue8271query4 0.1");
    assertThat(((Number) getResultValue(response)).doubleValue()).isEqualTo(3.4);
  }

  @Test
  void nonNumericValueRefusalUsesTheSameRealRedisTextOnBothSurfaces() throws Exception {
    // The query language used to answer "Key '<k>' is not a number" instead of real Redis' own text.
    final String respKey = getDatabaseName() + ".issue8271resp5";
    final Jedis jedis = connect();
    try {
      jedis.set(respKey, "not-a-number");
      assertThatThrownBy(() -> jedis.incr(respKey)).isInstanceOf(JedisDataException.class)
          .hasMessageContaining("value is not an integer or out of range");
    } finally {
      jedis.close();
    }

    executeCommand(0, "redis", "SET issue8271query5 not-a-number");
    assertThatThrownBy(() -> executeCommand(0, "redis", "INCR issue8271query5"))
        .hasMessageContaining("value is not an integer or out of range")
        .hasMessageNotContaining("is not a number");
  }

  /**
   * Found in review (not part of the original divergence table): {@code Double.parseDouble}/{@code Double.valueOf}
   * both accept the text "Infinity"/"NaN", so without a finiteness check INCRBYFLOAT would store one and every
   * later INCRBYFLOAT on that key would keep producing a non-finite value. Both surfaces share the same
   * {@code RedisCounterOperations.incrementByFloat}, so pinning it once here covers both.
   * <p>
   * The increment is driven through the query language's free-form command text, which can spell "Infinity"/"NaN"
   * directly: Jedis's typed {@code incrByFloat(key, double)} cannot reach this case over RESP, because it formats
   * an infinite/NaN {@code double} as Redis' own wire spelling ("+inf"/"nan"), which {@code Double.valueOf} does not
   * parse - a pre-existing, unrelated gap in the argument parsing upstream of this shared helper, out of scope
   * here. The RESP side of this test instead pins the other, equally reachable half of the same finiteness check:
   * a value already non-finite when it is READ, such as one written by a plain SET.
   */
  @Test
  void incrbyfloatRefusesANonFiniteIncrementOrStoredValueOnBothSurfaces() throws Exception {
    assertThatThrownBy(() -> executeCommand(0, "redis", "INCRBYFLOAT issue8271query6a Infinity"))
        .hasMessageContaining("increment would produce NaN or Infinity");
    assertThatThrownBy(() -> executeCommand(0, "redis", "INCRBYFLOAT issue8271query6b NaN"))
        .hasMessageContaining("increment would produce NaN or Infinity");

    executeCommand(0, "redis", "SET issue8271query6c Infinity");
    assertThatThrownBy(() -> executeCommand(0, "redis", "INCRBYFLOAT issue8271query6c 1"))
        .hasMessageContaining("value is not a valid float");

    final String respKey = getDatabaseName() + ".issue8271resp6";
    final Jedis jedis = connect();
    try {
      jedis.set(respKey, "Infinity");
      assertThatThrownBy(() -> jedis.incrByFloat(respKey, 1.0)).isInstanceOf(JedisDataException.class)
          .hasMessageContaining("value is not a valid float");
    } finally {
      jedis.close();
    }
  }

  private Jedis connect() {
    final Jedis jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
    return jedis;
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
