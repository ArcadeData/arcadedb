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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Boundary cases of {@link RedisCounterOperations} (#9058, #9059).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RedisCounterOperationsTest {

  @Test
  void parseIntegerAcceptsCanonicalInt64Only() {
    assertThat(RedisCounterOperations.parseInteger("0")).isEqualTo(0L);
    assertThat(RedisCounterOperations.parseInteger("-5")).isEqualTo(-5L);
    assertThat(RedisCounterOperations.parseInteger("9223372036854775807")).isEqualTo(Long.MAX_VALUE);
    assertThat(RedisCounterOperations.parseInteger("-9223372036854775808")).isEqualTo(Long.MIN_VALUE);

    for (final String text : new String[] { "", "-", "+1", "05", "-05", "-0", "9223372036854775808", "-9223372036854775809",
        "99999999999999999999", "1.5", " 1", "1 ", "0x10", "1e3" })
      assertThatThrownBy(() -> RedisCounterOperations.parseInteger(text)).isInstanceOf(RedisException.class)
          .hasMessage("value is not an integer or out of range");
  }

  @Test
  void incrementByFloatFormatsRedisDecimalText() {
    assertThat(RedisCounterOperations.incrementByFloat("0.2").apply("0.1")).isEqualTo("0.3");
    assertThat(RedisCounterOperations.incrementByFloat("-1.5").apply("1.5")).isEqualTo("0");
    assertThat(RedisCounterOperations.incrementByFloat("-0.0").apply(null)).isEqualTo("0");
    assertThat(RedisCounterOperations.incrementByFloat("-2.50").apply(null)).isEqualTo("-2.5");
    assertThat(RedisCounterOperations.incrementByFloat("1").apply(Long.MAX_VALUE)).isEqualTo("9223372036854775808");
    // rounded at the 17th decimal
    assertThat(RedisCounterOperations.incrementByFloat("0.123456789012345678").apply(null)).isEqualTo("0.12345678901234568");
    assertThat(RedisCounterOperations.incrementByFloat("1e-18").apply(null)).isEqualTo("0");
  }

  @Test
  void incrementByFloatRefusesNonFiniteAndNonNumbers() {
    assertThatThrownBy(() -> RedisCounterOperations.incrementByFloat("inf")).hasMessage("increment would produce NaN or Infinity");
    assertThatThrownBy(() -> RedisCounterOperations.incrementByFloat("abc")).hasMessage("value is not a valid float");
    assertThatThrownBy(() -> RedisCounterOperations.incrementByFloat("1").apply("abc")).hasMessage("value is not a valid float");
    assertThatThrownBy(() -> RedisCounterOperations.incrementByFloat("1").apply("NaN")).hasMessage("value is not a valid float");
  }
}
