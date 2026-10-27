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

/**
 * Issue #10694: the escape {@link RedisVariableKeys#toVariableName} applies must be exactly undone by the single leading
 * {@code $} the global-variable map strips, so that the map is keyed by the client's key verbatim.
 */
class RedisVariableKeysTest {

  @Test
  void aKeyWithoutALeadingDollarIsReturnedAsTheSameInstance() {
    final String key = "k";
    assertThat(RedisVariableKeys.toVariableName(key)).isSameAs(key);
    assertThat(RedisVariableKeys.toVariableName("")).isEmpty();
    assertThat(RedisVariableKeys.toVariableName("a$b")).isEqualTo("a$b");
  }

  @Test
  void aKeyWithALeadingDollarGetsExactlyOneMore() {
    assertThat(RedisVariableKeys.toVariableName("$")).isEqualTo("$$");
    assertThat(RedisVariableKeys.toVariableName("$k")).isEqualTo("$$k");
    assertThat(RedisVariableKeys.toVariableName("$$k")).isEqualTo("$$$k");
  }

  @Test
  void strippingOneLeadingDollarGivesBackTheClientKey() {
    for (final String key : new String[] { "", "$", "$$", "k", "$k", "$$k", "a.b", "$ключ" }) {
      final String name = RedisVariableKeys.toVariableName(key);
      final String stored = name.startsWith("$") ? name.substring(1) : name;
      assertThat(stored).as("key '%s'", key).isEqualTo(key);
    }
  }
}
