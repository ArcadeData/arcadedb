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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7233: the RESP protocol limits are SCOPE.SERVER, so they live in the server's {@link ContextConfiguration}
 * - written by the server configuration file, {@code SET SERVER SETTING} and the MCP tool - and used to be read off
 * the {@link GlobalConfiguration} enum, which only a system property or an environment variable ever writes.
 * <p>
 * The limit is resolved by a helper that takes the setting as a PARAMETER, which is exactly the shape
 * {@code Issue7233ServerScopeSettingReadsTest} documents it cannot see - so this asserts the runtime value rather
 * than the shape of the call.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7233RedisLimitsReadServerConfigurationTest {

  @Test
  void theLimitComesFromTheServerConfiguration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.REDIS_MAX_BULK_LENGTH, 4096);

    assertThat(RedisNetworkExecutor.sanitizedLimit(configuration, GlobalConfiguration.REDIS_MAX_BULK_LENGTH, 1))
        .isEqualTo(4096);
  }

  @Test
  void aServerConfigurationFileValueReachesTheLimitToo() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"redis.maxMultiBulkLength\":321}}");

    assertThat(RedisNetworkExecutor.sanitizedLimit(configuration, GlobalConfiguration.REDIS_MAX_MULTIBULK_LENGTH, 1))
        .isEqualTo(321);
  }

  @Test
  void anUnusableLimitStillFallsBackToTheDefault() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.REDIS_MAX_MULTIBULK_DEPTH, 0);

    assertThat(RedisNetworkExecutor.sanitizedLimit(configuration, GlobalConfiguration.REDIS_MAX_MULTIBULK_DEPTH, 2))
        .isEqualTo(((Number) GlobalConfiguration.REDIS_MAX_MULTIBULK_DEPTH.getDefValue()).intValue());
  }
}
