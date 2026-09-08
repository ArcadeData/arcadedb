/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HALogTest {

  @AfterEach
  void resetLogLevel() {
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(0);
    HALog.refreshLevel();
  }

  @Test
  void isEnabledReturnsFalseWhenLevelIsZero() {
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(0);
    HALog.refreshLevel();
    assertThat(HALog.isEnabled(HALog.BASIC)).isFalse();
    assertThat(HALog.isEnabled(HALog.DETAILED)).isFalse();
    assertThat(HALog.isEnabled(HALog.TRACE)).isFalse();
  }

  @Test
  void isEnabledRespectsConfiguredLevel() {
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(2);
    HALog.refreshLevel();
    assertThat(HALog.isEnabled(HALog.BASIC)).isTrue();
    assertThat(HALog.isEnabled(HALog.DETAILED)).isTrue();
    assertThat(HALog.isEnabled(HALog.TRACE)).isFalse();
  }

  /**
   * Issue #7233: {@code arcadedb.ha.logVerbose} is SCOPE.SERVER, so it is authoritative in the SERVER's
   * {@link com.arcadedb.ContextConfiguration} - which is what the server configuration file, {@code SET SERVER
   * SETTING} and the MCP tool write into - and this class used to cache it off the {@link GlobalConfiguration}
   * enum, which a system property or an environment variable alone ever writes. {@code RaftHAPlugin.configure}
   * now hands the server's configuration over.
   */
  @Test
  void configureTakesTheLevelFromTheServerConfiguration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"ha.logVerbose\":3}}");

    // The enum says the level is off; the server's own configuration says TRACE, and it is the one that counts.
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(0);
    HALog.configure(configuration);

    assertThat(HALog.isEnabled(HALog.TRACE)).isTrue();
  }

  @Test
  void logDoesNotThrowWhenDisabled() {
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(0);
    HALog.refreshLevel();
    HALog.log(this, HALog.BASIC, "should not appear: %s", "test");
  }

  @Test
  void logDoesNotThrowWhenEnabled() {
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(3);
    HALog.refreshLevel();
    HALog.log(this, HALog.TRACE, "trace message: %s %d", "test", 42);
  }
}
