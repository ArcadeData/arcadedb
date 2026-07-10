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
package com.arcadedb.log;

import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DefaultLogger#resolveConfigurableLogDir(String)}. Co-located in the
 * {@code com.arcadedb.log} package so the package-private helper and the {@code DEFAULT_LOG_DIR}
 * constant can be exercised directly, without reflection.
 */
class DefaultLoggerLogDirTest {
  /**
   * An unset ${arcadedb.server.logsDirectory} placeholder must fall back to the default directory
   * so existing single-host installs are unaffected.
   */
  @Test
  void logDirPlaceholderFallsBackToDefaultWhenUnset() {
    final String prev = System.getProperty("arcadedb.server.logsDirectory");
    System.clearProperty("arcadedb.server.logsDirectory");
    try {
      assertThat(DefaultLogger.resolveConfigurableLogDir("${arcadedb.server.logsDirectory}/arcadedb.log"))
          .isEqualTo("./log/arcadedb.log");
    } finally {
      if (prev != null)
        System.setProperty("arcadedb.server.logsDirectory", prev);
    }
  }

  /**
   * ${arcadedb.server.logsDirectory} must resolve from the system property so logs can be relocated
   * to a writable mount on a read-only root filesystem.
   */
  @Test
  void logDirPlaceholderResolvesFromSystemProperty() {
    final String prev = System.getProperty("arcadedb.server.logsDirectory");
    System.setProperty("arcadedb.server.logsDirectory", "/var/writable/arcade-logs");
    try {
      assertThat(DefaultLogger.resolveConfigurableLogDir("${arcadedb.server.logsDirectory}/arcadedb.log"))
          .isEqualTo("/var/writable/arcade-logs/arcadedb.log");
    } finally {
      if (prev != null)
        System.setProperty("arcadedb.server.logsDirectory", prev);
      else
        System.clearProperty("arcadedb.server.logsDirectory");
    }
  }

  /**
   * A literal FileHandler pattern with no ${...} placeholder must pass through unchanged, so
   * operators using an absolute path are unaffected.
   */
  @Test
  void logDirLiteralPatternPassesThroughUnchanged() {
    assertThat(DefaultLogger.resolveConfigurableLogDir("/custom/log/arcadedb.log"))
        .isEqualTo("/custom/log/arcadedb.log");
  }

  /**
   * The resolver's fallback directory and the configured default must agree, otherwise an unset
   * placeholder would resolve to a different directory than the documented configuration default.
   */
  @Test
  void defaultLogDirMatchesConfiguredDefault() {
    assertThat(GlobalConfiguration.SERVER_LOGS_DIRECTORY.getDefValue()).isEqualTo(DefaultLogger.DEFAULT_LOG_DIR);
  }
}
