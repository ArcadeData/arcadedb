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

import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8578, review of PR #8680: a {@code gremlin.*} server setting given as text is converted to the setting's type,
 * and one that cannot be converted leaves the default in place (with a WARNING) instead of failing the start.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinServerSettingsCoercionTest {

  @Test
  void textIsConvertedToTheSettingType() {
    final Settings settings = new Settings();
    GremlinServerPlugin.applyServerSetting(settings, "port", "18183");
    GremlinServerPlugin.applyServerSetting(settings, "gremlinPool", 3);
    assertThat(settings.port).isEqualTo(18183);
    assertThat(settings.gremlinPool).isEqualTo(3);
  }

  @Test
  void aValueThatCannotBeConvertedKeepsTheDefault() {
    final Settings settings = new Settings();
    final int defaultPort = settings.port;
    GremlinServerPlugin.applyServerSetting(settings, "port", "not-a-port");
    assertThat(settings.port).isEqualTo(defaultPort);
  }

  @Test
  void aBooleanIsOnlyTrueOrFalse() {
    final Settings settings = new Settings();
    final boolean defaultValue = settings.strictTransactionManagement;
    GremlinServerPlugin.applyServerSetting(settings, "strictTransactionManagement", "yes");
    assertThat(settings.strictTransactionManagement).as("'yes' is not a boolean: ignored, not turned into false").isEqualTo(defaultValue);
    GremlinServerPlugin.applyServerSetting(settings, "strictTransactionManagement", "TRUE");
    assertThat(settings.strictTransactionManagement).isTrue();
  }

  @Test
  void anUnknownSettingIsIgnored() {
    final Settings settings = new Settings();
    final int defaultPort = settings.port;
    GremlinServerPlugin.applyServerSetting(settings, "arcadedb.directory", "x");
    assertThat(settings.port).isEqualTo(defaultPort);
  }
}
