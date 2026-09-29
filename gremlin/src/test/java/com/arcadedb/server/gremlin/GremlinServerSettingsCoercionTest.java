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

import com.arcadedb.server.ServerException;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8578: a {@code gremlin.*} server setting given as text is converted to the setting's type, and one that cannot be
 * converted fails the start instead of leaving the default in place.
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
  void aValueThatCannotBeConvertedFailsTheStart() {
    final Settings settings = new Settings();
    assertThatThrownBy(() -> GremlinServerPlugin.applyServerSetting(settings, "port", "not-a-port"))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("gremlin.port")
        .hasMessageContaining("not-a-port");
  }

  @Test
  void otherNumericTypesAreConverted() {
    final Settings settings = new Settings();
    GremlinServerPlugin.applyServerSetting(settings, "evaluationTimeout", "1500");
    GremlinServerPlugin.applyServerSetting(settings, "maxContentLength", "2048");
    assertThat(settings.evaluationTimeout).isEqualTo(1500L);
    assertThat(settings.maxContentLength).isEqualTo(2048);
  }

  @Test
  void aBooleanIsOnlyTrueOrFalse() {
    final Settings settings = new Settings();
    assertThatThrownBy(() -> GremlinServerPlugin.applyServerSetting(settings, "strictTransactionManagement", "yes"))
        .as("'yes' is not a boolean: refused, not turned into false").isInstanceOf(ServerException.class);
    GremlinServerPlugin.applyServerSetting(settings, "strictTransactionManagement", "TRUE");
    assertThat(settings.strictTransactionManagement).isTrue();
  }

  @Test
  void aTextValueForANonScalarSettingIsSkippedNotFatal() {
    final Settings settings = new Settings();
    GremlinServerPlugin.applyServerSetting(settings, "serializers", "not-a-list");
    assertThat(settings.serializers).isNotEqualTo("not-a-list");
  }

  @Test
  void anUnknownSettingIsIgnored() {
    final Settings settings = new Settings();
    final int defaultPort = settings.port;
    GremlinServerPlugin.applyServerSetting(settings, "arcadedb.directory", "x");
    assertThat(settings.port).isEqualTo(defaultPort);
  }
}
