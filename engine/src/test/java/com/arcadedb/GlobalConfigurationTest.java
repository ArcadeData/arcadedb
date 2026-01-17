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
package com.arcadedb;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GlobalConfigurationTest extends TestHelper {
  @Test
  void serverMode() {
    final String original = GlobalConfiguration.SERVER_MODE.getValueAsString();

    GlobalConfiguration.SERVER_MODE.setValue("development");
    assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo("development");

    GlobalConfiguration.SERVER_MODE.setValue("test");
    assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo("test");

    GlobalConfiguration.SERVER_MODE.setValue("production");
    assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo("production");

    assertThatThrownBy(() -> GlobalConfiguration.SERVER_MODE.setValue("notvalid")).isInstanceOf(IllegalArgumentException.class);

    GlobalConfiguration.SERVER_MODE.setValue(original);
  }

  @Test
  void typeConversion() {
    final int original = GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.getValueAsInteger();

    assertThatThrownBy(() -> GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue("notvalid")).isInstanceOf(NumberFormatException.class);

    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue(original);
  }

  @Test
  void defaultValue() {
    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.reset();
    final int original = GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.getValueAsInteger();

    assertThat(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.getDefValue()).isEqualTo(original);
    assertThat(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.isChanged()).isFalse();
    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue(0);
    assertThat(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.isChanged()).isTrue();

    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue(original);
  }

  @Test
  void testHAEnhancedReconnectionConfig() {
    // Test feature flag
    assertThat(GlobalConfiguration.HA_ENHANCED_RECONNECTION).isNotNull();
    assertThat(GlobalConfiguration.HA_ENHANCED_RECONNECTION.getDefValue()).isEqualTo(false);
    assertThat(GlobalConfiguration.HA_ENHANCED_RECONNECTION.getType())
        .isEqualTo(Boolean.class);

    // Test transient failure config
    assertThat(GlobalConfiguration.HA_TRANSIENT_FAILURE_MAX_ATTEMPTS.getDefValue()).isEqualTo(3);
    assertThat(GlobalConfiguration.HA_TRANSIENT_FAILURE_BASE_DELAY_MS.getDefValue()).isEqualTo(1000L);

    // Test unknown error config
    assertThat(GlobalConfiguration.HA_UNKNOWN_ERROR_MAX_ATTEMPTS.getDefValue()).isEqualTo(5);
    assertThat(GlobalConfiguration.HA_UNKNOWN_ERROR_BASE_DELAY_MS.getDefValue()).isEqualTo(2000L);
  }
}
