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
  void maxPageRAMAutoTune() {
    final long originalValue = GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong();

    try {
      // When reset (no explicit value), maxPageRAM should be auto-tuned to ~25% of max heap
      GlobalConfiguration.MAX_PAGE_RAM.reset();
      final long autoTunedMB = GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong();
      final long maxHeapMB = Runtime.getRuntime().maxMemory() / 1024 / 1024;

      // Auto-tuned value should be approximately 25% of max heap, in MB
      assertThat(autoTunedMB).isLessThanOrEqualTo(maxHeapMB);
      assertThat(autoTunedMB).isEqualTo(Runtime.getRuntime().maxMemory() / 4 / 1024 / 1024);
    } finally {
      GlobalConfiguration.MAX_PAGE_RAM.setValue(originalValue);
    }
  }

  @Test
  void maxPageRAMCorrectionReturnsMB() {
    final long originalValue = GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong();

    try {
      // Set maxPageRAM to a value exceeding 80% of heap to trigger correction
      final long maxHeapMB = Runtime.getRuntime().maxMemory() / 1024 / 1024;
      final long excessiveValueMB = maxHeapMB; // 100% of heap, definitely > 80%

      GlobalConfiguration.MAX_PAGE_RAM.setValue(excessiveValueMB);
      final long correctedValue = GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong();

      // The corrected value must be in MB (should be ~50% of heap in MB)
      // Before the fix, this would return bytes instead of MB
      final long expectedMB = Runtime.getRuntime().maxMemory() / 2 / 1024 / 1024;
      assertThat(correctedValue).isEqualTo(expectedMB);
      assertThat(correctedValue).isLessThanOrEqualTo(maxHeapMB);
    } finally {
      GlobalConfiguration.MAX_PAGE_RAM.setValue(originalValue);
    }
  }

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
}
