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
import static org.assertj.core.api.Assertions.fail;

public class GlobalConfigurationTest extends TestHelper {
  @Test
  public void testServerMode() {
    final String original = GlobalConfiguration.SERVER_MODE.getValueAsString();

    GlobalConfiguration.SERVER_MODE.setValue("development");
    assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo("development");

    GlobalConfiguration.SERVER_MODE.setValue("test");
    assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo("test");

    GlobalConfiguration.SERVER_MODE.setValue("production");
    assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo("production");

    try {
      GlobalConfiguration.SERVER_MODE.setValue("notvalid");
      fail("");
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }

    GlobalConfiguration.SERVER_MODE.setValue(original);
  }

  @Test
  public void testTypeConversion() {
    final int original = GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.getValueAsInteger();

    try {
      GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue("notvalid");
      fail("");
    } catch (final NumberFormatException e) {
      // EXPECTED
    }

    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue(original);
  }

  @Test
  public void testDefaultValue() {
    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.reset();
    final int original = GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.getValueAsInteger();

    assertThat(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.getDefValue()).isEqualTo(original);
    assertThat(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.isChanged()).isFalse();
    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue(0);
    assertThat(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.isChanged()).isTrue();

    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue(original);
  }
}
