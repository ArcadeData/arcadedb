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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GlobalConfigurationTest extends TestHelper {
  @Test
  public void testServerMode() {
    final String original = GlobalConfiguration.SERVER_MODE.getValueAsString();

    GlobalConfiguration.SERVER_MODE.setValue("development");
    Assertions.assertEquals("development", GlobalConfiguration.SERVER_MODE.getValueAsString());

    GlobalConfiguration.SERVER_MODE.setValue("test");
    Assertions.assertEquals("test", GlobalConfiguration.SERVER_MODE.getValueAsString());

    GlobalConfiguration.SERVER_MODE.setValue("production");
    Assertions.assertEquals("production", GlobalConfiguration.SERVER_MODE.getValueAsString());

    try {
      GlobalConfiguration.SERVER_MODE.setValue("notvalid");
      Assertions.fail();
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
      Assertions.fail();
    } catch (final NumberFormatException e) {
      // EXPECTED
    }

    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue(original);
  }

  @Test
  public void testDefaultValue() {
    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.reset();
    final int original = GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.getValueAsInteger();

    Assertions.assertEquals(original, GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.getDefValue());
    Assertions.assertFalse(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.isChanged());
    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue(0);
    Assertions.assertTrue(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.isChanged());

    GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE.setValue(original);
  }
}
