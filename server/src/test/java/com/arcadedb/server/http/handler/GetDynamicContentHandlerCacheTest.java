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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link GetDynamicContentHandler#loadStaticResource(String)}: immutable classpath assets
 * are read once and their raw bytes cached for the process lifetime, so repeated requests skip the
 * stream read + copy. A missing resource returns {@code null}.
 */
class GetDynamicContentHandlerCacheTest {

  private static final String PROBE = "static/js/cache-probe.js";

  @Test
  void returnsResourceBytes() throws Exception {
    final byte[] bytes = GetDynamicContentHandler.loadStaticResource(PROBE);
    assertThat(bytes).isNotNull();
    assertThat(new String(bytes, StandardCharsets.UTF_8)).contains("cache-probe");
  }

  @Test
  void secondCallReturnsSameCachedInstance() throws Exception {
    final byte[] first = GetDynamicContentHandler.loadStaticResource(PROBE);
    final byte[] second = GetDynamicContentHandler.loadStaticResource(PROBE);
    assertThat(first).isNotNull();
    // Cache hit: the exact same array is handed back rather than a fresh copy.
    assertThat(second).isSameAs(first);
  }

  @Test
  void missingResourceReturnsNull() throws Exception {
    assertThat(GetDynamicContentHandler.loadStaticResource("static/js/does-not-exist-5035.js")).isNull();
  }
}
