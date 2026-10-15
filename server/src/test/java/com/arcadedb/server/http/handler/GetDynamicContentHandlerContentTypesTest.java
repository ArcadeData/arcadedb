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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10103: the binary types the build protects from resource filtering (the root {@code pom.xml} carve-out) and the
 * binary types the server serves ({@link GetDynamicContentHandler#CONTENT_TYPES}) are one list. The POM side is checked by
 * {@code studio/test/static-content-types.test.js}, which reads this table; this pins the table side: every one of those
 * extensions is answered with its own type, never the {@code text/plain} fallback, and is never run through the template
 * engine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GetDynamicContentHandlerContentTypesTest {

  @Test
  void everyBinaryTypeIsServedWithItsOwnContentType() {
    final Map<String, String> expected = Map.ofEntries(//
        Map.entry("png", "image/png"),//
        Map.entry("jpg", "image/jpeg"),//
        Map.entry("jpeg", "image/jpeg"),//
        Map.entry("gif", "image/gif"),//
        Map.entry("bmp", "image/bmp"),//
        Map.entry("webp", "image/webp"),//
        Map.entry("avif", "image/avif"),//
        Map.entry("ico", "image/x-icon"),//
        Map.entry("svg", "image/svg+xml"),//
        Map.entry("woff", "font/woff"),//
        Map.entry("woff2", "font/woff2"),//
        Map.entry("ttf", "font/ttf"),//
        Map.entry("otf", "font/otf"),//
        Map.entry("eot", "application/vnd.ms-fontobject"),//
        Map.entry("swf", "application/x-shockwave-flash"),//
        Map.entry("wasm", "application/wasm"));

    for (final Map.Entry<String, String> entry : expected.entrySet()) {
      final GetDynamicContentHandler.ContentTypeSpec spec = GetDynamicContentHandler.CONTENT_TYPES.get(entry.getKey());
      assertThat(spec).as("the table has an entry for .%s: it would be served as text/plain", entry.getKey()).isNotNull();
      assertThat(spec.contentType()).as("Content-Type of .%s", entry.getKey()).isEqualTo(entry.getValue());
      assertThat(spec.templated()).as(".%s is a binary: running it through the template engine would corrupt it", entry.getKey())
          .isFalse();
    }
  }

  @Test
  void anUnknownExtensionIsServedAsPlainText() {
    assertThat(GetDynamicContentHandler.CONTENT_TYPES.get("unknownext")).isNull();
    assertThat(GetDynamicContentHandler.UNKNOWN_CONTENT_TYPE.contentType()).isEqualTo("text/plain");
  }
}
