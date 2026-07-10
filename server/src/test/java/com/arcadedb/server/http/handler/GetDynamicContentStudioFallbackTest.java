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
 * Regression test for issue #5182 (Problem 2): the "base"/"headless" distributions ship without the optional
 * Studio module, so its static assets are absent from the classpath. Browsing to the server root must not
 * return a bare "Not Found" (which reads like a broken server); instead a small self-contained landing page
 * explains that Studio is not bundled and how to enable it. Any other missing asset stays a genuine 404.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GetDynamicContentStudioFallbackTest {

  @Test
  void missingIndexServesStudioNotBundledLandingPage() {
    final byte[] fallback = GetDynamicContentHandler.missingResourceFallback("static/index.html");
    assertThat(fallback).isNotNull();

    final String page = new String(fallback, StandardCharsets.UTF_8);
    assertThat(page).contains("<!DOCTYPE html>");
    assertThat(page).contains("Studio");
    assertThat(page).contains("not bundled");
    // The page must be self-contained (no external assets) so it renders without Studio installed.
    assertThat(page).doesNotContain("<script src=");
    assertThat(page).doesNotContain("<link rel=\"stylesheet\"");
  }

  @Test
  void otherMissingAssetsStayGenuine404() {
    // A missing JS/CSS/etc. asset must not be masked by the landing page: the caller emits a real 404.
    assertThat(GetDynamicContentHandler.missingResourceFallback("static/js/app-5182.js")).isNull();
    assertThat(GetDynamicContentHandler.missingResourceFallback("static/css/theme-5182.css")).isNull();
    assertThat(GetDynamicContentHandler.missingResourceFallback("static/studio/index.html")).isNull();
  }
}
