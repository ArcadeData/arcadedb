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
package com.arcadedb.server.monitor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9807: {@code ServerMonitor} built its low-disk and heap-pressure warnings with {@code String.format} and no
 * {@link Locale}. The text is the body of a server event that {@code GET /api/v1/server} serves to any REST client, so under
 * a comma-decimal locale the figures carried a comma and under a locale with its own numbering system they were not even
 * ASCII digits: one cluster answered with a different string from each node.
 * <p>
 * Mutates the JVM default locale for the duration of each test and restores it afterward - tests in this module run
 * sequentially, but this is still process-wide state.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9807ServerMonitorLocaleTest {

  private static final long GB = 1024L * 1024L * 1024L;

  private Locale originalDefault;

  @BeforeEach
  void saveDefaultLocale() {
    originalDefault = Locale.getDefault();
  }

  @AfterEach
  void restoreDefaultLocale() {
    Locale.setDefault(originalDefault);
  }

  @ParameterizedTest
  @ValueSource(strings = { "en-US", "de-DE", "fr-FR", "th-TH-u-nu-thai", "ar-EG-u-nu-arab" })
  void lowDiskWarningIsTheSameStringInEveryLocale(final String languageTag) {
    Locale.setDefault(Locale.forLanguageTag(languageTag));

    assertThat(ServerMonitor.lowDiskMessage(12.5F, (long) (1.2 * GB), 10 * GB, "/data")).isEqualTo(
        "Available space on disk is only 12.5% (1.20 GB free of 10.00 GB total) on '/data'");
  }

  @ParameterizedTest
  @ValueSource(strings = { "en-US", "de-DE", "fr-FR", "th-TH-u-nu-thai", "ar-EG-u-nu-arab" })
  void heapWarningIsTheSameStringInEveryLocale(final String languageTag) {
    Locale.setDefault(Locale.forLanguageTag(languageTag));

    assertThat(ServerMonitor.heapPressureMessage(8.4F, (long) (3.75 * GB), 4 * GB)).isEqualTo(
        "Server overloaded: available heap RAM is only 8.4% (3.75 GB used of 4.00 GB max)");
  }
}
