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
package com.arcadedb.integration.exporter;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8301: the default export file name was built with a {@code SimpleDateFormat} that took the default locale's
 * calendar and digits, so the same moment was named with the Buddhist year under th-TH and with Arabic-Indic digits
 * under ar-EG.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8301ExporterDefaultFileNameTest {

  @Test
  void defaultFileNameIsLocaleIndependent() {
    final Locale saved = Locale.getDefault();
    try {
      for (final String tag : new String[] { "th-TH", "ar-EG", "ja-JP-u-ca-japanese", "en-US" }) {
        Locale.setDefault(Locale.forLanguageTag(tag));
        final String year = String.valueOf(LocalDate.now().getYear());

        final ExporterSettings settings = new ExporterSettings();
        settings.format = "jsonl";
        settings.parseParameters(new String[0]);

        assertThat(settings.file).as("locale %s", tag).matches("arcadedb-backup-\\d{8}-\\d{9}\\.jsonl\\.tgz");
        assertThat(settings.file).as("locale %s", tag).startsWith("arcadedb-backup-" + year);
      }
    } finally {
      Locale.setDefault(saved);
    }
  }
}
